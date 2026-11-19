"""Validation of log segments (the wire format).

A segment is a single JSON object::

    {
      "segment": "seg-0007",        // producer-chosen unique id
      "batch": "B-0012",            // engineering batch this change belongs to
      "seq": 12, "total": 20,       // batch order, and how many batches exist
      "part": 0, "parts": 3,        // this segment's slice of the batch
      "upgrade": false,             // true on the batch that introduces zones
      "ids":      [{"old": "J-004521", "new": "C4:004521"}, ...],
      "taps":     [{"ref": "J-004521", "zone": "C4", "local": "004521",
                    "attrs": {"kind": "雨水"}}, ...],
      "lays":     [{"code": "P-000881", "from": "J-004521",
                    "to": "J-004522"}, ...],
      "redirects":[{"code": "P-000881", "from": "J-004530",
                    "to": "J-004531"}, ...],
      "retires":  [{"kind": "joint", "ref": "J-004540"}, ...]
    }

Semantics
---------
``ids``
    Identifier migration table.  Applies from this batch onwards: the legacy
    id and ``<zone>:<local>`` both address the same joint.
``taps``
    Add or update a joint.  ``ref`` is how the joint is named in this batch
    (either form); ``zone``/``local``, when present, give the post-upgrade
    identity explicitly.  Attributes merge; omitted attributes are kept.
``lays``
    Add pipe ``code`` from ``from`` to ``to``.  Re-laying an existing code
    replaces its endpoints ("改向") -- that is how a redirection is expressed
    without a separate verb.
``redirects``
    ``from`` is the upstream side: a joint reference, or a pipe code, or an
    object ``{"ref": ...}`` / ``{"code": ...}``.  When ``from`` is a joint,
    every live pipe currently starting at that joint is re-laid to start at
    ``to`` instead.  When ``from`` is a pipe code, its endpoints are replaced
    by ``to``, which is then ``{"from": ..., "to": ...}``.
``retires``
    ``kind`` is ``joint`` or ``pipe`` (``code`` is accepted as a synonym for
    ``pipe``, addressed by code).  Retiring a joint also retires every live
    pipe attached to it; those count as ``pipe_retired_by_joint`` in the
    batch's effects so a trace can cite exactly what happened.

Any unknown field, wrong type or malformed reference rejects the *whole
segment* before a single row is staged, so a rejected segment cannot leave
partial state behind.
"""

from __future__ import annotations

import hashlib
import json

from .ids import RefError, join_ref, parse_pipe_code, parse_ref

SEGMENT_ARRAYS = ("ids", "taps", "lays", "redirects", "retires")
SEGMENT_FIELDS = {"segment", "batch", "seq", "total", "part", "parts",
                  "upgrade", "producer", "sent_at", *SEGMENT_ARRAYS}
MAX_ID_LEN = 128


class SegmentError(ValueError):
    """A segment that must not be acknowledged."""

    def __init__(self, message: str, *, item: int | None = None,
                 field: str | None = None):
        self.item = item
        self.field = field
        where = []
        if field:
            where.append(f"field {field!r}")
        if item is not None:
            where.append(f"item {item}")
        super().__init__(message + (f" [{', '.join(where)}]" if where else ""))


# ---------------------------------------------------------------------------
# scalar coercion helpers
# ---------------------------------------------------------------------------

def _int(value, name, lo=None):
    if isinstance(value, bool) or not isinstance(value, int):
        raise SegmentError(f"{name} must be an integer", field=name)
    if lo is not None and value < lo:
        raise SegmentError(f"{name} must be >= {lo}", field=name)
    return value


def _str(value, name, *, maxlen=MAX_ID_LEN):
    if not isinstance(value, str):
        raise SegmentError(f"{name} must be a string", field=name)
    value = value.strip()
    if not value or len(value) > maxlen:
        raise SegmentError(f"{name} must be 1..{maxlen} characters", field=name)
    return value


def _attrs(value, name):
    if value is None:
        return None
    if not isinstance(value, dict):
        raise SegmentError(f"{name} must be an object", field=name)
    try:
        return json.dumps(value, ensure_ascii=False, sort_keys=True)
    except (TypeError, ValueError) as exc:
        raise SegmentError(f"{name} is not JSON-serialisable: {exc}",
                           field=name)


class Header:
    """Validated segment header."""

    __slots__ = ("segment", "batch", "seq", "total", "part", "parts",
                 "upgrade", "producer")

    def __init__(self, segment, batch, seq, total, part, parts, upgrade,
                 producer=None):
        self.segment = segment
        self.batch = batch
        self.seq = seq
        self.total = total
        self.part = part
        self.parts = parts
        self.upgrade = upgrade
        self.producer = producer

    def __repr__(self) -> str:
        return (f"Header(segment={self.segment!r}, batch={self.batch!r}, "
                f"part={self.part}/{self.parts}, seq={self.seq}/{self.total}, "
                f"upgrade={self.upgrade})")


_HEADER_KEYS = ("segment", "batch", "seq", "total", "part", "parts", "upgrade")


def validate_header(raw: dict) -> Header:
    missing = [k for k in _HEADER_KEYS if k not in raw]
    unknown = [k for k in raw if k not in SEGMENT_FIELDS]
    if missing or unknown:
        parts = []
        if missing:
            parts.append("missing " + ", ".join(sorted(missing)))
        if unknown:
            parts.append("unknown " + ", ".join(sorted(unknown)))
        raise SegmentError("; ".join(parts))
    upgrade = raw["upgrade"]
    if not isinstance(upgrade, bool):
        raise SegmentError("upgrade must be a boolean", field="upgrade")
    header = Header(
        _str(raw["segment"], "segment"),
        _str(raw["batch"], "batch"),
        _int(raw["seq"], "seq", lo=1),
        _int(raw["total"], "total", lo=1),
        _int(raw["part"], "part", lo=0),
        _int(raw["parts"], "parts", lo=1),
        upgrade,
        _str(raw["producer"], "producer") if "producer" in raw else None,
    )
    if header.part >= header.parts:
        raise SegmentError(f"part {header.part} is outside 0..{header.parts - 1}",
                           field="part")
    if header.seq > header.total:
        raise SegmentError(f"seq {header.seq} exceeds total {header.total}",
                           field="seq")
    return header


# ---------------------------------------------------------------------------
# per-array item validators.  Each returns a tuple bound to the staging table.
# ---------------------------------------------------------------------------

def _item_obj(item, name):
    if not isinstance(item, dict):
        raise SegmentError(f"{name} entry must be an object", field=name)
    return item


def _allowed_keys(item, allowed, name):
    extra = [k for k in item if k not in allowed]
    if extra:
        raise SegmentError(f"unknown key(s) {', '.join(sorted(extra))} "
                           f"in {name} entry", field=name)


def valid_ids(item) -> tuple[str, str]:
    item = _item_obj(item, "ids")
    _allowed_keys(item, {"old", "new"}, "ids")
    if "old" not in item or "new" not in item:
        raise SegmentError("ids entry needs 'old' and 'new'", field="ids")
    old = parse_ref(item["old"])
    new = parse_ref(item["new"])
    if not old.is_legacy:
        raise SegmentError(f"'old' must be a legacy id, got {old.text!r}",
                           field="ids")
    if new.is_legacy:
        raise SegmentError(f"'new' must be a zoned id, got {new.text!r}",
                           field="ids")
    return old.text, new.text


def valid_taps(item) -> tuple[str, str | None, str | None, str | None]:
    item = _item_obj(item, "taps")
    _allowed_keys(item, {"ref", "zone", "local", "attrs"}, "taps")
    if "ref" not in item:
        raise SegmentError("taps entry needs 'ref'", field="taps")
    ref = parse_ref(item["ref"])
    zone = item.get("zone")
    local = item.get("local")
    if (zone is None) != (local is None):
        raise SegmentError("taps entry needs both or neither of zone/local",
                           field="taps")
    if zone is not None:
        zref = parse_ref(join_ref(_str(zone, "zone", maxlen=6).upper(),
                                  _str(local, "local", maxlen=32)))
        if not ref.is_legacy and zref.text != ref.text:
            raise SegmentError(
                f"ref {ref.text!r} disagrees with zone/local {zref.text!r}",
                field="taps")
        zone_v, local_v = zref.zone, zref.local
    else:
        zone_v, local_v = (None, None) if ref.is_legacy else (ref.zone, ref.local)
    return ref.text, zone_v, local_v, _attrs(item.get("attrs"), "attrs")


def valid_lays(item) -> tuple[str, str, str, str | None]:
    item = _item_obj(item, "lays")
    _allowed_keys(item, {"code", "from", "to", "attrs"}, "lays")
    for k in ("code", "from", "to"):
        if k not in item:
            raise SegmentError(f"lays entry needs {k!r}", field="lays")
    return (parse_pipe_code(item["code"]),
            parse_ref(item["from"]).text,
            parse_ref(item["to"]).text,
            _attrs(item.get("attrs"), "attrs"))


def valid_redirects(item) -> tuple[str, str | None, str, str]:
    """Normalise a redirect to (kind, code, from_ref, to_ref).

    kind = 'joint' -> the pipes leaving ``from_ref`` leave ``to_ref`` instead
    kind = 'pipe'  -> ``code`` now runs ``from_ref`` -> ``to_ref``
    """
    item = _item_obj(item, "redirects")
    _allowed_keys(item, {"from", "to", "code"}, "redirects")
    if "to" not in item:
        raise SegmentError("redirects entry needs 'to'", field="redirects")
    src = item["from"] if "from" in item else item.get("code")
    if src is None:
        raise SegmentError("redirects entry needs 'from' (joint or pipe code)",
                           field="redirects")
    if isinstance(src, dict):
        _allowed_keys(src, {"ref", "code"}, "redirects.from")
        if "code" in src:
            f, t = _redirect_pipe_to(item)
            return "pipe", parse_pipe_code(src["code"]), f, t
        if "ref" in src:
            return ("joint", None, parse_ref(src["ref"]).text,
                    _redirect_joint_to(item))
        raise SegmentError("redirects 'from' object needs ref or code",
                           field="redirects")
    if not isinstance(src, str):
        raise SegmentError("redirects 'from' must be a reference, pipe code "
                           "or object", field="redirects")
    from .ids import PIPE_RE
    if PIPE_RE.match(src.strip()):
        # pipe codes are recognised first: the P- prefix is reserved for them
        f, t = _redirect_pipe_to(item)
        return "pipe", parse_pipe_code(src), f, t
    return "joint", None, parse_ref(src).text, _redirect_joint_to(item)


def _redirect_joint_to(item) -> str:
    return parse_ref(item["to"]).text


def _redirect_pipe_to(item) -> tuple[str, str]:
    to = item["to"]
    if not isinstance(to, dict):
        raise SegmentError("redirects of a pipe needs 'to' as "
                           "{'from': ..., 'to': ...}", field="redirects")
    _allowed_keys(to, {"from", "to"}, "redirects.to")
    if "from" not in to or "to" not in to:
        raise SegmentError("redirects 'to' object needs 'from' and 'to'",
                           field="redirects")
    return parse_ref(to["from"]).text, parse_ref(to["to"]).text


def valid_retires(item) -> tuple[str, str]:
    item = _item_obj(item, "retires")
    _allowed_keys(item, {"kind", "ref", "code"}, "retires")
    kind = item.get("kind", "pipe" if "code" in item else "joint")
    if not isinstance(kind, str) or kind not in ("joint", "pipe", "code"):
        raise SegmentError("retires 'kind' must be joint, pipe or code",
                           field="retires")
    if kind == "joint":
        if "ref" not in item:
            raise SegmentError("retires joint needs 'ref'", field="retires")
        return "joint", parse_ref(item["ref"]).text
    if "code" in item:
        return "pipe", parse_pipe_code(item["code"])
    if "ref" in item:
        return "pipe", parse_pipe_code(item["ref"])
    raise SegmentError("retires pipe needs 'code'", field="retires")


VALIDATORS = {
    "ids": valid_ids,
    "taps": valid_taps,
    "lays": valid_lays,
    "redirects": valid_redirects,
    "retires": valid_retires,
}


def validate_item(section: str, item, item_index: int):
    try:
        return VALIDATORS[section](item)
    except RefError as exc:
        raise SegmentError(str(exc), item=item_index, field=section) from exc
    except SegmentError as exc:
        exc.item = item_index
        raise


# ---------------------------------------------------------------------------
# digest
# ---------------------------------------------------------------------------

def canonical(value) -> bytes:
    return json.dumps(value, ensure_ascii=False, sort_keys=True,
                      separators=(",", ":")).encode("utf-8")


class SegmentDigest:
    """Order-independent across sections, order-sensitive within a section."""

    def __init__(self):
        self.h = hashlib.sha256()
        self.h.update(b"segment-v1\0")

    def header(self, header: Header):
        raw = {k: getattr(header, k) for k in _HEADER_KEYS}
        self.h.update(canonical(raw))

    def item(self, section: str, raw_item):
        self.h.update(b"\n" + section.encode() + b":")
        self.h.update(canonical(raw_item))

    def hexdigest(self) -> str:
        return self.h.hexdigest()
