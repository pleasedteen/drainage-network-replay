"""Applying a complete batch to the published network.

Batches are applied strictly in ``seq`` order, one at a time, each inside a
single write transaction.  Readers therefore only ever see the network as of
the end of some batch -- a half-applied batch is never visible.  A batch whose
data conflicts with the published network is rolled back *and retired*: it
keeps its ``rejected`` state plus an error, blocks later batches, and the last
good snapshot stays queryable.

Where the items come from
-------------------------
A confirmed segment keeps only its landed archive bytes, so applying a batch
replays those bytes straight from disk: for each section, each part's arrays
are read once, in ``(part, item)`` order, one item at a time -- no per-item
intermediate copy is built and the archive bytes read stay within the size of
the batch's own archives.  Segments confirmed by an older receiver may still
have their items in the legacy ``st_*`` staging tables; those parts are read
from staging instead, in the same order, so an old database's segments and
newly landed ones apply side by side in the batch they belong to.  A batch's
staging rows are dropped once the batch is applied.

Identity model
--------------
A joint has a stable internal ``uid`` for its whole life, a current
``(zone, local)`` identity, and a set of textual ``joint_refs`` (its legacy
``J-xxxxxx`` form and/or its zoned ``ZONE:xxxxx`` form).  Pipes point at
``uid``, so re-numbering a joint never rewrites a pipe's endpoints -- after
the upgrade batch, both old and new spellings address the same joint.
"""

from __future__ import annotations

import json
import time
from pathlib import Path

from . import jsonstream, wire
from .ids import parse_ref


class ApplyError(ValueError):
    """Data inside an otherwise complete batch contradicts the snapshot."""


class ReplayError(RuntimeError):
    """A confirmed segment's archive cannot be replayed (missing, truncated
    or changed on disk).  Not an ``ApplyError``: the batch stays staged so
    publishing can be retried once the archive is restored."""


def _now() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def meta_get(conn, key, default=None):
    row = conn.execute("SELECT value FROM meta WHERE key=?", (key,)).fetchone()
    return row["value"] if row else default


def meta_set(conn, key, value):
    conn.execute("INSERT INTO meta(key,value) VALUES(?,?) "
                 "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                 (key, str(value)))


# ---------------------------------------------------------------------------
# readiness
# ---------------------------------------------------------------------------

def batch_ready(conn, row) -> bool:
    if row["state"] != "staging":
        return False
    parts = [r[0] for r in conn.execute(
        "SELECT DISTINCT part FROM segments WHERE batch_id=?", (row["batch_id"],))]
    return (len(parts) == row["parts_expected"]
            and sorted(parts) == list(range(row["parts_expected"])))


def next_batch_to_apply(conn):
    applied = int(meta_get(conn, "applied_seq", 0))
    rejected = meta_get(conn, "rejected_seq")
    row = conn.execute(
        "SELECT * FROM batches WHERE seq=? AND state IN ('staging','rejected')",
        (applied + 1,)).fetchone()
    if row is None:
        return None
    if row["state"] == "rejected" or rejected == row["seq"]:
        return row                      # caller reports the block, does not apply
    if not batch_ready(conn, row):
        return None
    return row


# ---------------------------------------------------------------------------
# reference resolution
# ---------------------------------------------------------------------------

def ref_uid(conn, text: str):
    row = conn.execute("SELECT uid FROM joint_refs WHERE ref=?", (text,)).fetchone()
    return row["uid"] if row else None


def _split(ref: str):
    r = parse_ref(ref)
    return r.zone, r.local, r.kind


def _add_ref(conn, ref: str, uid: int, kind: str, seq: int):
    row = conn.execute("SELECT uid FROM joint_refs WHERE ref=?", (ref,)).fetchone()
    if row is not None:
        if row["uid"] != uid:
            raise ApplyError(f"reference {ref!r} already belongs to joint "
                             f"uid {row['uid']}, cannot also mean uid {uid}")
        return
    conn.execute("INSERT INTO joint_refs(ref,uid,kind,since_seq) VALUES(?,?,?,?)",
                 (ref, uid, kind, seq))


def _create_joint(conn, zone: str, local: str, seq: int, attrs) -> int:
    cur = conn.execute(
        "INSERT INTO joints(zone, local, created_seq, last_seq, attrs) "
        "VALUES(?,?,?,?,?)", (zone, local, seq, seq, attrs))
    return cur.lastrowid


def _joint_by_identity(conn, zone: str, local: str):
    row = conn.execute("SELECT uid FROM joints WHERE zone=? AND local=?",
                       (zone, local)).fetchone()
    return row["uid"] if row else None


def _rename_joint(conn, uid: int, zone: str, local: str, seq: int):
    other = _joint_by_identity(conn, zone, local)
    if other is not None and other != uid:
        raise ApplyError(
            f"cannot renumber uid {uid} to {zone}:{local}: that identity is "
            f"already held by uid {other}")
    conn.execute("UPDATE joints SET zone=?, local=?, last_seq=? WHERE uid=?",
                 (zone, local, seq, uid))


def _ensure_joint(conn, ref: str, zone, local, seq: int) -> int:
    """Resolve a reference, creating/upgrading the joint if needed."""
    uid = ref_uid(conn, ref)
    if uid is not None:
        if zone is not None:
            row = conn.execute("SELECT zone,local FROM joints WHERE uid=?",
                               (uid,)).fetchone()
            if row["zone"] and (row["zone"], row["local"]) != (zone, local):
                # explicitly re-zoning an existing joint: move it and drop the
                # stale qualified spelling so the old zone no longer answers
                _rename_joint(conn, uid, zone, local, seq)
                conn.execute("DELETE FROM joint_refs WHERE uid=? AND "
                             "kind='qualified' AND ref<>?",
                             (uid, f"{zone}:{local}"))
            elif not row["zone"]:
                _rename_joint(conn, uid, zone, local, seq)
            _add_ref(conn, f"{zone}:{local}", uid, "qualified", seq)
        conn.execute("UPDATE joints SET last_seq=?, retired_seq=NULL "
                     "WHERE uid=?", (seq, uid))
        return uid

    if zone is None:
        # legacy reference with no known mapping: keep the global id as-is
        uid = _create_joint(conn, "", ref, seq, None)
        _add_ref(conn, ref, uid, "legacy", seq)
        return uid

    uid = _joint_by_identity(conn, zone, local)
    if uid is None:
        uid = _create_joint(conn, zone, local, seq, None)
    _add_ref(conn, f"{zone}:{local}", uid, "qualified", seq)
    if not ref.startswith(f"{zone}:"):
        _add_ref(conn, ref, uid, "legacy", seq)
    conn.execute("UPDATE joints SET last_seq=?, retired_seq=NULL WHERE uid=?",
                 (seq, uid))
    return uid


def resolve_existing(conn, ref: str, what: str, seq: int) -> int:
    uid = ref_uid(conn, ref)
    if uid is None:
        raise ApplyError(f"{what} refers to unknown joint {ref!r}")
    return uid


# ---------------------------------------------------------------------------
# batch item source
# ---------------------------------------------------------------------------

def _archive_path(data_dir, stored: str) -> Path:
    p = Path(stored)
    return p if p.is_absolute() else Path(data_dir) / p


# project a legacy staging row to the same tuple wire.validate_item returns
_STAGED_PROJECT = {
    "ids": lambda r: (r["old_ref"], r["new_ref"]),
    "taps": lambda r: (r["ref"], r["zone"], r["local"], r["attrs"]),
    "lays": lambda r: (r["code"], r["from_ref"], r["to_ref"], r["attrs"]),
    "redirects": lambda r: (r["kind"], r["code"], r["from_ref"], r["to_ref"]),
    "retires": lambda r: (r["kind"], r["ref"]),
}


class _Items:
    """Stream one batch's items, section by section, in ``(part, item)``
    order -- exactly the order the source sent them in.

    Parts confirmed by the current receiver keep only their archive bytes and
    are replayed from disk; parts an older receiver confirmed still have
    staging rows and are read from there.  Both kinds mix freely within a
    batch.
    """

    def __init__(self, conn, data_dir, batch_id):
        self.conn = conn
        self.data_dir = data_dir
        self.batch_id = batch_id
        self.parts = conn.execute(
            "SELECT seg_id, part, path, ingested FROM segments "
            "WHERE batch_id=? ORDER BY part", (batch_id,)).fetchall()

    def section(self, name: str):
        for seg in self.parts:
            origin = seg["ingested"]
            if origin == "validated":
                yield from self._staged(seg, name)
            elif origin == "archived":
                yield from self._replayed(seg, name)
            else:
                raise ReplayError(f"segment {seg['seg_id']!r} carries unknown "
                                  f"ingested marker {origin!r}")

    def _staged(self, seg, name: str):
        for row in self.conn.execute(
                f"SELECT * FROM st_{name} WHERE batch_id=? AND part=? "
                f"ORDER BY item", (self.batch_id, seg["part"])):
            yield _STAGED_PROJECT[name](row)

    def _replayed(self, seg, name: str):
        spans = self.conn.execute(
            "SELECT byte_offset, items FROM segment_sections "
            "WHERE seg_id=? AND section=? ORDER BY byte_offset",
            (seg["seg_id"], name)).fetchall()
        if not spans:
            return
        path = _archive_path(self.data_dir, seg["path"])
        idx = 0
        try:
            with open(path, "rb") as fp:
                for span in spans:
                    got = 0
                    for item in jsonstream.iter_array_at(fp,
                                                         span["byte_offset"]):
                        yield wire.validate_item(name, item, idx)
                        idx += 1
                        got += 1
                    if got != span["items"]:
                        raise ReplayError(
                            f"archive {path} section {name!r}: expected "
                            f"{span['items']} items, found {got}")
        except FileNotFoundError as exc:
            raise ReplayError(
                f"archive for segment {seg['seg_id']!r} is missing: "
                f"{path}") from exc
        except jsonstream.JsonStreamError as exc:
            raise ReplayError(
                f"archive for segment {seg['seg_id']!r} cannot be replayed: "
                f"{exc}") from exc


def _effect(effects, kind, n=1):
    effects[kind] = effects.get(kind, 0) + n


def _apply_ids(conn, items, seq, effects):
    for old, new in items.section("ids"):
        zone, local, _ = _split(new)
        uid_old = ref_uid(conn, old)
        uid_new = ref_uid(conn, new)
        if uid_old is not None and uid_new is not None and uid_old != uid_new:
            raise ApplyError(
                f"id table says {old!r} == {new!r}, but they are two different "
                f"joints (uid {uid_old} and uid {uid_new})")
        if uid_old is None and uid_new is None:
            uid = _create_joint(conn, zone, local, seq, None)
            _add_ref(conn, new, uid, "qualified", seq)
            _add_ref(conn, old, uid, "legacy", seq)
            _effect(effects, "ids")
            continue
        uid = uid_old if uid_old is not None else uid_new
        jr = conn.execute("SELECT zone,local FROM joints WHERE uid=?",
                          (uid,)).fetchone()
        if jr["zone"] == "":
            _rename_joint(conn, uid, zone, local, seq)
        elif (jr["zone"], jr["local"]) != (zone, local):
            raise ApplyError(
                f"id table maps {old!r} to {new!r}, but that joint is already "
                f"{jr['zone']}:{jr['local']}")
        _add_ref(conn, old, uid, "legacy", seq)
        _add_ref(conn, new, uid, "qualified", seq)
        conn.execute("UPDATE joints SET last_seq=? WHERE uid=?", (seq, uid))
        _effect(effects, "ids")


def _apply_taps(conn, items, seq, effects):
    for ref, zone, local, attrs in items.section("taps"):
        existed = ref_uid(conn, ref) is not None
        uid = _ensure_joint(conn, ref, zone, local, seq)
        if attrs is not None:
            conn.execute(
                "UPDATE joints SET attrs = CASE "
                "WHEN attrs IS NULL THEN json(?) ELSE json_patch(attrs, json(?)) "
                "END WHERE uid=?", (attrs, attrs, uid))
        _effect(effects, "tap_updated" if existed else "tap_created")


def _apply_lays(conn, items, seq, effects):
    for code, from_ref, to_ref, attrs in items.section("lays"):
        fu = resolve_existing(conn, from_ref, f"pipe {code}", seq)
        tu = resolve_existing(conn, to_ref, f"pipe {code}", seq)
        if fu == tu:
            raise ApplyError(f"pipe {code} would connect joint uid {fu} to "
                             f"itself")
        old = conn.execute("SELECT from_uid,to_uid,retired_seq,attrs FROM pipes "
                           "WHERE code=?", (code,)).fetchone()
        if old is None:
            conn.execute(
                "INSERT INTO pipes(code,from_uid,to_uid,created_seq,last_seq,"
                "attrs) VALUES(?,?,?,?,?,?)",
                (code, fu, tu, seq, seq, attrs))
            _effect(effects, "lay_created")
        else:
            moved = (old["from_uid"], old["to_uid"]) != (fu, tu)
            merged = old["attrs"]
            if attrs is not None:
                merged = attrs if merged is None else json.dumps(
                    {**json.loads(merged), **json.loads(attrs)},
                    ensure_ascii=False, sort_keys=True)
            conn.execute(
                "UPDATE pipes SET from_uid=?, to_uid=?, last_seq=?, "
                "retired_seq=NULL, retired_by=NULL, attrs=? WHERE code=?",
                (fu, tu, seq, merged, code))
            _effect(effects, "lay_moved" if moved else "lay_updated")


def _apply_redirects(conn, items, seq, effects):
    for kind, code, from_ref, to_ref in items.section("redirects"):
        if kind == "joint":
            src = resolve_existing(conn, from_ref, "redirect", seq)
            dst = resolve_existing(conn, to_ref, "redirect target", seq)
            if src == dst:
                continue
            loop = conn.execute(
                "SELECT code FROM pipes WHERE from_uid=? AND to_uid=? "
                "AND retired_seq IS NULL LIMIT 1", (src, dst)).fetchone()
            if loop is not None:
                raise ApplyError(
                    f"redirecting pipes from joint {src} to joint {dst} would "
                    f"make pipe {loop['code']} start and end at the same joint")
            cur = conn.execute(
                "UPDATE pipes SET from_uid=?, last_seq=? "
                "WHERE from_uid=? AND retired_seq IS NULL", (dst, seq, src))
            _effect(effects, "redirect_joint", cur.rowcount)
        else:
            fu = resolve_existing(conn, from_ref, f"pipe {code}", seq)
            tu = resolve_existing(conn, to_ref, f"pipe {code}", seq)
            if fu == tu:
                raise ApplyError(f"pipe {code} would connect a joint to itself")
            cur = conn.execute(
                "UPDATE pipes SET from_uid=?, to_uid=?, last_seq=?, "
                "retired_seq=NULL, retired_by=NULL WHERE code=?",
                (fu, tu, seq, code))
            if cur.rowcount == 0:
                raise ApplyError(f"redirect names unknown pipe {code!r}")
            _effect(effects, "redirect_pipe")


def _apply_retires(conn, items, seq, effects):
    for kind, ref in items.section("retires"):
        if kind == "joint":
            uid = resolve_existing(conn, ref, "retire", seq)
            cur = conn.execute(
                "UPDATE joints SET retired_seq=?, last_seq=? "
                "WHERE uid=? AND retired_seq IS NULL", (seq, seq, uid))
            if cur.rowcount == 0:
                _effect(effects, "joint_already_retired")
                continue
            pipes = conn.execute(
                "UPDATE pipes SET retired_seq=?, retired_by='joint-retired' "
                "WHERE (from_uid=? OR to_uid=?) AND retired_seq IS NULL",
                (seq, uid, uid))
            _effect(effects, "joint_retired")
            _effect(effects, "pipe_retired_by_joint", pipes.rowcount)
        else:
            cur = conn.execute(
                "UPDATE pipes SET retired_seq=?, retired_by='explicit' "
                "WHERE code=? AND retired_seq IS NULL", (seq, ref))
            if cur.rowcount == 0:
                raise ApplyError(f"retire names unknown or already retired "
                                 f"pipe {ref!r}")
            _effect(effects, "pipe_retired")


APPLIERS = (
    ("ids", _apply_ids),
    ("taps", _apply_taps),
    ("lays", _apply_lays),
    ("redirects", _apply_redirects),
    ("retires", _apply_retires),
)


def apply_batch(conn, row, data_dir="") -> dict:
    """Apply one complete batch.  Caller owns the transaction."""
    seq, batch_id = row["seq"], row["batch_id"]
    effects: dict = {}

    applied_seq = int(meta_get(conn, "applied_seq", 0))
    if seq != applied_seq + 1:
        raise ApplyError(f"batch {batch_id} has seq {seq}, expected "
                         f"{applied_seq + 1}: batches must apply in order")

    if row["upgrade"]:
        first = meta_get(conn, "upgrade_seq")
        if first is not None and int(first) != seq:
            raise ApplyError(f"batch {batch_id} is marked upgrade, but the "
                             f"upgrade already happened at seq {first}")
        meta_set(conn, "upgrade_seq", seq)

    items = _Items(conn, data_dir, batch_id)
    for _, fn in APPLIERS:
        fn(conn, items, seq, effects)

    # an applied batch never needs its per-item rows again: drop whatever a
    # legacy receiver staged for it, so publishing leaves no item copy behind
    for name in wire.SEGMENT_ARRAYS:
        conn.execute(f"DELETE FROM st_{name} WHERE batch_id=?", (batch_id,))

    conn.execute("DELETE FROM batch_effects WHERE batch_id=?", (batch_id,))
    for kind, n in sorted(effects.items()):
        if n:
            conn.execute("INSERT INTO batch_effects(batch_id,seq,kind,n) "
                         "VALUES(?,?,?,?)", (batch_id, seq, kind, n))
    conn.execute("UPDATE batches SET state='applied', applied_at=?, error=NULL "
                 "WHERE batch_id=?", (_now(), batch_id))
    meta_set(conn, "applied_seq", seq)
    return effects


def publish_ready(conn, *, max_batches=None, data_dir="") -> dict:
    """Apply every batch that is complete and next in line.

    Returns a report: which batches were applied, and why publishing stopped.
    Safe against several publishers: the applicability check is repeated
    under the write lock, so a batch another process already applied is simply
    skipped rather than being mistaken for a conflict.
    """
    applied, stopped = [], None
    n = 0
    while max_batches is None or n < max_batches:
        row = next_batch_to_apply(conn)
        if row is None:
            break
        if row["state"] == "rejected":
            stopped = {"reason": "rejected", "batch": row["batch_id"],
                       "seq": row["seq"], "error": row["error"]}
            break
        conn.execute("BEGIN IMMEDIATE")
        fresh = conn.execute("SELECT * FROM batches WHERE batch_id=?",
                             (row["batch_id"],)).fetchone()
        if (fresh is None or fresh["state"] != "staging"
                or not batch_ready(conn, fresh)):
            conn.execute("ROLLBACK")
            continue
        try:
            effects = apply_batch(conn, fresh, data_dir)
            conn.execute("COMMIT")
        except ApplyError as exc:
            conn.execute("ROLLBACK")
            conn.execute("BEGIN IMMEDIATE")
            conn.execute("UPDATE batches SET state='rejected', error=? "
                         "WHERE batch_id=?", (str(exc), row["batch_id"]))
            meta_set(conn, "rejected_seq", row["seq"])
            conn.execute("COMMIT")
            stopped = {"reason": "rejected", "batch": row["batch_id"],
                       "seq": row["seq"], "error": str(exc)}
            break
        except Exception:
            # storage-level failure (including an archive that cannot be
            # replayed): leave the batch staged so it can be retried after a
            # restart instead of being rejected for good
            if conn.in_transaction:
                conn.execute("ROLLBACK")
            raise
        applied.append({"batch": row["batch_id"], "seq": row["seq"],
                        "effects": effects})
        n += 1
    else:
        stopped = {"reason": "max_batches"}
    if stopped is None:
        stopped = _blocker(conn)
    return {"applied": applied, "stopped": stopped}


def _blocker(conn):
    """Describe why the next batch cannot be published yet."""
    applied_seq = int(meta_get(conn, "applied_seq", 0))
    nxt = conn.execute("SELECT * FROM batches WHERE seq=?",
                       (applied_seq + 1,)).fetchone()
    if nxt is None:
        waiting = conn.execute(
            "SELECT batch_id, seq, parts_expected, parts_received FROM batches "
            "WHERE seq>? ORDER BY seq LIMIT 1", (applied_seq + 1,)).fetchone()
        if waiting is None:
            return {"reason": "up_to_date"}
        missing = sorted(set(range(waiting["parts_expected"]))
                         - set(r[0] for r in conn.execute(
                             "SELECT DISTINCT part FROM segments WHERE "
                             "batch_id=?", (waiting["batch_id"],))))
        if missing and applied_seq + 1 == waiting["seq"]:
            return {"reason": "waiting_for_parts", "batch": waiting["batch_id"],
                    "seq": waiting["seq"], "missing_parts": missing}
        return {"reason": "waiting_for_earlier_batch",
                "batch": waiting["batch_id"], "seq": waiting["seq"]}
    missing = sorted(set(range(nxt["parts_expected"]))
                     - set(r[0] for r in conn.execute(
                         "SELECT DISTINCT part FROM segments WHERE batch_id=?",
                         (nxt["batch_id"],))))
    if missing:
        return {"reason": "waiting_for_parts", "batch": nxt["batch_id"],
                "seq": nxt["seq"], "missing_parts": missing}
    if nxt["state"] == "rejected":
        return {"reason": "rejected", "batch": nxt["batch_id"],
                "seq": nxt["seq"], "error": nxt["error"]}
    return {"reason": "unknown", "batch": nxt["batch_id"], "seq": nxt["seq"]}
