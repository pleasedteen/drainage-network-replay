"""Applying a complete batch to the published network.

The staging rows live in the receive database; the published network lives in
its own file (see ``db``).  A publisher opens the published file, attaches the
receive file read-only as ``rcv``, and reads staged items from ``rcv.st_*``
while it writes joints/pipes to the published file.  The whole apply is one
write transaction on the published file, so readers never see a half-applied
batch -- and, crucially, the receive file's write lock is *not* held for the
duration of the apply, so another delivery can still be acknowledged.

Batches are applied strictly in ``seq`` order, one at a time.  A batch whose
data conflicts with the published network is rolled back *and retired*: it
keeps its ``rejected`` state plus an error, blocks later batches, and the last
good snapshot stays queryable.

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
import sqlite3
import time

from .ids import parse_ref

# receive-database schema prefix, valid because publishers ATTACH the receive
# file under this name before calling in
R = "rcv."


class ApplyError(ValueError):
    """Data inside an otherwise complete batch contradicts the snapshot."""


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
# readiness (receive file is attached read-only as ``rcv``)
# ---------------------------------------------------------------------------

def _published_state(conn, seq: int):
    return conn.execute("SELECT state,error FROM published_batches WHERE seq=?",
                        (seq,)).fetchone()


def batch_ready(conn, row) -> bool:
    parts = [r[0] for r in conn.execute(
        f"SELECT DISTINCT part FROM {R}segments WHERE batch_id=?",
        (row["batch_id"],))]
    return (len(parts) == row["parts_expected"]
            and sorted(parts) == list(range(row["parts_expected"])))


def next_batch_to_apply(conn):
    """The next batch in source order, if it is complete and unpublished.

    Returns one of ``None`` (nothing to do), ``("apply", row)`` or
    ``("rejected", row, error)``.
    """
    applied = int(meta_get(conn, "applied_seq", 0))
    seq = applied + 1
    row = conn.execute(f"SELECT * FROM {R}batches WHERE seq=?", (seq,)).fetchone()
    if row is None:
        return None
    led = _published_state(conn, seq)
    if led is not None:
        if led["state"] == "rejected":
            return ("rejected", row, led["error"])
        return None                     # already applied; nothing to do
    if not batch_ready(conn, row):
        return None
    return ("apply", row)


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
# batch application
# ---------------------------------------------------------------------------

def _rows(conn, table, batch_id):
    """Stream staged items of one section in a stable order."""
    return conn.execute(f"SELECT * FROM {R}{table} WHERE batch_id=? "
                        f"ORDER BY part, item", (batch_id,))


def _effect(effects, kind, n=1):
    effects[kind] = effects.get(kind, 0) + n


def _apply_ids(conn, batch_id, seq, effects):
    for row in _rows(conn, "st_ids", batch_id):
        old, new = row["old_ref"], row["new_ref"]
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


def _apply_taps(conn, batch_id, seq, effects):
    for row in _rows(conn, "st_taps", batch_id):
        existed = ref_uid(conn, row["ref"]) is not None
        uid = _ensure_joint(conn, row["ref"], row["zone"], row["local"], seq)
        if row["attrs"] is not None:
            conn.execute(
                "UPDATE joints SET attrs = CASE "
                "WHEN attrs IS NULL THEN json(?) ELSE json_patch(attrs, json(?)) "
                "END WHERE uid=?", (row["attrs"], row["attrs"], uid))
        _effect(effects, "tap_updated" if existed else "tap_created")


def _apply_lays(conn, batch_id, seq, effects):
    for row in _rows(conn, "st_lays", batch_id):
        code = row["code"]
        fu = resolve_existing(conn, row["from_ref"], f"pipe {code}", seq)
        tu = resolve_existing(conn, row["to_ref"], f"pipe {code}", seq)
        if fu == tu:
            raise ApplyError(f"pipe {code} would connect joint uid {fu} to "
                             f"itself")
        old = conn.execute("SELECT from_uid,to_uid,retired_seq,attrs FROM pipes "
                           "WHERE code=?", (code,)).fetchone()
        if old is None:
            conn.execute(
                "INSERT INTO pipes(code,from_uid,to_uid,created_seq,last_seq,"
                "attrs) VALUES(?,?,?,?,?,?)",
                (code, fu, tu, seq, seq, row["attrs"]))
            _effect(effects, "lay_created")
        else:
            moved = (old["from_uid"], old["to_uid"]) != (fu, tu)
            attrs = old["attrs"]
            if row["attrs"] is not None:
                attrs = row["attrs"] if attrs is None else json.dumps(
                    {**json.loads(attrs), **json.loads(row["attrs"])},
                    ensure_ascii=False, sort_keys=True)
            conn.execute(
                "UPDATE pipes SET from_uid=?, to_uid=?, last_seq=?, "
                "retired_seq=NULL, retired_by=NULL, attrs=? WHERE code=?",
                (fu, tu, seq, attrs, code))
            _effect(effects, "lay_moved" if moved else "lay_updated")


def _apply_redirects(conn, batch_id, seq, effects):
    for row in _rows(conn, "st_redirects", batch_id):
        if row["kind"] == "joint":
            src = resolve_existing(conn, row["from_ref"], "redirect", seq)
            dst = resolve_existing(conn, row["to_ref"], "redirect target", seq)
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
            code = row["code"]
            fu = resolve_existing(conn, row["from_ref"], f"pipe {code}", seq)
            tu = resolve_existing(conn, row["to_ref"], f"pipe {code}", seq)
            if fu == tu:
                raise ApplyError(f"pipe {code} would connect a joint to itself")
            cur = conn.execute(
                "UPDATE pipes SET from_uid=?, to_uid=?, last_seq=?, "
                "retired_seq=NULL, retired_by=NULL WHERE code=?",
                (fu, tu, seq, code))
            if cur.rowcount == 0:
                raise ApplyError(f"redirect names unknown pipe {code!r}")
            _effect(effects, "redirect_pipe")


def _apply_retires(conn, batch_id, seq, effects):
    for row in _rows(conn, "st_retires", batch_id):
        if row["kind"] == "joint":
            uid = resolve_existing(conn, row["ref"], "retire", seq)
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
                "WHERE code=? AND retired_seq IS NULL", (seq, row["ref"]))
            if cur.rowcount == 0:
                raise ApplyError(f"retire names unknown or already retired "
                                 f"pipe {row['ref']!r}")
            _effect(effects, "pipe_retired")


APPLIERS = (
    ("ids", _apply_ids),
    ("taps", _apply_taps),
    ("lays", _apply_lays),
    ("redirects", _apply_redirects),
    ("retires", _apply_retires),
)


def apply_batch(conn, row) -> dict:
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

    for _, fn in APPLIERS:
        fn(conn, batch_id, seq, effects)

    conn.execute("DELETE FROM batch_effects WHERE batch_id=?", (batch_id,))
    for kind, n in sorted(effects.items()):
        if n:
            conn.execute("INSERT INTO batch_effects(batch_id,seq,kind,n) "
                         "VALUES(?,?,?,?)", (batch_id, seq, kind, n))
    conn.execute("INSERT INTO published_batches(seq, batch_id, state, error, "
                 "applied_at) VALUES(?,?,'applied',NULL,?) "
                 "ON CONFLICT(seq) DO UPDATE SET state='applied', error=NULL, "
                 "applied_at=excluded.applied_at",
                 (seq, batch_id, _now()))
    meta_set(conn, "applied_seq", seq)
    return effects


def publish_ready(conn, *, max_batches=None) -> dict:
    """Apply every batch that is complete and next in line.

    ``conn`` is the *published* connection, with the receive file attached
    (read-only) as ``rcv``.  Each apply is one write transaction on the
    published file; the receive file's write lock is never taken, so a
    concurrent delivery can still be acknowledged.

    Returns a report: which batches were applied, and why publishing stopped.
    Safe against several publishers: the applicability check is repeated
    under the published write lock, so a batch another process already applied
    is simply skipped rather than being mistaken for a conflict.
    """
    applied, stopped = [], None
    n = 0
    while max_batches is None or n < max_batches:
        nxt = next_batch_to_apply(conn)
        if nxt is None:
            break
        if nxt[0] == "rejected":
            stopped = {"reason": "rejected", "batch": nxt[1]["batch_id"],
                       "seq": nxt[1]["seq"], "error": nxt[2]}
            break
        row = nxt[1]
        conn.execute("BEGIN IMMEDIATE")
        fresh = conn.execute(f"SELECT * FROM {R}batches WHERE batch_id=?",
                             (row["batch_id"],)).fetchone()
        if (fresh is None or _published_state(conn, row["seq"]) is not None
                or not batch_ready(conn, fresh)):
            conn.execute("ROLLBACK")
            continue
        try:
            effects = apply_batch(conn, fresh)
            conn.execute("COMMIT")
        except ApplyError as exc:
            conn.execute("ROLLBACK")
            conn.execute("BEGIN IMMEDIATE")
            conn.execute("INSERT INTO published_batches(seq, batch_id, state, "
                         "error, applied_at) VALUES(?,?,'rejected',?,NULL) "
                         "ON CONFLICT(seq) DO UPDATE SET state='rejected', "
                         "error=excluded.error",
                         (row["seq"], row["batch_id"], str(exc)))
            meta_set(conn, "rejected_seq", row["seq"])
            conn.execute("COMMIT")
            stopped = {"reason": "rejected", "batch": row["batch_id"],
                       "seq": row["seq"], "error": str(exc)}
            break
        except sqlite3.Error:
            # storage-level failure: leave the batch unpublished so it can be
            # retried after a restart instead of being rejected for good
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
    nxt = conn.execute(f"SELECT * FROM {R}batches WHERE seq=?",
                       (applied_seq + 1,)).fetchone()
    if nxt is None:
        waiting = conn.execute(
            f"SELECT batch_id, seq, parts_expected FROM {R}batches "
            "WHERE seq>? ORDER BY seq LIMIT 1", (applied_seq + 1,)).fetchone()
        if waiting is None:
            return {"reason": "up_to_date"}
        missing = sorted(set(range(waiting["parts_expected"]))
                         - set(r[0] for r in conn.execute(
                             f"SELECT DISTINCT part FROM {R}segments WHERE "
                             "batch_id=?", (waiting["batch_id"],))))
        if missing and applied_seq + 1 == waiting["seq"]:
            return {"reason": "waiting_for_parts", "batch": waiting["batch_id"],
                    "seq": waiting["seq"], "missing_parts": missing}
        return {"reason": "waiting_for_earlier_batch",
                "batch": waiting["batch_id"], "seq": waiting["seq"]}
    led = _published_state(conn, nxt["seq"])
    if led is not None and led["state"] == "rejected":
        return {"reason": "rejected", "batch": nxt["batch_id"],
                "seq": nxt["seq"], "error": led["error"]}
    missing = sorted(set(range(nxt["parts_expected"]))
                     - set(r[0] for r in conn.execute(
                         f"SELECT DISTINCT part FROM {R}segments WHERE batch_id=?",
                         (nxt["batch_id"],))))
    if missing:
        return {"reason": "waiting_for_parts", "batch": nxt["batch_id"],
                "seq": nxt["seq"], "missing_parts": missing}
    return {"reason": "unknown", "batch": nxt["batch_id"], "seq": nxt["seq"]}
