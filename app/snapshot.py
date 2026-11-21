"""Applying a complete batch to the published network.

Two database files are involved:

* the *inbox* holds the staging rows (``st_*``) and the per-batch receive
  state; it is written while segments are acknowledged;
* the *network* holds the joints, pipes, aliases and effect bookkeeping that
  queries read.  It is written only here.

Batches are applied strictly in ``seq`` order, one at a time, each inside a
single write transaction **on the network file only**.  Readers therefore only
ever see the network as of the end of some batch -- a half-applied batch is
never visible -- and, crucially, a long apply does not stop the inbox from
acknowledging later segments, because the two files take independent write
locks.  A batch whose data conflicts with the published network is rolled
back and recorded as ``rejected`` in the inbox: it blocks later batches and
the last good snapshot stays queryable.

The network file is the authority on what has been published
(``meta.applied_seq`` / ``applied_batches``).  The inbox batch states are
derived from it and reconciled on startup, so a kill after the network commit
but before the inbox update cannot make a published batch look un-published.

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


class ApplyError(ValueError):
    """Data inside an otherwise complete batch contradicts the snapshot."""


# Optional hook invoked after each section has been applied, while the network
# write transaction is still open.  It receives ``(section, net)`` so a test
# can both observe the partially written changes (through the uncommitted
# connection) and freeze the apply there.  Not part of the sync protocol.
progress_hook = None


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
# readiness (all of this lives in the inbox)
# ---------------------------------------------------------------------------

def batch_ready(conn, row) -> bool:
    if row["state"] != "staging":
        return False
    parts = [r[0] for r in conn.execute(
        "SELECT DISTINCT part FROM segments WHERE batch_id=?", (row["batch_id"],))]
    return (len(parts) == row["parts_expected"]
            and sorted(parts) == list(range(row["parts_expected"])))


def _missing_parts(conn, batch_id, parts_expected):
    parts = {r[0] for r in conn.execute(
        "SELECT DISTINCT part FROM segments WHERE batch_id=?", (batch_id,))}
    return sorted(set(range(parts_expected)) - parts)


def _mark_applied(conn, row) -> None:
    conn.execute("UPDATE batches SET state='applied', applied_at=?, error=NULL "
                 "WHERE batch_id=? AND state<>'applied'",
                 (_now(), row["batch_id"]))


def _reject(inbox, row, message: str) -> None:
    inbox.execute("UPDATE batches SET state='rejected', error=? "
                  "WHERE batch_id=?", (message, row["batch_id"]))
    meta_set(inbox, "rejected_seq", row["seq"])


def network_seq(net) -> int:
    """How far the published network has advanced (it is the authority)."""
    return int(meta_get(net, "applied_seq", 0))


def reconcile(inbox, net, *, best_effort: bool = False) -> int:
    """Bring inbox batch states in line with the network's applied position.

    The network commit happens first; if the process dies before the inbox
    update, the next publisher repairs the inbox from the network.  Returns
    the network's applied seq.  Requires a writable inbox connection.

    ``best_effort`` is used on the acknowledgement path: a concurrent shipper
    may hold the inbox write lock, and this is only a state repair (reporting
    derives the effective state from the network anyway), so a busy inbox is
    skipped instead of delaying an already committed acknowledgement.
    """
    applied_seq = network_seq(net)
    if applied_seq:
        try:
            lagging = inbox.execute(
                "SELECT 1 FROM batches WHERE seq<=? AND state<>'applied' "
                "LIMIT 1", (applied_seq,)).fetchone()
            if lagging is not None:
                inbox.execute(
                    "UPDATE batches SET state='applied', error=NULL "
                    "WHERE seq<=? AND state<>'applied'", (applied_seq,))
        except sqlite3.OperationalError:
            if not best_effort:
                raise
    return applied_seq


def effective_state(row, applied_seq: int) -> str:
    """The batch state implied by the network, for read-only reporting."""
    if row["state"] != "applied" and row["seq"] <= applied_seq:
        return "applied"
    return row["state"]


# ---------------------------------------------------------------------------
# reference resolution (network)
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
#
# Every applier reads its items from the inbox (``inbox``) and writes the
# published network (``net``).
# ---------------------------------------------------------------------------

def _rows(conn, table, batch_id):
    """Stream staged items of one section in a stable order."""
    return conn.execute(f"SELECT * FROM {table} WHERE batch_id=? "
                        f"ORDER BY part, item", (batch_id,))


def _effect(effects, kind, n=1):
    effects[kind] = effects.get(kind, 0) + n


def _apply_ids(inbox, net, batch_id, seq, effects):
    for row in _rows(inbox, "st_ids", batch_id):
        old, new = row["old_ref"], row["new_ref"]
        zone, local, _ = _split(new)
        uid_old = ref_uid(net, old)
        uid_new = ref_uid(net, new)
        if uid_old is not None and uid_new is not None and uid_old != uid_new:
            raise ApplyError(
                f"id table says {old!r} == {new!r}, but they are two different "
                f"joints (uid {uid_old} and uid {uid_new})")
        if uid_old is None and uid_new is None:
            uid = _create_joint(net, zone, local, seq, None)
            _add_ref(net, new, uid, "qualified", seq)
            _add_ref(net, old, uid, "legacy", seq)
            _effect(effects, "ids")
            continue
        uid = uid_old if uid_old is not None else uid_new
        jr = net.execute("SELECT zone,local FROM joints WHERE uid=?",
                         (uid,)).fetchone()
        if jr["zone"] == "":
            _rename_joint(net, uid, zone, local, seq)
        elif (jr["zone"], jr["local"]) != (zone, local):
            raise ApplyError(
                f"id table maps {old!r} to {new!r}, but that joint is already "
                f"{jr['zone']}:{jr['local']}")
        _add_ref(net, old, uid, "legacy", seq)
        _add_ref(net, new, uid, "qualified", seq)
        net.execute("UPDATE joints SET last_seq=? WHERE uid=?", (seq, uid))
        _effect(effects, "ids")


def _apply_taps(inbox, net, batch_id, seq, effects):
    for row in _rows(inbox, "st_taps", batch_id):
        existed = ref_uid(net, row["ref"]) is not None
        uid = _ensure_joint(net, row["ref"], row["zone"], row["local"], seq)
        if row["attrs"] is not None:
            net.execute(
                "UPDATE joints SET attrs = CASE "
                "WHEN attrs IS NULL THEN json(?) ELSE json_patch(attrs, json(?)) "
                "END WHERE uid=?", (row["attrs"], row["attrs"], uid))
        _effect(effects, "tap_updated" if existed else "tap_created")


def _apply_lays(inbox, net, batch_id, seq, effects):
    for row in _rows(inbox, "st_lays", batch_id):
        code = row["code"]
        fu = resolve_existing(net, row["from_ref"], f"pipe {code}", seq)
        tu = resolve_existing(net, row["to_ref"], f"pipe {code}", seq)
        if fu == tu:
            raise ApplyError(f"pipe {code} would connect joint uid {fu} to "
                             f"itself")
        old = net.execute("SELECT from_uid,to_uid,retired_seq,attrs FROM pipes "
                          "WHERE code=?", (code,)).fetchone()
        if old is None:
            net.execute(
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
            net.execute(
                "UPDATE pipes SET from_uid=?, to_uid=?, last_seq=?, "
                "retired_seq=NULL, retired_by=NULL, attrs=? WHERE code=?",
                (fu, tu, seq, attrs, code))
            _effect(effects, "lay_moved" if moved else "lay_updated")


def _apply_redirects(inbox, net, batch_id, seq, effects):
    for row in _rows(inbox, "st_redirects", batch_id):
        if row["kind"] == "joint":
            src = resolve_existing(net, row["from_ref"], "redirect", seq)
            dst = resolve_existing(net, row["to_ref"], "redirect target", seq)
            if src == dst:
                continue
            loop = net.execute(
                "SELECT code FROM pipes WHERE from_uid=? AND to_uid=? "
                "AND retired_seq IS NULL LIMIT 1", (src, dst)).fetchone()
            if loop is not None:
                raise ApplyError(
                    f"redirecting pipes from joint {src} to joint {dst} would "
                    f"make pipe {loop['code']} start and end at the same joint")
            cur = net.execute(
                "UPDATE pipes SET from_uid=?, last_seq=? "
                "WHERE from_uid=? AND retired_seq IS NULL", (dst, seq, src))
            _effect(effects, "redirect_joint", cur.rowcount)
        else:
            code = row["code"]
            fu = resolve_existing(net, row["from_ref"], f"pipe {code}", seq)
            tu = resolve_existing(net, row["to_ref"], f"pipe {code}", seq)
            if fu == tu:
                raise ApplyError(f"pipe {code} would connect a joint to itself")
            cur = net.execute(
                "UPDATE pipes SET from_uid=?, to_uid=?, last_seq=?, "
                "retired_seq=NULL, retired_by=NULL WHERE code=?",
                (fu, tu, seq, code))
            if cur.rowcount == 0:
                raise ApplyError(f"redirect names unknown pipe {code!r}")
            _effect(effects, "redirect_pipe")


def _apply_retires(inbox, net, batch_id, seq, effects):
    for row in _rows(inbox, "st_retires", batch_id):
        if row["kind"] == "joint":
            uid = resolve_existing(net, row["ref"], "retire", seq)
            cur = net.execute(
                "UPDATE joints SET retired_seq=?, last_seq=? "
                "WHERE uid=? AND retired_seq IS NULL", (seq, seq, uid))
            if cur.rowcount == 0:
                _effect(effects, "joint_already_retired")
                continue
            pipes = net.execute(
                "UPDATE pipes SET retired_seq=?, retired_by='joint-retired' "
                "WHERE (from_uid=? OR to_uid=?) AND retired_seq IS NULL",
                (seq, uid, uid))
            _effect(effects, "joint_retired")
            _effect(effects, "pipe_retired_by_joint", pipes.rowcount)
        else:
            cur = net.execute(
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


def apply_batch(inbox, net, row) -> dict:
    """Apply one complete batch.  Caller owns the network transaction."""
    seq, batch_id = row["seq"], row["batch_id"]
    effects: dict = {}

    applied_seq = int(meta_get(net, "applied_seq", 0))
    if seq != applied_seq + 1:
        raise ApplyError(f"batch {batch_id} has seq {seq}, expected "
                         f"{applied_seq + 1}: batches must apply in order")

    if row["upgrade"]:
        first = meta_get(net, "upgrade_seq")
        if first is not None and int(first) != seq:
            raise ApplyError(f"batch {batch_id} is marked upgrade, but the "
                             f"upgrade already happened at seq {first}")
        meta_set(net, "upgrade_seq", seq)

    for name, fn in APPLIERS:
        fn(inbox, net, batch_id, seq, effects)
        if progress_hook is not None:
            progress_hook(name, net)

    net.execute("DELETE FROM batch_effects WHERE batch_id=?", (batch_id,))
    for kind, n in sorted(effects.items()):
        if n:
            net.execute("INSERT INTO batch_effects(batch_id,seq,kind,n) "
                        "VALUES(?,?,?,?)", (batch_id, seq, kind, n))
    net.execute("INSERT INTO applied_batches(batch_id,seq,applied_at) "
                "VALUES(?,?,?) ON CONFLICT(batch_id) DO UPDATE SET "
                "seq=excluded.seq, applied_at=excluded.applied_at",
                (batch_id, seq, _now()))
    meta_set(net, "applied_seq", seq)
    return effects


def publish_ready(inbox, net, *, max_batches=None,
                  lock_wait: float | None = None) -> dict:
    """Apply every batch that is complete and next in line.

    Returns a report: which batches were applied, and why publishing stopped.
    Safe against a crash between the network commit and the inbox update: the
    inbox is reconciled from the network first.  Safe against several
    publishers too (the caller serialises them): the applicability check is
    repeated under the network write lock, and a batch that is already applied
    is skipped rather than mistaken for a conflict.

    If ``lock_wait`` is given and another process holds the network write lock
    for longer than that, publishing is deferred with ``reason='busy'`` instead
    of blocking: the segments involved are already acknowledged, and the next
    publisher (this process or a restarted one) applies them.  Reconciliation
    is best-effort in that mode too, so a busy inbox never delays an
    acknowledgement.
    """
    reconcile(inbox, net, best_effort=lock_wait is not None)
    applied, stopped = [], None
    n = 0
    while max_batches is None or n < max_batches:
        applied_seq = network_seq(net)
        row = inbox.execute("SELECT * FROM batches WHERE seq=?",
                            (applied_seq + 1,)).fetchone()
        if row is None:
            break
        if row["state"] == "rejected":
            stopped = {"reason": "rejected", "batch": row["batch_id"],
                       "seq": row["seq"], "error": row["error"]}
            break
        if not batch_ready(inbox, row):
            break
        try:
            _begin(net, lock_wait)
        except sqlite3.OperationalError as exc:
            if not _is_busy(exc):
                raise
            stopped = {"reason": "busy", "batch": row["batch_id"],
                       "seq": row["seq"],
                       "detail": "another publisher holds the network lock"}
            break
        try:
            fresh = network_seq(net)
            if row["seq"] <= fresh:
                net.execute("ROLLBACK")
                _mark_applied(inbox, row)
                continue
            effects = apply_batch(inbox, net, row)
            net.execute("COMMIT")
        except ApplyError as exc:
            net.execute("ROLLBACK")
            _reject(inbox, row, str(exc))
            stopped = {"reason": "rejected", "batch": row["batch_id"],
                       "seq": row["seq"], "error": str(exc)}
            break
        except sqlite3.Error:
            # storage-level failure: leave the batch staged so it can be
            # retried after a restart instead of being rejected for good
            if net.in_transaction:
                net.execute("ROLLBACK")
            raise
        _mark_applied(inbox, row)
        applied.append({"batch": row["batch_id"], "seq": row["seq"],
                        "effects": effects})
        n += 1
    else:
        stopped = {"reason": "max_batches"}
    if stopped is None:
        stopped = _blocker(inbox, net)
    return {"applied": applied, "stopped": stopped}


def _begin(net, lock_wait: float | None):
    if lock_wait is None:
        net.execute("BEGIN IMMEDIATE")
        return
    net.execute(f"PRAGMA busy_timeout={int(lock_wait * 1000)}")
    try:
        net.execute("BEGIN IMMEDIATE")
    finally:
        net.execute("PRAGMA busy_timeout=30000")


def _is_busy(exc: sqlite3.OperationalError) -> bool:
    return "locked" in str(exc).lower() or "busy" in str(exc).lower()


def _blocker(inbox, net):
    """Describe why the next batch cannot be published yet."""
    applied_seq = network_seq(net)
    nxt = inbox.execute("SELECT * FROM batches WHERE seq=?",
                        (applied_seq + 1,)).fetchone()
    if nxt is None:
        waiting = inbox.execute(
            "SELECT batch_id, seq, parts_expected FROM batches "
            "WHERE seq>? ORDER BY seq LIMIT 1", (applied_seq + 1,)).fetchone()
        if waiting is None:
            return {"reason": "up_to_date"}
        missing = _missing_parts(inbox, waiting["batch_id"],
                                 waiting["parts_expected"])
        if missing and applied_seq + 1 == waiting["seq"]:
            return {"reason": "waiting_for_parts", "batch": waiting["batch_id"],
                    "seq": waiting["seq"], "missing_parts": missing}
        return {"reason": "waiting_for_earlier_batch",
                "batch": waiting["batch_id"], "seq": waiting["seq"]}
    missing = _missing_parts(inbox, nxt["batch_id"], nxt["parts_expected"])
    if missing:
        return {"reason": "waiting_for_parts", "batch": nxt["batch_id"],
                "seq": nxt["seq"], "missing_parts": missing}
    if nxt["state"] == "rejected":
        return {"reason": "rejected", "batch": nxt["batch_id"],
                "seq": nxt["seq"], "error": nxt["error"]}
    # complete and not rejected, yet not in the network: a publisher is
    # applying it right now (or was interrupted mid-apply and will resume)
    return {"reason": "applying", "batch": nxt["batch_id"], "seq": nxt["seq"]}
