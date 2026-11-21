"""Applying a complete batch from the inbox to the published network.

Batches are applied strictly in ``seq`` order, one at a time, each inside a
single write transaction **on the publish database**.  Readers therefore only
ever see the network as of the end of some batch -- a half-applied batch is
never visible.  A batch whose data conflicts with the published network is
rolled back *and retired*: it keeps its ``rejected`` state plus an error,
blocks later batches, and the last good snapshot stays queryable.

Two databases, one commit point
-------------------------------
Receiving (inbox) and publishing (publish db) are separate files.  Applying a
batch is a read of the inbox's staging rows and a single transaction on the
publish database; the publish ledger row (``published_batches``) and
``meta.applied_seq`` commit *in that same transaction*, so the ledger always
agrees with the queryable network.  Only afterwards is the inbox batch row
marked ``applied`` (a second, tiny transaction).  A ``kill -9`` in between
leaves the inbox row at ``staging`` while the publish db already reflects the
batch; :func:`reconcile` repairs that on restart, and because the apply is
gated on ``seq == applied_seq + 1`` plus the ledger's ``UNIQUE(seq)``, the
committed effects of a batch can never be applied twice.

The publish database is updated **incrementally**: a batch that changes one
joint attribute writes a handful of rows and pages, no matter how large the
published network already is.

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
# reconciliation: make the inbox reflect what the publish db already committed
# ---------------------------------------------------------------------------

def reconcile(inbox, publish) -> dict:
    """Copy the publish ledger's truth into the inbox's batch rows.

    The publish database commits first, so it is the authority on which
    batches are applied or rejected.  A crash between that commit and the
    inbox update leaves the inbox row behind; this brings it up to date.
    Idempotent and cheap (one indexed pass over the ledger).
    """
    ledger = [dict(r) for r in publish.execute(
        "SELECT batch_id, seq, state, error, applied_at FROM published_batches")]
    fixed = []
    inbox.execute("BEGIN IMMEDIATE")
    try:
        for b in ledger:
            cur = inbox.execute(
                "UPDATE batches SET state=?, error=?, "
                "applied_at=COALESCE(applied_at, ?) "
                "WHERE batch_id=? AND state<>?",
                (b["state"], b["error"], b["applied_at"],
                 b["batch_id"], b["state"]))
            if cur.rowcount:
                fixed.append(b["batch_id"])
        inbox.execute("COMMIT")
    except BaseException:
        if inbox.in_transaction:
            inbox.execute("ROLLBACK")
        raise
    return {"reconciled": fixed}


# ---------------------------------------------------------------------------
# readiness
# ---------------------------------------------------------------------------

def batch_ready(inbox, row) -> bool:
    parts = [r[0] for r in inbox.execute(
        "SELECT DISTINCT part FROM segments WHERE batch_id=?",
        (row["batch_id"],))]
    return (len(parts) == row["parts_expected"]
            and sorted(parts) == list(range(row["parts_expected"])))


def _ledger_row(publish, seq: int):
    return publish.execute(
        "SELECT batch_id, state, error FROM published_batches WHERE seq=?",
        (seq,)).fetchone()


def next_batch_to_apply(inbox, publish):
    """The batch the publisher should look at next, or ``None``.

    Returns the inbox row for the next seq, with the publish ledger's
    rejection (if any) taking precedence, so a rejected batch is reported as
    a block rather than re-attempted.
    """
    applied = int(meta_get(publish, "applied_seq", 0))
    nxt = applied + 1
    led = _ledger_row(publish, nxt)
    if led is not None and led["state"] == "rejected":
        return {"batch_id": led["batch_id"], "seq": nxt, "state": "rejected",
                "error": led["error"]}
    row = inbox.execute("SELECT * FROM batches WHERE seq=?", (nxt,)).fetchone()
    if row is None:
        return None
    if row["state"] == "rejected":
        return dict(row)
    if row["state"] != "staging":
        return None                     # already mirrored by the ledger
    if not batch_ready(inbox, row):
        return None
    return row


# ---------------------------------------------------------------------------
# reference resolution (all on the publish database)
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
# batch application: staging rows stream out of the inbox, the network is
# rewritten inside one transaction on the publish database
# ---------------------------------------------------------------------------

def _rows(inbox, table, batch_id):
    """Stream staged items of one section in a stable order."""
    return inbox.execute(f"SELECT * FROM {table} WHERE batch_id=? "
                         "ORDER BY part, item", (batch_id,))


def _effect(effects, kind, n=1):
    effects[kind] = effects.get(kind, 0) + n


def _apply_ids(inbox, pub, batch_id, seq, effects):
    for row in _rows(inbox, "st_ids", batch_id):
        old, new = row["old_ref"], row["new_ref"]
        zone, local, _ = _split(new)
        uid_old = ref_uid(pub, old)
        uid_new = ref_uid(pub, new)
        if uid_old is not None and uid_new is not None and uid_old != uid_new:
            raise ApplyError(
                f"id table says {old!r} == {new!r}, but they are two different "
                f"joints (uid {uid_old} and uid {uid_new})")
        if uid_old is None and uid_new is None:
            uid = _create_joint(pub, zone, local, seq, None)
            _add_ref(pub, new, uid, "qualified", seq)
            _add_ref(pub, old, uid, "legacy", seq)
            _effect(effects, "ids")
            continue
        uid = uid_old if uid_old is not None else uid_new
        jr = pub.execute("SELECT zone,local FROM joints WHERE uid=?",
                         (uid,)).fetchone()
        if jr["zone"] == "":
            _rename_joint(pub, uid, zone, local, seq)
        elif (jr["zone"], jr["local"]) != (zone, local):
            raise ApplyError(
                f"id table maps {old!r} to {new!r}, but that joint is already "
                f"{jr['zone']}:{jr['local']}")
        _add_ref(pub, old, uid, "legacy", seq)
        _add_ref(pub, new, uid, "qualified", seq)
        pub.execute("UPDATE joints SET last_seq=? WHERE uid=?", (seq, uid))
        _effect(effects, "ids")


def _apply_taps(inbox, pub, batch_id, seq, effects):
    for row in _rows(inbox, "st_taps", batch_id):
        existed = ref_uid(pub, row["ref"]) is not None
        uid = _ensure_joint(pub, row["ref"], row["zone"], row["local"], seq)
        if row["attrs"] is not None:
            pub.execute(
                "UPDATE joints SET attrs = CASE "
                "WHEN attrs IS NULL THEN json(?) ELSE json_patch(attrs, json(?)) "
                "END WHERE uid=?", (row["attrs"], row["attrs"], uid))
        _effect(effects, "tap_updated" if existed else "tap_created")


def _apply_lays(inbox, pub, batch_id, seq, effects):
    for row in _rows(inbox, "st_lays", batch_id):
        code = row["code"]
        fu = resolve_existing(pub, row["from_ref"], f"pipe {code}", seq)
        tu = resolve_existing(pub, row["to_ref"], f"pipe {code}", seq)
        if fu == tu:
            raise ApplyError(f"pipe {code} would connect joint uid {fu} to "
                             f"itself")
        old = pub.execute("SELECT from_uid,to_uid,retired_seq,attrs FROM pipes "
                          "WHERE code=?", (code,)).fetchone()
        if old is None:
            pub.execute(
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
            pub.execute(
                "UPDATE pipes SET from_uid=?, to_uid=?, last_seq=?, "
                "retired_seq=NULL, retired_by=NULL, attrs=? WHERE code=?",
                (fu, tu, seq, attrs, code))
            _effect(effects, "lay_moved" if moved else "lay_updated")


def _apply_redirects(inbox, pub, batch_id, seq, effects):
    for row in _rows(inbox, "st_redirects", batch_id):
        if row["kind"] == "joint":
            src = resolve_existing(pub, row["from_ref"], "redirect", seq)
            dst = resolve_existing(pub, row["to_ref"], "redirect target", seq)
            if src == dst:
                continue
            loop = pub.execute(
                "SELECT code FROM pipes WHERE from_uid=? AND to_uid=? "
                "AND retired_seq IS NULL LIMIT 1", (src, dst)).fetchone()
            if loop is not None:
                raise ApplyError(
                    f"redirecting pipes from joint {src} to joint {dst} would "
                    f"make pipe {loop['code']} start and end at the same joint")
            cur = pub.execute(
                "UPDATE pipes SET from_uid=?, last_seq=? "
                "WHERE from_uid=? AND retired_seq IS NULL", (dst, seq, src))
            _effect(effects, "redirect_joint", cur.rowcount)
        else:
            code = row["code"]
            fu = resolve_existing(pub, row["from_ref"], f"pipe {code}", seq)
            tu = resolve_existing(pub, row["to_ref"], f"pipe {code}", seq)
            if fu == tu:
                raise ApplyError(f"pipe {code} would connect a joint to itself")
            cur = pub.execute(
                "UPDATE pipes SET from_uid=?, to_uid=?, last_seq=?, "
                "retired_seq=NULL, retired_by=NULL WHERE code=?",
                (fu, tu, seq, code))
            if cur.rowcount == 0:
                raise ApplyError(f"redirect names unknown pipe {code!r}")
            _effect(effects, "redirect_pipe")


def _apply_retires(inbox, pub, batch_id, seq, effects):
    for row in _rows(inbox, "st_retires", batch_id):
        if row["kind"] == "joint":
            uid = resolve_existing(pub, row["ref"], "retire", seq)
            cur = pub.execute(
                "UPDATE joints SET retired_seq=?, last_seq=? "
                "WHERE uid=? AND retired_seq IS NULL", (seq, seq, uid))
            if cur.rowcount == 0:
                _effect(effects, "joint_already_retired")
                continue
            pipes = pub.execute(
                "UPDATE pipes SET retired_seq=?, retired_by='joint-retired' "
                "WHERE (from_uid=? OR to_uid=?) AND retired_seq IS NULL",
                (seq, uid, uid))
            _effect(effects, "joint_retired")
            _effect(effects, "pipe_retired_by_joint", pipes.rowcount)
        else:
            cur = pub.execute(
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


def apply_batch(inbox, pub, row) -> dict:
    """Apply one complete batch.  Caller owns the publish-db transaction."""
    seq, batch_id = row["seq"], row["batch_id"]
    effects: dict = {}

    applied_seq = int(meta_get(pub, "applied_seq", 0))
    if seq != applied_seq + 1:
        raise ApplyError(f"batch {batch_id} has seq {seq}, expected "
                         f"{applied_seq + 1}: batches must apply in order")

    if row["upgrade"]:
        first = meta_get(pub, "upgrade_seq")
        if first is not None and int(first) != seq:
            raise ApplyError(f"batch {batch_id} is marked upgrade, but the "
                             f"upgrade already happened at seq {first}")
        meta_set(pub, "upgrade_seq", seq)

    for _, fn in APPLIERS:
        fn(inbox, pub, batch_id, seq, effects)

    pub.execute("DELETE FROM batch_effects WHERE batch_id=?", (batch_id,))
    for kind, n in sorted(effects.items()):
        if n:
            pub.execute("INSERT INTO batch_effects(batch_id,seq,kind,n) "
                        "VALUES(?,?,?,?)", (batch_id, seq, kind, n))
    pub.execute(
        "INSERT INTO published_batches(batch_id, seq, state, applied_at) "
        "VALUES(?,?, 'applied', ?)", (batch_id, seq, _now()))
    meta_set(pub, "applied_seq", seq)
    return effects


def _mark_inbox(inbox, batch_id, state, error=None):
    """Mirror a publish-side outcome into the inbox (its own transaction).

    A crash between the publish commit and this update is repaired by
    :func:`reconcile` on the next startup or publish run.
    """
    inbox.execute("BEGIN IMMEDIATE")
    try:
        inbox.execute("UPDATE batches SET state=?, error=?, applied_at=? "
                      "WHERE batch_id=?",
                      (state, error, _now() if state == "applied" else None,
                       batch_id))
        inbox.execute("COMMIT")
    except BaseException:
        if inbox.in_transaction:
            inbox.execute("ROLLBACK")
        raise


def publish_ready(inbox, pub, *, max_batches=None) -> dict:
    """Apply every batch that is complete and next in line.

    ``inbox`` and ``pub`` are connections to the two databases.  Returns a
    report: which batches were applied, and why publishing stopped.  Safe
    against several publishers: the applicability check is repeated under the
    publish database's write lock, so a batch another process already applied
    is simply skipped rather than being applied twice.
    """
    reconcile(inbox, pub)
    applied, stopped = [], None
    n = 0
    while max_batches is None or n < max_batches:
        row = next_batch_to_apply(inbox, pub)
        if row is None:
            break
        if row["state"] == "rejected":
            stopped = {"reason": "rejected", "batch": row["batch_id"],
                       "seq": row["seq"], "error": row["error"]}
            break
        pub.execute("BEGIN IMMEDIATE")
        # re-check under the publish write lock: another publisher may have
        # committed this seq while we were looking at the inbox
        led = _ledger_row(pub, row["seq"])
        fresh = inbox.execute("SELECT * FROM batches WHERE batch_id=?",
                              (row["batch_id"],)).fetchone()
        if led is not None or fresh is None or fresh["state"] != "staging" \
                or not batch_ready(inbox, fresh):
            pub.execute("ROLLBACK")
            if led is not None and led["state"] == "rejected":
                stopped = {"reason": "rejected", "batch": led["batch_id"],
                           "seq": row["seq"], "error": led["error"]}
                break
            continue
        try:
            effects = apply_batch(inbox, pub, fresh)
            pub.execute("COMMIT")
        except ApplyError as exc:
            pub.execute("ROLLBACK")
            pub.execute("BEGIN IMMEDIATE")
            pub.execute(
                "INSERT INTO published_batches(batch_id, seq, state, error) "
                "VALUES(?,?, 'rejected', ?) ON CONFLICT(batch_id) DO NOTHING",
                (row["batch_id"], row["seq"], str(exc)))
            pub.execute("COMMIT")
            _mark_inbox(inbox, row["batch_id"], "rejected", str(exc))
            stopped = {"reason": "rejected", "batch": row["batch_id"],
                       "seq": row["seq"], "error": str(exc)}
            break
        except sqlite3.Error:
            # storage-level failure: leave the batch staged so it can be
            # retried after a restart instead of being rejected for good
            if pub.in_transaction:
                pub.execute("ROLLBACK")
            raise
        _mark_inbox(inbox, row["batch_id"], "applied")
        applied.append({"batch": row["batch_id"], "seq": row["seq"],
                        "effects": effects})
        n += 1
    else:
        stopped = {"reason": "max_batches"}
    if stopped is None:
        stopped = _blocker(inbox, pub)
    return {"applied": applied, "stopped": stopped}


def _blocker(inbox, pub):
    """Describe why the next batch cannot be published yet."""
    applied_seq = int(meta_get(pub, "applied_seq", 0))
    led = _ledger_row(pub, applied_seq + 1)
    if led is not None and led["state"] == "rejected":
        return {"reason": "rejected", "batch": led["batch_id"],
                "seq": applied_seq + 1, "error": led["error"]}
    nxt = inbox.execute("SELECT * FROM batches WHERE seq=?",
                        (applied_seq + 1,)).fetchone()
    if nxt is None:
        waiting = inbox.execute(
            "SELECT batch_id, seq, parts_expected, parts_received FROM batches "
            "WHERE seq>? ORDER BY seq LIMIT 1", (applied_seq + 1,)).fetchone()
        if waiting is None:
            return {"reason": "up_to_date"}
        return {"reason": "waiting_for_earlier_batch",
                "batch": waiting["batch_id"], "seq": waiting["seq"]}
    if nxt["state"] == "rejected":
        return {"reason": "rejected", "batch": nxt["batch_id"],
                "seq": nxt["seq"], "error": nxt["error"]}
    missing = sorted(set(range(nxt["parts_expected"]))
                     - set(r[0] for r in inbox.execute(
                         "SELECT DISTINCT part FROM segments WHERE batch_id=?",
                         (nxt["batch_id"],))))
    if missing:
        return {"reason": "waiting_for_parts", "batch": nxt["batch_id"],
                "seq": nxt["seq"], "missing_parts": missing}
    return {"reason": "unknown", "batch": nxt["batch_id"], "seq": nxt["seq"]}
