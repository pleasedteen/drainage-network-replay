"""Per-batch history: recording during publish, and online backfill.

Recording
---------
While a batch is applied, the triggers installed by ``app/db.py`` copy every
actual change (renumbering, rezoning, re-laying, retiring, reference
additions/removals) into the ``hist_*`` temporal tables -- but only when
``hist_context.seq`` names the batch being applied.  ``apply_batch`` calls
``begin_batch`` / ``end_batch`` around the appliers:

* ``begin_batch`` arms the triggers only when the history watermark
  (``meta.history_upto_seq``) is exactly ``seq - 1`` -- i.e. history is
  complete right up to this batch.  Otherwise the batch is applied without
  recording, and the backfill will fill the gap later.
* ``end_batch`` advances the watermark in the same transaction, so a
  batch's history is either committed whole or not at all.  A rolled-back
  batch also rolls back the context, so the triggers never record anything
  for it.

Backfill
--------
A database created before history tracking (or one that published while the
backfill was still catching up) rebuilds the missing versions from *landed
data only*: the staged ``st_*`` rows of every applied batch are replayed, in
seq order, into a scratch database using the very same ``apply_batch`` code
as live publishing -- so the rebuilt history is exactly what live recording
would have written.  The replay holds no lock on the live database; the
finished versions are then copied over one seq per transaction, so live
queries and new publishes continue throughout.  The copy only ever advances
the watermark, and checks it inside the write transaction, so a concurrent
publisher either records a batch's history itself or leaves it to the
backfill -- never both, and never partially.
"""

from __future__ import annotations

import time
from pathlib import Path

from .db import connect, init_db, meta_get, meta_set

STAGING_TABLES = ("st_ids", "st_taps", "st_lays", "st_redirects", "st_retires")
HIST_TABLES = ("hist_joints", "hist_pipes", "hist_refs")
HIST_COLUMNS = {
    "hist_joints": ("uid", "from_seq", "to_seq", "zone", "local", "attrs",
                    "retired"),
    "hist_pipes": ("code", "from_seq", "to_seq", "from_uid", "to_uid",
                   "attrs", "retired"),
    "hist_refs": ("ref", "from_seq", "to_seq", "uid", "kind"),
}


class BackfillError(RuntimeError):
    """The landed data cannot be replayed faithfully."""


# ---------------------------------------------------------------------------
# watermark
# ---------------------------------------------------------------------------

def upto_seq(conn) -> int:
    """Highest seq whose history is complete (0 = nothing recorded yet)."""
    return int(meta_get(conn, "history_upto_seq", 0) or 0)


def applied_seq(conn) -> int:
    return int(meta_get(conn, "applied_seq", 0) or 0)


def begin_batch(conn, seq: int) -> bool:
    """Arm the history triggers for this batch if history is up to date.

    Called inside the apply transaction.  Returns True when the batch's
    changes are being recorded (and ``end_batch`` must be called).
    """
    live = upto_seq(conn) == seq - 1
    conn.execute("UPDATE hist_context SET seq=? WHERE id=1",
                 (seq if live else None,))
    return live


def end_batch(conn, seq: int) -> None:
    """Mark this batch's history complete (same transaction as the apply)."""
    meta_set(conn, "history_upto_seq", seq)
    conn.execute("UPDATE hist_context SET seq=NULL WHERE id=1")


def status_dict(conn) -> dict:
    applied = applied_seq(conn)
    upto = upto_seq(conn)
    return {
        "upto_seq": upto,
        "applied_seq": applied,
        "complete": upto >= applied,
        "pending": max(0, applied - upto),
    }


# ---------------------------------------------------------------------------
# backfill
# ---------------------------------------------------------------------------

def _scratch_path(db_path) -> Path:
    p = Path(db_path)
    return p.with_name(p.name + ".backfill-scratch")


def _remove_scratch(path: Path) -> None:
    for suffix in ("", "-wal", "-shm"):
        try:
            Path(str(path) + suffix).unlink()
        except FileNotFoundError:
            pass


def _ferry(src_conn, dst_conn, table: str, sql: str, params=()) -> int:
    """Copy rows selected by ``sql`` on src into ``table`` on dst."""
    n = 0
    cur = src_conn.execute(sql, params)
    names = [d[0] for d in cur.description]
    marks = ",".join("?" for _ in names)
    insert = f"INSERT INTO {table}({','.join(names)}) VALUES({marks})"
    batch = []
    for row in cur:
        batch.append(tuple(row))
        if len(batch) >= 2000:
            dst_conn.executemany(insert, batch)
            batch.clear()
            n += 2000
    if batch:
        dst_conn.executemany(insert, batch)
        n += len(batch)
    return n


def _replay_one(live, scratch, seq: int) -> None:
    """Replay one applied batch from the live staging rows into scratch."""
    from . import snapshot            # deferred: snapshot imports history

    brow = live.execute(
        "SELECT * FROM batches WHERE seq=? AND state='applied'",
        (seq,)).fetchone()
    if brow is None:
        raise BackfillError(f"cannot backfill seq {seq}: no applied batch "
                            f"with that seq")
    batch_id = brow["batch_id"]
    staged = 0
    scratch.execute("BEGIN IMMEDIATE")
    try:
        scratch.execute(
            "INSERT INTO batches(batch_id, seq, total, upgrade, state, error,"
            " parts_expected, parts_received, segments, created_at,"
            " applied_at) VALUES(?,?,?,?,?,?,?,?,?,?,?)",
            (brow["batch_id"], brow["seq"], brow["total"], brow["upgrade"],
             "staging", None, brow["parts_expected"], brow["parts_received"],
             brow["segments"], brow["created_at"], brow["applied_at"]))
        for table in STAGING_TABLES:
            staged += _ferry(live, scratch, table,
                             f"SELECT * FROM {table} WHERE batch_id=? "
                             "ORDER BY part, item", (batch_id,))
        snapshot.apply_batch(scratch, brow)
        # faithfulness check: the replay must produce the same effects the
        # live apply recorded when the batch was published
        want = {r["kind"]: r["n"] for r in live.execute(
            "SELECT kind,n FROM batch_effects WHERE batch_id=?", (batch_id,))}
        got = {r["kind"]: r["n"] for r in scratch.execute(
            "SELECT kind,n FROM batch_effects WHERE batch_id=?", (batch_id,))}
        if got != want:
            raise BackfillError(
                f"replaying batch {batch_id!r} from landed data produced "
                f"effects {got}, but the live publish recorded {want}: the "
                f"staged data is incomplete or corrupt")
        scratch.execute("COMMIT")
    except BaseException:
        if scratch.in_transaction:
            scratch.execute("ROLLBACK")
        raise


def _copy_upto(db_path, scratch, target: int, copied: list,
               progress) -> None:
    """Copy finished history versions into the live DB, one seq per commit.

    Never overwrites: each transaction re-checks the watermark and only
    appends the rows of the very next seq.  A publisher that records a
    batch's history itself simply moves the watermark past it, and the
    backfill skips ahead.
    """
    wconn = connect(db_path)
    try:
        seq = upto_seq(wconn) + 1
        while seq <= target:
            rows = {}
            for table in HIST_TABLES:
                cols = ",".join(HIST_COLUMNS[table])
                rows[table] = [tuple(r) for r in scratch.execute(
                    f"SELECT {cols} FROM {table} WHERE from_seq=?", (seq,))]
            wconn.execute("BEGIN IMMEDIATE")
            try:
                cur = upto_seq(wconn)
                if cur >= seq:          # a publisher (or another backfill)
                    wconn.execute("ROLLBACK")   # got here first; skip ahead
                    seq = cur + 1
                    continue
                if cur != seq - 1:      # watermark moved backwards: cannot
                    wconn.execute("ROLLBACK")   # happen; stop rather than
                    raise BackfillError(        # guess
                        f"history watermark moved from {seq - 1} to {cur} "
                        f"during backfill")
                for table in HIST_TABLES:
                    if rows[table]:
                        marks = ",".join("?" for _ in HIST_COLUMNS[table])
                        wconn.executemany(
                            f"INSERT INTO {table} VALUES({marks})",
                            rows[table])
                meta_set(wconn, "history_upto_seq", seq)
                wconn.execute("COMMIT")
            except BaseException:
                if wconn.in_transaction:
                    wconn.execute("ROLLBACK")
                raise
            copied.append(seq)
            if progress is not None:
                progress(seq, target)
            seq += 1
    finally:
        wconn.close()


def backfill(db_path, *, progress=None, settle: float = 0.0) -> dict:
    """Rebuild missing per-batch history from landed data.  Online.

    ``progress(seq, target)`` is called after each seq committed to the live
    database.  ``settle`` (seconds) is slept between outer passes -- only
    useful to give a concurrent publisher the lock in tests and demos.
    Returns a report; safe to re-run at any time (it only fills gaps).
    """
    db_path = str(db_path)
    init_db(db_path).close()
    scratch_path = _scratch_path(db_path)
    _remove_scratch(scratch_path)
    copied: list = []
    try:
        scratch = init_db(scratch_path)
        try:
            while True:
                live = connect(db_path, read_only=True)
                try:
                    applied = applied_seq(live)
                    if upto_seq(live) >= applied:
                        break
                    # replay every applied batch the scratch DB has not
                    # reached yet (its own applied_seq tracks the replay)
                    for seq in range(applied_seq(scratch) + 1, applied + 1):
                        _replay_one(live, scratch, seq)
                finally:
                    live.close()
                _copy_upto(db_path, scratch, applied, copied, progress)
                if settle:
                    time.sleep(settle)
        finally:
            scratch.close()
    finally:
        _remove_scratch(scratch_path)

    conn = connect(db_path, read_only=True)
    try:
        return {"copied": copied, "history": status_dict(conn)}
    finally:
        conn.close()
