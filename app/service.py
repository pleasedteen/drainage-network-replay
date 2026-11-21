"""Receiving many segments in one process, plus the apply trigger.

``SyncService`` is what the (offline) replay source calls.  It:

* lands each segment (validate -> digest -> stage, short commits per chunk);
* keeps a directory of accepted segments for the source to compare against;
* publishes every batch that is complete and next in line, so queries served
  by the same process immediately see a finished batch;
* remembers, for each segment, the exact acknowledgement answer, so a source
  asking again gets the same answer instead of a different-looking status.

Two database files back this (see ``db``): the receive/staging bookkeeping,
and the published network.  An acknowledgement writes only the receive file;
an apply writes only the published file.  Thus a small segment delivered while
a huge batch is being staged or applied is acknowledged within a short,
bounded pause rather than waiting for the huge batch.

Everything is restartable: a process killed after an acknowledgement but
before publishing simply publishes the already-staged batch on startup
(``SyncService.publish`` is called on construction).
"""

from __future__ import annotations

import sqlite3
import threading
from pathlib import Path

from . import archive, db, snapshot
from .db import connect, init_db

# How long an acknowledgement waits for the published file's write lock before
# giving up and leaving publishing to whoever holds it.  An acceptance is
# already durable before this point; the source may clean up its copy either
# way.  Keeping it small is what guarantees a small segment delivered during a
# large apply is still acknowledged within a second.
ACK_PUBLISH_WAIT = 0.25


class SyncService:
    def __init__(self, db_path, data_dir, *, auto_publish: bool = True,
                 chunk_size: int = 65536, resume_publish: bool = True):
        self.db_path = str(db_path)
        self.published_path = str(db.published_path(db_path))
        self.data_dir = Path(data_dir)
        self.auto_publish = auto_publish
        self.chunk_size = chunk_size
        self.data_dir.mkdir(parents=True, exist_ok=True)
        init_db(self.db_path).close()
        self._publish_lock = threading.Lock()
        if auto_publish and resume_publish:
            # publish anything already staged, e.g. before a kill -9.  Bounded:
            # if another process is mid-apply, leave it to finish rather than
            # block startup (the batch stays staged and publishes in order).
            try:
                self.publish(wait=ACK_PUBLISH_WAIT)
            except Exception:                  # noqa: BLE001 - never block startup
                pass

    # -- receive ----------------------------------------------------------
    def accept(self, path) -> dict:
        res = archive.land_segment(self.db_path, self.data_dir, path,
                                   chunk_size=self.chunk_size)
        out = res.as_dict()
        if res.acknowledged and self.auto_publish:
            # The acknowledgement is already committed; publishing in the same
            # call is best-effort and bounded, so a large apply running in
            # another thread/process cannot delay the answer.
            out["publish"] = self.publish(wait=ACK_PUBLISH_WAIT)
        elif not res.acknowledged:
            out["publish"] = {"applied": [], "stopped": {"reason": "batch "
                              "already acknowledged; nothing changed"}}
        return out

    def accept_many(self, paths) -> list[dict]:
        return [self.accept(p) for p in paths]

    # -- publish ----------------------------------------------------------
    def publish(self, *, max_batches=None, wait: float | None = None) -> dict:
        """Apply every complete, next-in-line batch, then report.

        ``wait`` bounds how long to wait for the published file's write lock
        (seconds).  ``None`` means the connection's default.  If the lock
        cannot be obtained in time, nothing is applied and a
        ``publisher_busy`` stop reason is returned; the batches stay staged and
        the holder of the lock (or a later publish/restart) applies them in
        source order.
        """
        timeout = 30.0 if wait is None else max(0.05, wait)
        # bound the wait for an in-process publisher too, not just the file lock
        if wait is None:
            acquired = self._publish_lock.acquire()
        else:
            acquired = self._publish_lock.acquire(timeout=timeout)
        if not acquired:
            return _busy()
        try:
            conn = connect(self.published_path, timeout=timeout)
            try:
                conn.execute("ATTACH DATABASE ? AS rcv",
                             (f"file:{self.db_path}?mode=ro",))
                return snapshot.publish_ready(conn, max_batches=max_batches)
            except sqlite3.OperationalError as exc:
                if "locked" in str(exc).lower() or "busy" in str(exc).lower():
                    return _busy()
                raise
            finally:
                conn.close()
        finally:
            self._publish_lock.release()

    # -- reports ----------------------------------------------------------
    def status(self) -> dict:
        conn = connect(self.db_path, read_only=True)
        pub = connect(self.published_path, read_only=True)
        try:
            batches = []
            for row in conn.execute("SELECT * FROM batches ORDER BY seq"):
                info = archive.batch_status(conn, row["batch_id"], published=pub)
                batches.append(info)
            applied_row = pub.execute(
                "SELECT CAST(value AS INTEGER) FROM meta WHERE key='applied_seq'"
            ).fetchone()
            applied = applied_row[0] if applied_row else 0
            return {
                "snapshot": _snapshot_dict(pub),
                "applied_seq": applied,
                "batches": batches,
                "publish_blocker": _blocker(conn, pub),
            }
        finally:
            conn.close()
            pub.close()

    def batch_detail(self, batch_id: str) -> dict:
        conn = connect(self.db_path, read_only=True)
        pub = connect(self.published_path, read_only=True)
        try:
            info = archive.batch_status(conn, batch_id, published=pub)
            if info is None:
                raise KeyError(batch_id)
            segs = [dict(r) for r in conn.execute(
                "SELECT seg_id,part,parts,nbytes,sha256,path,received_at "
                "FROM segments WHERE batch_id=? ORDER BY part", (batch_id,))]
            effects = {r["kind"]: r["n"] for r in pub.execute(
                "SELECT kind,n FROM batch_effects WHERE batch_id=? "
                "ORDER BY kind", (batch_id,))}
            info["segments_detail"] = segs
            info["effects"] = effects
            return info
        finally:
            conn.close()
            pub.close()


def _busy() -> dict:
    return {"applied": [], "stopped": {
        "reason": "publisher_busy",
        "detail": "another apply holds the published file; batches stay "
                  "staged and publish in source order"}}


def _blocker(conn, pub) -> dict:
    """Why the next batch is not published yet (receive file + published ledger)."""
    applied_row = pub.execute(
        "SELECT CAST(value AS INTEGER) FROM meta WHERE key='applied_seq'"
    ).fetchone()
    applied_seq = applied_row[0] if applied_row else 0
    seq = applied_seq + 1

    def parts(batch_id):
        return sorted(r[0] for r in conn.execute(
            "SELECT DISTINCT part FROM segments WHERE batch_id=?", (batch_id,)))

    row = conn.execute("SELECT * FROM batches WHERE seq=?", (seq,)).fetchone()
    if row is None:
        waiting = conn.execute(
            "SELECT * FROM batches WHERE seq>? ORDER BY seq LIMIT 1",
            (seq,)).fetchone()
        if waiting is None:
            return {"reason": "up_to_date"}
        return {"reason": "waiting_for_earlier_batch",
                "batch": waiting["batch_id"], "seq": waiting["seq"]}
    led = pub.execute("SELECT state,error FROM published_batches WHERE seq=?",
                      (seq,)).fetchone()
    if led is not None and led["state"] == "rejected":
        return {"reason": "rejected", "batch": row["batch_id"],
                "seq": row["seq"], "error": led["error"]}
    missing = sorted(set(range(row["parts_expected"]))
                     - set(parts(row["batch_id"])))
    if missing:
        return {"reason": "waiting_for_parts", "batch": row["batch_id"],
                "seq": row["seq"], "missing_parts": missing}
    return {"reason": "unknown", "batch": row["batch_id"], "seq": row["seq"]}


def _snapshot_dict(pub_conn) -> dict:
    from . import tracing
    return tracing.snapshot_info(pub_conn)
