"""Receiving many segments in one process, plus the apply trigger.

``SyncService`` is what the (offline) replay source calls.  It:

* lands each segment (validate -> digest -> archive, one commit per segment);
* keeps a directory of accepted segments for the source to compare against;
* publishes every batch that is complete and next in line, so queries served
  by the same process immediately see a finished batch;
* remembers, for each segment, the exact acknowledgement answer, so a source
  asking again gets the same answer instead of a different-looking status.

Everything is restartable: a process killed after an acknowledgement but
before publishing simply replays the already-archived batch on startup.
"""

from __future__ import annotations

import threading
from pathlib import Path

from . import archive, snapshot
from .db import connect, init_db


class SyncService:
    def __init__(self, db_path, data_dir, *, auto_publish: bool = True,
                 chunk_size: int = 65536):
        self.db_path = str(db_path)
        self.data_dir = Path(data_dir)
        self.auto_publish = auto_publish
        self.chunk_size = chunk_size
        self.data_dir.mkdir(parents=True, exist_ok=True)
        init_db(self.db_path).close()
        self._publish_lock = threading.Lock()

    # -- receive ----------------------------------------------------------
    def accept(self, path) -> dict:
        res = archive.land_segment(self.db_path, self.data_dir, path,
                                   chunk_size=self.chunk_size)
        out = res.as_dict()
        if res.acknowledged and self.auto_publish:
            out["publish"] = self.publish()
        elif not res.acknowledged:
            out["publish"] = {"applied": [], "stopped": {"reason": "batch "
                              "already acknowledged; nothing changed"}}
        return out

    def accept_many(self, paths) -> list[dict]:
        return [self.accept(p) for p in paths]

    # -- publish ----------------------------------------------------------
    def publish(self, *, max_batches=None) -> dict:
        with self._publish_lock:
            conn = connect(self.db_path)
            try:
                return snapshot.publish_ready(conn, max_batches=max_batches,
                                              data_dir=self.data_dir)
            finally:
                conn.close()

    # -- reports ----------------------------------------------------------
    def status(self) -> dict:
        conn = connect(self.db_path, read_only=True)
        try:
            batches = []
            for row in conn.execute("SELECT * FROM batches ORDER BY seq"):
                info = archive.batch_status(conn, row["batch_id"])
                batches.append(info)
            applied_row = conn.execute(
                "SELECT CAST(value AS INTEGER) FROM meta WHERE key='applied_seq'"
            ).fetchone()
            applied = applied_row[0] if applied_row else 0
            return {
                "snapshot": _snapshot_dict(conn),
                "applied_seq": applied,
                "batches": batches,
                "publish_blocker": snapshot._blocker(conn),
            }
        finally:
            conn.close()

    def batch_detail(self, batch_id: str) -> dict:
        conn = connect(self.db_path, read_only=True)
        try:
            info = archive.batch_status(conn, batch_id)
            if info is None:
                raise KeyError(batch_id)
            segs = [dict(r) for r in conn.execute(
                "SELECT seg_id,part,parts,nbytes,sha256,path,received_at "
                "FROM segments WHERE batch_id=? ORDER BY part", (batch_id,))]
            effects = {r["kind"]: r["n"] for r in conn.execute(
                "SELECT kind,n FROM batch_effects WHERE batch_id=? "
                "ORDER BY kind", (batch_id,))}
            info["segments_detail"] = segs
            info["effects"] = effects
            return info
        finally:
            conn.close()


def _snapshot_dict(conn) -> dict:
    from . import tracing
    return tracing.snapshot_info(conn)


def serve_in_thread(db_path, data_dir="", host="127.0.0.1", port=0):
    """Start the query API in a background thread (tests / demo)."""
    from .server import make_server
    srv = make_server(db_path, data_dir, host=host, port=port)
    t = threading.Thread(target=srv.serve_forever, daemon=True)
    t.start()
    return srv, t
