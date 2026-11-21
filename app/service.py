"""Receiving many segments in one process, plus the apply trigger.

``SyncService`` is what the (offline) replay source calls.  It:

* lands each segment (validate -> digest -> stage, one commit per segment) in
  the *inbox* database;
* keeps a directory of accepted segments for the source to compare against;
* publishes every batch that is complete and next in line, applying it in one
  transaction against the *network* database;
* remembers, for each segment, the exact acknowledgement answer, so a source
  asking again gets the same answer instead of a different-looking status.

Because the inbox and the network are separate files, acknowledging a segment
and applying a batch never contend for the same write lock.  A big batch can
be mid-apply while the next segment is landed and acknowledged, and queries
keep answering from the last complete snapshot.  Everything is restartable: a
process killed after an acknowledgement but before publishing, or killed
mid-apply, simply finishes publishing the already-staged batches on startup.
"""

from __future__ import annotations

import threading
from pathlib import Path

from . import archive, snapshot
from .db import connect, init_db, network_path


class SyncService:
    def __init__(self, db_path, data_dir, *, auto_publish: bool = True,
                 chunk_size: int = 65536, publish_lock_wait: float | None = 5.0):
        self.db_path = str(db_path)
        self.data_dir = Path(data_dir)
        self.auto_publish = auto_publish
        self.chunk_size = chunk_size
        self.publish_lock_wait = publish_lock_wait
        self.data_dir.mkdir(parents=True, exist_ok=True)
        init_db(self.db_path).close()
        self._publish_lock = threading.Lock()

    @property
    def network_path(self) -> str:
        return str(network_path(self.db_path))

    # -- receive ----------------------------------------------------------
    def accept(self, path) -> dict:
        res = archive.land_segment(self.db_path, self.data_dir, path,
                                   chunk_size=self.chunk_size)
        out = res.as_dict()
        # The acknowledgement is already committed at this point.  Publishing
        # is a courtesy to this process' own callers; if another process is
        # applying a big batch, it must neither delay nor fail the ack.
        if res.acknowledged and self.auto_publish:
            out["publish"] = self.publish(opportunistic=True)
        elif not res.acknowledged:
            out["publish"] = {"applied": [], "stopped": {"reason": "batch "
                              "already acknowledged; nothing changed"}}
        return out

    def accept_many(self, paths) -> list[dict]:
        return [self.accept(p) for p in paths]

    # -- publish ----------------------------------------------------------
    def publish(self, *, max_batches=None, opportunistic: bool = False) -> dict:
        """Apply every ready batch (see ``snapshot.publish_ready``).

        ``opportunistic`` is used on the acknowledgement path: the ack is
        already durable, so instead of waiting for a busy network lock this
        returns ``stopped.reason == 'busy'`` and lets the next publisher --
        this process, a sibling shipper, or the restart -- apply the batch.
        """
        lock_wait = 0.0 if opportunistic else self.publish_lock_wait
        with self._publish_lock:
            inbox = connect(self.db_path)
            try:
                net = connect(self.network_path)
                try:
                    return snapshot.publish_ready(
                        inbox, net, max_batches=max_batches, lock_wait=lock_wait)
                finally:
                    net.close()
            finally:
                inbox.close()

    # -- reports ----------------------------------------------------------
    def status(self) -> dict:
        inbox = connect(self.db_path, read_only=True)
        try:
            net = connect(self.network_path, read_only=True)
            try:
                applied = snapshot.network_seq(net)
                batches = []
                for row in inbox.execute("SELECT * FROM batches ORDER BY seq"):
                    info = archive.batch_status(inbox, row["batch_id"])
                    info["state"] = snapshot.effective_state(row, applied)
                    batches.append(info)
                return {
                    "snapshot": _snapshot_dict(net),
                    "applied_seq": applied,
                    "batches": batches,
                    "publish_blocker": snapshot._blocker(inbox, net),
                }
            finally:
                net.close()
        finally:
            inbox.close()

    def batch_detail(self, batch_id: str) -> dict:
        inbox = connect(self.db_path, read_only=True)
        try:
            net = connect(self.network_path, read_only=True)
            try:
                applied = snapshot.network_seq(net)
                info = archive.batch_status(inbox, batch_id)
                if info is None:
                    raise KeyError(batch_id)
                row = inbox.execute("SELECT * FROM batches WHERE batch_id=?",
                                    (batch_id,)).fetchone()
                info["state"] = snapshot.effective_state(row, applied)
                segs = [dict(r) for r in inbox.execute(
                    "SELECT seg_id,part,parts,nbytes,sha256,path,received_at "
                    "FROM segments WHERE batch_id=? ORDER BY part", (batch_id,))]
                effects = {r["kind"]: r["n"] for r in net.execute(
                    "SELECT kind,n FROM batch_effects WHERE batch_id=? "
                    "ORDER BY kind", (batch_id,))}
                info["segments_detail"] = segs
                info["effects"] = effects
                return info
            finally:
                net.close()
        finally:
            inbox.close()


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
