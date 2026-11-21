"""Receiving many segments in one process, plus the publish trigger.

``SyncService`` is what the (offline) replay source calls.  It works on two
independent databases:

* the **inbox** (接收库) -- each segment lands there (validate -> digest ->
  stage, one commit per segment); the acknowledgement is durable the moment
  that commit returns, whatever the publish side is doing;
* the **publish** database (发布库) -- every batch that is complete and next
  in line is applied there in one transaction per batch, so queries served
  from it immediately see a finished batch and never a half one.

Everything is restartable: a process killed after an acknowledgement but
before publishing simply publishes the already-staged batch on startup, and a
process killed between the publish commit and the inbox bookkeeping is
reconciled at startup, so the committed effects of a batch apply once and
only once.
"""

from __future__ import annotations

import os
import sqlite3
import threading
from pathlib import Path

from . import archive, snapshot
from .db import connect, init_inbox, init_publish


class SyncService:
    def __init__(self, inbox_db, publish_db, data_dir, *, auto_publish: bool = True,
                 chunk_size: int = 65536):
        self.inbox_db = str(inbox_db)
        self.publish_db = str(publish_db)
        self.data_dir = Path(data_dir)
        self.auto_publish = auto_publish
        self.chunk_size = chunk_size
        self.data_dir.mkdir(parents=True, exist_ok=True)
        init_inbox(self.inbox_db).close()
        init_publish(self.publish_db).close()
        self._publish_lock = threading.Lock()
        # a previous process may have died between the publish commit and the
        # inbox bookkeeping: align the inbox with the committed truth now
        inbox, pub = connect(self.inbox_db), connect(self.publish_db)
        try:
            snapshot.reconcile(inbox, pub)
        finally:
            inbox.close()
            pub.close()

    # -- receive ----------------------------------------------------------
    def accept(self, path) -> dict:
        res = archive.land_segment(self.inbox_db, self.data_dir, path,
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
            inbox, pub = connect(self.inbox_db), connect(self.publish_db)
            try:
                return snapshot.publish_ready(inbox, pub,
                                              max_batches=max_batches)
            finally:
                inbox.close()
                pub.close()

    # -- reports ----------------------------------------------------------
    def status(self) -> dict:
        return status_report(self.publish_db, self.inbox_db)

    def batch_detail(self, batch_id: str) -> dict:
        return batch_detail_report(self.publish_db, batch_id, self.inbox_db)


# ---------------------------------------------------------------------------
# read-only reports (used by the HTTP API): the publish database alone always
# answers; inbox progress is merged in when the inbox is available
# ---------------------------------------------------------------------------

def _inbox_conn(inbox_db):
    """Open the inbox read-only, or return None if it is unavailable."""
    if not inbox_db or not os.path.exists(inbox_db):
        return None
    try:
        return connect(inbox_db, read_only=True)
    except sqlite3.Error:
        return None


def _ledger(conn) -> dict:
    return {r["batch_id"]: dict(r) for r in conn.execute(
        "SELECT batch_id, seq, state, error, applied_at "
        "FROM published_batches")}


def status_report(publish_db, inbox_db=None) -> dict:
    """Snapshot + batch progress.  Read-only on both databases.

    With the inbox stopped or absent, the publish side still reports the
    snapshot and the committed batch states; receive progress is then marked
    unavailable instead of breaking the report.
    """
    pub = connect(publish_db, read_only=True)
    inbox = _inbox_conn(inbox_db)
    try:
        snap = _snapshot_dict(pub)
        ledger = _ledger(pub)
        inbox_state = "available" if inbox is not None else "unavailable"
        if inbox is not None:
            batches = []
            for row in inbox.execute("SELECT * FROM batches ORDER BY seq"):
                info = archive.batch_status(inbox, row["batch_id"])
                led = ledger.get(row["batch_id"])
                if led is not None:
                    # the publish ledger is the committed truth
                    info["state"] = led["state"]
                    info["error"] = led["error"]
                    if led["applied_at"]:
                        info["applied_at"] = led["applied_at"]
                batches.append(info)
            seen = {b["batch"] for b in batches}
            for b in ledger.values():          # ledger rows the inbox lost
                if b["batch_id"] not in seen:
                    batches.append({"batch": b["batch_id"], "seq": b["seq"],
                                    "state": b["state"], "error": b["error"],
                                    "applied_at": b["applied_at"]})
            batches.sort(key=lambda b: b["seq"])
            blocker = snapshot._blocker(inbox, pub)
        else:
            batches = [{"batch": b["batch_id"], "seq": b["seq"],
                        "state": b["state"], "error": b["error"],
                        "applied_at": b["applied_at"]}
                       for b in sorted(ledger.values(), key=lambda b: b["seq"])]
            blocker = {"reason": "inbox_unavailable",
                       "detail": "receive progress unknown; the inbox "
                                 "database is stopped or absent"}
        return {
            "snapshot": snap,
            "applied_seq": snap["applied_seq"],
            "inbox": inbox_state,
            "batches": batches,
            "publish_blocker": blocker,
        }
    finally:
        pub.close()
        if inbox is not None:
            inbox.close()


def batch_detail_report(publish_db, batch_id: str, inbox_db=None) -> dict:
    pub = connect(publish_db, read_only=True)
    try:
        effects = {r["kind"]: r["n"] for r in pub.execute(
            "SELECT kind,n FROM batch_effects WHERE batch_id=? "
            "ORDER BY kind", (batch_id,))}
        led = _ledger(pub).get(batch_id)
    finally:
        pub.close()

    inbox = _inbox_conn(inbox_db)
    info = None
    if inbox is not None:
        try:
            info = archive.batch_status(inbox, batch_id)
            if info is not None:
                info["segments_detail"] = [dict(r) for r in inbox.execute(
                    "SELECT seg_id,part,parts,nbytes,sha256,path,received_at "
                    "FROM segments WHERE batch_id=? ORDER BY part",
                    (batch_id,))]
        finally:
            inbox.close()
    if info is None:
        if led is None:
            raise KeyError(batch_id)
        info = {"batch": led["batch_id"], "seq": led["seq"],
                "state": led["state"], "error": led["error"],
                "inbox": "unavailable"}
    elif led is not None:
        info["state"] = led["state"]
        info["error"] = led["error"]
        if led["applied_at"]:
            info["applied_at"] = led["applied_at"]
    info["effects"] = effects
    return info


def _snapshot_dict(conn) -> dict:
    from . import tracing
    return tracing.snapshot_info(conn)


def serve_in_thread(publish_db, inbox_db=None, data_dir="", host="127.0.0.1",
                    port=0):
    """Start the query API in a background thread (tests / demo)."""
    from .server import make_server
    srv = make_server(publish_db, inbox_db=inbox_db, data_dir=data_dir,
                      host=host, port=port)
    t = threading.Thread(target=srv.serve_forever, daemon=True)
    t.start()
    return srv, t
