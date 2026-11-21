"""Split-storage behaviour: legacy migration, resumable publish, acks during
publish, write volume proportional to the change, hard-kill recovery, and
queries that survive an inbox outage.
"""

from __future__ import annotations

import json
import os
import signal
import sqlite3
import subprocess
import sys
import textwrap
import threading
import time
import unittest
import urllib.request
from pathlib import Path

from support import ROOT, TempDirTest, chain, taps

from app import migrate, replay, service, snapshot, tracing
from app.db import connect
from app.server import make_server


def big_segment(path, n_joints, n_pipes, batch="B-BIG", seq=1, total=1,
                part=0, parts=1):
    def joint_items():
        for i in range(n_joints):
            yield {"ref": f"J-{i:06d}", "attrs": {"kind": "污水", "n": i}}

    def pipe_items():
        for i in range(n_pipes):
            yield {"code": f"P-{i:06d}", "from": f"J-{i % n_joints:06d}",
                   "to": f"J-{(i * 7919 + 13) % n_joints:06d}",
                   "attrs": {"dn": 300}}

    return replay.write_streamed_segment(
        path,
        {"segment": f"{batch}-p{part}", "batch": batch, "seq": seq,
         "total": total, "part": part, "parts": parts, "upgrade": False,
         "producer": "split-test"},
        {"taps": joint_items, "lays": pipe_items,
         "ids": [], "redirects": [], "retires": []})


class MigrationTest(TempDirTest):
    def _build_legacy_db(self):
        """A realistic old single database: two applied batches, one batch
        fully received but unpublished, one acknowledged but missing a part."""
        svc = self.make_svc()                       # auto-publish
        b1 = self.write_batch("B-0001", 1, 4, items={
            "taps": taps(*[f"J-{i:06d}" for i in range(1, 8)]),
            "lays": chain(1, 7)})
        b2 = self.write_batch("B-0002", 2, 4, upgrade=True, items={
            "ids": [{"old": f"J-{i:06d}", "new": f"Q4:{i:05d}"}
                    for i in range(1, 8)]})
        for group in (b1, b2):
            for p in group:
                svc.accept(p)
        svc.auto_publish = False
        b3 = self.write_batch("B-0003", 3, 4, parts=2, name="src3", items={
            "taps": [{"ref": "Q4:00001", "attrs": {"note": "已收齐未发布"}}]})
        b4 = self.write_batch("B-0004", 4, 4, parts=2, name="src4", items={
            "taps": [{"ref": "Q4:00002", "attrs": {"note": "缺一片"}}]})
        for p in b3:
            svc.accept(p)                           # complete, never published
        svc.accept(b4[0])                           # part 0 of 2 only
        legacy = self.work / "legacy.db"
        migrate.build_legacy_from_split(self.inbox_db, self.publish_db, legacy)
        return legacy, b3, b4

    def test_migrate_preserves_snapshot_mapping_topology_and_basis(self):
        legacy, _, _ = self._build_legacy_db()
        report = migrate.migrate_legacy(
            legacy, self.work / "m-inbox.db", self.work / "m-publish.db")
        self.assertTrue(report["ok"], migrate.format_report(report))
        checks = report["checks"]
        self.assertEqual(checks["snapshot"]["migrated"]["published_batch"],
                         "B-0002")
        self.assertEqual([e["batch"] for e in checks["resume"]
                          ["staged_complete"]], ["B-0003"])
        self.assertEqual([e["batch"] for e in checks["resume"]
                          ["staged_partial"]], ["B-0004"])
        # the legacy database itself is untouched
        self.assertTrue(Path(legacy).exists())
        # refusing to overwrite existing targets
        with self.assertRaises(migrate.MigrationError):
            migrate.migrate_legacy(legacy, self.work / "m-inbox.db",
                                   self.work / "m-publish.db")

    def test_migrated_pair_resumes_unpublished_and_partial_batches(self):
        legacy, b3, b4 = self._build_legacy_db()
        migrate.migrate_legacy(legacy, self.work / "m-inbox.db",
                               self.work / "m-publish.db")
        svc = service.SyncService(self.work / "m-inbox.db",
                                  self.work / "m-publish.db",
                                  self.work / "data")
        # 已收齐未发布: B-0003 publishes from its staged rows, no redelivery
        rep = svc.publish()
        self.assertEqual([b["batch"] for b in rep["applied"]], ["B-0003"])
        st = svc.status()
        self.assertEqual(st["snapshot"]["published_batch"], "B-0003")
        self.assertEqual(st["publish_blocker"]["reason"], "waiting_for_parts")
        self.assertEqual(st["publish_blocker"]["missing_parts"], [1])
        # 已确认仍缺片: only the missing part has to arrive
        out = svc.accept(b4[1])
        self.assertEqual(out["publish"]["applied"][0]["batch"], "B-0004")
        st = svc.status()
        self.assertEqual(st["snapshot"]["published_batch"], "B-0004")
        self.assertEqual(st["publish_blocker"]["reason"], "up_to_date")
        # the network is exactly what the old database would have produced
        res = tracing.trace_upstream(svc.publish_db, "Q4:00007")
        self.assertEqual(res["totals"]["upstream_joints"], 7)
        self.assertEqual([b["batch"] for b in
                          res["basis"]["referenced_batches"]],
                         ["B-0001", "B-0002", "B-0003", "B-0004"])
        joint = svc.status()
        self.assertEqual(joint["snapshot"]["joints"], 7)


class ReconcileTest(TempDirTest):
    def test_publish_commit_without_inbox_mark_recovers_once(self):
        """Crash between the publish commit and the inbox bookkeeping."""
        svc = self.make_svc(auto_publish=False)
        seg = self.write_batch("B-0001", 1, 1, items={
            "taps": taps("J-000001", "J-000002"), "lays": chain(1, 2)})[0]
        svc.accept(seg)

        # apply directly and commit the publish db, then "die" before the
        # inbox row is updated
        inbox, pub = connect(self.inbox_db), connect(self.publish_db)
        row = inbox.execute("SELECT * FROM batches WHERE batch_id='B-0001'"
                            ).fetchone()
        pub.execute("BEGIN IMMEDIATE")
        snapshot.apply_batch(inbox, pub, row)
        pub.execute("COMMIT")
        inbox.close()
        pub.close()
        # inbox still says staging while the network already reflects B-0001
        raw = sqlite3.connect(self.inbox_db)
        self.assertEqual(raw.execute(
            "SELECT state FROM batches WHERE batch_id='B-0001'"
        ).fetchone()[0], "staging")
        raw.close()

        # restart: reconciliation aligns the inbox with the committed truth
        svc2 = service.SyncService(self.inbox_db, self.publish_db,
                                   self.work / "data", auto_publish=False)
        st = svc2.status()
        self.assertEqual(st["batches"][0]["state"], "applied")
        self.assertEqual(st["snapshot"]["published_batch"], "B-0001")
        # publishing again must not re-apply: the effect happens once
        rep = svc2.publish()
        self.assertEqual(rep["applied"], [])
        self.assertEqual(rep["stopped"]["reason"], "up_to_date")
        st = svc2.status()
        self.assertEqual((st["snapshot"]["joints"], st["snapshot"]["pipes"]),
                         (2, 1))
        detail = svc2.batch_detail("B-0001")
        self.assertEqual(detail["effects"].get("tap_created"), 2)


class AckDuringPublishTest(TempDirTest):
    def test_segment_is_acknowledged_while_a_big_batch_is_published(self):
        svc = self.make_svc(auto_publish=False)
        big = big_segment(self.work / "big.json", 40000, 40000)
        svc.accept(big)                             # staged, not publishing
        small = self.segment("s1", "B-0002", 2, 2, part=0,
                             taps=taps("J-090000"))

        done = {}
        barrier = threading.Barrier(2)

        def publisher():
            barrier.wait()
            done["t0"] = time.monotonic()
            done["report"] = svc.publish()
            done["t1"] = time.monotonic()

        t = threading.Thread(target=publisher)
        t.start()
        barrier.wait()                              # both start together
        svc2 = service.SyncService(self.inbox_db, self.publish_db,
                                   self.work / "data", auto_publish=False)
        out = svc2.accept(small)
        acked_at = time.monotonic()
        t.join(timeout=120)

        self.assertTrue(out["acknowledged"])
        # the ack landed while the big publish was still running ...
        self.assertLess(acked_at, done["t1"])
        self.assertLess(acked_at - done["t0"], 5.0)   # ... and fast
        self.assertEqual(done["report"]["applied"][0]["batch"], "B-BIG")
        # the ack is durable and the small batch publishes afterwards
        svc3 = service.SyncService(self.inbox_db, self.publish_db,
                                   self.work / "data", auto_publish=False)
        self.assertEqual(svc3.accept(small)["status"], "duplicate")
        svc3.publish()
        st = svc3.status()
        self.assertEqual(st["snapshot"]["joints"], 40001)
        self.assertEqual(st["snapshot"]["published_batch"], "B-0002")


class WriteVolumeTest(TempDirTest):
    def _wal_delta(self, publish_db, fn):
        """WAL bytes written by fn(): the true write volume of one batch."""
        keeper = connect(publish_db)        # keeps the db open between steps
        try:
            keeper.execute("PRAGMA wal_checkpoint(TRUNCATE)")
            wal = str(publish_db) + "-wal"
            before = os.path.getsize(wal) if os.path.exists(wal) else 0
            fn()
            after = os.path.getsize(wal) if os.path.exists(wal) else 0
            return after - before
        finally:
            keeper.close()

    def test_writes_scale_with_the_change_not_the_network(self):
        # a large published network
        svc = self.make_svc()
        svc.accept(big_segment(self.work / "big.json", 40000, 40000))
        self.assertEqual(svc.status()["snapshot"]["joints"], 40000)

        # a tiny batch: one joint attribute change
        tiny1 = self.segment("t1", "B-0002", 2, 4, part=0, taps=[
            {"ref": "J-000001", "attrs": {"note": "一处改动"}}])
        delta_big_net = self._wal_delta(self.publish_db,
                                        lambda: svc.accept(tiny1))

        # the same tiny change on a small network
        svc2 = service.SyncService(self.work / "i2.db", self.work / "p2.db",
                                   self.work / "data2")
        svc2.accept(self.segment("base", "B-0001", 1, 4, part=0,
                                 taps=taps("J-000001", "J-000002")))
        tiny2 = self.segment("t2", "B-0002", 2, 4, part=0, taps=[
            {"ref": "J-000001", "attrs": {"note": "一处改动"}}])
        delta_small_net = self._wal_delta(self.work / "p2.db",
                                          lambda: svc2.accept(tiny2))

        # a medium batch on the big network, for contrast
        medium = self.segment("m1", "B-0003", 3, 4, part=0, taps=taps(
            *[f"J-{i:06d}" for i in range(100, 2100)]))
        delta_medium = self._wal_delta(self.publish_db,
                                       lambda: svc.accept(medium))

        # tiny write volume is small and (nearly) independent of network size
        self.assertLess(delta_big_net, 512 * 1024)
        self.assertLess(delta_small_net, 512 * 1024)
        lo, hi = sorted((delta_big_net, delta_small_net))
        self.assertLess(hi, lo * 4 + 4096)
        # ... while a 2000-joint batch writes clearly more
        self.assertGreater(delta_medium, delta_big_net * 2)


class InboxOutageTest(TempDirTest):
    def test_queries_keep_answering_while_the_inbox_is_stopped(self):
        svc = self.make_svc()
        svc.accept(self.write_batch("B-0001", 1, 1, items={
            "taps": taps(*[f"J-{i:06d}" for i in range(1, 5)]),
            "lays": chain(1, 4)})[0])
        srv = make_server(self.publish_db, inbox_db=self.inbox_db, port=0)
        threading.Thread(target=srv.serve_forever, daemon=True).start()
        self.addCleanup(srv.server_close)
        self.addCleanup(srv.shutdown)
        port = srv.server_address[1]

        def get(path):
            with urllib.request.urlopen(
                    f"http://127.0.0.1:{port}{path}", timeout=10) as r:
                return r.status, json.loads(r.read().decode())

        # stop the inbox: the file is simply gone for the query process
        stopped = self.work / "inbox.db.stopped"
        os.rename(self.inbox_db, stopped)
        try:
            code, snap = get("/v1/snapshot")
            self.assertEqual((code, snap["published_batch"]), (200, "B-0001"))
            code, joint = get("/v1/joint?ref=J-000002")
            self.assertEqual((code, joint["uid"]), (200, 2))
            code, trace = get("/v1/trace/upstream?ref=J-000004")
            self.assertEqual(code, 200)
            self.assertEqual(trace["totals"]["upstream_pipes"], 3)
            self.assertEqual(trace["basis"]["referenced_batches"][0]["batch"],
                             "B-0001")
            code, batches = get("/v1/batches")
            self.assertEqual(code, 200)
            self.assertEqual(batches["inbox"], "unavailable")
            self.assertEqual(batches["publish_blocker"]["reason"],
                             "inbox_unavailable")
            self.assertEqual([b["batch"] for b in batches["batches"]],
                             ["B-0001"])
            code, detail = get("/v1/batches/B-0001")
            self.assertEqual((code, detail["state"]), (200, "applied"))
        finally:
            os.rename(stopped, self.inbox_db)
        code, batches = get("/v1/batches")
        self.assertEqual(batches["inbox"], "available")
        self.assertEqual(batches["batches"][0]["parts_received"], [0])


class HardKillRecoveryTest(TempDirTest):
    def test_kill_mid_publish_recovers_and_applies_exactly_once(self):
        n = 40000
        segs = [big_segment(self.work / "src" / f"big-p{p}.json", n, n,
                            batch="B-BIG", seq=1, total=1, part=p, parts=2)
                for p in (0, 1)]
        inbox, publish = self.inbox_db, self.publish_db
        data = self.work / "data"
        script = textwrap.dedent(f"""
            import sys; sys.path.insert(0, {str(ROOT)!r})
            from app import service
            svc = service.SyncService({str(inbox)!r}, {str(publish)!r},
                                      {str(data)!r}, auto_publish=False)
            for p in {list(map(str, segs))!r}:
                print("ACK", svc.accept(p)["segment"], flush=True)
            print("PUBLISHING", flush=True)
            rep = svc.publish()
            print("PUBLISHED", [b["batch"] for b in rep["applied"]],
                  flush=True)
            import time; time.sleep(60)
        """)
        proc = subprocess.Popen([sys.executable, "-I", "-c", script],
                                stdout=subprocess.PIPE, text=True)
        try:
            self.assertEqual(proc.stdout.readline().strip(), "ACK B-BIG-p0")
            self.assertEqual(proc.stdout.readline().strip(), "ACK B-BIG-p1")
            self.assertEqual(proc.stdout.readline().strip(), "PUBLISHING")
            time.sleep(0.3)          # die inside the apply transaction
        finally:
            os.kill(proc.pid, signal.SIGKILL)
            proc.wait()

        # restart: both acknowledgements are durable, state is reconciled
        svc = service.SyncService(inbox, publish, data, auto_publish=False)
        self.assertEqual(svc.accept(segs[0])["status"], "duplicate")
        self.assertEqual(svc.accept(segs[1])["status"], "duplicate")
        rep = svc.publish()
        st = svc.status()
        # the batch is applied exactly once, however far the child got
        self.assertEqual(st["snapshot"]["joints"], n)
        self.assertEqual(st["snapshot"]["pipes"], n)
        self.assertEqual(st["applied_seq"], 1)
        self.assertEqual(st["batches"][0]["state"], "applied")
        self.assertEqual(st["publish_blocker"]["reason"], "up_to_date")
        detail = svc.batch_detail("B-BIG")
        self.assertEqual(detail["effects"]["tap_created"], n)
        self.assertEqual(detail["effects"]["lay_created"], n)
        # inbox view and publish view agree
        pub = connect(publish, read_only=True)
        try:
            self.assertEqual(pub.execute(
                "SELECT state FROM published_batches WHERE batch_id='B-BIG'"
            ).fetchone()[0], "applied")
        finally:
            pub.close()


if __name__ == "__main__":
    unittest.main()
