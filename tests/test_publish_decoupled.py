"""Decoupled acceptance and crash-safe publishing under a big apply.

The scenario the field needs (offline: local files and processes only):

* a large batch is being applied and has already written part of its changes
  inside the open network transaction -- the apply is paused there;
* a *later* valid segment arrives from another shipper in that window; it is
  landed and acknowledged on its own, and the source may drop its copy;
* tracing keeps reading the previous complete snapshot throughout;
* the sync process is then hard-killed, and a restart against an *empty*
  delivery directory still publishes every acknowledged batch in order;
* a missing part or a rejection still blocks later batches.
"""

from __future__ import annotations

import os
import json
import signal
import sqlite3
import subprocess
import sys
import textwrap
import time
import unittest
from pathlib import Path

from support import ROOT, TempDirTest, chain, taps

from app import db, service, snapshot, tracing


def _wait_for(predicate, *, timeout=25.0, message="condition"):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if predicate():
            return
        time.sleep(0.01)
    raise AssertionError(f"timed out waiting for {message}")


def _wait_for_file(path: Path, timeout=25.0):
    _wait_for(path.exists, timeout=timeout, message=f"file {path.name}")


class PausedMidApplyTest(TempDirTest):
    """Pause after partial changes; ack a later segment; kill; restart."""

    def _segments(self):
        b1 = self.write_batch("B-0001", 1, 3, name="src1", items={
            "taps": taps("J-000001", "J-000002"),
            "lays": [{"code": "P-000001", "from": "J-000001",
                      "to": "J-000002"}]})
        b2 = self.write_batch("B-0002", 2, 3, name="src2", parts=2, items={
            "taps": taps(*[f"J-{i:06d}" for i in range(10, 31)]),
            "lays": chain(10, 30)})
        b3 = self.write_batch("B-0003", 3, 3, name="src3", items={
            "taps": taps("J-000050"),
            "lays": [{"code": "P-005000", "from": "J-000050",
                      "to": "J-000010"}]})
        return b1, b2, b3

    def test_ack_while_paused_then_kill_and_republish(self):
        db_path = self.work / "drain.db"
        data = self.work / "data"
        marker = self.work / "paused.marker"
        b1, b2, b3 = self._segments()

        # Stage everything; publish only B-0001 so B-0002 is ready and next.
        setup = service.SyncService(db_path, data, auto_publish=False)
        setup.accept(b1[0])
        setup.publish()
        self.assertEqual(setup.status()["snapshot"]["published_batch"], "B-0001")
        for p in b2:
            setup.accept(p)
        self.assertEqual(setup.batch_detail("B-0002")["parts_received"], [0, 1])

        # Start the "big applier" as a *separate process*.  It pauses inside
        # B-0002's network transaction after the taps section is written.
        script = textwrap.dedent(f"""
            import sys, time; sys.path.insert(0, {str(ROOT)!r})
            from app import service, snapshot
            def hook(section, net):
                seq = int(net.execute(
                    "SELECT CAST(value AS INTEGER) FROM meta "
                    "WHERE key='applied_seq'").fetchone()[0] or 0)
                if section == "taps" and seq == 1:
                    n = net.execute("SELECT COUNT(*) FROM joints").fetchone()[0]
                    open({str(marker)!r}, "w").write(str(n))
                    time.sleep(300)          # hold the apply open, uncommitted
            snapshot.progress_hook = hook
            svc = service.SyncService({str(db_path)!r}, {str(data)!r},
                                      auto_publish=False)
            svc.publish()
            print("UNREACHABLE", flush=True)
        """)
        child = subprocess.Popen([sys.executable, "-I", "-c", script],
                                 stdout=subprocess.PIPE,
                                 stderr=subprocess.PIPE, text=True)
        try:
            _wait_for_file(marker)
            # The applying connection sees its uncommitted B-0002 taps; the
            # published snapshot still reports B-0001 with only 2 joints.
            self.assertEqual(marker.read_text().strip(), str(2 + 21))
            self.assertEqual(tracing.trace_upstream(
                db_path, "J-000002")["snapshot"]["published_batch"], "B-0001")
            self.assertEqual(tracing.trace_upstream(
                db_path, "J-000002")["snapshot"]["joints"], 2)

            # A later valid segment arrives from ANOTHER shipper process in
            # exactly this window.  It must be landed and acknowledged without
            # waiting for the paused apply.
            shipper_script = textwrap.dedent(f"""
                import sys, json; sys.path.insert(0, {str(ROOT)!r})
                from app import service
                svc = service.SyncService({str(db_path)!r}, {str(data)!r})
                out = svc.accept({str(b3[0])!r})
                print(json.dumps(out), flush=True)
            """)
            started = time.time()
            shipper = subprocess.run(
                [sys.executable, "-I", "-c", shipper_script],
                capture_output=True, text=True, timeout=60)
            elapsed = time.time() - started
            self.assertEqual(shipper.returncode, 0, shipper.stderr)
            acked = json.loads(shipper.stdout.strip().splitlines()[-1])
            self.assertEqual(acked["status"], "accepted")
            self.assertTrue(acked["acknowledged"])
            self.assertLess(elapsed, 20.0)
            # publishing was deferred (busy), but the acknowledgement stands
            self.assertEqual(acked["publish"]["applied"], [])
            self.assertEqual(acked["publish"]["stopped"]["reason"], "busy")

            # The source drops its copy: the acknowledgement is durable.  Keep
            # the bytes aside so the test can prove redelivery is idempotent.
            dropped_bytes = b3[0].read_bytes()
            b3[0].unlink()

            # Tracing still reads the previous complete snapshot.
            still = tracing.trace_upstream(db_path, "J-000002")
            self.assertEqual(still["snapshot"]["published_batch"], "B-0001")
            self.assertEqual(still["totals"]["upstream_pipes"], 1)
        finally:
            os.kill(child.pid, signal.SIGKILL)     # hard stop mid-apply
            child.wait()
            child.stdout.close()
            child.stderr.close()

        # The half-written B-0002 was rolled back by the kill: the network is
        # still exactly B-0001, and the acked B-0003 segment survives.
        self.assertEqual(tracing.trace_upstream(
            db_path, "J-000002")["snapshot"]["published_batch"], "B-0001")

        # Restart against an EMPTY delivery directory (the source dropped the
        # segment).  Every acknowledged batch must now publish, in order.
        empty = self.work / "empty-drop"
        empty.mkdir()
        out = subprocess.run(
            [sys.executable, "-m", "app", "sync", "--db", str(db_path),
             "--data", str(data), "--from-source", str(empty)],
            cwd=ROOT, capture_output=True, text=True, timeout=120)
        self.assertEqual(out.returncode, 0, out.stderr)

        fresh = service.SyncService(db_path, data, auto_publish=False)
        st = fresh.status()
        self.assertEqual(st["snapshot"]["published_batch"], "B-0003")
        self.assertEqual(st["applied_seq"], 3)
        self.assertEqual([b["state"] for b in st["batches"]],
                         ["applied", "applied", "applied"])

        # The dropped segment is known: redelivery is a duplicate, not a
        # request to resend.
        b3[0].write_bytes(dropped_bytes)              # source re-sends it anyway
        again = fresh.accept(b3[0])
        self.assertEqual(again["status"], "duplicate")
        self.assertFalse(again["acknowledged"])
        self.assertEqual(again["segment"], acked["segment"])

        # And the later batches' effects are in the published network.
        res = tracing.trace_upstream(db_path, "J-000010")   # P-005000 flows here
        self.assertEqual(res["snapshot"]["published_batch"], "B-0003")
        self.assertIn("P-005000", {p["code"] for p in res["pipes"]})
        # B-0002's chain is there too: P-000010 feeds J-000011
        chain_res = tracing.trace_upstream(db_path, "J-000011")
        self.assertIn("P-000010", {p["code"] for p in chain_res["pipes"]})

    def test_restart_with_empty_drop_dir_publishes_in_order(self):
        db_path = self.work / "drain.db"
        data = self.work / "data"
        b1, b2, b3 = self._segments()
        setup = service.SyncService(db_path, data, auto_publish=False)
        setup.accept(b1[0])
        for p in b2 + b3:
            setup.accept(p)
        self.assertIsNone(setup.status()["snapshot"]["published_batch"])

        empty = self.work / "empty-drop"
        empty.mkdir()
        out = subprocess.run(
            [sys.executable, "-m", "app", "sync", "--db", str(db_path),
             "--data", str(data), "--from-source", str(empty)],
            cwd=ROOT, capture_output=True, text=True, timeout=120)
        self.assertEqual(out.returncode, 0, out.stderr)
        self.assertIn("recovered B-0001", out.stdout)
        self.assertIn("recovered B-0002", out.stdout)
        self.assertIn("recovered B-0003", out.stdout)

        st = service.SyncService(db_path, data, auto_publish=False).status()
        self.assertEqual(st["applied_seq"], 3)
        self.assertEqual([b["state"] for b in st["batches"]],
                         ["applied"] * 3)


class BlockingPublishTest(TempDirTest):
    """Missing parts and rejections still hold later batches back."""

    def test_missing_part_or_rejection_still_blocks_later_publish(self):
        svc = self.make_svc()                       # auto-publish
        b1 = self.write_batch("B-0001", 1, 4, name="src1", items={
            "taps": taps("J-000001", "J-000002"),
            "lays": [{"code": "P-000001", "from": "J-000001",
                      "to": "J-000002"}]})
        b2 = self.write_batch("B-0002", 2, 4, name="src2", parts=2, items={
            "taps": taps("J-000003")})
        b3 = self.write_batch("B-0003", 3, 4, name="src3", items={
            "taps": taps("J-000004")})
        b4 = self.write_batch("B-0004", 4, 4, name="src4", items={
            "taps": taps("J-000005")})
        svc.accept(b1[0])                          # publishes B-0001
        svc.accept(b2[0])                          # B-0002 part 0 only
        for p in b3 + b4:
            svc.accept(p)
        st = svc.status()
        self.assertEqual(st["snapshot"]["published_batch"], "B-0001")
        self.assertEqual(st["publish_blocker"]["reason"], "waiting_for_parts")
        self.assertEqual(st["publish_blocker"]["missing_parts"], [1])

        # Supplying the missing part unblocks only in order.
        svc.accept(b2[1])
        st = svc.status()
        self.assertEqual(st["snapshot"]["published_batch"], "B-0004")
        self.assertEqual([b["state"] for b in st["batches"]],
                         ["applied"] * 4)

        # Now a rejection: it halts later batches even when complete.
        b5 = self.write_batch("B-0005", 5, 6, name="src5", items={
            "taps": taps("J-000006")})
        bad = self.write_batch("B-0006", 6, 6, name="src6", items={
            "lays": [{"code": "P-009999", "from": "J-000006",
                      "to": "J-099999"}]})
        svc.accept(b5[0])
        svc.accept(bad[0])
        st = svc.status()
        self.assertEqual(st["snapshot"]["published_batch"], "B-0005")
        self.assertEqual(st["publish_blocker"]["reason"], "rejected")
        self.assertEqual(st["publish_blocker"]["batch"], "B-0006")
        self.assertEqual({b["batch"]: b["state"] for b in st["batches"]},
                         {"B-0001": "applied", "B-0002": "applied",
                          "B-0003": "applied", "B-0004": "applied",
                          "B-0005": "applied", "B-0006": "rejected"})


class RecoveryAfterNetworkCommitTest(TempDirTest):
    """A kill between the network commit and the inbox update is repaired."""

    def test_published_batch_reported_even_if_inbox_lags(self):
        db_path = self.work / "drain.db"
        data = self.work / "data"
        svc = service.SyncService(db_path, data, auto_publish=False)
        segs = self.write_batch("B-0001", 1, 1, items={
            "taps": taps("J-000001", "J-000002"),
            "lays": [{"code": "P-000001", "from": "J-000001",
                      "to": "J-000002"}]})
        svc.accept(segs[0])

        # Simulate the crash window: apply to the network, then leave the inbox
        # batch state untouched (as a kill just after COMMIT would).
        orig = snapshot._mark_applied
        snapshot._mark_applied = lambda conn, row: None
        try:
            svc.publish()
        finally:
            snapshot._mark_applied = orig
        inbox = sqlite3.connect(db_path)
        try:
            self.assertEqual(inbox.execute(
                "SELECT state FROM batches WHERE batch_id='B-0001'"
            ).fetchone()[0], "staging")
        finally:
            inbox.close()

        st = svc.status()        # read-only reporting must be honest
        self.assertEqual(st["snapshot"]["published_batch"], "B-0001")
        self.assertEqual(st["batches"][0]["state"], "applied")
        self.assertEqual(st["applied_seq"], 1)

        # A later publisher reconciles the stored state for good.
        svc.publish()
        self.assertEqual(svc.batch_detail("B-0001")["state"], "applied")
        # and re-publishing does not apply it twice
        self.assertEqual(svc.publish()["applied"], [])


if __name__ == "__main__":
    unittest.main()
