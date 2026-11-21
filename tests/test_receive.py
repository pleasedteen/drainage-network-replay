"""Receiving: out-of-order arrival, duplicates, rejection, crash safety."""

from __future__ import annotations

import json
import os
import signal
import sqlite3
import subprocess
import sys
import textwrap
import unittest
from pathlib import Path

from support import ROOT, TempDirTest, chain, taps

from app import archive, service, wire


def basic_items(n_from=1, n_to=4):
    return {"taps": taps(*[f"J-{i:06d}" for i in range(n_from, n_to + 1)]),
            "lays": chain(n_from, n_to)}


class ReceivingTest(TempDirTest):
    def test_out_of_order_parts_publish_only_when_complete(self):
        svc = self.make_svc()
        segs = self.write_batch("B-0001", 1, 2, parts=2, items=basic_items(1, 6))
        self.assertEqual(len(segs), 2)
        svc.accept(segs[1])                      # part 1 before part 0
        st = svc.status()
        self.assertIsNone(st["snapshot"]["published_batch"])
        self.assertEqual(st["publish_blocker"]["reason"], "waiting_for_parts")
        self.assertEqual(st["publish_blocker"]["missing_parts"], [0])
        out = svc.accept(segs[0])
        self.assertEqual(out["publish"]["applied"][0]["batch"], "B-0001")
        st = svc.status()
        self.assertEqual(st["snapshot"]["published_batch"], "B-0001")
        self.assertEqual((st["snapshot"]["joints"], st["snapshot"]["pipes"]),
                         (6, 5))

    def test_batches_publish_in_seq_order_regardless_of_arrival(self):
        svc = self.make_svc()
        b1 = self.write_batch("B-0001", 1, 3, items=basic_items(1, 4))
        b2 = self.write_batch("B-0002", 2, 3, items={"taps": taps("J-000010")},
                              name="src2")
        b3 = self.write_batch("B-0003", 3, 3, items={"taps": taps("J-000011")},
                              name="src3")
        for p in (b3[0], b2[0], b1[0]):          # newest arrives first
            svc.accept(p)
        st = svc.status()
        self.assertEqual(st["snapshot"]["published_batch"], "B-0003")
        self.assertEqual(st["applied_seq"], 3)
        self.assertEqual([b["state"] for b in st["batches"]],
                         ["applied"] * 3)

    def test_duplicate_delivery_is_a_no_op(self):
        svc = self.make_svc()
        segs = self.write_batch("B-0001", 1, 2, parts=2, items=basic_items(1, 5))
        first = svc.accept(segs[0])
        again = svc.accept(segs[0])
        self.assertEqual(first["status"], "accepted")
        self.assertEqual(again["status"], "duplicate")
        self.assertFalse(again["acknowledged"])
        self.assertEqual(again["digest"], first["digest"])

        data = json.loads(segs[1].read_text())
        p1 = self.segment_file("v1.json", data)
        p2 = self.segment_file("v2.json", data, subdir="other")
        p2.write_text(json.dumps(data, indent=2))
        self.assertEqual(svc.accept(p1)["status"], "accepted")
        self.assertEqual(svc.accept(p2)["status"], "duplicate")
        st = svc.status()
        self.assertEqual(st["snapshot"]["published_batch"], "B-0001")
        self.assertEqual(st["batches"][0]["segments"], 2)

    def test_conflicting_reuse_of_a_part_is_rejected(self):
        svc = self.make_svc()
        segs = self.write_batch("B-0001", 1, 2, parts=2, items=basic_items(1, 4))
        for p in segs:
            svc.accept(p)
        bad = self.segment("other-p0", "B-0001", 1, 2, part=0, parts=2,
                           taps=taps("J-000001"))
        with self.assertRaises(archive.LandError):
            svc.accept(bad)
        st = svc.status()
        self.assertEqual(st["batches"][0]["parts_received"], [0, 1])
        self.assertEqual(st["snapshot"]["joints"], 4)   # nothing leaked

    def test_two_batch_ids_may_not_claim_one_seq(self):
        svc = self.make_svc()
        segs = self.write_batch("B-0001", 1, 2, items=basic_items(1, 4))
        svc.accept(segs[0])
        clash = self.segment("clash", "B-0999", 1, 2, part=0,
                             taps=taps("J-000777"))
        with self.assertRaises(archive.LandError) as ctx:
            svc.accept(clash)
        self.assertIn("claims seq 1", str(ctx.exception))
        self.assertEqual([b["batch"] for b in svc.status()["batches"]],
                         ["B-0001"])

    def test_invalid_items_reject_the_whole_segment(self):
        svc = self.make_svc()
        cases = [
            ({"lays": [{"code": "P-1", "from": "J-000001", "to": "J-000002"}]},
             "bad pipe code"),
            ({"lays": [{"code": "P-000001", "from": "J-000001"}]}, "needs 'to'"),
            ({"taps": [{"ref": "not-an-id"}]}, "bad reference"),
            ({"taps": [{"ref": "J-000001", "zone": "C4"}]}, "both or neither"),
            ({"retires": [{"kind": "bogus", "ref": "J-000001"}]},
             "'kind' must be"),
            ({"ids": [{"old": "C4:1", "new": "C4:2"}]}, "'old' must be a legacy"),
            ({"taps": [{"ref": "J-000001", "colour": "red"}]}, "unknown key"),
        ]
        for i, (mutation, needle) in enumerate(cases):
            seg = self.segment(f"x{i}", "B-0001", 1, 1, **mutation)
            with self.assertRaises(wire.SegmentError, msg=str(mutation)) as ctx:
                svc.accept(seg)
            self.assertIn(needle, str(ctx.exception), msg=str(mutation))
        self.assertEqual(svc.status()["batches"], [])

    def test_malformed_json_does_not_acknowledge(self):
        svc = self.make_svc()
        p = self.work / "trunc.json"
        p.write_text('{"segment":"t","batch":"B-1","seq":1,"total":1,'
                     '"part":0,"parts":1,"upgrade":false,"taps":[{"ref":')
        with self.assertRaises(wire.SegmentError):
            svc.accept(p)
        self.assertEqual(svc.status()["batches"], [])

    def test_trailing_garbage_is_rejected(self):
        svc = self.make_svc()
        p = self.work / "trail.json"
        p.write_text('{"segment":"t","batch":"B-1","seq":1,"total":1,"part":0,'
                     '"parts":1,"upgrade":false}{"x":1}')
        with self.assertRaises(wire.SegmentError):
            svc.accept(p)

    def test_ack_survives_hard_kill_and_restart(self):
        inbox, publish = self.inbox_db, self.publish_db
        data = self.work / "data"
        segs = self.write_batch("B-0001", 1, 2, parts=2, items=basic_items(1, 6))
        script = textwrap.dedent(f"""
            import sys; sys.path.insert(0, {str(ROOT)!r})
            from app import service
            svc = service.SyncService({str(inbox)!r}, {str(publish)!r},
                                      {str(data)!r}, auto_publish=False)
            print(svc.accept({str(segs[0])!r})["status"], flush=True)
            import time; time.sleep(60)
        """)
        proc = subprocess.Popen([sys.executable, "-I", "-c", script],
                                stdout=subprocess.PIPE, text=True)
        try:
            line = proc.stdout.readline().strip()
            self.assertEqual(line, "accepted")
        finally:
            os.kill(proc.pid, signal.SIGKILL)
            proc.wait()

        svc = service.SyncService(inbox, publish, data)
        self.assertEqual(svc.accept(segs[0])["status"], "duplicate")
        self.assertEqual(svc.status()["batches"][0]["parts_received"], [0])
        svc.accept(segs[1])
        st = svc.status()
        self.assertEqual(st["snapshot"]["published_batch"], "B-0001")
        self.assertEqual(st["snapshot"]["joints"], 6)

    def test_crash_during_staging_leaves_no_partial_state(self):
        inbox, publish = self.inbox_db, self.publish_db
        data = self.work / "data"
        seg = self.write_batch("B-0001", 1, 1, items=basic_items(1, 2000))[0]

        orig = archive._stage

        def boom(path, scan, conn, **kw):
            raise RuntimeError("simulated power loss")

        archive._stage = boom
        try:
            svc = service.SyncService(inbox, publish, data)
            with self.assertRaises(RuntimeError):
                svc.accept(seg)
        finally:
            archive._stage = orig

        conn = sqlite3.connect(inbox)
        try:
            for table in ("segments", "st_taps", "st_lays", "batches"):
                n = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
                self.assertEqual(n, 0, table)
        finally:
            conn.close()

        svc = service.SyncService(inbox, publish, data)
        self.assertEqual(svc.accept(seg)["status"], "accepted")
        self.assertEqual(svc.status()["snapshot"]["joints"], 2000)

    def test_rejection_blocks_later_batches_but_keeps_snapshot(self):
        svc = self.make_svc()
        self.write_batch("B-0001", 1, 3, items=basic_items(1, 4))
        bad = self.write_batch("B-0002", 2, 3, items={
            "lays": [{"code": "P-090000", "from": "J-000004",
                      "to": "J-099999"}]}, name="src2")
        good = self.write_batch("B-0003", 3, 3,
                                items={"taps": taps("J-000005")}, name="src3")
        for p in (self.work / "src").glob("B-0001-*.json"):
            svc.accept(p)
        svc.accept(bad[0])
        svc.accept(good[0])
        st = svc.status()
        self.assertEqual(st["snapshot"]["published_batch"], "B-0001")
        self.assertEqual(st["publish_blocker"]["reason"], "rejected")
        self.assertEqual(st["publish_blocker"]["batch"], "B-0002")
        self.assertIn("unknown joint", st["publish_blocker"]["error"])
        self.assertEqual({b["batch"]: b["state"] for b in st["batches"]},
                         {"B-0001": "applied", "B-0002": "rejected",
                          "B-0003": "staging"})


if __name__ == "__main__":
    unittest.main()
