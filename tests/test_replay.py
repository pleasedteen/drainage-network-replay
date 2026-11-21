"""The offline replay source and the demo data set."""

from __future__ import annotations

import json
import subprocess
import sys
import unittest
from pathlib import Path

from support import ROOT, TempDirTest

from app import replay, tracing


class ReplayTest(TempDirTest):
    def test_demo_data_replays_to_a_single_published_snapshot(self):
        src = self.work / "source"
        replay.generate_demo(src)
        svc = self.make_svc()
        stats = replay.deliver(src, svc, duplicate_every=2, duplicate_seed=3,
                               verbose=False)
        self.assertEqual(stats["rejected"], [])
        self.assertGreater(stats["duplicates"], 0)
        st = svc.status()
        # B-0006 part 1 is held back on purpose
        self.assertEqual(st["snapshot"]["published_batch"], "B-0005")
        self.assertEqual(st["publish_blocker"],
                         {"reason": "waiting_for_parts", "batch": "B-0006",
                          "seq": 6, "missing_parts": [1]})
        self.assertEqual([b["state"] for b in st["batches"]],
                         ["applied"] * 5 + ["staging"])

    def test_demo_answers_are_stable_across_notations(self):
        src = self.work / "source"
        replay.generate_demo(src)
        svc = self.make_svc()
        replay.deliver(src, svc, verbose=False)
        pairs = [("J-001012", "C4:01012"), ("J-001015", "C5:01015"),
                 ("J-001099", "C5:00099"), ("J-001003", "C4:01003")]
        for old, new in pairs:
            a = tracing.trace_upstream(svc.publish_db, old)
            b = tracing.trace_upstream(svc.publish_db, new)
            self.assertEqual(a["totals"], b["totals"], (old, new))
            self.assertEqual(a["joints"], b["joints"], (old, new))
            self.assertEqual(a["basis"], b["basis"], (old, new))

    def test_redelivery_after_full_replay_changes_nothing(self):
        src = self.work / "source"
        replay.generate_demo(src)
        svc = self.make_svc()
        first = replay.deliver(src, svc, verbose=False)
        before = svc.status()
        second = replay.deliver(src, svc, verbose=False)
        self.assertEqual(second["accepted"], [])
        self.assertEqual(second["duplicates"], len(first["accepted"]))
        after = svc.status()
        self.assertEqual(before["snapshot"], after["snapshot"])
        self.assertEqual([b["parts_received"] for b in before["batches"]],
                         [b["parts_received"] for b in after["batches"]])

    def test_cli_sync_then_trace(self):
        src = self.work / "source"
        replay.generate_demo(src)
        inbox, publish = self.inbox_db, self.publish_db
        data = self.work / "data"
        out = subprocess.run(
            [sys.executable, "-m", "app", "sync", "--inbox-db", str(inbox),
             "--publish-db", str(publish), "--data", str(data),
             "--from-source", str(src)],
            cwd=ROOT, capture_output=True, text=True, timeout=120)
        self.assertEqual(out.returncode, 0, out.stderr)
        self.assertIn("published", out.stdout)
        out = subprocess.run(
            [sys.executable, "-m", "app", "trace", "--publish-db",
             str(publish), "--ref", "C4:01012", "--format", "text"],
            cwd=ROOT, capture_output=True, text=True, timeout=60)
        self.assertEqual(out.returncode, 0, out.stderr)
        self.assertIn("上游追溯", out.stdout)
        self.assertIn("C4:01012", out.stdout)
        self.assertIn("B-0005", out.stdout)
        out = subprocess.run(
            [sys.executable, "-m", "app", "status", "--publish-db",
             str(publish), "--inbox-db", str(inbox)],
            cwd=ROOT, capture_output=True, text=True, timeout=60)
        self.assertEqual(json.loads(out.stdout)["snapshot"]["published_batch"],
                         "B-0005")


if __name__ == "__main__":
    unittest.main()
