"""A large batch in flight while small segments of other batches are delivered.

The requirement this pins down:

* while a big batch is still **staging** and again while it is **applying**, a
  small segment of another batch is delivered and must receive a valid,
  source-cleanable acknowledgement (``acknowledged=true``) within one second;
* the source then deletes the small segment files and the process is hard
  killed;
* after restart the whole set publishes in source order, and traces answer
  from that complete published version.

The big batch is streamed (never held in memory).  The child process drives
the big batch in one thread and delivers the two small segments at the moment
staging / applying is observed, so the timing is measured under real
contention rather than arranged around a sleep.
"""

from __future__ import annotations

import os
import signal
import subprocess
import sys
import textwrap
import unittest

from support import ROOT, TempDirTest, taps

from app import replay, service, tracing

ACK_BUDGET = 1.0          # seconds, per the requirement


def _big_segment(path, n_joints, n_pipes):
    def joint_items():
        for i in range(n_joints):
            yield {"ref": f"J-{i:06d}", "attrs": {"kind": "污水", "n": i}}

    def pipe_items():
        for i in range(n_pipes):
            yield {"code": f"P-{i:06d}",
                   "from": f"J-{i % n_joints:06d}",
                   "to": f"J-{(i * 7919 + 13) % n_joints:06d}",
                   "attrs": {"dn": 300}}

    return replay.write_streamed_segment(
        path,
        {"segment": "BIG-p0", "batch": "B-0001", "seq": 1, "total": 3,
         "part": 0, "parts": 1, "upgrade": False, "producer": "scale-test"},
        {"taps": joint_items, "lays": pipe_items,
         "ids": [], "redirects": [], "retires": []})


class DeliveryDuringLargeBatchTest(TempDirTest):
    def test_small_segments_ack_before_the_big_batch_finishes(self):
        db = self.work / "drain.db"
        data = self.work / "data"
        src = self.work / "src"

        big = _big_segment(src / "big.json", 60000, 60000)

        small2 = replay.write_segments({
            "batch": "B-0002", "seq": 2, "total": 3, "parts": 1,
            "upgrade": False,
            "items": {"taps": taps("J-900001", "J-900002"),
                      "lays": [{"code": "P-900001", "from": "J-900001",
                                "to": "J-900002"}]}},
            src / "small2")[0]
        small3 = replay.write_segments({
            "batch": "B-0003", "seq": 3, "total": 3, "parts": 1,
            "upgrade": False,
            "items": {"taps": taps("J-900003"),
                      "lays": [{"code": "P-900002", "from": "J-900003",
                                "to": "J-900001"}]}},
            src / "small3")[0]

        script = textwrap.dedent(f"""
            import sys, os, threading, time
            sys.path.insert(0, {str(ROOT)!r})
            from app import service, archive, snapshot

            db, data = {str(db)!r}, {str(data)!r}
            big = {str(big)!r}
            small2 = {str(small2)!r}
            small3 = {str(small3)!r}

            stage_started = threading.Event()
            apply_started = threading.Event()

            _stage = archive._stage
            def stage(*a, **kw):
                stage_started.set()
                return _stage(*a, **kw)
            archive._stage = stage

            _apply = snapshot.apply_batch
            def apply(conn, row):
                apply_started.set()
                return _apply(conn, row)
            snapshot.apply_batch = apply

            def bigrun():
                svc = service.SyncService(db, data)
                out = svc.accept(big)
                print("BIG", out["status"], flush=True)

            threading.Thread(target=bigrun, daemon=True).start()

            def deliver(path, label, budget):
                t = time.time()
                svc = service.SyncService(db, data)
                out = svc.accept(path)
                dt = time.time() - t
                print("ACK", label, round(dt, 3), out["status"],
                      out["acknowledged"],
                      ",".join(b["batch"] for b in
                               (out.get("publish") or {{}}).get("applied", []))
                      or "-", flush=True)
                # source may clean up only after a valid acknowledgement
                if out["acknowledged"]:
                    os.unlink(path)
                    print("DELETED", label, flush=True)

            if not stage_started.wait(60):
                raise SystemExit("big batch never staged")
            time.sleep(0.05)
            deliver(small2, "during-staging", {ACK_BUDGET})

            if not apply_started.wait(120):
                raise SystemExit("big batch never applied")
            time.sleep(0.1)
            deliver(small3, "during-apply", {ACK_BUDGET})

            print("READY", flush=True)
            time.sleep(120)
        """)

        proc = subprocess.Popen([sys.executable, "-I", "-c", script],
                                stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                text=True)
        lines = []
        import time as _t
        try:
            end = _t.time() + 180
            while _t.time() < end:
                line = proc.stdout.readline()
                if not line:
                    break
                line = line.strip()
                lines.append(line)
                if line == "READY":
                    break
        finally:
            # hard kill: no chance to flush, clean up or publish politely
            os.kill(proc.pid, signal.SIGKILL)
            stderr = proc.stderr.read()
            proc.wait(timeout=30)
            proc.stdout.close()
            proc.stderr.close()

        acked = [ln for ln in lines if ln.startswith("ACK")]
        self.assertEqual(len(acked), 2, (lines, stderr[-4000:]))
        for ln in acked:
            _, label, latency, status, ack, applied = ln.split(" ", 5)
            self.assertEqual(status, "accepted", ln)
            self.assertEqual(ack, "True", ln)
            self.assertLess(float(latency), ACK_BUDGET, ln)
            # source order: neither small batch may leapfrog the big one
            self.assertEqual(applied, "-", ln)
        self.assertIn("DELETED during-staging", lines)
        self.assertIn("DELETED during-apply", lines)
        self.assertIn("READY", lines)
        # the source really cleaned up both small files
        self.assertFalse(small2.exists())
        self.assertFalse(small3.exists())

        # -- restart: the whole set publishes in source order -----------------
        svc = service.SyncService(db, data)        # resumes publishing on start
        report = svc.publish()
        st = svc.status()
        self.assertEqual(st["snapshot"]["published_batch"], "B-0003")
        self.assertEqual(st["applied_seq"], 3)
        self.assertEqual([b["state"] for b in st["batches"]],
                         ["applied", "applied", "applied"])
        self.assertEqual(report["applied"], [])     # startup had already done it

        # traces answer from the complete version, citing the last batch
        joined = tracing.trace_upstream(str(db), "J-900002")
        self.assertEqual(joined["snapshot"]["published_batch"], "B-0003")
        self.assertEqual(joined["totals"]["upstream_pipes"], 2)
        self.assertEqual({p["code"] for p in joined["pipes"]},
                         {"P-900001", "P-900002"})

        # the big batch is fully present, not half-applied
        self.assertEqual(st["snapshot"]["joints"], 60000 + 3)
        self.assertEqual(st["snapshot"]["pipes"], 60000 + 2)


if __name__ == "__main__":
    unittest.main()
