"""Scale and availability requirements:

* a single batch larger than the sync process' memory is received and applied
  without loading it into memory;
* queries keep working (serving the last complete snapshot) while such a
  batch is being received and applied.
"""

from __future__ import annotations

import os
import resource
import subprocess
import sys
import tempfile
import textwrap
import threading
import time
import unittest
from pathlib import Path

from support import ROOT, TempDirTest, taps

from app import replay, service, tracing


def big_segment(path, n_joints, n_pipes, batch="B-BIG", seq=1, total=1):
    """Stream a segment whose staging text is far larger than the heap cap
    the test process will run with."""
    def joint_items():
        for i in range(n_joints):
            yield {"ref": f"J-{i:06d}",
                   "attrs": {"kind": "污水", "note": "x" * 40, "n": i}}

    def pipe_items():
        for i in range(n_pipes):
            yield {"code": f"P-{i:06d}", "from": f"J-{i % n_joints:06d}",
                   "to": f"J-{(i * 7919 + 13) % n_joints:06d}",
                   "attrs": {"dn": 300}}

    return replay.write_streamed_segment(
        path,
        {"segment": f"{batch}-p0", "batch": batch, "seq": seq, "total": total,
         "part": 0, "parts": 1, "upgrade": False, "producer": "scale-test"},
        {"taps": joint_items, "lays": pipe_items,
         "ids": [], "redirects": [], "retires": []})


class ScaleTest(TempDirTest):
    def test_large_batch_received_and_applied_under_a_small_heap(self):
        """Run a child process with a hard 96 MiB data-segment cap."""
        seg = big_segment(self.work / "src" / "big.json", 40000, 40000)
        size = seg.stat().st_size
        self.assertGreater(size, 6_000_000)

        script = textwrap.dedent(f"""
            import sys; sys.path.insert(0, {str(ROOT)!r})
            from app import service
            svc = service.SyncService({str(self.inbox_db)!r},
                                      {str(self.publish_db)!r},
                                      {str(self.work / 'data')!r})
            out = svc.accept({str(seg)!r})
            st = svc.status()
            print(out['status'], st['snapshot']['joints'],
                  st['snapshot']['pipes'], st['publish_blocker']['reason'],
                  flush=True)
            svc.publish()
            print(svc.status()['snapshot']['published_batch'], flush=True)
        """)
        def limit_memory():
            cap = 96 * 1024 * 1024
            resource.setrlimit(resource.RLIMIT_DATA, (cap, cap))
            os.environ["PYTHONMALLOC"] = "malloc"

        proc = subprocess.run([sys.executable, "-I", "-c", script],
                              capture_output=True, text=True,
                              preexec_fn=limit_memory, timeout=300)
        self.assertEqual(proc.returncode, 0, proc.stderr[-4000:])
        lines = proc.stdout.strip().splitlines()
        self.assertEqual(lines[0].split()[:3], ["accepted", "40000", "40000"])
        self.assertEqual(lines[1], "B-BIG")

    def test_queries_stay_available_while_a_big_batch_is_applied(self):
        svc = self.make_svc()
        base = self.write_batch("B-0001", 1, 2, items={
            "taps": taps("J-000001", "J-000002"),
            "lays": [{"code": "P-000001", "from": "J-000001",
                      "to": "J-000002"}]})
        for p in base:
            svc.accept(p)
        self.assertEqual(svc.status()["snapshot"]["published_batch"], "B-0001")

        seg = big_segment(self.work / "big2.json", 20000, 20000,
                          batch="B-0002", seq=2, total=2)
        errors, results, stop = [], [], threading.Event()

        def reader():
            while not stop.is_set():
                try:
                    res = tracing.trace_upstream(svc.publish_db, "J-000002")
                    results.append((res["snapshot"]["published_batch"],
                                    res["totals"]["upstream_pipes"]))
                except Exception as exc:          # noqa: BLE001
                    errors.append(repr(exc))
                time.sleep(0.005)

        t = threading.Thread(target=reader, daemon=True)
        t.start()
        time.sleep(0.05)
        out = svc.accept(seg)                   # receive + publish big batch
        stop.set()
        t.join()
        self.assertEqual(errors, [])
        self.assertGreater(len(results), 10)
        self.assertEqual({r for r in results if r[0] == "B-0001"},
                         {("B-0001", 1)})
        self.assertTrue(all(r[0] in ("B-0001", "B-0002") for r in results))
        self.assertEqual(out["publish"]["applied"][0]["batch"], "B-0002")
        final = tracing.trace_upstream(svc.publish_db, "J-000002")
        self.assertEqual(final["snapshot"]["published_batch"], "B-0002")

    def test_trace_result_limit_is_enforced(self):
        svc = self.make_svc()
        n = 200
        items = {"taps": taps(*[f"J-{i:06d}" for i in range(n)]),
                 "lays": [{"code": f"P-{i:06d}", "from": f"J-{i:06d}",
                           "to": f"J-{i + 1:06d}"} for i in range(n - 1)]}
        seg = self.write_batch("B-0001", 1, 1, items=items)
        self.deliver = None
        svc.accept(seg[0])
        res = tracing.trace_upstream(svc.publish_db, f"J-{n - 1:06d}",
                                     limit=500)
        self.assertEqual(res["totals"]["upstream_joints"], n)
        with self.assertRaises(tracing.LimitExceeded):
            tracing.trace_upstream(svc.publish_db, f"J-{n - 1:06d}", limit=50)


if __name__ == "__main__":
    unittest.main()
