"""Concurrent delivery: several log shippers may push segments at once."""

from __future__ import annotations

import threading
import unittest

from support import TempDirTest, chain, taps

from app import service, tracing


class ConcurrencyTest(TempDirTest):
    def test_parallel_duplicate_delivery_acknowledges_once(self):
        svc = self.make_svc()
        segs = self.write_batch("B-0001", 1, 1, items={
            "taps": taps(*[f"J-{i:06d}" for i in range(1, 40)]),
            "lays": chain(1, 39)})
        self.assertEqual(len(segs), 1)
        results, errors = [], []
        barrier = threading.Barrier(8)

        def send():
            s = service.SyncService(self.work / "drain.db", self.work / "data")
            barrier.wait()
            try:
                results.append(s.accept(segs[0]))
            except Exception as exc:      # noqa: BLE001
                errors.append(repr(exc))

        threads = [threading.Thread(target=send) for _ in range(8)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=60)
        self.assertEqual(errors, [])
        self.assertEqual(sum(r["status"] == "accepted" for r in results), 1)
        self.assertEqual(sum(r["status"] == "duplicate" for r in results), 7)
        self.assertEqual(len({r["digest"] for r in results}), 1)
        st = svc.status()
        self.assertEqual(st["batches"][0]["segments"], 1)
        self.assertEqual(st["snapshot"]["joints"], 39)

    def test_parallel_delivery_of_different_parts(self):
        svc = self.make_svc()
        segs = self.write_batch("B-0001", 1, 1, parts=6, items={
            "taps": taps(*[f"J-{i:06d}" for i in range(1, 61)]),
            "lays": chain(1, 60)})
        errors = []
        barrier = threading.Barrier(len(segs))

        def send(p):
            s = service.SyncService(self.work / "drain.db", self.work / "data")
            barrier.wait()
            try:
                s.accept(p)
            except Exception as exc:      # noqa: BLE001
                errors.append(f"{p.name}: {exc!r}")

        threads = [threading.Thread(target=send, args=(p,)) for p in segs]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=60)
        self.assertEqual(errors, [])
        st = svc.status()
        self.assertEqual(st["snapshot"]["published_batch"], "B-0001")
        self.assertEqual(st["snapshot"]["joints"], 60)
        self.assertEqual(st["publish_blocker"]["reason"], "up_to_date")
        res = tracing.trace_upstream(svc.db_path, "J-000060")
        self.assertEqual(res["totals"]["upstream_pipes"], 59)


if __name__ == "__main__":
    unittest.main()
