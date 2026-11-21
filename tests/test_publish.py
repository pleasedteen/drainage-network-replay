"""Publishing is transactional and safe under concurrent publishers."""

from __future__ import annotations

import threading
import unittest

from support import TempDirTest, chain, taps

from app import service, tracing


class PublishTest(TempDirTest):
    def test_two_publishers_apply_a_batch_once(self):
        svc = self.make_svc(auto_publish=False)
        svc.accept(self.write_batch("B-0001", 1, 1, items={
            "taps": taps(*[f"J-{i:06d}" for i in range(1, 31)]),
            "lays": chain(1, 30)})[0])
        reports, errors = [], []
        barrier = threading.Barrier(4)

        def run():
            s = service.SyncService(self.inbox_db, self.publish_db,
                                    self.work / "data", auto_publish=False)
            barrier.wait()
            try:
                reports.append(s.publish())
            except Exception as exc:      # noqa: BLE001
                errors.append(repr(exc))

        threads = [threading.Thread(target=run) for _ in range(4)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=60)
        self.assertEqual(errors, [])
        total_applied = sum(len(r["applied"]) for r in reports)
        self.assertEqual(total_applied, 1)
        st = svc.status()
        self.assertEqual(st["snapshot"]["published_batch"], "B-0001")
        self.assertEqual(st["snapshot"]["joints"], 30)

    def test_restart_publishes_already_staged_batches(self):
        svc = self.make_svc(auto_publish=False)
        self.write_batch("B-0001", 1, 1, items={
            "taps": taps("J-000001", "J-000002"), "lays": chain(1, 2)})
        for p in (self.work / "src").glob("*.json"):
            svc.accept(p)
        self.assertIsNone(svc.status()["snapshot"]["published_batch"])

        restarted = service.SyncService(self.inbox_db, self.publish_db,
                                        self.work / "data")
        self.assertEqual(restarted.publish()["applied"][0]["batch"], "B-0001")
        res = tracing.trace_upstream(restarted.publish_db, "J-000002")
        self.assertEqual(res["totals"]["upstream_pipes"], 1)

    def test_queries_never_see_a_half_applied_batch(self):
        """Roll an apply failure back and check the snapshot is untouched."""
        svc = self.make_svc()
        ok = self.write_batch("B-0001", 1, 2, items={
            "taps": taps("J-000001", "J-000002"), "lays": chain(1, 2)})
        for p in ok:
            svc.accept(p)
        before = tracing.trace_upstream(svc.publish_db, "J-000002")

        # second batch adds a joint then hits a bad pipe reference
        bad = self.write_batch("B-0002", 2, 2, items={
            "taps": taps("J-000003"),
            "lays": [{"code": "P-000100", "from": "J-000003",
                      "to": "J-000404"}]})
        svc.accept(bad[0])
        after = tracing.trace_upstream(svc.publish_db, "J-000002")
        self.assertEqual(before["totals"], after["totals"])
        self.assertEqual(before["snapshot"], after["snapshot"])
        self.assertEqual(svc.status()["snapshot"]["published_batch"], "B-0001")
        self.assertEqual(svc.status()["snapshot"]["joints"], 2)


if __name__ == "__main__":
    unittest.main()
