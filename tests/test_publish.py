"""Publishing is transactional and safe under concurrent publishers."""

from __future__ import annotations

import builtins
import json
import os
import signal
import sqlite3
import subprocess
import sys
import textwrap
import threading
import unittest

from support import ROOT, TempDirTest, chain, taps

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
            s = service.SyncService(self.work / "drain.db", self.work / "data",
                                    auto_publish=False)
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

    def test_restart_publishes_confirmed_batches_from_their_archives(self):
        svc = self.make_svc(auto_publish=False)
        segs = self.write_batch("B-0001", 1, 1, items={
            "taps": taps("J-000001", "J-000002"), "lays": chain(1, 2)})
        for p in segs:
            svc.accept(p)
        self.assertIsNone(svc.status()["snapshot"]["published_batch"])

        # the source drops its copies once acknowledged; only the landed
        # archives remain to publish from
        for p in segs:
            p.unlink()

        restarted = service.SyncService(self.work / "drain.db",
                                        self.work / "data")
        self.assertEqual(restarted.publish()["applied"][0]["batch"], "B-0001")
        res = tracing.trace_upstream(restarted.db_path, "J-000002")
        self.assertEqual(res["totals"]["upstream_pipes"], 1)
        self.assertEqual(res["basis"]["published_batch"], "B-0001")

    def test_queries_never_see_a_half_applied_batch(self):
        """Roll an apply failure back and check the snapshot is untouched."""
        svc = self.make_svc()
        ok = self.write_batch("B-0001", 1, 2, items={
            "taps": taps("J-000001", "J-000002"), "lays": chain(1, 2)})
        for p in ok:
            svc.accept(p)
        before = tracing.trace_upstream(svc.db_path, "J-000002")

        # second batch adds a joint then hits a bad pipe reference
        bad = self.write_batch("B-0002", 2, 2, items={
            "taps": taps("J-000003"),
            "lays": [{"code": "P-000100", "from": "J-000003",
                      "to": "J-000404"}]})
        svc.accept(bad[0])
        after = tracing.trace_upstream(svc.db_path, "J-000002")
        self.assertEqual(before["totals"], after["totals"])
        self.assertEqual(before["snapshot"], after["snapshot"])
        self.assertEqual(svc.status()["snapshot"]["published_batch"], "B-0001")
        self.assertEqual(svc.status()["snapshot"]["joints"], 2)

    def test_legacy_staged_parts_and_new_parts_apply_in_one_batch(self):
        """Segments an old receiver confirmed (items in st_* rows, archive
        long gone) merge with freshly landed ones into the same batch, in
        the original (part, item) order."""
        svc = self.make_svc(auto_publish=False)
        conn = sqlite3.connect(svc.db_path)
        try:
            conn.execute(
                "INSERT INTO batches(batch_id, seq, total, upgrade, state, "
                "parts_expected, parts_received, segments, created_at) "
                "VALUES('B-0001', 1, 1, 0, 'staging', 2, 1, 1, "
                "'2026-11-22T00:00:00Z')")
            conn.execute(
                "INSERT INTO segments(seg_id, sha256, batch_id, part, parts, "
                "seq, total, upgrade, nbytes, path, received_at, ingested) "
                "VALUES('B-0001-p0', 'legacy-digest-0', 'B-0001', 0, 2, 1, 1, "
                "0, 0, 'segments/long-gone.json', '2026-11-22T00:00:00Z', "
                "'validated')")
            for i, ref in enumerate(("J-000001", "J-000002")):
                conn.execute(
                    "INSERT INTO st_taps(batch_id,part,item,ref,zone,local,"
                    "attrs) VALUES('B-0001',0,?,?,NULL,NULL,NULL)", (i, ref))
            conn.execute(
                "INSERT INTO st_lays(batch_id,part,item,code,from_ref,to_ref,"
                "attrs) VALUES('B-0001',0,0,'P-000001','J-000001','J-000002',"
                "NULL)")
            conn.commit()
        finally:
            conn.close()

        # part 1 arrives now, through the archive-only receiver
        seg = self.segment("B-0001-p1", "B-0001", 1, 1, part=1, parts=2,
                           taps=taps("J-000003"),
                           lays=[{"code": "P-000001", "from": "J-000002",
                                  "to": "J-000003"}])
        self.assertEqual(svc.accept(seg)["status"], "accepted")
        svc.publish()

        st = svc.status()
        self.assertEqual(st["snapshot"]["published_batch"], "B-0001")
        self.assertEqual(st["snapshot"]["joints"], 3)
        # part 0 laid P-000001 as J-1 -> J-2, part 1 re-laid it J-2 -> J-3;
        # the final endpoints prove the parts applied in their original order
        res = tracing.trace_upstream(svc.db_path, "J-000003")
        pipe = {p["code"]: p for p in res["pipes"]}["P-000001"]
        self.assertEqual((pipe["from"], pipe["to"]), ("J-000002", "J-000003"))
        self.assertEqual(res["basis"]["published_batch"], "B-0001")
        # publishing dropped the legacy staging rows it consumed
        conn = sqlite3.connect(svc.db_path)
        try:
            for table in ("st_taps", "st_lays"):
                n = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
                self.assertEqual(n, 0, table)
        finally:
            conn.close()

    def test_publish_reads_the_archive_within_budget(self):
        """No intermediate copy at publish time: the archive bytes read stay
        within twice the size of the batch's own archives."""
        svc = self.make_svc(auto_publish=False)
        segs = self.write_batch("B-0001", 1, 1, parts=2, items={
            "ids": [{"old": "J-000090", "new": "Q4:00090"}],
            "taps": taps("J-000001", "J-000002", "J-000003", "J-000090"),
            "lays": [{"code": "P-000001", "from": "J-000001", "to": "J-000002"},
                     {"code": "P-000002", "from": "J-000002", "to": "J-000003"},
                     {"code": "P-000003", "from": "J-000003",
                      "to": "J-000090"}],
            "redirects": [{"code": "P-000002",
                           "to": {"from": "J-000003", "to": "J-000001"}}],
            "retires": [{"kind": "pipe", "code": "P-000001"}]})
        for p in segs:
            svc.accept(p)
        budget = 2 * sum(p.stat().st_size for p in segs)

        read = {"n": 0}
        real_open = builtins.open

        class CountingReader:
            def __init__(self, fp):
                self._fp = fp

            def read(self, n=-1):
                data = self._fp.read(n)
                read["n"] += len(data)
                return data

            def seek(self, *args):
                return self._fp.seek(*args)

            def __enter__(self):
                return self

            def __exit__(self, *exc):
                self._fp.close()
                return False

        def counting_open(file, mode="r", *args, **kw):
            fp = real_open(file, mode, *args, **kw)
            if "b" in mode and str(file).endswith(".json"):
                return CountingReader(fp)
            return fp

        builtins.open = counting_open
        try:
            report = svc.publish()
        finally:
            builtins.open = real_open

        self.assertEqual([b["batch"] for b in report["applied"]], ["B-0001"])
        self.assertGreater(read["n"], 0)
        self.assertLessEqual(read["n"], budget)
        st = svc.status()
        self.assertEqual((st["snapshot"]["joints"], st["snapshot"]["pipes"]),
                         (4, 2))

    def test_offline_publish_after_source_deleted_and_hard_kill(self):
        """Acknowledged segments publish even after the source dropped its
        copies and the service was killed: the archives are enough."""
        db, data = self.work / "drain.db", self.work / "data"
        segs = self.write_batch("B-0001", 1, 1, parts=2, items={
            "taps": taps("J-000001", "J-000002", "J-000003", "J-000004"),
            "lays": chain(1, 4)})
        script = textwrap.dedent(f"""
            import sys; sys.path.insert(0, {str(ROOT)!r})
            from app import service
            svc = service.SyncService({str(db)!r}, {str(data)!r},
                                      auto_publish=False)
            for p in {[str(s) for s in segs]!r}:
                print(svc.accept(p)["status"], flush=True)
            import time; time.sleep(60)
        """)
        proc = subprocess.Popen([sys.executable, "-I", "-c", script],
                                stdout=subprocess.PIPE, text=True)
        try:
            for _ in segs:
                self.assertEqual(proc.stdout.readline().strip(), "accepted")
        finally:
            os.kill(proc.pid, signal.SIGKILL)
            proc.wait()

        # the source cleans up its acknowledged copies; only archives remain
        for p in segs:
            p.unlink()

        out = subprocess.run(
            [sys.executable, "-m", "app", "publish", "--db", str(db),
             "--data", str(data)],
            cwd=ROOT, capture_output=True, text=True, timeout=60)
        self.assertEqual(out.returncode, 0, out.stderr)
        report = json.loads(out.stdout)
        self.assertEqual([b["batch"] for b in report["applied"]], ["B-0001"])

        out = subprocess.run(
            [sys.executable, "-m", "app", "trace", "--db", str(db),
             "--ref", "J-000004", "--format", "json"],
            cwd=ROOT, capture_output=True, text=True, timeout=60)
        self.assertEqual(out.returncode, 0, out.stderr)
        res = json.loads(out.stdout)
        self.assertEqual(res["totals"]["upstream_pipes"], 3)
        self.assertEqual(res["snapshot"]["published_batch"], "B-0001")
        self.assertEqual(res["basis"]["published_batch"], "B-0001")
        self.assertEqual([b["batch"] for b in res["basis"]
                          ["referenced_batches"]], ["B-0001"])


if __name__ == "__main__":
    unittest.main()
