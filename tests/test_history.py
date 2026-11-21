"""Per-batch history: as-of traces, online backfill, storage growth.

The offline batches used here exercise the four change kinds the history
must survive: 跨编号升级 (id-table upgrade), 迁区 (rezone), 改向 (redirects),
停用 (retirements).
"""

from __future__ import annotations

import json
import subprocess
import sys
import threading
import unittest
import urllib.error
import urllib.request

from support import ROOT, TempDirTest, chain, taps

from app import service, tracing
from app.db import connect


def wipe_history(db_path):
    """Simulate a database created before history tracking existed: the
    schema is there (init_db creates it on open) but no version rows and no
    watermark."""
    conn = connect(db_path)
    try:
        conn.execute("BEGIN IMMEDIATE")
        for table in ("hist_joints", "hist_pipes", "hist_refs"):
            conn.execute(f"DELETE FROM {table}")
        conn.execute("DELETE FROM meta WHERE key='history_upto_seq'")
        conn.execute("COMMIT")
    finally:
        conn.close()


def hist_counts(db_path):
    conn = connect(db_path, read_only=True)
    try:
        return {t: conn.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
                for t in ("hist_joints", "hist_pipes", "hist_refs")}
    finally:
        conn.close()


class HistoryFixture(TempDirTest):
    """The five-batch story used by several tests."""

    def story(self, name="src"):
        b1 = self.write_batch("B-0001", 1, 5, name=name, items={
            "taps": taps("J-000001", "J-000002", "J-000003", "J-000009"),
            "lays": chain(1, 3) + [{"code": "P-000009", "from": "J-000009",
                                    "to": "J-000002"}]})
        b2 = self.write_batch("B-0002", 2, 5, upgrade=True, name=name, items={
            "ids": [{"old": "J-000001", "new": "Q4:00001"},
                    {"old": "J-000002", "new": "Q4:00002"},
                    {"old": "J-000003", "new": "Q5:00003"},
                    {"old": "J-000009", "new": "Q5:00009"}],
            "taps": [{"ref": "J-000001", "zone": "Q4", "local": "00001"},
                     {"ref": "J-000002", "zone": "Q4", "local": "00002"},
                     {"ref": "J-000003", "zone": "Q5", "local": "00003"},
                     {"ref": "J-000009", "zone": "Q5", "local": "00009"}]})
        b3 = self.write_batch("B-0003", 3, 5, name=name, items={
            "redirects": [{"code": "P-000009",
                           "to": {"from": "J-000009", "to": "Q5:00003"}}]})
        b4 = self.write_batch("B-0004", 4, 5, name=name, items={
            "taps": [{"ref": "J-000002", "zone": "Q6", "local": "00888"}]})
        b5 = self.write_batch("B-0005", 5, 5, name=name, items={
            "retires": [{"kind": "joint", "ref": "Q4:00001"}]})
        return (b1, b2, b3, b4, b5)

    def deliver(self, svc, groups):
        for group in groups:
            for p in group:
                svc.accept(p)


class AsOfTraceTest(HistoryFixture):
    def codes(self, res):
        return {p["code"] for p in res["pipes"]}

    def displays(self, res):
        return {j["display"] for j in res["joints"]}

    def test_pipe_direction_and_retirement_follow_the_requested_batch(self):
        svc = self.make_svc()
        self.deliver(svc, self.story())

        # as of B-0001: P-000009 still ends at J-000002, nothing retired
        res = tracing.trace_upstream(svc.db_path, "J-000002", batch="B-0001")
        self.assertEqual(self.codes(res), {"P-000001", "P-000009"})
        self.assertEqual(res["snapshot"]["published_batch"], "B-0001")
        self.assertTrue(res["snapshot"]["historical"])
        self.assertEqual(res["totals"]["retired_pipes_not_followed"], 0)

        # as of B-0003: P-000009 was redirected to end at J-000003
        res = tracing.trace_upstream(svc.db_path, "J-000002", batch="B-0003")
        self.assertEqual(self.codes(res), {"P-000001"})
        res3 = tracing.trace_upstream(svc.db_path, "Q5:00003", batch="B-0003")
        self.assertEqual(self.codes(res3),
                         {"P-000001", "P-000002", "P-000009"})

        # as of B-0005: J-000001 (Q4:00001) retired, P-000001 with it
        res = tracing.trace_upstream(svc.db_path, "J-000002", batch="B-0005")
        self.assertEqual(self.codes(res), set())
        self.assertEqual(res["totals"]["retired_pipes_not_followed"], 1)
        with_retired = tracing.trace_upstream(
            svc.db_path, "J-000002", batch="B-0005", include_retired=True)
        self.assertEqual(self.codes(with_retired), {"P-000001"})

    def test_numbering_upgrade_visible_per_batch(self):
        svc = self.make_svc()
        self.deliver(svc, self.story())

        # before the upgrade batch only legacy ids exist
        with self.assertRaises(tracing.NotFound):
            tracing.trace_upstream(svc.db_path, "Q4:00002", batch="B-0001")
        res = tracing.trace_upstream(svc.db_path, "J-000002", batch="B-0001")
        self.assertEqual(self.displays(res), {"J-000001", "J-000002",
                                              "J-000009"})
        self.assertIsNone(res["snapshot"]["upgrade_seq"])

        # from the upgrade batch on, both spellings address the same joint
        by_legacy = tracing.trace_upstream(svc.db_path, "J-000002",
                                           batch="B-0002")
        by_zoned = tracing.trace_upstream(svc.db_path, "Q4:00002",
                                          batch="B-0002")
        self.assertEqual(by_zoned["query"]["resolved"]["uid"],
                         by_legacy["query"]["resolved"]["uid"])
        self.assertEqual(self.displays(by_zoned),
                         {"Q4:00001", "Q4:00002", "Q5:00009"})
        self.assertEqual(by_zoned["snapshot"]["upgrade_seq"], 2)
        self.assertEqual({p["from"] for p in by_zoned["pipes"]},
                         {"Q4:00001", "Q5:00009"})

    def test_rezone_visible_per_batch(self):
        svc = self.make_svc()
        self.deliver(svc, self.story())

        # as of B-0002 the joint still lives in Q4
        res = tracing.trace_upstream(svc.db_path, "Q4:00002", batch="B-0002")
        self.assertEqual(res["query"]["resolved"]["display"], "Q4:00002")
        with self.assertRaises(tracing.NotFound):
            tracing.trace_upstream(svc.db_path, "Q6:00888", batch="B-0002")

        # as of B-0004 it has moved: old zoned id dead, legacy id kept
        with self.assertRaises(tracing.NotFound):
            tracing.trace_upstream(svc.db_path, "Q4:00002", batch="B-0004")
        moved = tracing.trace_upstream(svc.db_path, "Q6:00888", batch="B-0004")
        legacy = tracing.trace_upstream(svc.db_path, "J-000002", batch="B-0004")
        self.assertEqual(moved["query"]["resolved"]["uid"],
                         legacy["query"]["resolved"]["uid"])
        # pipes follow the joint across the rezone (uid-based)
        self.assertEqual(self.codes(moved), {"P-000001"})

    def test_basis_cites_only_batches_up_to_the_requested_one(self):
        svc = self.make_svc()
        self.deliver(svc, self.story())
        res = tracing.trace_upstream(svc.db_path, "Q5:00003", batch="B-0003")
        self.assertEqual(res["basis"]["published_batch"], "B-0003")
        self.assertEqual(res["basis"]["published_seq"], 3)
        cited = res["basis"]["referenced_batches"]
        # B-0001 laid the pipes, B-0002 renumbered the joints, B-0003
        # redirected P-000009 -- nothing younger is cited
        self.assertEqual([b["batch"] for b in cited],
                         ["B-0001", "B-0002", "B-0003"])
        self.assertTrue(all(b["seq"] <= 3 for b in cited))
        # the requested batch is the snapshot the answer is based on
        self.assertEqual(res["snapshot"]["published_batch"], "B-0003")

    def test_unspecified_batch_queries_the_latest_network(self):
        svc = self.make_svc()
        self.deliver(svc, self.story())
        latest = tracing.trace_upstream(svc.db_path, "J-000002")
        asof = tracing.trace_upstream(svc.db_path, "J-000002", batch="B-0005")
        self.assertEqual(latest["totals"], asof["totals"])
        self.assertEqual(self.codes(latest), self.codes(asof))
        self.assertEqual(latest["snapshot"]["published_batch"], "B-0005")
        self.assertNotIn("historical", latest["snapshot"])

    def test_attrs_follow_the_requested_batch(self):
        svc = self.make_svc()
        b1 = self.write_batch("B-0001", 1, 2, items={
            "taps": [{"ref": "J-000001",
                      "attrs": {"kind": "污水", "note": "old"}}]})
        b2 = self.write_batch("B-0002", 2, 2, items={
            "taps": [{"ref": "J-000001", "attrs": {"note": "new"}}]})
        self.deliver(svc, (b1, b2))
        res1 = tracing.trace_upstream(svc.db_path, "J-000001", batch="B-0001")
        self.assertEqual(res1["query"]["resolved"]["attrs"],
                         {"kind": "污水", "note": "old"})
        res2 = tracing.trace_upstream(svc.db_path, "J-000001", batch="B-0002")
        self.assertEqual(res2["query"]["resolved"]["attrs"],
                         {"kind": "污水", "note": "new"})

    def test_unknown_or_unpublished_batch_is_never_a_partial_answer(self):
        svc = self.make_svc()
        b1 = self.write_batch("B-0001", 1, 3, items={
            "taps": taps("J-000001", "J-000002"), "lays": chain(1, 2)})
        bad = self.write_batch("B-0002", 2, 3, items={
            "lays": [{"code": "P-000099", "from": "J-000099",
                      "to": "J-000001"}]})       # unknown joint -> rejected
        b3 = self.write_batch("B-0003", 3, 3, parts=2, items={
            "taps": taps("J-000003")})
        self.deliver(svc, (b1, bad))
        for p in b3[:1]:                        # one part missing: staging
            svc.accept(p)

        with self.assertRaises(tracing.NotFound):
            tracing.trace_upstream(svc.db_path, "J-000001", batch="B-9999")
        with self.assertRaises(tracing.HistoryNotReady):   # rejected
            tracing.trace_upstream(svc.db_path, "J-000001", batch="B-0002")
        with self.assertRaises(tracing.HistoryNotReady):   # not published
            tracing.trace_upstream(svc.db_path, "J-000001", batch="B-0003")
        # the last good snapshot still answers
        res = tracing.trace_upstream(svc.db_path, "J-000002")
        self.assertEqual(res["snapshot"]["published_batch"], "B-0001")


class BackfillTest(HistoryFixture):
    def test_unbuilt_history_returns_no_partial_results_then_backfills(self):
        svc = self.make_svc()
        self.deliver(svc, self.story())
        wipe_history(svc.db_path)

        st = svc.status()
        self.assertFalse(st["history"]["complete"])
        self.assertEqual(st["history"]["pending"], 5)

        # live queries keep working; every as-of query is refused whole
        self.assertEqual(tracing.trace_upstream(svc.db_path, "J-000002")
                         ["snapshot"]["published_batch"], "B-0005")
        for batch in ("B-0001", "B-0003", "B-0005"):
            with self.assertRaises(tracing.HistoryNotReady, msg=batch):
                tracing.trace_upstream(svc.db_path, "J-000002", batch=batch)

        out = svc.backfill_history()
        self.assertEqual(out["copied"], [1, 2, 3, 4, 5])
        self.assertTrue(out["history"]["complete"])
        self.assertTrue(svc.status()["history"]["complete"])
        res = tracing.trace_upstream(svc.db_path, "J-000002", batch="B-0001")
        self.assertEqual({p["code"] for p in res["pipes"]},
                         {"P-000001", "P-000009"})

    def test_backfilled_history_matches_live_recorded_history(self):
        groups = self.story()
        svc_a = self.make_svc()
        self.deliver(svc_a, groups)

        svc_b = service.SyncService(self.work / "b.db", self.work / "data-b")
        self.deliver(svc_b, groups)
        wipe_history(svc_b.db_path)
        svc_b.backfill_history()

        refs = ("J-000001", "J-000002", "J-000003", "J-000009",
                "Q4:00001", "Q4:00002", "Q5:00003", "Q6:00888")
        batches = ("B-0001", "B-0002", "B-0003", "B-0004", "B-0005")
        for batch in batches:
            for ref in refs:
                try:
                    a = tracing.trace_upstream(svc_a.db_path, ref, batch=batch)
                except tracing.NotFound:
                    with self.assertRaises(tracing.NotFound):
                        tracing.trace_upstream(svc_b.db_path, ref, batch=batch)
                    continue
                b = tracing.trace_upstream(svc_b.db_path, ref, batch=batch)
                a["snapshot"].pop("published_at")
                b["snapshot"].pop("published_at")
                self.assertEqual(a, b, f"{ref} as of {batch}")

    def test_backfill_while_publishing_and_querying_continue(self):
        svc = self.make_svc()
        self.deliver(svc, self.story()[:3])
        wipe_history(svc.db_path)

        started = threading.Event()
        release = threading.Event()
        errors, report = [], []

        def progress(seq, target):
            if seq == 1:
                started.set()
                release.wait(30)

        def run():
            try:
                report.append(svc.backfill_history(progress=progress))
            except Exception as exc:      # noqa: BLE001
                errors.append(repr(exc))

        t = threading.Thread(target=run, daemon=True)
        t.start()
        self.assertTrue(started.wait(10))

        # publishing continues while the backfill is mid-flight; this batch
        # is applied without history recording (the watermark is behind)
        self.deliver(svc, self.story()[3:4])
        self.assertEqual(svc.status()["applied_seq"], 4)
        # live queries continue too
        live = tracing.trace_upstream(svc.db_path, "J-000002")
        self.assertEqual(live["snapshot"]["published_batch"], "B-0004")
        # the not-yet-built versions refuse to answer partially
        with self.assertRaises(tracing.HistoryNotReady):
            tracing.trace_upstream(svc.db_path, "J-000002", batch="B-0004")

        release.set()
        t.join(60)
        self.assertEqual(errors, [])
        self.assertTrue(report[0]["history"]["complete"])
        # the batch published during the backfill is now traceable as well
        res = tracing.trace_upstream(svc.db_path, "Q6:00888", batch="B-0004")
        self.assertEqual(res["snapshot"]["published_batch"], "B-0004")

        # and batches published after the backfill are recorded inline
        self.deliver(svc, self.story()[4:5])
        res = tracing.trace_upstream(svc.db_path, "J-000002", batch="B-0005")
        self.assertEqual(res["totals"]["retired_pipes_not_followed"], 1)
        self.assertTrue(svc.status()["history"]["complete"])

    def test_queries_do_not_replay_landed_data(self):
        svc = self.make_svc()
        self.deliver(svc, self.story())
        before = tracing.trace_upstream(svc.db_path, "J-000002", batch="B-0003")

        # wipe every piece of landed data: staging rows and segment files
        conn = connect(svc.db_path)
        try:
            conn.execute("BEGIN IMMEDIATE")
            for table in ("st_ids", "st_taps", "st_lays", "st_redirects",
                          "st_retires"):
                conn.execute(f"DELETE FROM {table}")
            conn.execute("COMMIT")
        finally:
            conn.close()
        import shutil
        shutil.rmtree(svc.data_dir / "segments")

        after = tracing.trace_upstream(svc.db_path, "J-000002", batch="B-0003")
        self.assertEqual(before, after)


class StorageGrowthTest(TempDirTest):
    def test_history_storage_grows_with_actual_changes_only(self):
        svc = self.make_svc()
        n = 50
        b1 = self.write_batch("B-0001", 1, 5, items={
            "taps": taps(*[f"J-{i:06d}" for i in range(1, n + 1)]),
            "lays": chain(1, n)})
        b2 = self.write_batch("B-0002", 2, 5, items={      # 改向 one pipe
            "redirects": [{"code": "P-000010",
                           "to": {"from": "J-000049", "to": "J-000050"}}]})
        b3 = self.write_batch("B-0003", 3, 5, items={      # 停用 one pipe
            "retires": [{"kind": "pipe", "code": "P-000020"}]})
        b4 = self.write_batch("B-0004", 4, 5, items={      # touch, no change
            "taps": taps("J-000001", "J-000002", "J-000003")})
        b5 = self.write_batch("B-0005", 5, 5, items={      # one attrs change
            "taps": [{"ref": "J-000001", "attrs": {"kind": "污水"}}]})
        for group in (b1, b2, b3, b4, b5):
            for p in group:
                svc.accept(p)

        counts = hist_counts(svc.db_path)
        self.assertEqual(counts["hist_joints"], n + 1)   # one attrs version
        self.assertEqual(counts["hist_pipes"], n - 1 + 2)  # redirect + retire
        self.assertEqual(counts["hist_refs"], n)
        # every batch, including the no-op one, is traceable
        for batch in ("B-0001", "B-0002", "B-0003", "B-0004", "B-0005"):
            res = tracing.trace_upstream(svc.db_path, "J-000050", batch=batch)
            self.assertEqual(res["snapshot"]["published_batch"], batch)


class HttpHistoryTest(TempDirTest):
    def setUp(self):
        super().setUp()
        self.svc = self.make_svc()
        b1 = self.write_batch("B-0001", 1, 2, items={
            "taps": taps("J-000001", "J-000002"), "lays": chain(1, 2)})
        b2 = self.write_batch("B-0002", 2, 2, upgrade=True, items={
            "ids": [{"old": "J-000001", "new": "Q4:00001"},
                    {"old": "J-000002", "new": "Q4:00002"}]})
        for group in (b1, b2):
            for p in group:
                self.svc.accept(p)
        from app.server import make_server
        self.srv = make_server(self.svc.db_path, self.work / "data", port=0)
        self.port = self.srv.server_address[1]
        self.thread = threading.Thread(target=self.srv.serve_forever,
                                       daemon=True)
        self.thread.start()
        self.addCleanup(self._stop)

    def _stop(self):
        self.srv.shutdown()
        self.srv.server_close()
        self.thread.join(timeout=5)

    def get(self, path):
        try:
            with urllib.request.urlopen(
                    f"http://127.0.0.1:{self.port}{path}", timeout=10) as r:
                return r.status, r.read().decode()
        except urllib.error.HTTPError as exc:
            return exc.code, exc.read().decode()

    def test_trace_with_batch_param(self):
        code, body = self.get("/v1/trace/upstream?ref=J-000002&batch=B-0001")
        data = json.loads(body)
        self.assertEqual(code, 200)
        self.assertEqual(data["basis"]["published_batch"], "B-0001")
        self.assertTrue(data["snapshot"]["historical"])
        self.assertEqual(data["totals"]["upstream_pipes"], 1)
        # text rendering marks the historical snapshot
        code, text = self.get(
            "/v1/trace/upstream?ref=J-000002&batch=B-0001&format=text")
        self.assertEqual(code, 200)
        self.assertIn("as-of", text)

    def test_batch_param_errors(self):
        code, _ = self.get("/v1/trace/upstream?ref=J-000002&batch=B-9999")
        self.assertEqual(code, 404)
        # a staged-but-incomplete batch cannot be traced
        b3 = self.write_batch("B-0003", 3, 3, parts=2, name="src3",
                              items={"taps": taps("J-000003")})
        self.svc.accept(b3[0])
        code, body = self.get("/v1/trace/upstream?ref=J-000002&batch=B-0003")
        self.assertEqual(code, 409)
        self.assertIn("error", json.loads(body))

    def test_joint_resolution_with_batch_param(self):
        code, _ = self.get("/v1/joint?ref=Q4:00002&batch=B-0001")
        self.assertEqual(code, 404)          # pre-upgrade numbering
        code, body = self.get("/v1/joint?ref=Q4:00002&batch=B-0002")
        self.assertEqual(code, 200)
        self.assertEqual(json.loads(body)["legacy_id"], "J-000002")
        code, _ = self.get("/v1/joint?ref=Q4:00002&batch=B-9999")
        self.assertEqual(code, 404)

    def test_history_endpoint_and_backfill_flow(self):
        code, body = self.get("/v1/history")
        self.assertEqual((code, json.loads(body)["complete"]), (200, True))

        wipe_history(self.svc.db_path)
        code, body = self.get("/v1/history")
        self.assertEqual(json.loads(body)["pending"], 2)
        code, _ = self.get("/v1/trace/upstream?ref=J-000002&batch=B-0001")
        self.assertEqual(code, 409)          # never a partial answer

        self.svc.backfill_history()
        code, body = self.get("/v1/trace/upstream?ref=J-000002&batch=B-0001")
        self.assertEqual(code, 200)
        self.assertEqual(json.loads(body)["basis"]["published_batch"],
                         "B-0001")


class CliTest(TempDirTest):
    def test_backfill_and_trace_batch_cli(self):
        svc = self.make_svc()
        b1 = self.write_batch("B-0001", 1, 2, items={
            "taps": taps("J-000001", "J-000002"), "lays": chain(1, 2)})
        b2 = self.write_batch("B-0002", 2, 2, items={
            "retires": [{"kind": "pipe", "code": "P-000001"}]})
        for group in (b1, b2):
            for p in group:
                svc.accept(p)
        wipe_history(svc.db_path)

        env = dict(PYTHONPATH=str(ROOT))
        out = subprocess.run(
            [sys.executable, "-m", "app", "backfill", "--db", svc.db_path],
            capture_output=True, text=True, env=env, cwd=ROOT)
        self.assertEqual(out.returncode, 0, out.stderr)
        self.assertTrue(json.loads(out.stdout)["history"]["complete"])

        out = subprocess.run(
            [sys.executable, "-m", "app", "trace", "--db", svc.db_path,
             "--ref", "J-000002", "--batch", "B-0001"],
            capture_output=True, text=True, env=env, cwd=ROOT)
        self.assertEqual(out.returncode, 0, out.stderr)
        self.assertIn("P-000001", out.stdout)      # still there as of B-0001
        out = subprocess.run(
            [sys.executable, "-m", "app", "trace", "--db", svc.db_path,
             "--ref", "J-000002"],
            capture_output=True, text=True, env=env, cwd=ROOT)
        self.assertNotIn("P-000001:", out.stdout)  # retired in the latest


if __name__ == "__main__":
    unittest.main()
