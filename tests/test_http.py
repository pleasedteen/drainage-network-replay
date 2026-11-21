"""The local HTTP API: status codes, both reference spellings, text output."""

from __future__ import annotations

import json
import threading
import unittest
import urllib.error
import urllib.request

from support import TempDirTest, chain, taps

from app.server import make_server


class HttpTest(TempDirTest):
    def setUp(self):
        super().setUp()
        self.svc = self.make_svc()
        self.svc.accept(self.write_batch("B-0001", 1, 1, items={
            "taps": taps(*[f"J-{i:06d}" for i in range(1, 5)]),
            "lays": chain(1, 4)})[0])
        self.srv = make_server(self.svc.publish_db, inbox_db=self.svc.inbox_db,
                               data_dir=self.work / "data", port=0)
        self.port = self.srv.server_address[1]
        self.thread = threading.Thread(target=self.srv.serve_forever,
                                       daemon=True)
        self.thread.start()
        self.addCleanup(self._stop)

    def _stop(self):
        self.srv.shutdown()
        self.srv.server_close()
        self.thread.join(timeout=5)

    def get(self, path, expect=200):
        try:
            with urllib.request.urlopen(
                    f"http://127.0.0.1:{self.port}{path}", timeout=10) as r:
                return r.status, r.read().decode(), r.headers["Content-Type"]
        except urllib.error.HTTPError as exc:
            return exc.code, exc.read().decode(), exc.headers["Content-Type"]

    def test_health_and_snapshot(self):
        code, body, _ = self.get("/health")
        self.assertEqual((code, json.loads(body)), (200, {"ok": True}))
        code, body, _ = self.get("/v1/snapshot")
        data = json.loads(body)
        self.assertEqual(data["published_batch"], "B-0001")
        self.assertEqual((data["joints"], data["pipes"]), (4, 3))

    def test_trace_json_and_text(self):
        code, body, ctype = self.get("/v1/trace/upstream?ref=J-000004")
        self.assertEqual(code, 200)
        self.assertIn("application/json", ctype)
        data = json.loads(body)
        self.assertEqual(data["totals"]["upstream_joints"], 4)
        self.assertEqual(data["basis"]["published_batch"], "B-0001")
        code, text, ctype = self.get(
            "/v1/trace/upstream?ref=J-000004&format=text")
        self.assertIn("text/plain", ctype)
        self.assertIn("J-000004", text)
        self.assertIn("basis", text.lower())

    def test_both_spellings_give_the_same_answer(self):
        self.svc.accept(self.write_batch("B-0002", 2, 2, upgrade=True, items={
            "ids": [{"old": f"J-{i:06d}", "new": f"Q4:{i:05d}"}
                    for i in range(1, 5)],
        }, name="src2")[0])
        _, legacy, _ = self.get("/v1/trace/upstream?ref=J-000004")
        _, zoned, _ = self.get("/v1/trace/upstream?ref=Q4:00004")
        a, b = json.loads(legacy), json.loads(zoned)
        self.assertEqual(a["totals"], b["totals"])
        self.assertEqual(a["query"]["resolved"]["uid"],
                         b["query"]["resolved"]["uid"])
        self.assertEqual(b["basis"]["published_batch"], "B-0002")

    def test_error_responses(self):
        self.assertEqual(self.get("/v1/trace/upstream")[0], 400)
        self.assertEqual(self.get("/v1/trace/upstream?ref=NOPE")[0], 404)
        self.assertEqual(self.get("/v1/trace/upstream?ref=J-000004&limit=x")[0],
                         400)
        code, body, _ = self.get("/v1/trace/upstream?ref=J-000004&limit=1")
        self.assertEqual(code, 413)
        self.assertIn("limit", json.loads(body)["error"])
        self.assertEqual(self.get("/v1/nothing")[0], 404)
        self.assertEqual(self.get("/v1/batches/NOPE")[0], 404)

    def test_batches_report(self):
        self.svc.accept(self.write_batch("B-0002", 2, 2, parts=2, name="src2")[0])
        code, body, _ = self.get("/v1/batches")
        data = json.loads(body)
        self.assertEqual(code, 200)
        self.assertEqual(data["publish_blocker"]["reason"], "waiting_for_parts")
        self.assertEqual([b["batch"] for b in data["batches"]],
                         ["B-0001", "B-0002"])
        code, body, _ = self.get("/v1/batches/B-0002")
        self.assertEqual(json.loads(body)["parts_received"], [0])


if __name__ == "__main__":
    unittest.main()
