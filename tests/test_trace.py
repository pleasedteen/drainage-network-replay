"""Tracing semantics: flow direction, redirects, retirements, identifier
migration (legacy <-> zoned), and what the answer cites as its basis."""

from __future__ import annotations

import unittest

from support import TempDirTest, chain, taps

from app import tracing


class TraceTest(TempDirTest):
    def deliver(self, svc, *groups):
        for group in groups:
            for p in group:
                svc.accept(p)

    def trace(self, svc, ref, **kw):
        return tracing.trace_upstream(svc.publish_db, ref, **kw)

    def codes(self, res):
        return {p["code"] for p in res["pipes"]}

    def displays(self, res):
        return {j["display"] for j in res["joints"]}

    # ------------------------------------------------------------------
    def test_direction_and_indirect_upstream(self):
        svc = self.make_svc()
        # J1 -> J2 -> J3 -> J4, plus a side branch J5 -> J3
        seg = self.write_batch("B-0001", 1, 1, items={
            "taps": taps(*(f"J-{i:06d}" for i in range(1, 7))),
            "lays": chain(1, 4) + [
                {"code": "P-000005", "from": "J-000005", "to": "J-000003"},
                {"code": "P-000006", "from": "J-000006", "to": "J-000002"},
            ]})
        self.deliver(svc, seg)
        res = self.trace(svc, "J-000003")
        self.assertEqual(self.codes(res),
                         {"P-000001", "P-000002", "P-000005", "P-000006"})
        self.assertEqual(self.displays(res),
                         {"J-000001", "J-000002", "J-000003", "J-000005",
                          "J-000006"})
        self.assertEqual(res["query"]["resolved"]["display"], "J-000003")
        # depth: start 0, J2 1, J1/J5 2
        depth = {j["display"]: j["depth"] for j in res["joints"]}
        self.assertEqual(depth, {"J-000003": 0, "J-000002": 1, "J-000005": 1,
                                 "J-000001": 2, "J-000006": 2})

    def test_downstream_pipes_are_not_upstream(self):
        svc = self.make_svc()
        seg = self.write_batch("B-0001", 1, 1, items={
            "taps": taps("J-000001", "J-000002", "J-000003"),
            "lays": chain(1, 3)})
        self.deliver(svc, seg)
        res = self.trace(svc, "J-000002")
        self.assertEqual(self.codes(res), {"P-000001"})
        self.assertNotIn("P-000002", self.codes(res))
        self.assertEqual(res["totals"]["max_depth"], 1)

    def test_relaid_pipe_follows_its_current_endpoints_only(self):
        svc = self.make_svc()
        b1 = self.write_batch("B-0001", 1, 2, items={
            "taps": taps("J-000001", "J-000002", "J-000003", "J-000009"),
            "lays": chain(1, 3) + [{"code": "P-000009", "from": "J-000009",
                                    "to": "J-000002"}]})
        b2 = self.write_batch("B-0002", 2, 2, items={
            "lays": [{"code": "P-000009", "from": "J-000009",
                      "to": "J-000003"}]})
        self.deliver(svc, b1, b2)
        res = self.trace(svc, "J-000002")
        self.assertEqual(self.codes(res), {"P-000001"})
        res3 = self.trace(svc, "J-000003")
        self.assertEqual(self.codes(res3), {"P-000001", "P-000002", "P-000009"})
        self.assertEqual({p["code"]: p["batch"] for p in res3["pipes"]},
                         {"P-000001": "B-0001", "P-000002": "B-0001",
                          "P-000009": "B-0002"})

    def test_joint_redirect_moves_all_outgoing_pipes(self):
        svc = self.make_svc()
        b1 = self.write_batch("B-0001", 1, 2, items={
            "taps": taps("J-000001", "J-000002", "J-000003", "J-000010"),
            "lays": chain(1, 3) + [
                {"code": "P-000010", "from": "J-000010", "to": "J-000003"},
                {"code": "P-000011", "from": "J-000010", "to": "J-000002"},
            ]})
        b2 = self.write_batch("B-0002", 2, 2, items={
            "redirects": [{"from": "J-000010", "to": "J-000001"}]})
        self.deliver(svc, b1, b2)
        # J-000010 now delivers nothing; J-000010's old pipes start at J1
        self.assertEqual(self.codes(self.trace(svc, "J-000010")), set())
        res = self.trace(svc, "J-000002")
        self.assertEqual(self.codes(res), {"P-000001", "P-000011"})

    def test_retired_pipe_is_not_traversed_but_is_reported(self):
        svc = self.make_svc()
        b1 = self.write_batch("B-0001", 1, 2, items={
            "taps": taps("J-000001", "J-000002"), "lays": chain(1, 2)})
        b2 = self.write_batch("B-0002", 2, 2, items={
            "retires": [{"kind": "pipe", "code": "P-000001"}]})
        self.deliver(svc, b1, b2)
        res = self.trace(svc, "J-000002")
        self.assertEqual(self.codes(res), set())
        self.assertEqual(res["totals"]["retired_pipes_not_followed"], 1)
        with_retired = self.trace(svc, "J-000002", include_retired=True)
        self.assertEqual(self.codes(with_retired), {"P-000001"})

    def test_joint_retirement_retires_its_pipes(self):
        svc = self.make_svc()
        b1 = self.write_batch("B-0001", 1, 2, items={
            "taps": taps("J-000001", "J-000002", "J-000003"),
            "lays": chain(1, 3)})
        b2 = self.write_batch("B-0002", 2, 2, items={
            "retires": [{"kind": "joint", "ref": "J-000002"}]})
        self.deliver(svc, b1, b2)
        res = self.trace(svc, "J-000003")
        self.assertEqual(self.codes(res), set())
        detail = svc.batch_detail("B-0002")
        self.assertEqual(detail["effects"].get("pipe_retired_by_joint"), 2)
        self.assertEqual(detail["effects"].get("joint_retired"), 1)

    # -- identifier migration -----------------------------------------
    def test_legacy_and_zoned_spellings_resolve_to_one_joint(self):
        svc = self.make_svc()
        b1 = self.write_batch("B-0001", 1, 3, items={
            "taps": taps("J-000001", "J-000002", "J-000003"),
            "lays": chain(1, 3)})
        b2 = self.write_batch("B-0002", 2, 3, upgrade=True, parts=2, items={
            "ids": [{"old": f"J-{n:06d}", "new": f"Q4:{n:05d}"}
                    for n in (1, 2, 3)],
            "taps": [{"ref": f"J-{n:06d}", "zone": "Q4", "local": f"{n:05d}"}
                     for n in (1, 2, 3)]})
        self.deliver(svc, b1, b2)
        by_legacy = self.trace(svc, "J-000002")
        by_zoned = self.trace(svc, "Q4:00002")
        self.assertEqual(by_legacy["totals"], by_zoned["totals"])
        self.assertEqual(self.codes(by_legacy), self.codes(by_zoned))
        self.assertEqual(by_zoned["query"]["resolved"]["uid"],
                         by_legacy["query"]["resolved"]["uid"])
        self.assertEqual(by_zoned["query"]["resolved"]["legacy_id"], "J-000002")
        self.assertEqual(self.displays(by_zoned), {"Q4:00001", "Q4:00002"})

    def test_pipes_laid_before_upgrade_still_connect_after_it(self):
        svc = self.make_svc()
        b1 = self.write_batch("B-0001", 1, 2, items={
            "taps": taps("J-000001", "J-000002"), "lays": chain(1, 2)})
        b2 = self.write_batch("B-0002", 2, 2, upgrade=True, items={
            "ids": [{"old": "J-000001", "new": "Q4:00001"},
                    {"old": "J-000002", "new": "Q5:00002"}],
            "taps": [{"ref": "J-000001", "zone": "Q4", "local": "00001"},
                     {"ref": "J-000002", "zone": "Q5", "local": "00002"}]})
        self.deliver(svc, b1, b2)
        res = self.trace(svc, "Q5:00002")
        self.assertEqual(self.codes(res), {"P-000001"})
        self.assertEqual(res["pipes"][0]["from"], "Q4:00001")
        self.assertEqual(res["pipes"][0]["to"], "Q5:00002")
        # the pre-upgrade pipe still cites the batch that laid it
        self.assertEqual(res["pipes"][0]["batch"], "B-0001")

    def test_new_pipes_may_be_written_with_old_ids(self):
        svc = self.make_svc()
        b1 = self.write_batch("B-0001", 1, 2, items={
            "taps": taps("J-000001", "J-000002")})
        b2 = self.write_batch("B-0002", 2, 2, upgrade=True, items={
            "ids": [{"old": "J-000001", "new": "Q4:00001"},
                    {"old": "J-000002", "new": "Q4:00002"}]})
        b3 = self.write_batch("B-0003", 3, 3, items={
            "lays": [{"code": "P-000100", "from": "J-000001",
                      "to": "Q4:00002"}]})
        self.deliver(svc, b1, b2, b3)
        res = self.trace(svc, "J-000002")
        self.assertEqual(self.codes(res), {"P-000100"})
        self.assertEqual(res["query"]["resolved"]["zoned_id"], "Q4:00002")

    def test_same_local_number_in_two_zones_stays_two_joints(self):
        svc = self.make_svc()
        seg = self.write_batch("B-0001", 1, 1, items={
            "taps": [{"ref": "Q4:00001"}, {"ref": "Q5:00001"}],
            "lays": [{"code": "P-000001", "from": "Q4:00001",
                      "to": "Q5:00001"}]})
        self.deliver(svc, seg)
        res4 = self.trace(svc, "Q4:00001")
        res5 = self.trace(svc, "Q5:00001")
        self.assertEqual(res4["totals"]["upstream_joints"], 1)
        self.assertEqual(res5["totals"]["upstream_joints"], 2)
        self.assertEqual(self.codes(res4), set())
        self.assertEqual(self.codes(res5), {"P-000001"})

    def test_unknown_or_malformed_reference_is_a_lookup_error(self):
        svc = self.make_svc()
        for ref in ("J-999999", "Q9:00001", "banana", "Q4:", ""):
            with self.assertRaises(tracing.NotFound, msg=ref):
                self.trace(svc, ref)

    # -- basis / snapshot ---------------------------------------------
    def test_answer_cites_batches_and_published_snapshot(self):
        svc = self.make_svc()
        b1 = self.write_batch("B-0001", 1, 3, items={
            "taps": taps("J-000001", "J-000002"), "lays": chain(1, 2)})
        b2 = self.write_batch("B-0002", 2, 3, items={
            "taps": [{"ref": "J-000001", "attrs": {"note": "checked"}}]})
        # B-0003 is staged but incomplete: must not change the answer
        b3 = self.write_batch("B-0003", 3, 3, parts=2, items={
            "lays": [{"code": "P-000050", "from": "J-000001",
                      "to": "J-000002"}]})
        self.deliver(svc, b1, b2, b3[:1])
        res = self.trace(svc, "J-000002")
        self.assertEqual(res["snapshot"]["published_batch"], "B-0002")
        self.assertEqual(res["basis"]["published_seq"], 2)
        self.assertEqual([b["batch"] for b in res["basis"]["referenced_batches"]],
                         ["B-0001", "B-0002"])
        cited = {b["batch"]: b for b in res["basis"]["referenced_batches"]}
        self.assertEqual(cited["B-0001"]["pipes"], 1)
        self.assertEqual(cited["B-0001"]["joints"], 1)   # J-000002
        self.assertEqual(cited["B-0002"]["joints"], 1)   # J-000001, re-tapped
        self.assertEqual(res["totals"]["upstream_pipes"], 1)   # B-0003 not in


if __name__ == "__main__":
    unittest.main()


class RezoningTest(TempDirTest):
    def test_joint_moved_to_another_zone_keeps_its_uid_and_pipes(self):
        svc = self.make_svc()
        b1 = self.write_batch("B-0001", 1, 3, items={
            "taps": taps("J-000001", "J-000002"), "lays": chain(1, 2)})
        b2 = self.write_batch("B-0002", 2, 3, upgrade=True, items={
            "ids": [{"old": "J-000001", "new": "Q4:00001"},
                    {"old": "J-000002", "new": "Q4:00002"}]})
        b3 = self.write_batch("B-0003", 3, 3, items={
            "taps": [{"ref": "J-000002", "zone": "Q6", "local": "00888"}]})
        for group in (b1, b2, b3):
            for p in group:
                svc.accept(p)
        res = tracing.trace_upstream(svc.publish_db, "Q6:00888")
        self.assertEqual({p["code"] for p in res["pipes"]}, {"P-000001"})
        self.assertEqual(res["query"]["resolved"]["legacy_id"], "J-000002")
        # the stale zoned spelling no longer answers
        with self.assertRaises(tracing.NotFound):
            tracing.trace_upstream(svc.publish_db, "Q4:00002")
        # the legacy spelling still does
        again = tracing.trace_upstream(svc.publish_db, "J-000002")
        self.assertEqual(again["query"]["resolved"]["uid"],
                         res["query"]["resolved"]["uid"])
