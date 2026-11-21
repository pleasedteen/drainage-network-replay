"""Versioned sample generation: interruption, version flips, cleanup.

Three families of guarantees are exercised here:

* **interruption** -- a generator killed mid-write never damages the set that
  was selected before, and leaves no staging litter behind;
* **flips** -- regenerating never clears user files (even ones named exactly
  like a shard); a single replay consumes exactly one selected version; other
  JSON files never reach the confirm/publish path;
* **cleanup** -- after repeated successful flips unused old sets do not pile
  up, while sets still leased by an in-flight replay remain readable.
"""

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

from support import ROOT, TempDirTest, taps

from app import replay, sample_store, service


def _populate(names):
    """Return a ``populate`` callback writing one trivial segment per name."""
    def populate(staging: Path) -> None:
        for name in names:
            seg = {"segment": name, "batch": name.split("-p")[0], "seq": 1,
                   "total": 1, "part": 0, "parts": 1, "upgrade": False,
                   "taps": taps("J-000001")}
            (staging / f"{name}.json").write_text(
                json.dumps(seg, ensure_ascii=False), encoding="utf-8")
        (staging / "HELD-BACK.txt").write_text("held\n", encoding="utf-8")
    return populate


class _RecordingSync:
    """Minimal ``sync.accept`` stand-in that records what it was offered."""

    def __init__(self, on_first=None):
        self.offered: list[Path] = []
        self.on_first = on_first

    def accept(self, path):
        self.offered.append(Path(path))
        if self.on_first is not None:
            callback, self.on_first = self.on_first, None
            callback()
        p = Path(path)
        return {"status": "accepted", "segment": p.stem, "batch": "B-0001",
                "part": 0, "parts": 1, "publish": {"applied": []}}


class SampleLayoutTest(TempDirTest):
    def store(self, root=None):
        return sample_store.SampleStore(root or (self.work / "src"))

    def generate(self, names, root=None):
        root = root or (self.work / "src")
        return sample_store.publish_samples(root, _populate(names))

    def versions(self, root=None):
        vdir = (root or (self.work / "src")).joinpath(".samples", "versions")
        return sorted(p.name for p in vdir.iterdir() if p.is_dir())


class InterruptionTest(SampleLayoutTest):
    def test_failed_generation_keeps_previous_set_replayable(self):
        root = self.work / "src"
        first = self.generate(("B-0001-p0", "B-0001-p1"), root)

        def explode(staging: Path) -> None:
            (staging / "B-0009-p0.json").write_text("{\"half\":", encoding="utf-8")
            raise RuntimeError("simulated crash while writing")

        with self.assertRaises(RuntimeError):
            sample_store.publish_samples(root, explode)

        # the manifest still selects the first set, and every shard is intact
        source = sample_store.open_source(root)
        self.addCleanup(source.close)
        self.assertEqual(source.version, first)
        self.assertEqual([p.name for p in source.paths],
                         ["B-0001-p0.json", "B-0001-p1.json"])
        for p in source.paths:
            json.loads(p.read_text(encoding="utf-8"))
        # no half-written staging directory survives
        self.assertFalse(any(p.name.startswith(sample_store.STAGING_PREFIX)
                             for p in (root / ".samples").iterdir()))

    def test_hard_kill_mid_generation_keeps_previous_set_and_reaps_litter(self):
        root = self.work / "src"
        first = self.generate(("B-0001-p0",), root)

        script = textwrap.dedent(f"""
            import sys, time
            sys.path.insert(0, {str(ROOT)!r})
            from app import sample_store
            from pathlib import Path

            def populate(staging):
                (Path(staging) / "B-0001-p0.json").write_text(
                    '{{"segment": "half"}}', encoding="utf-8")
                print("partial", flush=True)
                time.sleep(60)

            sample_store.publish_samples({str(root)!r}, populate)
        """)
        proc = subprocess.Popen([sys.executable, "-I", "-c", script],
                                stdout=subprocess.PIPE, text=True)
        try:
            self.assertEqual(proc.stdout.readline().strip(), "partial")
        finally:
            os.kill(proc.pid, signal.SIGKILL)
            proc.wait()
            proc.stdout.close()

        # the set selected before the kill is still the current, complete one
        source = sample_store.open_source(root)
        self.addCleanup(source.close)
        self.assertEqual(source.version, first)
        self.assertEqual(source.paths, [root / ".samples" / "versions" / first
                                        / "B-0001-p0.json"])
        # the half-written staging copy never leaked into the selected set
        body = source.paths[0].read_text(encoding="utf-8")
        self.assertNotIn("half", body)
        self.assertEqual(json.loads(body)["segment"], "B-0001-p0")

        # the next successful generation reaps the dead process's staging dir
        self.generate(("B-0002-p0",), root)
        leftovers = [p.name for p in (root / ".samples").iterdir()
                     if p.name.startswith(sample_store.STAGING_PREFIX)]
        self.assertEqual(leftovers, [])


class VersionFlipTest(SampleLayoutTest):
    def test_regeneration_keeps_user_files_even_same_named_ones(self):
        root = self.work / "src"
        root.mkdir(parents=True)
        user_note = root / "notes.txt"
        user_note.write_text("mine", encoding="utf-8")
        # a user file that collides with a generated shard's name
        colliding = root / "B-0001-p0.json"
        colliding.write_text('{"user": "data"}', encoding="utf-8")

        self.generate(("B-0001-p0", "B-0001-p1"), root)
        self.generate(("B-0001-p0", "B-0001-p1"), root)   # regenerate in place

        self.assertEqual(user_note.read_text(encoding="utf-8"), "mine")
        self.assertEqual(colliding.read_text(encoding="utf-8"), '{"user": "data"}')

        source = sample_store.open_source(root)
        self.addCleanup(source.close)
        self.assertNotIn(colliding.resolve(), [p.resolve() for p in source.paths])
        self.assertEqual([p.name for p in source.paths],
                         ["B-0001-p0.json", "B-0001-p1.json"])

    def test_generate_demo_preserves_existing_user_files(self):
        root = self.work / "src"
        root.mkdir()
        (root / "notes.txt").write_text("keep me", encoding="utf-8")
        (root / "B-0001-p0.json").write_text("user json", encoding="utf-8")

        replay.generate_demo(root)
        replay.generate_demo(root)

        self.assertEqual((root / "notes.txt").read_text(encoding="utf-8"),
                         "keep me")
        self.assertEqual((root / "B-0001-p0.json").read_text(encoding="utf-8"),
                         "user json")

    def test_replay_consumes_only_the_selected_version_across_a_flip(self):
        root = self.work / "src"
        v1 = self.generate(("B-0001-p0", "B-0001-p1"), root)
        stray = root / "unrelated.json"
        stray.write_text('{"segment": "stray"}', encoding="utf-8")

        v2 = {}

        def flip():
            v2["version"] = self.generate(("B-0002-p0",), root)

        sync = _RecordingSync(on_first=flip)
        replay.deliver(root, sync, verbose=False, duplicate_seed=1)

        self.assertIn("version", v2)
        offered = [p.resolve() for p in sync.offered]
        v1_dir = (root / ".samples" / "versions" / v1).resolve()
        v2_dir = (root / ".samples" / "versions" / v2["version"]).resolve()
        self.assertTrue(all(p.parent == v1_dir for p in offered), offered)
        self.assertEqual(sorted(p.name for p in offered),
                         ["B-0001-p0.json", "B-0001-p1.json"])
        self.assertNotIn(stray.resolve(), offered)
        self.assertFalse(any(p.parent == v2_dir for p in offered))
        # the flip protected the in-flight version from being reaped
        self.assertIn(v1, self.versions(root))

    def test_sync_cli_ignores_json_outside_the_selected_version(self):
        root = self.work / "src"
        replay.generate_demo(root)
        decoy = root / "decoy.json"
        decoy.write_text('{"segment": "decoy", "this": "is not a segment"}',
                         encoding="utf-8")

        db, data = self.work / "drain.db", self.work / "data"
        out = subprocess.run(
            [sys.executable, "-m", "app", "sync", "--db", str(db),
             "--data", str(data), "--from-source", str(root)],
            cwd=ROOT, capture_output=True, text=True, timeout=120)
        self.assertEqual(out.returncode, 0, out.stderr)
        self.assertNotIn("decoy", out.stdout)
        self.assertNotIn("decoy", out.stderr)
        # the managed set itself is complete and published up to B-0005
        status = subprocess.run(
            [sys.executable, "-m", "app", "status", "--db", str(db)],
            cwd=ROOT, capture_output=True, text=True, timeout=60)
        self.assertEqual(json.loads(status.stdout)["snapshot"]["published_batch"],
                         "B-0005")
        # the user's decoy was not consumed, moved or deleted
        self.assertTrue(decoy.exists())


class CleanupTest(SampleLayoutTest):
    def test_repeated_flips_keep_only_the_current_version(self):
        root = self.work / "src"
        sizes = []
        for n in range(1, 8):
            self.generate((f"B-{n:04d}-p0",), root)
            sizes.append(sum(1 for p in (root / ".samples").rglob("*")
                             if p.is_file()))
        self.assertEqual(len(self.versions(root)), 1)
        # storage stays flat instead of growing with the generation count
        self.assertLessEqual(max(sizes), sizes[0] + 2)

    def test_identical_regeneration_does_not_create_a_new_version(self):
        root = self.work / "src"
        first = self.generate(("B-0001-p0",), root)
        for _ in range(3):
            self.assertEqual(self.generate(("B-0001-p0",), root), first)
        self.assertEqual(self.versions(root), [first])
        # the return value is still a usable, complete path list
        paths = sample_store.SampleStore(root).segment_paths(first)
        self.assertEqual([p.name for p in paths], ["B-0001-p0.json"])

    def test_live_lease_keeps_in_flight_version_readable(self):
        root = self.work / "src"
        old = self.generate(("B-0001-p0",), root)

        source = sample_store.open_source(root)
        self.addCleanup(source.close)
        old_paths = list(source.paths)

        new = self.generate(("B-0002-p0",), root)
        self.assertNotEqual(new, old)
        # not reaped while leased, and the bytes are still readable
        self.assertIn(old, self.versions(root))
        for p in old_paths:
            self.assertTrue(p.is_file())
            json.loads(p.read_text(encoding="utf-8"))

        # once the lease is released, a later flip collects it
        source.close()
        self.generate(("B-0003-p0",), root)
        self.assertNotIn(old, self.versions(root))

    def test_stale_lease_from_a_dead_process_is_reaped(self):
        root = self.work / "src"
        dead = self.generate(("B-0001-p0",), root)
        store = self.store(root)
        store.leases_dir.mkdir(parents=True, exist_ok=True)
        (store.leases_dir / "stale.json").write_text(json.dumps({
            "version": dead, "pid": 999_999_999,
            "host": os.uname().nodename, "created": 0.0}), encoding="utf-8")

        self.generate(("B-0002-p0",), root)
        self.assertNotIn(dead, self.versions(root))
        self.assertEqual(list(store.leases_dir.iterdir()), [])

    def test_expired_foreign_host_lease_does_not_pin_a_version(self):
        root = self.work / "src"
        dead = self.generate(("B-0001-p0",), root)
        store = self.store(root)
        store.leases_dir.mkdir(parents=True, exist_ok=True)
        lease = store.leases_dir / "foreign.json"
        lease.write_text(json.dumps({"version": dead, "pid": 1,
                                     "host": "some-other-container"}),
                         encoding="utf-8")
        old = 1_000_000_000.0
        os.utime(lease, (old, old))                 # older than any TTL

        self.generate(("B-0002-p0",), root)
        self.assertNotIn(dead, self.versions(root))

    def test_fresh_foreign_host_lease_still_protects(self):
        root = self.work / "src"
        held = self.generate(("B-0001-p0",), root)
        store = self.store(root)
        store.leases_dir.mkdir(parents=True, exist_ok=True)
        (store.leases_dir / "foreign.json").write_text(json.dumps({
            "version": held, "pid": 1, "host": "some-other-container"}),
            encoding="utf-8")

        self.generate(("B-0002-p0",), root)
        self.assertIn(held, self.versions(root))


class BareSourceTest(SampleLayoutTest):
    def test_bare_directory_keeps_legacy_glob_behaviour(self):
        root = self.work / "bare"
        replay.write_segments(
            {"batch": "B-0001", "seq": 1, "total": 1, "parts": 1,
             "items": {"taps": taps("J-000001")}}, root)
        source = sample_store.open_source(root)
        self.addCleanup(source.close)
        self.assertFalse(source.managed)
        self.assertEqual([p.name for p in source.paths], ["B-0001-p0.json"])

        # watch-mode semantics: a file dropped in later is picked up
        replay.write_segments(
            {"batch": "B-0002", "seq": 2, "total": 2, "parts": 1,
             "items": {"taps": taps("J-000002")}}, root)
        self.assertEqual(sorted(p.name for p in source.current_paths()),
                         ["B-0001-p0.json", "B-0002-p0.json"])

    def test_bare_directory_delivers_every_json(self):
        root = self.work / "bare"
        replay.write_segments(
            {"batch": "B-0001", "seq": 1, "total": 2, "parts": 1,
             "items": {"taps": taps("J-000001")}}, root)
        replay.write_segments(
            {"batch": "B-0002", "seq": 2, "total": 2, "parts": 1,
             "items": {"taps": taps("J-000002")}}, root)
        svc = self.make_svc()
        stats = replay.deliver(root, svc, verbose=False)
        self.assertEqual(len(stats["accepted"]), 2)
        self.assertEqual(svc.status()["snapshot"]["published_batch"], "B-0002")

    def test_generated_set_replays_to_the_expected_snapshot(self):
        """The managed layout is transparent to the shipped demo flow."""
        src = self.work / "source"
        replay.generate_demo(src)
        svc = self.make_svc()
        stats = replay.deliver(src, svc, verbose=False)
        self.assertEqual(stats["rejected"], [])
        self.assertEqual(svc.status()["snapshot"]["published_batch"], "B-0005")


if __name__ == "__main__":
    unittest.main()
