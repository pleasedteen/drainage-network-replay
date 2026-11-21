"""Shared helpers for the unittest suite (standard library only)."""

from __future__ import annotations

import json
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from app import replay, service  # noqa: E402

SECTIONS = replay.SECTIONS


class TempDirTest(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory(prefix="drain-test-")
        self.work = Path(self._tmp.name)
        self.addCleanup(self._tmp.cleanup)

    @property
    def inbox_db(self):
        return self.work / "inbox.db"

    @property
    def publish_db(self):
        return self.work / "publish.db"

    def make_svc(self, *, auto_publish=True):
        svc = service.SyncService(self.inbox_db, self.publish_db,
                                  self.work / "data",
                                  auto_publish=auto_publish)
        self.addCleanup(lambda: None)
        return svc

    # -- fixtures ----------------------------------------------------------
    def write_batch(self, batch, seq, total, *, parts=1, upgrade=False,
                    items=None, name=None):
        items = items or {}
        return replay.write_segments(
            {"batch": batch, "seq": seq, "total": total, "parts": parts,
             "upgrade": upgrade,
             "items": {k: items.get(k, []) for k in SECTIONS}},
            self.work / (name or "src"))

    def segment_file(self, name, payload, subdir=""):
        p = self.work / subdir / name
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
        return p

    def segment(self, name, batch, seq, total, part=0, parts=1, upgrade=False,
                **sections):
        payload = {"segment": name, "batch": batch, "seq": seq, "total": total,
                   "part": part, "parts": parts, "upgrade": upgrade}
        payload.update({k: v for k, v in sections.items() if v})
        return self.segment_file(name + ".json", payload)


def taps(*refs):
    return [{"ref": r} for r in refs]


def chain(start=1, end=5, prefix="P-"):
    return [{"code": f"{prefix}{i:06d}", "from": f"J-{i:06d}",
             "to": f"J-{i + 1:06d}"} for i in range(start, end)]
