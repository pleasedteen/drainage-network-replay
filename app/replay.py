"""Offline replay source.

Simulates the upstream engineering system that ships log segments:

* it owns a set of engineering batches, each split into several ``part``
  segments (so "单批日志可能超过同步进程可用内存" is exercised for real);
* it delivers segments in a deliberately hostile order -- parts out of order,
  duplicate deliveries, and batches that arrive before their predecessors;
* it writes each segment into a drop directory, asks the sync service to
  accept it, and only deletes its copy after the service acknowledges;
* it can be killed at any point and re-run: already-acknowledged segments are
  simply not re-sent, and re-sending is harmless anyway.

Generated sets are *versioned* (see :mod:`app.sample_store`): regeneration
never clears the target directory, keeps every pre-existing user file even
when it shares a name with a shard, and installs the new set atomically.  A
delivery run pins the version it selected, so JSON files belonging to another
version -- or to the user -- never enter the confirm/publish path.

A batch file looks like::

    {"batch": "B-0004", "seq": 4, "total": 6, "upgrade": true,
     "parts": 3, "items": {"ids": [...], "taps": [...], ...}}

``write_segments`` turns one such file into up to ``parts`` segment files,
``deliver`` drives delivery, and ``generate_demo`` writes the sample data set
under ``examples/sample_batches``.
"""

from __future__ import annotations

import argparse
import json
import random
import sys
from pathlib import Path

from . import sample_store

SECTIONS = ("ids", "taps", "lays", "redirects", "retires")


# ---------------------------------------------------------------------------
# batch file -> segment files
# ---------------------------------------------------------------------------

def write_segments(batch: dict, out_dir: Path) -> list[Path]:
    """Split one batch description into ``parts`` segment files."""
    out_dir.mkdir(parents=True, exist_ok=True)
    items = batch.get("items", {})
    parts = int(batch.get("parts", 1))
    if parts < 1:
        raise ValueError("parts must be >= 1")
    chunks = {name: _split(items.get(name, []), parts) for name in SECTIONS}
    paths = []
    for p in range(parts):
        seg = {
            "segment": f"{batch['batch']}-p{p}",
            "batch": batch["batch"],
            "seq": batch["seq"],
            "total": batch["total"],
            "part": p,
            "parts": parts,
            "upgrade": bool(batch.get("upgrade", False)),
            "producer": batch.get("producer", "offline-replay"),
            "sent_at": batch.get("sent_at", "2026-11-19T00:00:00Z"),
        }
        for name in SECTIONS:
            if chunks[name][p]:
                seg[name] = chunks[name][p]
        path = out_dir / f"{batch['batch']}-p{p}.json"
        with open(path, "w", encoding="utf-8") as fp:
            json.dump(seg, fp, ensure_ascii=False, separators=(",", ":"))
        paths.append(path)
    return paths


def _split(items: list, parts: int) -> list[list]:
    """Contiguous split.  ``parts`` may exceed ``len(items)``; empty parts are
    still emitted so every batch truly spans ``parts`` segments."""
    out = [[] for _ in range(parts)]
    if not items:
        return out
    size = (len(items) + parts - 1) // parts
    for i, item in enumerate(items):
        out[min(i // size, parts - 1)].append(item)
    return out


# ---------------------------------------------------------------------------
# streaming segment writer (for segments too big to build in memory)
# ---------------------------------------------------------------------------

def write_streamed_segment(path, header: dict, sections: dict) -> Path:
    """Write one segment file without materialising its items.

    ``sections`` maps section name -> iterable of items (or a callable that
    returns a fresh iterator).  Items are serialised one at a time, exactly as
    a producer with a large batch would do.
    """
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as fp:
        fp.write("{")
        first_key = True
        for key in ("segment", "batch", "seq", "total", "part", "parts",
                    "upgrade", "producer", "sent_at"):
            if key in header:
                if not first_key:
                    fp.write(",")
                first_key = False
                fp.write(json.dumps(key))
                fp.write(":")
                json.dump(header[key], fp, ensure_ascii=False)
        for name in SECTIONS:
            items = sections.get(name)
            if not items:
                continue
            if callable(items):
                items = items()
            fp.write(",")
            fp.write(json.dumps(name))
            fp.write(":[")
            first = True
            for item in items:
                if not first:
                    fp.write(",")
                first = False
                json.dump(item, fp, ensure_ascii=False,
                          separators=(",", ":"))
            fp.write("]")
        fp.write("}")
    return path


# ---------------------------------------------------------------------------
# delivery
# ---------------------------------------------------------------------------

def deliver(source_dir: Path, sync, *, order=None, duplicate_every=0,
            duplicate_seed=7, verbose=True) -> dict:
    """Deliver every segment of the selected sample set through ``sync``.

    ``source_dir`` may be a managed sample directory or a bare directory of
    ``*.json`` segments.  When it is managed, exactly the version selected at
    the start of the run is consumed: a generation that flips the pointer
    mid-run neither truncates the queue nor feeds the new set's files (or any
    unrelated JSON) into confirmation/publishing.
    """
    with sample_store.open_source(source_dir) as source:
        paths = list(source.paths)
        if order is not None:
            paths = order(paths)
        else:
            paths = _shuffle_like_source(paths, duplicate_seed)

        acked, dup, rejected = [], 0, []
        rng = random.Random(duplicate_seed)
        queue = list(paths)
        while queue:
            p = queue.pop(0)
            try:
                out = sync.accept(p)
            except Exception as exc:  # noqa: BLE001
                rejected.append((p.name, str(exc)))
                if verbose:
                    print(f"REJECTED  {p.name}: {exc}")
                continue
            if out["status"] == "duplicate":
                dup += 1
                if verbose:
                    print(f"duplicate {p.name}: {out.get('reason', '')}")
                continue
            acked.append(out["segment"])
            if verbose:
                applied = ",".join(b["batch"]
                                   for b in (out.get("publish") or {}).get("applied", []))
                print(f"accepted  {p.name} -> {out['segment']} "
                      f"(batch {out['batch']} part {out['part']}/{out['parts']}"
                      f"{', published ' + applied if applied else ''})")
            if duplicate_every and rng.random() < 1.0 / duplicate_every:
                queue.insert(min(len(queue), rng.randrange(0, len(queue) + 1)), p)
        return {"accepted": acked, "duplicates": dup, "rejected": rejected}


def _shuffle_like_source(paths, seed):
    """Deterministic hostile order: batches interleaved, parts reversed."""
    rng = random.Random(seed)
    paths = list(paths)
    rng.shuffle(paths)
    return paths


# ---------------------------------------------------------------------------
# demo data set
# ---------------------------------------------------------------------------

def _attrs(**kw):
    return kw


def generate_demo(root: Path) -> list[Path]:
    """Write the demo batches used by ``examples/README.md``.

    The set tells a small story: a legacy network, a batch that adds joints
    and redirects existing pipes, the numbering upgrade, a batch written in
    both notations, a batch that retires a joint (cascading to its pipes), and
    a final batch that the source has only half delivered -- so the demo ends
    with a published snapshot plus a visible "waiting for parts" blocker.

    ``root`` is never cleared: generation writes into a private version and
    flips the manifest to it atomically, so pre-existing user files (including
    ones named like a segment) survive and a hard kill mid-write leaves the
    previous version replayable.  Returns the shard paths of the new version.
    """
    root = Path(root)
    root.mkdir(parents=True, exist_ok=True)
    version = sample_store.publish_samples(root, _populate_demo)
    return sample_store.SampleStore(root).segment_paths(version)


def _populate_demo(staging: Path) -> None:
    batches = []

    # -- B-0001: the legacy network (global J- ids only) --------------------
    taps, lays = [], []
    for n in range(1001, 1021):
        taps.append({"ref": f"J-{n:06d}",
                     "attrs": {"kind": "污水", "district": "城东"}})
    for n in range(1001, 1019):
        lays.append({"code": f"P-{n:06d}", "from": f"J-{n:06d}",
                     "to": f"J-{n + 1:06d}", "attrs": {"dn": 300 + (n % 5) * 100}})
    taps.append({"ref": "J-001099", "attrs": {"kind": "雨水"}})
    lays += [
        {"code": "P-090001", "from": "J-001015", "to": "J-001006",
         "attrs": {"dn": 400}},
        {"code": "P-090002", "from": "J-001020", "to": "J-001012",
         "attrs": {"dn": 500}},
        {"code": "P-090003", "from": "J-001007", "to": "J-001003",
         "attrs": {"dn": 600}},
        {"code": "P-090099", "from": "J-001099", "to": "J-001002",
         "attrs": {"dn": 300}},
    ]
    batches.append({"batch": "B-0001", "seq": 1, "total": 6, "upgrade": False,
                    "parts": 2,
                    "items": {"ids": [], "taps": taps, "lays": lays,
                              "redirects": [], "retires": []}})

    # -- B-0002: new joints + 改向 (both verbs) + a retirement -------------
    batches.append({
        "batch": "B-0002", "seq": 2, "total": 6, "upgrade": False, "parts": 1,
        "items": {
            "ids": [],
            "taps": [{"ref": "J-001030", "attrs": {"kind": "污水"}},
                     {"ref": "J-001031", "attrs": {"kind": "雨水"}}],
            "lays": [
                {"code": "P-090100", "from": "J-001030", "to": "J-001010"},
                {"code": "P-090101", "from": "J-001031", "to": "J-001030"},
                # re-laying an existing code moves its upstream end
                {"code": "P-090001", "from": "J-001015", "to": "J-001004"},
            ],
            "redirects": [
                # joint redirect: everything that left J-001099 now leaves
                # J-001012 (P-090099: 1099->1002 becomes 1012->1002)
                {"from": "J-001099", "to": "J-001012"},
                # pipe redirect: P-090002 now ends at J-001011
                {"code": "P-090002",
                 "to": {"from": "J-001020", "to": "J-001011"}},
            ],
            "retires": [{"kind": "pipe", "code": "P-090003"}],
        }})

    # -- B-0003: the numbering upgrade ------------------------------------
    ids, taps = [], []
    for n in range(1001, 1021):
        zone = "C4" if n <= 1012 else "C5"
        ids.append({"old": f"J-{n:06d}", "new": f"{zone}:{n:05d}"})
        taps.append({"ref": f"J-{n:06d}", "zone": zone, "local": f"{n:05d}"})
    for n in (1030, 1031):
        ids.append({"old": f"J-{n:06d}", "new": f"C4:{n:05d}"})
        taps.append({"ref": f"J-{n:06d}", "zone": "C4", "local": f"{n:05d}"})
    ids.append({"old": "J-001099", "new": "C5:00099"})
    taps.append({"ref": "J-001099", "zone": "C5", "local": "00099"})
    batches.append({"batch": "B-0003", "seq": 3, "total": 6, "upgrade": True,
                    "parts": 2,
                    "items": {"ids": ids, "taps": taps, "lays": [],
                              "redirects": [], "retires": []}})

    # -- B-0004: post-upgrade changes, old and new spellings mixed --------
    batches.append({
        "batch": "B-0004", "seq": 4, "total": 6, "upgrade": False, "parts": 1,
        "items": {
            "ids": [],
            "taps": [
                {"ref": "J-001003", "attrs": {"note": "旧号仍可写"}},
                {"ref": "C4:01005", "attrs": {"kind": "污水", "district": "城东"}},
                {"ref": "C5:01015"},
            ],
            "lays": [
                {"code": "P-090200", "from": "J-001031", "to": "C4:01002"},
                {"code": "P-090201", "from": "C5:01015", "to": "J-001018"},
                {"code": "P-090202", "from": "J-001004", "to": "C4:01011"},
            ],
            "redirects": [
                # pipe re-pointed using the legacy spelling of its upstream end
                {"code": "P-090002",
                 "to": {"from": "J-001020", "to": "C5:01015"}},
                # joint redirect written with a legacy id
                {"from": "J-001016", "to": "C4:01004"},
            ],
            "retires": [],
        }})

    # -- B-0005: a retirement that cascades to attached pipes -------------
    batches.append({
        "batch": "B-0005", "seq": 5, "total": 6, "upgrade": False, "parts": 1,
        "items": {
            "ids": [],
            "taps": [{"ref": "C4:01040", "attrs": {"kind": "污水",
                                                  "district": "城东"}}],
            "lays": [{"code": "P-090300", "from": "C4:01040", "to": "J-001008"}],
            "redirects": [],
            "retires": [{"kind": "joint", "ref": "C4:01009"}],
        }})

    # -- B-0006: half delivered by the source -----------------------------
    batches.append({
        "batch": "B-0006", "seq": 6, "total": 6, "upgrade": False, "parts": 2,
        "items": {
            "ids": [], "taps": [{"ref": "C4:01019", "attrs": {"note": "later"}}],
            "lays": [], "redirects": [], "retires": [],
        }})

    paths = []
    for b in batches:
        paths += write_segments(b, staging)
    # the source holds back B-0006 part 1 in the demo
    held = staging / "B-0006-p1.json"
    if held.exists():
        held.unlink()
    (staging / "HELD-BACK.txt").write_text(
        "B-0006-p1.json is deliberately not delivered in this demo.\n"
        "The published snapshot stays at seq 5 while the source finishes the\n"
        "batch, and traces keep working.\n", encoding="utf-8")


def main(argv=None):
    ap = argparse.ArgumentParser(description="offline replay source")
    ap.add_argument("--out", default="examples/sample_batches",
                    help="directory to write generated segments into")
    ap.add_argument("--deliver-to", default=None,
                    help="if set: replay --out into this DB")
    ap.add_argument("--data", default=None)
    ap.add_argument("--duplicate-every", type=int, default=0)
    ap.add_argument("--quiet", action="store_true")
    args = ap.parse_args(argv)

    if args.deliver_to is None:
        paths = generate_demo(Path(args.out))
        print(f"wrote {len(paths)} segment files to {args.out}")
        return 0

    from .service import SyncService
    svc = SyncService(args.deliver_to, args.data or str(Path(args.deliver_to).parent / "data"))
    stats = deliver(Path(args.out), svc, duplicate_every=args.duplicate_every,
                    verbose=not args.quiet)
    print(json.dumps({"accepted": len(stats["accepted"]),
                      "duplicates": stats["duplicates"],
                      "rejected": stats["rejected"]}, ensure_ascii=False,
                     indent=1))
    return 0


if __name__ == "__main__":
    sys.exit(main())
