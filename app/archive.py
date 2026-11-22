"""Receiving segments: content-addressed landing plus a replay index.

A segment that passes validation is

1. stored byte-for-byte under ``<data>/segments/`` (content-addressed, so a
   repeated delivery of the identical bytes costs one hash), and
2. recorded in the ``segments`` table together with the byte offsets of its
   array sections (``segment_sections``), inside the *same* transaction.

Only after that transaction commits does the receiver answer ``accepted`` --
i.e. the source may drop its copy.  A crash before the commit rolls back the
segment record, so redelivery is always safe; the landed file itself is
content-addressed and simply reused by the next delivery.

No per-item copy is kept anywhere: publishing re-reads the landed archive,
one section at a time, straight from disk.  A large segment is therefore
never held in memory -- validation streams it item by item, and so does the
later replay.
"""

from __future__ import annotations

import hashlib
import os
import shutil
import tempfile
import time
from dataclasses import dataclass, field
from pathlib import Path

from . import jsonstream, wire
from .db import connect


class LandError(wire.SegmentError):
    """A segment that cannot be acknowledged for a reason other than shape."""


@dataclass
class LandResult:
    status: str                # 'accepted' | 'duplicate'
    segment: str
    batch: str
    seq: int
    total: int
    part: int
    parts: int
    upgrade: bool
    digest: str
    acknowledged: bool
    reason: str = ""
    counts: dict = field(default_factory=dict)

    def as_dict(self) -> dict:
        return {
            "status": self.status,
            "acknowledged": self.acknowledged,
            "segment": self.segment,
            "batch": self.batch,
            "seq": self.seq,
            "total": self.total,
            "part": self.part,
            "parts": self.parts,
            "upgrade": self.upgrade,
            "digest": self.digest,
            "items": self.counts,
            "reason": self.reason,
        }


@dataclass
class _Scan:
    header: wire.Header
    counts: dict
    digest: str
    spans: dict                # section -> [[byte_offset, items], ...]


def _now() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def _file_sha256(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as fp:
        for chunk in iter(lambda: fp.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def _scan(path: Path, *, chunk_size: int) -> _Scan:
    """Validate every item, build the logical digest, remember where each
    array section starts so the archive can be replayed section by section."""
    fields: dict = {}
    counts = {name: 0 for name in wire.SEGMENT_ARRAYS}
    spans: dict[str, list] = {}
    h = hashlib.sha256(b"segment-v1\0")

    def on_field(key, value):
        if key in wire.SEGMENT_ARRAYS:
            raise wire.SegmentError(f"{key!r} must be an array", field=key)
        if key == "sent_at":
            if not isinstance(value, (str, int, float)):
                raise wire.SegmentError("sent_at must be a string or number",
                                        field=key)
            return
        fields[key] = value

    def on_array_start(key, offset):
        if key in wire.SEGMENT_ARRAYS:
            spans.setdefault(key, []).append([offset, 0])

    def on_item(section, item):
        idx = counts[section]
        row = wire.validate_item(section, item, idx)
        counts[section] = idx + 1
        spans[section][-1][1] += 1
        h.update(b"\n" + section.encode() + b":")
        h.update(wire.canonical(row))

    try:
        with open(path, "rb") as fp:
            jsonstream.iter_segment(fp, chunk_size=chunk_size,
                                    on_field=on_field, on_array_item=on_item,
                                    on_array_start=on_array_start)
    except jsonstream.JsonStreamError as exc:
        raise wire.SegmentError(f"malformed JSON: {exc}") from exc
    header = wire.validate_header(fields)
    h.update(b"\nheader:")
    h.update(wire.canonical({k: getattr(header, k) for k in wire._HEADER_KEYS}))
    return _Scan(header, counts, h.hexdigest(), spans)


def _existing_row(conn, seg_id, digest):
    return conn.execute(
        "SELECT seg_id,batch_id,part,parts,seq,total,upgrade,sha256 "
        "FROM segments WHERE seg_id=? OR sha256=?",
        (seg_id, digest)).fetchone()


def _describe_duplicate(row, digest: str, reason: str, counts: dict) -> LandResult:
    return LandResult("duplicate", row["seg_id"], row["batch_id"], row["seq"],
                      row["total"], row["part"], row["parts"],
                      bool(row["upgrade"]), digest, False, reason, counts)


def _check_batch_declaration(conn, header: wire.Header) -> None:
    if conn.execute("SELECT 1 FROM batches WHERE seq=? AND batch_id<>?",
                    (header.seq, header.batch)).fetchone() is not None:
        raise LandError(
            f"batch {header.batch!r} claims seq {header.seq}, which is already "
            f"held by another batch: batch ids and their order must agree")
    row = conn.execute("SELECT seq,total,upgrade FROM batches WHERE batch_id=?",
                       (header.batch,)).fetchone()
    if row is None:
        return
    if (row["seq"], row["total"]) != (header.seq, header.total):
        raise LandError(
            f"batch {header.batch!r} was declared as seq {row['seq']}/"
            f"{row['total']} but this segment says {header.seq}/{header.total}")
    if bool(row["upgrade"]) != header.upgrade:
        raise LandError(
            f"batch {header.batch!r} upgrade flag disagrees with earlier "
            f"segments of the same batch")


def _parts_expected(conn, header: wire.Header) -> int:
    row = conn.execute("SELECT MAX(parts) FROM segments WHERE batch_id=?",
                       (header.batch,)).fetchone()
    seen = row[0]
    if seen is not None and seen != header.parts:
        raise LandError(
            f"batch {header.batch!r} spans {seen} parts, but segment "
            f"{header.segment!r} says {header.parts}")
    return header.parts


def _record_segment(conn, header: wire.Header, scan: _Scan, nbytes: int,
                    stored_path: str) -> None:
    """Insert the segment row and its replay index.  Caller owns the
    transaction; the landed archive file plus these rows are the whole
    acknowledgement."""
    conn.execute(
        "INSERT INTO segments(seg_id, sha256, batch_id, part, parts, "
        "seq, total, upgrade, nbytes, path, received_at, ingested) "
        "VALUES(?,?,?,?,?,?,?,?,?,?,?,'archived')",
        (header.segment, scan.digest, header.batch, header.part,
         header.parts, header.seq, header.total, int(header.upgrade),
         nbytes, stored_path, _now()))
    for section, arrays in scan.spans.items():
        for offset, items in arrays:
            conn.execute(
                "INSERT INTO segment_sections(seg_id,section,byte_offset,"
                "items) VALUES(?,?,?,?)",
                (header.segment, section, offset, items))


def land_segment(db_path, data_dir, source_path, *, chunk_size: int = 65536,
                 copy: bool = True) -> LandResult:
    """Validate, land and record one segment.  Idempotent.

    Raises ``wire.SegmentError`` or ``LandError`` for anything that must not
    be acknowledged.
    """
    src = Path(source_path)
    data = Path(data_dir)
    seg_dir = data / "segments"
    seg_dir.mkdir(parents=True, exist_ok=True)

    raw_sha = _file_sha256(src)
    nbytes = os.path.getsize(src)

    conn = connect(db_path)
    try:
        row = _existing_row(conn, None, raw_sha)
        if row is not None:
            return _describe_duplicate(row, raw_sha, "identical bytes already "
                                       "acknowledged", {})

        scan = _scan(src, chunk_size=chunk_size)
        header = scan.header

        row = _existing_row(conn, header.segment, scan.digest)
        if row is not None:
            same = row["seg_id"] == header.segment
            reason = ("same segment id and content already acknowledged"
                      if same else
                      "identical content already acknowledged under another "
                      "segment id")
            return _describe_duplicate(row, scan.digest, reason, scan.counts)

        row = conn.execute("SELECT seg_id FROM segments WHERE seg_id=?",
                           (header.segment,)).fetchone()
        if row is not None:
            raise LandError(
                f"segment id {header.segment!r} was already acknowledged with "
                f"different content")

        _check_batch_declaration(conn, header)
        parts_expected = _parts_expected(conn, header)
        row = conn.execute("SELECT seg_id FROM segments WHERE batch_id=? "
                           "AND part=?", (header.batch, header.part)).fetchone()
        if row is not None:
            raise LandError(
                f"batch {header.batch!r} part {header.part} was already "
                f"acknowledged by segment {row['seg_id']!r} with different "
                f"content")

        final = seg_dir / f"{raw_sha}.json"
        if not final.exists():
            if copy:
                # unique temp name: concurrent deliveries of the same bytes
                # must not trip over each other
                fd, tmp = tempfile.mkstemp(dir=seg_dir, prefix=".ingest-",
                                           suffix=".tmp")
                try:
                    with open(src, "rb") as fin, os.fdopen(fd, "wb") as fout:
                        shutil.copyfileobj(fin, fout, 1 << 20)
                    os.replace(tmp, final)
                except BaseException:
                    if os.path.exists(tmp):
                        os.unlink(tmp)
                    raise
            else:
                final = src

        conn.execute("BEGIN IMMEDIATE")
        try:
            # re-check under the write lock: another receiver may have
            # committed the same segment while this one was scanning
            row = _existing_row(conn, header.segment, scan.digest)
            if row is not None:
                conn.execute("ROLLBACK")
                return _describe_duplicate(row, scan.digest,
                                           "delivered concurrently and "
                                           "acknowledged by another receiver",
                                           scan.counts)
            row = conn.execute("SELECT seg_id FROM segments WHERE seg_id=?",
                               (header.segment,)).fetchone()
            if row is None:
                row = conn.execute("SELECT seg_id FROM segments WHERE "
                                   "batch_id=? AND part=?",
                                   (header.batch, header.part)).fetchone()
            if row is not None:
                raise LandError(
                    f"batch {header.batch!r} part {header.part} was "
                    f"acknowledged concurrently by segment {row['seg_id']!r} "
                    f"with different content")
            _check_batch_declaration(conn, header)
            _parts_expected(conn, header)
            conn.execute(
                "INSERT INTO batches(batch_id, seq, total, upgrade, state, "
                "parts_expected, parts_received, segments, created_at) "
                "VALUES(?,?,?,?,'staging',?,0,0,?) "
                "ON CONFLICT(batch_id) DO NOTHING",
                (header.batch, header.seq, header.total, int(header.upgrade),
                 parts_expected, _now()))
            _record_segment(conn, header, scan, nbytes,
                            str(final.relative_to(data)) if copy
                            else str(final))
            conn.execute(
                "UPDATE batches SET parts_expected=MAX(parts_expected,?) "
                "WHERE batch_id=?", (parts_expected, header.batch))
            conn.execute(
                "UPDATE batches SET parts_received=(SELECT COUNT(DISTINCT part) "
                "FROM segments WHERE batch_id=?), segments=(SELECT COUNT(*) "
                "FROM segments WHERE batch_id=?) WHERE batch_id=?",
                (header.batch, header.batch, header.batch))
            conn.execute("COMMIT")
        except BaseException:
            if conn.in_transaction:
                conn.execute("ROLLBACK")
            raise
        return LandResult("accepted", header.segment, header.batch, header.seq,
                          header.total, header.part, header.parts,
                          header.upgrade, scan.digest, True, "", scan.counts)
    finally:
        conn.close()


def batch_status(conn, batch_id: str) -> dict | None:
    row = conn.execute("SELECT * FROM batches WHERE batch_id=?",
                       (batch_id,)).fetchone()
    if row is None:
        return None
    parts = [r["part"] for r in conn.execute(
        "SELECT DISTINCT part FROM segments WHERE batch_id=? ORDER BY part",
        (batch_id,))]
    return {
        "batch": row["batch_id"],
        "seq": row["seq"],
        "total": row["total"],
        "upgrade": bool(row["upgrade"]),
        "state": row["state"],
        "error": row["error"],
        "parts_expected": row["parts_expected"],
        "parts_received": sorted(parts),
        "complete": len(parts) == row["parts_expected"],
        "segments": conn.execute("SELECT COUNT(*) FROM segments WHERE batch_id=?",
                                 (batch_id,)).fetchone()[0],
    }
