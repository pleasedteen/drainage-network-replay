"""Migrating the old single database into the split inbox/publish pair.

The pre-split service kept everything -- landed segments, staging rows, the
published network and the batch bookkeeping -- in one SQLite file.  This
module

* knows the old layout (``LEGACY_SCHEMA``),
* copies it into a fresh inbox database (receive progress, segments,
  staging) and a fresh publish database (network, id mappings, effects,
  publish ledger), and
* verifies the split afterwards: snapshot, id mapping, topology and batch
  basis must match the old database exactly, and batches that were fully
  received but unpublished -- or acknowledged but still missing parts --
  keep their progress so they resume from where they were.

The legacy database is only ever opened read-only; migration never writes
back to it.

``build_legacy_from_split`` exists for fixtures and tests: it merges a split
pair back into one old-format file, so a realistic "old database" can be
produced with the current code.
"""

from __future__ import annotations

import hashlib
import os

from .db import connect, init_inbox, init_publish
from . import tracing

# the old single-database layout, exactly as the pre-split service wrote it
LEGACY_SCHEMA = """
CREATE TABLE IF NOT EXISTS batches (
    batch_id       TEXT NOT NULL,
    seq            INTEGER NOT NULL,
    total          INTEGER NOT NULL,
    upgrade        INTEGER NOT NULL,
    state          TEXT NOT NULL,
    error          TEXT,
    parts_expected INTEGER NOT NULL,
    parts_received INTEGER NOT NULL DEFAULT 0,
    segments       INTEGER NOT NULL DEFAULT 0,
    created_at     TEXT NOT NULL,
    applied_at     TEXT,
    PRIMARY KEY (batch_id),
    UNIQUE (seq)
);
CREATE TABLE IF NOT EXISTS segments (
    seg_id      TEXT PRIMARY KEY,
    sha256      TEXT NOT NULL UNIQUE,
    batch_id    TEXT NOT NULL,
    part        INTEGER NOT NULL,
    parts       INTEGER NOT NULL,
    seq         INTEGER NOT NULL,
    total       INTEGER NOT NULL,
    upgrade     INTEGER NOT NULL,
    nbytes      INTEGER NOT NULL,
    path        TEXT NOT NULL,
    received_at TEXT NOT NULL,
    ingested    INTEGER NOT NULL DEFAULT 0,
    UNIQUE (batch_id, part)
);
CREATE INDEX IF NOT EXISTS segments_batch ON segments(batch_id, part);
CREATE TABLE IF NOT EXISTS st_ids (
    batch_id TEXT NOT NULL, part INTEGER NOT NULL, item INTEGER NOT NULL,
    old_ref TEXT NOT NULL, new_ref TEXT NOT NULL,
    PRIMARY KEY (batch_id, part, item)
) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS st_taps (
    batch_id TEXT NOT NULL, part INTEGER NOT NULL, item INTEGER NOT NULL,
    ref TEXT NOT NULL, zone TEXT, local TEXT, attrs TEXT,
    PRIMARY KEY (batch_id, part, item)
) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS st_lays (
    batch_id TEXT NOT NULL, part INTEGER NOT NULL, item INTEGER NOT NULL,
    code TEXT NOT NULL, from_ref TEXT NOT NULL, to_ref TEXT NOT NULL, attrs TEXT,
    PRIMARY KEY (batch_id, part, item)
) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS st_redirects (
    batch_id TEXT NOT NULL, part INTEGER NOT NULL, item INTEGER NOT NULL,
    kind TEXT NOT NULL, code TEXT, from_ref TEXT, to_ref TEXT,
    PRIMARY KEY (batch_id, part, item)
) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS st_retires (
    batch_id TEXT NOT NULL, part INTEGER NOT NULL, item INTEGER NOT NULL,
    kind TEXT NOT NULL, ref TEXT NOT NULL,
    PRIMARY KEY (batch_id, part, item)
) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS joints (
    uid         INTEGER PRIMARY KEY AUTOINCREMENT,
    zone        TEXT NOT NULL,
    local       TEXT NOT NULL,
    created_seq INTEGER NOT NULL,
    last_seq    INTEGER NOT NULL,
    retired_seq INTEGER,
    attrs       TEXT,
    UNIQUE (zone, local)
);
CREATE INDEX IF NOT EXISTS joints_local ON joints(local);
CREATE INDEX IF NOT EXISTS joints_zone_local ON joints(zone, local);
CREATE TABLE IF NOT EXISTS pipes (
    pc          INTEGER PRIMARY KEY AUTOINCREMENT,
    code        TEXT NOT NULL UNIQUE,
    from_uid    INTEGER NOT NULL REFERENCES joints(uid),
    to_uid      INTEGER NOT NULL REFERENCES joints(uid),
    created_seq INTEGER NOT NULL,
    last_seq    INTEGER NOT NULL,
    retired_seq INTEGER,
    retired_by  TEXT,
    attrs       TEXT
);
CREATE INDEX IF NOT EXISTS pipes_to ON pipes(to_uid);
CREATE INDEX IF NOT EXISTS pipes_from ON pipes(from_uid);
CREATE TABLE IF NOT EXISTS joint_refs (
    ref      TEXT PRIMARY KEY,
    uid      INTEGER NOT NULL REFERENCES joints(uid),
    kind     TEXT NOT NULL,
    since_seq INTEGER NOT NULL
) WITHOUT ROWID;
CREATE INDEX IF NOT EXISTS joint_refs_kind_uid ON joint_refs(kind, uid);
CREATE INDEX IF NOT EXISTS joint_refs_uid ON joint_refs(uid);
CREATE TABLE IF NOT EXISTS meta (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS batch_effects (
    batch_id TEXT NOT NULL,
    seq      INTEGER NOT NULL,
    kind     TEXT NOT NULL,
    n        INTEGER NOT NULL,
    PRIMARY KEY (batch_id, kind)
) WITHOUT ROWID;
"""

# old table -> which new database receives it
INBOX_TABLES = ("batches", "segments", "st_ids", "st_taps", "st_lays",
                "st_redirects", "st_retires")
PUBLISH_TABLES = ("joints", "pipes", "joint_refs", "meta", "batch_effects")

_ORDER = {
    "batches": "seq", "segments": "seg_id",
    "st_ids": "batch_id,part,item", "st_taps": "batch_id,part,item",
    "st_lays": "batch_id,part,item", "st_redirects": "batch_id,part,item",
    "st_retires": "batch_id,part,item",
    "joints": "uid", "pipes": "pc", "joint_refs": "ref", "meta": "key",
    "batch_effects": "batch_id,kind",
}


class MigrationError(RuntimeError):
    pass


def _table_digest(conn, table: str) -> tuple[int, str]:
    """(row count, sha256 over all rows) -- order-independent of storage."""
    h = hashlib.sha256()
    n = 0
    for row in conn.execute(f"SELECT * FROM {table} ORDER BY {_ORDER[table]}"):
        h.update(repr(tuple(row)).encode("utf-8", "backslashreplace"))
        h.update(b"\n")
        n += 1
    return n, h.hexdigest()


def _copy_table(src, dst, table: str) -> int:
    cols = [r[1] for r in src.execute(f"PRAGMA table_info({table})")]
    if not cols:
        raise MigrationError(f"legacy database has no table {table!r}: "
                             "is this really a pre-split database?")
    sel = f"SELECT {','.join(cols)} FROM {table}"
    ins = (f"INSERT INTO {table}({','.join(cols)}) "
           f"VALUES({','.join('?' for _ in cols)})")
    n = 0
    cur = src.execute(sel)
    while True:
        rows = cur.fetchmany(8192)
        if not rows:
            break
        dst.executemany(ins, rows)
        n += len(rows)
    return n


def _legacy_snapshot(conn) -> dict:
    """Snapshot summary computed against the *old* schema."""
    applied = conn.execute(
        "SELECT CAST(value AS INTEGER) FROM meta WHERE key='applied_seq'"
    ).fetchone()
    applied = applied[0] if applied else 0
    row = conn.execute("SELECT batch_id, applied_at FROM batches "
                       "WHERE seq=? AND state='applied'",
                       (applied,)).fetchone() if applied else None
    upgrade = conn.execute("SELECT CAST(value AS INTEGER) FROM meta "
                           "WHERE key='upgrade_seq'").fetchone()
    return {
        "applied_seq": applied,
        "published_batch": row["batch_id"] if row else None,
        "published_at": row["applied_at"] if row else None,
        "upgrade_seq": upgrade[0] if upgrade else None,
        "joints": conn.execute("SELECT COUNT(*) FROM joints WHERE retired_seq "
                               "IS NULL").fetchone()[0],
        "pipes": conn.execute("SELECT COUNT(*) FROM pipes WHERE retired_seq "
                              "IS NULL").fetchone()[0],
    }


def _parts_map(conn) -> dict:
    return {r["batch_id"]: sorted(
                p[0] for p in conn.execute(
                    "SELECT DISTINCT part FROM segments WHERE batch_id=?",
                    (r["batch_id"],)))
            for r in conn.execute("SELECT batch_id FROM batches")}


def migrate_legacy(legacy_db, inbox_db, publish_db, *, force: bool = False,
                   verify: bool = True) -> dict:
    """Split one old-format database into the inbox/publish pair.

    Returns a verification report; raises ``MigrationError`` if the targets
    exist (without ``force``) or the source is not a legacy database.
    """
    legacy_db = str(legacy_db)
    if not os.path.exists(legacy_db):
        raise MigrationError(f"no such legacy database: {legacy_db}")
    for target in (inbox_db, publish_db):
        target = str(target)
        if os.path.exists(target):
            if not force:
                raise MigrationError(
                    f"{target} already exists; pass force=True (or --force) "
                    f"to replace it")
            os.unlink(target)

    old = connect(legacy_db, read_only=True)
    try:
        inbox = init_inbox(inbox_db)
        try:
            inbox.execute("BEGIN IMMEDIATE")
            for table in INBOX_TABLES:
                _copy_table(old, inbox, table)
            inbox.execute("COMMIT")
        except BaseException:
            if inbox.in_transaction:
                inbox.execute("ROLLBACK")
            inbox.close()
            raise
        inbox.close()

        pub = init_publish(publish_db)
        try:
            pub.execute("BEGIN IMMEDIATE")
            for table in PUBLISH_TABLES:
                _copy_table(old, pub, table)
            rows = old.execute(
                "SELECT batch_id, seq, state, error, applied_at FROM batches "
                "WHERE state IN ('applied','rejected')").fetchall()
            pub.executemany(
                "INSERT INTO published_batches"
                "(batch_id, seq, state, error, applied_at) "
                "VALUES(?,?,?,?,?)",
                [tuple(r) for r in rows])
            pub.execute("COMMIT")
        except BaseException:
            if pub.in_transaction:
                pub.execute("ROLLBACK")
            pub.close()
            raise
        pub.close()

        report = {"legacy_db": legacy_db, "inbox_db": str(inbox_db),
                  "publish_db": str(publish_db)}
        if verify:
            report["checks"] = _verify(old, str(inbox_db), str(publish_db))
            report["ok"] = all(c["match"] for c in report["checks"].values())
        return report
    finally:
        old.close()


def _verify(old, inbox_db, publish_db) -> dict:
    """Per-area comparison of the legacy database and the split pair."""
    inbox = connect(inbox_db, read_only=True)
    pub = connect(publish_db, read_only=True)
    try:
        checks = {}

        # 快照 snapshot: what a duty officer would see
        legacy_snap = _legacy_snapshot(old)
        new_snap = tracing.snapshot_info(pub)
        same = all(legacy_snap[k] == new_snap[k] for k in
                   ("applied_seq", "published_batch", "upgrade_seq",
                    "joints", "pipes"))
        checks["snapshot"] = {"legacy": legacy_snap, "migrated": new_snap,
                              "match": same}

        # 编号映射 id mapping
        n_old, d_old = _table_digest(old, "joint_refs")
        n_new, d_new = _table_digest(pub, "joint_refs")
        checks["id_mapping"] = {"rows": n_old, "match": (n_old, d_old)
                                == (n_new, d_new)}

        # 拓扑 topology: joints + pipes exactly as before
        topo_ok = True
        detail = {}
        for table in ("joints", "pipes"):
            a = _table_digest(old, table)
            b = _table_digest(pub, table)
            detail[table] = a[0]
            topo_ok = topo_ok and a == b
        checks["topology"] = {"rows": detail, "match": topo_ok}

        # 批次依据 batch basis: effects + the applied/rejected ledger
        a = _table_digest(old, "batch_effects")
        b = _table_digest(pub, "batch_effects")
        legacy_ledger = {r["batch_id"]: (r["seq"], r["state"], r["error"])
                         for r in old.execute(
                             "SELECT batch_id,seq,state,error FROM batches "
                             "WHERE state IN ('applied','rejected')")}
        new_ledger = {r["batch_id"]: (r["seq"], r["state"], r["error"])
                      for r in pub.execute(
                          "SELECT batch_id,seq,state,error "
                          "FROM published_batches")}
        checks["basis"] = {"effects_rows": a[0],
                           "ledger_batches": len(new_ledger),
                           "match": a == b and legacy_ledger == new_ledger}

        # 续跑进度 resume progress: receive state of every batch, incl. the
        # fully-received-but-unpublished and the acknowledged-but-partial ones
        progress_ok = True
        for table in INBOX_TABLES:
            if _table_digest(old, table) != _table_digest(inbox, table):
                progress_ok = False
        resume = {"staged_complete": [], "staged_partial": []}
        parts = _parts_map(inbox)
        for r in inbox.execute(
                "SELECT batch_id, seq, state, parts_expected FROM batches "
                "WHERE state='staging' ORDER BY seq"):
            got = parts.get(r["batch_id"], [])
            entry = {"batch": r["batch_id"], "seq": r["seq"],
                     "parts_received": got,
                     "parts_expected": r["parts_expected"]}
            if len(got) == r["parts_expected"]:
                resume["staged_complete"].append(entry)
            else:
                resume["staged_partial"].append(entry)
        checks["resume"] = {"match": progress_ok, **resume}
        return checks
    finally:
        inbox.close()
        pub.close()


def build_legacy_from_split(inbox_db, publish_db, legacy_db, *,
                            force: bool = False) -> str:
    """Merge a split pair into one old-format database (fixtures/tests).

    The old schema is exactly the union of the two new ones, so this is a
    straight row copy: receive-side tables from the inbox, network-side
    tables from the publish database.
    """
    legacy_db = str(legacy_db)
    if os.path.exists(legacy_db):
        if not force:
            raise MigrationError(f"{legacy_db} already exists")
        os.unlink(legacy_db)
    legacy = connect(legacy_db)
    try:
        legacy.executescript(LEGACY_SCHEMA)
        for src_path, tables in ((inbox_db, INBOX_TABLES),
                                 (publish_db, PUBLISH_TABLES)):
            src = connect(src_path, read_only=True)
            try:
                legacy.execute("BEGIN IMMEDIATE")
                for table in tables:
                    _copy_table(src, legacy, table)
                legacy.execute("COMMIT")
            finally:
                src.close()
    finally:
        legacy.close()
    return legacy_db


def format_report(report: dict) -> str:
    """Human-readable rendering of a migration report (CLI / check script)."""
    lines = [f"legacy 旧库    : {report['legacy_db']}",
             f"inbox  接收库 : {report['inbox_db']}",
             f"publish 发布库: {report['publish_db']}"]
    checks = report.get("checks", {})
    labels = {"snapshot": "快照 snapshot", "id_mapping": "编号映射 id mapping",
              "topology": "拓扑 topology", "basis": "批次依据 batch basis",
              "resume": "续跑进度 resume progress"}
    for key, label in labels.items():
        c = checks.get(key)
        if not c:
            continue
        mark = "OK " if c["match"] else "FAIL"
        extra = ""
        if key == "snapshot":
            s = c["migrated"]
            extra = (f" published={s['published_batch']} "
                     f"seq={s['applied_seq']} joints={s['joints']} "
                     f"pipes={s['pipes']}")
        elif key == "id_mapping":
            extra = f" refs={c['rows']}"
        elif key == "topology":
            extra = (f" joints={c['rows']['joints']} "
                     f"pipes={c['rows']['pipes']}")
        elif key == "basis":
            extra = (f" effects={c['effects_rows']} "
                     f"ledger={c['ledger_batches']}")
        elif key == "resume":
            done = ", ".join(e["batch"] for e in c["staged_complete"]) or "-"
            part = ", ".join(
                f"{e['batch']}({len(e['parts_received'])}/"
                f"{e['parts_expected']})" for e in c["staged_partial"]) or "-"
            extra = (f" 已收齐未发布={done}  已确认仍缺片={part}")
        lines.append(f"  [{mark}] {label}{extra}")
    lines.append("migration " + ("OK" if report.get("ok") else "FAILED"))
    return "\n".join(lines)
