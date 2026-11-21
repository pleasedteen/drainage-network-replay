"""SQLite bootstrap: schema, connections, pragmas.

A single database file holds both the append-only sync bookkeeping (landed
segments, staging rows) and the published network.  That is deliberate: the
acknowledgement of a segment and the staging rows derived from it commit in
one transaction, so a ``kill -9`` can never leave the two out of step.

Readers open the database read-only and WAL lets them keep serving the last
published batch while a large batch is being applied.
"""

from __future__ import annotations

import os
import sqlite3
from pathlib import Path

SCHEMA = """
CREATE TABLE IF NOT EXISTS batches (
    batch_id       TEXT NOT NULL,
    seq            INTEGER NOT NULL,   -- position in the source's batch order
    total          INTEGER NOT NULL,   -- how many batches the source will send
    upgrade        INTEGER NOT NULL,   -- 1 on the batch that introduces zones
    state          TEXT NOT NULL,      -- staging | applied | rejected
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
    ingested    INTEGER NOT NULL DEFAULT 0,   -- 1 once staging rows exist
    UNIQUE (batch_id, part)
);
CREATE INDEX IF NOT EXISTS segments_batch ON segments(batch_id, part);

-- staging: one row per log item, ordered by (part, item) so a re-ingested
-- segment always replays in the same order.
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

-- published network
CREATE TABLE IF NOT EXISTS joints (
    uid         INTEGER PRIMARY KEY AUTOINCREMENT,
    zone        TEXT NOT NULL,            -- '' while the city still uses global ids
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
    retired_by  TEXT,                     -- 'explicit' | 'joint-retired' | NULL
    attrs       TEXT
);
CREATE INDEX IF NOT EXISTS pipes_to ON pipes(to_uid);
CREATE INDEX IF NOT EXISTS pipes_from ON pipes(from_uid);

-- every textual form a joint can be addressed by, with the precedence rule
-- already resolved.  ref is unique, so reference resolution is one lookup.
CREATE TABLE IF NOT EXISTS joint_refs (
    ref      TEXT PRIMARY KEY,
    uid      INTEGER NOT NULL REFERENCES joints(uid),
    kind     TEXT NOT NULL,               -- qualified | legacy
    since_seq INTEGER NOT NULL
) WITHOUT ROWID;
CREATE INDEX IF NOT EXISTS joint_refs_kind_uid ON joint_refs(kind, uid);
CREATE INDEX IF NOT EXISTS joint_refs_uid ON joint_refs(uid);

-- what each applied batch actually did; this is what a trace cites as basis.
CREATE TABLE IF NOT EXISTS meta (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
) WITHOUT ROWID;

-- ---------------------------------------------------------------------------
-- per-batch history ("as of batch N" queries)
--
-- Three temporal tables hold one row per *version* of a joint / pipe /
-- reference: valid from ``from_seq`` (inclusive) until ``to_seq`` (exclusive,
-- NULL = still current).  A batch only adds rows for the entities it actually
-- changed, so the storage increment of a small 改接 batch is a handful of
-- rows, and an as-of query is an indexed range scan -- never a replay.
--
-- The rows are written by triggers while a batch is applied, inside the same
-- transaction, but only when ``hist_context.seq`` names the batch being
-- applied (see app/history.py): the publisher sets it when the history
-- watermark allows, the backfill replay sets it for every replayed batch.
-- The watermark ``meta.history_upto_seq`` advances in the same transaction
-- as the rows, so a version is either complete and visible or not there at
-- all -- partial history is never served.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS hist_joints (
    uid      INTEGER NOT NULL,
    from_seq INTEGER NOT NULL,
    to_seq   INTEGER,
    zone     TEXT NOT NULL,
    local    TEXT NOT NULL,
    attrs    TEXT,
    retired  INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (uid, from_seq)
) WITHOUT ROWID;
CREATE INDEX IF NOT EXISTS hist_joints_from ON hist_joints(from_seq);

CREATE TABLE IF NOT EXISTS hist_pipes (
    code     TEXT NOT NULL,
    from_seq INTEGER NOT NULL,
    to_seq   INTEGER,
    from_uid INTEGER NOT NULL,
    to_uid   INTEGER NOT NULL,
    attrs    TEXT,
    retired  INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (code, from_seq)
) WITHOUT ROWID;
CREATE INDEX IF NOT EXISTS hist_pipes_to ON hist_pipes(to_uid, from_seq);
CREATE INDEX IF NOT EXISTS hist_pipes_from ON hist_pipes(from_seq);

CREATE TABLE IF NOT EXISTS hist_refs (
    ref      TEXT NOT NULL,
    from_seq INTEGER NOT NULL,
    to_seq   INTEGER,
    uid      INTEGER NOT NULL,
    kind     TEXT NOT NULL,
    PRIMARY KEY (ref, from_seq)
) WITHOUT ROWID;
CREATE INDEX IF NOT EXISTS hist_refs_uid ON hist_refs(uid, from_seq);
CREATE INDEX IF NOT EXISTS hist_refs_from ON hist_refs(from_seq);

-- one-row switchboard: which batch seq the triggers below are recording
-- (NULL = not recording).  Transactional, so a rolled-back batch clears it.
CREATE TABLE IF NOT EXISTS hist_context (
    id  INTEGER PRIMARY KEY CHECK (id = 1),
    seq INTEGER
);

CREATE TRIGGER IF NOT EXISTS hist_joints_ins AFTER INSERT ON joints
WHEN (SELECT seq FROM hist_context WHERE id = 1) IS NOT NULL
BEGIN
    INSERT INTO hist_joints(uid, from_seq, to_seq, zone, local, attrs, retired)
    VALUES (NEW.uid, (SELECT seq FROM hist_context WHERE id = 1), NULL,
            NEW.zone, NEW.local, NEW.attrs, NEW.retired_seq IS NOT NULL);
END;

-- a change to a tracked column closes the previous version (unless it was
-- opened by this very batch) and opens a new one; several changes to the same
-- joint inside one batch collapse into a single version row.
CREATE TRIGGER IF NOT EXISTS hist_joints_upd AFTER UPDATE ON joints
WHEN (SELECT seq FROM hist_context WHERE id = 1) IS NOT NULL
 AND (OLD.zone IS NOT NEW.zone OR OLD.local IS NOT NEW.local
      OR OLD.attrs IS NOT NEW.attrs
      OR (OLD.retired_seq IS NULL) IS NOT (NEW.retired_seq IS NULL))
BEGIN
    UPDATE hist_joints SET to_seq = (SELECT seq FROM hist_context WHERE id = 1)
    WHERE uid = OLD.uid AND to_seq IS NULL
      AND from_seq < (SELECT seq FROM hist_context WHERE id = 1);
    INSERT INTO hist_joints(uid, from_seq, to_seq, zone, local, attrs, retired)
    VALUES (NEW.uid, (SELECT seq FROM hist_context WHERE id = 1), NULL,
            NEW.zone, NEW.local, NEW.attrs, NEW.retired_seq IS NOT NULL)
    ON CONFLICT(uid, from_seq) DO UPDATE SET
        zone = excluded.zone, local = excluded.local, attrs = excluded.attrs,
        retired = excluded.retired, to_seq = NULL;
END;

CREATE TRIGGER IF NOT EXISTS hist_pipes_ins AFTER INSERT ON pipes
WHEN (SELECT seq FROM hist_context WHERE id = 1) IS NOT NULL
BEGIN
    INSERT INTO hist_pipes(code, from_seq, to_seq, from_uid, to_uid, attrs,
                           retired)
    VALUES (NEW.code, (SELECT seq FROM hist_context WHERE id = 1), NULL,
            NEW.from_uid, NEW.to_uid, NEW.attrs, NEW.retired_seq IS NOT NULL);
END;

CREATE TRIGGER IF NOT EXISTS hist_pipes_upd AFTER UPDATE ON pipes
WHEN (SELECT seq FROM hist_context WHERE id = 1) IS NOT NULL
 AND (OLD.from_uid IS NOT NEW.from_uid OR OLD.to_uid IS NOT NEW.to_uid
      OR OLD.attrs IS NOT NEW.attrs
      OR (OLD.retired_seq IS NULL) IS NOT (NEW.retired_seq IS NULL))
BEGIN
    UPDATE hist_pipes SET to_seq = (SELECT seq FROM hist_context WHERE id = 1)
    WHERE code = OLD.code AND to_seq IS NULL
      AND from_seq < (SELECT seq FROM hist_context WHERE id = 1);
    INSERT INTO hist_pipes(code, from_seq, to_seq, from_uid, to_uid, attrs,
                           retired)
    VALUES (NEW.code, (SELECT seq FROM hist_context WHERE id = 1), NULL,
            NEW.from_uid, NEW.to_uid, NEW.attrs, NEW.retired_seq IS NOT NULL)
    ON CONFLICT(code, from_seq) DO UPDATE SET
        from_uid = excluded.from_uid, to_uid = excluded.to_uid,
        attrs = excluded.attrs, retired = excluded.retired, to_seq = NULL;
END;

CREATE TRIGGER IF NOT EXISTS hist_refs_ins AFTER INSERT ON joint_refs
WHEN (SELECT seq FROM hist_context WHERE id = 1) IS NOT NULL
BEGIN
    INSERT INTO hist_refs(ref, from_seq, to_seq, uid, kind)
    VALUES (NEW.ref, (SELECT seq FROM hist_context WHERE id = 1), NULL,
            NEW.uid, NEW.kind)
    ON CONFLICT(ref, from_seq) DO UPDATE SET
        uid = excluded.uid, kind = excluded.kind, to_seq = NULL;
END;

CREATE TRIGGER IF NOT EXISTS hist_refs_del AFTER DELETE ON joint_refs
WHEN (SELECT seq FROM hist_context WHERE id = 1) IS NOT NULL
BEGIN
    -- a ref opened and dropped inside one batch leaves no trace at all
    DELETE FROM hist_refs WHERE ref = OLD.ref AND to_seq IS NULL
      AND from_seq = (SELECT seq FROM hist_context WHERE id = 1);
    UPDATE hist_refs SET to_seq = (SELECT seq FROM hist_context WHERE id = 1)
    WHERE ref = OLD.ref AND to_seq IS NULL
      AND from_seq < (SELECT seq FROM hist_context WHERE id = 1);
END;

CREATE TABLE IF NOT EXISTS batch_effects (
    batch_id TEXT NOT NULL,
    seq      INTEGER NOT NULL,
    kind     TEXT NOT NULL,               -- ids | tap_created | tap_updated | lay |
                                          -- redirect | joint_retired | pipe_retired
                                          -- | pipe_retired_by_joint
    n        INTEGER NOT NULL,
    PRIMARY KEY (batch_id, kind)
) WITHOUT ROWID;
"""


def connect(path: str | os.PathLike, *, read_only: bool = False,
            timeout: float = 30.0) -> sqlite3.Connection:
    p = Path(path)
    if read_only:
        conn = sqlite3.connect(f"file:{p}?mode=ro", uri=True, timeout=timeout,
                               isolation_level=None, check_same_thread=False)
    else:
        p.parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(str(p), timeout=timeout, isolation_level=None,
                               check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout=30000")
    conn.execute("PRAGMA foreign_keys=ON")
    if not read_only:
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=FULL")
        conn.execute("PRAGMA temp_store=FILE")   # big traces spill to disk
    else:
        # NB: no PRAGMA query_only here -- mode=ro already blocks writes to the
        # main database, while temp tables (used by the tracer) still work.
        conn.execute("PRAGMA temp_store=FILE")
    return conn


def init_db(path: str | os.PathLike) -> sqlite3.Connection:
    conn = connect(path)
    conn.executescript(SCHEMA)
    conn.execute("INSERT OR IGNORE INTO hist_context(id, seq) VALUES(1, NULL)")
    return conn


def meta_get(conn, key, default=None):
    row = conn.execute("SELECT value FROM meta WHERE key=?", (key,)).fetchone()
    return row["value"] if row else default


def meta_set(conn, key, value):
    conn.execute("INSERT INTO meta(key,value) VALUES(?,?) "
                 "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                 (key, str(value)))
