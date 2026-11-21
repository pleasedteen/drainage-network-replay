"""SQLite bootstrap: schema, connections, pragmas.

Two database files, deliberately:

``<db>``            append-only receive bookkeeping: landed segments, staging
                    rows derived from them, batch declarations.  A segment's
                    acknowledgement and its staging rows commit in *this*
                    file, in short transactions, so a ``kill -9`` can never
                    leave the two out of step.
``<db>.published``  the published network (joints, pipes, references, batch
                    effects).  Applying a complete batch is one long write
                    transaction here.

Keeping them apart is what makes a small segment deliverable while a huge
batch is being applied: the apply holds the write lock on the published file
only, so an acknowledgement still commits to the receive file within
milliseconds.  The staging pass itself is written in short chunk transactions
(see ``archive._stage``) so a delivery also gets the receive write lock while
a huge segment is still being staged.  Readers open the published file
read-only; WAL lets them keep serving the last complete batch for the whole
apply.

``init_db`` creates both files.  ``published_path`` derives the second path
from the first, so callers only ever name one database.
"""

from __future__ import annotations

import os
import sqlite3
from pathlib import Path

MAIN_SCHEMA = """
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
-- segment always replays in the same order.  Only segments with ingested=1
-- count as received; a torn staging pass leaves ingested=0 and is redone.
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
"""

PUBLISHED_SCHEMA = """
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

-- published state: applied_seq / applied batch, upgrade point, rejection.
CREATE TABLE IF NOT EXISTS meta (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
) WITHOUT ROWID;

-- the source-order ledger of batches that reached the published snapshot;
-- this is what a trace uses to name the batch behind every element.
CREATE TABLE IF NOT EXISTS published_batches (
    seq        INTEGER PRIMARY KEY,
    batch_id   TEXT NOT NULL UNIQUE,
    state      TEXT NOT NULL,             -- applied | rejected
    error      TEXT,
    applied_at TEXT
);

-- what each applied batch actually did; this is what a trace cites as basis.
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


def published_path(path: str | os.PathLike) -> Path:
    """The published-network file that belongs to a receive database."""
    return Path(str(path) + ".published")


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
    conn.execute(f"PRAGMA busy_timeout={max(1, int(timeout * 1000))}")
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
    """Create (if needed) both database files; return the receive connection."""
    conn = connect(path)
    conn.executescript(MAIN_SCHEMA)
    pub = connect(published_path(path))
    try:
        pub.executescript(PUBLISHED_SCHEMA)
    finally:
        pub.close()
    return conn
