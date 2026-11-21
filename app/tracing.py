"""Upstream tracing over the published snapshot -- current, or as of a batch.

"Upstream" follows water flow: a pipe carries water ``from`` -> ``to``, so the
pipes that can deliver water to joint ``X`` are exactly the live pipes whose
``to_uid`` is ``X`` or is reachable upstream from ``X``.

Only live pipes take part.  A pipe that a later batch redirected no longer
has its old endpoints (the same ``code`` row was re-pointed), so the old
connection simply is not there any more -- exactly "按实际连接" -- and a
retired pipe is decommissioned, so it neither carries water nor connects
anything.

Every element the answer cites carries the batch that last shaped it, and the
answer as a whole names the published snapshot batch, so a duty officer can
see which engineering batches the trace relies on.

As-of queries
-------------
``trace_upstream(..., batch="B-0003")`` answers the same question as it would
have stood right after batch B-0003 was published: the joint numbering
(legacy vs zoned), the pipe endpoints and the basis are all taken from the
temporal ``hist_*`` tables at that batch's seq -- an indexed range scan, not
a replay.  A batch whose history is not (yet) complete raises
``HistoryNotReady`` instead of returning a partial answer.
"""

from __future__ import annotations

import json

from . import history
from .db import connect
from .ids import parse_ref


class NotFound(LookupError):
    pass


class LimitExceeded(RuntimeError):
    pass


class HistoryNotReady(RuntimeError):
    """The requested batch cannot be served (yet): not published, or its
    history is still being backfilled.  Answering anyway would mean
    returning partial results, which never happens."""


def _display(zone: str, local: str, legacy: str | None) -> str:
    """Canonical spelling at the queried point in time."""
    if zone:
        return f"{zone}:{local}"
    return legacy or local


# ---------------------------------------------------------------------------
# point-in-time views
# ---------------------------------------------------------------------------

class _Live:
    """The current published snapshot (the ``joints``/``pipes`` tables)."""

    historical = False

    def __init__(self, conn):
        self.conn = conn

    def resolve(self, text: str) -> dict:
        try:
            r = parse_ref(text)
        except ValueError as exc:
            raise NotFound(f"invalid joint reference: {exc}") from exc
        row = self.conn.execute("SELECT uid FROM joint_refs WHERE ref=?",
                                (r.text,)).fetchone()
        if row is None:
            hint = ""
            if r.is_legacy:
                hint = (" (before the numbering upgrade only legacy ids "
                        "exist; after it, the id table maps them)")
            elif r.zone:
                hint = (f" (a zoned id only resolves inside zone {r.zone!r}; "
                        "check the zone code)")
            raise NotFound(f"unknown joint reference {text!r}{hint}")
        return self.joint(row["uid"])

    def joint(self, uid: int) -> dict:
        row = self.conn.execute(
            "SELECT uid,zone,local,attrs,created_seq,last_seq,retired_seq "
            "FROM joints WHERE uid=?", (uid,)).fetchone()
        if row is None:
            raise NotFound(f"joint uid {uid} not found")
        aliases = [dict(ref=a["ref"], kind=a["kind"], since_seq=a["since_seq"])
                   for a in self.conn.execute(
                       "SELECT ref,kind,since_seq FROM joint_refs WHERE uid=? "
                       "ORDER BY kind DESC, ref", (uid,))]
        return _joint_dict(row, aliases, with_aliases=True)

    def fill_reach(self, start_uid: int) -> None:
        self.conn.execute(
            "INSERT INTO temp.reach(uid) "
            "WITH RECURSIVE up(uid) AS ("
            "  SELECT ? "
            "  UNION "
            "  SELECT p.from_uid FROM pipes p JOIN up ON p.to_uid=up.uid "
            "  WHERE p.retired_seq IS NULL"
            ") SELECT uid FROM up", (start_uid,))

    def joints_in_reach(self):
        return self.conn.execute(
            "SELECT j.uid,j.zone,j.local,j.attrs,j.created_seq,j.last_seq,"
            "j.retired_seq FROM joints j JOIN temp.reach r ON j.uid=r.uid"
        ).fetchall()

    def aliases_in_reach(self) -> dict[int, list[dict]]:
        rows = self.conn.execute(
            "SELECT uid,ref,kind,since_seq FROM joint_refs "
            "WHERE uid IN (SELECT uid FROM temp.reach) "
            "ORDER BY uid, kind DESC, ref").fetchall()
        return _group_aliases(rows)

    def pipes_into_reach(self, include_retired: bool):
        where = "p.to_uid IN (SELECT uid FROM temp.reach)"
        if not include_retired:
            where += " AND p.retired_seq IS NULL"
        return self.conn.execute(
            f"SELECT p.code,p.from_uid,p.to_uid,p.attrs,p.created_seq,"
            f"p.last_seq,p.retired_seq FROM pipes p WHERE {where} "
            f"ORDER BY p.code").fetchall()

    def retired_into_reach(self) -> int:
        return self.conn.execute(
            "SELECT COUNT(*) FROM pipes p WHERE p.to_uid IN "
            "(SELECT uid FROM temp.reach) AND p.retired_seq IS NOT NULL"
        ).fetchone()[0]

    def snapshot(self) -> dict:
        return snapshot_info(self.conn)


class _AsOf:
    """The network as it stood right after batch seq ``seq`` was published.

    All reads go to the temporal ``hist_*`` tables with the validity
    predicate ``from_seq <= seq AND (to_seq IS NULL OR to_seq > seq)``.
    """

    historical = True

    def __init__(self, conn, seq: int):
        self.conn = conn
        self.seq = seq

    def _valid(self, table_alias: str) -> tuple[str, tuple]:
        return (f"{table_alias}.from_seq<=? "
                f"AND ({table_alias}.to_seq IS NULL OR {table_alias}.to_seq>?)",
                (self.seq, self.seq))

    def resolve(self, text: str) -> dict:
        try:
            r = parse_ref(text)
        except ValueError as exc:
            raise NotFound(f"invalid joint reference: {exc}") from exc
        valid, params = self._valid("h")
        row = self.conn.execute(
            f"SELECT h.uid FROM hist_refs h WHERE h.ref=? AND {valid}",
            (r.text, *params)).fetchone()
        if row is None:
            hint = f" (as of seq {self.seq}"
            if r.is_legacy:
                hint += ("; before the numbering upgrade only legacy ids "
                         "exist, after it the id table maps them)")
            elif r.zone:
                hint += (f"; a zoned id only resolves inside zone {r.zone!r} "
                         "once the id table maps it)")
            else:
                hint += ")"
            raise NotFound(f"unknown joint reference {text!r}{hint}")
        return self.joint(row["uid"])

    def joint(self, uid: int) -> dict:
        valid, params = self._valid("h")
        row = self.conn.execute(
            f"SELECT h.uid,h.zone,h.local,h.attrs,"
            f" (SELECT MIN(f.from_seq) FROM hist_joints f WHERE f.uid=h.uid)"
            f"   AS created_seq,"
            f" h.from_seq AS last_seq,"
            f" CASE WHEN h.retired THEN h.from_seq END AS retired_seq"
            f" FROM hist_joints h WHERE h.uid=? AND {valid}",
            (uid, *params)).fetchone()
        if row is None:
            raise NotFound(f"joint uid {uid} not found as of seq {self.seq}")
        aliases = [dict(ref=a["ref"], kind=a["kind"], since_seq=a["since_seq"])
                   for a in self.conn.execute(
                       f"SELECT h.ref,h.kind,h.from_seq AS since_seq "
                       f"FROM hist_refs h WHERE h.uid=? AND {valid} "
                       f"ORDER BY h.kind DESC, h.ref", (uid, *params))]
        return _joint_dict(row, aliases, with_aliases=True)

    def fill_reach(self, start_uid: int) -> None:
        self.conn.execute(
            "INSERT INTO temp.reach(uid) "
            "WITH RECURSIVE up(uid) AS ("
            "  SELECT ? "
            "  UNION "
            "  SELECT p.from_uid FROM hist_pipes p JOIN up ON p.to_uid=up.uid "
            "  WHERE p.retired=0 AND p.from_seq<=? "
            "    AND (p.to_seq IS NULL OR p.to_seq>?)"
            ") SELECT uid FROM up", (start_uid, self.seq, self.seq))

    def joints_in_reach(self):
        return self.conn.execute(
            "SELECT h.uid,h.zone,h.local,h.attrs,"
            " (SELECT MIN(f.from_seq) FROM hist_joints f WHERE f.uid=h.uid)"
            "   AS created_seq,"
            " h.from_seq AS last_seq,"
            " CASE WHEN h.retired THEN h.from_seq END AS retired_seq"
            " FROM hist_joints h JOIN temp.reach r ON h.uid=r.uid"
            " WHERE h.from_seq<=? AND (h.to_seq IS NULL OR h.to_seq>?)",
            (self.seq, self.seq)).fetchall()

    def aliases_in_reach(self) -> dict[int, list[dict]]:
        rows = self.conn.execute(
            "SELECT h.uid,h.ref,h.kind,h.from_seq AS since_seq "
            "FROM hist_refs h "
            "WHERE h.uid IN (SELECT uid FROM temp.reach) "
            "AND h.from_seq<=? AND (h.to_seq IS NULL OR h.to_seq>?) "
            "ORDER BY h.uid, h.kind DESC, h.ref",
            (self.seq, self.seq)).fetchall()
        return _group_aliases(rows)

    def pipes_into_reach(self, include_retired: bool):
        where = ("h.to_uid IN (SELECT uid FROM temp.reach) "
                 "AND h.from_seq<=? AND (h.to_seq IS NULL OR h.to_seq>?)")
        if not include_retired:
            where += " AND h.retired=0"
        return self.conn.execute(
            f"SELECT h.code,h.from_uid,h.to_uid,h.attrs,"
            f" (SELECT MIN(f.from_seq) FROM hist_pipes f WHERE f.code=h.code)"
            f"   AS created_seq,"
            f" h.from_seq AS last_seq,"
            f" CASE WHEN h.retired THEN h.from_seq END AS retired_seq"
            f" FROM hist_pipes h WHERE {where} ORDER BY h.code",
            (self.seq, self.seq)).fetchall()

    def retired_into_reach(self) -> int:
        return self.conn.execute(
            "SELECT COUNT(*) FROM hist_pipes h WHERE h.to_uid IN "
            "(SELECT uid FROM temp.reach) AND h.retired=1 "
            "AND h.from_seq<=? AND (h.to_seq IS NULL OR h.to_seq>?)",
            (self.seq, self.seq)).fetchone()[0]

    def snapshot(self) -> dict:
        return snapshot_info_asof(self.conn, self.seq)


def _group_aliases(rows) -> dict[int, list[dict]]:
    out: dict[int, list[dict]] = {}
    for r in rows:
        out.setdefault(r["uid"], []).append(
            dict(ref=r["ref"], kind=r["kind"], since_seq=r["since_seq"]))
    return out


def _joint_dict(row, aliases, *, with_aliases: bool) -> dict:
    legacy = next((a["ref"] for a in aliases if a["kind"] == "legacy"), None)
    zoned = next((a["ref"] for a in aliases if a["kind"] == "qualified"), None)
    out = {
        "uid": row["uid"],
        "zone": row["zone"] or None,
        "local": row["local"],
        "display": _display(row["zone"], row["local"], legacy),
        "legacy_id": legacy,
        "zoned_id": zoned,
        "attrs": json.loads(row["attrs"]) if row["attrs"] else {},
        "created_seq": row["created_seq"],
        "last_seq": row["last_seq"],
        "retired_seq": row["retired_seq"],
    }
    if with_aliases:
        out["aliases"] = aliases
    return out


# ---------------------------------------------------------------------------
# public lookups
# ---------------------------------------------------------------------------

def resolve_ref(conn, text: str, *, asof: int | None = None) -> dict:
    """Resolve one textual reference to the joint record.

    ``asof`` (a batch seq) resolves against the historical numbering instead
    of the current one.
    """
    view = _Live(conn) if asof is None else _AsOf(conn, asof)
    return view.resolve(text)


def require_history(conn, batch: str) -> int:
    """Validate that ``batch`` can be traced; return its seq.

    Raises ``NotFound`` for an unknown batch and ``HistoryNotReady`` when the
    batch is not published or its history is not completely built -- the
    caller must never answer with a partially built version.
    """
    row = conn.execute("SELECT seq,state,error FROM batches WHERE batch_id=?",
                       (batch,)).fetchone()
    if row is None:
        raise NotFound(f"unknown batch {batch!r}")
    if row["state"] == "rejected":
        detail = f": {row['error']}" if row["error"] else ""
        raise HistoryNotReady(
            f"batch {batch!r} was rejected and never published{detail}")
    if row["state"] != "applied":
        raise HistoryNotReady(
            f"batch {batch!r} is not published yet (state: {row['state']}); "
            f"only published batches can be traced")
    upto = history.upto_seq(conn)
    if row["seq"] > upto:
        raise HistoryNotReady(
            f"history for batch {batch!r} (seq {row['seq']}) is not built "
            f"yet (built up to seq {upto}); run `python -m app backfill "
            f"--db DB` -- partial history is never served")
    return row["seq"]


def snapshot_info(conn) -> dict:
    applied_seq = conn.execute(
        "SELECT CAST(value AS INTEGER) FROM meta WHERE key='applied_seq'"
    ).fetchone()
    applied_seq = applied_seq[0] if applied_seq else 0
    row = conn.execute("SELECT batch_id,seq,applied_at FROM batches "
                       "WHERE seq=? AND state='applied'",
                       (applied_seq,)).fetchone() if applied_seq else None
    upgrade = conn.execute("SELECT CAST(value AS INTEGER) FROM meta "
                           "WHERE key='upgrade_seq'").fetchone()
    return {
        "applied_seq": applied_seq,
        "published_batch": row["batch_id"] if row else None,
        "published_at": row["applied_at"] if row else None,
        "upgrade_seq": upgrade[0] if upgrade else None,
        "joints": conn.execute("SELECT COUNT(*) FROM joints WHERE retired_seq "
                               "IS NULL").fetchone()[0],
        "pipes": conn.execute("SELECT COUNT(*) FROM pipes WHERE retired_seq "
                              "IS NULL").fetchone()[0],
    }


def snapshot_info_asof(conn, seq: int) -> dict:
    """The snapshot summary as it stood right after batch ``seq``."""
    row = conn.execute("SELECT batch_id,applied_at FROM batches "
                       "WHERE seq=? AND state='applied'", (seq,)).fetchone()
    upgrade = conn.execute("SELECT CAST(value AS INTEGER) FROM meta "
                           "WHERE key='upgrade_seq'").fetchone()
    upgrade = upgrade[0] if upgrade else None
    return {
        "applied_seq": seq,
        "published_batch": row["batch_id"] if row else None,
        "published_at": row["applied_at"] if row else None,
        "upgrade_seq": upgrade if upgrade is not None and upgrade <= seq
        else None,
        "joints": conn.execute(
            "SELECT COUNT(*) FROM hist_joints WHERE retired=0 "
            "AND from_seq<=? AND (to_seq IS NULL OR to_seq>?)",
            (seq, seq)).fetchone()[0],
        "pipes": conn.execute(
            "SELECT COUNT(*) FROM hist_pipes WHERE retired=0 "
            "AND from_seq<=? AND (to_seq IS NULL OR to_seq>?)",
            (seq, seq)).fetchone()[0],
        "historical": True,
    }


def _seq_batch_map(conn) -> dict[int, str]:
    return {r["seq"]: r["batch_id"] for r in conn.execute(
        "SELECT seq,batch_id FROM batches")}


# ---------------------------------------------------------------------------
# the trace itself
# ---------------------------------------------------------------------------

def trace_upstream(db_path, ref: str, *, limit: int = 50000,
                   include_retired: bool = False,
                   batch: str | None = None) -> dict:
    """All joints and pipes that can deliver water to ``ref``.

    With ``batch`` the answer is computed as of that published batch: joint
    numbering, pipe endpoints and cited basis are all of that point in time.
    """
    conn = connect(db_path, read_only=True)
    try:
        if batch is None:
            view = _Live(conn)
        else:
            view = _AsOf(conn, require_history(conn, batch))
        return _trace(view, ref, limit=limit, include_retired=include_retired)
    finally:
        conn.close()


def _trace(view, ref: str, *, limit: int, include_retired: bool) -> dict:
    conn = view.conn
    start = view.resolve(ref)
    start_uid = start["uid"]

    conn.execute("DROP TABLE IF EXISTS temp.reach")
    conn.execute("CREATE TEMP TABLE reach(uid INTEGER PRIMARY KEY)")
    view.fill_reach(start_uid)

    n_reach = conn.execute("SELECT COUNT(*) FROM temp.reach").fetchone()[0]
    if n_reach > limit:
        raise LimitExceeded(
            f"traced network has {n_reach} joints, above the {limit} "
            f"result limit; narrow the query or raise --limit")

    aliases = view.aliases_in_reach()
    base = {}
    for row in view.joints_in_reach():
        rid = row["uid"]
        base[rid] = _joint_dict(row, aliases.get(rid, []), with_aliases=False)

    def lookup(uid):
        j = base.get(uid)
        return j["display"] if j else f"uid:{uid}"

    pipe_rows = view.pipes_into_reach(include_retired)

    # depth = fewest pipes from the incident joint, walking upstream
    adjacency: dict[int, list[int]] = {}
    for row in pipe_rows:
        if row["retired_seq"] is None:
            adjacency.setdefault(row["to_uid"], []).append(row["from_uid"])
    depth = {start_uid: 0}
    frontier = [start_uid]
    while frontier:
        nxt = []
        for u in frontier:
            d = depth[u] + 1
            for v in adjacency.get(u, ()):
                if v not in depth:
                    depth[v] = d
                    nxt.append(v)
        frontier = nxt

    excluded = 0 if include_retired else view.retired_into_reach()

    seqmap = _seq_batch_map(conn)
    joints_out = []
    for j in sorted(base.values(), key=lambda j: (depth.get(j["uid"], 10**9),
                                                  j["display"])):
        j = dict(j)
        j["depth"] = depth.get(j["uid"])
        j["batch"] = seqmap.get(j["last_seq"])
        joints_out.append(j)

    pipes_out = []
    for row in pipe_rows:
        p = _pipe_row(row, lookup)
        p["batch"] = seqmap.get(row["last_seq"])
        pipes_out.append(p)

    cited = _cite(joints_out, pipes_out)

    snap = view.snapshot()
    return {
        "query": {"ref": ref, "resolved": start},
        "snapshot": snap,
        "totals": {
            "upstream_joints": len(joints_out),
            "upstream_pipes": len(pipes_out),
            "max_depth": max(depth.values()) if depth else 0,
            "retired_pipes_not_followed": excluded,
        },
        "joints": joints_out,
        "pipes": pipes_out,
        "basis": {
            "published_batch": snap["published_batch"],
            "published_seq": snap["applied_seq"],
            "referenced_batches": cited,
        },
    }


def _pipe_row(row, joint_lookup) -> dict:
    return {
        "code": row["code"],
        "from_uid": row["from_uid"],
        "to_uid": row["to_uid"],
        "from": joint_lookup(row["from_uid"]),
        "to": joint_lookup(row["to_uid"]),
        "attrs": json.loads(row["attrs"]) if row["attrs"] else {},
        "created_seq": row["created_seq"],
        "last_seq": row["last_seq"],
        "retired_seq": row["retired_seq"],
    }


def _cite(joints_out, pipes_out) -> list[dict]:
    batches: dict[str | None, dict] = {}
    for item in joints_out:
        b = batches.setdefault(item["batch"], {
            "batch": item["batch"], "seq": item["last_seq"],
            "joints": 0, "pipes": 0})
        b["joints"] += 1
    for item in pipes_out:
        b = batches.setdefault(item["batch"], {
            "batch": item["batch"], "seq": item["last_seq"],
            "joints": 0, "pipes": 0})
        b["pipes"] += 1
    return sorted(batches.values(),
                  key=lambda b: (b["seq"] is None, b["seq"] or 0))
