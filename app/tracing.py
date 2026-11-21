"""Upstream tracing over the published snapshot.

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
"""

from __future__ import annotations

import json

from .db import connect, network_path
from .ids import parse_ref


class NotFound(LookupError):
    pass


def _display(zone: str, local: str, legacy: str | None) -> str:
    """Current canonical spelling: zoned once the upgrade has renamed it."""
    if zone:
        return f"{zone}:{local}"
    return legacy or local


def resolve_ref(conn, text: str) -> dict:
    """Resolve one textual reference to the current joint record."""
    try:
        r = parse_ref(text)
    except ValueError as exc:
        raise NotFound(f"invalid joint reference: {exc}") from exc
    row = conn.execute("SELECT uid FROM joint_refs WHERE ref=?", (r.text,)).fetchone()
    if row is None:
        hint = ""
        if r.is_legacy:
            hint = (" (before the numbering upgrade only legacy ids exist; "
                    "after it, the id table maps them)")
        elif r.zone:
            hint = (f" (a zoned id only resolves inside zone {r.zone!r}; "
                    "check the zone code)")
        raise NotFound(f"unknown joint reference {text!r}{hint}")
    return _joint(conn, row["uid"])


def _aliases(conn, uid: int) -> list[dict]:
    return [dict(ref=r["ref"], kind=r["kind"], since_seq=r["since_seq"])
            for r in conn.execute(
                "SELECT ref,kind,since_seq FROM joint_refs WHERE uid=? "
                "ORDER BY kind DESC, ref", (uid,))]


def _joint(conn, uid: int) -> dict:
    row = conn.execute("SELECT * FROM joints WHERE uid=?", (uid,)).fetchone()
    if row is None:
        raise NotFound(f"joint uid {uid} not found")
    aliases = _aliases(conn, uid)
    legacy = next((a["ref"] for a in aliases if a["kind"] == "legacy"), None)
    zoned = next((a["ref"] for a in aliases if a["kind"] == "qualified"), None)
    return {
        "uid": row["uid"],
        "zone": row["zone"] or None,
        "local": row["local"],
        "display": _display(row["zone"], row["local"], legacy),
        "legacy_id": legacy,
        "zoned_id": zoned,
        "aliases": aliases,
        "attrs": json.loads(row["attrs"]) if row["attrs"] else {},
        "created_seq": row["created_seq"],
        "last_seq": row["last_seq"],
        "retired_seq": row["retired_seq"],
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


def snapshot_info(conn) -> dict:
    applied_seq = conn.execute(
        "SELECT CAST(value AS INTEGER) FROM meta WHERE key='applied_seq'"
    ).fetchone()
    applied_seq = applied_seq[0] if applied_seq else 0
    row = conn.execute("SELECT batch_id,seq,applied_at FROM applied_batches "
                       "WHERE seq=?", (applied_seq,)).fetchone() if applied_seq \
        else None
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


def _seq_batch_map(conn) -> dict[int, str]:
    return {r["seq"]: r["batch_id"] for r in conn.execute(
        "SELECT seq,batch_id FROM applied_batches")}


def trace_upstream(db_path, ref: str, *, limit: int = 50000,
                   include_retired: bool = False) -> dict:
    """All joints and pipes that can deliver water to ``ref``."""
    conn = connect(network_path(db_path), read_only=True)
    try:
        start = resolve_ref(conn, ref)
        start_uid = start["uid"]

        conn.execute("DROP TABLE IF EXISTS temp.reach")
        conn.execute("CREATE TEMP TABLE reach(uid INTEGER PRIMARY KEY)")
        conn.execute(
            "INSERT INTO temp.reach(uid) "
            "WITH RECURSIVE up(uid) AS ("
            "  SELECT ? "
            "  UNION "
            "  SELECT p.from_uid FROM pipes p JOIN up ON p.to_uid=up.uid "
            "  WHERE p.retired_seq IS NULL"
            ") SELECT uid FROM up", (start_uid,))

        n_reach = conn.execute("SELECT COUNT(*) FROM temp.reach").fetchone()[0]
        if n_reach > limit:
            raise LimitExceeded(
                f"traced network has {n_reach} joints, above the {limit} "
                f"result limit; narrow the query or raise --limit")

        joint_rows = conn.execute(
            "SELECT j.* FROM joints j JOIN temp.reach r ON j.uid=r.uid").fetchall()
        base = {}
        for row in joint_rows:
            rid = row["uid"]
            aliases = _aliases(conn, rid)
            legacy = next((a["ref"] for a in aliases if a["kind"] == "legacy"),
                          None)
            zoned = next((a["ref"] for a in aliases if a["kind"] == "qualified"),
                         None)
            base[rid] = {
                "uid": rid,
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

        def lookup(uid):
            j = base.get(uid)
            return j["display"] if j else f"uid:{uid}"

        if include_retired:
            pipe_where = "p.to_uid IN (SELECT uid FROM temp.reach)"
        else:
            pipe_where = ("p.to_uid IN (SELECT uid FROM temp.reach) "
                          "AND p.retired_seq IS NULL")
        pipe_rows = conn.execute(
            f"SELECT * FROM pipes p WHERE {pipe_where} ORDER BY p.code"
        ).fetchall()

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

        excluded = conn.execute(
            "SELECT COUNT(*) FROM pipes p WHERE p.to_uid IN "
            "(SELECT uid FROM temp.reach) AND p.retired_seq IS NOT NULL"
        ).fetchone()[0] if not include_retired else 0

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

        snap = snapshot_info(conn)
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
    finally:
        conn.close()


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


class LimitExceeded(RuntimeError):
    pass
