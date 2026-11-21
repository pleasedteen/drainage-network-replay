"""Offline acceptance check for the split inbox/publish storage.

One command, local fixtures only, no network:

    timeout 80 python3 examples/splitcheck.py

Covers, end to end:

1. 旧库迁移      -- a pre-split single database is migrated into the
                    inbox/publish pair; snapshot, id mapping, topology and
                    batch basis all match, and batches resume from their
                    original progress (已收齐未发布 / 已确认仍缺片).
2. 发布期间确认  -- while a large batch is being published, a newly arrived
                    segment still gets its (durable, resumable) acknowledgement;
                    and a one-attribute batch writes in proportion to the
                    change, not to the whole network.
3. 强停恢复      -- kill -9 in the middle of publishing: acknowledgements
                    survive, batch state is reconciled with the queryable
                    version, and the batch's effects apply exactly once.
4. 接收库停止    -- HTTP queries (joint id, upstream trace, basis batches)
                    keep answering from the publish database alone.

Prints a readable report and exits non-zero if any check fails.
Everything it creates goes to a temporary directory (override with --work).
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import signal
import subprocess
import sys
import tempfile
import threading
import time
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from app import migrate, replay, service, tracing      # noqa: E402
from app.db import connect                              # noqa: E402
from app.server import make_server                      # noqa: E402

FAILURES = []


def say(msg=""):
    print(msg, flush=True)


def check(label, ok, detail=""):
    mark = "✓" if ok else "✗"
    say(f"   {mark} {label}" + (f"  ({detail})" if detail else ""))
    if not ok:
        FAILURES.append(label)


def big_segment(path, n_joints, n_pipes, batch="B-BIG", seq=1, total=1,
                part=0, parts=1):
    def joint_items():
        for i in range(n_joints):
            yield {"ref": f"J-{i:06d}", "attrs": {"kind": "污水", "n": i}}

    def pipe_items():
        for i in range(n_pipes):
            yield {"code": f"P-{i:06d}", "from": f"J-{i % n_joints:06d}",
                   "to": f"J-{(i * 7919 + 13) % n_joints:06d}",
                   "attrs": {"dn": 300}}

    return replay.write_streamed_segment(
        path,
        {"segment": f"{batch}-p{part}", "batch": batch, "seq": seq,
         "total": total, "part": part, "parts": parts, "upgrade": False,
         "producer": "splitcheck"},
        {"taps": joint_items, "lays": pipe_items,
         "ids": [], "redirects": [], "retires": []})


def small_segment(path, seg_id, batch, seq, total, taps=(), extra=()):
    payload = {"segment": seg_id, "batch": batch, "seq": seq, "total": total,
               "part": 0, "parts": 1, "upgrade": False,
               "taps": list(taps) + list(extra)}
    Path(path).write_text(json.dumps(payload, ensure_ascii=False),
                          encoding="utf-8")
    return Path(path)


def wal_delta(publish_db, fn):
    """WAL bytes written by fn(): the write volume of one publish."""
    keeper = connect(publish_db)
    try:
        keeper.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        wal = str(publish_db) + "-wal"
        before = os.path.getsize(wal) if os.path.exists(wal) else 0
        fn()
        after = os.path.getsize(wal) if os.path.exists(wal) else 0
        return after - before
    finally:
        keeper.close()


# ---------------------------------------------------------------------------
# 1. legacy migration
# ---------------------------------------------------------------------------

def check_migration(work):
    say("=" * 78)
    say("[1/4] 旧库迁移：单库拆成接收库 + 发布库，四处一致，断点续跑")
    fix = work / "legacy-fixture"
    # -- build a realistic old single database with the current engine, then
    #    merge it into the old one-file layout
    svc = service.SyncService(fix / "inbox.db", fix / "publish.db",
                              fix / "data")
    replay.write_segments(
        {"batch": "B-0001", "seq": 1, "total": 4, "parts": 1, "items": {
            "taps": [{"ref": f"J-{i:06d}"} for i in range(1, 8)],
            "lays": [{"code": f"P-{i:06d}", "from": f"J-{i:06d}",
                      "to": f"J-{i + 1:06d}"} for i in range(1, 7)]}},
        fix / "src")
    replay.write_segments(
        {"batch": "B-0002", "seq": 2, "total": 4, "parts": 1, "upgrade": True,
         "items": {"ids": [{"old": f"J-{i:06d}", "new": f"Q4:{i:05d}"}
                           for i in range(1, 8)]}},
        fix / "src2")
    replay.write_segments(
        {"batch": "B-0003", "seq": 3, "total": 4, "parts": 2,
         "items": {"taps": [{"ref": "Q4:00001",
                             "attrs": {"note": "已收齐未发布"}}]}},
        fix / "src3")
    replay.write_segments(
        {"batch": "B-0004", "seq": 4, "total": 4, "parts": 2,
         "items": {"taps": [{"ref": "Q4:00002", "attrs": {"note": "缺一片"}}]}},
        fix / "src4")
    for p in sorted((fix / "src").glob("*.json")) + \
             sorted((fix / "src2").glob("*.json")):
        svc.accept(p)                               # B-0001, B-0002 published
    svc.auto_publish = False
    for p in sorted((fix / "src3").glob("*.json")):
        svc.accept(p)                               # complete, never published
    held = sorted((fix / "src4").glob("*.json"))
    svc.accept(held[0])                             # B-0004: part 0 of 2 only
    legacy = migrate.build_legacy_from_split(
        fix / "inbox.db", fix / "publish.db", fix / "legacy.db")
    say(f"   夹具旧库: {legacy}  (B-0001/B-0002 已发布, B-0003 已收齐未发布,"
        " B-0004 缺 1 片)")

    # -- migrate and verify
    report = migrate.migrate_legacy(legacy, work / "m-inbox.db",
                                    work / "m-publish.db")
    for line in migrate.format_report(report).splitlines()[3:]:
        say("   " + line)
    check("快照/编号映射/拓扑/批次依据四处一致", report["ok"])
    check("已收齐未发布批次被识别", [e["batch"] for e in report["checks"]
          ["resume"]["staged_complete"]] == ["B-0003"])
    check("已确认仍缺片批次被识别", [e["batch"] for e in report["checks"]
          ["resume"]["staged_partial"]] == ["B-0004"])

    # -- resume from the original progress
    svc2 = service.SyncService(work / "m-inbox.db", work / "m-publish.db",
                               fix / "data")
    rep = svc2.publish()
    check("B-0003 从暂存续跑发布（无需重传）",
          [b["batch"] for b in rep["applied"]] == ["B-0003"])
    out = svc2.accept(held[1])                      # only the missing part
    check("B-0004 补齐缺片后发布",
          out["publish"]["applied"][0]["batch"] == "B-0004")
    st = svc2.status()
    res = tracing.trace_upstream(svc2.publish_db, "Q4:00007")
    check("迁移后快照推进到 B-0004 且拓扑完整",
          st["snapshot"]["published_batch"] == "B-0004"
          and res["totals"]["upstream_joints"] == 7)
    check("追溯依据批次引用 B-0001..B-0004",
          [b["batch"] for b in res["basis"]["referenced_batches"]]
          == ["B-0001", "B-0002", "B-0003", "B-0004"])


# ---------------------------------------------------------------------------
# 2. acks during publish + write volume
# ---------------------------------------------------------------------------

def check_ack_during_publish(work):
    say("=" * 78)
    say("[2/4] 发布大批次期间：后续片段拿到可恢复的确认；写入量随改动规模")
    svc = service.SyncService(work / "inbox.db", work / "publish.db",
                              work / "data", auto_publish=False)
    big = big_segment(work / "big.json", 40000, 40000)
    svc.accept(big)
    say(f"   大批次 B-BIG 已暂存 (40000 接点 + 40000 管段, "
        f"{big.stat().st_size / 1e6:.1f} MB)，开始后台发布…")

    done = {}
    barrier = threading.Barrier(2)

    def publisher():
        barrier.wait()
        done["t0"] = time.monotonic()
        done["report"] = svc.publish()
        done["t1"] = time.monotonic()

    t = threading.Thread(target=publisher)
    t.start()
    barrier.wait()
    small = small_segment(work / "small.json", "B-0002-p0", "B-0002", 2, 2,
                          taps=[{"ref": "J-090000"}])
    svc2 = service.SyncService(work / "inbox.db", work / "publish.db",
                               work / "data", auto_publish=False)
    out = svc2.accept(small)
    acked_at = time.monotonic()
    t.join(timeout=120)
    publish_secs = done["t1"] - done["t0"]
    ack_secs = acked_at - done["t0"]
    say(f"   发布耗时 {publish_secs:.2f}s；片段确认耗时 {ack_secs:.2f}s")
    check("发布期间后续片段被确认（可恢复的确认）",
          out["acknowledged"] and acked_at < done["t1"],
          f"ack {ack_secs:.2f}s < publish {publish_secs:.2f}s")
    svc3 = service.SyncService(work / "inbox.db", work / "publish.db",
                               work / "data")
    check("重启后该确认仍然有效（重投=duplicate）",
          svc3.accept(small)["status"] == "duplicate")
    svc3.publish()
    check("小批次随后正常发布", svc3.status()["snapshot"]["joints"] == 40001)

    # -- write volume vs change size
    tiny = small_segment(work / "tiny.json", "B-0003-p0", "B-0003", 3, 4,
                         taps=[{"ref": "J-000001",
                                "attrs": {"note": "只改一处接点属性"}}])
    delta_tiny_big = wal_delta(svc3.publish_db, lambda: svc3.accept(tiny))
    small_net = service.SyncService(work / "i2.db", work / "p2.db",
                                    work / "data2")
    small_net.accept(small_segment(work / "base.json", "B-0001-p0", "B-0001",
                                   1, 4, taps=[{"ref": "J-000001"},
                                               {"ref": "J-000002"}]))
    tiny2 = small_segment(work / "tiny2.json", "B-0002-p0", "B-0002", 2, 4,
                          taps=[{"ref": "J-000001",
                                 "attrs": {"note": "只改一处接点属性"}}])
    delta_tiny_small = wal_delta(small_net.publish_db,
                                 lambda: small_net.accept(tiny2))
    medium = small_segment(work / "medium.json", "B-0004-p0", "B-0004", 4, 4,
                           taps=[{"ref": f"J-{i:06d}"}
                                 for i in range(100, 2100)])
    delta_medium = wal_delta(svc3.publish_db, lambda: svc3.accept(medium))
    say(f"   小批(改 1 处属性) 写入: 4 万接点网络上 {delta_tiny_big / 1024:.0f} KiB,"
        f" 2 接点网络上 {delta_tiny_small / 1024:.0f} KiB；"
        f" 2000 接点批次写入 {delta_medium / 1024:.0f} KiB")
    check("小批写入量小且不随全网体量放大",
          delta_tiny_big < 512 * 1024
          and delta_tiny_big < delta_tiny_small * 4 + 4096
          and delta_tiny_small < delta_tiny_big * 4 + 4096)
    check("写入量随改动规模增长", delta_medium > delta_tiny_big * 2,
          f"{delta_medium / 1024:.0f} KiB > 2 × "
          f"{delta_tiny_big / 1024:.0f} KiB")
    return svc3


# ---------------------------------------------------------------------------
# 3. hard-kill recovery
# ---------------------------------------------------------------------------

def check_hard_kill(work):
    say("=" * 78)
    say("[3/4] 强停恢复：kill -9 后确认不丢、状态一致、效果只生效一次")
    segs = [big_segment(work / "kill-src" / f"big-p{p}.json", 40000, 40000,
                        batch="B-KILL", seq=1, total=1, part=p, parts=2)
            for p in (0, 1)]
    inbox, publish = work / "k-inbox.db", work / "k-publish.db"
    script = f"""
import sys; sys.path.insert(0, {str(ROOT)!r})
from app import service
svc = service.SyncService({str(inbox)!r}, {str(publish)!r},
                          {str(work / 'k-data')!r}, auto_publish=False)
for p in {[str(s) for s in segs]!r}:
    print("ACK", svc.accept(p)["segment"], flush=True)
print("PUBLISHING", flush=True)
svc.publish()
print("PUBLISHED", flush=True)
import time; time.sleep(60)
"""
    proc = subprocess.Popen([sys.executable, "-I", "-c", script],
                            stdout=subprocess.PIPE, text=True)
    killed_at = "?"
    try:
        say(f"   子进程: {proc.stdout.readline().strip()} / "
            f"{proc.stdout.readline().strip()}")
        say(f"   子进程: {proc.stdout.readline().strip()}  →  0.3s 后 kill -9")
        killed_at = "mid-publish"
        time.sleep(0.3)
    finally:
        os.kill(proc.pid, signal.SIGKILL)
        proc.wait()
    say(f"   已强停（{killed_at}），重启服务…")

    svc = service.SyncService(inbox, publish, work / "k-data",
                              auto_publish=False)
    dup = all(svc.accept(s)["status"] == "duplicate" for s in segs)
    check("强停前已确认的片段重启后仍有效", dup)
    svc.publish()
    st = svc.status()
    pub = connect(publish, read_only=True)
    try:
        ledger_state = pub.execute(
            "SELECT state FROM published_batches WHERE batch_id='B-KILL'"
        ).fetchone()[0]
    finally:
        pub.close()
    check("批次状态与可查询版本一致（接收库=发布库=applied）",
          st["batches"][0]["state"] == "applied" and ledger_state == "applied")
    detail = svc.batch_detail("B-KILL")
    check("已提交的改接效果只生效一次（接点/管段数精确）",
          st["snapshot"]["joints"] == 40000
          and st["snapshot"]["pipes"] == 40000
          and detail["effects"]["tap_created"] == 40000
          and detail["effects"]["lay_created"] == 40000,
          f"joints={st['snapshot']['joints']} "
          f"tap_created={detail['effects']['tap_created']}")
    return svc


# ---------------------------------------------------------------------------
# 4. queries survive an inbox outage
# ---------------------------------------------------------------------------

def check_inbox_outage(svc, work):
    say("=" * 78)
    say("[4/4] 接收库停止：HTTP 查接点编号、上游、依据批次不断线")
    srv = make_server(svc.publish_db, inbox_db=svc.inbox_db, port=0)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    port = srv.server_address[1]

    def get(path):
        with urllib.request.urlopen(f"http://127.0.0.1:{port}{path}",
                                    timeout=10) as r:
            return json.loads(r.read().decode())

    stopped = str(svc.inbox_db) + ".stopped"
    os.rename(svc.inbox_db, stopped)
    try:
        joint = get("/v1/joint?ref=J-000000")
        trace = get("/v1/trace/upstream?ref=J-000100")
        batches = get("/v1/batches")
        check("接收库停了，接点编号查询照常",
              joint["uid"] == 1 and joint["display"] == "J-000000")
        check("接收库停了，上游追溯照常",
              trace["totals"]["upstream_joints"] > 0)
        check("接收库停了，依据批次照常",
              trace["basis"]["referenced_batches"][0]["batch"] == "B-BIG")
        check("批次进度降级为发布侧视图（不报错）",
              batches["inbox"] == "unavailable"
              and batches["publish_blocker"]["reason"] == "inbox_unavailable")
    finally:
        os.rename(stopped, svc.inbox_db)
        srv.shutdown()
        srv.server_close()
    check("接收库恢复后进度视图回来",
          service.status_report(svc.publish_db, svc.inbox_db)["inbox"]
          == "available")


# ---------------------------------------------------------------------------

def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--work", default=None,
                    help="directory for fixtures (default: a temp dir)")
    ap.add_argument("--keep", action="store_true",
                    help="keep the work directory afterwards")
    args = ap.parse_args()

    if args.work:
        work = Path(args.work)
        if work.exists():
            shutil.rmtree(work)
        work.mkdir(parents=True)
    else:
        work = Path(tempfile.mkdtemp(prefix="drain-splitcheck-"))

    t0 = time.monotonic()
    say(f"工作目录 work dir: {work}")
    check_migration(work / "m")
    svc2 = check_ack_during_publish(work / "a")
    check_hard_kill(work / "k")
    check_inbox_outage(svc2, work / "a")

    secs = time.monotonic() - t0
    say("=" * 78)
    if FAILURES:
        say(f"结果: {len(FAILURES)} 项未通过: {FAILURES}  ({secs:.1f}s)")
        say("SPLITCHECK FAILED")
        return 1
    say(f"结果: 全部通过  ({secs:.1f}s < 80s)")
    say("SPLITCHECK OK")
    if not args.keep and not args.work:
        shutil.rmtree(work, ignore_errors=True)
    else:
        say(f"夹具保留在 {work}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
