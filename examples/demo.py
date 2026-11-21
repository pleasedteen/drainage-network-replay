"""End-to-end walk-through: replay the sample source, query by HTTP, show the
answer and the publish state -- and prove that queries keep answering when
the inbox database is stopped.

Run:  python3 examples/demo.py
Everything it creates goes to a temporary directory that it prints.
"""

from __future__ import annotations

import json
import sys
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from app import replay, service, tracing            # noqa: E402
from app.ids import parse_ref                        # noqa: E402
from app.server import make_server                   # noqa: E402


def get(port, path):
    with urllib.request.urlopen(f"http://127.0.0.1:{port}{path}", timeout=10) as r:
        return r.read().decode()


def main():
    work = Path("/tmp/drain-demo")
    if work.exists():
        import shutil
        shutil.rmtree(work)
    work.mkdir(parents=True)

    print("=" * 78)
    print("1. offline replay source writes its segment files")
    src = work / "source"
    paths = replay.generate_demo(src)
    print(f"   {len(paths)} segment files generated in {src}")
    print(f"   batches: {sorted({p.name.split('-p')[0] for p in paths})}")

    print()
    print("2. sync service receives them (out of order, with duplicates)")
    print("   receiving -> inbox db, publishing -> publish db (two files)")
    svc = service.SyncService(work / "inbox.db", work / "publish.db",
                              work / "data")
    order = lambda ps: list(reversed(sorted(ps)))
    stats = replay.deliver(src, svc, order=order, duplicate_every=3,
                           duplicate_seed=11)
    print(f"   accepted={len(stats['accepted'])} "
          f"duplicates={stats['duplicates']} rejected={stats['rejected']}")

    print()
    print("3. publish state")
    st = svc.status()
    print(f"   published snapshot : {st['snapshot']}")
    print(f"   blocker            : {st['publish_blocker']}")
    for b in st["batches"]:
        print(f"   {b['batch']:8s} seq={b['seq']} state={b['state']:8s} "
              f"parts={b.get('parts_received')}/{b.get('parts_expected')}"
              f"{'  error=' + b['error'] if b['error'] else ''}")

    print()
    print("4. query over HTTP (both spellings of the same joint)")
    srv = make_server(work / "publish.db", inbox_db=work / "inbox.db",
                      data_dir=work / "data", port=0)
    import threading
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    port = srv.server_address[1]
    for ref in ("J-001012", "C4:01012"):
        body = get(port, f"/v1/trace/upstream?ref={ref}")
        data = json.loads(body)
        print(f"   GET /v1/trace/upstream?ref={ref}")
        print(f"     -> {data['totals']}")
        print(f"     -> basis batches "
              f"{[b['batch'] for b in data['basis']['referenced_batches']]}")

    print()
    print("5. the inbox stops (接收库停止): queries keep answering")
    stopped = work / "inbox.db.stopped"
    (work / "inbox.db").rename(stopped)     # simulate an inbox outage
    for suffix in ("-wal", "-shm"):
        side = work / f"inbox.db{suffix}"
        if side.exists():
            side.rename(work / f"inbox.db.stopped{suffix}")
    body = get(port, "/v1/trace/upstream?ref=C4:01012")
    data = json.loads(body)
    print(f"   trace still answers: {data['totals']}")
    body = get(port, "/v1/joint?ref=J-001012")
    print(f"   joint lookup still answers: uid={json.loads(body)['uid']}")
    body = json.loads(get(port, "/v1/batches"))
    print(f"   /v1/batches degrades: inbox={body['inbox']} "
          f"blocker={body['publish_blocker']['reason']} "
          f"(publish-side batch states still listed: "
          f"{[b['batch'] for b in body['batches']]})")
    stopped.rename(work / "inbox.db")       # inbox is back
    for suffix in ("-wal", "-shm"):
        side = work / f"inbox.db.stopped{suffix}"
        if side.exists():
            side.rename(work / f"inbox.db{suffix}")

    print()
    print("6. human-readable answer for the duty officer")
    print(get(port, "/v1/trace/upstream?ref=C4:01012&format=text"))
    srv.shutdown()

    print("artifacts kept in", work)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
