"""Command line entry point.

    python -m app sync    --inbox-db I --publish-db P --data DIR
                          [--from-source DIR] [--watch SEC]
    python -m app serve   --publish-db P [--inbox-db I] [--host H] [--port P]
    python -m app status  --publish-db P [--inbox-db I]
    python -m app trace   --publish-db P --ref REF [--format text|json]
    python -m app migrate --db OLD --inbox-db I --publish-db P [--force]

Receiving (改接日志接收) and publishing (管网发布) use two independent
database files: queries keep answering from the publish database while the
inbox is stopped, and receiving keeps acknowledging while a big batch is
being published.

``sync --from-source`` replays every ``*.json`` segment in a directory (the
offline replay source's delivery folder); ``--watch`` keeps polling it so
segments that arrive later are picked up too.

``migrate`` splits a pre-split single database into the inbox/publish pair
and verifies that snapshot, id mapping, topology and batch basis all match.
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path

from . import migrate, service, tracing


def _cmd_sync(args):
    svc = service.SyncService(args.inbox_db, args.publish_db, args.data)
    seen = set()
    total = 0
    while True:
        paths = sorted(Path(args.from_source).glob("*.json"))
        for p in paths:
            key = (p.name, p.stat().st_mtime_ns, p.stat().st_size)
            if key in seen:
                continue
            seen.add(key)
            try:
                out = svc.accept(p)
            except Exception as exc:  # noqa: BLE001
                # nothing is acknowledged, so the source keeps its copy and
                # can fix and resend; keep replaying the rest of the folder
                print(f"REJECTED {p.name}: {exc}", file=sys.stderr)
                continue
            total += 1
            pub = out.get("publish") or {}
            applied = ",".join(b["batch"] for b in pub.get("applied", [])) or "-"
            print(f"{out['status']:9s} {out['segment']} batch={out['batch']} "
                  f"part={out['part']}/{out['parts']} applied={applied}")
        if not args.watch:
            break
        time.sleep(args.watch)
    status = svc.status()
    print(json.dumps({"accepted": total,
                      "snapshot": status["snapshot"],
                      "publish_blocker": status["publish_blocker"]},
                     ensure_ascii=False, indent=1))
    return 0


def _cmd_serve(args):
    from .server import make_server
    if not Path(args.publish_db).exists():
        print(f"no publish database at {args.publish_db}: run `sync` first",
              file=sys.stderr)
        return 2
    srv = make_server(args.publish_db, inbox_db=args.inbox_db,
                      data_dir=args.data, host=args.host, port=args.port,
                      quiet=not args.verbose)
    host, port = srv.server_address[:2]
    print(f"serving on http://{host}:{port}/v1/trace/upstream?ref=REF",
          flush=True)
    try:
        srv.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        srv.server_close()
    return 0


def _cmd_status(args):
    if not Path(args.publish_db).exists():
        print(f"no publish database at {args.publish_db}: run `sync` first",
              file=sys.stderr)
        return 2
    print(json.dumps(service.status_report(args.publish_db, args.inbox_db),
                     ensure_ascii=False, indent=1))
    return 0


def _cmd_trace(args):
    if not Path(args.publish_db).exists():
        print(f"no publish database at {args.publish_db}: run `sync` first",
              file=sys.stderr)
        return 2
    try:
        res = tracing.trace_upstream(args.publish_db, args.ref,
                                     limit=args.limit)
    except tracing.NotFound as exc:
        print(f"not found: {exc}", file=sys.stderr)
        return 3
    except tracing.LimitExceeded as exc:
        print(f"too large: {exc}", file=sys.stderr)
        return 4
    if args.format == "text":
        from .server import render_text
        print(render_text(res), end="")
    else:
        print(json.dumps(res, ensure_ascii=False, indent=1))
    return 0


def _cmd_migrate(args):
    try:
        report = migrate.migrate_legacy(args.db, args.inbox_db,
                                        args.publish_db, force=args.force)
    except migrate.MigrationError as exc:
        print(f"migration failed: {exc}", file=sys.stderr)
        return 2
    print(migrate.format_report(report))
    return 0 if report.get("ok") else 1


def main(argv=None):
    ap = argparse.ArgumentParser(prog="app", description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)

    s = sub.add_parser("sync", help="receive segment files and publish batches")
    s.add_argument("--inbox-db", required=True, help="receiving database")
    s.add_argument("--publish-db", required=True, help="publishing database")
    s.add_argument("--data", required=True)
    s.add_argument("--from-source", required=True,
                   help="directory of *.json segments to deliver")
    s.add_argument("--watch", type=float, default=0,
                   help="poll the directory every N seconds")
    s.set_defaults(func=_cmd_sync)

    s = sub.add_parser("serve", help="run the local query API")
    s.add_argument("--publish-db", required=True)
    s.add_argument("--inbox-db", default=None,
                   help="optional: adds receive progress to /v1/batches")
    s.add_argument("--data", default="")
    s.add_argument("--host", default="127.0.0.1")
    s.add_argument("--port", type=int, default=8787)
    s.add_argument("--verbose", action="store_true")
    s.set_defaults(func=_cmd_serve)

    s = sub.add_parser("status", help="print sync / publish state")
    s.add_argument("--publish-db", required=True)
    s.add_argument("--inbox-db", default=None)
    s.set_defaults(func=_cmd_status)

    s = sub.add_parser("trace", help="trace upstream from the command line")
    s.add_argument("--publish-db", required=True)
    s.add_argument("--ref", required=True)
    s.add_argument("--format", choices=("text", "json"), default="text")
    s.add_argument("--limit", type=int, default=50000)
    s.set_defaults(func=_cmd_trace)

    s = sub.add_parser("migrate",
                       help="split a pre-split single database into the "
                            "inbox/publish pair")
    s.add_argument("--db", required=True, help="the old single database")
    s.add_argument("--inbox-db", required=True)
    s.add_argument("--publish-db", required=True)
    s.add_argument("--force", action="store_true",
                   help="replace existing target databases")
    s.set_defaults(func=_cmd_migrate)

    args = ap.parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())
