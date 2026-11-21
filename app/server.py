"""Local HTTP query API (standard library only).

Read-only: opens the database with ``mode=ro`` per request.  Because batches
are published transactionally, a request either sees the network as of batch
N or as of batch N+1, never something in between.

Endpoints
---------
``GET /health``
``GET /v1/snapshot``                     current published snapshot summary
``GET /v1/batches``                      batches, parts, applied/rejected state
``GET /v1/batches/<batch_id>``           one batch: segments, effects
``GET /v1/trace/upstream?ref=...``       the answer the duty officer needs
    optional: ``limit`` (default 50000), ``include_retired=1``, ``format=text``
``GET /v1/joint?ref=...``                resolve one reference (alias check)
"""

from __future__ import annotations

import json
import traceback
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, unquote, urlparse

from . import service, tracing

MAX_TRACE_LIMIT = 500000


class ApiError(Exception):
    def __init__(self, status: int, message: str):
        self.status = status
        self.message = message
        super().__init__(message)


class Handler(BaseHTTPRequestHandler):
    server_version = "DrainTrace/1.0"
    protocol_version = "HTTP/1.1"

    # wiring set by make_server
    db_path: str = ""

    def log_message(self, fmt, *args):  # quieter than the default
        if not getattr(self.server, "quiet", True):
            super().log_message(fmt, *args)

    # -- helpers ----------------------------------------------------------
    def _send(self, status: int, payload: dict | str, ctype="application/json"):
        if not isinstance(payload, str):
            body = json.dumps(payload, ensure_ascii=False, indent=1).encode()
        else:
            body = payload.encode()
        self.send_response(status)
        self.send_header("Content-Type", ctype + "; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def _query(self):
        parsed = urlparse(self.path)
        return parsed.path.rstrip("/") or "/", parse_qs(parsed.query)

    def _one(self, qs, name, default=None):
        vals = qs.get(name)
        if not vals:
            return default
        return vals[-1]

    def do_GET(self):  # noqa: N802
        try:
            path, qs = self._query()
            if path == "/health":
                return self._send(200, {"ok": True})
            if path == "/v1/snapshot":
                conn = _ro(self.db_path)
                try:
                    return self._send(200, tracing.snapshot_info(conn))
                finally:
                    conn.close()
            if path == "/v1/batches":
                svc = service.SyncService(self.db_path, self._data_dir(),
                                          auto_publish=False)
                return self._send(200, svc.status())
            if path.startswith("/v1/batches/"):
                bid = unquote(path[len("/v1/batches/"):])
                svc = service.SyncService(self.db_path, self._data_dir(),
                                          auto_publish=False)
                try:
                    return self._send(200, svc.batch_detail(bid))
                except KeyError:
                    raise ApiError(404, f"unknown batch {bid!r}")
            if path == "/v1/joint":
                ref = self._one(qs, "ref")
                if not ref:
                    raise ApiError(400, "missing ?ref=")
                conn = _ro(self.db_path)
                try:
                    return self._send(200, tracing.resolve_ref(conn, ref))
                finally:
                    conn.close()
            if path == "/v1/trace/upstream":
                return self._trace(qs)
            raise ApiError(404, f"no such endpoint: {path}")
        except ApiError as exc:
            self._send(exc.status, {"error": exc.message})
        except tracing.NotFound as exc:
            self._send(404, {"error": str(exc)})
        except tracing.LimitExceeded as exc:
            self._send(413, {"error": str(exc)})
        except Exception:                      # noqa: BLE001
            tb = traceback.format_exc(limit=6)
            self._send(500, {"error": "internal error", "detail": tb})

    def _trace(self, qs):
        ref = self._one(qs, "ref")
        if not ref:
            raise ApiError(400, "missing ?ref= (e.g. ref=J-004521 or "
                                "ref=Q4:04521)")
        limit = self._one(qs, "limit", str(MAX_TRACE_LIMIT))
        try:
            limit = max(1, min(int(limit), MAX_TRACE_LIMIT))
        except ValueError:
            raise ApiError(400, "limit must be an integer")
        include_retired = self._one(qs, "include_retired", "0") not in (
            "0", "false", "no", "")
        result = tracing.trace_upstream(self.db_path, ref, limit=limit,
                                        include_retired=include_retired)
        if self._one(qs, "format") == "text":
            return self._send(200, render_text(result),
                              ctype="text/plain")
        return self._send(200, result)

    def _data_dir(self):
        return getattr(self.server, "data_dir", "")


def _ro(db_path):
    from .db import connect, network_path
    return connect(network_path(db_path), read_only=True)


def render_text(result: dict) -> str:
    q = result["query"]
    t = result["totals"]
    snap = result["snapshot"]
    lines = [
        f"上游追溯 / upstream trace for {q['ref']}",
        f"  接点 joint   : {q['resolved']['display']}"
        f"  (uid {q['resolved']['uid']}, zone {q['resolved']['zone'] or '-'})",
        f"  快照 snapshot: batch {snap['published_batch']} "
        f"(seq {snap['applied_seq']}, at {snap['published_at']})",
        f"  上游接点 joints: {t['upstream_joints']}   "
        f"上游管段 pipes: {t['upstream_pipes']}   "
        f"最远层数 max depth: {t['max_depth']}",
        "",
        "上游接点 (由近及远):",
    ]
    for j in result["joints"]:
        legacy = ""
        if j["legacy_id"] and j["legacy_id"] != j["display"]:
            legacy = f"  (旧号 {j['legacy_id']})"
        lines.append(f"  [{j['depth']}] {j['display']}{legacy}"
                     f"  zone={j['zone'] or '-'}  batch={j['batch']}")
    lines += ["", "上游管段 (水流 from -> to):"]
    for p in result["pipes"]:
        lines.append(f"  {p['code']}: {p['from']} -> {p['to']}"
                     f"  batch={p['batch']}")
    lines += ["", "依据批次 (basis):"]
    for b in result["basis"]["referenced_batches"]:
        lines.append(f"  {b['batch']} (seq {b['seq']}): "
                     f"{b['joints']} joints, {b['pipes']} pipes")
    return "\n".join(lines) + "\n"


def make_server(db_path, data_dir="", host="127.0.0.1", port=0,
                quiet=True) -> ThreadingHTTPServer:
    handler = type("BoundHandler", (Handler,), {"db_path": str(db_path)})
    srv = ThreadingHTTPServer((host, port), handler)
    srv.data_dir = str(data_dir)
    srv.quiet = quiet
    srv.daemon_threads = True
    return srv
