"""The endpoint Odoo's "Refresh from OASIS" button calls.

WHY THIS EXISTS AND WHERE IT LIVES
----------------------------------
Odoo cannot compute the transfer plan: OASIS is a separate application with its
own dependencies, and it needs full ERP visibility — cost price, item master,
receipt dates. So the button in Odoo has to call OASIS over HTTP, and something
has to answer.

Not the Cloud Hub. The hub is deliberately supplier-facing and privacy-filtered
— it carries no cost price and no item master by design — so it cannot compute
ordering or transfers, and giving it the access to do so would undo the reason
it is safe to expose. This runs ON-PREM, beside Odoo, where the data already is.

Standard library only, like the rest of the connector.

    python connectors/odoo/scan_service.py
    python connectors/odoo/scan_service.py --host 0.0.0.0 --port 8710

    POST /scan      {"limit": 200}  ->  {"created": 200, "skipped": 0}
    GET  /health                    ->  {"ok": true, "scanning": false}

SECURITY
--------
A successful POST rewrites a queue an operator approves stock movements from.
That is worth a token. If OASIS_SCAN_TOKEN is set, callers must present it as
``Authorization: Bearer <token>``. Binding to anything other than loopback
WITHOUT a token is refused outright rather than warned about — an unauthenticated
endpoint on 0.0.0.0 that drives an ERP is not a default anyone should get by
omission.

FROM INSIDE DOCKER
------------------
Odoo reaches this by host, not by localhost — localhost inside the Odoo
container is the Odoo container. On Docker Desktop set the Odoo system
parameter ``oasis.scan_url`` to:

    http://host.docker.internal:8710/scan
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)

from push_transfer_suggestions import run_scan            # noqa: E402

log = logging.getLogger("oasis.scan")

#: One scan at a time. A scan reads fourteen sites and rewrites the queue; two
#: overlapping runs would race on oasis_replace_queue and could leave the
#: operator looking at half of one plan and half of another. A second caller is
#: told to wait rather than queued, because by the time a queued scan ran its
#: answer would be stale anyway.
_LOCK = threading.Lock()
_STATE = {"scanning": False, "last": None, "last_result": None}


class Handler(BaseHTTPRequestHandler):
    server_version = "OASIS-scan/1.0"
    token = ""
    default_limit = 0

    def log_message(self, fmt, *args):          # route through logging
        log.info("%s %s", self.address_string(), fmt % args)

    def _send(self, code, payload):
        body = json.dumps(payload).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _authorised(self):
        if not self.token:
            return True
        got = (self.headers.get("Authorization") or "").strip()
        return got == "Bearer %s" % self.token

    def do_GET(self):
        if self.path.rstrip("/") in ("/health", ""):
            return self._send(200, {"ok": True, "scanning": _STATE["scanning"],
                                    "last_scan": _STATE["last"],
                                    "last_result": _STATE["last_result"]})
        self._send(404, {"error": "not found"})

    def do_POST(self):
        if self.path.rstrip("/") != "/scan":
            return self._send(404, {"error": "not found"})
        if not self._authorised():
            log.warning("rejected unauthorised scan from %s", self.address_string())
            return self._send(401, {"error": "bad or missing bearer token"})

        try:
            n = int(self.headers.get("Content-Length") or 0)
            body = json.loads(self.rfile.read(n) or b"{}") if n else {}
        except (ValueError, json.JSONDecodeError):
            return self._send(400, {"error": "body must be JSON"})

        limit = int(body.get("limit") or self.default_limit)

        if not _LOCK.acquire(blocking=False):
            return self._send(409, {
                "error": "a scan is already running",
                "detail": "Fourteen sites take a minute or so. Try again when "
                          "it finishes — the queue updates in one step."})
        try:
            _STATE["scanning"] = True
            t0 = time.time()
            log.info("scan requested by %s (limit=%s)", self.address_string(), limit)
            res = run_scan(limit=limit, log=lambda m: log.info("%s", m))
            res["seconds"] = round(time.time() - t0, 1)
            _STATE["last"] = time.strftime("%Y-%m-%d %H:%M:%S")
            _STATE["last_result"] = res
            log.info("scan done in %ss: %s", res["seconds"], res)
            return self._send(200, res)
        except Exception as e:                       # never leak a traceback
            log.exception("scan failed")
            return self._send(500, {"error": type(e).__name__,
                                    "detail": str(e)[:300]})
        finally:
            _STATE["scanning"] = False
            _LOCK.release()


def main(argv=None):
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--host", default=os.getenv("OASIS_SCAN_HOST", "127.0.0.1"))
    p.add_argument("--port", type=int, default=int(os.getenv("OASIS_SCAN_PORT", "8710")))
    p.add_argument("--limit", type=int, default=int(os.getenv("OASIS_SCAN_LIMIT", "0")),
                   help="default suggestions per scan when the caller sends none")
    args = p.parse_args(argv)

    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(message)s")
    token = (os.getenv("OASIS_SCAN_TOKEN") or "").strip()

    loopback = args.host in ("127.0.0.1", "localhost", "::1")
    if not loopback and not token:
        raise SystemExit(
            "Refusing to bind %s without a token.\n\n"
            "A POST to /scan rewrites the queue an operator approves stock "
            "movements from, so an endpoint reachable off this machine needs "
            "one. Set OASIS_SCAN_TOKEN and put the same value in Odoo's "
            "'oasis.scan_token' system parameter.\n\n"
            "For a local-only trial use --host 127.0.0.1." % args.host)

    Handler.token = token
    Handler.default_limit = args.limit
    srv = ThreadingHTTPServer((args.host, args.port), Handler)
    log.info("OASIS scan service on http://%s:%d  (token %s, default limit %s)",
             args.host, args.port, "required" if token else "NOT SET — loopback only",
             args.limit or "all")
    log.info("Odoo system parameter  oasis.scan_url = http://<host>:%d/scan", args.port)
    try:
        srv.serve_forever()
    except KeyboardInterrupt:
        log.info("stopping")
        srv.shutdown()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
