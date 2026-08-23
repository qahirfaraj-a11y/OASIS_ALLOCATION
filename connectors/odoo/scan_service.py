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

    POST /scan   {"limit": 200}                   -> transfers (the default)
    POST /scan   {"kind": "orders", "limit": 200} -> replenishment
                 ->  {"created": 200, "skipped": 0, "kind": "orders"}
    GET  /health                                  -> {"ok": true, "scanning": false}

ONE ENDPOINT, TWO PLANS. `kind` selects which queue is rebuilt. It defaults to
transfers because that is what every request from an older OASIS Transfers
looks like, and upgrading this service must not break an installed Refresh
button. An unrecognised kind is refused rather than defaulted: computing the
wrong plan and reporting success is the worst available outcome.

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
from push_order_suggestions import run_scan as run_order_scan   # noqa: E402

#: One endpoint, two plans. The body says which.
#:
#: `kind` is absent from every request an older OASIS Transfers sends, so the
#: default MUST stay transfers — an existing install's Refresh button has to go
#: on working after this service is upgraded. Replenishment sends
#: {"kind": "orders"} explicitly.
_SCANS = {
    "transfers": run_scan,
    "orders": run_order_scan,
}

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
        kind = str(body.get("kind") or "transfers").strip().lower()
        if kind not in _SCANS:
            return self._send(400, {
                "error": "unknown scan kind %r" % kind,
                "detail": "Expected one of: %s. An unrecognised kind is refused "
                          "rather than defaulting, because silently computing "
                          "the wrong plan and reporting success is worse than "
                          "an error." % ", ".join(sorted(_SCANS))})

        if not _LOCK.acquire(blocking=False):
            return self._send(409, {
                "error": "a scan is already running",
                "detail": "Fourteen sites take a minute or so. Try again when "
                          "it finishes — the queue updates in one step."})
        try:
            _STATE["scanning"] = True
            t0 = time.time()
            log.info("%s scan requested by %s (limit=%s)",
                     kind, self.address_string(), limit)
            res = _SCANS[kind](limit=limit, log=lambda m: log.info("%s", m))
            res["kind"] = kind
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


def _scheduled(interval_min, offset_min, limit):
    """Rescan on a fixed cadence, phase-shifted off the hour.

    STAGGERED ON PURPOSE. The connector's telemetry cron and this scan both
    read the same Odoo, and both are heavy: the cron walks new movement, the
    scan reads every product at every site. Firing them together doubles peak
    load on the database that is also serving the tills. The offset moves this
    off the hour and away from a cron sitting on :00/:30.

    A scan is skipped, never queued, while one is already running — see the
    lock. A queued scan's answer would be stale by the time it ran.
    """
    period = interval_min * 60
    while True:
        now = time.time()
        # next multiple of the period, plus the phase offset
        nxt = (now // period + 1) * period + offset_min * 60
        time.sleep(max(5.0, nxt - now))
        if not _LOCK.acquire(blocking=False):
            log.info("scheduled scan skipped — one is already running")
            continue
        try:
            _STATE["scanning"] = True
            t0 = time.time()
            # BOTH plans on the schedule, in one slot under one lock.
            #
            # A client running Replenishment on a cadence should not have to
            # stand up a second service, and running the two on separate
            # timers would let them overlap — the thing the lock exists to
            # prevent. A kind whose module is not installed simply reports a
            # failure for that half; the other still lands.
            res = {}
            for kind, fn in sorted(_SCANS.items()):
                try:
                    res[kind] = fn(limit=limit, log=lambda m: log.info("%s", m))
                except Exception as e:
                    log.warning("scheduled %s scan failed: %s", kind, str(e)[:200])
                    res[kind] = {"error": type(e).__name__, "detail": str(e)[:200]}
            res["seconds"] = round(time.time() - t0, 1)
            _STATE["last"] = time.strftime("%Y-%m-%d %H:%M:%S")
            _STATE["last_result"] = res
            log.info("scheduled scan: %s", res)
        except Exception:
            log.exception("scheduled scan failed")
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
    p.add_argument("--interval", type=int,
                   default=int(os.getenv("OASIS_SCAN_INTERVAL_MIN", "0")),
                   help="rescan every N minutes (0 = only when asked)")
    p.add_argument("--offset", type=int,
                   default=int(os.getenv("OASIS_SCAN_OFFSET_MIN", "15")),
                   help="minutes past the interval boundary, to stay clear of "
                        "the connector's telemetry cron (default 15)")
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
    if args.interval > 0:
        threading.Thread(target=_scheduled,
                         args=(args.interval, args.offset, args.limit),
                         daemon=True).start()
        log.info("rescanning every %d min at +%d min past the boundary, "
                 "staggered clear of the telemetry cron",
                 args.interval, args.offset)
    else:
        log.info("on-demand only — pass --interval to rescan on a cadence")
    try:
        srv.serve_forever()
    except KeyboardInterrupt:
        log.info("stopping")
        srv.shutdown()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
