"""
Hub push client — ships movements from Odoo to the OASIS Cloud Hub.

Stdlib only (``urllib``): an Odoo addon should add zero pip dependencies to a
customer's server. The transport is injectable (``poster``) so tests drive the
exact same batching/retry logic against the hub's FastAPI TestClient — the push
path is proven end-to-end, not mocked away.

Idempotency is the hub's job (unique on store_id + source_ref); the client just
chunks and retries, so a re-run after a crash is always safe.
"""

import json
import time
import logging
import urllib.error
import urllib.request
from typing import Callable, List, Optional, Tuple

logger = logging.getLogger("OASIS.Connector.Odoo.Push")

# poster(url, headers, body_dict) -> (status_code, response_dict)
Poster = Callable[[str, dict, dict], Tuple[int, dict]]


class HubPushError(RuntimeError):
    pass


def _urllib_poster(url: str, headers: dict, body: dict) -> Tuple[int, dict]:
    data = json.dumps(body).encode("utf-8")
    req = urllib.request.Request(url, data=data, headers=headers, method="POST")
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            payload = json.loads(resp.read().decode("utf-8") or "{}")
            return resp.getcode(), payload
    except urllib.error.HTTPError as e:
        body_txt = e.read().decode("utf-8", "replace")
        try:
            payload = json.loads(body_txt)
        except ValueError:
            payload = {"detail": body_txt}
        return e.code, payload


class HubPushClient:
    """Batches movement dicts and POSTs them to ``/ingest/movements``."""

    def __init__(self, hub_url: str, ingest_token: str, *,
                 poster: Optional[Poster] = None, batch_size: int = 500,
                 max_retries: int = 3, backoff: float = 1.5):
        self.base = hub_url.rstrip("/")
        self.token = ingest_token
        self._post = poster or _urllib_poster
        self.batch_size = max(1, batch_size)
        self.max_retries = max(0, max_retries)
        self.backoff = backoff

    def _headers(self) -> dict:
        return {"Authorization": f"Bearer {self.token}",
                "Content-Type": "application/json"}

    def _post_batch(self, movements: List[dict]) -> dict:
        url = f"{self.base}/ingest/movements"
        body = {"movements": movements}
        attempt = 0
        while True:
            try:
                status, payload = self._post(url, self._headers(), body)
            except (urllib.error.URLError, OSError) as e:
                status, payload = 0, {"detail": str(e)}
            if status == 200:
                return payload
            # 4xx (except 429) are permanent — don't retry a bad token/payload
            if status in (400, 401, 403, 404, 422):
                raise HubPushError(f"hub rejected batch ({status}): "
                                   f"{payload.get('detail')}")
            attempt += 1
            if attempt > self.max_retries:
                raise HubPushError(f"hub push failed after {attempt} attempts "
                                   f"(last status {status}): {payload.get('detail')}")
            sleep_s = self.backoff ** attempt
            logger.warning("hub push transient failure (status %s), retry %d/%d in %.1fs",
                           status, attempt, self.max_retries, sleep_s)
            time.sleep(sleep_s)

    def push(self, movements: List[dict]) -> dict:
        """Push all movements in chunks. Returns aggregate accepted/duplicates."""
        accepted = duplicates = batches = 0
        for i in range(0, len(movements), self.batch_size):
            chunk = movements[i:i + self.batch_size]
            if not chunk:
                continue
            res = self._post_batch(chunk)
            accepted += res.get("accepted", 0)
            duplicates += res.get("duplicates", 0)
            batches += 1
        logger.info("hub push complete: %d accepted, %d duplicate, %d batch(es)",
                    accepted, duplicates, batches)
        return {"accepted": accepted, "duplicates": duplicates, "batches": batches}
