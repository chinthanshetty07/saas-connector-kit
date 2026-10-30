"""An in-process fake SaaS CRM API, served through httpx.MockTransport.

It behaves like real SaaS APIs do in the ways that break connectors: OAuth tokens expire,
results come in cursor pages, the API rate-limits with 429 + Retry-After, it has bad moments
(5xx), records get soft-deleted, nested objects appear, and new fields show up without notice.
All data is synthetic.
"""
from __future__ import annotations

import base64
import json
import secrets
from collections import deque
from datetime import datetime, timedelta, timezone

import httpx

BASE = "https://api.mockcrm.test"


def iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


class MockCRM:
    def __init__(self, client_id: str = "demo-client", client_secret: str = "demo-secret",
                 token_ttl_requests: int = 50, max_page_size: int = 100) -> None:
        self.client_id, self.client_secret = client_id, client_secret
        self.token_ttl_requests = token_ttl_requests
        self.max_page_size = max_page_size
        self.records: dict[str, dict] = {}
        self.tokens: dict[str, int] = {}  # token -> remaining requests
        self.faults: deque[str] = deque()  # "ok", "429", "500", "503", "401", "400": one consumed per API call
        self.clock = datetime(2026, 10, 1, tzinfo=timezone.utc)
        self.calls: list[str] = []

    # --- data management (what "users of the SaaS app" do) -----------------------------
    def tick(self, seconds: int = 1) -> str:
        self.clock += timedelta(seconds=seconds)
        return iso(self.clock)

    def upsert(self, record_id: str, *, same_timestamp: bool = False, **fields) -> dict:
        ts = iso(self.clock) if same_timestamp else self.tick()
        rec = self.records.get(record_id, {"id": record_id, "deleted": False})
        rec.update(fields)
        rec["updated_at"] = ts
        self.records[record_id] = rec
        return rec

    def delete(self, record_id: str) -> None:
        self.records[record_id]["deleted"] = True
        self.records[record_id]["updated_at"] = self.tick()

    def seed(self, n: int) -> None:
        for i in range(1, n + 1):
            self.upsert(f"c_{i:04d}", email=f"person{i}@example.test", name=f"Person {i}",
                        company={"name": f"Company {i % 7}", "size": 10 * (i % 5 + 1)})

    def inject(self, *faults: str) -> None:
        self.faults.extend(faults)

    # --- HTTP --------------------------------------------------------------------------
    def transport(self) -> httpx.MockTransport:
        return httpx.MockTransport(self.handle)

    def handle(self, request: httpx.Request) -> httpx.Response:
        self.calls.append(f"{request.method} {request.url.path}")
        if request.url.path == "/oauth/token":
            return self._token(request)
        if request.url.path == "/v1/contacts":
            fault = self.faults.popleft() if self.faults else None
            if fault == "429":
                return httpx.Response(429, headers={"Retry-After": "2"}, json={"error": "rate_limited"})
            if fault in ("500", "502", "503"):
                return httpx.Response(int(fault), json={"error": "upstream"})
            if fault == "400":
                return httpx.Response(400, json={"error": "bad_request"})
            if fault == "401" or not self._authorized(request):
                return httpx.Response(401, json={"error": "invalid_token"})
            return self._list(request)
        return httpx.Response(404, json={"error": "not_found"})

    def _token(self, request: httpx.Request) -> httpx.Response:
        form = dict(x.split("=", 1) for x in request.content.decode().split("&") if "=" in x)
        if form.get("grant_type") != "client_credentials" or form.get("client_id") != self.client_id \
                or form.get("client_secret") != self.client_secret:
            return httpx.Response(401, json={"error": "invalid_client"})
        token = secrets.token_hex(8)
        self.tokens[token] = self.token_ttl_requests
        return httpx.Response(200, json={"access_token": token, "token_type": "bearer", "expires_in": 3600})

    def _authorized(self, request: httpx.Request) -> bool:
        auth = request.headers.get("Authorization", "")
        token = auth.removeprefix("Bearer ")
        if self.tokens.get(token, 0) <= 0:
            return False
        self.tokens[token] -= 1
        return True

    def _list(self, request: httpx.Request) -> httpx.Response:
        q = request.url.params
        since = q.get("updated_since")
        limit = min(int(q.get("limit", "50")), self.max_page_size)
        rows = sorted(self.records.values(), key=lambda r: (r["updated_at"], r["id"]))
        if since:
            rows = [r for r in rows if r["updated_at"] >= since]  # inclusive, like most APIs
        if q.get("cursor"):
            after = tuple(json.loads(base64.urlsafe_b64decode(q["cursor"])))
            rows = [r for r in rows if (r["updated_at"], r["id"]) > after]
        page = rows[:limit]
        next_cursor = None
        if len(rows) > limit:
            last = page[-1]
            next_cursor = base64.urlsafe_b64encode(json.dumps([last["updated_at"], last["id"]]).encode()).decode()
        return httpx.Response(200, json={"data": [dict(r) for r in page], "next_cursor": next_cursor})
