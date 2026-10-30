"""HTTP client for a SaaS source: OAuth client-credentials, retries and rate-limit handling.

- 401: refresh the token once and retry (tokens expire on the provider's schedule, not ours).
- 429: wait for Retry-After (capped), then retry; this doesn't count as a failure.
- 5xx and timeouts: exponential backoff, up to max_retries.
- Other 4xx: raise immediately; retrying a bad request only burns quota.
"""
from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Callable

import httpx


class ConnectorError(Exception):
    def __init__(self, message: str, status: int | None = None, retryable: bool = False) -> None:
        super().__init__(message)
        self.status = status
        self.retryable = retryable


@dataclass
class Stats:
    requests: int = 0
    retries: int = 0
    rate_limited: int = 0
    token_refreshes: int = 0
    waited_seconds: float = 0.0
    errors: dict = field(default_factory=dict)


class ApiClient:
    def __init__(self, base_url: str, client_id: str, client_secret: str, *, transport: httpx.BaseTransport | None = None,
                 sleep: Callable[[float], None] = time.sleep, max_retries: int = 5, backoff_s: float = 1.0,
                 max_retry_after_s: float = 60.0, max_rate_limit_waits: int = 20, timeout_s: float = 30.0) -> None:
        self._http = httpx.Client(base_url=base_url, transport=transport, timeout=timeout_s)
        self._client_id, self._client_secret = client_id, client_secret
        self._token: str | None = None
        self._sleep = sleep
        self.max_retries, self.backoff_s, self.max_retry_after_s = max_retries, backoff_s, max_retry_after_s
        self.max_rate_limit_waits = max_rate_limit_waits
        self.stats = Stats()

    def _refresh_token(self) -> None:
        r = self._http.post("/oauth/token", data={"grant_type": "client_credentials", "client_id": self._client_id,
                                                  "client_secret": self._client_secret})
        if r.status_code != 200:
            raise ConnectorError("Authentication failed: check client id and secret", r.status_code)
        self._token = r.json()["access_token"]
        self.stats.token_refreshes += 1

    def _wait(self, seconds: float) -> None:
        self.stats.waited_seconds += seconds
        self._sleep(seconds)

    def get(self, path: str, params: dict) -> dict:
        if self._token is None:
            self._refresh_token()
        failures = 0
        rate_limit_waits = 0
        refreshed = False
        while True:
            self.stats.requests += 1
            try:
                r = self._http.get(path, params=params, headers={"Authorization": f"Bearer {self._token}"})
            except httpx.TransportError as exc:  # timeouts, resets
                status, r = None, None
                reason = type(exc).__name__
            else:
                status, reason = r.status_code, f"HTTP {r.status_code}"
            if r is not None and status == 200:
                return r.json()
            self.stats.errors[reason] = self.stats.errors.get(reason, 0) + 1

            if status == 401 and not refreshed:
                refreshed = True
                self._refresh_token()
                continue
            if status == 429:
                rate_limit_waits += 1
                if rate_limit_waits > self.max_rate_limit_waits:
                    raise ConnectorError("Still rate-limited after repeated waits; try again later", 429, retryable=True)
                self.stats.rate_limited += 1
                retry_after = _retry_after(r.headers.get("Retry-After"), default=self.backoff_s)
                self._wait(min(retry_after, self.max_retry_after_s))
                continue
            if status is None or status >= 500:
                failures += 1
                if failures > self.max_retries:
                    raise ConnectorError(f"Gave up after {failures} attempts: {reason}", status, retryable=True)
                self.stats.retries += 1
                self._wait(self.backoff_s * 2 ** (failures - 1))
                continue
            raise ConnectorError(f"Request rejected by source: {reason}", status, retryable=False)


def _retry_after(header: str | None, default: float) -> float:
    try:
        return max(0.0, float(header)) if header is not None else default
    except ValueError:
        return default  # HTTP-date form isn't used by this source; fall back rather than crash
