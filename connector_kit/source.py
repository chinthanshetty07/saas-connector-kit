"""An incremental stream over the CRM's /v1/contacts endpoint.

The checkpoint is a high-water mark: the last updated_at we stored, plus the ids already
stored AT that exact timestamp. The API's updated_since filter is inclusive and timestamps are
only second-precise, so on the next sync we re-request from that timestamp and keep:
  - anything newer than it, and
  - anything at exactly that timestamp whose id we haven't stored there yet.

A plain (updated_at, id) cursor looks equivalent but isn't: a record edited after the checkpoint
in the same second, with an id that sorts lower, would be skipped forever. A test covers that.

One edge remains: the same record edited twice within that one second, after we read it.
lookback_seconds re-reads a window before the mark to cover it, at the cost of re-fetching a
few rows (harmless: the destination upserts).
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Iterator

from .http import ApiClient


@dataclass
class Page:
    records: list[dict]
    state: dict  # the checkpoint to store once these records are safely written


def _shift(ts: str, seconds: int) -> str:
    dt = datetime.fromisoformat(ts.replace("Z", "+00:00")) - timedelta(seconds=seconds)
    return dt.isoformat().replace("+00:00", "Z")


class ContactsStream:
    name = "contacts"
    primary_key = "id"
    cursor_field = "updated_at"

    def __init__(self, client: ApiClient, page_size: int = 100, lookback_seconds: int = 0) -> None:
        self.client = client
        self.page_size = page_size
        self.lookback_seconds = lookback_seconds

    def read(self, state: dict | None) -> Iterator[Page]:
        mark = state["updated_at"] if state else None
        seen_at_mark = set(state.get("ids_at_mark", [])) if state else set()
        params = {"limit": self.page_size}
        if mark:
            params["updated_since"] = _shift(mark, self.lookback_seconds) if self.lookback_seconds else mark
        cursor = None
        while True:
            body = self.client.get("/v1/contacts", {**params, **({"cursor": cursor} if cursor else {})})
            rows = [r for r in body["data"] if self._is_new(r, mark, seen_at_mark)]
            if rows:
                newest = max(r["updated_at"] for r in rows)
                if mark is None or newest > mark:
                    mark, seen_at_mark = newest, set()
                seen_at_mark |= {r["id"] for r in rows if r["updated_at"] == mark}
                yield Page(rows, {"updated_at": mark, "ids_at_mark": sorted(seen_at_mark)})
            cursor = body.get("next_cursor")
            if not cursor:
                return

    def _is_new(self, r: dict, mark: str | None, seen_at_mark: set[str]) -> bool:
        if mark is None or r["updated_at"] > mark:
            return True
        if r["updated_at"] == mark:
            return r["id"] not in seen_at_mark
        return self.lookback_seconds > 0  # inside the lookback window: re-read, upsert makes it harmless
