"""python -m connector_kit.demo : a full backfill, then incremental syncs through realistic trouble."""
from __future__ import annotations

import json

from .destination import SQLiteDestination
from .http import ApiClient, ConnectorError
from .mock_api import BASE, MockCRM
from .source import ContactsStream
from .sync import run_sync


def main() -> None:
    crm = MockCRM(token_ttl_requests=2)  # tokens expire every 2 calls, so refreshes happen mid-sync
    crm.seed(250)
    dest = SQLiteDestination()

    def stream():
        client = ApiClient(BASE, "demo-client", "demo-secret", transport=crm.transport(), sleep=lambda s: None)
        return ContactsStream(client, page_size=100)

    def show(title, r):
        print(f"\n== {title}")
        print(json.dumps({k: r.to_dict()[k] for k in ("pages", "records", "upserted", "deleted", "new_columns", "state")}, indent=1))
        h = r.http
        print(f"   http: {h['requests']} requests, {h['retries']} retries, {h['rate_limited']} rate-limited, "
              f"{h['token_refreshes']} token refreshes, waited {h['waited_seconds']}s")

    crm.inject("429", "503")
    show("1. Backfill 250 contacts (one 429 and one 503 along the way)", run_sync(stream(), dest))

    crm.upsert("c_0003", title="CTO")  # a new field appears
    crm.upsert("c_0004", same_timestamp=True, name="Person Four")  # same timestamp as the previous change
    crm.delete("c_0005")
    show("2. Incremental: 1 new field, 2 changes sharing a timestamp, 1 delete", run_sync(stream(), dest))

    for i in range(251, 451):
        crm.upsert(f"c_{i:04d}", email=f"person{i}@example.test", name=f"Person {i}")
    crm.inject("ok", *["503"] * 6)  # page 1 succeeds, then the source goes down
    try:
        run_sync(stream(), dest)
    except ConnectorError as e:
        print(f"\n== 3. Source outage mid-sync -> {e}. Checkpoint kept: {dest.get_state('contacts')}")
    show("4. Resume after the outage (no duplicates, no gaps)", run_sync(stream(), dest))

    rows = dest.rows("contacts")
    print(f"\nTotal rows: {len(rows)} (expected 450), deleted: {sum(r['_deleted'] for r in rows)}, "
          f"columns: {[c for c in rows[0] if not c.startswith('_')]}")


if __name__ == "__main__":
    main()
