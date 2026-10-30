# SaaS Connector Kit

[![CI](https://github.com/chinthanshetty07/saas-connector-kit/actions/workflows/ci.yml/badge.svg)](https://github.com/chinthanshetty07/saas-connector-kit/actions/workflows/ci.yml)

A small Python kit for syncing data out of a SaaS API into a warehouse table, focused on what
makes connectors break in production: expiring tokens, rate limits, flaky servers, incremental
syncs that silently skip records, crashes halfway through, and fields that appear without warning.

> **Independent learning project.** I built it after applying for Hevo Data's SDE I role, which
> describes implementing SaaS connectors (auth, data extraction, REST components, pipeline
> efficiency). It is not affiliated with Hevo and uses none of their code. The "SaaS API" is an
> in-process fake CRM with synthetic data. Built with Claude Code as a pair programmer.

## Run it

```bash
python -m venv .venv && .venv/bin/pip install -r requirements-dev.txt
.venv/bin/pytest -q                  # 18 tests
.venv/bin/python -m connector_kit.demo
```

The demo backfills 250 contacts through a 429 and a 503, then runs an incremental sync with a new
field, two edits in the same second and a delete, then loses the source halfway through a sync and
resumes. It ends with exactly the expected 450 rows:

```
== 1. Backfill 250 contacts (one 429 and one 503 along the way)
   http: 6 requests, 1 retries, 1 rate-limited, 2 token refreshes, waited 3.0s
== 3. Source outage mid-sync -> Gave up after 6 attempts: HTTP 503. Checkpoint kept: {...}
== 4. Resume after the outage (no duplicates, no gaps)
Total rows: 450 (expected 450), deleted: 1, columns: [..., 'company__name', 'company__size', ..., 'title']
```

## What it handles

| Problem | How |
|---|---|
| **Tokens expire** | OAuth client-credentials; on a 401 the client refreshes once and retries. Bad credentials fail fast with a clear message. |
| **Rate limits** | A 429 waits for the source's `Retry-After` (capped), then retries; it doesn't count as a failure. Endless rate-limiting gives up cleanly instead of looping. |
| **Flaky source** | 5xx and timeouts back off exponentially up to a limit. Other 4xx errors are not retried, since that only burns quota. |
| **Incremental syncs** | A checkpoint stores the last `updated_at` **and the ids already stored at that timestamp**. See below. |
| **Crash mid-sync** | Each page's rows and the checkpoint are written in **one transaction**. A resumed sync starts exactly where the last committed page ended: no duplicates, no gaps. |
| **Schema drift** | New fields become new columns (`ALTER TABLE ADD COLUMN`) instead of failing. Names are sanitised (`Lead Score (%)` → `lead_score`). |
| **Nested data** | Objects flatten (`company.hq.city` → `company__hq__city`); lists are stored as JSON. |
| **Deletes** | Soft deletes with `_deleted = 1`, plus `_synced_at` and `_source_updated_at` metadata. |
| **Replays** | Upserts by primary key, so re-writing a page (e.g. after a crash between write and acknowledgement) changes nothing. |

### The incremental-sync trap

Most APIs filter `updated_since` inclusively, and timestamps are only second-precise. The obvious
checkpoint, `(last updated_at, last id)`, has a hole: if record `c_0001` is edited **after** the
sync, in the same second as the last stored record `c_0003`, then `(T, c_0001) < (T, c_0003)` and
it is skipped forever. My first version had exactly that bug; a test caught it.

The fix stores the set of ids already written at the last timestamp, so a late edit in the same
second is picked up, and a sync with nothing new reads nothing. One edge remains (the *same*
record edited twice within that one second, after being read), which `lookback_seconds` covers
by re-reading a small window; the upsert makes the re-read harmless.

## Layout

```
connector_kit/
  http.py         OAuth, retries, Retry-After, backoff, request stats
  source.py       incremental ContactsStream with the checkpoint logic above
  destination.py  SQLite upserts, flattening, schema evolution, page + checkpoint in one transaction
  sync.py         runs a stream into a destination, checkpointing after every page
  mock_api.py     the fake CRM: cursor pages, expiring tokens, injectable 429/5xx/401/400, deletes
  demo.py
```

## Not done (yet)

- One stream and one destination. Next: a stream base class so a new connector is mostly a config
  (endpoint, cursor field, pagination style), plus offset and link-header pagination.
- SQLite stands in for a warehouse. A real loader would stage files and bulk-merge (e.g. Snowflake
  `MERGE`), batching many pages per transaction.
- No parallelism across streams, and rate limits are handled reactively; a token-bucket would
  respect a known quota proactively.
