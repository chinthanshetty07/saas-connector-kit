import pytest

from connector_kit.destination import SQLiteDestination, flatten, sanitize
from connector_kit.http import ConnectorError
from connector_kit.source import ContactsStream
from connector_kit.sync import run_sync
from conftest import Harness


# --- extraction ------------------------------------------------------------------------------

def test_backfill_reads_every_page(h):
    h.crm.seed(237)
    r = run_sync(h.stream(page_size=50), h.dest)
    assert (r.pages, r.records) == (5, 237)
    assert len(set(h.ids())) == 237


def test_incremental_sync_only_reads_changes(h):
    h.crm.seed(120)
    run_sync(h.stream(), h.dest)
    h.crm.upsert("c_0007", name="Renamed")
    h.crm.upsert("c_9999", email="new@example.test", name="New")
    r = run_sync(h.stream(), h.dest)
    assert r.records == 2
    assert {row["id"]: row["name"] for row in h.dest.rows("contacts")}["c_0007"] == "Renamed"


def test_records_sharing_the_checkpoint_timestamp_are_not_skipped(h):
    h.crm.seed(10)
    run_sync(h.stream(), h.dest)
    # c_0003 changes at exactly the timestamp of the last stored record (c_0010).
    h.crm.upsert("c_0003", same_timestamp=True, name="Same Second")
    r = run_sync(h.stream(), h.dest)
    assert r.records == 1
    assert {row["id"]: row["name"] for row in h.dest.rows("contacts")}["c_0003"] == "Same Second"


def test_a_lower_id_edited_in_the_same_second_is_not_lost(h):
    # The trap a plain (updated_at, id) cursor falls into.
    h.crm.seed(3)
    run_sync(h.stream(), h.dest)          # checkpoint: c_0003 at T
    h.crm.upsert("c_0001", same_timestamp=True, name="Late edit in the same second")
    assert run_sync(h.stream(), h.dest).records == 1
    assert run_sync(h.stream(), h.dest).records == 0  # and it is not re-read forever


def test_lookback_rereads_a_window_harmlessly(h):
    h.crm.seed(20)
    run_sync(h.stream(), h.dest)
    r = run_sync(ContactsStream(h.client(), lookback_seconds=3), h.dest)
    assert r.records >= 3 and len(h.ids()) == 20


def test_a_sync_with_nothing_new_reads_nothing_and_keeps_state(h):
    h.crm.seed(30)
    first = run_sync(h.stream(), h.dest)
    again = run_sync(h.stream(), h.dest)
    assert again.records == 0 and again.state == first.state


# --- resilience ------------------------------------------------------------------------------

def test_rate_limits_wait_for_retry_after(h):
    h.crm.seed(60)
    h.crm.inject("429", "ok", "429")
    stream = h.stream()
    r = run_sync(stream, h.dest)
    assert r.records == 60
    assert h.sleeps == [2.0, 2.0]  # the Retry-After header, not our own backoff
    assert stream.client.stats.rate_limited == 2


def test_retry_after_is_capped(h):
    h.crm.seed(5)
    h.crm.inject("429")
    run_sync(h.stream(max_retry_after_s=1.0), h.dest)
    assert h.sleeps == [1.0]


def test_endless_rate_limiting_gives_up_cleanly(h):
    h.crm.seed(5)
    h.crm.inject(*["429"] * 5)
    with pytest.raises(ConnectorError, match="rate-limited"):
        run_sync(h.stream(max_rate_limit_waits=3), h.dest)


def test_server_errors_back_off_exponentially_then_succeed(h):
    h.crm.seed(5)
    h.crm.inject("500", "503")
    run_sync(h.stream(backoff_s=0.5), h.dest)
    assert h.sleeps == [0.5, 1.0]


def test_client_errors_are_not_retried(h):
    h.crm.seed(5)
    h.crm.inject("400")
    with pytest.raises(ConnectorError) as err:
        run_sync(h.stream(), h.dest)
    assert err.value.status == 400 and not err.value.retryable and h.sleeps == []


def test_expired_tokens_are_refreshed_mid_sync():
    h = Harness(ttl=2)
    h.crm.seed(200)
    stream = h.stream(page_size=40)
    r = run_sync(stream, h.dest)
    assert r.records == 200
    assert stream.client.stats.token_refreshes >= 3


def test_bad_credentials_fail_fast(h):
    from connector_kit.http import ApiClient
    from connector_kit.mock_api import BASE
    from connector_kit.source import ContactsStream
    client = ApiClient(BASE, "demo-client", "wrong", transport=h.crm.transport(), sleep=h.sleeps.append)
    with pytest.raises(ConnectorError, match="Authentication failed"):
        run_sync(ContactsStream(client), h.dest)


def test_a_crash_mid_sync_resumes_without_duplicates_or_gaps(h):
    h.crm.seed(300)
    h.crm.inject("ok", "ok", *["503"] * 4)  # two pages land, then the source dies (1 try + 3 retries)
    with pytest.raises(ConnectorError):
        run_sync(h.stream(page_size=50, max_retries=3), h.dest)
    assert len(h.ids()) == 100
    assert h.dest.get_state("contacts")["ids_at_mark"] == ["c_0100"]

    r = run_sync(h.stream(page_size=50), h.dest)
    assert r.records == 200  # only what was missing
    assert h.ids() == [f"c_{i:04d}" for i in range(1, 301)]


def test_rows_and_checkpoint_commit_together():
    dest = SQLiteDestination()
    dest.write_page("contacts", "id", [{"id": "a", "updated_at": "t1"}], {"updated_at": "t1", "id": "a"})
    with pytest.raises(Exception):
        # a bad value inside the transaction rolls back both the row and the checkpoint
        dest.write_page("contacts", "id", [{"id": "b", "updated_at": "t2"}, {"id": None, "updated_at": object()}],
                        {"updated_at": "t2", "id": "b"})
    assert [r["id"] for r in dest.rows("contacts")] == ["a"]
    assert dest.get_state("contacts") == {"updated_at": "t1", "id": "a"}


# --- loading ---------------------------------------------------------------------------------

def test_new_fields_become_new_columns(h):
    h.crm.seed(5)
    run_sync(h.stream(), h.dest)
    h.crm.upsert("c_0002", title="CTO", **{"Lead Score": 87})
    r = run_sync(h.stream(), h.dest)
    assert set(r.new_columns) == {"contacts.title", "contacts.lead_score"}
    row = {x["id"]: x for x in h.dest.rows("contacts")}
    assert row["c_0002"]["title"] == "CTO" and row["c_0001"]["title"] is None


def test_deletes_are_soft_and_replays_are_idempotent(h):
    h.crm.seed(5)
    run_sync(h.stream(), h.dest)
    h.crm.delete("c_0004")
    r = run_sync(h.stream(), h.dest)
    assert r.deleted == 1
    rows = {x["id"]: x for x in h.dest.rows("contacts")}
    assert rows["c_0004"]["_deleted"] == 1 and len(rows) == 5
    # Replaying the same page (as after a crash between write and ack) changes nothing.
    page = [dict(h.crm.records["c_0004"])]
    h.dest.write_page("contacts", "id", page, h.dest.get_state("contacts"))
    assert len(h.dest.rows("contacts")) == 5


def test_flattening_and_name_sanitising():
    assert flatten({"id": "1", "company": {"name": "X", "hq": {"city": "BLR"}}, "tags": ["b", "a"], "vip": True}) == \
        {"id": "1", "company__name": "X", "company__hq__city": "BLR", "tags": '["b", "a"]', "vip": 1}  # list order is data
    assert sanitize("Lead Score (%)") == "lead_score" and sanitize("2fa enabled") == "c_2fa_enabled"
