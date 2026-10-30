import pytest

from connector_kit.destination import SQLiteDestination
from connector_kit.http import ApiClient
from connector_kit.mock_api import BASE, MockCRM
from connector_kit.source import ContactsStream


class Harness:
    def __init__(self, ttl=1000):
        self.crm = MockCRM(token_ttl_requests=ttl)
        self.dest = SQLiteDestination()
        self.sleeps: list[float] = []

    def client(self, **kw):
        return ApiClient(BASE, "demo-client", "demo-secret", transport=self.crm.transport(), sleep=self.sleeps.append, **kw)

    def stream(self, page_size=50, **kw):
        return ContactsStream(self.client(**kw), page_size=page_size)

    def ids(self):
        return [r["id"] for r in self.dest.rows("contacts")]


@pytest.fixture
def h():
    return Harness()
