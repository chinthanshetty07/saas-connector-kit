"""Run one sync of a stream into a destination, checkpointing after every page."""
from __future__ import annotations

from dataclasses import asdict, dataclass, field


@dataclass
class SyncResult:
    stream: str
    pages: int = 0
    records: int = 0
    upserted: int = 0
    deleted: int = 0
    new_columns: list[str] = field(default_factory=list)
    http: dict = field(default_factory=dict)
    state: dict | None = None

    def to_dict(self) -> dict:
        return asdict(self)


def run_sync(stream, destination) -> SyncResult:
    result = SyncResult(stream.name)
    columns_before = len(destination.new_columns)
    state = destination.get_state(stream.name)
    for page in stream.read(state):
        counts = destination.write_page(stream.name, stream.primary_key, page.records, page.state)
        result.pages += 1
        result.records += len(page.records)
        result.upserted += counts["upserted"]
        result.deleted += counts["deleted"]
        result.state = page.state
    result.new_columns = destination.new_columns[columns_before:]
    result.http = asdict(stream.client.stats)
    result.state = destination.get_state(stream.name)
    return result
