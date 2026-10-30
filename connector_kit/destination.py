"""A small warehouse-style destination on SQLite.

- Upserts by primary key, so replaying a page after a crash never duplicates rows.
- Flattens nested objects (company.name -> company__name) and stores lists as JSON.
- Evolves the schema: a field the source starts sending gets a new column, never an error.
- Soft deletes: a deleted source record keeps its row with _deleted = 1, like most ELT tools.
- Writes a page's rows and the stream's checkpoint in ONE transaction. That is the property
  that makes resume-after-crash exact: either both landed or neither did.
"""
from __future__ import annotations

import json
import re
import sqlite3
from datetime import datetime, timezone

META = {"_deleted": "INTEGER NOT NULL DEFAULT 0", "_synced_at": "TEXT", "_source_updated_at": "TEXT"}


def sanitize(name: str) -> str:
    out = re.sub(r"[^a-z0-9_]+", "_", name.strip().lower()).strip("_") or "col"
    return f"c_{out}" if out[0].isdigit() else out


def flatten(record: dict, parent: str = "") -> dict:
    flat: dict = {}
    for key, value in record.items():
        col = sanitize(f"{parent}__{key}" if parent else key)
        if isinstance(value, dict):
            flat.update(flatten(value, col))
        elif isinstance(value, list):
            flat[col] = json.dumps(value, sort_keys=True)
        elif isinstance(value, bool):
            flat[col] = int(value)
        else:
            flat[col] = value
    return flat


class SQLiteDestination:
    def __init__(self, path: str = ":memory:") -> None:
        self.db = sqlite3.connect(path)
        self.db.execute("CREATE TABLE IF NOT EXISTS _connector_state (stream TEXT PRIMARY KEY, state TEXT NOT NULL)")
        self.new_columns: list[str] = []

    def get_state(self, stream: str) -> dict | None:
        row = self.db.execute("SELECT state FROM _connector_state WHERE stream = ?", (stream,)).fetchone()
        return json.loads(row[0]) if row else None

    def _columns(self, table: str) -> set[str]:
        return {r[1] for r in self.db.execute(f'PRAGMA table_info("{table}")')}

    def _ensure_table(self, table: str, pk: str) -> None:
        meta = ", ".join(f'"{k}" {v}' for k, v in META.items())
        self.db.execute(f'CREATE TABLE IF NOT EXISTS "{table}" ("{pk}" TEXT PRIMARY KEY, {meta})')

    def write_page(self, table: str, pk: str, records: list[dict], state: dict) -> dict:
        counts = {"upserted": 0, "deleted": 0}
        now = datetime.now(timezone.utc).isoformat()
        with self.db:  # one transaction: rows + checkpoint
            self._ensure_table(table, pk)
            existing = self._columns(table)
            for record in records:
                row = flatten({k: v for k, v in record.items() if k != "deleted"})
                row["_deleted"] = int(bool(record.get("deleted")))
                row["_synced_at"] = now
                row["_source_updated_at"] = record.get("updated_at")
                for col in row:
                    if col not in existing:
                        self.db.execute(f'ALTER TABLE "{table}" ADD COLUMN "{col}"')
                        existing.add(col)
                        self.new_columns.append(f"{table}.{col}")
                cols = list(row)
                placeholders = ", ".join("?" for _ in cols)
                updates = ", ".join(f'"{c}" = excluded."{c}"' for c in cols if c != pk)
                self.db.execute(
                    f'INSERT INTO "{table}" ({", ".join(chr(34) + c + chr(34) for c in cols)}) VALUES ({placeholders}) '
                    f'ON CONFLICT("{pk}") DO UPDATE SET {updates}',
                    [row[c] for c in cols],
                )
                counts["deleted" if row["_deleted"] else "upserted"] += 1
            self.db.execute("INSERT INTO _connector_state (stream, state) VALUES (?, ?) "
                            "ON CONFLICT(stream) DO UPDATE SET state = excluded.state", (table, json.dumps(state)))
        return counts

    def rows(self, table: str) -> list[dict]:
        cur = self.db.execute(f'SELECT * FROM "{table}" ORDER BY "id"')
        names = [d[0] for d in cur.description]
        return [dict(zip(names, r)) for r in cur.fetchall()]
