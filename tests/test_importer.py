from __future__ import annotations

import json
from pathlib import Path

from src.db import Database
from src.ingest.importer import import_export_json


def test_importer_inserts_and_normalizes(tmp_path: Path) -> None:
    payload = json.loads((Path("tests/fixtures/export_sample.json")).read_text(encoding="utf-8"))
    db = Database(str(tmp_path / "test.db"))
    db.init_schema()

    inserted, skipped = import_export_json(
        db=db,
        chat_id=-100123,
        payload=payload,
        ingested_at_utc="2025-01-01T00:00:00+00:00",
        export_chat_name=None,
    )

    assert inserted == 4
    assert skipped == 0

    row = db.conn.execute(
        "SELECT text, reply_to_message_id, thread_id FROM messages WHERE chat_id = ? AND message_id = ?;",
        (-100123, 12),
    ).fetchone()
    assert row is not None
    assert row["reply_to_message_id"] == 11
    assert row["thread_id"] == 10
    assert "hi there" in row["text"]
    assert "https://example.com/path" in row["text"]

    general = db.conn.execute(
        "SELECT thread_id FROM messages WHERE chat_id = ? AND message_id = ?;",
        (-100123, 13),
    ).fetchone()
    assert general is not None
    assert general["thread_id"] == 1

    topic = db.conn.execute(
        "SELECT title FROM topics WHERE chat_id = ? AND thread_id = ?;",
        (-100123, 10),
    ).fetchone()
    assert topic is not None
    assert topic["title"] == "Topic A"
