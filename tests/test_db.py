from __future__ import annotations

import pytest

from src.db import Database


def test_search_fts5_best_effort() -> None:
    db = Database(":memory:")
    db.init_schema()

    db.upsert_message(
        {
            "chat_id": 1,
            "message_id": 1,
            "thread_id": None,
            "date_utc": "2025-01-01T00:00:00+00:00",
            "from_id": 1,
            "from_username": "alice",
            "from_display": "Alice",
            "text": "hello world",
            "raw_json": "{}",
            "reply_to_message_id": None,
            "is_service": 0,
            "edit_date_utc": None,
            "ingested_at_utc": "2025-01-01T00:00:00+00:00",
        }
    )

    try:
        hits = db.search_messages(chat_id=1, query="hello", limit=10)
    except RuntimeError:
        pytest.skip("FTS5 not available in this SQLite build")

    assert hits
    assert hits[0].message_id == 1

