from __future__ import annotations

import json
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


def test_backfill_topic_titles_from_raw_json() -> None:
    db = Database(":memory:")
    db.init_schema()

    update = {
        "update_id": 1,
        "message": {
            "message_id": 100,
            "date": 1735689660,
            "message_thread_id": 7562,
            "chat": {"id": -1001},
            "from": {"id": 7, "username": "alice", "first_name": "Alice"},
            "text": "replying",
            "reply_to_message": {
                "message_id": 7562,
                "date": 1735689600,
                "message_thread_id": 7562,
                "chat": {"id": -1001},
                "forum_topic_created": {"name": "Covenants++", "icon_color": 16478047},
            },
        },
    }

    db.upsert_message(
        {
            "chat_id": -1001,
            "message_id": 100,
            "thread_id": 7562,
            "date_utc": "2025-01-01T00:01:00+00:00",
            "from_id": 7,
            "from_username": "alice",
            "from_display": "Alice",
            "text": "replying",
            "raw_json": json.dumps(update),
            "reply_to_message_id": 7562,
            "is_service": 0,
            "edit_date_utc": None,
            "ingested_at_utc": "2025-01-01T00:01:00+00:00",
        }
    )

    updated = db.backfill_topic_titles_from_raw_json(
        chat_id=-1001,
        thread_ids=[7562],
        limit=50,
        now_utc_iso="2025-01-01T00:02:00+00:00",
    )
    assert updated == 1

    row = db.conn.execute(
        "SELECT title FROM topics WHERE chat_id = ? AND thread_id = ?;",
        (-1001, 7562),
    ).fetchone()
    assert row is not None
    assert row["title"] == "Covenants++"
