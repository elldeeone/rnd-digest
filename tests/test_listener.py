from __future__ import annotations

from src.config import Config
from src.db import Database
from src.ingest.listener import ingest_update


def test_listener_ingests_message_and_topic() -> None:
    db = Database(":memory:")
    db.init_schema()

    config = Config(
        telegram_bot_token="TEST",
        source_chat_id=-1001,
        control_chat_ids={-2002},
    )

    update_topic = {
        "update_id": 1,
        "message": {
            "message_id": 10,
            "date": 1735689600,
            "message_thread_id": 123,
            "chat": {"id": -1001},
            "forum_topic_created": {"name": "Build"},
        },
    }
    ingest_update(db=db, config=config, update=update_topic)

    topic_row = db.conn.execute(
        "SELECT title FROM topics WHERE chat_id = ? AND thread_id = ?;",
        (-1001, 123),
    ).fetchone()
    assert topic_row is not None
    assert topic_row["title"] == "Build"

    update_message = {
        "update_id": 2,
        "message": {
            "message_id": 11,
            "date": 1735689660,
            "message_thread_id": 123,
            "chat": {"id": -1001},
            "from": {"id": 7, "username": "alice", "first_name": "Alice"},
            "text": "hello",
        },
    }
    ingest_update(db=db, config=config, update=update_message)

    msg_row = db.conn.execute(
        "SELECT text, from_username FROM messages WHERE chat_id = ? AND message_id = ?;",
        (-1001, 11),
    ).fetchone()
    assert msg_row is not None
    assert msg_row["text"] == "hello"
    assert msg_row["from_username"] == "alice"

