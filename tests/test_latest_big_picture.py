from __future__ import annotations

from src.commands.latest import build_latest_brief
from src.config import Config
from src.db import Database


def _insert_message(
    db: Database,
    *,
    chat_id: int,
    message_id: int,
    thread_id: int | None,
    date_utc: str,
    text: str,
) -> None:
    db.upsert_message(
        {
            "chat_id": chat_id,
            "message_id": message_id,
            "thread_id": thread_id,
            "date_utc": date_utc,
            "from_id": 1,
            "from_username": "alice",
            "from_display": "Alice",
            "text": text,
            "raw_json": "{}",
            "reply_to_message_id": None,
            "is_service": 0,
            "edit_date_utc": None,
            "ingested_at_utc": date_utc,
        }
    )


def test_latest_brief_includes_big_picture_without_llm() -> None:
    db = Database(":memory:")
    db.init_schema()

    config = Config(telegram_bot_token="t", source_chat_id=-1001, control_chat_ids={123})

    db.upsert_topic(
        chat_id=config.source_chat_id,
        thread_id=101,
        title="Rust stratum bridge",
        now_utc_iso="2025-01-01T00:00:00+00:00",
    )
    db.upsert_topic(
        chat_id=config.source_chat_id,
        thread_id=202,
        title="Long-term post-quantum discussion",
        now_utc_iso="2025-01-01T00:00:00+00:00",
    )
    db.upsert_topic(
        chat_id=config.source_chat_id,
        thread_id=303,
        title="PoW Attestations",
        now_utc_iso="2025-01-01T00:00:00+00:00",
    )

    _insert_message(
        db,
        chat_id=config.source_chat_id,
        message_id=10,
        thread_id=101,
        date_utc="2025-01-01T01:00:00+00:00",
        text="PR landed for a Rust stratum bridge; miners/pools integration notes.",
    )
    _insert_message(
        db,
        chat_id=config.source_chat_id,
        message_id=11,
        thread_id=202,
        date_utc="2025-01-01T01:10:00+00:00",
        text="Post-quantum signatures: Falcon vs SLH-DSA; NIST/FIPS links.",
    )
    _insert_message(
        db,
        chat_id=config.source_chat_id,
        message_id=12,
        thread_id=303,
        date_utc="2025-01-01T01:20:00+00:00",
        text="Attestation idea: coinbase-spend voting; coordination concerns.",
    )

    out = build_latest_brief(
        db=db,
        config=config,
        window_label="last 24h",
        window_start_utc="2025-01-01T00:00:00+00:00",
        window_end_utc="2025-01-02T00:00:00+00:00",
    )

    assert "Big picture" in out
    assert "Mining infrastructure" in out
    assert "https://t.me/" in out
