from __future__ import annotations

from dataclasses import dataclass
import os
from typing import Iterable


def _parse_int(value: str, *, name: str) -> int:
    try:
        return int(value)
    except ValueError as exc:
        raise ValueError(f"Invalid integer for {name}: {value!r}") from exc


def _parse_csv_ints(value: str) -> set[int]:
    items = [part.strip() for part in value.split(",")]
    return {int(item) for item in items if item}


def _first_non_empty(values: Iterable[str | None]) -> str | None:
    for value in values:
        if value is not None and value.strip():
            return value.strip()
    return None


@dataclass(frozen=True)
class Config:
    telegram_bot_token: str
    source_chat_id: int
    control_chat_ids: set[int]

    db_path: str = "./data/kaspa.db"

    tz: str = "Australia/Sydney"
    daily_digest_time: str = "09:00"

    latest_default_window_hours: int = 24
    poll_timeout_seconds: int = 30

    control_digest_thread_id: int | None = None
    digest_max_topics: int = 12
    digest_max_quotes_per_topic: int = 3
    digest_max_messages_per_topic: int = 80

    @staticmethod
    def from_env() -> "Config":
        token = _first_non_empty([os.getenv("TELEGRAM_BOT_TOKEN")])
        if not token:
            raise RuntimeError("Missing env var TELEGRAM_BOT_TOKEN")

        source_chat_id_raw = _first_non_empty([os.getenv("SOURCE_CHAT_ID")])
        if not source_chat_id_raw:
            raise RuntimeError("Missing env var SOURCE_CHAT_ID")
        source_chat_id = _parse_int(source_chat_id_raw, name="SOURCE_CHAT_ID")

        control_chat_ids_raw = _first_non_empty(
            [os.getenv("CONTROL_CHAT_IDS"), os.getenv("CONTROL_CHAT_ID")]
        )
        if not control_chat_ids_raw:
            raise RuntimeError("Missing env var CONTROL_CHAT_ID(S)")
        control_chat_ids = _parse_csv_ints(control_chat_ids_raw)
        if not control_chat_ids:
            raise RuntimeError("CONTROL_CHAT_ID(S) is empty")

        db_path = os.getenv("DB_PATH", "./data/kaspa.db")
        tz = os.getenv("TZ", "Australia/Sydney")
        daily_digest_time = os.getenv("DAILY_DIGEST_TIME", "09:00")

        latest_default_window_hours = int(os.getenv("LATEST_DEFAULT_WINDOW_HOURS", "24"))
        poll_timeout_seconds = int(os.getenv("POLL_TIMEOUT_SECONDS", "30"))

        digest_max_topics = int(os.getenv("DIGEST_MAX_TOPICS", "12"))
        digest_max_quotes_per_topic = int(os.getenv("DIGEST_MAX_QUOTES_PER_TOPIC", "3"))
        digest_max_messages_per_topic = int(os.getenv("DIGEST_MAX_MESSAGES_PER_TOPIC", "80"))

        control_digest_thread_id_raw = _first_non_empty([os.getenv("CONTROL_DIGEST_THREAD_ID")])
        control_digest_thread_id = (
            _parse_int(control_digest_thread_id_raw, name="CONTROL_DIGEST_THREAD_ID")
            if control_digest_thread_id_raw
            else None
        )

        return Config(
            telegram_bot_token=token,
            source_chat_id=source_chat_id,
            control_chat_ids=control_chat_ids,
            db_path=db_path,
            tz=tz,
            daily_digest_time=daily_digest_time,
            latest_default_window_hours=latest_default_window_hours,
            poll_timeout_seconds=poll_timeout_seconds,
            control_digest_thread_id=control_digest_thread_id,
            digest_max_topics=digest_max_topics,
            digest_max_quotes_per_topic=digest_max_quotes_per_topic,
            digest_max_messages_per_topic=digest_max_messages_per_topic,
        )
