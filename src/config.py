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


def _parse_float(value: str, *, name: str) -> float:
    try:
        return float(value)
    except ValueError as exc:
        raise ValueError(f"Invalid float for {name}: {value!r}") from exc


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
    source_chat_username: str | None = None

    db_path: str = "./data/kaspa.db"

    tz: str = "Australia/Sydney"
    daily_digest_time: str = "09:00"

    latest_default_window_hours: int = 24
    poll_timeout_seconds: int = 30

    control_digest_thread_id: int | None = None
    digest_max_topics: int = 12
    digest_max_quotes_per_topic: int = 3
    digest_max_messages_per_topic: int = 80

    llm_provider: str = "none"
    llm_timeout_seconds: int = 60

    openrouter_api_key: str | None = None
    openrouter_model: str | None = None
    openrouter_base_url: str = "https://openrouter.ai/api/v1"
    openrouter_site_url: str | None = None
    openrouter_app_name: str | None = None

    digest_llm_max_tokens: int = 1200
    digest_llm_temperature: float = 0.2

    ask_llm_max_tokens: int = 800
    ask_llm_temperature: float = 0.1

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

        source_chat_username = _first_non_empty([os.getenv("SOURCE_CHAT_USERNAME")])

        llm_provider = os.getenv("LLM_PROVIDER", "none").strip().lower()
        llm_timeout_seconds = int(os.getenv("LLM_TIMEOUT_SECONDS", "60"))

        openrouter_api_key = _first_non_empty([os.getenv("OPENROUTER_API_KEY")])
        openrouter_model = _first_non_empty([os.getenv("OPENROUTER_MODEL")])
        openrouter_base_url = os.getenv("OPENROUTER_BASE_URL", "https://openrouter.ai/api/v1").strip()
        openrouter_site_url = _first_non_empty([os.getenv("OPENROUTER_SITE_URL")])
        openrouter_app_name = _first_non_empty([os.getenv("OPENROUTER_APP_NAME")])

        digest_llm_max_tokens = int(os.getenv("DIGEST_LLM_MAX_TOKENS", "1200"))
        digest_llm_temperature = _parse_float(
            os.getenv("DIGEST_LLM_TEMPERATURE", "0.2"), name="DIGEST_LLM_TEMPERATURE"
        )

        ask_llm_max_tokens = int(os.getenv("ASK_LLM_MAX_TOKENS", "800"))
        ask_llm_temperature = _parse_float(
            os.getenv("ASK_LLM_TEMPERATURE", "0.1"), name="ASK_LLM_TEMPERATURE"
        )

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
            source_chat_username=source_chat_username,
            db_path=db_path,
            tz=tz,
            daily_digest_time=daily_digest_time,
            latest_default_window_hours=latest_default_window_hours,
            poll_timeout_seconds=poll_timeout_seconds,
            control_digest_thread_id=control_digest_thread_id,
            digest_max_topics=digest_max_topics,
            digest_max_quotes_per_topic=digest_max_quotes_per_topic,
            digest_max_messages_per_topic=digest_max_messages_per_topic,
            llm_provider=llm_provider,
            llm_timeout_seconds=llm_timeout_seconds,
            openrouter_api_key=openrouter_api_key,
            openrouter_model=openrouter_model,
            openrouter_base_url=openrouter_base_url,
            openrouter_site_url=openrouter_site_url,
            openrouter_app_name=openrouter_app_name,
            digest_llm_max_tokens=digest_llm_max_tokens,
            digest_llm_temperature=digest_llm_temperature,
            ask_llm_max_tokens=ask_llm_max_tokens,
            ask_llm_temperature=ask_llm_temperature,
        )
