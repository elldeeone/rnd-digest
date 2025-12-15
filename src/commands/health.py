from __future__ import annotations

from src.config import Config
from src.db import Database


def handle_health(*, db: Database, config: Config) -> str:
    source_count = db.get_message_count(chat_id=config.source_chat_id)
    source_last = db.get_last_ingested_message_time(chat_id=config.source_chat_id)
    last_import = db.get_state("last_import_at_utc")
    last_digest_end = db.get_state("last_digest_end_utc")
    last_ingest = db.get_state("last_ingest_at_utc")
    offset = db.get_state("telegram_update_offset")

    return (
        "Health\n"
        f"- db_path: {db.db_path}\n"
        f"- source_chat_id: {config.source_chat_id}\n"
        f"- source_messages: {source_count}\n"
        f"- source_last_date_utc: {source_last}\n"
        f"- last_import_at_utc: {last_import}\n"
        f"- last_digest_end_utc: {last_digest_end}\n"
        f"- last_ingest_at_utc: {last_ingest}\n"
        f"- telegram_update_offset: {offset}\n"
    )

