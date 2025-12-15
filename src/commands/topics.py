from __future__ import annotations

from src.config import Config
from src.db import Database
from src.util.time import now_utc, to_iso_utc


def handle_set_topic_title(*, db: Database, config: Config, args: str) -> str:
    parts = args.strip().split(maxsplit=1)
    if len(parts) != 2:
        return "Usage: /set_topic_title <thread_id> <title>"

    thread_id_raw, title = parts[0], parts[1].strip()
    if not title:
        return "Usage: /set_topic_title <thread_id> <title>"

    try:
        thread_id = int(thread_id_raw)
    except ValueError:
        return f"Invalid thread_id: {thread_id_raw!r}"

    now_iso = to_iso_utc(now_utc())
    db.upsert_topic(
        chat_id=config.source_chat_id,
        thread_id=thread_id,
        title=title,
        now_utc_iso=now_iso,
    )
    return f"Set topic title for thread {thread_id}: {title}"


def handle_backfill_topics(*, db: Database, config: Config, args: str) -> str:
    # Best-effort scan of recent stored updates (limited).
    now_iso = to_iso_utc(now_utc())
    updated = db.backfill_topic_titles_from_raw_json(
        chat_id=config.source_chat_id,
        thread_ids=None,
        limit=5000,
        now_utc_iso=now_iso,
    )
    return f"Backfilled {updated} topic title(s)."

