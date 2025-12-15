from __future__ import annotations

from datetime import timedelta

from src.config import Config
from src.db import Database
from src.util.time import now_utc, to_iso_utc, parse_duration


def _format_topic_label(*, title: str | None, thread_id: int | None) -> str:
    if title:
        return title
    if thread_id is None:
        return "No topic"
    return f"Thread {thread_id}"


def handle_latest(*, db: Database, config: Config, args: str) -> str:
    if args:
        duration = parse_duration(args)
    else:
        duration = timedelta(hours=config.latest_default_window_hours)

    end_dt = now_utc()
    start_dt = end_dt - duration
    window_start = to_iso_utc(start_dt)
    window_end = to_iso_utc(end_dt)

    activity = db.get_topic_activity(
        chat_id=config.source_chat_id,
        window_start_utc=window_start,
        window_end_utc=window_end,
        limit=12,
    )
    thread_ids = [int(row["thread_id"]) for row in activity if row["thread_id"] is not None]
    titles = db.get_topic_titles(chat_id=config.source_chat_id, thread_ids=thread_ids)
    missing = [tid for tid in thread_ids if tid not in titles]
    if missing:
        db.backfill_topic_titles_from_raw_json(
            chat_id=config.source_chat_id,
            thread_ids=missing,
            limit=500,
            now_utc_iso=window_end,
        )
        titles = db.get_topic_titles(chat_id=config.source_chat_id, thread_ids=thread_ids)

    lines: list[str] = []
    lines.append(f"Latest ({args or f'{config.latest_default_window_hours}h'})")
    lines.append(f"Window (UTC): {window_start} → {window_end}")

    if not activity:
        lines.append("")
        lines.append("No messages in window.")
        return "\n".join(lines)

    for row in activity:
        thread_id = row["thread_id"]
        title = titles.get(int(thread_id)) if thread_id is not None else None
        label = _format_topic_label(title=title, thread_id=int(thread_id) if thread_id is not None else None)
        count = int(row["message_count"])

        lines.append("")
        lines.append(f"- {label} ({count} msgs)")

        msgs = db.get_messages_for_topic(
            chat_id=config.source_chat_id,
            thread_id=int(thread_id) if thread_id is not None else None,
            window_start_utc=window_start,
            window_end_utc=window_end,
            limit=10,
        )
        for msg in msgs[-3:]:
            author = msg["from_display"] or msg["from_username"] or "?"
            text = (msg["text"] or "").strip().replace("\n", " ")
            if len(text) > 180:
                text = text[:177] + "..."
            lines.append(f"  • [{msg['date_utc']}] {author}: {text}")

    return "\n".join(lines)
