from __future__ import annotations

from collections import OrderedDict
from datetime import datetime
import re
from zoneinfo import ZoneInfo

from src.config import Config
from src.db import Database
from src.util.time import now_utc, to_iso_utc


_URL_RE = re.compile(r"https?://\S+")


def _topic_label(*, title: str | None, thread_id: int | None) -> str:
    if title:
        return title
    if thread_id is None:
        return "No topic"
    return f"Thread {thread_id}"


def build_extractive_digest(
    *,
    db: Database,
    config: Config,
    window_start_utc: str,
    window_end_utc: str,
) -> str:
    tz = ZoneInfo(config.tz)
    local_day = datetime.now(tz=tz).date().isoformat()

    activity = db.get_topic_activity(
        chat_id=config.source_chat_id,
        window_start_utc=window_start_utc,
        window_end_utc=window_end_utc,
        limit=config.digest_max_topics,
    )
    thread_ids = [int(row["thread_id"]) for row in activity if row["thread_id"] is not None]
    titles = db.get_topic_titles(chat_id=config.source_chat_id, thread_ids=thread_ids)
    missing = [tid for tid in thread_ids if tid not in titles]
    if missing:
        db.backfill_topic_titles_from_raw_json(
            chat_id=config.source_chat_id,
            thread_ids=missing,
            limit=2000,
            now_utc_iso=to_iso_utc(now_utc()),
        )
        titles = db.get_topic_titles(chat_id=config.source_chat_id, thread_ids=thread_ids)

    lines: list[str] = []
    lines.append(f"Daily Digest — {local_day} ({config.tz})")
    lines.append(f"Window (UTC): {window_start_utc} → {window_end_utc}")

    if not activity:
        lines.append("")
        lines.append("No messages in window.")
        return "\\n".join(lines)

    lines.append("")
    lines.append("Top threads")
    for row in activity:
        thread_id = row["thread_id"]
        title = titles.get(int(thread_id)) if thread_id is not None else None
        label = _topic_label(title=title, thread_id=int(thread_id) if thread_id is not None else None)
        lines.append(f"- {label} ({int(row['message_count'])} msgs)")

    lines.append("")
    lines.append("By topic")

    for row in activity:
        thread_id = row["thread_id"]
        title = titles.get(int(thread_id)) if thread_id is not None else None
        label = _topic_label(title=title, thread_id=int(thread_id) if thread_id is not None else None)
        count = int(row["message_count"])

        msgs = db.get_messages_for_topic(
            chat_id=config.source_chat_id,
            thread_id=int(thread_id) if thread_id is not None else None,
            window_start_utc=window_start_utc,
            window_end_utc=window_end_utc,
            limit=config.digest_max_messages_per_topic,
        )

        # Links
        links = OrderedDict()
        for msg in msgs:
            text = msg["text"]
            if not text:
                continue
            for url in _URL_RE.findall(text):
                if url not in links:
                    links[url] = True
                if len(links) >= 8:
                    break
            if len(links) >= 8:
                break

        # Quotes: last N non-empty messages
        quotes = []
        for msg in reversed(msgs):
            text = (msg["text"] or "").strip()
            if not text:
                continue
            author = msg["from_display"] or msg["from_username"] or "?"
            text_one_line = text.replace("\\n", " ")
            if len(text_one_line) > 240:
                text_one_line = text_one_line[:237] + "..."
            quotes.append(f'- [{msg["date_utc"]}] {author}: {text_one_line}')
            if len(quotes) >= config.digest_max_quotes_per_topic:
                break
        quotes.reverse()

        lines.append("")
        lines.append(f"Topic: {label} ({count} msgs)")
        if links:
            lines.append("Links:")
            for url in links.keys():
                lines.append(f"- {url}")
        if quotes:
            lines.append("Quotes:")
            lines.extend(quotes)

    return "\\n".join(lines)
