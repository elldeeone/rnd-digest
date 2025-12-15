from __future__ import annotations

from datetime import timedelta
import re

from src.config import Config
from src.db import Database
from src.util.telegram_links import build_message_link
from src.util.time import now_utc, parse_duration, to_iso_utc


_URL_RE = re.compile(r"https?://\S+")


def _excerpt(text: str, *, max_chars: int) -> str:
    text = re.sub(r"\s+", " ", text).strip()
    if len(text) <= max_chars:
        return text
    return text[: max_chars - 1].rstrip() + "…"


def _format_topic_label(*, title: str | None, thread_id: int | None) -> str:
    if title:
        return title
    if thread_id is None:
        return "No topic"
    return f"Thread {thread_id}"


def handle_topic(*, db: Database, config: Config, args: str) -> str:
    parts = args.strip().split()
    if not parts:
        return "Usage: /topic <thread_id> [6h|2d|1w]"

    thread_raw = parts[0].strip().lower()
    if thread_raw in {"none", "no_topic", "no-topic"}:
        thread_id: int | None = None
    else:
        try:
            thread_id = int(parts[0])
        except ValueError:
            return "Usage: /topic <thread_id> [6h|2d|1w]"

    if len(parts) >= 2:
        try:
            duration = parse_duration(parts[1])
        except ValueError:
            return "Usage: /topic <thread_id> [6h|2d|1w]"
    else:
        duration = timedelta(hours=config.latest_default_window_hours)

    end_dt = now_utc()
    start_dt = end_dt - duration
    window_start = to_iso_utc(start_dt)
    window_end = to_iso_utc(end_dt)

    title = None
    if thread_id is not None:
        title = db.get_topic_titles(chat_id=config.source_chat_id, thread_ids=[thread_id]).get(thread_id)
    label = _format_topic_label(title=title, thread_id=thread_id)

    rollup = db.get_topic_rollups(chat_id=config.source_chat_id, thread_ids=[thread_id]).get(thread_id)

    msgs = db.get_last_messages_for_topic_in_window(
        chat_id=config.source_chat_id,
        thread_id=thread_id,
        window_start_utc=window_start,
        window_end_utc=window_end,
        limit=60,
    )
    if not msgs:
        return f"No messages for topic in window (UTC: {window_start} → {window_end})."

    links: dict[str, bool] = {}
    for msg in msgs:
        text = msg["text"] or ""
        for url in _URL_RE.findall(text):
            if url not in links:
                links[url] = True
            if len(links) >= 10:
                break
        if len(links) >= 10:
            break

    recent = msgs[-10:]

    lines: list[str] = []
    lines.append(f"Topic: {label}")
    lines.append(f"Window (UTC): {window_start} → {window_end}")
    lines.append(f"Messages: {len(msgs)} (showing last {len(recent)})")

    if rollup and rollup.summary.strip():
        lines.append("")
        lines.append("Rollup")
        lines.append(
            f"- updated_at_utc: {rollup.updated_at_utc}"
            + (f", last_message_id: {rollup.last_message_id}" if rollup.last_message_id else "")
        )
        lines.append(rollup.summary.strip())
    else:
        lines.append("")
        lines.append("Rollup")
        lines.append("- (none yet) Run: /rollup <thread_id> rebuild")

    if links:
        lines.append("")
        lines.append("Links")
        for url in links.keys():
            lines.append(f"- {url}")

    lines.append("")
    lines.append("Recent")
    for msg in recent:
        author = msg["from_display"] or msg["from_username"] or "?"
        text = _excerpt(msg["text"] or "", max_chars=260)
        link = build_message_link(
            chat_id=config.source_chat_id,
            message_id=int(msg["message_id"]),
            thread_id=thread_id,
            username=config.source_chat_username,
        )
        line = f'- [{msg["date_utc"]}] {author}: {text}'
        if link:
            line += f" — {link}"
        lines.append(line)

    return "\n".join(lines)

