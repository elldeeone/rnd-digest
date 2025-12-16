from __future__ import annotations

import re

from src.config import Config
from src.db import Database
from src.util.telegram_links import build_message_link


_URL_RE = re.compile(r"https?://\S+", flags=re.IGNORECASE)
_WS_RE = re.compile(r"\s+")


def _one_line(text: str) -> str:
    return _WS_RE.sub(" ", text).strip()


def _excerpt(text: str, *, max_chars: int) -> str:
    text = _one_line(text)
    if len(text) <= max_chars:
        return text
    return text[: max_chars - 1].rstrip() + "…"


def _format_topic_label(*, title: str | None, thread_id: int | None) -> str:
    if title:
        return title
    if thread_id is None:
        return "No topic"
    return f"Thread {thread_id}"


def build_topic_links(
    *,
    db: Database,
    config: Config,
    thread_id: int | None,
    window_start_utc: str,
    window_end_utc: str,
) -> str:
    title = (
        db.get_topic_titles(chat_id=config.source_chat_id, thread_ids=[thread_id]).get(thread_id)
        if thread_id is not None
        else None
    )
    label = _format_topic_label(title=title, thread_id=thread_id)

    msgs = db.get_messages_for_topic(
        chat_id=config.source_chat_id,
        thread_id=thread_id,
        window_start_utc=window_start_utc,
        window_end_utc=window_end_utc,
        limit=max(150, config.digest_max_messages_per_topic),
    )
    if not msgs:
        return f"No messages for topic in window (UTC: {window_start_utc} → {window_end_utc})."

    links: list[str] = []
    seen: set[str] = set()
    for msg in msgs:
        text = msg["text"] or ""
        for url in _URL_RE.findall(text):
            if url in seen:
                continue
            seen.add(url)
            links.append(url)
            if len(links) >= 25:
                break
        if len(links) >= 25:
            break

    lines: list[str] = []
    lines.append(f"Links: {label} (id={thread_id if thread_id is not None else 'none'})")
    lines.append(f"Window (UTC): {window_start_utc} → {window_end_utc}")
    if not links:
        lines.append("")
        lines.append("No links found.")
        return "\n".join(lines)

    lines.append("")
    for url in links:
        lines.append(f"- {url}")
    return "\n".join(lines)


def build_topic_receipts(
    *,
    db: Database,
    config: Config,
    thread_id: int | None,
    window_start_utc: str,
    window_end_utc: str,
) -> str:
    title = (
        db.get_topic_titles(chat_id=config.source_chat_id, thread_ids=[thread_id]).get(thread_id)
        if thread_id is not None
        else None
    )
    label = _format_topic_label(title=title, thread_id=thread_id)

    msgs = db.get_messages_for_topic(
        chat_id=config.source_chat_id,
        thread_id=thread_id,
        window_start_utc=window_start_utc,
        window_end_utc=window_end_utc,
        limit=max(200, config.digest_max_messages_per_topic),
    )
    if not msgs:
        return f"No messages for topic in window (UTC: {window_start_utc} → {window_end_utc})."

    links: list[str] = []
    seen_links: set[str] = set()
    for msg in msgs:
        text = msg["text"] or ""
        for url in _URL_RE.findall(text):
            if url in seen_links:
                continue
            seen_links.add(url)
            links.append(url)
            if len(links) >= 8:
                break
        if len(links) >= 8:
            break

    # Prefer last N non-empty messages as receipts.
    max_quote_chars = min(config.digest_quote_max_chars, 220)
    quotes: list[str] = []
    for msg in reversed(msgs):
        text = (msg["text"] or "").strip()
        if not text:
            continue
        author = msg["from_display"] or msg["from_username"] or "?"
        link = build_message_link(
            chat_id=config.source_chat_id,
            message_id=int(msg["message_id"]),
            thread_id=thread_id,
            username=config.source_chat_username,
        )
        suffix = f" — {link}" if link else ""
        quotes.append(
            f"- [{msg['date_utc']}] {author}: {_excerpt(text, max_chars=max_quote_chars)}{suffix}"
        )
        if len(quotes) >= 6:
            break
    quotes.reverse()

    lines: list[str] = []
    lines.append(f"Receipts: {label} (id={thread_id if thread_id is not None else 'none'})")
    lines.append(f"Window (UTC): {window_start_utc} → {window_end_utc}")

    if links:
        lines.append("")
        lines.append("Links")
        lines.extend([f"- {u}" for u in links])

    lines.append("")
    lines.append("Quotes")
    lines.extend(quotes)
    return "\n".join(lines)
