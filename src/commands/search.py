from __future__ import annotations

from src.config import Config
from src.db import Database
from src.util.telegram_links import build_message_link


def handle_search(*, db: Database, config: Config, args: str) -> str:
    query = args.strip()
    if not query:
        return "Usage: /search <terms>"

    try:
        hits = db.search_messages(chat_id=config.source_chat_id, query=query, limit=10)
    except RuntimeError as exc:
        return f"Search unavailable: {exc}"

    if not hits:
        return f"No matches for: {query!r}"

    lines: list[str] = [f"Search: {query!r}"]
    for hit in hits:
        author = hit.from_display or hit.from_username or "?"
        snippet = hit.snippet or (hit.text or "")
        snippet = snippet.replace("\n", " ").strip()
        if len(snippet) > 200:
            snippet = snippet[:197] + "..."
        lines.append(f"- [{hit.date_utc}] {author}: {snippet}")
        link = build_message_link(
            chat_id=config.source_chat_id,
            message_id=hit.message_id,
            thread_id=hit.thread_id,
            username=config.source_chat_username,
        )
        if link:
            lines.append(f"  {link}")
    return "\n".join(lines)
