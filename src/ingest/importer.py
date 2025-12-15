from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
import logging
import os
import re
from typing import Any

from dotenv import load_dotenv

from src.db import Database
from src.util.time import now_utc, to_iso_utc
from src.util.logging import configure_logging


log = logging.getLogger(__name__)


_FROM_ID_RE = re.compile(r"^(?:user|channel|chat)(\d+)$")


def _parse_export_from_id(value: Any) -> int | None:
    if value is None:
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, str):
        value = value.strip()
        if value.isdigit():
            return int(value)
        match = _FROM_ID_RE.match(value)
        if match:
            return int(match.group(1))
    return None


def _normalize_export_text(text_field: Any) -> str | None:
    if text_field is None:
        return None
    if isinstance(text_field, str):
        return text_field
    if isinstance(text_field, list):
        parts: list[str] = []
        for item in text_field:
            if isinstance(item, str):
                parts.append(item)
                continue
            if isinstance(item, dict):
                # Telegram export fragments often look like:
                # {"type":"bold","text":"foo"} or {"type":"link","text":"...","href":"..."}
                fragment_text = item.get("text")
                if isinstance(fragment_text, str):
                    parts.append(fragment_text)
        joined = "".join(parts)
        return joined if joined else None
    return None


def _parse_export_unixtime(value: Any) -> str | None:
    if value is None:
        return None
    try:
        ts = int(value)
    except (TypeError, ValueError):
        return None
    return datetime.fromtimestamp(ts, tz=timezone.utc).replace(microsecond=0).isoformat()


def _extract_messages(payload: Any, *, export_chat_name: str | None) -> list[dict[str, Any]]:
    if isinstance(payload, dict) and isinstance(payload.get("messages"), list):
        return payload["messages"]

    chats = None
    if isinstance(payload, dict):
        chats = payload.get("chats", {}).get("list")

    if isinstance(chats, list):
        if export_chat_name:
            for chat in chats:
                if isinstance(chat, dict) and chat.get("name") == export_chat_name:
                    messages = chat.get("messages")
                    if isinstance(messages, list):
                        return messages
            raise SystemExit(f"No chat named {export_chat_name!r} found in export JSON")

        if len(chats) == 1 and isinstance(chats[0], dict) and isinstance(chats[0].get("messages"), list):
            return chats[0]["messages"]

        raise SystemExit(
            "Export JSON contains multiple chats; pass --export-chat-name to select one"
        )

    raise SystemExit("Unrecognized Telegram export JSON structure (expected 'messages' list)")


def _extract_export_topics(messages: list[dict[str, Any]]) -> dict[int, str]:
    topics: dict[int, str] = {}
    for msg in messages:
        if not isinstance(msg, dict):
            continue
        if msg.get("type") != "service":
            continue
        if msg.get("action") != "topic_created":
            continue
        message_id = msg.get("id")
        title = msg.get("title")
        if isinstance(message_id, int) and isinstance(title, str) and title.strip():
            topics[message_id] = title.strip()
    return topics


def _export_has_topics(messages: list[dict[str, Any]]) -> bool:
    for msg in messages:
        if not isinstance(msg, dict):
            continue
        if msg.get("type") != "service":
            continue
        if msg.get("action") in {"topic_created", "topic_edit"}:
            return True
    return False


def _resolve_export_thread_ids(
    *, messages: list[dict[str, Any]], topic_roots: dict[int, str], assume_general_thread: bool
) -> dict[int, int | None]:
    reply_to: dict[int, int] = {}
    for msg in messages:
        if not isinstance(msg, dict):
            continue
        message_id = msg.get("id")
        parent = msg.get("reply_to_message_id")
        if isinstance(message_id, int) and isinstance(parent, int):
            reply_to[message_id] = parent

    cache: dict[int, int | None] = {}

    general_thread_id: int | None = 1 if assume_general_thread else None

    for msg in messages:
        if not isinstance(msg, dict):
            continue
        message_id = msg.get("id")
        if not isinstance(message_id, int):
            continue
        if message_id in cache:
            continue

        path: list[int] = []
        cur = message_id
        seen: set[int] = set()

        while True:
            if cur in cache:
                root = cache[cur]
                break
            if cur in topic_roots:
                root = cur
                break

            parent = reply_to.get(cur)
            if parent is None:
                root = general_thread_id
                break

            if cur in seen:
                root = general_thread_id
                break
            seen.add(cur)

            path.append(cur)
            cur = parent

        for mid in path:
            cache[mid] = root
        cache[message_id] = root

    return cache


def import_export_json(
    *,
    db: Database,
    chat_id: int,
    payload: Any,
    ingested_at_utc: str,
    export_chat_name: str | None = None,
) -> tuple[int, int]:
    messages = _extract_messages(payload, export_chat_name=export_chat_name)

    topics = _extract_export_topics(messages)
    has_topics = _export_has_topics(messages)
    if has_topics:
        db.upsert_topic(chat_id=chat_id, thread_id=1, title="General", now_utc_iso=ingested_at_utc)
    for thread_id, title in topics.items():
        db.upsert_topic(chat_id=chat_id, thread_id=thread_id, title=title, now_utc_iso=ingested_at_utc)

    thread_ids = _resolve_export_thread_ids(
        messages=messages, topic_roots=topics, assume_general_thread=has_topics
    )

    inserted = 0
    skipped = 0

    for msg in messages:
        if not isinstance(msg, dict):
            skipped += 1
            continue

        message_id = msg.get("id")
        if not isinstance(message_id, int):
            skipped += 1
            continue

        date_utc = _parse_export_unixtime(msg.get("date_unixtime")) or None
        if not date_utc:
            # Fallback: keep best-effort ISO string as UTC.
            date_str = msg.get("date")
            if isinstance(date_str, str):
                try:
                    parsed = datetime.fromisoformat(date_str)
                    if parsed.tzinfo is None:
                        parsed = parsed.replace(tzinfo=timezone.utc)
                    date_utc = to_iso_utc(parsed)
                except ValueError:
                    date_utc = None
        if not date_utc:
            skipped += 1
            continue

        edit_date_utc = _parse_export_unixtime(msg.get("edited_unixtime"))

        db.upsert_message(
            {
                "chat_id": chat_id,
                "message_id": message_id,
                "thread_id": thread_ids.get(message_id),
                "date_utc": date_utc,
                "from_id": _parse_export_from_id(msg.get("from_id")),
                "from_username": None,
                "from_display": msg.get("from") if isinstance(msg.get("from"), str) else None,
                "text": _normalize_export_text(msg.get("text")),
                "raw_json": json.dumps(msg, ensure_ascii=False, separators=(",", ":")),
                "reply_to_message_id": msg.get("reply_to_message_id")
                if isinstance(msg.get("reply_to_message_id"), int)
                else None,
                "is_service": 0 if msg.get("type") == "message" else 1,
                "edit_date_utc": edit_date_utc,
                "ingested_at_utc": ingested_at_utc,
            }
        )
        inserted += 1

    return inserted, skipped


def main() -> None:
    load_dotenv()
    configure_logging()

    parser = argparse.ArgumentParser(description="Import Telegram Desktop JSON export into SQLite.")
    parser.add_argument("--chat-id", type=int, required=True, help="DB chat_id (SOURCE_CHAT_ID)")
    parser.add_argument(
        "--path",
        nargs="+",
        required=True,
        help="Path(s) to Telegram Desktop export JSON file(s)",
    )
    parser.add_argument(
        "--db-path",
        default=os.getenv("DB_PATH", "./data/kaspa.db"),
        help="SQLite DB path (default: env DB_PATH or ./data/kaspa.db)",
    )
    parser.add_argument(
        "--export-chat-name",
        default=None,
        help="If the export JSON contains multiple chats, select by exact chat name",
    )
    args = parser.parse_args()

    db = Database(args.db_path)
    db.init_schema()

    ingested_at = to_iso_utc(now_utc())
    total_inserted = 0
    total_skipped = 0

    for path in args.path:
        log.info("Importing %s", path)
        with open(path, "r", encoding="utf-8") as f:
            payload = json.load(f)
        inserted, skipped = import_export_json(
            db=db,
            chat_id=args.chat_id,
            payload=payload,
            ingested_at_utc=ingested_at,
            export_chat_name=args.export_chat_name,
        )
        log.info("Imported=%s skipped=%s from %s", inserted, skipped, path)
        total_inserted += inserted
        total_skipped += skipped

    db.set_state("last_import_at_utc", ingested_at)
    log.info("Done. Total imported=%s skipped=%s", total_inserted, total_skipped)


if __name__ == "__main__":
    main()
