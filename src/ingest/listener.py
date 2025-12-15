from __future__ import annotations

from datetime import datetime, timezone
import json
import logging
from typing import Any

from src.config import Config
from src.db import Database
from src.util.time import now_utc, to_iso_utc


log = logging.getLogger(__name__)


_SERVICE_KEYS = {
    "forum_topic_created",
    "forum_topic_edited",
    "new_chat_members",
    "left_chat_member",
    "pinned_message",
    "new_chat_title",
    "delete_chat_photo",
    "group_chat_created",
    "supergroup_chat_created",
    "channel_chat_created",
    "message_auto_delete_timer_changed",
    "migrate_to_chat_id",
    "migrate_from_chat_id",
}


def _iso_from_unix_seconds(value: Any) -> str | None:
    if not isinstance(value, int):
        return None
    return datetime.fromtimestamp(value, tz=timezone.utc).replace(microsecond=0).isoformat()


def ingest_update(*, db: Database, config: Config, update: dict[str, Any]) -> None:
    message = update.get("message") or update.get("edited_message")
    kind = "message" if "message" in update else ("edited_message" if "edited_message" in update else None)
    if kind is None or not isinstance(message, dict):
        return

    chat_id = message.get("chat", {}).get("id")
    if not isinstance(chat_id, int):
        return

    allowed_chat_ids = {config.source_chat_id, *config.control_chat_ids}
    if chat_id not in allowed_chat_ids:
        return

    message_id = message.get("message_id")
    if not isinstance(message_id, int):
        return

    date_utc = _iso_from_unix_seconds(message.get("date"))
    if not date_utc:
        return

    ingested_at_utc = to_iso_utc(now_utc())

    from_obj = message.get("from") if isinstance(message.get("from"), dict) else {}
    first_name = from_obj.get("first_name") if isinstance(from_obj.get("first_name"), str) else None
    last_name = from_obj.get("last_name") if isinstance(from_obj.get("last_name"), str) else None
    from_display = " ".join([part for part in [first_name, last_name] if part]) or None

    thread_id = message.get("message_thread_id") if isinstance(message.get("message_thread_id"), int) else None
    reply_to_message_id = None
    reply_to = message.get("reply_to_message")
    if isinstance(reply_to, dict) and isinstance(reply_to.get("message_id"), int):
        reply_to_message_id = int(reply_to["message_id"])

    text = None
    if isinstance(message.get("text"), str):
        text = message["text"]
    elif isinstance(message.get("caption"), str):
        text = message["caption"]

    is_service = 1 if any(k in message for k in _SERVICE_KEYS) else 0

    edit_date_utc = _iso_from_unix_seconds(message.get("edit_date")) if kind == "edited_message" else None

    db.upsert_message(
        {
            "chat_id": chat_id,
            "message_id": message_id,
            "thread_id": thread_id,
            "date_utc": date_utc,
            "from_id": int(from_obj["id"]) if isinstance(from_obj.get("id"), int) else None,
            "from_username": from_obj.get("username") if isinstance(from_obj.get("username"), str) else None,
            "from_display": from_display,
            "text": text,
            "raw_json": json.dumps(update, ensure_ascii=False, separators=(",", ":")),
            "reply_to_message_id": reply_to_message_id,
            "is_service": is_service,
            "edit_date_utc": edit_date_utc,
            "ingested_at_utc": ingested_at_utc,
        }
    )

    db.set_state("last_ingest_at_utc", ingested_at_utc)

    if thread_id is not None and chat_id == config.source_chat_id:
        db.upsert_topic(chat_id=chat_id, thread_id=thread_id, title=None, now_utc_iso=ingested_at_utc)

    if isinstance(message.get("forum_topic_created"), dict) and thread_id is not None:
        title = message["forum_topic_created"].get("name")
        title = title if isinstance(title, str) else None
        db.upsert_topic(chat_id=chat_id, thread_id=thread_id, title=title, now_utc_iso=ingested_at_utc)
        log.info("Topic created: chat_id=%s thread_id=%s title=%r", chat_id, thread_id, title)

    if isinstance(message.get("forum_topic_edited"), dict) and thread_id is not None:
        title = message["forum_topic_edited"].get("name")
        title = title if isinstance(title, str) else None
        db.upsert_topic(chat_id=chat_id, thread_id=thread_id, title=title, now_utc_iso=ingested_at_utc)
        log.info("Topic edited: chat_id=%s thread_id=%s title=%r", chat_id, thread_id, title)

