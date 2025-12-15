from __future__ import annotations

from typing import Any


def handle_debug_ids(*, message: dict[str, Any]) -> str:
    chat_id = message.get("chat", {}).get("id")
    thread_id = message.get("message_thread_id")

    return (
        "Debug IDs:\n"
        f"chat_id: {chat_id}\n"
        f"thread_id: {thread_id}\n"
    )

