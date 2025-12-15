from __future__ import annotations


def _internal_chat_id_for_tme(chat_id: int) -> int | None:
    """
    For t.me/c/<internal_id>/<message_id> links, Telegram uses the supergroup id
    without the -100 prefix.
    """
    chat_id_abs = abs(int(chat_id))
    if chat_id_abs >= 1_000_000_000_000:
        internal = chat_id_abs - 1_000_000_000_000
        return internal if internal > 0 else None
    return chat_id_abs if chat_id_abs > 0 else None


def build_message_link(
    *,
    chat_id: int,
    message_id: int,
    thread_id: int | None,
    username: str | None,
) -> str | None:
    """
    Best-effort permalink to a Telegram message.

    Public forum chat:
      - https://t.me/<username>/<message_id>
      - https://t.me/<username>/<thread_id>/<message_id>

    Private/supergroup without username:
      - https://t.me/c/<internal_id>/<message_id>
      - https://t.me/c/<internal_id>/<thread_id>/<message_id>
    """
    thread_part = None if thread_id in (None, 1) else int(thread_id)
    if username:
        if thread_part is not None:
            return f"https://t.me/{username}/{thread_part}/{int(message_id)}"
        return f"https://t.me/{username}/{int(message_id)}"

    internal_id = _internal_chat_id_for_tme(chat_id)
    if internal_id is None:
        return None
    if thread_part is not None:
        return f"https://t.me/c/{internal_id}/{thread_part}/{int(message_id)}"
    return f"https://t.me/c/{internal_id}/{int(message_id)}"

