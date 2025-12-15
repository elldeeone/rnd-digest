from __future__ import annotations

from src.util.telegram_links import build_message_link


def test_build_message_link_public_no_thread() -> None:
    assert (
        build_message_link(chat_id=-1001, message_id=123, thread_id=None, username="kasparnd")
        == "https://t.me/kasparnd/123"
    )


def test_build_message_link_public_with_thread() -> None:
    assert (
        build_message_link(chat_id=-1001, message_id=73, thread_id=72, username="kasparnd")
        == "https://t.me/kasparnd/72/73"
    )


def test_build_message_link_private_fallback_with_thread() -> None:
    assert (
        build_message_link(chat_id=-1002471422883, message_id=73, thread_id=72, username=None)
        == "https://t.me/c/2471422883/72/73"
    )

