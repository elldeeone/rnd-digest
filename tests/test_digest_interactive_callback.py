from __future__ import annotations

from src.digest.interactive import DigestCallback, encode_digest_callback, parse_digest_callback


def test_callback_roundtrip_menu() -> None:
    data = encode_digest_callback(DigestCallback(start_ts=1, end_ts=2, kind="menu", action="teach"))
    cb = parse_digest_callback(data)
    assert cb is not None
    assert cb.kind == "menu"
    assert cb.action == "teach"
    assert cb.thread_id is None


def test_callback_roundtrip_do_with_none_thread() -> None:
    data = encode_digest_callback(DigestCallback(start_ts=1, end_ts=2, kind="do", action="teach", thread_id=None))
    cb = parse_digest_callback(data)
    assert cb is not None
    assert cb.kind == "do"
    assert cb.action == "teach"
    assert cb.thread_id is None

