from __future__ import annotations

from src.commands.router import AskRequest, CommandContext, LatestRequest, RollupRequest, TextResponse, handle_command
from src.config import Config
from src.db import Database


def _make_ctx() -> CommandContext:
    db = Database(":memory:")
    db.init_schema()
    config = Config(telegram_bot_token="t", source_chat_id=-1001, control_chat_ids={123})
    return CommandContext(config=config, db=db)


def test_latest_parses_default() -> None:
    ctx = _make_ctx()
    result = handle_command(ctx=ctx, message={"text": "/latest"})
    assert isinstance(result, LatestRequest)
    assert result.duration is None
    assert result.mode == "brief"
    assert result.advance_state is True
    assert result.reset is False


def test_latest_parses_flags_and_duration() -> None:
    ctx = _make_ctx()
    result = handle_command(ctx=ctx, message={"text": "/latest 6h full peek"})
    assert isinstance(result, LatestRequest)
    assert result.duration is not None
    assert int(result.duration.total_seconds()) == 6 * 3600
    assert result.mode == "full"
    assert result.advance_state is False


def test_latest_free_text_shortcut() -> None:
    ctx = _make_ctx()
    result = handle_command(ctx=ctx, message={"text": "Give me the latest"})
    assert isinstance(result, LatestRequest)


def test_ask_returns_request() -> None:
    ctx = _make_ctx()
    result = handle_command(ctx=ctx, message={"text": "/ask 6h what's going on?"})
    assert isinstance(result, AskRequest)


def test_ask_requires_args() -> None:
    ctx = _make_ctx()
    result = handle_command(ctx=ctx, message={"text": "/ask"})
    assert isinstance(result, TextResponse)


def test_rollup_returns_request() -> None:
    ctx = _make_ctx()
    result = handle_command(ctx=ctx, message={"text": "/rollup 123 rebuild"})
    assert isinstance(result, RollupRequest)
