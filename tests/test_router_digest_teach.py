from __future__ import annotations

from datetime import timedelta

from src.commands.router import CommandContext, DigestRequest, TeachRequest, TextResponse, handle_command
from src.config import Config
from src.db import Database


def _ctx() -> CommandContext:
    db = Database(":memory:")
    db.init_schema()
    config = Config(telegram_bot_token="t", source_chat_id=-1001, control_chat_ids={123})
    return CommandContext(config=config, db=db)


def test_digest_defaults_to_overview_and_advances() -> None:
    result = handle_command(ctx=_ctx(), message={"text": "/digest"})
    assert isinstance(result, DigestRequest)
    assert result.duration is None
    assert result.advance_state is True
    assert result.mode == "overview"


def test_digest_full_flag() -> None:
    result = handle_command(ctx=_ctx(), message={"text": "/digest full"})
    assert isinstance(result, DigestRequest)
    assert result.duration is None
    assert result.advance_state is True
    assert result.mode == "full"


def test_digest_duration_defaults_to_preview() -> None:
    result = handle_command(ctx=_ctx(), message={"text": "/digest 6h"})
    assert isinstance(result, DigestRequest)
    assert result.duration == timedelta(hours=6)
    assert result.advance_state is False
    assert result.mode == "overview"


def test_digest_duration_with_advance() -> None:
    result = handle_command(ctx=_ctx(), message={"text": "/digest 6h advance"})
    assert isinstance(result, DigestRequest)
    assert result.duration == timedelta(hours=6)
    assert result.advance_state is True


def test_digest_duration_with_full_mode() -> None:
    result = handle_command(ctx=_ctx(), message={"text": "/digest 6h full"})
    assert isinstance(result, DigestRequest)
    assert result.duration == timedelta(hours=6)
    assert result.mode == "full"
    assert result.advance_state is False


def test_teach_parses() -> None:
    result = handle_command(ctx=_ctx(), message={"text": "/teach 123"})
    assert isinstance(result, TeachRequest)
    assert result.args == "123"


def test_teach_requires_args() -> None:
    result = handle_command(ctx=_ctx(), message={"text": "/teach"})
    assert isinstance(result, TextResponse)
    assert "Usage:" in result.text

