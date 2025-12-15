from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from src.commands.debug import handle_debug_ids
from src.commands.health import handle_health
from src.commands.help import handle_help
from src.commands.latest import handle_latest
from src.commands.search import handle_search
from src.config import Config
from src.db import Database


@dataclass(frozen=True)
class CommandContext:
    config: Config
    db: Database


@dataclass(frozen=True)
class TextResponse:
    text: str


@dataclass(frozen=True)
class DigestRequest:
    pass


def _parse_command(text: str) -> tuple[str, str] | None:
    text = text.strip()
    if not text.startswith("/"):
        return None
    first_line = text.splitlines()[0]
    head, *rest = first_line.split(maxsplit=1)
    command = head[1:]
    if "@" in command:
        command = command.split("@", 1)[0]
    args = rest[0] if rest else ""
    return command.lower(), args.strip()


CommandResult = TextResponse | DigestRequest | None


def handle_command(*, ctx: CommandContext, message: dict[str, Any]) -> CommandResult:
    text = message.get("text")
    if not isinstance(text, str):
        return None

    parsed = _parse_command(text)
    if not parsed:
        return None

    command, args = parsed

    if command in {"help", "start"}:
        return TextResponse(handle_help())
    if command == "health":
        return TextResponse(handle_health(db=ctx.db, config=ctx.config))
    if command == "latest":
        return TextResponse(handle_latest(db=ctx.db, config=ctx.config, args=args))
    if command == "search":
        return TextResponse(handle_search(db=ctx.db, config=ctx.config, args=args))
    if command == "debug_ids":
        return TextResponse(handle_debug_ids(message=message))
    if command == "digest":
        return DigestRequest()

    return TextResponse(f"Unknown command: /{command}\n\n{handle_help()}")
