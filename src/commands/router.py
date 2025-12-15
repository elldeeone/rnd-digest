from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta
from typing import Any

from src.commands.ask import handle_ask
from src.commands.debug import handle_debug_ids
from src.commands.health import handle_health
from src.commands.help import handle_help
from src.commands.latest import handle_latest
from src.commands.rollup import handle_rollup
from src.commands.search import handle_search
from src.commands.topic import handle_topic
from src.commands.topics import handle_backfill_topics, handle_set_topic_title
from src.config import Config
from src.db import Database
from src.util.time import parse_duration


@dataclass(frozen=True)
class CommandContext:
    config: Config
    db: Database


@dataclass(frozen=True)
class TextResponse:
    text: str


@dataclass(frozen=True)
class DigestRequest:
    duration: timedelta | None
    advance_state: bool


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
    if command == "ask":
        return TextResponse(handle_ask(db=ctx.db, config=ctx.config, args=args))
    if command == "rollup":
        return TextResponse(handle_rollup(db=ctx.db, config=ctx.config, args=args))
    if command == "topic":
        return TextResponse(handle_topic(db=ctx.db, config=ctx.config, args=args))
    if command == "debug_ids":
        return TextResponse(handle_debug_ids(message=message))
    if command == "digest":
        args_parts = args.split()
        if not args_parts:
            return DigestRequest(duration=None, advance_state=True)

        token = args_parts[0].strip().lower()
        if token in {"since_last", "since-last", "since"}:
            return DigestRequest(duration=None, advance_state=True)

        try:
            duration = parse_duration(token)
        except ValueError:
            return TextResponse("Usage: /digest [6h|2d]\n\nTip: /digest (no args) posts since last digest.")

        # Ad-hoc digest: do not move the scheduled digest boundary.
        advance_state = any(part.lower() in {"advance", "commit"} for part in args_parts[1:])
        return DigestRequest(duration=duration, advance_state=advance_state)
    if command in {"set_topic_title", "set_topic"}:
        return TextResponse(handle_set_topic_title(db=ctx.db, config=ctx.config, args=args))
    if command == "backfill_topics":
        return TextResponse(handle_backfill_topics(db=ctx.db, config=ctx.config, args=args))

    return TextResponse(f"Unknown command: /{command}\n\n{handle_help()}")
