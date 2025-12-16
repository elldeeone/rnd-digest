from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta
import re
from typing import Any

from src.commands.debug import handle_debug_ids
from src.commands.health import handle_health
from src.commands.help import handle_help
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
    mode: str


@dataclass(frozen=True)
class AskRequest:
    args: str


@dataclass(frozen=True)
class RollupRequest:
    args: str


@dataclass(frozen=True)
class LatestRequest:
    duration: timedelta | None
    mode: str
    advance_state: bool
    reset: bool


@dataclass(frozen=True)
class TeachRequest:
    args: str


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


_WS_RE = re.compile(r"\s+")
_TRAILING_PUNCT_RE = re.compile(r"[\s\.\!\?]+$")


def _normalize_free_text(text: str) -> str:
    text = _WS_RE.sub(" ", text.strip().lower())
    text = _TRAILING_PUNCT_RE.sub("", text)
    return text


def _parse_free_text_intent(text: str) -> tuple[str, str] | None:
    """
    In control chat, allow a small set of natural-language shortcuts so Luke can
    just type "latest" without remembering slash commands.
    """
    norm = _normalize_free_text(text)
    if not norm:
        return None

    if norm == "latest":
        return "latest", ""
    if norm.startswith("latest "):
        return "latest", norm[len("latest ") :].strip()

    for prefix in [
        "give me the latest",
        "give me latest",
        "what's the latest",
        "whats the latest",
        "what happened since we last spoke",
        "what has happened since we last spoke",
        "what did i miss",
        "catch me up",
    ]:
        if norm == prefix:
            return "latest", ""
        if norm.startswith(prefix + " "):
            return "latest", norm[len(prefix) + 1 :].strip()

    return None


def _parse_latest_args(args: str) -> LatestRequest | TextResponse:
    """
    /latest [6h|2d] [brief|full] [peek]

    No duration => since the user's last /latest check-in.
    """
    parts = [p.strip() for p in args.split() if p.strip()]
    duration: timedelta | None = None
    mode = "brief"
    advance_state = True
    reset = False

    for part in parts:
        token = part.lower()
        if token in {"please", "pls", "in", "for", "last", "past", "since"}:
            continue
        if token in {"brief", "gist", "summary"}:
            mode = "brief"
            continue
        if token in {"full", "verbose", "details", "detail"}:
            mode = "full"
            continue
        if token in {"peek", "dry", "noadvance", "no-advance"}:
            advance_state = False
            continue
        if token in {"reset", "clear"}:
            reset = True
            continue

        if duration is None:
            try:
                duration = parse_duration(token)
            except ValueError:
                return TextResponse(
                    "Usage: /latest [6h|2d] [brief|full] [peek]\nTip: you can also just send 'latest'."
                )
        else:
            return TextResponse("Usage: /latest [6h|2d] [brief|full] [peek]")

    return LatestRequest(duration=duration, mode=mode, advance_state=advance_state, reset=reset)


CommandResult = TextResponse | DigestRequest | LatestRequest | AskRequest | RollupRequest | TeachRequest | None


def handle_command(*, ctx: CommandContext, message: dict[str, Any]) -> CommandResult:
    text = message.get("text")
    if not isinstance(text, str):
        return None

    parsed = _parse_command(text) or _parse_free_text_intent(text)
    if not parsed:
        return None

    command, args = parsed

    if command in {"help", "start"}:
        return TextResponse(handle_help())
    if command == "health":
        return TextResponse(handle_health(db=ctx.db, config=ctx.config))
    if command == "latest":
        return _parse_latest_args(args)
    if command == "search":
        return TextResponse(handle_search(db=ctx.db, config=ctx.config, args=args))
    if command == "ask":
        if not args.strip():
            return TextResponse("Usage: /ask [6h|2d|all] <question>")
        return AskRequest(args=args)
    if command in {"teach", "explain"}:
        if not args.strip():
            return TextResponse("Usage: /teach <thread_id> [6h|2d|1w]")
        return TeachRequest(args=args)
    if command == "rollup":
        if not args.strip():
            return TextResponse("Usage: /rollup <thread_id> [6h|2d|all|rebuild]")
        return RollupRequest(args=args)
    if command == "topic":
        return TextResponse(handle_topic(db=ctx.db, config=ctx.config, args=args))
    if command == "debug_ids":
        return TextResponse(handle_debug_ids(message=message))
    if command == "digest":
        args_parts = [p.strip() for p in args.split() if p.strip()]
        duration: timedelta | None = None
        mode = "overview"
        advance_state: bool | None = None

        for raw in args_parts:
            token = raw.strip().lower()
            if token in {"since_last", "since-last", "since"}:
                duration = None
                continue
            if token in {"full", "text", "verbose", "details"}:
                mode = "full"
                continue
            if token in {"overview", "brief", "ui", "interactive"}:
                mode = "overview"
                continue
            if token in {"advance", "commit"}:
                advance_state = True
                continue
            if token in {"peek", "preview", "dry", "noadvance", "no-advance"}:
                advance_state = False
                continue

            if duration is None:
                try:
                    duration = parse_duration(token)
                except ValueError:
                    return TextResponse(
                        "Usage: /digest [6h|2d] [overview|full] [advance]\n\n"
                        "Tip: /digest (no args) posts since last digest."
                    )
            else:
                return TextResponse("Usage: /digest [6h|2d] [overview|full] [advance]")

        if advance_state is None:
            advance_state = False if duration is not None else True

        return DigestRequest(duration=duration, advance_state=advance_state, mode=mode)
    if command in {"set_topic_title", "set_topic"}:
        return TextResponse(handle_set_topic_title(db=ctx.db, config=ctx.config, args=args))
    if command == "backfill_topics":
        return TextResponse(handle_backfill_topics(db=ctx.db, config=ctx.config, args=args))

    return TextResponse(f"Unknown command: /{command}\n\n{handle_help()}")
