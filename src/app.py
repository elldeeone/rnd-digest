from __future__ import annotations

import logging
import time
from dataclasses import replace
from datetime import timedelta

import requests
from dotenv import load_dotenv

from src.config import Config
from src.db import Database
from src.commands.ask import handle_ask
from src.commands.latest import build_latest_brief, build_latest_full
from src.commands.rollup import handle_rollup
from src.commands.router import (
    AskRequest,
    CommandContext,
    DigestRequest,
    LatestRequest,
    RollupRequest,
    TextResponse,
    handle_command,
)
from src.digest.build_digest import build_digest
from src.ingest.listener import ingest_update
from src.rollups.refresh import maybe_refresh_rollups_before_digest
from src.telegram_client import TelegramClient
from src.util.logging import configure_logging
from src.util.telegram_format import chunk_text
from src.util.time import DailyTime, next_run_utc, now_utc, to_iso_utc


log = logging.getLogger(__name__)


def main() -> None:
    load_dotenv()
    configure_logging()

    config = Config.from_env()
    log.info(
        "Loaded config (source_chat_id=%s, control_chat_ids=%s, db_path=%s)",
        config.source_chat_id,
        sorted(config.control_chat_ids),
        config.db_path,
    )

    db = Database(config.db_path)
    db.init_schema()

    client = TelegramClient(config.telegram_bot_token)

    if not config.source_chat_username:
        try:
            chat = client.get_chat(chat_id=config.source_chat_id)
            username = chat.get("username") if isinstance(chat, dict) else None
            if isinstance(username, str) and username.strip():
                config = replace(config, source_chat_username=username.strip())
                log.info("Detected source chat username: @%s", config.source_chat_username)
            else:
                log.warning(
                    "SOURCE_CHAT_USERNAME not set and getChat returned no username; receipts may be less clickable"
                )
        except Exception:
            log.exception("Failed to auto-detect SOURCE_CHAT_USERNAME via getChat")

    offset_raw = db.get_state("telegram_update_offset")
    offset = int(offset_raw) if offset_raw else None

    digest_time = DailyTime.parse(config.daily_digest_time)
    next_digest_utc = next_run_utc(tz_name=config.tz, daily_time=digest_time)
    log.info("Next digest scheduled at %s (UTC)", to_iso_utc(next_digest_utc))

    ctx = CommandContext(config=config, db=db)
    backoff_seconds = 1.0

    try:
        while True:
            poll_now_iso = to_iso_utc(now_utc())
            prior_backoff = backoff_seconds
            try:
                updates = client.get_updates(
                    offset=offset,
                    timeout_seconds=config.poll_timeout_seconds,
                    allowed_updates=["message", "edited_message"],
                )
                db.set_state("last_poll_ok_at_utc", poll_now_iso)
                if prior_backoff > 1.0:
                    log.info("getUpdates recovered")
                backoff_seconds = 1.0
            except requests.exceptions.RequestException as exc:
                db.set_state("last_poll_error_at_utc", poll_now_iso)
                log.warning(
                    "getUpdates failed (%s); retrying in %ss",
                    exc.__class__.__name__,
                    backoff_seconds,
                )
                time.sleep(backoff_seconds)
                backoff_seconds = min(backoff_seconds * 2, 60.0)
                updates = []
            except Exception:
                db.set_state("last_poll_error_at_utc", poll_now_iso)
                log.exception("getUpdates failed; retrying in %ss", backoff_seconds)
                time.sleep(backoff_seconds)
                backoff_seconds = min(backoff_seconds * 2, 60.0)
                updates = []

            for update in updates:
                try:
                    ingest_update(db=db, config=config, update=update)
                except Exception:
                    log.exception("Failed to ingest update_id=%s", update.get("update_id"))
                    continue

                offset = int(update["update_id"]) + 1
                db.set_state("telegram_update_offset", str(offset))

                message = update.get("message") or update.get("edited_message")
                if not isinstance(message, dict):
                    continue

                chat_id = message.get("chat", {}).get("id")
                if chat_id not in config.control_chat_ids:
                    continue

                try:
                    result = handle_command(ctx=ctx, message=message)
                    if isinstance(result, TextResponse):
                        client.send_message_fallback_plain(
                            chat_id=int(chat_id),
                            message_thread_id=message.get("message_thread_id"),
                            text=result.text,
                        )
                    elif isinstance(result, LatestRequest):
                        _run_latest(
                            db=db,
                            client=client,
                            config=config,
                            target_chat_id=int(chat_id),
                            target_thread_id=message.get("message_thread_id")
                            if isinstance(message.get("message_thread_id"), int)
                            else None,
                            request=result,
                            message=message,
                        )
                    elif isinstance(result, AskRequest):
                        _run_ask(
                            db=db,
                            client=client,
                            config=config,
                            target_chat_id=int(chat_id),
                            target_thread_id=message.get("message_thread_id")
                            if isinstance(message.get("message_thread_id"), int)
                            else None,
                            request=result,
                        )
                    elif isinstance(result, RollupRequest):
                        _run_rollup(
                            db=db,
                            client=client,
                            config=config,
                            target_chat_id=int(chat_id),
                            target_thread_id=message.get("message_thread_id")
                            if isinstance(message.get("message_thread_id"), int)
                            else None,
                            request=result,
                        )
                    elif isinstance(result, DigestRequest):
                        _run_digest(
                            db=db,
                            client=client,
                            config=config,
                            target_chat_id=int(chat_id),
                            target_thread_id=config.control_digest_thread_id
                            or (
                                message.get("message_thread_id")
                                if isinstance(message.get("message_thread_id"), int)
                                else None
                            ),
                            duration=result.duration,
                            advance_state=result.advance_state,
                            show_processing_notice=True,
                        )
                except Exception:
                    log.exception("Failed handling command message_id=%s", message.get("message_id"))

            now = now_utc()
            if now >= next_digest_utc:
                control_chat_id = sorted(config.control_chat_ids)[0]
                try:
                    _run_digest(
                        db=db,
                        client=client,
                        config=config,
                        target_chat_id=control_chat_id,
                        target_thread_id=config.control_digest_thread_id,
                        duration=None,
                        advance_state=True,
                        show_processing_notice=False,
                    )
                except Exception:
                    log.exception("Scheduled digest failed; will retry in 5 minutes")
                    next_digest_utc = now + timedelta(minutes=5)
                else:
                    next_digest_utc = next_run_utc(tz_name=config.tz, daily_time=digest_time, now=now)
                    log.info(
                        "Digest posted. Next digest scheduled at %s (UTC)", to_iso_utc(next_digest_utc)
                    )

            # Keep loop responsive if no updates (getUpdates long poll blocks).
            time.sleep(0.2)
    finally:
        db.close()


def _format_duration(duration: timedelta) -> str:
    seconds = int(duration.total_seconds())
    if seconds <= 0:
        return "0s"
    if seconds % (7 * 86400) == 0:
        return f"{seconds // (7 * 86400)}w"
    if seconds % 86400 == 0:
        return f"{seconds // 86400}d"
    if seconds % 3600 == 0:
        return f"{seconds // 3600}h"
    if seconds % 60 == 0:
        return f"{seconds // 60}m"
    return f"{seconds}s"


def _latest_checkpoint_key(*, control_chat_id: int, user_id: int) -> str:
    return f"latest_checkpoint_end_utc:{control_chat_id}:{user_id}"


def _llm_probably_enabled(config: Config) -> bool:
    provider = config.llm_provider.strip().lower()
    if provider in {"none", "off", "disabled"}:
        return False
    if provider == "openrouter":
        return bool(config.openrouter_api_key and config.openrouter_model)
    return True


def _send_processing_notice(
    *,
    client: TelegramClient,
    chat_id: int,
    message_thread_id: int | None,
    text: str,
) -> int | None:
    try:
        res = client.send_message_fallback_plain(
            chat_id=chat_id,
            message_thread_id=message_thread_id,
            text=text,
        )
    except Exception:
        log.exception("Failed sending processing notice")
        return None
    return res.message_ids[0] if res.message_ids else None


def _send_with_optional_edit(
    *,
    client: TelegramClient,
    chat_id: int,
    message_thread_id: int | None,
    text: str,
    ack_message_id: int | None,
) -> list[int]:
    if ack_message_id is None:
        return client.send_message_fallback_plain(
            chat_id=chat_id,
            message_thread_id=message_thread_id,
            text=text,
        ).message_ids

    chunks = chunk_text(text)
    if not chunks:
        return []

    message_ids: list[int] = []
    try:
        client.edit_message_text(
            chat_id=chat_id,
            message_id=ack_message_id,
            text=chunks[0],
        )
        message_ids.append(ack_message_id)
    except Exception:
        log.exception("Failed editing processing notice; sending response normally")
        return client.send_message_fallback_plain(
            chat_id=chat_id,
            message_thread_id=message_thread_id,
            text=text,
        ).message_ids

    for chunk in chunks[1:]:
        res = client.send_message_fallback_plain(
            chat_id=chat_id,
            message_thread_id=message_thread_id,
            text=chunk,
        )
        message_ids.extend(res.message_ids)
    return message_ids


def _run_latest(
    *,
    db: Database,
    client: TelegramClient,
    config: Config,
    target_chat_id: int,
    target_thread_id: int | None,
    request: LatestRequest,
    message: dict[str, object],
) -> None:
    now = now_utc()
    window_end = to_iso_utc(now)

    from_obj = message.get("from") if isinstance(message.get("from"), dict) else {}
    user_id = from_obj.get("id") if isinstance(from_obj.get("id"), int) else None
    checkpoint_key = (
        _latest_checkpoint_key(control_chat_id=target_chat_id, user_id=int(user_id))
        if user_id is not None
        else None
    )

    if request.reset:
        if request.advance_state and checkpoint_key is not None:
            db.set_state(checkpoint_key, window_end)
        _send_with_optional_edit(
            chat_id=target_chat_id,
            message_thread_id=target_thread_id,
            text=f"Latest checkpoint reset.\n- window_end_utc: {window_end}",
            client=client,
            ack_message_id=None,
        )
        return

    ack_id = None
    if request.mode == "brief" and _llm_probably_enabled(config):
        ack_id = _send_processing_notice(
            client=client,
            chat_id=target_chat_id,
            message_thread_id=target_thread_id,
            text="Message received — summarizing now.",
        )

    duration = request.duration
    if duration is not None:
        window_start = to_iso_utc(now - duration)
        window_label = f"last {_format_duration(duration)}"
    else:
        last_end = db.get_state(checkpoint_key) if checkpoint_key is not None else None
        if last_end and last_end > window_end:
            last_end = None
        if last_end:
            window_start = last_end
            window_label = "since last check-in"
        else:
            window_start = to_iso_utc(now - timedelta(hours=config.latest_default_window_hours))
            window_label = f"last {config.latest_default_window_hours}h"

    if request.mode == "full":
        text = build_latest_full(
            db=db,
            config=config,
            window_label=window_label,
            window_start_utc=window_start,
            window_end_utc=window_end,
        )
    else:
        text = build_latest_brief(
            db=db,
            config=config,
            window_label=window_label,
            window_start_utc=window_start,
            window_end_utc=window_end,
        )

    _send_with_optional_edit(
        client=client,
        chat_id=target_chat_id,
        message_thread_id=target_thread_id,
        text=text,
        ack_message_id=ack_id,
    )

    if request.advance_state and checkpoint_key is not None:
        db.set_state(checkpoint_key, window_end)


def _run_ask(
    *,
    db: Database,
    client: TelegramClient,
    config: Config,
    target_chat_id: int,
    target_thread_id: int | None,
    request: AskRequest,
) -> None:
    ack_id = None
    if _llm_probably_enabled(config):
        ack_id = _send_processing_notice(
            client=client,
            chat_id=target_chat_id,
            message_thread_id=target_thread_id,
            text="Message received — thinking now.",
        )

    text = handle_ask(db=db, config=config, args=request.args)
    _send_with_optional_edit(
        client=client,
        chat_id=target_chat_id,
        message_thread_id=target_thread_id,
        text=text,
        ack_message_id=ack_id,
    )


def _run_rollup(
    *,
    db: Database,
    client: TelegramClient,
    config: Config,
    target_chat_id: int,
    target_thread_id: int | None,
    request: RollupRequest,
) -> None:
    ack_id = None
    if _llm_probably_enabled(config):
        ack_id = _send_processing_notice(
            client=client,
            chat_id=target_chat_id,
            message_thread_id=target_thread_id,
            text="Message received — updating rollup now.",
        )

    text = handle_rollup(db=db, config=config, args=request.args)
    _send_with_optional_edit(
        client=client,
        chat_id=target_chat_id,
        message_thread_id=target_thread_id,
        text=text,
        ack_message_id=ack_id,
    )


def _run_digest(
    *,
    db: Database,
    client: TelegramClient,
    config: Config,
    target_chat_id: int,
    target_thread_id: int | None,
    duration: timedelta | None,
    advance_state: bool,
    show_processing_notice: bool,
) -> None:
    now = now_utc()
    window_end = to_iso_utc(now)

    ack_id = None
    if show_processing_notice and _llm_probably_enabled(config):
        ack_id = _send_processing_notice(
            client=client,
            chat_id=target_chat_id,
            message_thread_id=target_thread_id,
            text="Message received — generating digest now.",
        )

    if duration is not None:
        window_start = to_iso_utc(now - duration)
    else:
        last_end = db.get_state("last_digest_end_utc")
        window_start = last_end or to_iso_utc(now - timedelta(hours=config.latest_default_window_hours))

    if advance_state:
        try:
            maybe_refresh_rollups_before_digest(
                db=db,
                config=config,
                window_start_utc=window_start,
                window_end_utc=window_end,
            )
        except Exception:
            log.exception("Rollup auto-refresh failed; continuing without rollups")

    digest = build_digest(
        db=db,
        config=config,
        window_start_utc=window_start,
        window_end_utc=window_end,
    )

    message_ids = _send_with_optional_edit(
        client=client,
        chat_id=target_chat_id,
        message_thread_id=target_thread_id,
        text=digest,
        ack_message_id=ack_id,
    )
    created_at = to_iso_utc(now)
    db.insert_digest(
        chat_id=target_chat_id,
        thread_id=target_thread_id,
        window_start_utc=window_start,
        window_end_utc=window_end,
        digest_markdown=digest,
        created_at_utc=created_at,
        telegram_message_ids=message_ids,
    )
    if advance_state:
        db.set_state("last_digest_end_utc", window_end)


if __name__ == "__main__":
    main()
