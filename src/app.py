from __future__ import annotations

import logging
import time
from dataclasses import replace
from datetime import timedelta

from dotenv import load_dotenv

from src.config import Config
from src.db import Database
from src.commands.router import CommandContext, DigestRequest, TextResponse, handle_command
from src.digest.build_digest import build_digest
from src.ingest.listener import ingest_update
from src.telegram_client import TelegramClient
from src.util.logging import configure_logging
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
            try:
                updates = client.get_updates(
                    offset=offset,
                    timeout_seconds=config.poll_timeout_seconds,
                    allowed_updates=["message", "edited_message"],
                )
                backoff_seconds = 1.0
            except Exception:
                log.exception("getUpdates failed; retrying in %ss", backoff_seconds)
                time.sleep(backoff_seconds)
                backoff_seconds = min(backoff_seconds * 2, 60.0)
                continue

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


def _run_digest(
    *,
    db: Database,
    client: TelegramClient,
    config: Config,
    target_chat_id: int,
    target_thread_id: int | None,
    duration: timedelta | None,
    advance_state: bool,
) -> None:
    now = now_utc()
    window_end = to_iso_utc(now)
    if duration is not None:
        window_start = to_iso_utc(now - duration)
    else:
        last_end = db.get_state("last_digest_end_utc")
        window_start = last_end or to_iso_utc(now - timedelta(hours=config.latest_default_window_hours))

    digest = build_digest(
        db=db,
        config=config,
        window_start_utc=window_start,
        window_end_utc=window_end,
    )

    send_res = client.send_message_fallback_plain(
        chat_id=target_chat_id,
        message_thread_id=target_thread_id,
        text=digest,
    )
    created_at = to_iso_utc(now)
    db.insert_digest(
        chat_id=target_chat_id,
        thread_id=target_thread_id,
        window_start_utc=window_start,
        window_end_utc=window_end,
        digest_markdown=digest,
        created_at_utc=created_at,
        telegram_message_ids=send_res.message_ids,
    )
    if advance_state:
        db.set_state("last_digest_end_utc", window_end)


if __name__ == "__main__":
    main()
