from __future__ import annotations

from datetime import datetime, timezone
import logging

from src.config import Config
from src.db import Database
from src.llm.factory import create_llm_client
from src.rollups.service import update_topic_rollup
from src.util.time import now_utc, to_iso_utc


log = logging.getLogger(__name__)


def _parse_iso_utc(value: str) -> datetime | None:
    try:
        dt = datetime.fromisoformat(value)
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def maybe_refresh_rollups_before_digest(
    *,
    db: Database,
    config: Config,
    window_start_utc: str,
    window_end_utc: str,
) -> None:
    if not config.rollup_auto_refresh_before_digest:
        return

    try:
        llm = create_llm_client(config)
    except Exception:
        log.exception("Rollup refresh skipped: LLM client init failed")
        return
    if llm is None:
        return

    now = now_utc()
    now_iso = to_iso_utc(now)

    last_refresh_raw = db.get_state("last_rollup_refresh_at_utc")
    last_refresh = _parse_iso_utc(last_refresh_raw) if last_refresh_raw else None
    if last_refresh is not None:
        delta = (now - last_refresh).total_seconds()
        if delta < float(config.rollup_refresh_min_interval_seconds):
            return

    activity = db.get_topic_activity(
        chat_id=config.source_chat_id,
        window_start_utc=window_start_utc,
        window_end_utc=window_end_utc,
        limit=config.rollup_refresh_max_topics,
    )
    if not activity:
        db.set_state("last_rollup_refresh_at_utc", now_iso)
        return

    attempted = 0
    updated = 0
    for row in activity:
        thread_id = int(row["thread_id"]) if row["thread_id"] is not None else None
        attempted += 1
        try:
            res = update_topic_rollup(db=db, config=config, llm=llm, thread_id=thread_id, mode=None)
        except Exception:
            log.exception("Rollup refresh failed for thread_id=%s", thread_id)
            continue
        if res.updated:
            updated += 1

    db.set_state("last_rollup_refresh_at_utc", now_iso)
    log.info("Rollup refresh done (updated=%s attempted=%s)", updated, attempted)

