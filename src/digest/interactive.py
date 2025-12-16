from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import re
from typing import Any

from src.commands.latest import build_latest_brief
from src.config import Config
from src.db import Database


_MAX_BUTTON_TEXT = 24
_WS_RE = re.compile(r"\s+")


def _to_iso_utc(ts: int) -> str:
    return datetime.fromtimestamp(int(ts), tz=timezone.utc).replace(microsecond=0).isoformat()


def _format_window_label(*, start_ts: int, end_ts: int) -> str:
    seconds = max(0, int(end_ts) - int(start_ts))
    if seconds <= 0:
        return "last 0s"
    if seconds < 90:
        return f"last {seconds}s"
    if seconds < 3600:
        minutes = max(1, int(round(seconds / 60)))
        return f"last {minutes}m"
    if seconds < 86400:
        hours = max(1, int(round(seconds / 3600)))
        return f"last {hours}h"
    if seconds < 7 * 86400:
        days = max(1, int(round(seconds / 86400)))
        return f"last {days}d"
    weeks = max(1, int(round(seconds / (7 * 86400))))
    return f"last {weeks}w"


def _short_label(label: str) -> str:
    cleaned = _WS_RE.sub(" ", label).strip()
    if len(cleaned) <= _MAX_BUTTON_TEXT:
        return cleaned
    return cleaned[: _MAX_BUTTON_TEXT - 1].rstrip() + "…"


def _encode_thread_id(thread_id: int | None) -> str:
    return "n" if thread_id is None else str(int(thread_id))


def _decode_thread_id(token: str) -> int | None:
    if token == "n":
        return None
    return int(token)


@dataclass(frozen=True)
class DigestCallback:
    start_ts: int
    end_ts: int
    kind: str  # "menu" or "do"
    action: str
    thread_id: int | None = None


def encode_digest_callback(cb: DigestCallback) -> str:
    base = f"dg|{int(cb.start_ts)}|{int(cb.end_ts)}|{cb.kind}|{cb.action}"
    if cb.kind == "do":
        return base + f"|{_encode_thread_id(cb.thread_id)}"
    return base


def parse_digest_callback(data: str) -> DigestCallback | None:
    parts = [p.strip() for p in data.split("|")]
    if len(parts) < 5 or parts[0] != "dg":
        return None
    try:
        start_ts = int(parts[1])
        end_ts = int(parts[2])
    except ValueError:
        return None
    kind = parts[3]
    action = parts[4]
    if kind not in {"menu", "do"}:
        return None
    thread_id = None
    if kind == "do":
        token = parts[5] if len(parts) >= 6 else "n"
        try:
            thread_id = _decode_thread_id(token)
        except ValueError:
            return None
    return DigestCallback(start_ts=start_ts, end_ts=end_ts, kind=kind, action=action, thread_id=thread_id)


def build_digest_main_keyboard(*, start_ts: int, end_ts: int) -> dict[str, Any]:
    return {
        "inline_keyboard": [
            [
                {
                    "text": "Teach me",
                    "callback_data": encode_digest_callback(
                        DigestCallback(start_ts, end_ts, "menu", "teach")
                    ),
                },
                {
                    "text": "Receipts",
                    "callback_data": encode_digest_callback(
                        DigestCallback(start_ts, end_ts, "menu", "receipts")
                    ),
                },
                {
                    "text": "Links",
                    "callback_data": encode_digest_callback(
                        DigestCallback(start_ts, end_ts, "menu", "links")
                    ),
                },
            ]
        ]
    }


def build_digest_overview_text(*, db: Database, config: Config, start_ts: int, end_ts: int) -> str:
    window_start_utc = _to_iso_utc(start_ts)
    window_end_utc = _to_iso_utc(end_ts)
    window_label = _format_window_label(start_ts=start_ts, end_ts=end_ts)
    return build_latest_brief(
        db=db,
        config=config,
        header="Daily Digest",
        window_label=window_label,
        window_start_utc=window_start_utc,
        window_end_utc=window_end_utc,
        limit_topics=config.digest_max_topics,
        include_summary=False,
        include_topic_links=False,
        expand_hint="Buttons: Teach me (explain) • Receipts (quotes) • Links (URLs) • Back returns here",
    )


def build_digest_topic_view_keyboard(
    *,
    start_ts: int,
    end_ts: int,
    thread_id: int | None,
    view: str,
) -> dict[str, Any]:
    if view not in {"teach", "teach_detail", "receipts", "links"}:
        raise ValueError(f"Unsupported view: {view!r}")

    rows: list[list[dict[str, str]]] = []
    if view == "teach":
        rows.append(
            [
                {
                    "text": "Details",
                    "callback_data": encode_digest_callback(
                        DigestCallback(start_ts=start_ts, end_ts=end_ts, kind="do", action="teach_detail", thread_id=thread_id)
                    ),
                }
            ]
        )
        rows.append(
            [
                {
                    "text": "Receipts",
                    "callback_data": encode_digest_callback(
                        DigestCallback(start_ts=start_ts, end_ts=end_ts, kind="do", action="receipts", thread_id=thread_id)
                    ),
                },
                {
                    "text": "Links",
                    "callback_data": encode_digest_callback(
                        DigestCallback(start_ts=start_ts, end_ts=end_ts, kind="do", action="links", thread_id=thread_id)
                    ),
                },
            ]
        )
        picker_action = "teach"
    elif view == "teach_detail":
        rows.append(
            [
                {
                    "text": "Summary",
                    "callback_data": encode_digest_callback(
                        DigestCallback(start_ts=start_ts, end_ts=end_ts, kind="do", action="teach", thread_id=thread_id)
                    ),
                }
            ]
        )
        rows.append(
            [
                {
                    "text": "Receipts",
                    "callback_data": encode_digest_callback(
                        DigestCallback(start_ts=start_ts, end_ts=end_ts, kind="do", action="receipts", thread_id=thread_id)
                    ),
                },
                {
                    "text": "Links",
                    "callback_data": encode_digest_callback(
                        DigestCallback(start_ts=start_ts, end_ts=end_ts, kind="do", action="links", thread_id=thread_id)
                    ),
                },
            ]
        )
        picker_action = "teach"
    elif view == "receipts":
        rows.append(
            [
                {
                    "text": "Teach me",
                    "callback_data": encode_digest_callback(
                        DigestCallback(start_ts=start_ts, end_ts=end_ts, kind="do", action="teach", thread_id=thread_id)
                    ),
                },
                {
                    "text": "Links",
                    "callback_data": encode_digest_callback(
                        DigestCallback(start_ts=start_ts, end_ts=end_ts, kind="do", action="links", thread_id=thread_id)
                    ),
                },
            ]
        )
        picker_action = "receipts"
    else:
        rows.append(
            [
                {
                    "text": "Teach me",
                    "callback_data": encode_digest_callback(
                        DigestCallback(start_ts=start_ts, end_ts=end_ts, kind="do", action="teach", thread_id=thread_id)
                    ),
                },
                {
                    "text": "Receipts",
                    "callback_data": encode_digest_callback(
                        DigestCallback(start_ts=start_ts, end_ts=end_ts, kind="do", action="receipts", thread_id=thread_id)
                    ),
                },
            ]
        )
        picker_action = "links"

    rows.append(
        [
            {
                "text": "Pick topic",
                "callback_data": encode_digest_callback(
                    DigestCallback(start_ts=start_ts, end_ts=end_ts, kind="menu", action=picker_action)
                ),
            },
            {
                "text": "Back",
                "callback_data": encode_digest_callback(
                    DigestCallback(start_ts=start_ts, end_ts=end_ts, kind="menu", action="main")
                ),
            },
        ]
    )

    return {"inline_keyboard": rows}


def _format_topic_label(*, title: str | None, thread_id: int | None) -> str:
    if title:
        return title
    if thread_id is None:
        return "No topic"
    return f"Thread {thread_id}"


def _topic_activity(
    *,
    db: Database,
    config: Config,
    window_start_utc: str,
    window_end_utc: str,
    limit: int,
) -> list[dict[str, Any]]:
    activity = db.get_topic_activity(
        chat_id=config.source_chat_id,
        window_start_utc=window_start_utc,
        window_end_utc=window_end_utc,
        limit=limit,
    )
    thread_ids = [int(row["thread_id"]) for row in activity if row["thread_id"] is not None]
    titles = db.get_topic_titles(chat_id=config.source_chat_id, thread_ids=thread_ids)
    missing = [tid for tid in thread_ids if tid not in titles]
    if missing:
        db.backfill_topic_titles_from_raw_json(
            chat_id=config.source_chat_id,
            thread_ids=missing,
            limit=2000,
            now_utc_iso=window_end_utc,
        )
        titles = db.get_topic_titles(chat_id=config.source_chat_id, thread_ids=thread_ids)

    topics: list[dict[str, Any]] = []
    for idx, row in enumerate(activity, start=1):
        thread_id = int(row["thread_id"]) if row["thread_id"] is not None else None
        title = titles.get(int(thread_id)) if thread_id is not None else None
        topics.append(
            {
                "idx": idx,
                "thread_id": thread_id,
                "label": _format_topic_label(title=title, thread_id=thread_id),
                "count": int(row["message_count"]),
            }
        )
    return topics


def build_digest_topics_keyboard(
    *,
    db: Database,
    config: Config,
    start_ts: int,
    end_ts: int,
    action: str,
) -> dict[str, Any]:
    if action not in {"teach", "receipts", "links"}:
        raise ValueError(f"Unsupported action: {action!r}")

    window_start_utc = _to_iso_utc(start_ts)
    window_end_utc = _to_iso_utc(end_ts)

    topics = _topic_activity(
        db=db,
        config=config,
        window_start_utc=window_start_utc,
        window_end_utc=window_end_utc,
        limit=config.digest_max_topics,
    )

    rows: list[list[dict[str, str]]] = []
    for t in topics:
        text = f"T{t['idx']}: {_short_label(str(t['label']))}"
        rows.append(
            [
                {
                    "text": text,
                    "callback_data": encode_digest_callback(
                        DigestCallback(start_ts=start_ts, end_ts=end_ts, kind="do", action=action, thread_id=t["thread_id"])
                    ),
                }
            ]
        )

    rows.append(
        [
            {
                "text": "Back",
                "callback_data": encode_digest_callback(
                    DigestCallback(start_ts=start_ts, end_ts=end_ts, kind="menu", action="main")
                ),
            }
        ]
    )

    return {"inline_keyboard": rows}
