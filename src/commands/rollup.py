from __future__ import annotations

from src.config import Config
from src.db import Database
from src.llm.factory import create_llm_client
from src.rollups.service import update_topic_rollup


def _parse_rollup_args(args: str) -> tuple[int | None, str | None]:
    parts = args.strip().split()
    if not parts:
        raise ValueError("Usage: /rollup <thread_id> [6h|2d|all|rebuild]")

    thread_raw = parts[0].strip().lower()
    if thread_raw in {"none", "no_topic", "no-topic"}:
        thread_id = None
    else:
        thread_id = int(parts[0])

    mode = parts[1].strip().lower() if len(parts) > 1 else None
    return thread_id, mode


def handle_rollup(*, db: Database, config: Config, args: str) -> str:
    try:
        thread_id, mode = _parse_rollup_args(args)
    except Exception as exc:
        return str(exc)

    try:
        llm = create_llm_client(config)
    except Exception as exc:
        llm = None
        llm_err = str(exc)
    else:
        llm_err = None

    if llm is None:
        return f"LLM unavailable: {llm_err or 'LLM_PROVIDER=none'}"

    try:
        result = update_topic_rollup(db=db, config=config, llm=llm, thread_id=thread_id, mode=mode)
    except Exception as exc:
        return f"Rollup failed: {exc}"

    if not result.updated:
        return (
            "Topic rollup (no new messages)\n"
            f"- topic: {result.label}\n"
            f"- updated_at_utc: {result.updated_at_utc}\n\n"
            f"{result.summary}"
        )

    return (
        "Topic rollup updated\n"
        f"- topic: {result.label}\n"
        f"- window: {result.window_label}\n"
        f"- last_message_id: {result.last_message_id}\n"
        f"- updated_at_utc: {result.updated_at_utc}\n\n"
        f"{result.summary}"
    )
