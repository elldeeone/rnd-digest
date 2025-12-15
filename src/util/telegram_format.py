from __future__ import annotations

from dataclasses import dataclass


TELEGRAM_MAX_MESSAGE_CHARS = 4096


def chunk_text(text: str, *, limit: int = TELEGRAM_MAX_MESSAGE_CHARS) -> list[str]:
    if len(text) <= limit:
        return [text]

    chunks: list[str] = []
    remaining = text
    while remaining:
        if len(remaining) <= limit:
            chunks.append(remaining)
            break

        # Prefer splitting on high-level section boundaries so related content
        # (e.g., a whole "Topic: ..." block) stays in the same Telegram message.
        cut = remaining.rfind("\n\nTopic:", 0, limit)
        if cut <= 0:
            cut = remaining.rfind("\n\n", 0, limit)
        if cut <= 0:
            cut = remaining.rfind("\n", 0, limit)
        if cut <= 0:
            cut = limit

        chunks.append(remaining[:cut].rstrip())
        remaining = remaining[cut:].lstrip("\n")
    return [c for c in chunks if c]


@dataclass(frozen=True)
class SendResult:
    message_ids: list[int]
