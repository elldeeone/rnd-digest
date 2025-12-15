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
        cut = remaining.rfind("\n", 0, limit)
        if cut <= 0:
            cut = limit
        chunks.append(remaining[:cut].rstrip())
        remaining = remaining[cut:].lstrip("\n")
    return chunks


@dataclass(frozen=True)
class SendResult:
    message_ids: list[int]

