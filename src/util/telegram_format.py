from __future__ import annotations

from dataclasses import dataclass


TELEGRAM_MAX_MESSAGE_CHARS = 4096


_ORPHAN_HEADERS = {
    "Summary",
    "Top threads",
    "By topic",
    "Links:",
    "Quotes:",
    "Answer",
    "Receipts",
}


def _avoid_orphan_header_cut(chunk: str) -> int | None:
    """
    If a chunk ends with a header (e.g. "Quotes:"), move that header to the next
    Telegram message so it isn't orphaned from its content.

    Returns a new cut position within the chunk (0-based), or None if no change.
    """
    stripped = chunk.rstrip()
    if not stripped:
        return None

    lines = stripped.splitlines()
    while lines and not lines[-1].strip():
        lines.pop()
    if not lines:
        return None

    last = lines[-1].strip()
    if last not in _ORPHAN_HEADERS and not last.startswith("Topic: "):
        return None

    prefix = "\n".join(lines[:-1])
    new_cut = len(prefix)
    return new_cut if new_cut > 0 else None


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

        cut_override = _avoid_orphan_header_cut(remaining[:cut])
        if cut_override is not None and cut_override > 0:
            cut = cut_override

        chunks.append(remaining[:cut].rstrip())
        remaining = remaining[cut:].lstrip("\n")
    return [c for c in chunks if c]


@dataclass(frozen=True)
class SendResult:
    message_ids: list[int]
