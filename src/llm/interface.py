from __future__ import annotations

from dataclasses import dataclass
from typing import Literal, Protocol


Role = Literal["system", "user", "assistant"]


@dataclass(frozen=True)
class ChatMessage:
    role: Role
    content: str


class LLMClient(Protocol):
    def chat(
        self,
        *,
        messages: list[ChatMessage],
        temperature: float,
        max_tokens: int,
        timeout_seconds: int,
    ) -> str: ...

