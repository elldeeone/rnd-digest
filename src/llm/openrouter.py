from __future__ import annotations

from dataclasses import dataclass
import logging
from typing import Any

import requests

from src.llm.interface import ChatMessage


log = logging.getLogger(__name__)


@dataclass(frozen=True)
class OpenRouterClient:
    api_key: str
    model: str
    base_url: str = "https://openrouter.ai/api/v1"
    site_url: str | None = None
    app_name: str | None = None

    def chat(
        self,
        *,
        messages: list[ChatMessage],
        temperature: float,
        max_tokens: int,
        timeout_seconds: int,
    ) -> str:
        url = f"{self.base_url.rstrip('/')}/chat/completions"
        headers: dict[str, str] = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
        }
        if self.site_url:
            headers["HTTP-Referer"] = self.site_url
        if self.app_name:
            headers["X-Title"] = self.app_name

        payload: dict[str, Any] = {
            "model": self.model,
            "messages": [{"role": m.role, "content": m.content} for m in messages],
            "temperature": float(temperature),
            "max_tokens": int(max_tokens),
        }

        resp = requests.post(url, json=payload, headers=headers, timeout=timeout_seconds)
        try:
            data = resp.json()
        except ValueError:
            resp.raise_for_status()
            raise RuntimeError(f"OpenRouter chat failed: HTTP {resp.status_code} (non-JSON response)")

        if not resp.ok:
            err = data.get("error") if isinstance(data, dict) else None
            msg = None
            if isinstance(err, dict):
                msg = err.get("message")
            raise RuntimeError(
                f"OpenRouter chat failed: HTTP {resp.status_code}"
                + (f", {msg}" if isinstance(msg, str) and msg.strip() else "")
            )

        choices = data.get("choices") if isinstance(data, dict) else None
        if not isinstance(choices, list) or not choices:
            raise RuntimeError(f"OpenRouter chat returned unexpected payload: {data!r}")

        message = choices[0].get("message") if isinstance(choices[0], dict) else None
        if not isinstance(message, dict):
            raise RuntimeError(f"OpenRouter chat returned unexpected payload: {data!r}")

        content = message.get("content")
        if not isinstance(content, str):
            raise RuntimeError(f"OpenRouter chat returned unexpected payload: {data!r}")

        return content

