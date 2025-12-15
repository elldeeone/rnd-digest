from __future__ import annotations

from dataclasses import dataclass
import logging
from typing import Any

import requests
import json

from src.util.telegram_format import SendResult, chunk_text


log = logging.getLogger(__name__)


@dataclass(frozen=True)
class TelegramClient:
    token: str

    @property
    def base_url(self) -> str:
        return f"https://api.telegram.org/bot{self.token}"

    def get_updates(
        self,
        *,
        offset: int | None,
        timeout_seconds: int,
        allowed_updates: list[str] | None = None,
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        params: dict[str, Any] = {"timeout": timeout_seconds, "limit": limit}
        if offset is not None:
            params["offset"] = offset
        if allowed_updates is not None:
            params["allowed_updates"] = json.dumps(allowed_updates)

        resp = requests.get(f"{self.base_url}/getUpdates", params=params, timeout=timeout_seconds + 10)
        resp.raise_for_status()
        payload = resp.json()
        if not payload.get("ok"):
            raise RuntimeError(f"getUpdates failed: {payload!r}")
        return payload.get("result", [])

    def send_message(
        self,
        *,
        chat_id: int,
        text: str,
        message_thread_id: int | None = None,
        parse_mode: str | None = None,
        disable_web_page_preview: bool = True,
    ) -> SendResult:
        message_ids: list[int] = []
        for chunk in chunk_text(text):
            params: dict[str, Any] = {
                "chat_id": chat_id,
                "text": chunk,
                "disable_web_page_preview": disable_web_page_preview,
            }
            if message_thread_id is not None:
                params["message_thread_id"] = message_thread_id
            if parse_mode is not None:
                params["parse_mode"] = parse_mode

            resp = requests.post(f"{self.base_url}/sendMessage", data=params, timeout=30)
            try:
                payload = resp.json()
            except ValueError:
                resp.raise_for_status()
                raise RuntimeError(f"sendMessage failed: HTTP {resp.status_code} (non-JSON response)")

            if not resp.ok or not payload.get("ok"):
                description = payload.get("description")
                raise RuntimeError(
                    f"sendMessage failed: HTTP {resp.status_code}, {description or payload!r}"
                )

            message_ids.append(int(payload["result"]["message_id"]))
        return SendResult(message_ids=message_ids)

    def send_message_fallback_plain(
        self,
        *,
        chat_id: int,
        text: str,
        message_thread_id: int | None = None,
    ) -> SendResult:
        # Intentionally send as plain text (no parse_mode) to avoid Telegram MarkdownV2
        # escaping issues for command output.
        return self.send_message(
            chat_id=chat_id,
            text=text,
            message_thread_id=message_thread_id,
            parse_mode=None,
        )
