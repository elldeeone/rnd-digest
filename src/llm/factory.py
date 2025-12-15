from __future__ import annotations

from src.config import Config
from src.llm.interface import LLMClient
from src.llm.openrouter import OpenRouterClient


def create_llm_client(config: Config) -> LLMClient | None:
    provider = config.llm_provider.strip().lower()
    if provider in {"none", "off", "disabled"}:
        return None

    if provider in {"openrouter"}:
        if not config.openrouter_api_key:
            raise RuntimeError("LLM_PROVIDER=openrouter requires OPENROUTER_API_KEY")
        if not config.openrouter_model:
            raise RuntimeError("LLM_PROVIDER=openrouter requires OPENROUTER_MODEL")
        return OpenRouterClient(
            api_key=config.openrouter_api_key,
            model=config.openrouter_model,
            base_url=config.openrouter_base_url,
            site_url=config.openrouter_site_url,
            app_name=config.openrouter_app_name,
        )

    raise RuntimeError(f"Unknown LLM_PROVIDER: {config.llm_provider!r}")

