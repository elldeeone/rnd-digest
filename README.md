# Kaspa RND Digest Bot

A Telegram bot that mirrors a source chat into SQLite and provides digest/query commands in a private control chat.

This repo is the implementation of `plan.md` (Milestones A–C).

## Quick start (local)

1. Create a bot token via BotFather.
2. Copy `.env.example` to `.env` and fill values.
3. Install deps:
   - `python -m venv .venv && source .venv/bin/activate`
   - `pip install -r requirements.txt`
4. Run:
   - `python -m src.app`

## Import Telegram Desktop export (backfill)

Place exported JSON under `./data/exports/`, then run:

`python -m src.ingest.importer --chat-id "$SOURCE_CHAT_ID" --path ./data/exports/<export>.json`

## LLM (OpenRouter)

The bot can optionally use an LLM for `/latest` (brief), `/digest` summaries, and `/ask` Q&A.
For these commands, it immediately posts a short “Message received…” placeholder and edits it into the final response.

Set in `.env`:

- `LLM_PROVIDER=openrouter`
- `OPENROUTER_API_KEY=...`
- `OPENROUTER_MODEL=...` (whatever model name OpenRouter exposes)

Optional: keep rollups fresh before scheduled digests:

- `ROLLUP_AUTO_REFRESH_BEFORE_DIGEST=true`

## Commands (control chat only)

- `/help`
- `/health`
- `/latest [6h|2d] [brief|full] [peek]` (no args = since last check-in; shortcut: send `latest`)
- `/search <terms>`
- `/ask [6h|2d|all] <question>`
- `/teach <thread_id> [6h|2d|1w] [detail]` (teach-me explainer)
- `/topic <thread_id> [6h|2d|1w]`
- `/rollup <thread_id> [6h|2d|all|rebuild]`
- `/digest [6h|2d] [overview|full] [advance]` (default = overview w/ buttons; no args = since last digest)

## Docker

`docker compose -f docker/docker-compose.yml up -d --build`

## Tests

`pip install -r requirements-dev.txt && pytest`
