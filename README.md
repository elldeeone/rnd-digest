# Kaspa RND Digest Bot

A Telegram bot that mirrors a source chat into SQLite and provides digest/query commands in a private control chat.

This repo is the implementation of `plan.md` (Milestone A + basic digest plumbing).

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

## Docker

`docker compose -f docker/docker-compose.yml up -d --build`

## Tests

`pip install -r requirements-dev.txt && pytest`
