from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
import json
import logging
import os
import sqlite3
from typing import Any, Iterable


log = logging.getLogger(__name__)


SCHEMA_VERSION = "1"


def _ensure_parent_dir(path: str) -> None:
    parent = os.path.dirname(os.path.abspath(path))
    if parent:
        os.makedirs(parent, exist_ok=True)


@dataclass(frozen=True)
class SearchHit:
    chat_id: int
    message_id: int
    thread_id: int | None
    date_utc: str
    from_display: str | None
    from_username: str | None
    text: str | None
    snippet: str | None


class Database:
    def __init__(self, db_path: str) -> None:
        _ensure_parent_dir(db_path)
        self.db_path = db_path
        self.conn = sqlite3.connect(db_path, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row

        self.conn.execute("PRAGMA journal_mode=WAL;")
        self.conn.execute("PRAGMA synchronous=NORMAL;")
        self.conn.execute("PRAGMA foreign_keys=ON;")

    def close(self) -> None:
        self.conn.close()

    def init_schema(self) -> None:
        with self.conn:
            self.conn.execute(
                """
                CREATE TABLE IF NOT EXISTS messages (
                    id INTEGER PRIMARY KEY,
                    chat_id INTEGER NOT NULL,
                    message_id INTEGER NOT NULL,
                    thread_id INTEGER NULL,
                    date_utc TEXT NOT NULL,
                    from_id INTEGER NULL,
                    from_username TEXT NULL,
                    from_display TEXT NULL,
                    text TEXT NULL,
                    raw_json TEXT NOT NULL,
                    reply_to_message_id INTEGER NULL,
                    is_service INTEGER NOT NULL DEFAULT 0,
                    edit_date_utc TEXT NULL,
                    ingested_at_utc TEXT NOT NULL,
                    UNIQUE(chat_id, message_id)
                );
                """
            )
            self.conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_messages_chat_thread_date ON messages(chat_id, thread_id, date_utc);"
            )

            self.conn.execute(
                """
                CREATE TABLE IF NOT EXISTS topics (
                    id INTEGER PRIMARY KEY,
                    chat_id INTEGER NOT NULL,
                    thread_id INTEGER NOT NULL,
                    title TEXT NULL,
                    created_at_utc TEXT NULL,
                    updated_at_utc TEXT NULL,
                    UNIQUE(chat_id, thread_id)
                );
                """
            )

            self.conn.execute(
                """
                CREATE TABLE IF NOT EXISTS digests (
                    id INTEGER PRIMARY KEY,
                    chat_id INTEGER NOT NULL,
                    thread_id INTEGER NULL,
                    window_start_utc TEXT NOT NULL,
                    window_end_utc TEXT NOT NULL,
                    digest_markdown TEXT NOT NULL,
                    created_at_utc TEXT NOT NULL,
                    telegram_message_ids TEXT NULL
                );
                """
            )

            self.conn.execute(
                """
                CREATE TABLE IF NOT EXISTS state (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL
                );
                """
            )

            self.conn.execute(
                "INSERT OR IGNORE INTO state(key, value) VALUES ('schema_version', ?);",
                (SCHEMA_VERSION,),
            )

            current_version = self.get_state("schema_version")
            if current_version != SCHEMA_VERSION:
                raise RuntimeError(
                    f"Unsupported schema_version={current_version!r} (expected {SCHEMA_VERSION!r})"
                )

            self._init_fts()

    def _init_fts(self) -> None:
        """
        Best-effort FTS5. If FTS5 isn't available in the runtime SQLite build,
        search commands will be disabled but ingestion continues.
        """
        try:
            self.conn.execute(
                """
                CREATE VIRTUAL TABLE IF NOT EXISTS messages_fts
                USING fts5(text, content='messages', content_rowid='id');
                """
            )
            self.conn.execute(
                """
                CREATE TRIGGER IF NOT EXISTS messages_ai AFTER INSERT ON messages BEGIN
                    INSERT INTO messages_fts(rowid, text) VALUES (new.id, coalesce(new.text, ''));
                END;
                """
            )
            self.conn.execute(
                """
                CREATE TRIGGER IF NOT EXISTS messages_ad AFTER DELETE ON messages BEGIN
                    INSERT INTO messages_fts(messages_fts, rowid, text)
                    VALUES ('delete', old.id, coalesce(old.text, ''));
                END;
                """
            )
            self.conn.execute(
                """
                CREATE TRIGGER IF NOT EXISTS messages_au AFTER UPDATE ON messages BEGIN
                    INSERT INTO messages_fts(messages_fts, rowid, text)
                    VALUES ('delete', old.id, coalesce(old.text, ''));
                    INSERT INTO messages_fts(rowid, text) VALUES (new.id, coalesce(new.text, ''));
                END;
                """
            )
            self.conn.execute("INSERT INTO messages_fts(messages_fts) VALUES('rebuild');")
        except sqlite3.OperationalError:
            log.exception("FTS5 unavailable; /search will be disabled")

    def get_state(self, key: str) -> str | None:
        row = self.conn.execute("SELECT value FROM state WHERE key = ?;", (key,)).fetchone()
        if not row:
            return None
        return str(row["value"])

    def set_state(self, key: str, value: str) -> None:
        with self.conn:
            self.conn.execute(
                """
                INSERT INTO state(key, value) VALUES (?, ?)
                ON CONFLICT(key) DO UPDATE SET value = excluded.value;
                """,
                (key, value),
            )

    def upsert_message(self, record: dict[str, Any]) -> None:
        with self.conn:
            self.conn.execute(
                """
                INSERT INTO messages (
                    chat_id,
                    message_id,
                    thread_id,
                    date_utc,
                    from_id,
                    from_username,
                    from_display,
                    text,
                    raw_json,
                    reply_to_message_id,
                    is_service,
                    edit_date_utc,
                    ingested_at_utc
                ) VALUES (
                    :chat_id,
                    :message_id,
                    :thread_id,
                    :date_utc,
                    :from_id,
                    :from_username,
                    :from_display,
                    :text,
                    :raw_json,
                    :reply_to_message_id,
                    :is_service,
                    :edit_date_utc,
                    :ingested_at_utc
                )
                ON CONFLICT(chat_id, message_id) DO UPDATE SET
                    thread_id = excluded.thread_id,
                    date_utc = excluded.date_utc,
                    from_id = excluded.from_id,
                    from_username = excluded.from_username,
                    from_display = excluded.from_display,
                    text = excluded.text,
                    raw_json = excluded.raw_json,
                    reply_to_message_id = excluded.reply_to_message_id,
                    is_service = excluded.is_service,
                    edit_date_utc = excluded.edit_date_utc,
                    ingested_at_utc = excluded.ingested_at_utc;
                """,
                record,
            )

    def upsert_topic(
        self, *, chat_id: int, thread_id: int, title: str | None, now_utc_iso: str
    ) -> None:
        with self.conn:
            self.conn.execute(
                """
                INSERT INTO topics(chat_id, thread_id, title, created_at_utc, updated_at_utc)
                VALUES (:chat_id, :thread_id, :title, :now_utc_iso, :now_utc_iso)
                ON CONFLICT(chat_id, thread_id) DO UPDATE SET
                    title = COALESCE(excluded.title, topics.title),
                    updated_at_utc = excluded.updated_at_utc;
                """,
                {
                    "chat_id": chat_id,
                    "thread_id": thread_id,
                    "title": title,
                    "now_utc_iso": now_utc_iso,
                },
            )

    def get_last_ingested_message_time(self, *, chat_id: int) -> str | None:
        row = self.conn.execute(
            "SELECT MAX(date_utc) AS last_date_utc FROM messages WHERE chat_id = ?;",
            (chat_id,),
        ).fetchone()
        if not row:
            return None
        return row["last_date_utc"]

    def get_message_count(self, *, chat_id: int) -> int:
        row = self.conn.execute(
            "SELECT COUNT(*) AS c FROM messages WHERE chat_id = ?;",
            (chat_id,),
        ).fetchone()
        return int(row["c"]) if row else 0

    def get_topic_activity(
        self,
        *,
        chat_id: int,
        window_start_utc: str,
        window_end_utc: str,
        limit: int,
    ) -> list[sqlite3.Row]:
        return list(
            self.conn.execute(
                """
                SELECT
                    thread_id,
                    COUNT(*) AS message_count,
                    MIN(date_utc) AS first_date_utc,
                    MAX(date_utc) AS last_date_utc
                FROM messages
                WHERE
                    chat_id = :chat_id
                    AND is_service = 0
                    AND date_utc >= :window_start_utc
                    AND date_utc <= :window_end_utc
                GROUP BY thread_id
                ORDER BY message_count DESC
                LIMIT :limit;
                """,
                {
                    "chat_id": chat_id,
                    "window_start_utc": window_start_utc,
                    "window_end_utc": window_end_utc,
                    "limit": limit,
                },
            )
        )

    def get_messages_for_topic(
        self,
        *,
        chat_id: int,
        thread_id: int | None,
        window_start_utc: str,
        window_end_utc: str,
        limit: int,
    ) -> list[sqlite3.Row]:
        if thread_id is None:
            where_thread = "thread_id IS NULL"
            params: dict[str, Any] = {}
        else:
            where_thread = "thread_id = :thread_id"
            params = {"thread_id": thread_id}

        return list(
            self.conn.execute(
                f"""
                SELECT
                    message_id,
                    thread_id,
                    date_utc,
                    from_username,
                    from_display,
                    text
                FROM messages
                WHERE
                    chat_id = :chat_id
                    AND is_service = 0
                    AND {where_thread}
                    AND date_utc >= :window_start_utc
                    AND date_utc <= :window_end_utc
                ORDER BY date_utc ASC
                LIMIT :limit;
                """,
                {
                    "chat_id": chat_id,
                    "window_start_utc": window_start_utc,
                    "window_end_utc": window_end_utc,
                    "limit": limit,
                    **params,
                },
            )
        )

    def get_topic_titles(self, *, chat_id: int, thread_ids: Iterable[int]) -> dict[int, str]:
        ids = [int(tid) for tid in thread_ids]
        if not ids:
            return {}

        placeholders = ",".join(["?"] * len(ids))
        rows = self.conn.execute(
            f"""
            SELECT thread_id, title
            FROM topics
            WHERE chat_id = ? AND thread_id IN ({placeholders});
            """,
            (chat_id, *ids),
        ).fetchall()

        return {int(row["thread_id"]): str(row["title"]) for row in rows if row["title"]}

    def search_messages(self, *, chat_id: int, query: str, limit: int = 10) -> list[SearchHit]:
        try:
            rows = self.conn.execute(
                """
                SELECT
                    m.chat_id,
                    m.message_id,
                    m.thread_id,
                    m.date_utc,
                    m.from_display,
                    m.from_username,
                    m.text,
                    snippet(messages_fts, 0, '[', ']', '…', 10) AS snippet
                FROM messages_fts
                JOIN messages m ON m.id = messages_fts.rowid
                WHERE m.chat_id = :chat_id AND messages_fts MATCH :query
                ORDER BY bm25(messages_fts)
                LIMIT :limit;
                """,
                {"chat_id": chat_id, "query": query, "limit": limit},
            ).fetchall()
        except sqlite3.OperationalError as exc:
            raise RuntimeError("FTS search unavailable (FTS5 not enabled)") from exc

        return [
            SearchHit(
                chat_id=int(r["chat_id"]),
                message_id=int(r["message_id"]),
                thread_id=int(r["thread_id"]) if r["thread_id"] is not None else None,
                date_utc=str(r["date_utc"]),
                from_display=str(r["from_display"]) if r["from_display"] is not None else None,
                from_username=str(r["from_username"]) if r["from_username"] is not None else None,
                text=str(r["text"]) if r["text"] is not None else None,
                snippet=str(r["snippet"]) if r["snippet"] is not None else None,
            )
            for r in rows
        ]

    def insert_digest(
        self,
        *,
        chat_id: int,
        thread_id: int | None,
        window_start_utc: str,
        window_end_utc: str,
        digest_markdown: str,
        created_at_utc: str,
        telegram_message_ids: list[int] | None,
    ) -> int:
        telegram_message_ids_json = (
            json.dumps(telegram_message_ids) if telegram_message_ids is not None else None
        )
        with self.conn:
            cur = self.conn.execute(
                """
                INSERT INTO digests(
                    chat_id,
                    thread_id,
                    window_start_utc,
                    window_end_utc,
                    digest_markdown,
                    created_at_utc,
                    telegram_message_ids
                ) VALUES (?, ?, ?, ?, ?, ?, ?);
                """,
                (
                    chat_id,
                    thread_id,
                    window_start_utc,
                    window_end_utc,
                    digest_markdown,
                    created_at_utc,
                    telegram_message_ids_json,
                ),
            )
        return int(cur.lastrowid)

