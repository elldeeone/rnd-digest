from __future__ import annotations

from datetime import timedelta

from src.commands.ask import (
    _build_fts_query,
    _extract_citations,
    _extract_query_tokens,
    _is_broad_question,
    _parse_ask_args,
    _score_message,
)


def test_build_fts_query_removes_stopwords() -> None:
    assert _build_fts_query("What is the status of the stratum bridge?") == "status OR stratum OR bridge"


def test_parse_ask_args_duration() -> None:
    parsed = _parse_ask_args("6h what changed?")
    assert parsed is not None
    duration, all_time, question = parsed
    assert duration == timedelta(hours=6)
    assert all_time is False
    assert question == "what changed?"


def test_parse_ask_args_all() -> None:
    parsed = _parse_ask_args("all what changed?")
    assert parsed == (None, True, "what changed?")


def test_extract_citations() -> None:
    text = "Answer:\nfoo\n\nCitations: E1, E3, E999"
    assert _extract_citations(text, max_evidence=5) == [1, 3]


def test_is_broad_question_tldr() -> None:
    q = "give me the tldr on the last 24h"
    tokens = _extract_query_tokens(q)
    assert _is_broad_question(q, tokens=tokens) is True


def test_score_message_prefers_github_pr_over_logs() -> None:
    log_text = "2025-12-15T11:17:30.495514Z  INFO [[Instance 1]] Processed 83 blocks and 83 headers in 10.00s"
    pr_text = "PR #784 is up for review https://github.com/kaspanet/rusty-kaspa/pull/784"
    assert _score_message(pr_text) > _score_message(log_text)
