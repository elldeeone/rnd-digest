from __future__ import annotations

from datetime import timedelta

from src.commands.ask import _build_fts_query, _extract_citations, _parse_ask_args


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

