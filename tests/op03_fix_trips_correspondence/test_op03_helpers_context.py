from __future__ import annotations

import json
from typing import Any

import pytest

from pylondrina.errors import FixError
from pylondrina.fixing import (
    _sanitize_context_with_issues,
    sanitize_correspondence_context,
)
from pylondrina.reports import Issue


def _assert_json_safe(value: Any) -> None:
    """Assert auxiliar: el valor debe poder serializarse como JSON."""
    json.dumps(value)


def _issue_codes(issues: list[Issue]) -> list[str]:
    """Retorna los códigos de issues en el mismo orden en que fueron emitidos."""
    return [issue.code for issue in issues]


def _issue_by_code(issues: list[Issue], code: str) -> Issue:
    """Busca un issue por código y falla explícitamente si no existe."""
    for issue in issues:
        if issue.code == code:
            return issue
    raise AssertionError(f"No se encontró el issue esperado: {code}")


def test_sanitize_correspondence_context_preserves_valid_context() -> None:
    """Verifica que un contexto permitido y JSON-safe se preserve sin issues."""
    context_ok = {
        "reason": "corrección manual tras revisión",
        "author": "sebastian",
        "source": {"type": "catalog", "name": "EOD_modo", "version": "3.1"},
        "scope": {"fields": ["mode", "purpose"], "rows": {"n_target_rows": 12}},
        "notes": "ajuste de categorías a canónicos",
    }

    context_sanitized, unknown_keys, dropped_paths = sanitize_correspondence_context(context_ok)

    assert context_sanitized == context_ok
    assert unknown_keys == []
    assert dropped_paths == []
    _assert_json_safe(context_sanitized)


def test_sanitize_correspondence_context_drops_unknown_keys_and_non_serializable_fragments() -> None:
    """Verifica que se descarten keys no permitidas y solo fragmentos no serializables."""
    context_mixed = {
        "reason": "ajuste manual",
        "foo": 123,
        "scope": {
            "fields": ["mode"],
            "rows": {"n_target_rows": 10, "bad": object()},
        },
        "notes": {"valid": "texto", "bad": object()},
    }

    context_sanitized, unknown_keys, dropped_paths = sanitize_correspondence_context(context_mixed)

    assert unknown_keys == ["foo"]
    assert context_sanitized is not None

    assert "foo" not in context_sanitized
    assert context_sanitized["reason"] == context_mixed["reason"]
    assert context_sanitized["scope"]["fields"] == context_mixed["scope"]["fields"]
    assert context_sanitized["scope"]["rows"]["n_target_rows"] == context_mixed["scope"]["rows"]["n_target_rows"]
    assert "bad" not in context_sanitized["scope"]["rows"]
    assert context_sanitized["notes"]["valid"] == context_mixed["notes"]["valid"]
    assert "bad" not in context_sanitized["notes"]

    assert "scope.rows.bad" in dropped_paths
    assert "notes.bad" in dropped_paths
    _assert_json_safe(context_sanitized)


def test_sanitize_correspondence_context_rejects_invalid_root() -> None:
    """Verifica que el helper puro rechace un root distinto de dict o None."""
    with pytest.raises(TypeError):
        sanitize_correspondence_context(["not", "a", "dict"])


def test_sanitize_context_with_issues_emits_degradable_context_warnings() -> None:
    """Verifica que el helper interno sanee el contexto y emita warnings esperados."""
    n_rows_total = 12
    sample_rows_per_issue = 5
    context_with_issues = {
        "reason": "ajuste manual",
        "unknown_key": "x",
        "notes": {"ok": "texto", "bad": object()},
    }

    context_sanitized, issues = _sanitize_context_with_issues(
        context_with_issues,
        sample_rows_per_issue=sample_rows_per_issue,
        n_rows_total=n_rows_total,
    )

    assert context_sanitized is not None
    assert context_sanitized["reason"] == context_with_issues["reason"]
    assert context_sanitized["notes"]["ok"] == context_with_issues["notes"]["ok"]
    assert "bad" not in context_sanitized["notes"]
    assert "unknown_key" not in context_sanitized
    _assert_json_safe(context_sanitized)

    assert _issue_codes(issues) == [
        "FIX.CONTEXT.UNKNOWN_KEYS_DROPPED",
        "FIX.CONTEXT.NON_SERIALIZABLE_DROPPED",
    ]
    assert all(issue.level == "warning" for issue in issues)

    unknown_issue = _issue_by_code(issues, "FIX.CONTEXT.UNKNOWN_KEYS_DROPPED")
    assert unknown_issue.details is not None
    assert unknown_issue.details["unknown_keys"] == ["unknown_key"]
    assert "unknown_key" not in context_sanitized

    non_serializable_issue = _issue_by_code(issues, "FIX.CONTEXT.NON_SERIALIZABLE_DROPPED")
    assert non_serializable_issue.details is not None
    assert "notes.bad" in non_serializable_issue.details["dropped_paths"]

    for issue in issues:
        _assert_json_safe(issue.details)


def test_sanitize_context_with_issues_translates_invalid_root_to_fix_error() -> None:
    """Verifica que el helper interno traduzca un root inválido a FixError operativo."""
    with pytest.raises(FixError) as exc_info:
        _sanitize_context_with_issues(
            ["not", "a", "dict"],
            sample_rows_per_issue=5,
            n_rows_total=3,
        )

    err = exc_info.value
    assert err.code == "FIX.CONTEXT.INVALID_ROOT"
    assert err.issue is not None
    assert err.issue.code == "FIX.CONTEXT.INVALID_ROOT"
    assert err.issues is not None
    assert _issue_codes(list(err.issues)) == ["FIX.CONTEXT.INVALID_ROOT"]
    _assert_json_safe(err.details)