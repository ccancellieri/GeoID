#    Copyright 2026 FAO
#    Licensed under the Apache License, Version 2.0 (the "License").

"""Unit tests for AppliedPresetsService.list_for_scope."""
from __future__ import annotations

import json
from datetime import datetime
from typing import Any, Optional
from unittest.mock import AsyncMock, MagicMock, patch

import pytest


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _dt(ts: str) -> datetime:
    return datetime.fromisoformat(ts)


def _make_row(
    preset_name: str,
    scope_key: str,
    state: str = "applied",
    applied_at: Optional[datetime] = None,
) -> dict:
    return {
        "preset_name": preset_name,
        "scope_key": scope_key,
        "state": state,
        "applied_at": applied_at,
        "applied_by": None,
        "params_snapshot": "{}",
        "revoke_descriptor": None,
        "apply_task_id": None,
        "revoke_task_id": None,
        "last_error": None,
        "updated_at": applied_at,
    }


def _svc(engine: Any = None):
    from dynastore.modules.iam.applied_presets_service import AppliedPresetsService
    return AppliedPresetsService(engine)


# ---------------------------------------------------------------------------
# Cursor encode/decode round-trip
# ---------------------------------------------------------------------------

def test_cursor_encode_decode_round_trip():
    from dynastore.modules.iam.applied_presets_service import _encode_cursor, _decode_cursor

    at_iso = "2026-05-01T10:00:00+00:00"
    name = "some_preset"
    cursor = _encode_cursor(at_iso, name)
    assert isinstance(cursor, str)
    decoded_at, decoded_name = _decode_cursor(cursor)
    assert decoded_at == at_iso
    assert decoded_name == name


def test_cursor_decode_none_applied_at():
    from dynastore.modules.iam.applied_presets_service import _encode_cursor, _decode_cursor

    cursor = _encode_cursor(None, "preset_x")
    at_iso, name = _decode_cursor(cursor)
    assert at_iso is None
    assert name == "preset_x"


def test_cursor_decode_malformed_raises_value_error():
    from dynastore.modules.iam.applied_presets_service import _decode_cursor

    with pytest.raises(ValueError, match="invalid cursor"):
        _decode_cursor("not_valid_base64!!!")


def test_cursor_decode_wrong_shape_raises_value_error():
    import base64
    from dynastore.modules.iam.applied_presets_service import _decode_cursor

    bad = base64.urlsafe_b64encode(json.dumps({"bad": "shape"}).encode()).decode()
    with pytest.raises(ValueError, match="invalid cursor"):
        _decode_cursor(bad)


def test_cursor_decode_rejects_malformed_iso_timestamp():
    """Crafted cursor with a non-ISO ``applied_at_iso`` must raise ValueError
    at decode time so the endpoint returns 400 instead of forwarding the
    string to PostgreSQL where the TIMESTAMPTZ cast would 500."""
    import base64
    from dynastore.modules.iam.applied_presets_service import _decode_cursor

    bad = base64.urlsafe_b64encode(
        json.dumps(["not-a-timestamp", "preset_x"]).encode()
    ).decode()
    with pytest.raises(ValueError, match="ISO 8601"):
        _decode_cursor(bad)


# ---------------------------------------------------------------------------
# list_for_scope — happy path by scope tier
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_list_for_scope_returns_rows_platform():
    svc = _svc(MagicMock())
    rows = [_make_row("defaults_postgres", "platform", applied_at=_dt("2026-05-01T10:00:00+00:00"))]

    with patch("dynastore.modules.iam.applied_presets_queries.LIST_FOR_SCOPE") as mock_q:
        mock_q.execute = AsyncMock(return_value=rows)
        result, next_cur = await svc.list_for_scope(scope_key="platform", state="applied")

    assert len(result) == 1
    assert result[0]["preset_name"] == "defaults_postgres"
    assert next_cur is None  # only 1 row, limit default=50


@pytest.mark.asyncio
async def test_list_for_scope_returns_rows_catalog():
    svc = _svc(MagicMock())
    rows = [_make_row("public_catalog", "catalog:test", applied_at=_dt("2026-05-02T09:00:00+00:00"))]

    with patch("dynastore.modules.iam.applied_presets_queries.LIST_FOR_SCOPE") as mock_q:
        mock_q.execute = AsyncMock(return_value=rows)
        result, next_cur = await svc.list_for_scope(scope_key="catalog:test", state="applied")

    assert result[0]["scope_key"] == "catalog:test"
    assert next_cur is None


@pytest.mark.asyncio
async def test_list_for_scope_returns_rows_collection():
    svc = _svc(MagicMock())
    scope = "catalog:test/collection:col1"
    rows = [_make_row("private_collection", scope, applied_at=_dt("2026-05-03T08:00:00+00:00"))]

    with patch("dynastore.modules.iam.applied_presets_queries.LIST_FOR_SCOPE") as mock_q:
        mock_q.execute = AsyncMock(return_value=rows)
        result, _ = await svc.list_for_scope(scope_key=scope, state="applied")

    assert result[0]["scope_key"] == scope


# ---------------------------------------------------------------------------
# Filters by state
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_list_for_scope_filters_by_revoked_state():
    svc = _svc(MagicMock())
    rows = [_make_row("public_catalog", "platform", state="revoked")]

    with patch("dynastore.modules.iam.applied_presets_queries.LIST_FOR_SCOPE") as mock_q:
        mock_q.execute = AsyncMock(return_value=rows)
        result, _ = await svc.list_for_scope(scope_key="platform", state="revoked")

    mock_q.execute.assert_awaited_once()
    call_kwargs = mock_q.execute.await_args[1]
    assert call_kwargs["state"] == "revoked"
    assert result[0]["state"] == "revoked"


# ---------------------------------------------------------------------------
# Empty result
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_list_for_scope_empty_returns_none_cursor():
    svc = _svc(MagicMock())

    with patch("dynastore.modules.iam.applied_presets_queries.LIST_FOR_SCOPE") as mock_q:
        mock_q.execute = AsyncMock(return_value=[])
        result, next_cur = await svc.list_for_scope(scope_key="platform")

    assert result == []
    assert next_cur is None


@pytest.mark.asyncio
async def test_list_for_scope_none_response_treated_as_empty():
    svc = _svc(MagicMock())

    with patch("dynastore.modules.iam.applied_presets_queries.LIST_FOR_SCOPE") as mock_q:
        mock_q.execute = AsyncMock(return_value=None)
        result, next_cur = await svc.list_for_scope(scope_key="platform")

    assert result == []
    assert next_cur is None


# ---------------------------------------------------------------------------
# Keyset pagination — next_cursor produced when len == limit
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_list_for_scope_produces_next_cursor_when_full_page():
    svc = _svc(MagicMock())
    at = _dt("2026-05-01T12:00:00+00:00")
    rows = [_make_row(f"preset_{i}", "platform", applied_at=at) for i in range(3)]

    with patch("dynastore.modules.iam.applied_presets_queries.LIST_FOR_SCOPE") as mock_q:
        mock_q.execute = AsyncMock(return_value=rows)
        result, next_cur = await svc.list_for_scope(scope_key="platform", limit=3)

    assert len(result) == 3
    assert next_cur is not None


@pytest.mark.asyncio
async def test_list_for_scope_no_cursor_when_partial_page():
    svc = _svc(MagicMock())
    at = _dt("2026-05-01T12:00:00+00:00")
    rows = [_make_row("preset_a", "platform", applied_at=at)]

    with patch("dynastore.modules.iam.applied_presets_queries.LIST_FOR_SCOPE") as mock_q:
        mock_q.execute = AsyncMock(return_value=rows)
        _, next_cur = await svc.list_for_scope(scope_key="platform", limit=5)

    assert next_cur is None


@pytest.mark.asyncio
async def test_list_for_scope_cursor_routes_to_cursor_query():
    from dynastore.modules.iam.applied_presets_service import _encode_cursor

    svc = _svc(MagicMock())
    cursor = _encode_cursor("2026-05-01T12:00:00+00:00", "preset_z")

    with patch("dynastore.modules.iam.applied_presets_queries.LIST_FOR_SCOPE_CURSOR") as mock_q:
        mock_q.execute = AsyncMock(return_value=[])
        await svc.list_for_scope(scope_key="platform", cursor=cursor)

    mock_q.execute.assert_awaited_once()


@pytest.mark.asyncio
async def test_list_for_scope_no_cursor_routes_to_base_query():
    svc = _svc(MagicMock())

    with patch("dynastore.modules.iam.applied_presets_queries.LIST_FOR_SCOPE") as mock_q:
        mock_q.execute = AsyncMock(return_value=[])
        await svc.list_for_scope(scope_key="platform")

    mock_q.execute.assert_awaited_once()


# ---------------------------------------------------------------------------
# Limit clamping
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_list_for_scope_limit_clamped_to_200():
    svc = _svc(MagicMock())

    with patch("dynastore.modules.iam.applied_presets_queries.LIST_FOR_SCOPE") as mock_q:
        mock_q.execute = AsyncMock(return_value=[])
        await svc.list_for_scope(scope_key="platform", limit=9999)

    called_limit = mock_q.execute.await_args[1]["limit"]
    assert called_limit == 200


@pytest.mark.asyncio
async def test_list_for_scope_limit_clamped_to_1():
    svc = _svc(MagicMock())

    with patch("dynastore.modules.iam.applied_presets_queries.LIST_FOR_SCOPE") as mock_q:
        mock_q.execute = AsyncMock(return_value=[])
        await svc.list_for_scope(scope_key="platform", limit=0)

    called_limit = mock_q.execute.await_args[1]["limit"]
    assert called_limit == 1
