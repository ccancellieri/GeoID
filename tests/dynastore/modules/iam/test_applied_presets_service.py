"""State machine and CRUD tests for AppliedPresetsService using mock DB."""
from __future__ import annotations

from typing import Any, Dict, Optional
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import uuid4

import pytest

from dynastore.modules.iam.applied_presets_service import AppliedPresetsService


# ---------------------------------------------------------------------------
# Mock infrastructure
# ---------------------------------------------------------------------------

def _row(**fields: Any) -> MagicMock:
    """Build a mock DB row with _mapping attribute."""
    row = MagicMock()
    row._mapping = fields
    return row


class _MockEngine:
    """Fake engine for the service constructor."""


def _make_service() -> tuple[AppliedPresetsService, dict]:
    """Return (service, state) where state tracks calls to mock queries."""
    state: Dict[str, Any] = {}
    engine = _MockEngine()

    svc = AppliedPresetsService(engine)  # type: ignore[arg-type]
    return svc, state


# ---------------------------------------------------------------------------
# Test get / list (pure read — mock the query module)
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_get_returns_none_when_row_missing():
    svc, _ = _make_service()
    with patch(
        "dynastore.modules.iam.applied_presets_service._q.SELECT_ROW"
    ) as mock_q:
        mock_q.execute = AsyncMock(return_value=None)
        result = await svc.get("some-preset", "platform")
    assert result is None


@pytest.mark.asyncio
async def test_get_returns_dict_when_row_present():
    svc, _ = _make_service()
    row = _row(preset_name="p", scope_key="platform", state="applied")
    with patch(
        "dynastore.modules.iam.applied_presets_service._q.SELECT_ROW"
    ) as mock_q:
        mock_q.execute = AsyncMock(return_value=row)
        result = await svc.get("p", "platform")
    assert result is not None
    assert result["state"] == "applied"


# ---------------------------------------------------------------------------
# insert_pending — state machine entry
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_insert_pending_returns_pending_row():
    svc, _ = _make_service()
    row = _row(
        preset_name="p", scope_key="platform", state="pending",
        applied_by=None, params_snapshot="{}"
    )
    with patch(
        "dynastore.modules.iam.applied_presets_service._q.UPSERT_PENDING"
    ) as mock_q:
        mock_q.execute = AsyncMock(return_value=row)
        result = await svc.insert_pending("p", "platform", {}, applied_by=None)
    assert result["state"] == "pending"


@pytest.mark.asyncio
async def test_insert_pending_serialises_params():
    """insert_pending must JSON-serialise the params dict before storing."""
    svc, _ = _make_service()
    import json

    row = _row(preset_name="p", scope_key="platform", state="pending", params_snapshot='{"x": 1}')
    captured: Dict[str, Any] = {}

    async def _capture(resource, **kwargs: Any):
        captured.update(kwargs)
        return row

    with patch("dynastore.modules.iam.applied_presets_service._q.UPSERT_PENDING") as mock_q:
        mock_q.execute = _capture
        await svc.insert_pending("p", "platform", {"x": 1}, applied_by=None)

    # The params_snapshot kwarg should be a JSON string.
    assert json.loads(captured["params_snapshot"]) == {"x": 1}


# ---------------------------------------------------------------------------
# State transitions
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_mark_in_progress():
    svc, _ = _make_service()
    row = _row(preset_name="p", scope_key="platform", state="in_progress")
    with patch("dynastore.modules.iam.applied_presets_service._q.MARK_IN_PROGRESS") as mock_q:
        mock_q.execute = AsyncMock(return_value=row)
        result = await svc.mark_in_progress("p", "platform")
    assert result is not None
    assert result["state"] == "in_progress"


@pytest.mark.asyncio
async def test_mark_applied_stores_revoke_descriptor():
    import json

    svc, _ = _make_service()
    captured: Dict[str, Any] = {}

    row = _row(preset_name="p", scope_key="platform", state="applied")

    async def _capture(resource, **kwargs: Any):
        captured.update(kwargs)
        return row

    with patch("dynastore.modules.iam.applied_presets_service._q.MARK_APPLIED") as mock_q:
        mock_q.execute = _capture
        await svc.mark_applied("p", "platform", revoke_descriptor={"key": "val"})

    assert json.loads(captured["revoke_descriptor"]) == {"key": "val"}


@pytest.mark.asyncio
async def test_mark_failed_stores_error():
    svc, _ = _make_service()
    captured: Dict[str, Any] = {}
    row = _row(state="failed", last_error="boom")

    async def _capture(resource, **kwargs: Any):
        captured.update(kwargs)
        return row

    with patch("dynastore.modules.iam.applied_presets_service._q.MARK_FAILED") as mock_q:
        mock_q.execute = _capture
        await svc.mark_failed("p", "platform", last_error="boom")

    assert captured["last_error"] == "boom"


@pytest.mark.asyncio
async def test_mark_revoke_pending():
    svc, _ = _make_service()
    row = _row(state="revoke_pending")
    with patch("dynastore.modules.iam.applied_presets_service._q.MARK_REVOKE_PENDING") as mock_q:
        mock_q.execute = AsyncMock(return_value=row)
        result = await svc.mark_revoke_pending("p", "platform")
    assert result is not None
    assert result["state"] == "revoke_pending"


@pytest.mark.asyncio
async def test_mark_revoked():
    svc, _ = _make_service()
    row = _row(state="revoked")
    with patch("dynastore.modules.iam.applied_presets_service._q.MARK_REVOKED") as mock_q:
        mock_q.execute = AsyncMock(return_value=row)
        result = await svc.mark_revoked("p", "platform")
    assert result is not None
    assert result["state"] == "revoked"


@pytest.mark.asyncio
async def test_mark_partial_includes_child_info():
    svc, _ = _make_service()
    captured: Dict[str, Any] = {}
    row = _row(state="partial")

    async def _capture(resource, **kwargs: Any):
        captured.update(kwargs)
        return row

    with patch("dynastore.modules.iam.applied_presets_service._q.MARK_PARTIAL") as mock_q:
        mock_q.execute = _capture
        await svc.mark_partial("comp", "platform", child_name="child-a", child_error="timeout")

    assert "child-a" in captured["last_error"]
    assert "timeout" in captured["last_error"]


# ---------------------------------------------------------------------------
# list / pagination
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_list_returns_empty_when_no_rows():
    svc, _ = _make_service()
    with patch("dynastore.modules.iam.applied_presets_service._q.LIST_BY_SCOPE_PREFIX") as mock_q:
        mock_q.execute = AsyncMock(return_value=[])
        result = await svc.list(scope_key_prefix="platform")
    assert result == []


@pytest.mark.asyncio
async def test_list_with_cursor_uses_cursor_query():
    svc, _ = _make_service()
    with patch(
        "dynastore.modules.iam.applied_presets_service._q.LIST_BY_SCOPE_PREFIX_CURSOR"
    ) as mock_cursor_q, patch(
        "dynastore.modules.iam.applied_presets_service._q.LIST_BY_SCOPE_PREFIX"
    ) as mock_base_q:
        mock_cursor_q.execute = AsyncMock(return_value=[])
        mock_base_q.execute = AsyncMock(return_value=[])
        await svc.list(cursor="abc")
        mock_cursor_q.execute.assert_awaited_once()
        mock_base_q.execute.assert_not_awaited()
