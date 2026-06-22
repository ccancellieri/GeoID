#    Copyright 2026 FAO
#
#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at
#
#        http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS,
#    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#    See the License for the specific language governing permissions and
#    limitations under the License.
#
#    Author: Carlo Cancellieri (ccancellieri@gmail.com)
#    Company: FAO, Viale delle Terme di Caracalla, 00100 Rome, Italy
#    Contact: copyright@fao.org - http://fao.org/contact-us/terms/en/

"""State machine and CRUD tests for AppliedPresetsService using mock DB."""
from __future__ import annotations

from typing import Any, Dict
from unittest.mock import AsyncMock, MagicMock, patch

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


# ---------------------------------------------------------------------------
# delete_for_catalog
# ---------------------------------------------------------------------------

# All delete_for_catalog tests supply a mock connection so _write takes the
# ``conn is not None`` fast path and never attempts to open a real DB connection.

@pytest.mark.asyncio
async def test_delete_for_catalog_exact_scope():
    """delete_for_catalog passes exact scope key catalog:<id>."""
    svc, _ = _make_service()
    captured: Dict[str, Any] = {}
    mock_conn = MagicMock()

    async def _capture(resource, **kwargs: Any):
        captured.update(kwargs)
        return 1

    with patch(
        "dynastore.modules.iam.applied_presets_service._q.DELETE_BY_CATALOG_SCOPE"
    ) as mock_q:
        mock_q.execute = _capture
        await svc.delete_for_catalog("my-catalog", conn=mock_conn)

    assert captured["scope_key"] == "catalog:my-catalog"


@pytest.mark.asyncio
async def test_delete_for_catalog_descendant_prefix():
    """delete_for_catalog passes a LIKE prefix that covers collection descendants."""
    svc, _ = _make_service()
    captured: Dict[str, Any] = {}
    mock_conn = MagicMock()

    async def _capture(resource, **kwargs: Any):
        captured.update(kwargs)
        return 2

    with patch(
        "dynastore.modules.iam.applied_presets_service._q.DELETE_BY_CATALOG_SCOPE"
    ) as mock_q:
        mock_q.execute = _capture
        await svc.delete_for_catalog("my-catalog", conn=mock_conn)

    # The prefix must match catalog:my-catalog/<anything> but no sibling catalog.
    assert captured["scope_prefix"] == "catalog:my-catalog/%"


@pytest.mark.asyncio
async def test_delete_for_catalog_escapes_underscore():
    """Underscore in catalog_id is LIKE-escaped so it matches only itself.

    Without ESCAPE, ``catalog:my_catalog/%`` would let ``_`` act as a
    single-character wildcard and could match sibling catalogs whose id
    differs by exactly one character in that position.
    """
    svc, _ = _make_service()
    captured: Dict[str, Any] = {}
    mock_conn = MagicMock()

    async def _capture(resource, **kwargs: Any):
        captured.update(kwargs)
        return 1

    with patch(
        "dynastore.modules.iam.applied_presets_service._q.DELETE_BY_CATALOG_SCOPE"
    ) as mock_q:
        mock_q.execute = _capture
        await svc.delete_for_catalog("my_catalog", conn=mock_conn)

    # Underscore must be escaped so "my_catalog" does not act as a wildcard.
    assert captured["scope_key"] == "catalog:my_catalog"
    assert captured["scope_prefix"] == r"catalog:my\_catalog/%"


@pytest.mark.asyncio
async def test_delete_for_catalog_escapes_percent():
    """Percent in catalog_id is LIKE-escaped so it matches only itself."""
    svc, _ = _make_service()
    captured: Dict[str, Any] = {}
    mock_conn = MagicMock()

    async def _capture(resource, **kwargs: Any):
        captured.update(kwargs)
        return 1

    with patch(
        "dynastore.modules.iam.applied_presets_service._q.DELETE_BY_CATALOG_SCOPE"
    ) as mock_q:
        mock_q.execute = _capture
        await svc.delete_for_catalog("my%catalog", conn=mock_conn)

    assert captured["scope_key"] == "catalog:my%catalog"
    assert captured["scope_prefix"] == r"catalog:my\%catalog/%"


@pytest.mark.asyncio
async def test_delete_for_catalog_returns_rowcount():
    """delete_for_catalog returns the integer rowcount from the query."""
    svc, _ = _make_service()
    mock_conn = MagicMock()

    with patch(
        "dynastore.modules.iam.applied_presets_service._q.DELETE_BY_CATALOG_SCOPE"
    ) as mock_q:
        mock_q.execute = AsyncMock(return_value=3)
        result = await svc.delete_for_catalog("cat", conn=mock_conn)

    assert result == 3
