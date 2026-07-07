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

"""Unit tests for the single-boolean platform bootstrap guard.

All DB interactions are mocked — no real PostgreSQL required.
"""
from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from dynastore.modules.catalog.bootstrap_guard import (
    BOOTSTRAP_GUARD_KEY,
    is_initialized,
    mark_initialized,
)


# ---------------------------------------------------------------------------
# is_initialized
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_is_initialized_returns_false_when_protocol_missing() -> None:
    """When PropertiesProtocol is not registered, treat as uninitialised."""
    with patch("dynastore.tools.discovery.get_protocol", return_value=None):
        result = await is_initialized()
    assert result is False


@pytest.mark.asyncio
async def test_is_initialized_returns_false_when_property_absent() -> None:
    """Property row does not exist (None) → not initialised."""
    mock_props = AsyncMock()
    mock_props.get_property = AsyncMock(return_value=None)
    with patch("dynastore.tools.discovery.get_protocol", return_value=mock_props):
        result = await is_initialized()
    assert result is False
    mock_props.get_property.assert_awaited_once_with(BOOTSTRAP_GUARD_KEY, db_resource=None)


@pytest.mark.asyncio
async def test_is_initialized_returns_true_when_property_set() -> None:
    """Property value 'true' → initialised."""
    mock_props = AsyncMock()
    mock_props.get_property = AsyncMock(return_value="true")
    with patch("dynastore.tools.discovery.get_protocol", return_value=mock_props):
        result = await is_initialized()
    assert result is True


@pytest.mark.asyncio
async def test_is_initialized_returns_false_on_db_error() -> None:
    """DB errors degrade gracefully — treat as uninitialised so boot retries."""
    mock_props = AsyncMock()
    mock_props.get_property = AsyncMock(side_effect=RuntimeError("db gone"))
    with patch("dynastore.tools.discovery.get_protocol", return_value=mock_props):
        result = await is_initialized()
    assert result is False


@pytest.mark.asyncio
async def test_is_initialized_passes_db_resource() -> None:
    """db_resource is forwarded to get_property for transaction-aware reads."""
    mock_props = AsyncMock()
    mock_props.get_property = AsyncMock(return_value="true")
    sentinel_conn = object()
    with patch("dynastore.tools.discovery.get_protocol", return_value=mock_props):
        result = await is_initialized(db_resource=sentinel_conn)
    assert result is True
    mock_props.get_property.assert_awaited_once_with(BOOTSTRAP_GUARD_KEY, db_resource=sentinel_conn)


# ---------------------------------------------------------------------------
# is_initialized — early-boot direct fallback (PropertiesProtocol not yet up)
# ---------------------------------------------------------------------------


def _dqlquery_factory(*, value: object = None, error: BaseException | None = None):
    """Build a DQLQuery(...) replacement whose .execute resolves value/raises."""
    inst = MagicMock()
    if error is not None:
        inst.execute = AsyncMock(side_effect=error)
    else:
        inst.execute = AsyncMock(return_value=value)
    return MagicMock(return_value=inst), inst


@pytest.mark.asyncio
async def test_direct_fallback_true_when_marker_set() -> None:
    """Protocol absent but marker row reads 'true' → initialised (direct read)."""
    factory, inst = _dqlquery_factory(value="true")
    conn = object()
    with patch("dynastore.tools.discovery.get_protocol", return_value=None), patch(
        "dynastore.modules.db_config.query_executor.DQLQuery", factory
    ):
        result = await is_initialized(db_resource=conn)
    assert result is True
    inst.execute.assert_awaited_once()
    call = inst.execute.await_args
    assert call.args[0] is conn
    assert call.kwargs.get("key_name") == BOOTSTRAP_GUARD_KEY


@pytest.mark.asyncio
async def test_direct_fallback_false_when_marker_absent() -> None:
    """Protocol absent and marker row missing (None) → not initialised."""
    factory, _ = _dqlquery_factory(value=None)
    with patch("dynastore.tools.discovery.get_protocol", return_value=None), patch(
        "dynastore.modules.db_config.query_executor.DQLQuery", factory
    ):
        result = await is_initialized(db_resource=object())
    assert result is False


@pytest.mark.asyncio
async def test_direct_fallback_false_on_db_error() -> None:
    """Protocol absent and the direct read raises (e.g. table missing on a fresh
    DB) → degrade to not-initialised so the caller bootstraps."""
    factory, _ = _dqlquery_factory(error=RuntimeError("relation does not exist"))
    with patch("dynastore.tools.discovery.get_protocol", return_value=None), patch(
        "dynastore.modules.db_config.query_executor.DQLQuery", factory
    ):
        result = await is_initialized(db_resource=object())
    assert result is False


@pytest.mark.asyncio
async def test_direct_fallback_false_when_no_resource() -> None:
    """Protocol absent AND no db_resource → not initialised, no query attempted."""
    factory, inst = _dqlquery_factory(value="true")
    with patch("dynastore.tools.discovery.get_protocol", return_value=None), patch(
        "dynastore.modules.db_config.query_executor.DQLQuery", factory
    ):
        result = await is_initialized()
    assert result is False
    inst.execute.assert_not_awaited()


# ---------------------------------------------------------------------------
# mark_initialized
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_mark_initialized_writes_true() -> None:
    """mark_initialized sets the property value to 'true'."""
    mock_props = AsyncMock()
    mock_props.set_property = AsyncMock(return_value=1)
    with patch("dynastore.tools.discovery.get_protocol", return_value=mock_props):
        await mark_initialized()
    mock_props.set_property.assert_awaited_once()
    args = mock_props.set_property.await_args
    assert args.args[0] == BOOTSTRAP_GUARD_KEY
    assert args.args[1] == "true"


@pytest.mark.asyncio
async def test_mark_initialized_raises_when_protocol_missing() -> None:
    """RuntimeError when PropertiesProtocol not registered — caller must not silence it."""
    with patch("dynastore.tools.discovery.get_protocol", return_value=None):
        with pytest.raises(RuntimeError, match="PropertiesProtocol not registered"):
            await mark_initialized()


@pytest.mark.asyncio
async def test_mark_initialized_passes_db_resource() -> None:
    """db_resource is forwarded to set_property for transaction-aware writes."""
    mock_props = AsyncMock()
    mock_props.set_property = AsyncMock(return_value=1)
    sentinel_conn = object()
    with patch("dynastore.tools.discovery.get_protocol", return_value=mock_props):
        await mark_initialized(db_resource=sentinel_conn)
    _, kwargs = mock_props.set_property.await_args
    assert kwargs.get("db_resource") is sentinel_conn
