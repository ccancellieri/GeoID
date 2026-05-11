"""Unit tests for ``capability_oracle.is_capability_live``.

Contract:
- Returns True when the sentinel key exists in the async cache backend.
- Returns False when the key is absent / expired.
- Returns True (fail-open) on any cache error or when no backend is
  registered — a missing oracle must never cause a false DLQ.
"""
from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from dynastore.modules.tasks.capability_oracle import (
    capability_key,
    is_capability_live,
)


@pytest.mark.asyncio
async def test_oracle_returns_true_when_key_present():
    backend = MagicMock()
    backend.exists = AsyncMock(return_value=True)
    mgr = MagicMock()
    mgr.get_async_backend = MagicMock(return_value=backend)
    with patch("dynastore.tools.cache.get_cache_manager", return_value=mgr):
        assert await is_capability_live("collection_elasticsearch_driver") is True
    backend.exists.assert_awaited_once_with(
        capability_key("collection_elasticsearch_driver"),
    )


@pytest.mark.asyncio
async def test_oracle_returns_false_when_key_absent():
    backend = MagicMock()
    backend.exists = AsyncMock(return_value=False)
    mgr = MagicMock()
    mgr.get_async_backend = MagicMock(return_value=backend)
    with patch("dynastore.tools.cache.get_cache_manager", return_value=mgr):
        assert await is_capability_live("missing_driver") is False


@pytest.mark.asyncio
async def test_oracle_fails_open_when_no_backend_registered():
    mgr = MagicMock()
    mgr.get_async_backend = MagicMock(
        side_effect=RuntimeError("No async cache backends registered"),
    )
    with patch("dynastore.tools.cache.get_cache_manager", return_value=mgr):
        assert await is_capability_live("anything") is True


@pytest.mark.asyncio
async def test_oracle_fails_open_on_cache_error():
    backend = MagicMock()
    backend.exists = AsyncMock(side_effect=ConnectionError("valkey down"))
    mgr = MagicMock()
    mgr.get_async_backend = MagicMock(return_value=backend)
    with patch("dynastore.tools.cache.get_cache_manager", return_value=mgr):
        assert await is_capability_live("anything") is True


def test_capability_key_shape():
    assert capability_key("foo") == "dynastore:caps:foo"
