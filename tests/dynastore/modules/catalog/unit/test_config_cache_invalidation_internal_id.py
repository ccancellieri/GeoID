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

"""Unit tests: _collection_config_cache is invalidated under the internal id.

Issue #2430 — when external_id != internal_id (e.g. after a collection rename),
the four write/delete paths used to call cache_invalidate with the external id.
That left the cache entry keyed on the internal id (the one populated by
get_collection_config_internal_cached) alive until TTL expiry (~300 s).

These tests verify that after a set or delete, the invalidation arg matches
the internal_collection_id, not the original external id passed by the caller.
"""

from __future__ import annotations

from contextlib import asynccontextmanager
from unittest.mock import AsyncMock, MagicMock

import pytest

from dynastore.models.resolved_ids import ResolvedCollectionIds

_CATALOG_ID = "cat_test"
_EXTERNAL_ID = "my_collection"       # what the REST caller passes
_INTERNAL_ID = "col_abc1234xyz5678"  # what resolve_collection_ids returns


def _resolved() -> ResolvedCollectionIds:
    return ResolvedCollectionIds(
        id=_INTERNAL_ID,
        external_id=_EXTERNAL_ID,
        catalog_id=_CATALOG_ID,
    )


def _make_service(mock_catalogs):
    """Minimal ConfigService wired to mock_catalogs — no DB, no registry."""
    from dynastore.modules.catalog.config_service import ConfigService
    return ConfigService(engine=MagicMock(), catalog_manager=mock_catalogs)


# ---------------------------------------------------------------------------
# _delete_collection_config
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_delete_collection_config_invalidates_internal_id(monkeypatch):
    """_delete_collection_config must bust the cache under internal_collection_id."""
    import dynastore.modules.catalog.config_service as svc_mod
    from dynastore.modules.tiles.tiles_config import TilesConfig

    mock_catalogs = MagicMock()
    mock_catalogs.collections.resolve_collection_ids = AsyncMock(return_value=_resolved())
    mock_catalogs.resolve_physical_schema = AsyncMock(return_value="phys_test")

    svc = _make_service(mock_catalogs)

    @asynccontextmanager
    async def _fake_txn(_engine):
        yield MagicMock()

    monkeypatch.setattr(svc_mod, "managed_transaction", _fake_txn)
    monkeypatch.setattr(svc_mod, "DriverContext", MagicMock)
    monkeypatch.setattr(svc_mod, "check_table_exists", AsyncMock(return_value=True))

    mock_delete_query = MagicMock()
    mock_delete_query.return_value.execute = AsyncMock(return_value=1)
    monkeypatch.setattr(svc_mod._cq, "delete_collection_config", mock_delete_query)

    invalidate_calls: list = []
    monkeypatch.setattr(
        svc_mod._collection_config_cache,
        "cache_invalidate",
        lambda *args, **kwargs: invalidate_calls.append(args),
    )

    result = await svc._delete_collection_config(_CATALOG_ID, _EXTERNAL_ID, TilesConfig)

    assert result is True
    assert invalidate_calls, "cache_invalidate was never called"
    # Positional args to cache_invalidate:
    # (engine, catalog_manager, catalog_id, collection_id, class_key)
    invalidated_collection_id = invalidate_calls[0][3]
    assert invalidated_collection_id == _INTERNAL_ID, (
        f"Cache must be invalidated under internal id {_INTERNAL_ID!r}; "
        f"got {invalidated_collection_id!r}. Stale entry survives until TTL expiry."
    )


# ---------------------------------------------------------------------------
# _set_collection_config
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_set_collection_config_invalidates_internal_id(monkeypatch):
    """_set_collection_config must bust the cache under internal_collection_id."""
    import dynastore.modules.catalog.config_service as svc_mod
    from dynastore.modules.tiles.tiles_config import TilesConfig

    mock_catalogs = MagicMock()
    mock_catalogs.collections.resolve_collection_ids = AsyncMock(return_value=_resolved())
    mock_catalogs.ensure_collection_exists = AsyncMock()
    mock_catalogs.ensure_catalog_exists = AsyncMock()
    mock_catalogs.resolve_physical_schema = AsyncMock(return_value="phys_test")

    svc = _make_service(mock_catalogs)

    @asynccontextmanager
    async def _fake_txn(_engine):
        yield MagicMock()

    monkeypatch.setattr(svc_mod, "managed_transaction", _fake_txn)
    monkeypatch.setattr(svc_mod, "DriverContext", MagicMock)
    monkeypatch.setattr(svc_mod, "run_validate_handlers", AsyncMock())
    monkeypatch.setattr(svc_mod, "run_apply_handlers", AsyncMock())

    mock_upsert_query = MagicMock()
    mock_upsert_query.return_value.execute = AsyncMock()
    monkeypatch.setattr(svc_mod._cq, "upsert_collection_config", mock_upsert_query)

    invalidate_calls: list = []
    monkeypatch.setattr(
        svc_mod._collection_config_cache,
        "cache_invalidate",
        lambda *args, **kwargs: invalidate_calls.append(args),
    )

    # check_immutability=False skips the select_collection_config_for_update call
    await svc._set_collection_config(
        _CATALOG_ID,
        _EXTERNAL_ID,
        TilesConfig,
        TilesConfig(enabled=True),
        check_immutability=False,
    )

    assert invalidate_calls, "cache_invalidate was never called"
    # Positional args to cache_invalidate:
    # (engine, catalogs, catalog_id, collection_id, class_key)
    invalidated_collection_id = invalidate_calls[0][3]
    assert invalidated_collection_id == _INTERNAL_ID, (
        f"Cache must be invalidated under internal id {_INTERNAL_ID!r}; "
        f"got {invalidated_collection_id!r}. Stale entry survives until TTL expiry."
    )
