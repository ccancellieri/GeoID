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

Also covers #2895: the config caches must not remember a negative (``None``)
lookup, must key on the internal *catalog* id consistently between read and
write, and must be dropped on catalog delete/reclaim.
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
    # Already-internal catalog_id: resolve_catalog_id is external-only and
    # returns None for it (see ConfigService._internal_catalog_id).
    mock_catalogs.resolve_catalog_id = AsyncMock(return_value=None)

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
# #2895 (i) — a miss must not be cached
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_negative_catalog_config_lookup_not_cached(monkeypatch):
    """A ``None`` result (catalog not yet provisioned) must not survive to the
    next call even without an explicit invalidation in between — otherwise a
    catalog that becomes resolvable right after the miss keeps serving the
    cached ``None`` for the L1 TTL window (#2895 i).
    """
    import dynastore.modules.catalog.config_service as svc_mod
    from dynastore.modules.tiles.tiles_config import TilesConfig

    mock_catalogs = MagicMock()
    mock_catalogs.resolve_catalog_id = AsyncMock(return_value=None)
    # First call: not provisioned yet (no physical schema). Second call: ready.
    mock_catalogs.resolve_physical_schema = AsyncMock(side_effect=[None, "phys_test"])

    svc = _make_service(mock_catalogs)

    @asynccontextmanager
    async def _fake_txn(_engine):
        yield MagicMock()

    monkeypatch.setattr(svc_mod, "managed_transaction", _fake_txn)
    monkeypatch.setattr(svc_mod, "DriverContext", MagicMock)
    monkeypatch.setattr(svc_mod, "check_table_exists", AsyncMock(return_value=True))

    mock_select_query = MagicMock()
    mock_select_query.return_value.execute = AsyncMock(
        return_value={"class_key": TilesConfig.class_key(), "config_data": {"enabled": True}}
    )
    monkeypatch.setattr(svc_mod._cq, "select_catalog_config", mock_select_query)

    class_key = TilesConfig.class_key()

    miss = await svc.get_catalog_config_internal_cached(_CATALOG_ID, class_key)
    assert miss is None

    hit = await svc.get_catalog_config_internal_cached(_CATALOG_ID, class_key)
    assert hit is not None, (
        "Negative result was cached despite the catalog now resolving — "
        "condition=lambda v: v is not None regressed (#2895 i)."
    )


# ---------------------------------------------------------------------------
# #2895 (ii) — read and write invalidation must key on the same catalog id
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_catalog_config_read_and_write_normalize_same_external_id(monkeypatch):
    """``get_catalog_config_internal_cached`` (read) and ``_set_catalog_config``
    (write invalidation) both route ``catalog_id`` through the same
    ``ConfigService._internal_catalog_id`` normalization, so an external-keyed
    read and set_config's invalidation agree on the same cache key (#2895 ii).
    """
    import dynastore.modules.catalog.config_service as svc_mod
    from dynastore.modules.tiles.tiles_config import TilesConfig

    external_catalog_id = "my_catalog"
    internal_catalog_id = "c_abc123"

    mock_catalogs = MagicMock()
    mock_catalogs.resolve_catalog_id = AsyncMock(return_value=internal_catalog_id)
    mock_catalogs.resolve_physical_schema = AsyncMock(return_value="phys_test")
    mock_catalogs.ensure_catalog_exists = AsyncMock()

    svc = _make_service(mock_catalogs)

    @asynccontextmanager
    async def _fake_txn(_engine):
        yield MagicMock()

    monkeypatch.setattr(svc_mod, "managed_transaction", _fake_txn)
    monkeypatch.setattr(svc_mod, "DriverContext", MagicMock)
    monkeypatch.setattr(svc_mod, "check_table_exists", AsyncMock(return_value=False))
    monkeypatch.setattr(svc_mod, "run_validate_handlers", AsyncMock())
    monkeypatch.setattr(svc_mod, "run_apply_handlers", AsyncMock())

    mock_upsert_query = MagicMock()
    mock_upsert_query.return_value.execute = AsyncMock()
    monkeypatch.setattr(svc_mod._cq, "upsert_catalog_config", mock_upsert_query)

    invalidate_calls: list = []
    monkeypatch.setattr(
        svc_mod._catalog_config_cache,
        "cache_invalidate",
        lambda *args, **kwargs: invalidate_calls.append(args),
    )

    resolved_ids: list = []
    real_internal_catalog_id = svc._internal_catalog_id

    async def _spy_internal_catalog_id(catalog_id):
        result = await real_internal_catalog_id(catalog_id)
        resolved_ids.append((catalog_id, result))
        return result

    monkeypatch.setattr(svc, "_internal_catalog_id", _spy_internal_catalog_id)

    class_key = TilesConfig.class_key()

    await svc.get_catalog_config_internal_cached(external_catalog_id, class_key)
    await svc._set_catalog_config(
        external_catalog_id, TilesConfig, TilesConfig(enabled=True),
        check_immutability=False,
    )

    assert resolved_ids == [
        (external_catalog_id, internal_catalog_id),
        (external_catalog_id, internal_catalog_id),
    ], "read and write must normalize the same external id to the same internal id"

    assert invalidate_calls, "cache_invalidate was never called"
    invalidated_catalog_id = invalidate_calls[0][2]
    assert invalidated_catalog_id == internal_catalog_id, (
        f"Write invalidation must clear the internal-keyed entry {internal_catalog_id!r} "
        f"that the external-keyed read populated; got {invalidated_catalog_id!r}."
    )


# ---------------------------------------------------------------------------
# #2895 (iii) — catalog delete drops the config caches
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_delete_catalog_invalidates_config_caches(monkeypatch):
    """Soft-deleting a catalog must drop its config-cache entries so a
    reclaimed external_id cannot serve a stale config (#2895 iii)."""
    import dynastore.modules.catalog.catalog_service as cat_svc_mod
    import dynastore.modules.catalog.config_service as cfg_svc_mod
    from dynastore.modules.catalog.catalog_service import CatalogService

    internal_id = "c_abc123"

    @asynccontextmanager
    async def _txn(_engine):
        yield MagicMock()

    monkeypatch.setattr(cat_svc_mod, "managed_transaction", _txn)
    monkeypatch.setattr(cat_svc_mod, "get_catalog_engine", lambda *_a, **_kw: MagicMock())
    monkeypatch.setattr(
        cat_svc_mod._soft_delete_catalog_query, "execute", AsyncMock(return_value=1)
    )
    monkeypatch.setattr(cat_svc_mod, "emit_event", AsyncMock())
    monkeypatch.setattr(cat_svc_mod, "_invalidate_catalog_external_id_cache", MagicMock())
    monkeypatch.setattr(cat_svc_mod, "_invalidate_catalog_model_cache", MagicMock())

    invalidated: list = []
    monkeypatch.setattr(
        cfg_svc_mod, "invalidate_catalog_config_caches", lambda cid: invalidated.append(cid)
    )

    svc = CatalogService(engine=MagicMock())
    svc.resolve_catalog_id = AsyncMock(return_value=internal_id)

    result = await svc.delete_catalog("external-cat", force=False)

    assert result is True
    assert invalidated == [internal_id]


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
    mock_catalogs.resolve_catalog_id = AsyncMock(return_value=None)

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
