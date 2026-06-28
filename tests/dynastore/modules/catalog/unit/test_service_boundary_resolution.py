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

"""Phase 2 unit tests: public methods resolve external→internal id at the boundary.

Contract verified:
- CatalogService.get_catalog(external_id) resolves to internal id before
  calling get_catalog_model(internal_id).
- CatalogService.get_catalog raises ValueError on an unknown external_id
  (same error shape as a genuinely missing catalog).
- CatalogService.delete_catalog returns False (not raises) for an unknown
  external_id.
- CollectionService.get_collection resolves both catalog and collection
  external ids before calling get_collection_model(internal, internal).
- CollectionService.get_collection returns None for an unknown external_id
  without raising.
- No internal path re-resolves an already-internal id (no double-resolution
  where an internal id is passed directly to get_catalog_model).
"""
from __future__ import annotations

import pytest
from unittest.mock import AsyncMock, MagicMock, patch

from dynastore.modules.catalog.catalog_service import (
    CatalogService,
    _catalog_external_id_cache,
)
from dynastore.modules.catalog.collection_service import (
    CollectionService,
    _collection_external_id_cache,
)
from dynastore.modules.catalog.models import Catalog, Collection


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_catalog_service(
    external_to_internal: dict[str, str | None],
    model_by_internal: dict[str, Catalog | None],
) -> CatalogService:
    """Build a CatalogService stub with patched resolve and get_catalog_model."""
    svc = CatalogService.__new__(CatalogService)

    async def _resolve(ext_id: str, allow_missing: bool = False):
        result = external_to_internal.get(ext_id)
        if result is None and not allow_missing:
            raise ValueError(f"Catalog '{ext_id}' not found.")
        return result

    svc.resolve_catalog_id = _resolve  # type: ignore[assignment]

    async def _get_model(int_id: str, ctx=None, hints=frozenset()):
        return model_by_internal.get(int_id)

    svc.get_catalog_model = _get_model  # type: ignore[assignment]

    return svc


def _make_collection_service(
    cat_internal_map: dict[str, str | None],
    col_internal_map: dict[tuple[str, str], str | None],
    model_by_internal: dict[tuple[str, str], Collection | None],
) -> CollectionService:
    """Build a CollectionService stub."""
    svc = CollectionService.__new__(CollectionService)

    async def _resolve_col(cat_id: str, ext_col_id: str, allow_missing: bool = False):
        result = col_internal_map.get((cat_id, ext_col_id))
        if result is None and not allow_missing:
            raise ValueError(f"Collection '{ext_col_id}' not found in catalog '{cat_id}'.")
        return result

    svc.resolve_collection_id = _resolve_col  # type: ignore[assignment]

    async def _get_model(cat_id: str, col_id: str, db_resource=None, hints=frozenset()):
        return model_by_internal.get((cat_id, col_id))

    svc.get_collection_model = _get_model  # type: ignore[assignment]

    return svc


# ===========================================================================
# CatalogService.get_catalog — happy path
# ===========================================================================


@pytest.mark.asyncio
async def test_get_catalog_resolves_external_to_internal(monkeypatch):
    """get_catalog passes the INTERNAL id to get_catalog_model, not the external one."""
    _catalog_external_id_cache.cache_clear()

    catalog_model = Catalog.model_validate({"id": "c_internal01"})
    svc = CatalogService.__new__(CatalogService)

    resolve_calls: list[str] = []
    model_calls: list[str] = []

    async def _resolve(ext_id: str, allow_missing: bool = False):
        resolve_calls.append(ext_id)
        return "c_internal01"

    async def _get_model(int_id: str, ctx=None, hints=frozenset()):
        model_calls.append(int_id)
        return catalog_model

    svc.resolve_catalog_id = _resolve  # type: ignore[assignment]
    svc.get_catalog_model = _get_model  # type: ignore[assignment]

    # Patch out the visibility resolver (IAM off → None means no filter).
    monkeypatch.setattr(
        "dynastore.modules.catalog.catalog_service.resolve_catalog_listing_ids",
        AsyncMock(return_value=None),
        raising=False,
    )

    # Patch the import inside the method
    with patch(
        "dynastore.modules.catalog.catalog_service.resolve_catalog_listing_ids",
        AsyncMock(return_value=None),
    ):
        result = await svc.get_catalog("my-catalog")

    assert result is catalog_model
    assert resolve_calls == ["my-catalog"], "resolve_catalog_id must be called with the external id"
    assert model_calls == ["c_internal01"], "get_catalog_model must be called with the internal id"


@pytest.mark.asyncio
async def test_get_catalog_raises_on_unknown_external_id():
    """get_catalog raises ValueError when the external_id has no registry entry
    AND the catalog does not exist by its id.

    Phase 2 uses allow_missing=True + passthrough: when resolve_catalog_id
    returns None the original id is used unchanged, and get_catalog_model
    returning None triggers the ValueError.  The error shape is the same as
    a genuinely missing catalog.
    """
    svc = CatalogService.__new__(CatalogService)

    async def _resolve(ext_id: str, allow_missing: bool = False):
        # No external_id mapping → passthrough (allow_missing always True in get_catalog).
        return None

    async def _get_model(int_id: str, ctx=None, hints=frozenset()):
        return None  # catalog does not exist

    svc.resolve_catalog_id = _resolve  # type: ignore[assignment]
    svc.get_catalog_model = _get_model  # type: ignore[assignment]

    with patch(
        "dynastore.models.protocols.visibility.resolve_catalog_listing_ids",
        AsyncMock(return_value=None),
    ):
        with pytest.raises(ValueError, match="not found"):
            await svc.get_catalog("nonexistent-catalog")


# ===========================================================================
# CatalogService.delete_catalog — missing external_id returns False
# ===========================================================================


@pytest.mark.asyncio
async def test_delete_catalog_returns_false_for_unknown_external_id():
    """delete_catalog returns False (not raises) when the external_id is missing.

    The original implementation performed a soft-delete UPDATE that returned 0
    rows for an unknown id; Phase 2 preserves that semantics via resolve +
    allow_missing=True. The tombstone probe must also return None for an
    external_id that was never created (distinguishes 'never existed' → 404
    from 'already tombstoned' → 204 idempotent).
    """
    svc = CatalogService.__new__(CatalogService)

    async def _resolve(ext_id: str, allow_missing: bool = False):
        return None  # Unknown external_id — never in the active registry

    async def _no_tombstone(ext_id: str):
        return None  # Also not tombstoned — never existed

    svc.resolve_catalog_id = _resolve  # type: ignore[assignment]
    svc._get_tombstoned_catalog_id_by_external_id_db = _no_tombstone  # type: ignore[assignment]

    result = await svc.delete_catalog("nonexistent-catalog")
    assert result is False


# ===========================================================================
# CatalogService — internal callers do NOT double-resolve
# ===========================================================================


@pytest.mark.asyncio
async def test_get_catalog_model_not_called_for_external_id():
    """get_catalog_model accepts internal ids — no resolution occurs inside it.

    This guards against accidental double-resolution: an internal caller that
    already resolved catalog_id and calls get_catalog_model(internal_id) must
    not trigger another resolve_catalog_id call.
    """
    catalog_model = Catalog.model_validate({"id": "c_internal99"})
    svc = CatalogService.__new__(CatalogService)

    resolve_calls: list[str] = []

    async def _db_resolve(ext_id: str):
        resolve_calls.append(ext_id)
        return None  # Simulates "not found as external_id" — internal id path

    svc._get_catalog_id_by_external_id_db = _db_resolve  # type: ignore[assignment]

    async def _get_model_db(int_id: str):
        return catalog_model

    svc._get_catalog_model_db = _get_model_db  # type: ignore[assignment]

    _catalog_external_id_cache.cache_clear()

    # Call get_catalog_model directly with an internal id (no resolution expected).
    result = await svc.get_catalog_model("c_internal99")

    # _get_catalog_model_db is called (via the cache miss path), but
    # _get_catalog_id_by_external_id_db (the resolve path) is only called via
    # the _catalog_external_id_cache — which get_catalog_model does NOT call.
    assert result is catalog_model
    assert resolve_calls == [], "get_catalog_model must not call the external_id resolver"


# ===========================================================================
# CollectionService.get_collection — happy path
# ===========================================================================


@pytest.mark.asyncio
async def test_get_collection_resolves_both_ids(monkeypatch):
    """get_collection resolves catalog AND collection external ids before querying."""
    _collection_external_id_cache.cache_clear()

    col_model = Collection.model_validate({"id": "col_int002"})

    svc = CollectionService.__new__(CollectionService)

    col_resolve_calls: list[tuple] = []

    async def _resolve_col(cat_id: str, ext_col: str, allow_missing: bool = False):
        col_resolve_calls.append((cat_id, ext_col))
        return "col_int002"

    model_calls: list[tuple] = []

    async def _get_model(cat_id: str, col_id: str, db_resource=None, hints=frozenset()):
        model_calls.append((cat_id, col_id))
        return col_model

    svc.resolve_collection_id = _resolve_col  # type: ignore[assignment]
    svc.get_collection_model = _get_model  # type: ignore[assignment]

    # Mock the catalog protocol resolution.
    fake_catalogs = MagicMock()
    fake_catalogs.resolve_catalog_id = AsyncMock(return_value="c_int001")

    def _get_protocol_side_effect(protocol_type):
        from dynastore.models.protocols import CatalogsProtocol as _CP

        if protocol_type is _CP:
            return fake_catalogs
        return None  # No localization service in this test

    with patch(
        "dynastore.modules.catalog.collection_service.get_protocol",
        side_effect=_get_protocol_side_effect,
    ):
        result = await svc.get_collection("my-catalog", "my-collection")

    assert result is col_model
    # Collection resolver was called with the INTERNAL catalog id.
    assert col_resolve_calls == [("c_int001", "my-collection")], (
        "resolve_collection_id must receive the already-resolved internal catalog id"
    )
    # get_collection_model was called with both internal ids.
    assert model_calls == [("c_int001", "col_int002")], (
        "get_collection_model must receive both internal ids"
    )


@pytest.mark.asyncio
async def test_get_collection_returns_none_for_unknown_catalog(monkeypatch):
    """get_collection returns None when the catalog external_id has no mapping
    AND the catalog/collection genuinely does not exist.

    Phase 2 passthrough: when resolve_catalog_id returns None the original
    catalog_id is used unchanged; get_collection_model returning None yields
    the None result.
    """
    svc = CollectionService.__new__(CollectionService)

    fake_catalogs = MagicMock()
    fake_catalogs.resolve_catalog_id = AsyncMock(return_value=None)

    async def _resolve_col(cat_id: str, ext_col: str, allow_missing: bool = False):
        return None  # No collection mapping either

    async def _get_model(cat_id: str, col_id: str, db_resource=None, hints=frozenset()):
        return None  # Collection does not exist

    svc.resolve_collection_id = _resolve_col  # type: ignore[assignment]
    svc.get_collection_model = _get_model  # type: ignore[assignment]

    def _get_protocol_side_effect(protocol_type):
        from dynastore.models.protocols import CatalogsProtocol as _CP
        if protocol_type is _CP:
            return fake_catalogs
        return None

    with patch(
        "dynastore.modules.catalog.collection_service.get_protocol",
        side_effect=_get_protocol_side_effect,
    ):
        result = await svc.get_collection("nonexistent-catalog", "some-collection")

    assert result is None


@pytest.mark.asyncio
async def test_get_collection_returns_none_for_unknown_collection(monkeypatch):
    """get_collection returns None when the collection external_id has no mapping
    AND the collection genuinely does not exist.

    Phase 2 passthrough: resolve_collection_id returning None means the original
    collection_id is used unchanged; get_collection_model returning None yields None.
    """
    svc = CollectionService.__new__(CollectionService)

    async def _resolve_col(cat_id: str, ext_col: str, allow_missing: bool = False):
        return None  # No collection external_id mapping → passthrough

    async def _get_model(cat_id: str, col_id: str, db_resource=None, hints=frozenset()):
        return None  # Collection does not exist

    svc.resolve_collection_id = _resolve_col  # type: ignore[assignment]
    svc.get_collection_model = _get_model  # type: ignore[assignment]

    fake_catalogs = MagicMock()
    fake_catalogs.resolve_catalog_id = AsyncMock(return_value="c_int001")

    def _get_protocol_side_effect(protocol_type):
        from dynastore.models.protocols import CatalogsProtocol as _CP
        if protocol_type is _CP:
            return fake_catalogs
        return None

    with patch(
        "dynastore.modules.catalog.collection_service.get_protocol",
        side_effect=_get_protocol_side_effect,
    ):
        result = await svc.get_collection("my-catalog", "nonexistent-collection")

    assert result is None


# ===========================================================================
# CatalogService.resolve_physical_schema — dual-mode (external + internal)
# ===========================================================================


@pytest.mark.asyncio
async def test_resolve_physical_schema_accepts_external_id(monkeypatch):
    """resolve_physical_schema resolves external catalog_id → internal before DB lookup."""
    svc = CatalogService.__new__(CatalogService)

    # Simulate cache: external_id → internal_id lookup
    async def _db_external(ext_id: str):
        if ext_id == "my-catalog":
            return "c_int003"
        return None

    svc._get_catalog_id_by_external_id_db = _db_external  # type: ignore[assignment]

    # Simulate physical schema lookup via internal id
    async def _db_schema(int_id: str):
        if int_id == "c_int003":
            return "s_xyz123"
        return None

    svc._get_physical_schema_db = _db_schema  # type: ignore[assignment]

    _catalog_external_id_cache.cache_clear()
    from dynastore.modules.catalog.catalog_service import _physical_schema_cache

    _physical_schema_cache.cache_clear()

    # resolve_physical_schema with external id → should find the schema
    result = await svc.resolve_physical_schema("my-catalog", allow_missing=True)
    assert result == "s_xyz123"


@pytest.mark.asyncio
async def test_resolve_physical_schema_passthrough_internal_id(monkeypatch):
    """resolve_physical_schema passes through an already-internal id when not in
    the external_id cache (no double-resolution).
    """
    svc = CatalogService.__new__(CatalogService)

    # The external_id → internal lookup returns None (internal id not in external_id col)
    async def _db_external(ext_id: str):
        return None

    svc._get_catalog_id_by_external_id_db = _db_external  # type: ignore[assignment]

    schema_calls: list[str] = []

    async def _db_schema(int_id: str):
        schema_calls.append(int_id)
        if int_id == "c_alreadyinternal":
            return "s_abc999"
        return None

    svc._get_physical_schema_db = _db_schema  # type: ignore[assignment]

    _catalog_external_id_cache.cache_clear()
    from dynastore.modules.catalog.catalog_service import _physical_schema_cache

    _physical_schema_cache.cache_clear()

    result = await svc.resolve_physical_schema("c_alreadyinternal", allow_missing=True)
    assert result == "s_abc999"
    assert schema_calls == ["c_alreadyinternal"], (
        "An already-internal id must be passed straight through to the schema lookup"
    )


@pytest.mark.asyncio
async def test_resolve_physical_schema_internal_first_beats_external_collision():
    """An already-internal catalog id must resolve to ITS OWN schema even when a
    DIFFERENT catalog's ``external_id`` collides with that id.

    Regression: the old external-first order resolved ``catalog_id`` as an
    external_id BEFORE checking it as an internal id.  When a legacy catalog
    carries a ``c_…``-shaped external_id equal to another catalog's internal id
    (observed on dev), an already-resolved internal id passed to create_collection
    got hijacked to the colliding catalog's schema, scattering registry rows into
    the wrong schema (write-gate then reports the collection ``missing``).
    Internal-first must take the authoritative PK hit and never consult the
    external resolver.
    """
    svc = CatalogService.__new__(CatalogService)

    external_lookups: list[str] = []

    async def _db_external(ext_id: str):
        external_lookups.append(ext_id)
        # Pollution: a different catalog carries external_id == 'c_victim'.
        if ext_id == "c_victim":
            return "c_collider"
        return None

    svc._get_catalog_id_by_external_id_db = _db_external  # type: ignore[assignment]

    async def _db_schema(int_id: str):
        # id IS the schema; both catalogs exist as schemas.
        if int_id in ("c_victim", "c_collider"):
            return int_id
        return None

    svc._get_physical_schema_db = _db_schema  # type: ignore[assignment]

    _catalog_external_id_cache.cache_clear()
    from dynastore.modules.catalog.catalog_service import _physical_schema_cache

    _physical_schema_cache.cache_clear()

    result = await svc.resolve_physical_schema("c_victim", allow_missing=True)
    assert result == "c_victim", (
        "internal id must resolve to its own schema, not the colliding "
        f"external_id owner; got {result!r}"
    )
    assert external_lookups == [], (
        "a direct internal-id hit must short-circuit before the external resolver "
        f"(external lookups: {external_lookups})"
    )
