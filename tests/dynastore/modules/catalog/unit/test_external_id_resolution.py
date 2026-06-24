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

"""Unit tests for the external_id → internal id resolvers.

Contract:
- ``CatalogService.resolve_catalog_id(external_id)`` returns the internal
  ``id`` from the ``catalog.catalogs`` registry via the module-level
  ``_catalog_external_id_cache``.
- ``CollectionService.resolve_collection_id(catalog_id, external_id)``
  returns the internal ``id`` from ``{schema}.collections`` via the
  module-level ``_collection_external_id_cache``.
- Both raise ``ValueError`` on a miss unless ``allow_missing=True``.
- The caches are module-level (shared across service instances) so
  invalidation from one instance is visible to reads through another.
- ``BaseMetadata.external_id`` round-trips through Pydantic v2 with
  ``None`` as the default.
"""
from __future__ import annotations

import pytest

from dynastore.models.shared_models import Catalog, Collection
from dynastore.modules.catalog.catalog_service import (
    CatalogService,
    _catalog_external_id_cache,
    _invalidate_catalog_external_id_cache,
)
from dynastore.modules.catalog.collection_service import (
    CollectionService,
    _collection_external_id_cache,
    _invalidate_collection_external_id_cache,
)


# ---------------------------------------------------------------------------
# BaseMetadata.external_id field contract
# ---------------------------------------------------------------------------


def test_catalog_model_has_external_id_field():
    """BaseMetadata (via Catalog) must expose external_id as an Optional[str]."""
    assert "external_id" in Catalog.model_fields
    field = Catalog.model_fields["external_id"]
    assert field.default is None


def test_collection_model_has_external_id_field():
    """BaseMetadata (via Collection) must expose external_id as an Optional[str]."""
    assert "external_id" in Collection.model_fields
    field = Collection.model_fields["external_id"]
    assert field.default is None


def test_catalog_model_external_id_defaults_to_none():
    cat = Catalog.model_validate({"id": "c_test123"})
    assert cat.external_id is None


def test_catalog_model_external_id_round_trips():
    cat = Catalog.model_validate({"id": "c_test123", "external_id": "my-catalog"})
    assert cat.external_id == "my-catalog"


def test_collection_model_external_id_round_trips():
    col = Collection.model_validate({"id": "col_test456", "external_id": "my-collection"})
    assert col.external_id == "my-collection"


# ---------------------------------------------------------------------------
# CatalogService.resolve_catalog_id — happy path
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_resolve_catalog_id_returns_internal_id(monkeypatch):
    """resolve_catalog_id returns the internal id looked up from the registry."""
    _catalog_external_id_cache.cache_clear()
    svc = CatalogService.__new__(CatalogService)

    async def _registry(ext_id: str):
        return "c_internal001"

    monkeypatch.setattr(svc, "_get_catalog_id_by_external_id_db", _registry)

    result = await svc.resolve_catalog_id("my-catalog")
    assert result == "c_internal001"


# ---------------------------------------------------------------------------
# CatalogService.resolve_catalog_id — missing catalog
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_resolve_catalog_id_missing_raises_by_default(monkeypatch):
    """A missing external_id raises ValueError when allow_missing is False (default)."""
    _catalog_external_id_cache.cache_clear()
    svc = CatalogService.__new__(CatalogService)

    async def _absent(ext_id: str):
        return None

    monkeypatch.setattr(svc, "_get_catalog_id_by_external_id_db", _absent)

    with pytest.raises(ValueError, match="not found"):
        await svc.resolve_catalog_id("nonexistent-catalog")


@pytest.mark.asyncio
async def test_resolve_catalog_id_missing_returns_none_when_allowed(monkeypatch):
    """When allow_missing=True a miss returns None instead of raising."""
    _catalog_external_id_cache.cache_clear()
    svc = CatalogService.__new__(CatalogService)

    async def _absent(ext_id: str):
        return None

    monkeypatch.setattr(svc, "_get_catalog_id_by_external_id_db", _absent)

    result = await svc.resolve_catalog_id("nonexistent-catalog", allow_missing=True)
    assert result is None


# ---------------------------------------------------------------------------
# CatalogService cache — module-level sharing across instances
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_catalog_external_id_cache_is_shared_across_instances(monkeypatch):
    """The external_id cache is module-level; a write on svc1 is visible via svc2."""
    _catalog_external_id_cache.cache_clear()

    svc1 = CatalogService.__new__(CatalogService)
    svc2 = CatalogService.__new__(CatalogService)
    calls: list[str] = []

    async def _registry(ext_id: str):
        calls.append(ext_id)
        return "c_shared001"

    monkeypatch.setattr(svc1, "_get_catalog_id_by_external_id_db", _registry)
    monkeypatch.setattr(svc2, "_get_catalog_id_by_external_id_db", _registry)

    # First call goes to the DB.
    r1 = await svc1.resolve_catalog_id("shared-cat")
    # Second call on a different instance should hit the cache.
    r2 = await svc2.resolve_catalog_id("shared-cat")
    assert r1 == r2 == "c_shared001"
    assert len(calls) == 1, "DB should only be called once; second hit should use cache"


@pytest.mark.asyncio
async def test_catalog_external_id_cache_invalidation(monkeypatch):
    """Invalidating the cache causes the next read to hit the DB again."""
    _catalog_external_id_cache.cache_clear()
    svc = CatalogService.__new__(CatalogService)
    call_count = 0

    async def _registry(ext_id: str):
        nonlocal call_count
        call_count += 1
        return "c_reloaded001"

    monkeypatch.setattr(svc, "_get_catalog_id_by_external_id_db", _registry)

    await svc.resolve_catalog_id("cat-to-invalidate")
    assert call_count == 1

    _invalidate_catalog_external_id_cache("cat-to-invalidate")

    await svc.resolve_catalog_id("cat-to-invalidate")
    assert call_count == 2


# ---------------------------------------------------------------------------
# CollectionService.resolve_collection_id — happy path
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_resolve_collection_id_returns_internal_id(monkeypatch):
    """resolve_collection_id returns the internal id from the registry."""
    _collection_external_id_cache.cache_clear()
    svc = CollectionService.__new__(CollectionService)

    async def _registry(cat_id: str, ext_id: str):
        return "col_internal002"

    monkeypatch.setattr(svc, "_get_collection_id_by_external_id_db", _registry)

    result = await svc.resolve_collection_id("c_cat001", "my-collection")
    assert result == "col_internal002"


# ---------------------------------------------------------------------------
# CollectionService.resolve_collection_id — missing collection
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_resolve_collection_id_missing_raises_by_default(monkeypatch):
    """A missing external_id raises ValueError when allow_missing is False (default)."""
    _collection_external_id_cache.cache_clear()
    svc = CollectionService.__new__(CollectionService)

    async def _absent(cat_id: str, ext_id: str):
        return None

    monkeypatch.setattr(svc, "_get_collection_id_by_external_id_db", _absent)

    with pytest.raises(ValueError, match="not found"):
        await svc.resolve_collection_id("c_cat001", "nonexistent-col")


@pytest.mark.asyncio
async def test_resolve_collection_id_missing_returns_none_when_allowed(monkeypatch):
    """When allow_missing=True a miss returns None instead of raising."""
    _collection_external_id_cache.cache_clear()
    svc = CollectionService.__new__(CollectionService)

    async def _absent(cat_id: str, ext_id: str):
        return None

    monkeypatch.setattr(svc, "_get_collection_id_by_external_id_db", _absent)

    result = await svc.resolve_collection_id("c_cat001", "nonexistent-col", allow_missing=True)
    assert result is None


# ---------------------------------------------------------------------------
# CollectionService cache — module-level sharing across instances
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_collection_external_id_cache_is_shared_across_instances(monkeypatch):
    """The external_id cache is module-level; svc1 populates, svc2 reads from cache."""
    _collection_external_id_cache.cache_clear()

    svc1 = CollectionService.__new__(CollectionService)
    svc2 = CollectionService.__new__(CollectionService)
    calls: list[tuple[str, str]] = []

    async def _registry(cat_id: str, ext_id: str):
        calls.append((cat_id, ext_id))
        return "col_shared002"

    monkeypatch.setattr(svc1, "_get_collection_id_by_external_id_db", _registry)
    monkeypatch.setattr(svc2, "_get_collection_id_by_external_id_db", _registry)

    r1 = await svc1.resolve_collection_id("c_cat001", "shared-col")
    r2 = await svc2.resolve_collection_id("c_cat001", "shared-col")
    assert r1 == r2 == "col_shared002"
    assert len(calls) == 1


@pytest.mark.asyncio
async def test_collection_external_id_cache_invalidation(monkeypatch):
    """Invalidating the cache causes the next read to re-query the DB."""
    _collection_external_id_cache.cache_clear()
    svc = CollectionService.__new__(CollectionService)
    call_count = 0

    async def _registry(cat_id: str, ext_id: str):
        nonlocal call_count
        call_count += 1
        return "col_reloaded002"

    monkeypatch.setattr(svc, "_get_collection_id_by_external_id_db", _registry)

    await svc.resolve_collection_id("c_cat001", "col-to-invalidate")
    assert call_count == 1

    _invalidate_collection_external_id_cache("c_cat001", "col-to-invalidate")

    await svc.resolve_collection_id("c_cat001", "col-to-invalidate")
    assert call_count == 2


# ---------------------------------------------------------------------------
# Cross-collection isolation: different (catalog_id, external_id) pairs
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_collection_cache_isolates_by_catalog_and_external_id(monkeypatch):
    """Two collections with the same external_id in different catalogs must not collide."""
    _collection_external_id_cache.cache_clear()
    svc = CollectionService.__new__(CollectionService)

    async def _registry(cat_id: str, ext_id: str):
        return f"col_internal_{cat_id}"

    monkeypatch.setattr(svc, "_get_collection_id_by_external_id_db", _registry)

    r_a = await svc.resolve_collection_id("c_catA", "my-collection")
    r_b = await svc.resolve_collection_id("c_catB", "my-collection")
    assert r_a == "col_internal_c_catA"
    assert r_b == "col_internal_c_catB"
    assert r_a != r_b
