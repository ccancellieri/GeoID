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

"""Regression coverage: ``CollectionService.ensure_collection_exists`` must be
idempotent.

The bulk-ingestion task funnels its just-in-time collection creation through
``catalog_module.ensure_collection_exists`` → ``CollectionService.
ensure_collection_exists`` (db_resource-first signature, bypassing the
CatalogService wrapper).  That method does a check-then-create: an existence
SELECT via ``resolve_collection_id`` followed by ``create_collection``.

Two ways the SELECT can miss a collection the INSERT then collides with on the
``collections_external_uq`` unique index:

1. catalog_id form mismatch — the existence check resolved the physical schema
   from the raw (external) catalog_id while ``create_collection`` resolved it to
   the internal id first, so the two targeted different schemas.
2. TOCTOU / task retry — a concurrent or retried caller created the collection
   between our SELECT and INSERT.

Ingestion tasks retry on failure, so a non-idempotent create here fails the
whole job on the second attempt with a duplicate-key error. ``ensure_*`` must
treat the conflict as success.  The explicit ``create_collection`` API path is
unchanged and still raises (HTTP 409) on a genuine user-initiated duplicate.
"""

from __future__ import annotations

import inspect
from unittest.mock import AsyncMock, MagicMock

import pytest

from dynastore.modules.catalog import collection_service as collection_service_mod
from dynastore.modules.catalog.collection_service import CollectionService
from dynastore.modules.db_config.exceptions import UniqueViolationError


# ---------------------------------------------------------------------------
# Source-shape: cheap, deterministic regression guard
# ---------------------------------------------------------------------------


def _ensure_source() -> str:
    return inspect.getsource(CollectionService.ensure_collection_exists)


def test_ensure_collection_exists_source_guards_unique_violation() -> None:
    src = _ensure_source()
    assert "UniqueViolationError" in src, (
        "ensure_collection_exists no longer catches UniqueViolationError — a "
        "retried ingestion task will fail the whole job on a duplicate-key "
        "(collections_external_uq) instead of treating the existing collection "
        "as success. Re-add the idempotency guard around create_collection."
    )


def test_ensure_collection_exists_source_resolves_catalog_id() -> None:
    src = _ensure_source()
    assert "resolve_catalog_id" in src, (
        "ensure_collection_exists must resolve the catalog external_id → "
        "internal id once up front so the existence check and create_collection "
        "target the same physical schema (otherwise the SELECT misses and the "
        "INSERT collides on collections_external_uq)."
    )


def test_ensure_collection_exists_source_guards_internal_collection_id() -> None:
    src = _ensure_source()
    assert "is_internal_physical_name" in src, (
        "ensure_collection_exists must short-circuit when handed an already-"
        "internal collection_id (col_<token>). The item write boundary resolves "
        "external→internal before reaching ensure_physical_table_exists → "
        "set_config → _set_collection_config → ensure_collection_exists, so an "
        "internal id arrives here meaning the collection PROVABLY exists. "
        "resolve_collection_id is forward-only and would MISS it, driving a "
        "spurious create_collection that the internal-id-shape guard rejects with "
        "InvalidIdentifierError ('... is reserved ...'). Re-add the early return."
    )


# ---------------------------------------------------------------------------
# Runtime behaviour
# ---------------------------------------------------------------------------


@pytest.fixture
def svc(monkeypatch):
    """A CollectionService whose catalog resolution is a passthrough stub."""
    s = CollectionService(engine=MagicMock())

    class _FakeCatalogsProtocol:
        async def resolve_catalog_id(self, external_id, allow_missing=False):
            # No external_id mapping in unit context — passthrough (returns None
            # so ensure_collection_exists keeps the id unchanged).
            return None

    monkeypatch.setattr(
        collection_service_mod,
        "get_protocol",
        lambda proto: _FakeCatalogsProtocol(),
    )

    class _AnyDriverContext:
        def __init__(self, db_resource=None, **kwargs):
            self.db_resource = db_resource

    monkeypatch.setattr(collection_service_mod, "DriverContext", _AnyDriverContext)
    return s


@pytest.mark.asyncio
async def test_ensure_swallows_unique_violation_on_retry(svc, monkeypatch):
    """Existence check misses, create_collection raises the external_uq
    UniqueViolationError → ensure_collection_exists must NOT propagate it."""
    monkeypatch.setattr(
        CollectionService, "resolve_collection_id", AsyncMock(return_value=None)
    )
    create = AsyncMock(
        side_effect=UniqueViolationError("collection 'gaul' already exists")
    )
    monkeypatch.setattr(CollectionService, "create_collection", create)

    # Must not raise.
    await svc.ensure_collection_exists(None, "cat_cool_meadow", "gaul")

    create.assert_awaited_once()


@pytest.mark.asyncio
async def test_ensure_returns_early_when_collection_present(svc, monkeypatch):
    """When the existence check resolves, create_collection is never called."""
    monkeypatch.setattr(
        CollectionService, "resolve_collection_id", AsyncMock(return_value="col_abc123")
    )
    create = AsyncMock()
    monkeypatch.setattr(CollectionService, "create_collection", create)

    await svc.ensure_collection_exists(None, "cat_cool_meadow", "gaul")

    create.assert_not_awaited()


@pytest.mark.asyncio
async def test_ensure_returns_early_for_internal_collection_id(svc, monkeypatch):
    """An already-internal collection_id (col_<token>) provably exists. ensure
    must early-return WITHOUT calling resolve_collection_id (forward-only —
    would miss the internal id) or create_collection (whose internal-shape
    guard would reject it with InvalidIdentifierError). This is the live
    ingestion failure: item_service.upsert resolves gaul→col_<token> at the
    write boundary, then _set_collection_config re-enters ensure with it."""
    resolve = AsyncMock(return_value=None)
    monkeypatch.setattr(CollectionService, "resolve_collection_id", resolve)
    create = AsyncMock()
    monkeypatch.setattr(CollectionService, "create_collection", create)

    # col_ + 13 base32 chars — the real internal id shape.
    await svc.ensure_collection_exists(None, "cat_cool_meadow", "col_tooimv7odhd9k")

    resolve.assert_not_awaited()
    create.assert_not_awaited()


@pytest.mark.asyncio
async def test_ensure_propagates_non_unique_errors(svc, monkeypatch):
    """The guard is narrow: a non-duplicate failure (e.g. catalog missing) must
    still surface so genuine errors are not silently swallowed."""
    monkeypatch.setattr(
        CollectionService, "resolve_collection_id", AsyncMock(return_value=None)
    )
    monkeypatch.setattr(
        CollectionService,
        "create_collection",
        AsyncMock(side_effect=ValueError("Catalog 'cat_cool_meadow' does not exist.")),
    )

    with pytest.raises(ValueError):
        await svc.ensure_collection_exists(None, "cat_cool_meadow", "gaul")
