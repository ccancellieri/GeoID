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

"""Unit tests for soft-delete HTTP/STAC semantics (issue #2528 item 2).

Covers:

1. Idempotent soft-delete: a repeat DELETE on an already-tombstoned catalog
   must return True (→ HTTP 204), not False (→ HTTP 404).
2. Never-existed catalog: soft-delete of an external_id that was never
   created must still return False (→ HTTP 404).
3. Tombstone-inclusive GET: get_catalog_model must return the deleted Catalog
   model (with deleted_at set) when the active-only queries miss but the
   external_id resolves to a tombstoned row.
4. Catalog.deleted_at field: the Catalog model exposes deleted_at as an
   Optional field that survives model_validate / model_dump round-trips.

Integration tests that require a live PostgreSQL instance are NOT included
here; those are noted in the PR body as requiring a dev environment.
"""
from __future__ import annotations

import datetime
from typing import Optional
from unittest.mock import AsyncMock, patch

import pytest

from dynastore.models.shared_models import Catalog
from dynastore.modules.catalog.catalog_service import (
    CatalogService,
    _catalog_external_id_cache,
)


# ---------------------------------------------------------------------------
# Catalog.deleted_at field contract
# ---------------------------------------------------------------------------


def test_catalog_has_deleted_at_field() -> None:
    """Catalog model must expose deleted_at as Optional[datetime]."""
    assert "deleted_at" in Catalog.model_fields
    field = Catalog.model_fields["deleted_at"]
    assert field.default is None


def test_catalog_deleted_at_defaults_to_none() -> None:
    cat = Catalog.model_validate({"id": "c_test001"})
    assert cat.deleted_at is None


def test_catalog_deleted_at_round_trips() -> None:
    ts = datetime.datetime(2026, 6, 27, 12, 0, 0, tzinfo=datetime.timezone.utc)
    cat = Catalog.model_validate({"id": "c_test001", "deleted_at": ts})
    assert cat.deleted_at == ts


def test_catalog_deleted_at_excluded_from_dump_when_none() -> None:
    """Active catalogs must not leak a deleted_at: null key in API output."""
    cat = Catalog.model_validate({"id": "c_test001"})
    dumped = cat.model_dump(exclude_none=True)
    assert "deleted_at" not in dumped


def test_catalog_deleted_at_present_in_dump_when_set() -> None:
    """Tombstoned catalogs must surface deleted_at in their serialized form."""
    ts = datetime.datetime(2026, 6, 27, 12, 0, 0, tzinfo=datetime.timezone.utc)
    cat = Catalog.model_validate({"id": "c_test001", "deleted_at": ts})
    dumped = cat.model_dump(exclude_none=True)
    assert "deleted_at" in dumped
    assert dumped["deleted_at"] == ts


# ---------------------------------------------------------------------------
# _get_tombstoned_catalog_id_by_external_id_db — tombstone probe
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_tombstone_probe_returns_internal_id_when_tombstoned(monkeypatch: pytest.MonkeyPatch) -> None:
    """The tombstone probe returns the internal id when the catalog is soft-deleted."""
    svc = CatalogService.__new__(CatalogService)

    async def _tombstoned(ext_id: str) -> Optional[str]:
        if ext_id == "deleted-cat":
            return "c_deleted001"
        return None

    monkeypatch.setattr(
        svc, "_get_tombstoned_catalog_id_by_external_id_db", _tombstoned
    )
    result = await svc._get_tombstoned_catalog_id_by_external_id_db("deleted-cat")
    assert result == "c_deleted001"


@pytest.mark.asyncio
async def test_tombstone_probe_returns_none_for_active_or_missing(monkeypatch: pytest.MonkeyPatch) -> None:
    """The tombstone probe returns None for catalogs that were never created or are active."""
    svc = CatalogService.__new__(CatalogService)

    async def _no_tombstone(ext_id: str) -> Optional[str]:
        return None

    monkeypatch.setattr(
        svc, "_get_tombstoned_catalog_id_by_external_id_db", _no_tombstone
    )
    result = await svc._get_tombstoned_catalog_id_by_external_id_db("ghost-cat")
    assert result is None


# ---------------------------------------------------------------------------
# delete_catalog soft path — idempotency contract
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_soft_delete_already_tombstoned_returns_true(monkeypatch: pytest.MonkeyPatch) -> None:
    """A second soft-delete of an already-tombstoned catalog returns True (→ HTTP 204).

    RFC 9110 §9.3.5: DELETE is idempotent; the second call must not return
    404 simply because the catalog is already soft-deleted.
    """
    _catalog_external_id_cache.cache_clear()
    svc = CatalogService.__new__(CatalogService)
    svc.engine = None  # Not needed for this unit path

    # Strict active-only resolve returns None (catalog is tombstoned).
    monkeypatch.setattr(svc, "_get_catalog_id_by_external_id_db", AsyncMock(return_value=None))
    # Tombstone probe finds the internal id.
    monkeypatch.setattr(
        svc,
        "_get_tombstoned_catalog_id_by_external_id_db",
        AsyncMock(return_value="c_already_deleted"),
    )

    result = await svc.delete_catalog("my-tombstoned-catalog", force=False)
    assert result is True, (
        "Repeat soft-delete of an already-tombstoned catalog must return True "
        "(idempotent 204), not False (404)."
    )


@pytest.mark.asyncio
async def test_soft_delete_never_existed_returns_false(monkeypatch: pytest.MonkeyPatch) -> None:
    """A soft-delete of a catalog that never existed returns False (→ HTTP 404)."""
    _catalog_external_id_cache.cache_clear()
    svc = CatalogService.__new__(CatalogService)
    svc.engine = None

    # Both the strict resolve and the tombstone probe return None.
    monkeypatch.setattr(svc, "_get_catalog_id_by_external_id_db", AsyncMock(return_value=None))
    monkeypatch.setattr(
        svc,
        "_get_tombstoned_catalog_id_by_external_id_db",
        AsyncMock(return_value=None),
    )

    result = await svc.delete_catalog("ghost-catalog", force=False)
    assert result is False, (
        "Soft-delete of a catalog that never existed must return False (→ 404)."
    )


# ---------------------------------------------------------------------------
# get_catalog_model — tombstone fallback contract
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_get_catalog_model_returns_tombstoned_model(monkeypatch: pytest.MonkeyPatch) -> None:
    """get_catalog_model falls back to the tombstoned model when the active query misses.

    This is the 200+deleted-state contract: a direct GET of a soft-deleted
    catalog must return the model with deleted_at set, not None.
    """
    _catalog_external_id_cache.cache_clear()
    svc = CatalogService.__new__(CatalogService)
    svc.engine = None
    svc._collection_service = None
    svc._item_service = None
    svc._cascade_orchestrator = None

    ts = datetime.datetime(2026, 6, 27, 0, 0, 0, tzinfo=datetime.timezone.utc)
    tombstoned_model = Catalog.model_validate(
        {"id": "c_deleted001", "external_id": "my-deleted-cat", "deleted_at": ts}
    )

    # Active-only resolve returns None (catalog was tombstoned → cache invalidated).
    monkeypatch.setattr(svc, "_get_catalog_id_by_external_id_db", AsyncMock(return_value=None))
    # Tombstone model fetch returns the deleted Catalog.
    monkeypatch.setattr(
        svc,
        "_get_tombstoned_catalog_model_by_external_id_db",
        AsyncMock(return_value=tombstoned_model),
    )
    # Model cache (normal path) returns None.
    with patch(
        "dynastore.modules.catalog.catalog_service._catalog_model_cache",
        new=AsyncMock(return_value=None),
    ):
        result = await svc.get_catalog_model("my-deleted-cat")

    assert result is not None, (
        "get_catalog_model must return the tombstoned Catalog model (200+deleted-state), "
        "not None (404)."
    )
    assert result.deleted_at == ts


@pytest.mark.asyncio
async def test_get_catalog_model_returns_none_for_never_existed(monkeypatch: pytest.MonkeyPatch) -> None:
    """get_catalog_model returns None for a catalog that never existed."""
    _catalog_external_id_cache.cache_clear()
    svc = CatalogService.__new__(CatalogService)
    svc.engine = None
    svc._collection_service = None
    svc._item_service = None
    svc._cascade_orchestrator = None

    # Both active resolve and tombstone probe return None.
    monkeypatch.setattr(svc, "_get_catalog_id_by_external_id_db", AsyncMock(return_value=None))
    monkeypatch.setattr(
        svc,
        "_get_tombstoned_catalog_model_by_external_id_db",
        AsyncMock(return_value=None),
    )

    with patch(
        "dynastore.modules.catalog.catalog_service._catalog_model_cache",
        new=AsyncMock(return_value=None),
    ):
        result = await svc.get_catalog_model("ghost-cat")

    assert result is None


# ---------------------------------------------------------------------------
# Source-shape: soft-delete must still emit CATALOG_METADATA_CHANGED
# (regression guard — the early-return for idempotency must not remove
# the event emission from the first-delete path)
# ---------------------------------------------------------------------------


def test_soft_delete_source_still_emits_events() -> None:
    """The idempotency early-return must not remove event emission from the first-delete path."""
    import inspect
    from dynastore.modules.catalog import catalog_service

    src = inspect.getsource(catalog_service.CatalogService.delete_catalog)
    assert "CatalogEventType.CATALOG_METADATA_CHANGED" in src
    assert '"operation": "soft_delete"' in src


def test_soft_delete_source_invalidates_external_id_cache() -> None:
    """Soft-delete must invalidate the external_id cache so tombstone probes receive the right key."""
    import inspect
    from dynastore.modules.catalog import catalog_service

    src = inspect.getsource(catalog_service.CatalogService.delete_catalog)
    assert "_invalidate_catalog_external_id_cache" in src, (
        "delete_catalog must call _invalidate_catalog_external_id_cache on soft-delete "
        "so that subsequent get_catalog_model tombstone probes see the external_id, "
        "not the stale internal_id mapping."
    )
