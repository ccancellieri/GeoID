"""Catalog provisioning status updates must propagate through the metadata router.

Bug observed on review env 2026-04-30:
  * ``CatalogService.create_catalog`` writes the row with
    ``provisioning_status='provisioning'`` and fans out to the
    metadata router → ES indexer sees 'provisioning'.
  * Async provisioning (gcp_provision/task.py) completes and calls
    ``CatalogService.update_provisioning_status(catalog_id, 'ready')``.
  * The PG row flips to 'ready' but ES still shows 'provisioning' —
    update_provisioning_status didn't fan out to the metadata router.

These tests pin the contract that ``update_provisioning_status``:
  1. writes the source-of-truth row in ``catalog.catalogs``
  2. fans the change out via ``catalog_metadata_router.upsert_catalog_metadata``
     so every CatalogStore driver (PG CORE, PG STAC, ES indexer)
     stays in sync with the row

Also pins that ``provisioning_status`` is in the metadata payload built
by ``_build_catalog_metadata_payload`` — without it, the ES doc never
contains the field even when the router is called.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from sqlalchemy.ext.asyncio import AsyncConnection

from dynastore.modules.catalog.catalog_service import (
    _build_catalog_metadata_payload,
)


def _spec_conn() -> MagicMock:
    """A MagicMock that satisfies DriverContext.db_resource's isinstance check.

    DriverContext validates db_resource against (Connection | Session |
    AsyncEngine | AsyncConnection | AsyncSession). spec=AsyncConnection
    makes our mock pass that union check.
    """
    return MagicMock(spec=AsyncConnection)


# ---------------------------------------------------------------------------
# Payload builder — provisioning_status must be in the output
# ---------------------------------------------------------------------------

def test_build_catalog_metadata_payload_includes_provisioning_status() -> None:
    catalog_model = MagicMock()
    # Set every attribute the builder reads to None except provisioning_status,
    # so the test isolates the new field. Title / description etc. would
    # normally bring in their own keys; setting them to None makes them omitted.
    for attr in (
        "title", "description", "keywords", "license", "extra_metadata",
        "stac_version", "stac_extensions", "links", "assets",
    ):
        setattr(catalog_model, attr, None)
    catalog_model.provisioning_status = "ready"
    # `conformsTo` accessed via getattr; a MagicMock attr would be truthy
    catalog_model.conformsTo = None

    payload = _build_catalog_metadata_payload(catalog_model)
    assert payload.get("provisioning_status") == "ready"


def test_build_catalog_metadata_payload_omits_status_when_none() -> None:
    """``None`` provisioning_status should be omitted, not serialised as null."""
    catalog_model = MagicMock()
    for attr in (
        "title", "description", "keywords", "license", "extra_metadata",
        "stac_version", "stac_extensions", "links", "assets", "conformsTo",
    ):
        setattr(catalog_model, attr, None)
    catalog_model.provisioning_status = None

    payload = _build_catalog_metadata_payload(catalog_model)
    assert "provisioning_status" not in payload


def test_build_catalog_metadata_payload_carries_status_alongside_other_fields() -> None:
    """Status must coexist with the existing CORE / STAC fields, not replace them."""
    title_mock = MagicMock()
    title_mock.model_dump.return_value = {"en": "My Catalog"}

    catalog_model = MagicMock()
    catalog_model.title = title_mock
    catalog_model.description = None
    catalog_model.keywords = None
    catalog_model.license = None
    catalog_model.extra_metadata = None
    catalog_model.stac_version = "1.0.0"
    catalog_model.stac_extensions = None
    catalog_model.conformsTo = None
    catalog_model.links = None
    catalog_model.assets = None
    catalog_model.provisioning_status = "provisioning"

    payload = _build_catalog_metadata_payload(catalog_model)
    assert payload["title"] == {"en": "My Catalog"}
    assert payload["stac_version"] == "1.0.0"
    assert payload["provisioning_status"] == "provisioning"


# ---------------------------------------------------------------------------
# update_provisioning_status — must fan out to the metadata router
# ---------------------------------------------------------------------------

def _make_service():
    """Build a minimal CatalogService without running __init__.

    Mirrors the helper in test_catalog_service_router_overlay.py — keeps the
    unit tests off the live DB while preserving real method bodies.
    """
    from dynastore.modules.catalog.catalog_service import CatalogService

    svc = CatalogService.__new__(CatalogService)
    # Cache invalidation is a method on a wrapped descriptor — stub the surface
    cache = MagicMock()
    cache.cache_invalidate = MagicMock()
    svc._get_catalog_model_cached = cache  # type: ignore[attr-defined]
    return svc


@pytest.mark.asyncio
async def test_update_provisioning_status_fans_out_to_metadata_router() -> None:
    """After PG UPDATE, the router's upsert_catalog_metadata is called with
    the fresh model's full metadata (including the new provisioning_status)."""
    svc = _make_service()

    # Mock the source-of-truth UPDATE returning a row (success).
    dql_result = {"id": "cat_x"}

    # Mock the post-UPDATE re-fetch returning a model with status='ready'.
    fresh_model = MagicMock()
    for attr in (
        "title", "description", "keywords", "license", "extra_metadata",
        "stac_version", "stac_extensions", "links", "assets", "conformsTo",
    ):
        setattr(fresh_model, attr, None)
    fresh_model.provisioning_status = "ready"
    svc.get_catalog_model = AsyncMock(return_value=fresh_model)

    # Patch managed_transaction to yield a sentinel connection.
    sentinel_conn = _spec_conn()

    class _CtxMgr:
        async def __aenter__(self_inner):
            return sentinel_conn
        async def __aexit__(self_inner, *_):
            return False

    upsert_called_with: dict = {}

    async def _fake_upsert(catalog_id, metadata, *, db_resource=None, **_):
        upsert_called_with["catalog_id"] = catalog_id
        upsert_called_with["metadata"] = metadata
        upsert_called_with["db_resource"] = db_resource

    with patch(
        "dynastore.modules.catalog.catalog_service.managed_transaction",
        return_value=_CtxMgr(),
    ), patch(
        "dynastore.modules.catalog.catalog_service.get_catalog_engine",
        return_value=MagicMock(),
    ), patch(
        "dynastore.modules.catalog.catalog_service.DQLQuery",
    ) as DQLQueryCls, patch(
        "dynastore.modules.catalog.catalog_metadata_router.upsert_catalog_metadata",
        new=_fake_upsert,
    ):
        # DQLQuery(...).execute(...) returns the row dict
        DQLQueryCls.return_value.execute = AsyncMock(return_value=dql_result)

        ok = await svc.update_provisioning_status("cat_x", "ready")

    assert ok is True
    # Cache invalidated on the source-of-truth flip
    svc._get_catalog_model_cached.cache_invalidate.assert_called_once_with("cat_x")
    # Router fan-out actually happened with the new status carried in
    assert upsert_called_with["catalog_id"] == "cat_x"
    assert upsert_called_with["metadata"]["provisioning_status"] == "ready"
    # Same connection passed through so the read participates in the txn
    assert upsert_called_with["db_resource"] is sentinel_conn


@pytest.mark.asyncio
async def test_update_provisioning_status_skips_router_when_pg_returns_no_row() -> None:
    """If the UPDATE returns no row (catalog gone), don't call the router."""
    svc = _make_service()
    svc.get_catalog_model = AsyncMock(return_value=None)

    class _CtxMgr:
        async def __aenter__(self_inner):
            return _spec_conn()
        async def __aexit__(self_inner, *_):
            return False

    router_called = False

    async def _fake_upsert(*_args, **_kwargs):
        nonlocal router_called
        router_called = True

    with patch(
        "dynastore.modules.catalog.catalog_service.managed_transaction",
        return_value=_CtxMgr(),
    ), patch(
        "dynastore.modules.catalog.catalog_service.get_catalog_engine",
        return_value=MagicMock(),
    ), patch(
        "dynastore.modules.catalog.catalog_service.DQLQuery",
    ) as DQLQueryCls, patch(
        "dynastore.modules.catalog.catalog_metadata_router.upsert_catalog_metadata",
        new=_fake_upsert,
    ):
        DQLQueryCls.return_value.execute = AsyncMock(return_value=None)
        ok = await svc.update_provisioning_status("missing_cat", "ready")

    assert ok is False
    assert router_called is False
    svc._get_catalog_model_cached.cache_invalidate.assert_not_called()


@pytest.mark.asyncio
async def test_update_provisioning_status_skips_router_when_payload_empty() -> None:
    """Defensive: if the model has nothing the payload builder picks up, the
    router is not called (matches the create_catalog short-circuit)."""
    svc = _make_service()

    # Model with EVERYTHING None — including provisioning_status — produces
    # an empty payload from _build_catalog_metadata_payload.
    empty_model = MagicMock()
    for attr in (
        "title", "description", "keywords", "license", "extra_metadata",
        "stac_version", "stac_extensions", "links", "assets", "conformsTo",
        "provisioning_status",
    ):
        setattr(empty_model, attr, None)
    svc.get_catalog_model = AsyncMock(return_value=empty_model)

    class _CtxMgr:
        async def __aenter__(self_inner):
            return _spec_conn()
        async def __aexit__(self_inner, *_):
            return False

    router_called = False

    async def _fake_upsert(*_args, **_kwargs):
        nonlocal router_called
        router_called = True

    with patch(
        "dynastore.modules.catalog.catalog_service.managed_transaction",
        return_value=_CtxMgr(),
    ), patch(
        "dynastore.modules.catalog.catalog_service.get_catalog_engine",
        return_value=MagicMock(),
    ), patch(
        "dynastore.modules.catalog.catalog_service.DQLQuery",
    ) as DQLQueryCls, patch(
        "dynastore.modules.catalog.catalog_metadata_router.upsert_catalog_metadata",
        new=_fake_upsert,
    ):
        DQLQueryCls.return_value.execute = AsyncMock(return_value={"id": "cat_x"})
        ok = await svc.update_provisioning_status("cat_x", "ready")

    assert ok is True
    # Cache still invalidated (the row DID flip), but router skipped (empty payload)
    svc._get_catalog_model_cached.cache_invalidate.assert_called_once_with("cat_x")
    assert router_called is False
