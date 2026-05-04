"""Regression tests for TRANSFORM-only driver filtering in the metadata routers.

Pins the fix for the production crash

    NotImplementedError: BigQueryMetadataTransformDriver is TRANSFORM-only;
    route writes to a Primary driver.

triggered by collection creation in a GCP-enabled catalog.  The GCP
module auto-registers ``BigQueryMetadataTransformDriver`` via
``register_plugin`` during ``GCPModule.lifespan()``; the driver
advertises ``capabilities = frozenset({TRANSFORM})`` and its
``upsert_metadata`` stub raises on invocation (see
``TransformOnlyCollectionStoreMixin`` in
``models/protocols/entity_store.py``).

Before the fix, the default fan-out path in both
``collection_metadata_router`` and ``catalog_metadata_router`` iterated
every registered driver, invoking the raising stub and aborting the
fan-out with no preceding driver writes completed.  The fix is a
``EntityStoreCapability.WRITE`` filter applied only on the default-
resolution path; callers that pass ``drivers=`` explicitly keep full
control (dry-run tooling, tests that want to observe the raise).
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from dynastore.models.protocols.entity_store import EntityStoreCapability


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _write_driver() -> MagicMock:
    d = MagicMock(name="WriteDriver")
    d.capabilities = frozenset({EntityStoreCapability.WRITE})
    d.upsert_metadata = AsyncMock()
    d.delete_metadata = AsyncMock()
    d.upsert_catalog_metadata = AsyncMock()
    d.delete_catalog_metadata = AsyncMock()
    return d


def _transform_only_driver() -> MagicMock:
    d = MagicMock(name="TransformOnlyDriver")
    d.capabilities = frozenset({EntityStoreCapability.TRANSFORM})
    # Mirrors the raising mixin stub: any accidental invocation blows up.
    d.upsert_metadata = AsyncMock(side_effect=NotImplementedError(
        "TransformOnly is TRANSFORM-only; route writes to a Primary driver."
    ))
    d.delete_metadata = AsyncMock(side_effect=NotImplementedError(
        "TransformOnly is TRANSFORM-only; route deletes to a Primary driver."
    ))
    d.upsert_catalog_metadata = AsyncMock(side_effect=NotImplementedError(
        "TransformOnly is TRANSFORM-only; route writes to a Primary driver."
    ))
    d.delete_catalog_metadata = AsyncMock(side_effect=NotImplementedError(
        "TransformOnly is TRANSFORM-only; route deletes to a Primary driver."
    ))
    # domain is read by _emit_catalog_metadata_changed when a WRITE-capable
    # driver is present alongside the transform driver.
    d.domain = None
    return d


# ---------------------------------------------------------------------------
# Collection router
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_upsert_collection_skips_transform_only_driver():
    from dynastore.modules.catalog import collection_metadata_router as cmr

    write = _write_driver()
    transform = _transform_only_driver()

    with patch.object(cmr, "_resolve_drivers", return_value=[write, transform]):
        await cmr.upsert_collection_metadata(
            "cat", "col", {"title": {"en": "T"}},
        )

    write.upsert_metadata.assert_awaited_once()
    transform.upsert_metadata.assert_not_awaited()


@pytest.mark.asyncio
async def test_delete_collection_skips_transform_only_driver():
    from dynastore.modules.catalog import collection_metadata_router as cmr

    write = _write_driver()
    transform = _transform_only_driver()

    with patch.object(cmr, "_resolve_drivers", return_value=[write, transform]):
        await cmr.delete_collection_metadata("cat", "col")

    write.delete_metadata.assert_awaited_once()
    transform.delete_metadata.assert_not_awaited()


@pytest.mark.asyncio
async def test_upsert_collection_noops_when_only_transform_drivers(caplog):
    """Fan-out with zero WRITE-capable drivers degrades to a logged no-op."""
    from dynastore.modules.catalog import collection_metadata_router as cmr

    # Reset per-process warning latch so the log fires within this test.
    cmr._MISSING_DRIVERS_LOGGED["collection_write"] = False

    transform = _transform_only_driver()

    with patch.object(cmr, "_resolve_drivers", return_value=[transform]):
        with caplog.at_level("WARNING"):
            await cmr.upsert_collection_metadata("cat", "col", {"title": "T"})

    transform.upsert_metadata.assert_not_awaited()
    assert any(
        "No WRITE-capable CollectionStore drivers" in r.message
        for r in caplog.records
    )


@pytest.mark.asyncio
async def test_explicit_drivers_kwarg_bypasses_capability_filter():
    """Callers passing ``drivers=`` take full responsibility — no filter."""
    from dynastore.modules.catalog import collection_metadata_router as cmr

    transform = _transform_only_driver()

    with pytest.raises(NotImplementedError):
        await cmr.upsert_collection_metadata(
            "cat", "col", {"title": "T"}, drivers=[transform],
        )


# ---------------------------------------------------------------------------
# Catalog router
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_upsert_catalog_skips_transform_only_driver():
    from dynastore.modules.catalog import catalog_metadata_router as cmr

    write = _write_driver()
    # Domain attribute is read when emitting the changed event.
    write.domain = MagicMock(value="CORE")
    transform = _transform_only_driver()

    with patch.object(
        cmr, "_resolve_catalog_metadata_drivers",
        return_value=[write, transform],
    ), patch.object(cmr, "_emit_catalog_metadata_changed", new=AsyncMock()):
        await cmr.upsert_catalog_metadata("cat", {"title": {"en": "T"}})

    write.upsert_catalog_metadata.assert_awaited_once()
    transform.upsert_catalog_metadata.assert_not_awaited()


@pytest.mark.asyncio
async def test_delete_catalog_skips_transform_only_driver():
    from dynastore.modules.catalog import catalog_metadata_router as cmr

    write = _write_driver()
    write.domain = MagicMock(value="CORE")
    transform = _transform_only_driver()

    with patch.object(
        cmr, "_resolve_catalog_metadata_drivers",
        return_value=[write, transform],
    ), patch.object(cmr, "_emit_catalog_metadata_changed", new=AsyncMock()):
        await cmr.delete_catalog_metadata("cat")

    write.delete_catalog_metadata.assert_awaited_once()
    transform.delete_catalog_metadata.assert_not_awaited()


@pytest.mark.asyncio
async def test_upsert_catalog_noops_when_only_transform_drivers(caplog):
    from dynastore.modules.catalog import catalog_metadata_router as cmr

    cmr._MISSING_DRIVERS_LOGGED["catalog_write"] = False
    transform = _transform_only_driver()

    with patch.object(
        cmr, "_resolve_catalog_metadata_drivers",
        return_value=[transform],
    ), patch.object(cmr, "_emit_catalog_metadata_changed", new=AsyncMock()) \
            as emit:
        with caplog.at_level("WARNING"):
            await cmr.upsert_catalog_metadata("cat", {"title": "T"})

    transform.upsert_catalog_metadata.assert_not_awaited()
    emit.assert_not_awaited()
    assert any(
        "No WRITE-capable CatalogStore drivers" in r.message
        for r in caplog.records
    )
