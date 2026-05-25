import pytest
from dynastore.models.protocols import CatalogsProtocol
from dynastore.tools.discovery import get_protocol


@pytest.fixture
def catalogs_svc(app_lifespan):
    from dynastore.tools.discovery import _DYNASTORE_PLUGINS
    import logging
    log = logging.getLogger("fixture_debug")
    for p in _DYNASTORE_PLUGINS:
        if p.__class__.__name__ == "CatalogService":
            log.warning(f"Found CatalogService: {p}")
            log.warning(f"Is instance of CatalogsProtocol? {isinstance(p, CatalogsProtocol)}")
            log.warning(f"Has is_available? {hasattr(p, 'is_available')}")
            if hasattr(p, 'is_available'):
                log.warning(f"Return of is_available: {p.is_available()}")

    log.warning(f"CatalogsProtocol discovery: _DYNASTORE_PLUGINS={_DYNASTORE_PLUGINS}")
    svc = get_protocol(CatalogsProtocol)
    if not svc:
        pytest.fail("CatalogsProtocol not available")
    return svc


@pytest.mark.asyncio
async def test_catalog_ops(app_lifespan, catalogs_svc, catalog_obj, catalog_id):
    """Test core catalog CRUD operations via CatalogsProtocol."""
    # Ensure cleaned up from previous runs
    await catalogs_svc.delete_catalog(catalog_id, force=True)

    try:
        # 1. Create
        created = await catalogs_svc.create_catalog(catalog_obj)
        assert created.id == catalog_id

        # 2. Get
        retrieved = await catalogs_svc.get_catalog(catalog_id)
        assert retrieved is not None
        assert retrieved.id == catalog_id

        # 3. List - use q= so the check is independent of total catalog count in the DB
        catalogs = await catalogs_svc.list_catalogs(q=catalog_id)
        assert any(c.id == catalog_id for c in catalogs)
    finally:
        # 4. Delete
        success = await catalogs_svc.delete_catalog(catalog_id, force=True)
        assert success is True


@pytest.mark.asyncio
async def test_collection_ops(
    app_lifespan, catalogs_svc, catalog_obj, catalog_id, collection_obj, collection_id
):
    """Test core collection CRUD operations via CatalogsProtocol."""
    await catalogs_svc.delete_catalog(catalog_id, force=True)

    # Setup Catalog
    await catalogs_svc.create_catalog(catalog_obj)

    try:
        # 1. Create Collection
        created = await catalogs_svc.create_collection(catalog_id, collection_obj)
        assert created.id == collection_id

        # 2. Get Collection
        retrieved = await catalogs_svc.get_collection(catalog_id, collection_id)
        assert retrieved is not None
        assert retrieved.id == collection_id

        # 3. List Collections
        collections = await catalogs_svc.list_collections(catalog_id)
        assert any(c.id == collection_id for c in collections)
    finally:
        # Cleanup
        await catalogs_svc.delete_catalog(catalog_id, force=True)


@pytest.mark.asyncio
async def test_collection_soft_delete_then_recreate(
    app_lifespan, catalogs_svc, catalog_obj, catalog_id, collection_obj, collection_id
):
    """A soft-deleted collection id can be recreated. Regression for #317.

    A plain (default, force=False) DELETE leaves a tombstoned ``collections``
    row (``deleted_at`` set). Recreating the same id must resurrect/reset it
    instead of failing with a ``collections_pkey`` unique-violation (HTTP 409).
    """
    await catalogs_svc.delete_catalog(catalog_id, force=True)
    await catalogs_svc.create_catalog(catalog_obj)

    try:
        await catalogs_svc.create_collection(catalog_id, collection_obj)

        # Soft delete (force defaults to False) — leaves a tombstone row.
        assert await catalogs_svc.delete_collection(catalog_id, collection_id) is True

        # Recreate the SAME id — must succeed by resurrecting the tombstone,
        # not raise a PK unique-violation.
        recreated = await catalogs_svc.create_collection(catalog_id, collection_obj)
        assert recreated.id == collection_id

        # It must be live and listable again.
        retrieved = await catalogs_svc.get_collection(catalog_id, collection_id)
        assert retrieved is not None
        assert retrieved.id == collection_id
        collections = await catalogs_svc.list_collections(catalog_id)
        assert any(c.id == collection_id for c in collections)
    finally:
        await catalogs_svc.delete_catalog(catalog_id, force=True)


@pytest.mark.asyncio
async def test_recreate_live_collection_still_conflicts(
    app_lifespan, catalogs_svc, catalog_obj, catalog_id, collection_obj, collection_id
):
    """Recreating a still-live collection must still conflict (no silent resurrect).

    The tombstone-reset path in ``create_collection`` only reclaims
    soft-deleted rows; a live collection with the same id must keep raising.
    """
    await catalogs_svc.delete_catalog(catalog_id, force=True)
    await catalogs_svc.create_catalog(catalog_obj)

    try:
        await catalogs_svc.create_collection(catalog_id, collection_obj)
        with pytest.raises(Exception):
            await catalogs_svc.create_collection(catalog_id, collection_obj)
    finally:
        await catalogs_svc.delete_catalog(catalog_id, force=True)


@pytest.mark.asyncio
async def test_catalog_soft_delete_then_recreate(
    app_lifespan, catalogs_svc, catalog_obj, catalog_id
):
    """A soft-deleted catalog id can be recreated. Regression for #1082.

    A plain (default, force=False) DELETE tombstones the catalog.catalogs
    row (deleted_at set) but leaves the physical schema and metadata sidecars
    intact. Recreating the same id must purge that residue and succeed rather
    than failing with a catalogs_pkey unique-violation (HTTP 409).
    """
    # Ensure a clean starting state.
    await catalogs_svc.delete_catalog(catalog_id, force=True)

    try:
        await catalogs_svc.create_catalog(catalog_obj)

        # Soft delete (force defaults to False) — leaves a tombstone row with
        # the physical schema and metadata sidecars retained.
        assert await catalogs_svc.delete_catalog(catalog_id) is True

        # Verify it is no longer visible.
        assert await catalogs_svc.get_catalog_model(catalog_id) is None

        # Recreate the SAME id — must succeed by reclaiming the tombstone,
        # not raise a PK unique-violation.
        recreated = await catalogs_svc.create_catalog(catalog_obj)
        assert recreated.id == catalog_id

        # It must be live and listable again.
        retrieved = await catalogs_svc.get_catalog_model(catalog_id)
        assert retrieved is not None
        assert retrieved.id == catalog_id
        catalogs = await catalogs_svc.list_catalogs(q=catalog_id)
        assert any(c.id == catalog_id for c in catalogs)
    finally:
        await catalogs_svc.delete_catalog(catalog_id, force=True)


@pytest.mark.asyncio
async def test_recreate_live_catalog_still_conflicts(
    app_lifespan, catalogs_svc, catalog_obj, catalog_id
):
    """Recreating a still-live catalog must still conflict (no silent resurrect).

    The tombstone-reset path in create_catalog only reclaims soft-deleted rows;
    a live catalog with the same id must keep raising.
    """
    await catalogs_svc.delete_catalog(catalog_id, force=True)

    try:
        await catalogs_svc.create_catalog(catalog_obj)
        with pytest.raises(Exception):
            await catalogs_svc.create_catalog(catalog_obj)
    finally:
        await catalogs_svc.delete_catalog(catalog_id, force=True)


@pytest.mark.asyncio
async def test_item_ops(
    app_lifespan,
    catalogs_svc,
    catalog_obj,
    catalog_id,
    collection_obj,
    collection_id,
    item_data_for_db,
    item_id,
):
    """Test core item operations via CatalogsProtocol."""
    await catalogs_svc.delete_catalog(catalog_id, force=True)

    await catalogs_svc.create_catalog(catalog_obj)
    await catalogs_svc.create_collection(catalog_id, collection_obj)

    try:
        from dynastore.extensions.features.ogc_models import FeatureDefinition

        # Create FeatureDefinition from fixture data
        feat_def = FeatureDefinition(
            type="Feature",
            id=item_id,
            geometry={"type": "Point", "coordinates": [12.5, 41.9]},
            properties={"name": "Test Item Integration"},
        )

        # 1. Create Item
        # Use upsert for single item
        res = await catalogs_svc.upsert(catalog_id, collection_id, feat_def)
        assert res is not None
        # res is a Feature object now
        assert res.id == item_id

        # 2. Delete Item
        # Use ID to ensure deletion
        rows = await catalogs_svc.delete_item(catalog_id, collection_id, str(res.id))
        assert rows > 0
    finally:
        from dynastore.modules.catalog.lifecycle_manager import lifecycle_registry

        await lifecycle_registry.wait_for_all_tasks()
        await catalogs_svc.delete_catalog(catalog_id, force=True)


@pytest.mark.asyncio
async def test_delete_enqueues_prior_bbox_tile_invalidation(
    app_lifespan,
    catalogs_svc,
    catalog_obj,
    catalog_id,
    collection_obj,
    collection_id,
    item_id,
    monkeypatch,
):
    """Phase 2 (#1297): deleting an item enqueues a tile invalidation carrying
    the extent the feature used to occupy.

    With the tile cache forced active, ``delete_item`` reads the live item's
    bbox before the soft-delete and dispatches an invalidate task with that
    extent as ``prior_bboxes`` (and no new feature), so the tiles it occupied
    are dropped. The enqueue is captured to assert the wiring end-to-end on a
    real collection + feature.
    """
    import dynastore.modules.tiles.tile_cache_sync as tcs

    captured = []

    async def _active(*_a, **_k):
        return True

    async def _capture_enqueue(
        cat, col, features, *, engine, schema, prior_bboxes=None,
    ):
        captured.append((cat, col, list(features), prior_bboxes))
        return len(prior_bboxes or [])

    monkeypatch.setattr(tcs, "is_tile_cache_active", _active)
    monkeypatch.setattr(tcs, "enqueue_tile_invalidation_task", _capture_enqueue)

    await catalogs_svc.delete_catalog(catalog_id, force=True)
    await catalogs_svc.create_catalog(catalog_obj)
    await catalogs_svc.create_collection(catalog_id, collection_obj)
    try:
        from dynastore.extensions.features.ogc_models import FeatureDefinition

        feat_def = FeatureDefinition(
            type="Feature",
            id=item_id,
            geometry={"type": "Point", "coordinates": [12.5, 41.9]},
            properties={"name": "prior-bbox delete"},
        )
        res = await catalogs_svc.upsert(catalog_id, collection_id, feat_def)
        assert res is not None

        rows = await catalogs_svc.delete_item(
            catalog_id, collection_id, str(res.id),
        )
        assert rows > 0

        # The upsert also invalidates (Phase 1, new bbox), so isolate the
        # DELETE's Phase-2 enqueue: no NEW feature, a prior bbox present.
        delete_calls = [c for c in captured if c[2] == [] and c[3]]
        assert len(delete_calls) == 1, (
            f"delete must enqueue one prior-bbox invalidation; got {captured!r}"
        )
        cat, col, _feats, prior = delete_calls[0]
        assert (cat, col) == (catalog_id, collection_id)
        assert prior == [(12.5, 41.9, 12.5, 41.9)], (
            "the deleted point's extent must be forwarded as prior_bboxes"
        )
    finally:
        from dynastore.modules.catalog.lifecycle_manager import lifecycle_registry

        await lifecycle_registry.wait_for_all_tasks()
        await catalogs_svc.delete_catalog(catalog_id, force=True)
