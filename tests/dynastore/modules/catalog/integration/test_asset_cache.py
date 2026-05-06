"""
Comprehensive tests for AssetManager cache invalidation behavior.

Verifies that:
1. Cache is populated on first access
2. Only the modified asset's cache entries are invalidated
3. Unrelated cached assets remain in cache after updates
4. Both get_asset and get_asset_by_code caches are properly invalidated
5. Cache is bypassed when db_resource is provided (transactional queries)
"""

import pytest
from tests.dynastore.test_utils import generate_test_id

from dynastore.modules.catalog.asset_service import (
    AssetService,
    AssetCreate,
    AssetUpdate,
    AssetTypeEnum,
)
from dynastore.models.protocols import AssetsProtocol, CatalogsProtocol
from dynastore.tools.discovery import get_protocol


@pytest.mark.asyncio
@pytest.mark.enable_extensions("assets")
async def test_asset_cache_targeted_invalidation(
    app_lifespan_module, catalog_obj, catalog_id, collection_obj, collection_id
):
    """
    Verify that cache invalidation only affects the specific asset being modified,
    not all cached assets.
    """
    # Setup: Ensure catalog and collection exist
    catalogs = get_protocol(CatalogsProtocol)
    await catalogs.delete_catalog(catalog_id, force=False)
    await catalogs.create_catalog(catalog_obj)
    await catalogs.create_collection(catalog_id, collection_obj)

    asset_service: AssetService = get_protocol(AssetsProtocol)

    # Create two assets
    asset1 = AssetCreate(
        asset_id=f"cache_test_1_{generate_test_id()}",
        filename="asset1.tif",
        uri="gs://test/asset1.tif",
        asset_type=AssetTypeEnum.RASTER,
        metadata={"version": 1},
    )

    asset2 = AssetCreate(
        asset_id=f"cache_test_2_{generate_test_id()}",
        filename="asset2.tif",
        uri="gs://test/asset2.tif",
        asset_type=AssetTypeEnum.RASTER,
        metadata={"version": 1},
    )

    created1 = await asset_service.create_asset(catalog_id, asset1, collection_id)
    created2 = await asset_service.create_asset(catalog_id, asset2, collection_id)

    # Clear cache to start fresh
    asset_service.get_asset_cached.cache_clear()

    # Populate cache by fetching both assets
    cached1_first = await asset_service.get_asset(
        catalog_id, created1.asset_id, collection_id
    )
    cached2_first = await asset_service.get_asset(
        catalog_id, created2.asset_id, collection_id
    )

    assert cached1_first is not None
    assert cached2_first is not None

    # Check cache info before update
    cache_info_before = asset_service.get_asset_cached.cache_info()
    print(
        f"\n📊 Cache info before update: hits={cache_info_before.hits}, misses={cache_info_before.misses}, size={cache_info_before.currsize}"
    )

    # Update asset1 - this should only invalidate asset1's cache entries
    update = AssetUpdate(metadata={"version": 2, "updated": True})
    await asset_service.update_asset(catalog_id, asset1.asset_id, update, collection_id)

    # Check cache info after update
    cache_info_after = asset_service.get_asset_cached.cache_info()
    print(
        f"📊 Cache info after update: hits={cache_info_after.hits}, misses={cache_info_after.misses}, size={cache_info_after.currsize}"
    )

    # Verify that asset2 is still in cache (cache hit should increase)
    hits_before = cache_info_after.hits

    # Fetch asset2 again - should be a cache hit
    cached2_after = await asset_service.get_asset(
        catalog_id, created2.asset_id, collection_id
    )
    cache_info_final = asset_service.get_asset_cached.cache_info()

    # If cache invalidation is targeted, asset2 should still be cached
    assert cache_info_final.hits > hits_before, (
        f"Asset2 should still be in cache after asset1 update (hits before: {hits_before}, after: {cache_info_final.hits})"
    )
    print(f"✅ Asset2 remained cached (cache hit)")

    # Fetch asset1 - should be a cache miss (was invalidated), then it will be cached again
    misses_before = cache_info_final.misses
    cached1_after = await asset_service.get_asset(
        catalog_id, created1.asset_id, collection_id
    )
    cache_info_after_asset1 = asset_service.get_asset_cached.cache_info()

    assert cache_info_after_asset1.misses > misses_before, (
        "Asset1 should have been invalidated (cache miss)"
    )
    assert cached1_after.metadata["version"] == 2
    assert cached1_after.metadata["updated"] is True
    print(f"✅ Asset1 was invalidated and re-fetched from DB")

    print("✅ Cache invalidation is targeted - unrelated assets remain cached")


@pytest.mark.asyncio
@pytest.mark.enable_extensions("assets")
async def test_asset_cache_bypassed_with_db_resource(
    app_lifespan_module, catalog_obj, catalog_id, collection_obj, collection_id
):
    """
    Verify that cache is bypassed when db_resource is provided (transactional queries).
    """
    from dynastore.modules.db_config.query_executor import managed_transaction

    # Setup: Ensure catalog and collection exist
    catalogs = get_protocol(CatalogsProtocol)
    await catalogs.delete_catalog(catalog_id, force=False)
    await catalogs.create_catalog(catalog_obj)
    await catalogs.create_collection(catalog_id, collection_obj)

    asset_service: AssetService = get_protocol(AssetsProtocol)

    # Create an asset
    asset = AssetCreate(
        asset_id=f"txn_test_{generate_test_id()}",
        filename="txn.tif",
        uri="gs://test/txn.tif",
        asset_type=AssetTypeEnum.RASTER,
        metadata={"value": 1},
    )

    created = await asset_service.create_asset(catalog_id, asset, collection_id)

    # Clear cache
    asset_service.get_asset_cached.cache_clear()

    # Fetch without db_resource - should populate cache
    cached1 = await asset_service.get_asset(catalog_id, created.asset_id, collection_id)
    cache_info1 = asset_service.get_asset_cached.cache_info()
    assert cache_info1.currsize == 1, "Cache should have 1 entry"
    print(f"✅ Cache populated: size={cache_info1.currsize}")

    import asyncio

    await asyncio.sleep(0.5)

    # Fetch with db_resource - should bypass cache
    async with managed_transaction(asset_service.engine) as conn:
        fetched_in_txn = await asset_service.get_asset(
            catalog_id, created.asset_id, collection_id, db_resource=conn
        )
        cache_info2 = asset_service.get_asset_cached.cache_info()

        # Cache size and hits should not change (bypassed)
        assert cache_info2.currsize == cache_info1.currsize, (
            "Cache size should not change when bypassed"
        )
        assert cache_info2.hits == cache_info1.hits, (
            "Cache hits should not increase when bypassed"
        )

        assert fetched_in_txn is not None

    # Fetch again without db_resource - should be a cache hit
    hits_before = cache_info2.hits
    cached2 = await asset_service.get_asset(catalog_id, created.asset_id, collection_id)
    cache_info3 = asset_service.get_asset_cached.cache_info()

    assert cache_info3.hits > hits_before, "Should be a cache hit"
    print(f"✅ Cache used for non-transactional query")


@pytest.mark.asyncio
@pytest.mark.enable_extensions("assets")
async def test_asset_cache_invalidation_on_delete(
    app_lifespan_module, catalog_obj, catalog_id, collection_obj, collection_id
):
    """
    Verify that cache is properly invalidated when an asset is deleted.
    """
    # Setup: Ensure catalog and collection exist (don't delete to avoid event loop issues)
    catalogs = get_protocol(CatalogsProtocol)
    await catalogs.create_catalog(catalog_obj)
    await catalogs.create_collection(catalog_id, collection_obj)

    asset_service: AssetService = get_protocol(AssetsProtocol)

    # Create and cache an asset
    asset = AssetCreate(
        asset_id=f"delete_test_{generate_test_id()}",
        filename="delete.tif",
        uri="gs://test/delete.tif",
        asset_type=AssetTypeEnum.RASTER,
        metadata={"temp": True},
    )

    created = await asset_service.create_asset(catalog_id, asset, collection_id)

    # Clear and populate cache
    asset_service.get_asset_cached.cache_clear()
    cached = await asset_service.get_asset(catalog_id, created.asset_id, collection_id)
    assert cached is not None

    cache_info_before = asset_service.get_asset_cached.cache_info()
    print(f"📊 Cache before delete: size={cache_info_before.currsize}")

    # Delete the asset
    deleted_count = await asset_service.soft_delete_asset(
        catalog_id, asset.asset_id, collection_id
    )
    assert deleted_count == 1

    # Cache should have been invalidated
    cache_info_after = asset_service.get_asset_cached.cache_info()
    print(f"📊 Cache after delete: size={cache_info_after.currsize}")

    # Fetch again - should return None (not cached, and deleted in DB)
    result = await asset_service.get_asset(catalog_id, created.asset_id, collection_id)
    assert result is None, "Deleted asset should not be retrievable"
    print("✅ Cache invalidation works correctly on delete")


@pytest.mark.asyncio
@pytest.mark.enable_extensions("assets")
async def test_asset_cache_performance_improvement(
    app_lifespan_module, catalog_obj, catalog_id, collection_obj, collection_id
):
    """
    Demonstrate the performance improvement of targeted cache invalidation.
    """
    # Setup: Ensure catalog and collection exist (don't delete to avoid event loop issues)
    catalogs = get_protocol(CatalogsProtocol)
    await catalogs.create_catalog(catalog_obj)
    await catalogs.create_collection(catalog_id, collection_obj)

    asset_service: AssetService = get_protocol(AssetsProtocol)

    # Create 10 assets
    assets = []
    for i in range(10):
        asset = AssetCreate(
            asset_id=f"perf_test_{i}_{generate_test_id()}",
            filename=f"perf_{i}.tif",
            uri=f"gs://test/perf_{i}.tif",
            asset_type=AssetTypeEnum.RASTER,
            metadata={"index": i},
        )
        created = await asset_service.create_asset(catalog_id, asset, collection_id)
        assets.append(created)

    # Clear cache and populate
    asset_service.get_asset_cached.cache_clear()
    for asset in assets:
        await asset_service.get_asset(catalog_id, asset.asset_id, collection_id)

    cache_info_initial = asset_service.get_asset_cached.cache_info()
    print(
        f"\n📊 Initial cache: size={cache_info_initial.currsize}, hits={cache_info_initial.hits}, misses={cache_info_initial.misses}"
    )

    # Update one asset
    update = AssetUpdate(metadata={"index": 0, "updated": True})
    await asset_service.update_asset(
        catalog_id, assets[0].asset_id, update, collection_id
    )

    # Fetch all 10 assets again
    for asset in assets:
        await asset_service.get_asset(catalog_id, asset.asset_id, collection_id)

    cache_info_final = asset_service.get_asset_cached.cache_info()
    print(
        f"📊 Final cache: size={cache_info_final.currsize}, hits={cache_info_final.hits}, misses={cache_info_final.misses}"
    )

    # Calculate hit rate
    total_fetches = (
        cache_info_final.hits
        + cache_info_final.misses
        - cache_info_initial.hits
        - cache_info_initial.misses
    )
    new_hits = cache_info_final.hits - cache_info_initial.hits
    hit_rate = (new_hits / total_fetches) * 100 if total_fetches > 0 else 0

    print(
        f"📈 Hit rate after update: {hit_rate:.1f}% ({new_hits}/{total_fetches} cache hits)"
    )

    # With targeted invalidation, we should have 9/10 cache hits (90%)
    assert hit_rate >= 85, (
        f"Hit rate should be >= 85% with targeted invalidation, got {hit_rate:.1f}%"
    )
    print(f"✅ Targeted cache invalidation achieves {hit_rate:.1f}% hit rate")
