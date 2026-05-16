"""End-to-end verification of `X-Tile-Cache` / `X-Tile-Source` headers (F5).

PR #601 added the headers and pinned them at the helper level
(`tests/dynastore/extensions/tiles/test_tile_cache_headers.py`). This
suite verifies the full route hop: first request emits a miss header,
second request — after the background `save_tile` finishes — emits a
hit header. Uses the PG-backed tile cache (no GCP credentials needed).
"""
from __future__ import annotations

import asyncio

import pytest
from httpx import AsyncClient

from tests.dynastore.test_utils import generate_test_id


@pytest.mark.asyncio
@pytest.mark.enable_modules("db_config", "db", "catalog", "tiles")
@pytest.mark.enable_extensions("tiles", "assets", "features")
async def test_first_request_is_miss_second_request_is_hit(
    in_process_client: AsyncClient,
):
    """First tile request reports `X-Tile-Cache: miss`; after the
    background populate runs, the second request reports `hit`."""
    catalog_id = f"c_hdr_{generate_test_id()}"
    collection_id = "headers_collection"

    # Catalog + collection
    r = await in_process_client.post(
        "/features/catalogs",
        json={"id": catalog_id, "title": "Cache-header test catalog"},
    )
    assert r.status_code == 201, r.text

    r = await in_process_client.post(
        f"/features/catalogs/{catalog_id}/collections",
        json={
            "id": collection_id,
            "title": "Cache-header test collection",
            "extent": {
                "spatial": {"bbox": [[-180, -90, 180, 90]]},
                "temporal": {"interval": [["2020-01-01T00:00:00Z", None]]},
            },
        },
    )
    assert r.status_code == 201, r.text

    # PG-backed cache so we don't need GCS.
    r = await in_process_client.put(
        f"/configs/catalogs/{catalog_id}/plugins/tiles_preseed_config",
        json={"storage_priority": ["pg"]},
    )
    assert r.status_code == 200, r.text

    # One feature covering tile (0,0,0)
    r = await in_process_client.post(
        f"/features/catalogs/{catalog_id}/collections/{collection_id}/items",
        json={
            "type": "FeatureCollection",
            "features": [{
                "type": "Feature",
                "geometry": {
                    "type": "Polygon",
                    "coordinates": [[[0, 0], [10, 0], [10, 10], [0, 10], [0, 0]]],
                },
                "properties": {"name": "header probe"},
            }],
        },
    )
    assert r.status_code == 201, r.text
    await asyncio.sleep(0.1)  # transaction settle (matches sibling tests)

    tile_url = (
        f"/tiles/{catalog_id}/tiles/0/0/0.mvt?collections={collection_id}"
    )

    # 1st request — must be a miss (postgis-generated)
    first = await in_process_client.get(tile_url)
    assert first.status_code in (200, 204), first.text
    assert first.headers.get("X-Tile-Cache") == "miss", (
        f"first request should miss; got headers={dict(first.headers)}"
    )
    assert first.headers.get("X-Tile-Source") == "postgis"

    # Prime the cache directly through the provider — the route's
    # `background_tasks.add_task(provider.save_tile, ...)` runs after
    # the response is sent and is not deterministic to await in
    # AsyncTestClient. We're testing the READ-PATH header contract here;
    # the save-path is covered by the helper-level unit test in
    # `test_tile_cache_headers.py`.
    from dynastore.tools.discovery import get_protocol
    from dynastore.modules.tiles.tiles_module import TileStorageProtocol

    provider = get_protocol(TileStorageProtocol)
    assert provider is not None, "TileStorageProtocol not registered"
    assert first.content, "cannot prime cache from empty first response"
    await provider.save_tile(
        catalog_id, collection_id, "WebMercatorQuad", 0, 0, 0,
        first.content, "mvt",
    )

    # 2nd request — must be a hit
    second = await in_process_client.get(tile_url)
    assert second.status_code in (200, 204), second.text
    assert second.headers.get("X-Tile-Cache") == "hit", (
        f"second request should hit; got headers={dict(second.headers)}"
    )
    # PG provider has no signed-URL path -> proxy bytes.
    assert second.headers.get("X-Tile-Source") == "bucket_proxy", (
        f"unexpected X-Tile-Source on cache hit: "
        f"{second.headers.get('X-Tile-Source')!r}"
    )


@pytest.mark.asyncio
@pytest.mark.enable_modules("db_config", "db", "catalog", "tiles")
@pytest.mark.enable_extensions("tiles", "assets", "features")
async def test_cache_disabled_via_config_flips_hit_back_to_miss(
    in_process_client: AsyncClient,
):
    """F3 contract: with `TilesCachingConfig.cache_enabled=False`, the second
    request reports `miss` even after the background populate runs."""
    catalog_id = f"c_dis_{generate_test_id()}"
    collection_id = "disabled_collection"

    await in_process_client.post(
        "/features/catalogs",
        json={"id": catalog_id, "title": "Cache-disabled test catalog"},
    )
    await in_process_client.post(
        f"/features/catalogs/{catalog_id}/collections",
        json={
            "id": collection_id,
            "title": "Cache-disabled test collection",
            "extent": {
                "spatial": {"bbox": [[-180, -90, 180, 90]]},
                "temporal": {"interval": [["2020-01-01T00:00:00Z", None]]},
            },
        },
    )
    await in_process_client.put(
        f"/configs/catalogs/{catalog_id}/plugins/tiles_preseed_config",
        json={"storage_priority": ["pg"]},
    )
    # F3: platform-tier kill switch for the L2 cache.
    r = await in_process_client.put(
        "/configs/plugins/tiles_caching_config",
        json={"cache_enabled": False},
    )
    assert r.status_code == 200, r.text

    await in_process_client.post(
        f"/features/catalogs/{catalog_id}/collections/{collection_id}/items",
        json={
            "type": "FeatureCollection",
            "features": [{
                "type": "Feature",
                "geometry": {
                    "type": "Polygon",
                    "coordinates": [[[0, 0], [10, 0], [10, 10], [0, 10], [0, 0]]],
                },
                "properties": {"name": "disabled probe"},
            }],
        },
    )
    await asyncio.sleep(0.1)

    tile_url = (
        f"/tiles/{catalog_id}/tiles/0/0/0.mvt?collections={collection_id}"
    )

    first = await in_process_client.get(tile_url)
    assert first.headers.get("X-Tile-Cache") == "miss"

    # Even after the background populate window, a disabled L2 cache
    # keeps returning miss.
    await asyncio.sleep(1.0)
    second = await in_process_client.get(tile_url)
    assert second.headers.get("X-Tile-Cache") == "miss"
    assert second.headers.get("X-Tile-Source") == "postgis"

    # Restore default to avoid polluting later tests in the same session.
    await in_process_client.delete("/configs/plugins/tiles_caching_config")
