"""Verify the assets extension registers PUT (replace) and PATCH (update)
as distinct routes per OGC API Features Part 4 conformance — Issue #282.

Asset metadata has only one mutable field (``metadata``, free-form dict)
so the partial-vs-full distinction collapses at the data layer; both
methods bind to the same handler. The split lives at the route layer so
the OpenAPI spec exposes both operations correctly.
"""

from __future__ import annotations

import pytest


@pytest.mark.asyncio
@pytest.mark.enable_extensions("assets")
async def test_assets_routes_register_put_and_patch(in_process_client):
    """OpenAPI sanity: ``PUT`` and ``PATCH`` both registered for both
    catalog-asset and collection-asset paths."""
    response = await in_process_client.get("/openapi.json")
    assert response.status_code == 200
    paths = response.json()["paths"]

    catalog_asset_path = "/assets/catalogs/{catalog_id}/assets/{asset_id}"
    coll_asset_path = (
        "/assets/catalogs/{catalog_id}/collections/{collection_id}/assets/{asset_id}"
    )

    for path in (catalog_asset_path, coll_asset_path):
        assert path in paths, f"missing path {path}"
        ops = paths[path]
        assert "put" in ops, f"PUT missing on {path}"
        assert "patch" in ops, f"PATCH missing on {path}"
