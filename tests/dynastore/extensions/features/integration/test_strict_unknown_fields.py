"""Integration test for `CollectionSchema.strict_unknown_fields` enforcement
on the live POST /features/.../items path (closes issue #189).

PR #178 added the strict-mode helper, but the old call site (PG driver's
`_enforce_strict_schema`) was bypassed by Branch B in `item_service.upsert`
when PG was the WRITE primary — leaving strict-mode dead code on the live
write path. The check is now at `item_service.upsert` itself, so every
caller passes through it once regardless of which driver owns the write.
"""

from __future__ import annotations

import pytest
from httpx import AsyncClient

from tests.dynastore.test_utils import generate_test_id


@pytest.mark.asyncio
@pytest.mark.enable_extensions("features", "assets", "stac")
async def test_post_item_rejects_unknown_field_with_422(
    sysadmin_in_process_client: AsyncClient,
) -> None:
    catalog_id = f"c_{generate_test_id()}"
    collection_id = f"col_{generate_test_id()}"

    cat_resp = await sysadmin_in_process_client.post(
        "/features/catalogs",
        json={"id": catalog_id, "title": "strict-mode test"},
        timeout=60.0,
    )
    assert cat_resp.status_code == 201, cat_resp.text

    col_resp = await sysadmin_in_process_client.post(
        f"/features/catalogs/{catalog_id}/collections",
        json={
            "id": collection_id,
            "description": "strict-mode test collection",
            "extent": {
                "spatial": {"bbox": [[-180, -90, 180, 90]]},
                "temporal": {"interval": [[None, None]]},
            },
        },
        timeout=60.0,
    )
    assert col_resp.status_code == 201, col_resp.text

    schema_body = {
        "fields": {
            "road_id": {"name": "road_id", "data_type": "text"},
            "lanes": {"name": "lanes", "data_type": "integer"},
        },
        "strict_unknown_fields": True,
        "allow_app_level_enforcement": True,
    }
    schema_resp = await sysadmin_in_process_client.put(
        f"/configs/catalogs/{catalog_id}/collections/{collection_id}"
        "/plugins/collection_schema",
        json=schema_body,
        timeout=60.0,
    )
    assert schema_resp.status_code in (200, 201), schema_resp.text

    rogue_feature = {
        "type": "Feature",
        "geometry": {"type": "Point", "coordinates": [0, 0]},
        "properties": {
            "road_id": "r1",
            "lanes": 2,
            "unauthorized_field": "boom",
        },
    }
    bad_resp = await sysadmin_in_process_client.post(
        f"/features/catalogs/{catalog_id}/collections/{collection_id}/items",
        json=rogue_feature,
        timeout=60.0,
    )
    assert bad_resp.status_code == 422, (
        f"expected 422 for unknown field, got {bad_resp.status_code}: "
        f"{bad_resp.text}"
    )
    body = bad_resp.json()
    detail = body.get("detail")
    assert detail is not None
    if isinstance(detail, dict):
        assert "unauthorized_field" in detail.get("unknown_fields", [])
        assert set(detail.get("allowed_fields", [])) >= {"road_id", "lanes"}
    else:
        assert "unauthorized_field" in str(detail)

    clean_feature = {
        "type": "Feature",
        "geometry": {"type": "Point", "coordinates": [1, 1]},
        "properties": {"road_id": "r2", "lanes": 3},
    }
    ok_resp = await sysadmin_in_process_client.post(
        f"/features/catalogs/{catalog_id}/collections/{collection_id}/items",
        json=clean_feature,
        timeout=60.0,
    )
    assert ok_resp.status_code == 201, ok_resp.text
