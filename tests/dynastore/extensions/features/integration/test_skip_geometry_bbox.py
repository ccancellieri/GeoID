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

"""``skipGeometry=true`` must still return ``bbox`` (#2899).

End-to-end repro of the bug: a real item with a non-trivial polygon geometry
is ingested (the geometries sidecar writes ``bbox_geom`` by default), then read
back via ``GET /items`` with ``skipGeometry=true`` — the response Feature must
carry ``geometry: null`` alongside a populated, correct ``bbox``, sourced from
the cheap ``bbox_geom`` envelope column rather than the full geometry.
"""

import pytest

from httpx import AsyncClient
from tests.dynastore.test_utils import generate_test_id

_POLYGON = {
    "type": "Polygon",
    "coordinates": [[
        [10.0, 20.0], [10.0, 25.0], [15.0, 25.0], [15.0, 20.0], [10.0, 20.0],
    ]],
}


async def _drive_catalog_to_ready(catalog_id: str) -> None:
    """Catalog creation is always async (#2329): the HTTP create call returns
    202 the moment at least one provisioner (``catalog_core`` is always
    active) is registered, well before the tenant schema exists. There is no
    background dispatcher in this test process to claim the enqueued
    ``catalog_provision`` task, so run the executor directly — mirrors
    ``_provision_catalog`` in ``test_group_by_query.py``.
    """
    import uuid

    from dynastore.modules.tasks.models import TaskPayload
    from dynastore.models.protocols.catalogs import CatalogsProtocol
    from dynastore.tasks.catalog_provision.task import (
        CatalogProvisionInputs,
        CatalogProvisionTask,
    )
    from dynastore.tools.discovery import get_protocol

    catalogs = get_protocol(CatalogsProtocol)
    cat = await catalogs.get_catalog(catalog_id)
    if getattr(cat, "provisioning_status", "ready") == "ready":
        return
    payload = TaskPayload(
        task_id=uuid.uuid4(),
        caller_id="test",
        inputs=CatalogProvisionInputs(
            catalog_id=cat.id, scope="catalog", operation="provision",
        ),
    )
    await CatalogProvisionTask().run(payload)


async def _create_catalog_and_collection(client: AsyncClient, test_data_loader):
    catalog_id = f"c_{generate_test_id()}"
    collection_id = "test_skip_geometry_bbox"

    catalog_data = test_data_loader("catalog.json")
    catalog_data["id"] = catalog_id
    r = await client.post("/features/catalogs", json=catalog_data, timeout=60.0)
    assert r.status_code in (201, 202), r.text
    if r.status_code == 202:
        await _drive_catalog_to_ready(catalog_id)

    collection_data = test_data_loader("collection.json")
    collection_data["id"] = collection_id
    r = await client.post(
        f"/features/catalogs/{catalog_id}/collections", json=collection_data, timeout=60.0
    )
    assert r.status_code == 201, r.text

    # A declared items_schema is required for pushdown_read_select to narrow
    # the SELECT at all (a schemaless collection has no "all declared
    # properties" to narrow to and stays on the wildcard read) — the
    # narrowed, schema-driven projection is exactly the path #2899 fixes.
    schema_resp = await client.put(
        f"/configs/catalogs/{catalog_id}/collections/{collection_id}/plugins/items_schema",
        json={"fields": {"name": {"name": "name", "data_type": "string", "required": False}}},
        timeout=60.0,
    )
    assert schema_resp.status_code in (200, 201), schema_resp.text

    return catalog_id, collection_id


@pytest.mark.asyncio
@pytest.mark.enable_extensions("features", "assets", "stac")
async def test_skip_geometry_true_returns_bbox_without_geometry(
    sysadmin_in_process_client: AsyncClient, test_data_loader
):
    catalog_id, collection_id = await _create_catalog_and_collection(
        sysadmin_in_process_client, test_data_loader
    )

    payload = {
        "type": "Feature",
        "geometry": _POLYGON,
        "properties": {"name": "bbox-repro"},
    }
    r = await sysadmin_in_process_client.post(
        f"/features/catalogs/{catalog_id}/collections/{collection_id}/items",
        json=payload,
        timeout=60.0,
    )
    assert r.status_code == 201

    r = await sysadmin_in_process_client.get(
        f"/features/catalogs/{catalog_id}/collections/{collection_id}/items",
        params={"skipGeometry": "true", "limit": 1},
    )
    assert r.status_code == 200
    body = r.json()
    assert len(body["features"]) == 1
    feature = body["features"][0]

    assert feature["geometry"] is None
    assert feature.get("bbox") is not None
    xmin, ymin, xmax, ymax = feature["bbox"]
    assert xmin == pytest.approx(10.0)
    assert ymin == pytest.approx(20.0)
    assert xmax == pytest.approx(15.0)
    assert ymax == pytest.approx(25.0)


@pytest.mark.asyncio
@pytest.mark.enable_extensions("features", "assets", "stac")
async def test_skip_geometry_false_regression_unchanged(
    sysadmin_in_process_client: AsyncClient, test_data_loader
):
    """Regression: default (``skipGeometry=false``) still returns geometry
    plus the same bbox — the fix must not alter the non-skip path."""
    catalog_id, collection_id = await _create_catalog_and_collection(
        sysadmin_in_process_client, test_data_loader
    )

    payload = {
        "type": "Feature",
        "geometry": _POLYGON,
        "properties": {"name": "bbox-repro"},
    }
    r = await sysadmin_in_process_client.post(
        f"/features/catalogs/{catalog_id}/collections/{collection_id}/items",
        json=payload,
        timeout=60.0,
    )
    assert r.status_code == 201

    r = await sysadmin_in_process_client.get(
        f"/features/catalogs/{catalog_id}/collections/{collection_id}/items",
        params={"limit": 1},
    )
    assert r.status_code == 200
    body = r.json()
    feature = body["features"][0]

    assert feature["geometry"] is not None
    assert feature["geometry"]["type"] == "Polygon"
    assert feature.get("bbox") is not None
    xmin, ymin, xmax, ymax = feature["bbox"]
    assert xmin == pytest.approx(10.0)
    assert ymin == pytest.approx(20.0)
    assert xmax == pytest.approx(15.0)
    assert ymax == pytest.approx(25.0)
