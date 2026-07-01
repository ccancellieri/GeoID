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

"""Live-DB integration coverage for the RFC #2550 geometry capability on
RECORDS collections (issue #2645).

A RECORDS collection is geometry-less by default (Req 55 of OGC API -
Records Part 1 allows ``geometry`` to be ``null``), but a collection can now
opt into a real footprint geometry via ``CollectionInfo.allow_geometry``
independent of ``kind`` — without a hard kind-gate and without any runtime
DDL migration (the geometry sidecar table is provisioning-time DDL only,
same as VECTOR).

Requires a reachable PostgreSQL service (Docker/CI test path), like every
test in this directory.
"""

from __future__ import annotations

import asyncio

import pytest

from tests.dynastore.test_utils import generate_test_id

MARKER = pytest.mark.enable_extensions("records", "features", "assets", "configs")
MODULES_MARKER = pytest.mark.enable_modules(
    "db_config", "db", "catalog", "stats", "iam", "stac",
    "collection_postgresql", "catalog_postgresql", "tasks",
)


async def _wait_for_catalog_ready(client, catalog_id: str, *, attempts: int = 90) -> None:
    """Catalog creation is always asynchronous (202 + background provisioning
    task). Poll until ``provisioning_status`` reaches ``'ready'`` — the
    background ``catalog_provision`` task's terminal success state (see
    ``dynastore.modules.catalog.catalog_service._create_catalog_async``).
    """
    for attempt in range(attempts):
        r = await client.get(f"/features/catalogs/{catalog_id}")
        if r.status_code == 200 and r.json().get("provisioning_status") == "ready":
            return
        await asyncio.sleep(min(0.2 * (attempt + 1), 1.0))
    raise AssertionError(f"catalog {catalog_id} never reached 'ready'")


async def _create_collection(client, catalog_id: str, collection_id: str) -> None:
    """Create a catalog + a bare default (VECTOR) collection over /features.

    Records has no collection-creation route of its own — collections are
    driver-agnostic and shared across every OGC facade (features/records/
    stac all read the same physical items table for a given
    catalog/collection). ``kind`` is then set independently via the
    ``collection_info`` plugin config.
    """
    r = await client.post(
        "/features/catalogs",
        json={"id": catalog_id, "title": "records-geometry-capability test"},
    )
    assert r.status_code in (200, 201, 202), f"catalog: {r.status_code} {r.text}"
    if r.status_code == 202:
        await _wait_for_catalog_ready(client, catalog_id)

    # The catalog's tenant schema can briefly lag its 'ready' status flip
    # under xdist load (the same class of settle race the WFS integration
    # fixture documents for its collection_plugin_config PUT). Retry the
    # collection create on a transient 5xx.
    last: object = None
    for attempt in range(30):
        r = await client.post(
            f"/features/catalogs/{catalog_id}/collections",
            json={
                "id": collection_id,
                "description": "records geometry capability test collection",
                "extent": {
                    "spatial": {"bbox": [[-180.0, -90.0, 180.0, 90.0]]},
                    "temporal": {"interval": [[None, None]]},
                },
            },
        )
        if r.status_code in (200, 201):
            return
        last = r
        await asyncio.sleep(min(0.2 * (attempt + 1), 1.0))
    assert False, f"collection: {last.status_code} {last.text}"  # type: ignore[union-attr]


async def _set_collection_info(
    client, catalog_id: str, collection_id: str, *, kind: str, allow_geometry
) -> None:
    # A freshly-created collection can briefly 404/409 on its first config
    # PUT while provisioning settles (same race the WFS integration fixture
    # documents for ``collection_plugin_config``). Retry on both.
    last: object = None
    for attempt in range(30):
        r = await client.put(
            f"/configs/catalogs/{catalog_id}/collections/{collection_id}"
            "/plugins/collection_info",
            json={"kind": kind, "allow_geometry": allow_geometry},
        )
        if r.status_code in (200, 201):
            return
        if r.status_code not in (404, 409):
            break
        last = r
        await asyncio.sleep(min(0.2 * (attempt + 1), 1.0))
    assert False, f"collection_info: {(last or r).status_code} {(last or r).text}"  # type: ignore[union-attr]


@MARKER
@MODULES_MARKER
async def test_records_allow_geometry_true_round_trips_geometry_and_bbox(
    sysadmin_in_process_client,
):
    """(a) RECORDS + ``allow_geometry=True`` → geometry sidecar provisioned;
    a POSTed record with a Point geometry round-trips (stored + returned)
    with a resolved ``bbox``.
    """
    client = sysadmin_in_process_client
    catalog_id = f"c_{generate_test_id()}"
    collection_id = f"col_{generate_test_id()}"
    await _create_collection(client, catalog_id, collection_id)
    await _set_collection_info(
        client, catalog_id, collection_id, kind="RECORDS", allow_geometry=True,
    )

    payload = {
        "type": "Feature",
        "geometry": {"type": "Point", "coordinates": [12.5, 41.9]},
        "properties": {"title": "geo-record", "description": "has a footprint"},
    }
    r = await client.post(
        f"/records/catalogs/{catalog_id}/collections/{collection_id}/items",
        json=payload,
    )
    assert r.status_code == 201, f"add_records: {r.status_code} {r.text}"
    created = r.json()
    record_id = created["id"]
    assert created["geometry"] is not None, "geometry must round-trip on create response"
    assert created["geometry"]["type"] == "Point"

    r = await client.get(
        f"/records/catalogs/{catalog_id}/collections/{collection_id}"
        f"/items/{record_id}"
    )
    assert r.status_code == 200, r.text
    fetched = r.json()
    assert fetched["geometry"] is not None, "geometry must be readable back"
    assert fetched["geometry"]["type"] == "Point"
    assert fetched["geometry"]["coordinates"] == [12.5, 41.9]
    assert fetched.get("bbox") is not None, "bbox must be resolved by the geometry sidecar"


@MARKER
@MODULES_MARKER
async def test_records_default_stays_geometry_less(sysadmin_in_process_client):
    """(b) RECORDS default (``allow_geometry`` unset) → geometry-less, byte-
    identical to pre-#2645 behaviour: no sidecar, submitted geometry is
    dropped, ``geometry`` is always ``null`` on read.
    """
    client = sysadmin_in_process_client
    catalog_id = f"c_{generate_test_id()}"
    collection_id = f"col_{generate_test_id()}"
    await _create_collection(client, catalog_id, collection_id)
    await _set_collection_info(
        client, catalog_id, collection_id, kind="RECORDS", allow_geometry=None,
    )

    payload = {
        "type": "Feature",
        # A geometry-less collection must silently drop this, not error.
        "geometry": {"type": "Point", "coordinates": [12.5, 41.9]},
        "properties": {"title": "plain-record"},
    }
    r = await client.post(
        f"/records/catalogs/{catalog_id}/collections/{collection_id}/items",
        json=payload,
    )
    assert r.status_code == 201, f"add_records: {r.status_code} {r.text}"
    created = r.json()
    assert created["geometry"] is None
    record_id = created["id"]

    r = await client.get(
        f"/records/catalogs/{catalog_id}/collections/{collection_id}"
        f"/items/{record_id}"
    )
    assert r.status_code == 200, r.text
    assert r.json()["geometry"] is None


@MARKER
@MODULES_MARKER
async def test_reclassify_allow_geometry_false_hides_geometry_without_migration(
    sysadmin_in_process_client,
):
    """(d) Reclassification safety: flipping ``allow_geometry`` from
    ``True`` to ``False`` on a RECORDS collection that already has a stored
    geometry must not error or require any DDL — the physical sidecar
    persists, only the resolved read/write capability changes.
    """
    client = sysadmin_in_process_client
    catalog_id = f"c_{generate_test_id()}"
    collection_id = f"col_{generate_test_id()}"
    await _create_collection(client, catalog_id, collection_id)
    await _set_collection_info(
        client, catalog_id, collection_id, kind="RECORDS", allow_geometry=True,
    )

    payload = {
        "type": "Feature",
        "geometry": {"type": "Point", "coordinates": [1.0, 2.0]},
        "properties": {"title": "reclassify-me"},
    }
    r = await client.post(
        f"/records/catalogs/{catalog_id}/collections/{collection_id}/items",
        json=payload,
    )
    assert r.status_code == 201, f"add_records: {r.status_code} {r.text}"
    record_id = r.json()["id"]
    assert r.json()["geometry"] is not None

    # Flip the capability off — no DDL, no ALTER TABLE, just config.
    await _set_collection_info(
        client, catalog_id, collection_id, kind="RECORDS", allow_geometry=False,
    )

    r = await client.get(
        f"/records/catalogs/{catalog_id}/collections/{collection_id}"
        f"/items/{record_id}"
    )
    assert r.status_code == 200, r.text
    assert r.json()["geometry"] is None, (
        "allow_geometry=False must hide the geometry on read even though "
        "the physical column still carries the previously-written value"
    )
