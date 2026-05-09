"""PUT-replace semantics for STAC catalog and collection.

Issue #282 — OGC API Features Part 4 / STAC Transaction Extension require
PUT to replace the whole resource (idempotent, full body) and PATCH to
partially update. These tests pin the new ``replace_stac_catalog`` /
``replace_stac_collection`` handlers introduced to honour that contract.
"""

import pytest


@pytest.mark.asyncio
@pytest.mark.enable_extensions("stac", "assets", "features")
async def test_stac_put_replaces_catalog(
    sysadmin_in_process_client, in_process_client, catalog_data, catalog_id
):
    r = await sysadmin_in_process_client.post("/stac/catalogs", json=catalog_data)
    assert r.status_code == 201

    replacement = {**catalog_data, "title": "Replaced Title"}
    r = await sysadmin_in_process_client.put(
        f"/stac/catalogs/{catalog_id}", json=replacement
    )
    assert r.status_code == 200, r.text
    assert r.json()["title"] == "Replaced Title"

    r = await in_process_client.get(f"/stac/catalogs/{catalog_id}")
    assert r.json()["title"] == "Replaced Title"


@pytest.mark.asyncio
@pytest.mark.enable_extensions("stac", "assets", "features")
async def test_stac_put_rejects_id_mismatch(
    sysadmin_in_process_client, catalog_data, catalog_id
):
    r = await sysadmin_in_process_client.post("/stac/catalogs", json=catalog_data)
    assert r.status_code == 201

    bad = {**catalog_data, "id": "different-id"}
    r = await sysadmin_in_process_client.put(
        f"/stac/catalogs/{catalog_id}", json=bad
    )
    assert r.status_code == 400


@pytest.mark.asyncio
@pytest.mark.enable_extensions("stac", "assets", "features")
async def test_stac_put_rejects_partial_body(
    sysadmin_in_process_client, catalog_data, catalog_id
):
    r = await sysadmin_in_process_client.post("/stac/catalogs", json=catalog_data)
    assert r.status_code == 201

    # Missing required ``id`` — ``STACCatalogRequest`` enforces the field, so
    # the framework rejects with 422 before the handler runs. This is the key
    # PUT-vs-PATCH distinction (PATCH would accept the same body).
    r = await sysadmin_in_process_client.put(
        f"/stac/catalogs/{catalog_id}", json={"title": "no-id"}
    )
    assert r.status_code == 422


@pytest.mark.asyncio
@pytest.mark.enable_extensions("stac", "assets", "features")
async def test_stac_put_replaces_collection(
    sysadmin_in_process_client,
    in_process_client,
    catalog_data,
    catalog_id,
    collection_data,
    collection_id,
):
    r = await sysadmin_in_process_client.post("/stac/catalogs", json=catalog_data)
    assert r.status_code == 201
    r = await sysadmin_in_process_client.post(
        f"/stac/catalogs/{catalog_id}/collections", json=collection_data
    )
    assert r.status_code == 201

    replacement = {**collection_data, "description": "Replaced Description"}
    r = await sysadmin_in_process_client.put(
        f"/stac/catalogs/{catalog_id}/collections/{collection_id}",
        json=replacement,
    )
    assert r.status_code == 200, r.text
    assert r.json()["description"] == "Replaced Description"

    r = await in_process_client.get(
        f"/stac/catalogs/{catalog_id}/collections/{collection_id}"
    )
    assert r.json()["description"] == "Replaced Description"
