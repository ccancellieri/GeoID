import pytest


@pytest.mark.asyncio
@pytest.mark.enable_extensions("stac", "assets", "features")
async def test_stac_search_filters(sysadmin_in_process_client, in_process_client, setup_catalog, setup_collection):
    catalog_id = setup_catalog
    collection_id = setup_collection

    # Create items with distinct properties and geometries
    item1 = {
        "id": "item1",
        "stac_version": "1.0.0",
        "type": "Feature",
        "geometry": {"type": "Point", "coordinates": [10.0, 10.0]},
        "bbox": [10.0, 10.0, 10.0, 10.0],
        "properties": {"datetime": "2024-01-01T10:00:00Z", "prop_a": "value1"},
        "links": [{"rel": "self", "href": "http://example.com/item1"}],
        "assets": {},
    }

    item2 = {
        "id": "item2",
        "stac_version": "1.0.0",
        "type": "Feature",
        "geometry": {"type": "Point", "coordinates": [20.0, 20.0]},
        "bbox": [20.0, 20.0, 20.0, 20.0],
        "properties": {"datetime": "2024-01-02T10:00:00Z", "prop_a": "value2"},
        "links": [{"rel": "self", "href": "http://example.com/item2"}],
        "assets": {},
    }

    # Insert items.
    # Per #1212, ``feature.id`` defaults to the system-assigned geoid: the
    # client-supplied ``"item1"`` / ``"item2"`` are replaced. Capture the
    # returned ids so the search assertions stay decoupled from that contract.
    r1 = await sysadmin_in_process_client.post(
        f"/stac/catalogs/{catalog_id}/collections/{collection_id}/items", json=item1
    )
    print(f"Item1 Insert: {r1.status_code} {r1.text}")
    assert r1.status_code in [200, 201]
    item1_id = r1.json()["id"]

    r2 = await sysadmin_in_process_client.post(
        f"/stac/catalogs/{catalog_id}/collections/{collection_id}/items", json=item2
    )
    print(f"Item2 Insert: {r2.status_code} {r2.text}")
    assert r2.status_code in [200, 201]
    item2_id = r2.json()["id"]

    # All searches use the canonical catalog-scoped path.
    _search_url = f"/stac/catalogs/{catalog_id}/search"

    # 1. Test BBOX Filter (Spatial Only Optimization Candidate)
    # item1 is in [9,9,11,11], item2 is not
    search_bbox = {
        "bbox": [9.0, 9.0, 11.0, 11.0],
        "collections": [collection_id],
    }
    r = await sysadmin_in_process_client.post(_search_url, json=search_bbox)
    if r.status_code != 200:
        print(f"\nResponse: {r.json()}")
    assert r.status_code == 200
    features = r.json()["features"]
    assert len(features) == 1
    assert features[0]["id"] == item1_id

    # 3. Test IDs Filter (Attributes Sidecar)
    # PG sidecar matches ``ids`` against both ``external_id`` and ``geoid``
    # (see attributes.py), so passing the original caller-supplied ids still
    # resolves these features. ES driver matches against ``_id`` (the geoid)
    # — that path requires the geoid.
    search_ids = {
        "ids": ["item1", "item2", item1_id, item2_id],
        "collections": [collection_id],
    }
    r = await sysadmin_in_process_client.post(_search_url, json=search_ids)
    if r.status_code != 200:
        print(f"\nResponse: {r.json()}")
    assert r.status_code == 200
    features = r.json()["features"]
    assert len(features) == 2

    search_ids_single = {
        "ids": ["item2", item2_id],
        "collections": [collection_id],
    }
    r = await sysadmin_in_process_client.post(_search_url, json=search_ids_single)
    if r.status_code != 200:
        print(f"\nResponse: {r.json()}")
    assert r.status_code == 200
    features = r.json()["features"]
    assert len(features) == 1
    assert features[0]["id"] == item2_id

    # 4. Test datetime filter (Refs un-fao/dynastore#339)
    # On a PG-only collection with no validity column (post-#974 default,
    # ``enable_validity=False``) the STAC ``/search`` dispatch sorts by the
    # item's own datetime via the raw SELECT projection
    # ``(sc_attributes.attributes->>'datetime')::timestamptz AS valid_from``.
    # Previously the query optimizer scanned only ``raw_where`` for sidecar
    # aliases, so the ``attributes`` sidecar was never JOINed and the request
    # 500'd with ``missing FROM-clause entry for table "sc_attributes"``.
    # The fix scans the raw SELECT projections too, so the request now succeeds.
    # The bare ``validity`` temporal predicate resolves to match-all when no
    # validity column exists, so both items are returned — the assertion here
    # guards the absence of the 500, not temporal narrowing.
    search_datetime = {
        "datetime": "2024-01-01T00:00:00Z/2024-01-03T00:00:00Z",
        "collections": [collection_id],
    }
    r = await sysadmin_in_process_client.post(_search_url, json=search_datetime)
    if r.status_code != 200:
        print(f"\nResponse: {r.json()}")
    assert r.status_code == 200, r.text
    features = r.json()["features"]
    returned_ids = {f["id"] for f in features}
    assert {item1_id, item2_id} <= returned_ids

    # 5. Test combined bbox + ids + datetime path
    # The combined search exercises the spatial JOIN, the attributes-sidecar
    # JOIN (ids + datetime), and the datetime projection together — the exact
    # composition that previously 500'd. The bbox narrows the result to item1.
    search_combined = {
        "bbox": [9.0, 9.0, 11.0, 11.0],
        "ids": ["item1", "item2", item1_id, item2_id],
        "datetime": "2024-01-01T00:00:00Z/2024-01-03T00:00:00Z",
        "collections": [collection_id],
    }
    r = await sysadmin_in_process_client.post(_search_url, json=search_combined)
    if r.status_code != 200:
        print(f"\nResponse: {r.json()}")
    assert r.status_code == 200, r.text
    features = r.json()["features"]
    assert len(features) == 1
    assert features[0]["id"] == item1_id

    # Cleanup handled by fixtures
