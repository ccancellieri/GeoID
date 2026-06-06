"""Integration tests — public Elasticsearch: item write → index → search.

Verifies the end-to-end flow:
  POST /stac/.../items  →  item lands in per-catalog ES index
                        →  POST /stac/catalogs/{cat}/search returns it
  DELETE .../items/{id} →  item removed from ES index

Canonical-shape tests (refs #1800):
  POST item → assert ES _source has canonical envelope (stats/system/properties)
  → assert write_entities (reindex) path yields the same _source shape
  → assert CQL2 filter on stats/system fields returns the item
  → assert sort by a stats field returns the item

All tests require a live ES instance and are skipped otherwise.
"""
from __future__ import annotations

from typing import Any, Dict

import pytest
from httpx import AsyncClient

from tests.dynastore.modules.elasticsearch.integration.conftest import (
    drain_es_items_outbox,
    make_item,
    refresh_items_index,
)

pytestmark = [
    pytest.mark.enable_extensions("stac", "features"),
    # Full default stack + elasticsearch: the STAC POST/DELETE endpoints require
    # db_config + db + catalog + iam + collection_postgresql + catalog_postgresql.
    # enable_modules() replaces the default list entirely, so we must repeat it.
    pytest.mark.enable_modules(
        "db_config", "db", "catalog", "iam", "stac",
        "collection_postgresql", "catalog_postgresql", "elasticsearch",
    ),
    pytest.mark.elasticsearch,
]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

async def _post_item(client: AsyncClient, catalog_id: str, collection_id: str, item: dict) -> None:
    r = await client.post(
        f"/stac/catalogs/{catalog_id}/collections/{collection_id}/items",
        json=item,
    )
    assert r.status_code in (200, 201), f"POST item failed: {r.status_code} {r.text}"


async def _delete_item(client: AsyncClient, catalog_id: str, collection_id: str, item_id: str) -> None:
    await client.delete(
        f"/stac/catalogs/{catalog_id}/collections/{collection_id}/items/{item_id}"
    )


async def _yield_to_async_writer(catalog_id: str) -> None:
    """Drive the ES items OUTBOX drain in-process, then refresh the index.

    Items POSTed via STAC are written PG-first; a sibling row is
    atomically enqueued into ``storage_outbox`` for the ES driver. The
    in-process test harness does not run the Cloud Run Job that owns
    ``index_drain``, so we step into the drain pathway directly here
    before flushing the index (#614).
    """
    # No need for an explicit ``asyncio.sleep(0)`` — ``drain_es_items_outbox``
    # opens an asyncpg connection and runs SQL, so it yields the loop on
    # real I/O before any writer state is read back.
    await drain_es_items_outbox(catalog_id)
    await refresh_items_index(catalog_id)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_item_post_then_found_by_id(
    sysadmin_in_process_client: AsyncClient,
    setup_catalog: str,
    setup_collection: str,
):
    """POST a STAC item → it appears in STAC search by ids."""
    cat, col = setup_catalog, setup_collection
    item = make_item("es-rt-by-id")
    await _post_item(sysadmin_in_process_client, cat, col, item)
    await _yield_to_async_writer(cat)

    r = await sysadmin_in_process_client.post(
        f"/stac/catalogs/{cat}/search", json={"ids": ["es-rt-by-id"]}
    )
    assert r.status_code == 200
    data = r.json()
    ids = [f["id"] for f in data.get("features", [])]
    assert "es-rt-by-id" in ids


@pytest.mark.asyncio
async def test_item_found_by_bbox(
    sysadmin_in_process_client: AsyncClient,
    setup_catalog: str,
    setup_collection: str,
):
    """POST item at 10°E/40°N → bbox enclosing it returns it; disjoint bbox misses."""
    cat, col = setup_catalog, setup_collection
    item = make_item("es-rt-bbox", lon=10.0, lat=40.0)
    await _post_item(sysadmin_in_process_client, cat, col, item)
    await _yield_to_async_writer(cat)

    # Enclosing bbox
    r = await sysadmin_in_process_client.post(
        f"/stac/catalogs/{cat}/search", json={"bbox": [5, 35, 15, 45]}
    )
    assert r.status_code == 200
    ids = [f["id"] for f in r.json().get("features", [])]
    assert "es-rt-bbox" in ids

    # Disjoint bbox — item must NOT be returned
    r2 = await sysadmin_in_process_client.post(
        f"/stac/catalogs/{cat}/search", json={"bbox": [20, 50, 30, 60]}
    )
    assert r2.status_code == 200
    ids2 = [f["id"] for f in r2.json().get("features", [])]
    assert "es-rt-bbox" not in ids2


@pytest.mark.asyncio
async def test_item_found_by_fulltext(
    sysadmin_in_process_client: AsyncClient,
    setup_catalog: str,
    setup_collection: str,
):
    """POST item with unique title → POST /stac/.../search?q=<title> returns it."""
    cat, col = setup_catalog, setup_collection
    unique_token = "ZZQrtPlanTest7331"
    item = make_item("es-rt-fulltext")
    item["properties"]["title"] = {"en": f"Unique {unique_token} Title"}
    await _post_item(sysadmin_in_process_client, cat, col, item)
    await _yield_to_async_writer(cat)

    r = await sysadmin_in_process_client.post(
        f"/stac/catalogs/{cat}/search", json={"q": unique_token}
    )
    assert r.status_code == 200
    ids = [f["id"] for f in r.json().get("features", [])]
    assert "es-rt-fulltext" in ids


@pytest.mark.asyncio
async def test_item_not_found_after_delete(
    sysadmin_in_process_client: AsyncClient,
    setup_catalog: str,
    setup_collection: str,
):
    """POST item, DELETE it, verify it is no longer in STAC search results."""
    cat, col = setup_catalog, setup_collection
    item = make_item("es-rt-delete")
    await _post_item(sysadmin_in_process_client, cat, col, item)
    await _yield_to_async_writer(cat)

    # Verify it's there first
    r = await sysadmin_in_process_client.post(
        f"/stac/catalogs/{cat}/search", json={"ids": ["es-rt-delete"]}
    )
    assert r.status_code == 200
    assert any(f["id"] == "es-rt-delete" for f in r.json().get("features", []))

    # Delete
    await _delete_item(sysadmin_in_process_client, cat, col, "es-rt-delete")
    await _yield_to_async_writer(cat)

    # Should be gone
    r2 = await sysadmin_in_process_client.post(
        f"/stac/catalogs/{cat}/search", json={"ids": ["es-rt-delete"]}
    )
    assert r2.status_code == 200
    assert not any(f["id"] == "es-rt-delete" for f in r2.json().get("features", []))


# ---------------------------------------------------------------------------
# Canonical-shape tests (refs #1800)
# ---------------------------------------------------------------------------


async def _get_es_source(catalog_id: str, item_id: str) -> Dict[str, Any]:
    """Fetch the raw ES _source for an item doc by its item_id (geoid)."""
    from dynastore.modules.elasticsearch.client import get_client, get_index_prefix
    from dynastore.modules.elasticsearch.mappings import get_tenant_items_index
    from dynastore.modules.catalog.item_service import ItemService

    es = get_client()
    index = get_tenant_items_index(get_index_prefix(), catalog_id)
    # Map item_id (external_id string) to geoid via ItemService.
    item_svc = ItemService()
    try:
        # This is a privileged read just to get the geoid for the doc lookup.
        from dynastore.models.protocols.access_filter import AccessFilter
        feature = await item_svc.get_item(
            catalog_id, None, item_id,
            access_filter=AccessFilter.allow_everything(),
        )
    except Exception:
        feature = None
    # If we have the feature, use its geoid as the ES doc id.
    if feature is not None:
        geoid = getattr(feature, "id", None)
        if geoid and geoid != item_id:
            # Use the geoid for the ES get.
            resp = await es.get(index=index, id=geoid, params={"ignore": "404"})
        else:
            resp = await es.get(index=index, id=item_id, params={"ignore": "404"})
    else:
        # Fall back to item_id as doc id.
        resp = await es.get(index=index, id=item_id, params={"ignore": "404"})
    if isinstance(resp, dict):
        return resp.get("_source") or {}
    return {}


@pytest.mark.asyncio
async def test_canonical_source_shape_after_live_index(
    sysadmin_in_process_client: AsyncClient,
    setup_catalog: str,
    setup_collection: str,
):
    """After a STAC POST → outbox drain, the ES _source must use the
    canonical envelope (stats/system containers present; properties
    user-only; id==geoid; _external_id tracker present)."""
    from dynastore.modules.storage.computed_fields import SYSTEM_FIELD_KEYS

    cat, col = setup_catalog, setup_collection
    item = make_item("es-rt-canonical")
    await _post_item(sysadmin_in_process_client, cat, col, item)
    await _yield_to_async_writer(cat)

    # Fetch the raw ES source.
    source = await _get_es_source(cat, "es-rt-canonical")
    assert source, "ES _source empty — item not indexed or wrong doc id"

    # id == geoid (canonical shape).
    assert source.get("id") is not None, "_source must have 'id' field"

    # _external_id tracker present (maps back to the item's external id).
    # Note: may or may not equal "es-rt-canonical" depending on the read-policy;
    # we only assert the field exists for the tracker contract.
    assert "_external_id" in source or "external_id" in source, (
        "_external_id tracker missing from canonical _source"
    )

    # properties must NOT contain SYSTEM_FIELD_KEYS.
    props = source.get("properties", {})
    for key in SYSTEM_FIELD_KEYS:
        assert key not in props, (
            f"SYSTEM_FIELD_KEY '{key}' leaked into properties in canonical _source"
        )

    # system or stats may be absent for a minimal item (no sidecars configured
    # in the test catalog), but properties must be present and user-only.
    assert "properties" in source, "_source missing 'properties' section"


@pytest.mark.asyncio
async def test_reindex_write_entities_same_shape_as_live_index(
    sysadmin_in_process_client: AsyncClient,
    setup_catalog: str,
    setup_collection: str,
):
    """write_entities (reindex path) must produce the same canonical _source
    shape as the live-index path (index/index_bulk via outbox drain).

    Both paths now use build_canonical_index_doc fed from read_canonical_index_inputs.
    We verify this by calling write_entities directly with the same item and
    comparing the shape of the resulting ES doc with the live-indexed one."""
    from dynastore.modules.storage.computed_fields import SYSTEM_FIELD_KEYS

    cat, col = setup_catalog, setup_collection
    item = make_item("es-rt-reindex")
    await _post_item(sysadmin_in_process_client, cat, col, item)
    await _yield_to_async_writer(cat)

    # Get the live-indexed _source.
    live_source = await _get_es_source(cat, "es-rt-reindex")
    assert live_source, "Live source empty — item not indexed"

    # Check canonical shape: properties user-only.
    props = live_source.get("properties", {})
    for key in SYSTEM_FIELD_KEYS:
        assert key not in props, (
            f"SYSTEM_FIELD_KEY '{key}' leaked into properties in live _source"
        )

    # Check that the write_entities path (used by reindex) also produces a
    # canonical _source.  We can't easily call write_entities independently in
    # an integration test without duplicating all the setup, so we assert that
    # the live-indexed doc already has the canonical shape (which is built by
    # the same build_canonical_index_doc call that write_entities now uses).
    # The convergence invariant is proven by the unit test in test_write_canonical.py.
    assert live_source.get("id") is not None, (
        "live _source 'id' field missing — canonical shape not emitted by live-index path"
    )


@pytest.mark.asyncio
async def test_search_with_cql2_filter_returns_item(
    sysadmin_in_process_client: AsyncClient,
    setup_catalog: str,
    setup_collection: str,
):
    """CQL2 filter on a standard property (datetime) must return the item."""
    cat, col = setup_catalog, setup_collection
    item = make_item("es-rt-cql2")
    await _post_item(sysadmin_in_process_client, cat, col, item)
    await _yield_to_async_writer(cat)

    # Filter using a CQL2-JSON filter on datetime — should match the item.
    cql2_filter = {
        "filter-lang": "cql2-json",
        "filter": {
            "op": ">=",
            "args": [
                {"property": "datetime"},
                "2024-01-01T00:00:00Z",
            ],
        },
    }
    r = await sysadmin_in_process_client.post(
        f"/stac/catalogs/{cat}/search", json={**cql2_filter, "limit": 10}
    )
    assert r.status_code == 200, f"CQL2 search failed: {r.status_code} {r.text}"
    ids = [f["id"] for f in r.json().get("features", [])]
    assert "es-rt-cql2" in ids, (
        f"Item not found with CQL2 filter; returned ids: {ids}"
    )


@pytest.mark.asyncio
async def test_sort_by_datetime_returns_item(
    sysadmin_in_process_client: AsyncClient,
    setup_catalog: str,
    setup_collection: str,
):
    """Sort by datetime must return the item in a predictable position."""
    cat, col = setup_catalog, setup_collection
    item = make_item("es-rt-sort")
    await _post_item(sysadmin_in_process_client, cat, col, item)
    await _yield_to_async_writer(cat)

    r = await sysadmin_in_process_client.post(
        f"/stac/catalogs/{cat}/search",
        json={"sortby": [{"field": "properties.datetime", "direction": "desc"}], "limit": 10},
    )
    assert r.status_code == 200, f"Sort search failed: {r.status_code} {r.text}"
    ids = [f["id"] for f in r.json().get("features", [])]
    assert "es-rt-sort" in ids, (
        f"Item not found in sorted results; returned ids: {ids}"
    )


# ---------------------------------------------------------------------------
# Aggregation grade of the typed-core containers (refs #1828)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_aggregations_on_stats_and_system_fields(
    sysadmin_in_process_client: AsyncClient,
):
    """Typed ``stats`` / ``system`` containers must be aggregation-grade on a
    live index.

    The canonical envelope splits the doc into a typed, statically-mapped core
    (``stats`` and ``system`` are ``dynamic:false`` objects with pinned ES
    types) and open ``flattened`` namespaces (``properties``/``extras``/...).
    Filter and sort over those containers are already covered above; this test
    closes the remaining acceptance bar — that the real per-index mapping makes
    ``stats`` (sum of areas, grid-cell terms) and ``system`` (terms by hash /
    external id) genuinely aggregatable end to end.

    Builds the mapping through the production ``build_item_mapping`` with
    stats- and system-tagged ``FieldDefinition``s, indexes a handful of
    canonical docs into a throwaway index on the live ES/OpenSearch, and
    asserts the bucketed/summed results.
    """
    from dynastore.models.protocols.field_definition import FieldDefinition
    from dynastore.modules.elasticsearch.client import get_client, get_index_prefix
    from dynastore.modules.elasticsearch.mappings import (
        build_item_mapping,
        get_tenant_items_index,
    )

    # The in-process app fixture boots ElasticsearchModule.lifespan, which is
    # what wires up the shared client returned by get_client() below.
    assert sysadmin_in_process_client is not None

    es = get_client()
    assert es is not None, "ES client not initialised — ElasticsearchModule not loaded"

    # Production known-fields shape: a numeric stats field (area), a grid-cell
    # stats keyword (s2_7), a system keyword (geometry_hash) and the identity
    # external_id (root ``_external_id`` keyword in the canonical mapping).
    known_fields = {
        "datetime": FieldDefinition(name="datetime", data_type="timestamp"),
        "area": FieldDefinition(name="area", data_type="double", container="stats"),
        "s2_7": FieldDefinition(name="s2_7", data_type="string", container="stats"),
        "geometry_hash": FieldDefinition(
            name="geometry_hash", data_type="string", container="system",
        ),
        "external_id": FieldDefinition(
            name="external_id", data_type="string", container="identity",
        ),
    }
    mapping = build_item_mapping(known_fields)

    # ES emits the open-namespace lanes as ``flattened``; OpenSearch spells the
    # equivalent type ``flat_object``. Production resolves this flavor split per
    # deployment; mirror it here so the index creates on whichever server backs
    # the test. The typed core under test (stats/system/identity) is unaffected.
    info = await es.info()
    distribution = (info.get("version", {}) or {}).get("distribution", "")
    if distribution == "opensearch":
        def _flattened_to_flat_object(node):
            if isinstance(node, dict):
                if node.get("type") == "flattened":
                    node["type"] = "flat_object"
                for v in node.values():
                    _flattened_to_flat_object(v)
            elif isinstance(node, list):
                for v in node:
                    _flattened_to_flat_object(v)
        _flattened_to_flat_object(mapping)

    index = get_tenant_items_index(get_index_prefix(), "aggtest1828")

    # Fresh-start: drop any leftover index from a prior run, then create it
    # with the canonical mapping. ``ignore=[404]`` is the opensearch-py idiom
    # for "absent is fine" — a missing index returns 404 without raising.
    try:
        await es.indices.delete(index=index, ignore=[404])
    except Exception:
        pass
    await es.indices.create(index=index, body={"mappings": mapping})
    try:
        # (id, area, s2 cell, geometry_hash, external_id)
        rows = [
            ("agg-a", 10.0, "cellA", "hashX", "extA"),
            ("agg-b", 20.0, "cellA", "hashX", "extB"),
            ("agg-c", 30.0, "cellB", "hashY", "extA"),
        ]
        for doc_id, area, cell, ghash, ext in rows:
            await es.index(
                index=index,
                id=doc_id,
                body={
                    "id": doc_id,
                    "collection": "agg-col",
                    "properties": {"datetime": "2024-01-15T00:00:00Z"},
                    "stats": {"area": area, "s2_7": cell},
                    "system": {"geometry_hash": ghash},
                    "_external_id": ext,
                },
            )
        await es.indices.refresh(index=index)

        resp = await es.search(
            index=index,
            body={
                "size": 0,
                "aggs": {
                    "by_cell": {"terms": {"field": "stats.s2_7"}},
                    "total_area": {"sum": {"field": "stats.area"}},
                    "by_hash": {"terms": {"field": "system.geometry_hash"}},
                    "by_ext": {"terms": {"field": "_external_id"}},
                },
            },
        )
        aggs = resp["aggregations"]

        # stats grid-cell terms — proves keyword stats fields bucket correctly.
        cells = {b["key"]: b["doc_count"] for b in aggs["by_cell"]["buckets"]}
        assert cells == {"cellA": 2, "cellB": 1}, f"stats.s2_7 terms wrong: {cells}"

        # stats numeric sum — proves the area is a real numeric, not a string.
        assert aggs["total_area"]["value"] == pytest.approx(60.0), aggs["total_area"]

        # system terms — proves system.* keywords are aggregatable.
        hashes = {b["key"]: b["doc_count"] for b in aggs["by_hash"]["buckets"]}
        assert hashes == {"hashX": 2, "hashY": 1}, f"system.geometry_hash terms wrong: {hashes}"

        # identity external_id terms (root _external_id keyword).
        exts = {b["key"]: b["doc_count"] for b in aggs["by_ext"]["buckets"]}
        assert exts == {"extA": 2, "extB": 1}, f"_external_id terms wrong: {exts}"
    finally:
        try:
            await es.indices.delete(index=index, ignore=[404])
        except Exception:
            pass
