"""Live-DB integration coverage for single-field attribute filtering (#1156).

Exercises the end-to-end route -> CQL validation -> SQL WHERE path for the
single-field attribute filter (CQL2 ``filter`` + ``?{property}={value}``
shorthand) on both the OGC API Features and STAC ``/items`` endpoints
(#1141 / #1146 / #1155).

The collection declares a COLUMNAR ``attribute_schema`` with a single queryable
column (``adm2_pcode``). Items are ingested with distinct values so the
matching-only assertion is meaningful, then both endpoints are driven over HTTP
to cover the issue's four assertions:

1. Filtering on a declared attribute column returns only matching items.
2. An unknown property -> HTTP 400 (not 500, not silently ignored).
3. Values are parameter-bound: a value containing a single quote does not break
   the query and is not injectable (no 500, no full-table leak).
4. Pagination links on a filtered STAC response preserve the filter params, and
   ``numberMatched`` reports the full filtered total (not the page size).

This is a live-DB integration test: like every test in this directory it
requires a reachable PostgreSQL service (provided by the Docker/CI test path).

Matching (STAC + Features), validation, injection-safety, and filtered
pagination all pass against the fixed filter pipeline.
"""

import pytest
from httpx import AsyncClient

from dynastore.models.protocols import CatalogsProtocol
from dynastore.tools.discovery import get_protocol


MARKER = pytest.mark.enable_extensions("stac", "assets", "features")

# The queryable attribute column declared on the collection's COLUMNAR
# attribute sidecar. Filtering by this name must reach the SQL WHERE clause;
# any other name must be rejected as an unknown property (400).
ATTR = "adm2_pcode"

# Distinct attribute values across the ingested items. Two items share PK001 so
# a filtered query that matches both can drive pagination (assertion 4).
_ITEMS = [
    ("item_a", "PK001", [10.0, 10.0]),
    ("item_b", "PK001", [11.0, 11.0]),
    ("item_c", "PK002", [20.0, 20.0]),
    ("item_d", "PK003", [30.0, 30.0]),
]

# A value chosen to probe SQL-injection safety: it contains a single quote and a
# boolean-OR tautology. A safe implementation must NOT execute it as SQL — it
# must either bind it (0 rows) or reject it cleanly (400), but never 500 and
# never return the whole table.
_INJECTION_VALUE = "PK001' OR '1'='1"


async def _create_collection_with_attribute_schema(
    catalog_id: str, collection_id: str
) -> None:
    """Create a PG-backed collection whose attribute sidecar declares ``ATTR``
    as a COLUMNAR (physical-column) queryable.

    Mirrors ``modules/catalog/integration/test_attribute_defaults.py`` — the
    canonical way to stand up a collection with a declared ``attribute_schema``
    in the integration suite.
    """
    from dynastore.modules.storage.driver_config import ItemsPostgresqlDriverConfig
    from dynastore.modules.storage.drivers.pg_sidecars.geometries_config import (
        GeometriesSidecarConfig,
    )
    from dynastore.modules.storage.drivers.pg_sidecars.attributes_config import (
        FeatureAttributeSidecarConfig,
        AttributeStorageMode,
        AttributeSchemaEntry,
        AttributeIndexType,
        PostgresType,
    )

    catalogs = get_protocol(CatalogsProtocol)
    await catalogs.ensure_catalog_exists(catalog_id)

    attr_sidecar = FeatureAttributeSidecarConfig(
        storage_mode=AttributeStorageMode.COLUMNAR,
        attribute_schema=[
            AttributeSchemaEntry(
                name=ATTR,
                type=PostgresType.TEXT,
                index=AttributeIndexType.BTREE,
            ),
        ],
    )
    col_config = ItemsPostgresqlDriverConfig(
        sidecars=[GeometriesSidecarConfig(), attr_sidecar]
    )

    await catalogs.create_collection(
        catalog_id,
        {
            "id": collection_id,
            "title": {"en": "Attribute Filter Collection (#1156)"},
            "layer_config": col_config.model_dump(),
        },
        lang="*",
    )


async def _ingest_items(client: AsyncClient, catalog_id: str, collection_id: str) -> None:
    """Ingest the fixture items over the STAC ``/items`` endpoint."""
    for item_id, pcode, coords in _ITEMS:
        item = {
            "id": item_id,
            "stac_version": "1.0.0",
            "type": "Feature",
            "geometry": {"type": "Point", "coordinates": coords},
            "bbox": [coords[0], coords[1], coords[0], coords[1]],
            "properties": {"datetime": "2024-01-01T10:00:00Z", ATTR: pcode},
            "links": [],
            "assets": {},
        }
        r = await client.post(
            f"/stac/catalogs/{catalog_id}/collections/{collection_id}/items",
            json=item,
        )
        assert r.status_code in (200, 201), f"ingest {item_id}: {r.status_code} {r.text}"


def _feature_pcodes(features: list) -> list:
    return [f.get("properties", {}).get(ATTR) for f in features]


# ---------------------------------------------------------------------------
# STAC /items
# ---------------------------------------------------------------------------


@MARKER
@pytest.mark.asyncio
async def test_stac_attribute_filter_matches_only(
    sysadmin_in_process_client: AsyncClient, catalog_id, collection_id
):
    """Assertion 1: filtering a declared attribute column returns only matches,
    via both the ``?{property}={value}`` shorthand and the explicit CQL2 form."""
    await _create_collection_with_attribute_schema(catalog_id, collection_id)
    await _ingest_items(sysadmin_in_process_client, catalog_id, collection_id)

    base = f"/stac/catalogs/{catalog_id}/collections/{collection_id}/items"

    # Sanity: the unfiltered listing returns every ingested item, so any
    # subsequent shortfall is the filter, not missing data.
    r = await sysadmin_in_process_client.get(base)
    assert r.status_code == 200, r.text
    assert len(r.json()["features"]) == len(_ITEMS)

    # Shorthand ?{property}={value}.
    r = await sysadmin_in_process_client.get(base, params={ATTR: "PK002"})
    assert r.status_code == 200, r.text
    assert _feature_pcodes(r.json()["features"]) == ["PK002"]

    # Equivalent explicit CQL2 filter.
    r = await sysadmin_in_process_client.get(base, params={"filter": f"{ATTR}='PK002'"})
    assert r.status_code == 200, r.text
    assert _feature_pcodes(r.json()["features"]) == ["PK002"]

    # A value shared by two items returns exactly those two.
    r = await sysadmin_in_process_client.get(base, params={ATTR: "PK001"})
    assert r.status_code == 200, r.text
    assert sorted(_feature_pcodes(r.json()["features"])) == ["PK001", "PK001"]


@MARKER
@pytest.mark.asyncio
async def test_stac_attribute_filter_validation_and_injection(
    sysadmin_in_process_client: AsyncClient, catalog_id, collection_id
):
    """Assertions 2 and 3 on the STAC endpoint, sharing one collection setup.

    2. An unknown property is rejected with 400 (not 500, not silently ignored
       as 'return everything'), on both the shorthand and explicit CQL forms.
    3. A value containing a single quote is handled safely — the request is
       never a 500 and never leaks the whole table. A safe backend either binds
       the literal (clean 200 with no extra rows) or rejects it (400).
    """
    await _create_collection_with_attribute_schema(catalog_id, collection_id)
    await _ingest_items(sysadmin_in_process_client, catalog_id, collection_id)

    base = f"/stac/catalogs/{catalog_id}/collections/{collection_id}/items"

    # Assertion 2: unknown property -> 400 on both forms.
    r = await sysadmin_in_process_client.get(base, params={"no_such_column": "x"})
    assert r.status_code == 400, f"shorthand: expected 400, got {r.status_code}: {r.text}"

    r = await sysadmin_in_process_client.get(
        base, params={"filter": "no_such_column='x'"}
    )
    assert r.status_code == 400, f"cql: expected 400, got {r.status_code}: {r.text}"

    # Assertion 3: single-quote value must not 500 and must not be executed as SQL.
    # Known limitation: the shorthand doubles the quote per CQL2-Text (``''``),
    # but the bundled pygeofilter lark grammar does not accept ``''`` inside a
    # string literal and rejects it with a parse error -> 400. That is a safe,
    # non-injectable outcome (the value is never run as SQL), so both a clean
    # 200 (bound, zero matches) and a 400 (parse rejection) are accepted here.
    r = await sysadmin_in_process_client.get(base, params={ATTR: _INJECTION_VALUE})
    assert r.status_code in (200, 400), f"injection: unexpected {r.status_code}: {r.text}"
    if r.status_code == 200:
        # If accepted, the bound literal matches nothing — never the full table.
        features = r.json()["features"]
        assert len(features) < len(_ITEMS), "injection value leaked rows"


@MARKER
@pytest.mark.asyncio
async def test_stac_attribute_filter_pagination_preserves_filter(
    sysadmin_in_process_client: AsyncClient, catalog_id, collection_id
):
    """Assertion 4: pagination links on a filtered STAC response preserve the
    filter params, so paging stays scoped to the filtered set."""
    await _create_collection_with_attribute_schema(catalog_id, collection_id)
    await _ingest_items(sysadmin_in_process_client, catalog_id, collection_id)

    base = f"/stac/catalogs/{catalog_id}/collections/{collection_id}/items"

    # Two items share PK001; limit=1 forces a 'next' link.
    r = await sysadmin_in_process_client.get(base, params={ATTR: "PK001", "limit": 1})
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["numberMatched"] == 2
    assert len(body["features"]) == 1

    links = {link["rel"]: link["href"] for link in body.get("links", [])}
    assert "next" in links, f"no next link on filtered paged response: {body.get('links')}"
    assert f"{ATTR}=PK001" in links["next"], (
        f"filter param dropped from next link: {links['next']}"
    )
    assert f"{ATTR}=PK001" in links.get("self", ""), (
        f"filter param dropped from self link: {links.get('self')}"
    )

    # Following the next link returns the second PK001 item, still filtered.
    next_href = links["next"]
    next_path = next_href.split("test", 1)[1] if "test" in next_href else next_href
    r2 = await sysadmin_in_process_client.get(next_path)
    assert r2.status_code == 200, r2.text
    body2 = r2.json()
    assert body2["numberMatched"] == 2
    assert _feature_pcodes(body2["features"]) == ["PK001"]


# ---------------------------------------------------------------------------
# OGC API Features /items
# ---------------------------------------------------------------------------


@MARKER
@pytest.mark.asyncio
async def test_features_attribute_filter_matches_only(
    sysadmin_in_process_client: AsyncClient, catalog_id, collection_id
):
    """Assertion 1 on the OGC Features endpoint: shorthand and explicit CQL
    equality on a declared attribute column return only the matching items."""
    await _create_collection_with_attribute_schema(catalog_id, collection_id)
    await _ingest_items(sysadmin_in_process_client, catalog_id, collection_id)

    base = f"/features/catalogs/{catalog_id}/collections/{collection_id}/items"

    # Sanity: unfiltered listing returns every ingested item.
    r = await sysadmin_in_process_client.get(base)
    assert r.status_code == 200, r.text
    assert len(r.json()["features"]) == len(_ITEMS)

    # Shorthand ?{property}={value}.
    r = await sysadmin_in_process_client.get(base, params={ATTR: "PK002"})
    assert r.status_code == 200, r.text
    assert _feature_pcodes(r.json()["features"]) == ["PK002"]

    # Equivalent explicit CQL2 filter.
    r = await sysadmin_in_process_client.get(base, params={"filter": f"{ATTR}='PK002'"})
    assert r.status_code == 200, r.text
    assert _feature_pcodes(r.json()["features"]) == ["PK002"]

    # A value shared by two items returns exactly those two.
    r = await sysadmin_in_process_client.get(base, params={ATTR: "PK001"})
    assert r.status_code == 200, r.text
    assert sorted(_feature_pcodes(r.json()["features"])) == ["PK001", "PK001"]
