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

"""Real-PostgreSQL integration test for ``fetch_distinct_region_ids``
(un-fao/GeoID#2824, dynastore#443).

``fetch_distinct_region_ids`` issues a dedicated SQL query directly against
the source collection's physical attributes table, deliberately bypassing
``CatalogsProtocol.search_items``/``stream_items`` and the storage-driver
routing they trigger. This exercises that query against a real PostgreSQL
instance for both physical storage shapes a source collection can take —
a columnar (schema-declared) collection and a JSONB (schemaless)
collection — and checks that soft-deleted rows never surface.
"""
from __future__ import annotations

import pytest

from dynastore.extensions.tools.catalog_readiness import wait_for_catalog_ready
from dynastore.models.protocols.catalogs import CatalogsProtocol
from dynastore.models.protocols.field_definition import FieldDefinition
from dynastore.modules.storage.driver_config import ItemsSchema
from dynastore.modules.storage.hints import Hint
from dynastore.tools.discovery import get_protocol
from dynastore.tools.identifiers import generate_id_hex

pytestmark = pytest.mark.enable_modules(
    "db_config", "db", "catalog", "stats", "iam", "stac",
    "collection_postgresql", "catalog_postgresql", "tasks",
)


def _feature(item_id: str, region_code: str) -> dict:
    """A VECTOR feature carrying one ``region_code`` property -- mirrors a
    country/admin-boundary layer, the shape ``region_mapping.apply()``
    claims properties against."""
    return {
        "type": "Feature",
        "id": item_id,
        "geometry": {"type": "Point", "coordinates": [0.0, 0.0]},
        "properties": {"region_code": region_code},
    }


async def _provision_catalog(catalog_id: str) -> None:
    catalogs = get_protocol(CatalogsProtocol)
    assert catalogs is not None
    await catalogs.create_catalog(
        {"id": catalog_id, "title": {"en": "fetch_distinct_region_ids test"}},
        lang="*",
        hints=frozenset({Hint.DEFER}),
    )
    await wait_for_catalog_ready(catalog_id, catalogs_svc=catalogs, caller="test")


async def _create_collection(
    catalog_id: str, collection_id: str, *, columnar: bool,
) -> None:
    """VECTOR collection (default kind) -- columnar when a schema is
    declared for ``region_code``, JSONB when no schema is declared at all
    (collections are all-columnar or all-JSONB, never mixed)."""
    catalogs = get_protocol(CatalogsProtocol)
    assert catalogs is not None
    collection_data: dict = {
        "id": collection_id,
        "title": {"en": collection_id},
    }
    if columnar:
        collection_data["schema"] = ItemsSchema(
            fields={
                "region_code": FieldDefinition(name="region_code", data_type="string"),
            },
        )
    await catalogs.create_collection(catalog_id, collection_data, lang="*")


@pytest.mark.asyncio
async def test_fetch_distinct_region_ids_columnar_collection_sorted_deduped_and_excludes_deleted(
    app_lifespan,
) -> None:
    from dynastore.extensions.region_mapping.claims import fetch_distinct_region_ids
    from dynastore.tools.cache import cache_clear

    cache_clear(fetch_distinct_region_ids)

    catalog_id = f"cat_{generate_id_hex()}"
    collection_id = f"coll_{generate_id_hex()}"
    await _provision_catalog(catalog_id)
    await _create_collection(catalog_id, collection_id, columnar=True)

    catalogs = get_protocol(CatalogsProtocol)
    assert catalogs is not None
    for i, code in enumerate(["ITA", "FRA", "ITA", "DEU"]):
        await catalogs.upsert(catalog_id, collection_id, _feature(f"row-{i}", code))
    ghost = await catalogs.upsert(catalog_id, collection_id, _feature("row-ghost", "GHOST"))
    # ``upsert``'s returned feature id is the internal geoid (the wire-shape
    # default, #1212) -- ``delete_item`` needs that, not the input "row-ghost"
    # external label, to resolve the row.
    deleted_rows = await catalogs.delete_item(catalog_id, collection_id, ghost.id)
    assert deleted_rows == 1, "setup: soft-delete of the ghost row must succeed"

    values = await fetch_distinct_region_ids(catalog_id, collection_id, "region_code")

    assert values == ["DEU", "FRA", "ITA"]


@pytest.mark.asyncio
async def test_fetch_distinct_region_ids_jsonb_collection_sorted_and_deduped(
    app_lifespan,
) -> None:
    from dynastore.extensions.region_mapping.claims import fetch_distinct_region_ids
    from dynastore.tools.cache import cache_clear

    cache_clear(fetch_distinct_region_ids)

    catalog_id = f"cat_{generate_id_hex()}"
    collection_id = f"coll_{generate_id_hex()}"
    await _provision_catalog(catalog_id)
    await _create_collection(catalog_id, collection_id, columnar=False)

    catalogs = get_protocol(CatalogsProtocol)
    assert catalogs is not None
    for i, code in enumerate(["ITA", "FRA", "ITA", "DEU"]):
        await catalogs.upsert(catalog_id, collection_id, _feature(f"row-{i}", code))

    values = await fetch_distinct_region_ids(catalog_id, collection_id, "region_code")

    assert values == ["DEU", "FRA", "ITA"]


@pytest.mark.asyncio
async def test_fetch_distinct_region_ids_unknown_collection_returns_empty(
    app_lifespan,
) -> None:
    """A collection with no physical storage (never created) degrades to an
    empty list rather than raising."""
    from dynastore.extensions.region_mapping.claims import fetch_distinct_region_ids
    from dynastore.tools.cache import cache_clear

    cache_clear(fetch_distinct_region_ids)

    values = await fetch_distinct_region_ids(
        f"cat_{generate_id_hex()}", f"coll_{generate_id_hex()}", "region_code",
    )

    assert values == []

