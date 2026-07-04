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

"""Real-PostgreSQL integration test for ``fetch_region_ids_by_unique_id``.

TerriaJS's MVT region matching treats a ``regionIds`` values array
positionally: ``values[i]`` must be the region code of the feature whose
``uniqueIdProp`` attribute equals ``i``. Unlike ``fetch_distinct_region_ids``
(deduplicated, alphabetically sorted -- for CSV templates),
``fetch_region_ids_by_unique_id`` must preserve one entry per feature
(region codes may repeat), positioned by the feature's numeric unique-id
column rather than sorted by the region code itself.
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


def _feature(item_id: str, region_code: str, fid: int) -> dict:
    """A VECTOR feature carrying ``region_code`` and a sequential ``fid`` --
    mirrors a country/admin-boundary layer where several features can share
    one region code (e.g. several admin-1 features under the same country)."""
    return {
        "type": "Feature",
        "id": item_id,
        "geometry": {"type": "Point", "coordinates": [0.0, 0.0]},
        "properties": {"region_code": region_code, "fid": fid},
    }


async def _provision_catalog(catalog_id: str) -> None:
    catalogs = get_protocol(CatalogsProtocol)
    assert catalogs is not None
    await catalogs.create_catalog(
        {"id": catalog_id, "title": {"en": "fetch_region_ids_by_unique_id test"}},
        lang="*",
        hints=frozenset({Hint.DEFER}),
    )
    await wait_for_catalog_ready(catalog_id, catalogs_svc=catalogs, caller="test")


async def _create_collection(
    catalog_id: str, collection_id: str, *, columnar: bool,
) -> None:
    """VECTOR collection (default kind) -- columnar when a schema is declared
    for ``region_code``/``fid``, JSONB when no schema is declared at all
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
                "fid": FieldDefinition(name="fid", data_type="integer"),
            },
        )
    await catalogs.create_collection(catalog_id, collection_data, lang="*")


@pytest.mark.asyncio
async def test_fetch_region_ids_by_unique_id_columnar_collection_ordered_by_fid_with_repeats(
    app_lifespan,
) -> None:
    from dynastore.extensions.region_mapping.claims import fetch_region_ids_by_unique_id
    from dynastore.tools.cache import cache_clear

    cache_clear(fetch_region_ids_by_unique_id)

    catalog_id = f"cat_{generate_id_hex()}"
    collection_id = f"coll_{generate_id_hex()}"
    await _provision_catalog(catalog_id)
    await _create_collection(catalog_id, collection_id, columnar=True)

    catalogs = get_protocol(CatalogsProtocol)
    assert catalogs is not None
    # fid 0 and 2 both carry "ITA" -- a repeated region code at non-adjacent
    # positions, exactly the many-features-one-country shape this function
    # must preserve (a plain DISTINCT would collapse them).
    rows = [("ITA", 0), ("FRA", 1), ("ITA", 2), ("DEU", 3)]
    for i, (code, fid) in enumerate(rows):
        await catalogs.upsert(catalog_id, collection_id, _feature(f"row-{i}", code, fid))
    ghost = await catalogs.upsert(catalog_id, collection_id, _feature("row-ghost", "GHOST", 4))
    deleted_rows = await catalogs.delete_item(catalog_id, collection_id, ghost.id)
    assert deleted_rows == 1, "setup: soft-delete of the ghost row must succeed"

    values = await fetch_region_ids_by_unique_id(catalog_id, collection_id, "region_code", "fid")

    assert values == ["ITA", "FRA", "ITA", "DEU"]


@pytest.mark.asyncio
async def test_fetch_region_ids_by_unique_id_jsonb_collection_ordered_by_fid(
    app_lifespan,
) -> None:
    from dynastore.extensions.region_mapping.claims import fetch_region_ids_by_unique_id
    from dynastore.tools.cache import cache_clear

    cache_clear(fetch_region_ids_by_unique_id)

    catalog_id = f"cat_{generate_id_hex()}"
    collection_id = f"coll_{generate_id_hex()}"
    await _provision_catalog(catalog_id)
    await _create_collection(catalog_id, collection_id, columnar=False)

    catalogs = get_protocol(CatalogsProtocol)
    assert catalogs is not None
    for i, (code, fid) in enumerate([("DEU", 2), ("FRA", 0), ("ITA", 1)]):
        await catalogs.upsert(catalog_id, collection_id, _feature(f"row-{i}", code, fid))

    values = await fetch_region_ids_by_unique_id(catalog_id, collection_id, "region_code", "fid")

    assert values == ["FRA", "ITA", "DEU"]


@pytest.mark.asyncio
async def test_fetch_region_ids_by_unique_id_unknown_collection_returns_empty(
    app_lifespan,
) -> None:
    """A collection with no physical storage (never created) degrades to an
    empty list rather than raising."""
    from dynastore.extensions.region_mapping.claims import fetch_region_ids_by_unique_id
    from dynastore.tools.cache import cache_clear

    cache_clear(fetch_region_ids_by_unique_id)

    values = await fetch_region_ids_by_unique_id(
        f"cat_{generate_id_hex()}", f"coll_{generate_id_hex()}", "region_code", "fid",
    )

    assert values == []
