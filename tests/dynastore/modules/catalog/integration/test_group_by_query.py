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

"""Real-PG regression coverage for ``QueryRequest.group_by`` (#2829).

Covers three request shapes against both attribute-storage shapes (JSONB blob
and COLUMNAR sidecar table):

- ``grouped_field_only`` — the exact shape region-mapping's
  ``fetch_distinct_region_ids`` used to issue: a single named
  ``FieldSelection``, matching ``group_by`` and ``sort`` on the same field.
- ``empty_select`` — ``group_by`` without separately re-selecting the field
  (the caller only wants the distinct values, no explicit ``select``).
- ``superset_select`` — a ``select`` that names a field ("id") which is
  neither grouped nor aggregated alongside the grouped field; that field must
  be silently dropped from the projection rather than producing a SQL error.
"""

from __future__ import annotations

import uuid
from typing import Callable, Dict, List

import pytest

from dynastore.models.protocols import CatalogsProtocol
from dynastore.models.query_builder import FieldSelection, QueryRequest, SortOrder
from dynastore.modules.storage.driver_config import ItemsPostgresqlDriverConfig
from dynastore.modules.storage.drivers.pg_sidecars.geometries_config import (
    GeometriesSidecarConfig,
)
from dynastore.modules.storage.drivers.pg_sidecars.attributes_config import (
    AttributeSchemaEntry,
    AttributeStorageMode,
    FeatureAttributeSidecarConfig,
    PostgresType,
)
from dynastore.modules.catalog.provisioning_registry import STATUS_READY
from dynastore.tools.discovery import get_protocol
from tests.dynastore.test_utils import generate_test_id

pytestmark = [
    pytest.mark.enable_modules(
        "db_config", "db", "catalog", "stats", "stac",
        "collection_postgresql", "catalog_postgresql", "tasks",
    ),
]


SELECT_SHAPES: Dict[str, Callable[[], List[FieldSelection]]] = {
    "grouped_field_only": lambda: [FieldSelection(field="region")],
    "empty_select": lambda: [],
    "superset_select": lambda: [
        FieldSelection(field="id"), FieldSelection(field="region"),
    ],
}


async def _provision_catalog(catalogs: CatalogsProtocol, catalog_id: str) -> None:
    """Create ``catalog_id`` and synchronously drive its provisioning task to
    ``ready`` (mirrors ``test_async_create_executor_e2e.py``): catalog create
    is always-async, so the tenant physical schema does not exist until the
    ``catalog_provision`` task runs — there is no background dispatcher in
    this test process to claim it, so the executor is invoked directly.
    """
    from dynastore.tasks.catalog_provision.task import (
        CatalogProvisionInputs,
        CatalogProvisionTask,
    )
    from dynastore.modules.tasks.models import TaskPayload

    await catalogs.delete_catalog(catalog_id, force=True)
    created = await catalogs.create_catalog(
        {"id": catalog_id, "title": {"en": catalog_id}}, lang="*"
    )
    internal_id = created.id

    payload = TaskPayload(
        task_id=uuid.uuid4(),
        caller_id="test",
        inputs=CatalogProvisionInputs(
            catalog_id=internal_id, scope="catalog", operation="provision",
        ),
    )
    await CatalogProvisionTask().run(payload)

    final = await catalogs.get_catalog(catalog_id)
    assert final.provisioning_status == STATUS_READY, (
        f"catalog '{catalog_id}' did not reach ready: {final.provisioning_status}"
    )


async def _make_collection(
    catalogs: CatalogsProtocol,
    catalog_id: str,
    collection_id: str,
    storage_mode: AttributeStorageMode,
) -> None:
    # JSONB mode: no declared ``attribute_schema`` — "region" lives as a free
    # key inside the shared JSONB blob and is resolved dynamically at query
    # time (``get_dynamic_field_definition``), matching how the region-mapping
    # source collections are actually configured. COLUMNAR mode declares
    # "region" so it materialises as a real, typed sidecar column.
    attr_sidecar = FeatureAttributeSidecarConfig(
        storage_mode=storage_mode,
        attribute_schema=(
            [AttributeSchemaEntry(name="region", type=PostgresType.TEXT)]
            if storage_mode == AttributeStorageMode.COLUMNAR
            else []
        ),
    )
    col_config = ItemsPostgresqlDriverConfig(
        sidecars=[GeometriesSidecarConfig(), attr_sidecar]
    )
    await catalogs.create_collection(
        catalog_id,
        {
            "id": collection_id,
            "title": {"en": f"group_by test ({storage_mode.value})"},
            "layer_config": col_config.model_dump(),
        },
        lang="*",
    )


async def _upsert_regions(
    catalogs: CatalogsProtocol, catalog_id: str, collection_id: str,
) -> None:
    """Three points: two carry ``region='A'``, one carries ``region='B'``."""
    for idx, region in enumerate(["A", "A", "B"]):
        await catalogs.upsert(
            catalog_id,
            collection_id,
            {
                "id": f"item-{idx}",
                "type": "Feature",
                "geometry": {"type": "Point", "coordinates": [idx, idx]},
                "properties": {"region": region},
            },
        )


def _group_by_region_request(select_shape: str) -> QueryRequest:
    return QueryRequest(
        select=SELECT_SHAPES[select_shape](),
        group_by=["region"],
        sort=[SortOrder(field="region")],
        limit=10,
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("select_shape", sorted(SELECT_SHAPES))
async def test_group_by_jsonb_attributes(
    app_lifespan, catalog_id, collection_id, select_shape,
):
    catalogs = get_protocol(CatalogsProtocol)
    assert catalogs is not None
    catalog_id = f"{catalog_id}_{generate_test_id()}"

    await _provision_catalog(catalogs, catalog_id)
    try:
        await _make_collection(
            catalogs, catalog_id, collection_id, AttributeStorageMode.JSONB
        )
        await _upsert_regions(catalogs, catalog_id, collection_id)

        features = await catalogs.search_items(
            catalog_id, collection_id, _group_by_region_request(select_shape)
        )

        values = sorted(
            (f.properties or {}).get("region") for f in features
        )
        assert values == ["A", "B"], (
            f"expected one row per distinct region, got {values!r}"
        )
    finally:
        await catalogs.delete_catalog(catalog_id, force=True)


@pytest.mark.asyncio
@pytest.mark.parametrize("select_shape", sorted(SELECT_SHAPES))
async def test_group_by_columnar_attributes(
    app_lifespan, catalog_id, collection_id, select_shape,
):
    catalogs = get_protocol(CatalogsProtocol)
    assert catalogs is not None
    catalog_id = f"{catalog_id}_{generate_test_id()}"

    await _provision_catalog(catalogs, catalog_id)
    try:
        await _make_collection(
            catalogs, catalog_id, collection_id, AttributeStorageMode.COLUMNAR
        )
        await _upsert_regions(catalogs, catalog_id, collection_id)

        features = await catalogs.search_items(
            catalog_id, collection_id, _group_by_region_request(select_shape)
        )

        values = sorted(
            (f.properties or {}).get("region") for f in features
        )
        assert values == ["A", "B"], (
            f"expected one row per distinct region, got {values!r} "
            "(a None/missing value here is the #2829 columnar silent-drop bug)"
        )
    finally:
        await catalogs.delete_catalog(catalog_id, force=True)
