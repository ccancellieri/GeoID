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

"""Real-Postgres regression coverage for GeoID #3015.

#3010 made the ingestion resume path probe for a pre-existing asset via
``AssetsProtocol.get_asset(..., ctx=DriverContext(db_resource=engine))``
before falling back to ``create_asset``. That fix's own regression test
(``test_ingestion_resume_asset_reuse_3001.py``) exercises the call-site logic
against a mocked ``asset_manager`` — it never runs the real
``AssetService.get_asset`` -> ``AssetPostgresqlDriver.get_asset`` path
against a real Postgres row, which is exactly the gap #3015 reported: a
fresh dev deploy carrying the #3010 fix still hit the same duplicate-key
error because the probe silently returned ``None`` for a row that
demonstrably existed.

This test exercises the real driver stack (``CatalogService.resolve_catalog_id``
/ ``resolve_physical_schema`` / ``resolve_collection_id``, ``AssetService`` and
``AssetPostgresqlDriver``) against a real Postgres database, and never
publishes a ``RequestVisibility`` (no HTTP request ever passes through the
IAM middleware in this test — matching how a Cloud Run Job process actually
executes ``run_ingestion_task``: no FastAPI request, no authorization
middleware, just direct in-process protocol calls). It confirms a
background/task-context caller can read back an asset it just created,
addressed the same way ``main_ingestion.run_ingestion_task``'s resume probe
addresses it: external catalog/collection ids and
``ctx=DriverContext(db_resource=<real engine>)``.

Catalog/collection registry rows are seeded directly through the same
internal DDL helpers the real (always-async) ``create_catalog`` /
``create_collection`` provisioning path uses (``_insert_catalog_row_with_pk_retry``
+ ``CatalogService._run_core_init`` + ``_insert_collection_row_with_pk_retry``)
rather than going through the async ``catalog_provision`` checklist/task
machinery — that machinery is a separate, unrelated concern (background task
dispatch/timing) from the asset-visibility bug under test here, and driving
it end-to-end only adds unrelated flakiness to this regression test.
"""
from __future__ import annotations

import pytest

from dynastore.models.driver_context import DriverContext
from dynastore.models.protocols import AssetsProtocol, CatalogsProtocol
from dynastore.modules.catalog.asset_service import VirtualAssetCreate
from dynastore.modules.catalog.models import Catalog
from dynastore.modules.db_config.query_executor import managed_transaction
from dynastore.tools.discovery import get_protocol

_HREF = "gs://fao-aip-geospatial-review-data/demo_data/ph4_sc7ao_network_smooth.gpkg"
_ASSET_ID = "ph4_sc7ao_network_smooth_gpkg"

pytestmark = pytest.mark.enable_modules(
    "db_config", "db", "catalog", "collection_postgresql", "catalog_postgresql",
)


async def _seed_catalog_and_collection(engine, catalog_external_id: str, collection_external_id: str):
    """Seed a real catalog + collection registry row synchronously, bypassing
    the async ``catalog_provision`` checklist/task machinery (unrelated to
    this regression). Returns (internal_catalog_id, internal_collection_id)."""
    from dynastore.modules.catalog.catalog_service import (
        _insert_catalog_row_with_pk_retry,
    )
    from dynastore.modules.catalog.collection_service import (
        _insert_collection_row_with_pk_retry,
    )

    catalogs = get_protocol(CatalogsProtocol)
    assert catalogs is not None

    async with managed_transaction(engine) as conn:
        internal_catalog_id = await _insert_catalog_row_with_pk_retry(
            conn, external_id=catalog_external_id, provisioning_status="ready",
        )
        # resolve_physical_schema is internal-id-first and the physical
        # schema name IS the catalog's internal id (see its docstring).
        catalog_model = Catalog(id=internal_catalog_id, title=catalog_external_id)
        await catalogs._run_core_init(
            conn, catalog_model, catalog_external_id, internal_catalog_id,
        )
        internal_collection_id = await _insert_collection_row_with_pk_retry(
            conn,
            phys_schema=internal_catalog_id,
            external_id=collection_external_id,
            catalog_id=internal_catalog_id,
            lifecycle_status=None,
        )

    return internal_catalog_id, internal_collection_id


@pytest.mark.asyncio
async def test_get_asset_resume_probe_finds_real_row_no_visibility_context(
    app_lifespan, data_id
):
    """Create a real asset row, then probe for it exactly the way
    ``run_ingestion_task``'s resume path does — external catalog/collection
    ids, a real engine via ``ctx=DriverContext(db_resource=...)``, and no
    caller identity published (background task context). Must find it."""
    catalog_id = f"demo8m_{data_id}"
    collection_id = "network"
    engine = app_lifespan.engine

    await _seed_catalog_and_collection(engine, catalog_id, collection_id)

    assets = get_protocol(AssetsProtocol)

    created = await assets.create_asset(
        catalog_id,
        VirtualAssetCreate(asset_id=_ASSET_ID, href=_HREF, metadata={}),
        collection_id,
        ctx=DriverContext(db_resource=engine),
    )
    assert created.asset_id == _ASSET_ID

    # This is the exact call shape main_ingestion.py's resume probe makes
    # (see run_ingestion_task): external ids, ctx carrying the task's own
    # engine, no RequestVisibility ever published in this process.
    found = await assets.get_asset(
        catalog_id, _ASSET_ID, collection_id,
        ctx=DriverContext(db_resource=engine),
    )

    assert found is not None, (
        "get_asset must find the asset it just created when called the same "
        "way the ingestion resume path calls it (#3015)"
    )
    assert found.asset_id == _ASSET_ID
    assert found.href == _HREF
