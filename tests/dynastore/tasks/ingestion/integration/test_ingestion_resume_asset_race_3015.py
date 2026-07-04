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

"""Real-Postgres end-to-end regression coverage for GeoID #3015.

#3010 made ``run_ingestion_task``'s resume path probe for a pre-existing
asset via ``AssetsProtocol.get_asset(..., ctx=DriverContext(db_resource=engine))``
before falling back to ``create_asset``. A subsequent dev deploy carrying
that fix still hit the exact same ``23505`` duplicate-key failure on
``create_asset`` — the probe legitimately found nothing (no exception, no
error log — because it genuinely doesn't exist) and ``create_asset`` then
collided with the asset's ``(catalog_id, collection_id, href)`` uniqueness
constraint.

Root cause (confirmed by this test against real Postgres, not mocks): the
asset that satisfies this href was registered under a *different* asset_id
than the one this resume request derives from the URI basename (e.g. it was
created via a separate flow — direct upload, or an earlier request that
supplied an explicit ``asset_id`` — while this resume request's
``asset.asset_id`` is unset). ``get_asset(asset_id_for_lookup, ...)`` is
correctly ``None`` for that asset_id; retrying the identical lookup (as a
naive fix would) only repeats the miss. The fix in ``main_ingestion.py``
catches the href-uniqueness conflict on ``create_asset`` and looks the row
up by ``href`` instead (``AssetsProtocol.search_assets``), reusing whatever
asset_id it actually carries.

``test_ingestion_resume_asset_reuse_3001.py`` covers the *other* half of
#3010/#3015 (the call-site contract — the right ``ctx``/args reach
``get_asset``) against a fully mocked ``asset_manager``; this test exercises
the real ``AssetService`` -> ``AssetPostgresqlDriver`` chain end to end
through the real ``run_ingestion_task``, which is exactly the gap #3015
reported.
"""
from __future__ import annotations

import contextlib
from typing import Any, Dict, List
from unittest.mock import AsyncMock, patch

import pytest

from dynastore.models.driver_context import DriverContext
from dynastore.models.protocols import AssetsProtocol, CatalogsProtocol
from dynastore.modules.catalog.asset_service import VirtualAssetCreate
from dynastore.tools.discovery import get_protocol

_HREF = "gs://fao-aip-geospatial-review-data/demo_data/ph4_sc7ao_network_smooth.gpkg"
# The asset_id an out-of-band flow (e.g. direct upload) chose explicitly —
# deliberately NOT what the ingestion resume path would derive from the URI
# basename ("ph4_sc7ao_network_smooth_gpkg"), reproducing the identity
# mismatch that makes the get_asset probe legitimately miss.
_EXISTING_ASSET_ID = "manually_registered_network_asset"

pytestmark = pytest.mark.enable_modules(
    "db_config", "db", "catalog", "collection_postgresql", "catalog_postgresql",
)


class _FakeReporter:
    def __init__(self) -> None:
        self.finished_calls: List[Any] = []

    async def task_started(self, *args, **kwargs) -> None:
        pass

    async def update_progress(self, *args, **kwargs) -> None:
        pass

    async def process_batch_outcome(self, outcomes) -> None:
        pass

    async def task_finished(self, status, **kwargs) -> None:
        self.finished_calls.append((status, kwargs))


def _make_reader_class(records: List[Dict[str, Any]]):
    class _FakeReader:
        reader_id = "fake_reader"

        def feature_count(self, path, content_type=None):
            return len(records)

        def open(self, path, **kwargs):
            return contextlib.nullcontext(iter(records))

    return _FakeReader


def _feature(i: int) -> Dict[str, Any]:
    return {
        "id": f"f{i}",
        "geometry": {"type": "Point", "coordinates": [float(i), float(i)]},
        "properties": {"name": f"row-{i}"},
    }


@pytest.mark.asyncio
async def test_resume_recovers_by_href_when_asset_id_derivation_mismatches(
    app_lifespan, data_id
):
    """Regression for GeoID #3015: a resume request that omits asset_id must
    still complete when the href is already registered under a different
    (e.g. manually-assigned) asset_id — recovering via a real href lookup,
    not failing on the real duplicate-key conflict."""
    from dynastore.tasks.ingestion.main_ingestion import run_ingestion_task
    from dynastore.tasks.ingestion.ingestion_models import (
        ColumnMappingConfig,
        IngestionAsset,
        TaskIngestionRequest,
    )
    from tests.dynastore.modules.catalog.integration.test_get_asset_task_context_3015 import (
        _seed_catalog_and_collection,
    )

    catalog_id = f"demo8m_{data_id}"
    collection_id = "network"
    engine = app_lifespan.engine

    await _seed_catalog_and_collection(engine, catalog_id, collection_id)

    # Pre-register the asset under an explicit asset_id, exactly as a direct
    # upload / manual registration (or an earlier request that supplied one)
    # would — not the id this resume request will derive from the URI.
    assets = get_protocol(AssetsProtocol)
    existing = await assets.create_asset(
        catalog_id,
        VirtualAssetCreate(asset_id=_EXISTING_ASSET_ID, href=_HREF, metadata={}),
        collection_id,
        ctx=DriverContext(db_resource=engine),
    )
    assert existing.asset_id == _EXISTING_ASSET_ID

    # Obtain the REAL registered CatalogsProtocol provider and monkeypatch
    # only the heavy, ingestion-pipeline-irrelevant surface (collection
    # lifecycle / config / items upsert) — id resolution, schema resolution
    # and .assets (AssetService) stay 100% real, so the asset lookup/create
    # race under test runs against real Postgres end to end.
    real_catalog_module = get_protocol(CatalogsProtocol)
    assert real_catalog_module is not None

    with patch.object(
        real_catalog_module, "ensure_collection_exists", new=AsyncMock(),
    ), patch.object(
        real_catalog_module, "get_collection_config", new=AsyncMock(return_value=None),
    ), patch.object(
        real_catalog_module, "upsert", new=AsyncMock(return_value=[]),
    ), patch(
        "dynastore.tasks.ingestion.main_ingestion.initialize_reporters",
        return_value=(reporters := [_FakeReporter()]),
    ), patch(
        "dynastore.tasks.ingestion.readers.resolve_reader",
        return_value=_make_reader_class([_feature(0)]),
    ), patch(
        "dynastore.modules.storage.router.get_write_drivers",
        new=AsyncMock(return_value=[]),
    ), patch(
        "dynastore.tasks.ingestion.main_ingestion._maybe_apply_ingest_backpressure",
        new=AsyncMock(),
    ), patch(
        "dynastore.tasks.ingestion.main_ingestion.recalculate_and_update_extents",
        new=AsyncMock(return_value=None),
    ), patch(
        "dynastore.tasks.ingestion.main_ingestion.enqueue_collection_reindex_task",
        new=AsyncMock(),
    ), patch(
        "dynastore.tasks.ingestion.temp_reaper.reap_orphan_task_dirs",
        new=AsyncMock(),
    ), patch(
        "dynastore.tasks.ingestion.main_ingestion._persist_ingestion_cursor",
        new=AsyncMock(),
    ):
        # Resume request omits asset_id (as the reported production request
        # did) — main_ingestion derives "ph4_sc7ao_network_smooth_gpkg" from
        # the URI, which does NOT match _EXISTING_ASSET_ID.
        await run_ingestion_task(
            engine=engine,
            task_id="task-resume-3015",
            catalog_id=catalog_id,
            collection_id=collection_id,
            task_request=TaskIngestionRequest(
                asset=IngestionAsset(asset_id=None, uri=_HREF),
                column_mapping=ColumnMappingConfig(),
                database_batch_size=50,
                offset=0,
            ),
        )

    assert reporters[0].finished_calls[0][0] == "COMPLETED"

    # No duplicate asset was created — the href-conflict recovery must have
    # reused the existing row rather than minting a second one.
    remaining = await assets.search_assets(
        catalog_id,
        filters=[],
        collection_id=collection_id,
        limit=10,
        db_resource=engine,
    )
    assert len(remaining) == 1
    assert remaining[0].asset_id == _EXISTING_ASSET_ID
