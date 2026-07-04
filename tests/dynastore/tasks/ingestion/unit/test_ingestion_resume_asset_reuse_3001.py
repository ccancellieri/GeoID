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

"""Regression coverage for GeoID #3001: resuming an ingestion whose asset was
already created on a prior attempt must reuse it, not attempt a duplicate
``create_asset`` that dies on the asset's href-uniqueness constraint.

Two compounding bugs produced the reported ``23505`` on
``assets_uq_href_*``:

1. When the request omitted ``asset.asset_id`` (only ``uri`` was given), the
   task never probed for a pre-existing asset at all — it derived an id from
   the URI and went straight to ``create_asset``.
2. When ``asset.asset_id`` *was* given explicitly, the existence check called
   ``asset_manager.get_asset(...)`` without a ``ctx``/``db_resource``, riding
   ``AssetService``'s own cached read path instead of the task's own engine —
   a stale "not found" negative there sends a fast resume straight back into
   ``create_asset``.

Either path lands on ``create_asset``, whose upsert only covers the
identity constraint (``assets_identity_uq``) — a *different*, unmatched
unique index on the asset's href still raises a normal ``23505`` for an
already-existing virtual asset, even on an exact re-insert.

This harness fakes ``asset_manager.get_asset`` to only return the
pre-existing asset when called WITH a ``ctx`` carrying a ``db_resource``
(reproducing bug #2 faithfully) and fakes ``create_asset`` to raise the real
duplicate-key error were it ever invoked — so these tests fail loudly if the
resume path regresses to skipping/misusing the existence check.
"""
from __future__ import annotations

import contextlib
from typing import Any, Dict, List, Optional
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from sqlalchemy.ext.asyncio import AsyncEngine

from dynastore.modules.catalog.asset_service import AssetKind, AssetStatus
from dynastore.tasks.ingestion.ingestion_models import (
    ColumnMappingConfig,
    IngestionAsset,
    TaskIngestionRequest,
)

_HREF = "gs://fao-aip-geospatial-review-data/demo_data/ph4_sc7ao_network_smooth.gpkg"
_DERIVED_ASSET_ID = "ph4_sc7ao_network_smooth_gpkg"


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


class _DuplicateHrefError(Exception):
    """Stands in for the real asyncpg/SQLAlchemy 23505 on
    ``assets_uq_href_*`` — create_asset must never be reached in these tests,
    so any call raises loudly instead of silently succeeding."""


def _make_asset_manager(existing_asset: MagicMock) -> MagicMock:
    """An asset_manager whose ``get_asset`` only finds the pre-existing
    asset when called with a ``ctx`` carrying a ``db_resource`` — exactly
    the pre-fix engine/cache mismatch — and whose ``create_asset`` always
    raises the real duplicate-key error (it must never be invoked once the
    existence check is reading through the right connection)."""

    async def _get_asset(catalog_id, asset_id, collection_id=None, ctx=None):
        if ctx is None or ctx.db_resource is None:
            return None
        if asset_id == existing_asset.asset_id:
            return existing_asset
        return None

    async def _create_asset(*args, **kwargs):
        raise _DuplicateHrefError(
            'duplicate key value violates unique constraint '
            '"assets_uq_href_x" DETAIL: Key (catalog_id, collection_id, href)'
            f"=(cat1, col1, {_HREF}) already exists."
        )

    asset_manager = MagicMock()
    asset_manager.get_asset = AsyncMock(side_effect=_get_asset)
    asset_manager.create_asset = AsyncMock(side_effect=_create_asset)
    asset_manager.add_asset_reference = AsyncMock()
    return asset_manager


def _make_fake_catalog_module(asset_manager: MagicMock) -> MagicMock:
    catalog_module = MagicMock()
    catalog_module.resolve_physical_schema = AsyncMock(return_value="tasks")
    catalog_module.ensure_collection_exists = AsyncMock()
    catalog_module.get_collection_config = AsyncMock(return_value=None)
    catalog_module.configs = MagicMock()
    catalog_module.configs.get_config = AsyncMock(return_value=None)
    catalog_module.get_catalog = AsyncMock(return_value=MagicMock())
    catalog_module.get_collection = AsyncMock(return_value=MagicMock())
    catalog_module.upsert = AsyncMock(return_value=[])
    catalog_module.assets = asset_manager
    return catalog_module


@contextlib.contextmanager
def _run_ingestion_harness(records, catalog_module):
    reporters = [_FakeReporter()]
    with (
        patch("dynastore.tools.discovery.get_protocol", return_value=catalog_module),
        patch(
            "dynastore.tasks.ingestion.main_ingestion.initialize_reporters",
            return_value=reporters,
        ),
        patch(
            "dynastore.tasks.ingestion.readers.resolve_reader",
            return_value=_make_reader_class(records),
        ),
        patch(
            "dynastore.modules.storage.router.get_write_drivers",
            new=AsyncMock(return_value=[]),
        ),
        patch(
            "dynastore.tasks.ingestion.main_ingestion._maybe_apply_ingest_backpressure",
            new=AsyncMock(),
        ),
        patch(
            "dynastore.tasks.ingestion.main_ingestion.recalculate_and_update_extents",
            new=AsyncMock(return_value=None),
        ),
        patch(
            "dynastore.tasks.ingestion.main_ingestion.enqueue_collection_reindex_task",
            new=AsyncMock(),
        ),
        patch(
            "dynastore.tasks.ingestion.temp_reaper.reap_orphan_task_dirs",
            new=AsyncMock(),
        ),
        patch(
            "dynastore.tasks.ingestion.main_ingestion._persist_ingestion_cursor",
            new=AsyncMock(),
        ),
    ):
        yield reporters


def _existing_asset() -> MagicMock:
    asset = MagicMock()
    asset.asset_id = _DERIVED_ASSET_ID
    asset.kind = AssetKind.VIRTUAL
    asset.status = AssetStatus.ACTIVE
    asset.href = _HREF
    asset.uri = None
    asset.metadata = {}
    return asset


def _feature(i: int) -> Dict[str, Any]:
    return {
        "id": f"f{i}",
        "geometry": {"type": "Point", "coordinates": [float(i), float(i)]},
        "properties": {"name": f"row-{i}"},
    }


def _make_task_request(*, asset_id: Optional[str]) -> TaskIngestionRequest:
    return TaskIngestionRequest(
        asset=IngestionAsset(asset_id=asset_id, uri=_HREF),
        column_mapping=ColumnMappingConfig(),
        database_batch_size=50,
        offset=0,
    )


@pytest.mark.asyncio
async def test_resume_with_explicit_matching_asset_id_reuses_existing_asset():
    """Request shape #2 from the bug report: an explicit asset_id matching
    the deterministic derivation. Must reuse the existing asset — never call
    create_asset."""
    from dynastore.tasks.ingestion.main_ingestion import run_ingestion_task

    asset_manager = _make_asset_manager(_existing_asset())
    catalog_module = _make_fake_catalog_module(asset_manager)
    records = [_feature(0)]

    with _run_ingestion_harness(records, catalog_module) as reporters:
        await run_ingestion_task(
            engine=MagicMock(spec=AsyncEngine),
            task_id="task-resume-3001-a",
            catalog_id="cat1",
            collection_id="col1",
            task_request=_make_task_request(asset_id=_DERIVED_ASSET_ID),
        )

    assert reporters[0].finished_calls[0][0] == "COMPLETED"
    asset_manager.create_asset.assert_not_awaited()
    # The lookup must have read through the task's own engine, not the
    # AssetService cached path (ctx/db_resource omitted).
    _, kwargs = asset_manager.get_asset.await_args_list[0]
    assert kwargs.get("ctx") is not None
    assert kwargs["ctx"].db_resource is not None


@pytest.mark.asyncio
async def test_resume_without_asset_id_derives_and_reuses_existing_asset():
    """Request shape #1 from the bug report: no asset_id, only uri. Must
    derive the same asset_id as at creation time, find the existing asset,
    and never call create_asset."""
    from dynastore.tasks.ingestion.main_ingestion import run_ingestion_task

    asset_manager = _make_asset_manager(_existing_asset())
    catalog_module = _make_fake_catalog_module(asset_manager)
    records = [_feature(0)]

    with _run_ingestion_harness(records, catalog_module) as reporters:
        await run_ingestion_task(
            engine=MagicMock(spec=AsyncEngine),
            task_id="task-resume-3001-b",
            catalog_id="cat1",
            collection_id="col1",
            task_request=_make_task_request(asset_id=None),
        )

    assert reporters[0].finished_calls[0][0] == "COMPLETED"
    asset_manager.create_asset.assert_not_awaited()
    args, kwargs = asset_manager.get_asset.await_args_list[0]
    assert args[1] == _DERIVED_ASSET_ID
    assert kwargs.get("ctx") is not None
    assert kwargs["ctx"].db_resource is not None


@pytest.mark.asyncio
async def test_create_asset_is_used_when_no_existing_asset_found():
    """Sanity check: when no prior asset exists at all, the normal
    create-on-first-ingest path still fires (the fake create_asset here
    returns a fresh asset instead of raising, unlike the "already exists"
    harness above)."""
    from dynastore.tasks.ingestion.main_ingestion import run_ingestion_task

    asset_manager = MagicMock()
    asset_manager.get_asset = AsyncMock(return_value=None)
    created = _existing_asset()
    asset_manager.create_asset = AsyncMock(return_value=created)
    asset_manager.add_asset_reference = AsyncMock()
    catalog_module = _make_fake_catalog_module(asset_manager)
    records = [_feature(0)]

    with _run_ingestion_harness(records, catalog_module) as reporters:
        await run_ingestion_task(
            engine=MagicMock(spec=AsyncEngine),
            task_id="task-resume-3001-c",
            catalog_id="cat1",
            collection_id="col1",
            task_request=_make_task_request(asset_id=None),
        )

    assert reporters[0].finished_calls[0][0] == "COMPLETED"
    asset_manager.create_asset.assert_awaited_once()
