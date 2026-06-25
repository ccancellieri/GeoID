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

"""Unit tests for modules/renders/preseed_sync.

Pure: all external protocols and DB calls are monkeypatched.
"""
from __future__ import annotations

from typing import Any, Dict, List
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

import dynastore.modules.renders.preseed_sync as ps
from dynastore.modules.renders.config import RenderPreseedConfig
from dynastore.modules.renders.preseed_sync import enqueue_render_preseed_task

# The enqueue helper imports get_protocol lazily from dynastore.modules;
# patch the source location so the lazy import picks up the mock.
_GET_PROTOCOL_PATH = "dynastore.modules.get_protocol"
# tasks_module.create_task is called directly from within the function
_CREATE_TASK_PATH = "dynastore.modules.tasks.tasks_module.create_task"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_cfg(**kwargs: Any) -> RenderPreseedConfig:
    base = dict(
        enabled=True,
        min_zoom=0,
        max_zoom=4,
        seed_raster=True,
        seed_vector=False,
        tms_ids=["WebMercatorQuad"],
        style_id="sld_fire",
    )
    base.update(kwargs)
    return RenderPreseedConfig(**base)


class _FakeTask:
    def __init__(self, task_id: str = "abc-123"):
        self.task_id = task_id


# ---------------------------------------------------------------------------
# enqueue_render_preseed_task
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_enqueue_skipped_when_disabled(monkeypatch):
    """No task row when RenderPreseedConfig.enabled is False."""
    cfg = _make_cfg(enabled=False)

    cfg_svc_mock = MagicMock()
    cfg_svc_mock.get_config = AsyncMock(return_value=cfg)

    created: List[Any] = []

    async def _fake_create_task(engine, task_data, schema, initial_status="PENDING"):
        created.append(task_data)
        return _FakeTask()

    with (
        patch(_GET_PROTOCOL_PATH, return_value=cfg_svc_mock),
        patch(_CREATE_TASK_PATH, _fake_create_task),
    ):
        result = await enqueue_render_preseed_task(
            "cat1", "col1", "asset1",
            producer_kind="raster",
            engine=object(),
            schema="s_cat1",
        )

    assert result is False
    assert created == []


@pytest.mark.asyncio
async def test_enqueue_raster_creates_task(monkeypatch):
    """Enabled + raster → task row inserted with correct inputs."""
    cfg = _make_cfg(enabled=True, seed_raster=True)

    cfg_svc_mock = MagicMock()
    cfg_svc_mock.get_config = AsyncMock(return_value=cfg)

    created: List[Any] = []

    async def _fake_create_task(engine, task_data, schema, initial_status="PENDING"):
        created.append(task_data)
        return _FakeTask()

    with (
        patch(_GET_PROTOCOL_PATH, return_value=cfg_svc_mock),
        patch(_CREATE_TASK_PATH, _fake_create_task),
    ):
        result = await enqueue_render_preseed_task(
            "cat1", "col1", "asset1",
            producer_kind="raster",
            engine=object(),
            schema="s_cat1",
        )

    assert result is True
    assert len(created) == 1
    tc = created[0]
    assert tc.task_type == "render_preseed"
    assert tc.inputs["producer_kind"] == "raster"
    assert tc.inputs["min_zoom"] == 0
    assert tc.inputs["max_zoom"] == 4
    assert tc.inputs["style_id"] == "sld_fire"
    assert tc.dedup_key == "render-preseed:cat1:col1:raster"


@pytest.mark.asyncio
async def test_enqueue_vector_skipped_when_seed_vector_false(monkeypatch):
    """seed_vector=False → no task for vector kind."""
    cfg = _make_cfg(enabled=True, seed_raster=True, seed_vector=False)

    cfg_svc_mock = MagicMock()
    cfg_svc_mock.get_config = AsyncMock(return_value=cfg)

    created: List[Any] = []

    async def _fake_create_task(engine, task_data, schema, initial_status="PENDING"):
        created.append(task_data)
        return _FakeTask()

    with (
        patch(_GET_PROTOCOL_PATH, return_value=cfg_svc_mock),
        patch(_CREATE_TASK_PATH, _fake_create_task),
    ):
        result = await enqueue_render_preseed_task(
            "cat1", "col1", "asset1",
            producer_kind="vector",
            engine=object(),
            schema="s_cat1",
        )

    assert result is False
    assert created == []


@pytest.mark.asyncio
async def test_enqueue_dedup_hit_returns_false(monkeypatch):
    """create_task returning None (dedup) → enqueue returns False."""
    cfg = _make_cfg(enabled=True, seed_raster=True)

    cfg_svc_mock = MagicMock()
    cfg_svc_mock.get_config = AsyncMock(return_value=cfg)

    async def _fake_create_task(engine, task_data, schema, initial_status="PENDING"):
        return None  # dedup hit

    with (
        patch(_GET_PROTOCOL_PATH, return_value=cfg_svc_mock),
        patch(_CREATE_TASK_PATH, _fake_create_task),
    ):
        result = await enqueue_render_preseed_task(
            "cat1", "col1", "asset1",
            producer_kind="raster",
            engine=object(),
            schema="s_cat1",
        )

    assert result is False


@pytest.mark.asyncio
async def test_enqueue_never_raises_on_exception(monkeypatch):
    """Any internal failure → returns False, never raises."""

    with patch(_GET_PROTOCOL_PATH, side_effect=RuntimeError("kaboom")):
        result = await enqueue_render_preseed_task(
            "cat1", "col1", "asset1",
            producer_kind="raster",
            engine=object(),
            schema="s_cat1",
        )

    assert result is False


@pytest.mark.asyncio
async def test_enqueue_style_id_defaults_to_literal_default(monkeypatch):
    """When style_id is None in config, the stored key uses 'default'."""
    cfg = _make_cfg(enabled=True, seed_raster=True, style_id=None)

    cfg_svc_mock = MagicMock()
    cfg_svc_mock.get_config = AsyncMock(return_value=cfg)

    created: List[Any] = []

    async def _fake_create_task(engine, task_data, schema, initial_status="PENDING"):
        created.append(task_data)
        return _FakeTask()

    with (
        patch(_GET_PROTOCOL_PATH, return_value=cfg_svc_mock),
        patch(_CREATE_TASK_PATH, _fake_create_task),
    ):
        await enqueue_render_preseed_task(
            "cat1", "col1", "asset1",
            producer_kind="raster",
            engine=object(),
            schema="s_cat1",
        )

    assert created[0].inputs["style_id"] == "default"


# ---------------------------------------------------------------------------
# RenderPreseedSubscriber.on_asset_creation
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_subscriber_enqueues_raster_for_data_role(monkeypatch):
    """Asset with role='data' → producer_kind='raster' obligation."""
    enqueue_calls: List[Dict] = []

    async def _fake_enqueue(catalog_id, collection_id, asset_id, **kwargs):
        enqueue_calls.append({"catalog_id": catalog_id, "kind": kwargs["producer_kind"]})
        return True

    async def _fake_resolve_schema(catalog_id, ctx=None):
        return "s_cat1"

    catalogs_mock = MagicMock()
    catalogs_mock.resolve_physical_schema = _fake_resolve_schema

    monkeypatch.setattr(ps, "enqueue_render_preseed_task", _fake_enqueue)

    fake_engine = MagicMock()  # any non-None truthy value
    fake_ctx = MagicMock()

    with (
        patch(_GET_PROTOCOL_PATH, return_value=catalogs_mock),
        patch("dynastore.tools.protocol_helpers.get_engine", return_value=fake_engine),
        # DriverContext validates db_resource type; stub it out so test stays pure
        patch("dynastore.models.driver_context.DriverContext", return_value=fake_ctx),
    ):
        from dynastore.modules.renders.preseed_sync import RenderPreseedSubscriber
        await RenderPreseedSubscriber.on_asset_creation(
            catalog_id="cat1",
            collection_id="col1",
            asset_id="a1",
            payload={"role": "data"},
        )

    assert len(enqueue_calls) == 1
    assert enqueue_calls[0]["kind"] == "raster"


@pytest.mark.asyncio
async def test_subscriber_skips_thumbnail_role(monkeypatch):
    """Asset with role='thumbnail' → no obligation enqueued."""
    enqueue_calls: List[Dict] = []

    async def _fake_enqueue(catalog_id, collection_id, asset_id, **kwargs):
        enqueue_calls.append({"kind": kwargs["producer_kind"]})
        return True

    monkeypatch.setattr(ps, "enqueue_render_preseed_task", _fake_enqueue)

    from dynastore.modules.renders.preseed_sync import RenderPreseedSubscriber
    await RenderPreseedSubscriber.on_asset_creation(
        catalog_id="cat1",
        collection_id="col1",
        asset_id="a1",
        payload={"role": "thumbnail"},
    )

    assert enqueue_calls == []


@pytest.mark.asyncio
async def test_subscriber_skips_when_catalog_id_missing():
    """Missing catalog_id → early return, no error."""
    from dynastore.modules.renders.preseed_sync import RenderPreseedSubscriber
    # Must not raise
    await RenderPreseedSubscriber.on_asset_creation(
        catalog_id=None,
        collection_id="col1",
        asset_id="a1",
    )
