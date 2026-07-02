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

"""Unit tests for the ``stac_harvester`` preset and the ``stac_harvest`` task mappers.

These tests do NOT touch the database, network, or OGC process engine.  All
external collaborators are mocked.  The test suite has two sections:

1. Preset apply() — verifies that applying stac_harvester with {url: ...} seeds
   a stac_harvest process/task with correctly mapped inputs.
2. Worker mapping — verifies that map_collection and map_item produce expected
   dynastore payloads from sample source dicts.
"""
from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest


# ---------------------------------------------------------------------------
# Helper: build a minimal PresetContext with a fake DB engine
# ---------------------------------------------------------------------------


def _make_ctx(db: Any = None, principal: Any = None, catalogs: Any = None) -> Any:
    from dynastore.modules.storage.presets.preset import PresetContext

    return PresetContext(
        db=db or MagicMock(),
        iam=None,
        policy=None,
        config=None,
        tasks=None,
        cron=None,
        libs=None,
        principal=principal,
        scope="catalog:test-cat",
        catalogs=catalogs,
    )


# ---------------------------------------------------------------------------
# 1. Preset apply() — seeds stac_harvest process with mapped inputs
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_preset_apply_submits_stac_harvest_process() -> None:
    """apply() must call execute_process with process_id='stac_harvest' and
    the inputs mapped from StacHarvesterParams."""
    from dynastore.extensions.stac.presets.stac_harvester import (
        STAC_HARVESTER_PRESET,
        StacHarvesterParams,
    )

    captured: list[dict] = []

    async def _fake_execute(process_id: str, exec_request: Any, *, engine: Any,
                             caller_id: str, preferred_mode: Any,
                             catalog_id: Any = None,
                             dedup_key: Any = None) -> MagicMock:
        result = MagicMock()
        result.jobID = "job-abc-123"
        captured.append({
            "process_id": process_id,
            "inputs": dict(exec_request.inputs),
            "catalog_id": catalog_id,
            "preferred_mode": preferred_mode,
        })
        return result

    ctx = _make_ctx()
    params = StacHarvesterParams(url="https://example.test/stac")

    with patch(
        "dynastore.modules.processes.processes_module.execute_process",
        _fake_execute,
    ):
        descriptor = await STAC_HARVESTER_PRESET.apply(params, "catalog:test-cat", ctx)

    assert len(captured) == 1, "execute_process should be called exactly once"
    call = captured[0]
    assert call["process_id"] == "stac_harvest"
    assert call["inputs"]["catalog_url"] == "https://example.test/stac"
    assert call["inputs"]["target_catalog"] == "test-cat"
    assert call["inputs"]["max_collections"] == 0
    assert call["inputs"]["max_items"] == 0
    assert call["inputs"]["with_assets"] is True
    assert call["inputs"]["drivers"] == "es"
    # catalog_id must be propagated so the task row lands in the catalog schema.
    assert call["catalog_id"] == "test-cat"
    # Harvest is always async (Cloud Run Job on GCP, async background task
    # elsewhere) — it is a heavy/offload-routed process.
    from dynastore.modules.processes.models import JobControlOptions
    assert call["preferred_mode"] == JobControlOptions.ASYNC_EXECUTE

    # Descriptor should record the job id and parameters.
    assert descriptor.payload["job_id"] == "job-abc-123"
    assert descriptor.payload["catalog_url"] == "https://example.test/stac"
    assert descriptor.payload["target_catalog"] == "test-cat"


@pytest.mark.asyncio
async def test_preset_apply_explicit_target_catalog_overrides_scope() -> None:
    """When target_catalog is explicitly set in params it wins over the scope."""
    from dynastore.extensions.stac.presets.stac_harvester import (
        STAC_HARVESTER_PRESET,
        StacHarvesterParams,
    )

    captured: list[dict] = []

    async def _fake_execute(process_id: str, exec_request: Any, **_kw: Any) -> MagicMock:
        captured.append({"inputs": dict(exec_request.inputs)})
        return MagicMock(jobID="job-xyz")

    ctx = _make_ctx()
    params = StacHarvesterParams(
        url="https://example.test/stac",
        target_catalog="explicit-cat",
    )

    with patch(
        "dynastore.modules.processes.processes_module.execute_process",
        _fake_execute,
    ):
        descriptor = await STAC_HARVESTER_PRESET.apply(params, "catalog:scope-cat", ctx)

    assert captured[0]["inputs"]["target_catalog"] == "explicit-cat"
    assert descriptor.payload["target_catalog"] == "explicit-cat"


@pytest.mark.asyncio
async def test_preset_apply_maps_all_params() -> None:
    """Custom max_collections / max_items / with_assets / drivers are forwarded."""
    from dynastore.extensions.stac.presets.stac_harvester import (
        STAC_HARVESTER_PRESET,
        StacHarvesterParams,
    )

    captured: list[dict] = []

    async def _fake_execute(process_id: str, exec_request: Any, **_kw: Any) -> MagicMock:
        captured.append({"inputs": dict(exec_request.inputs)})
        return MagicMock(jobID="job-params")

    ctx = _make_ctx()
    params = StacHarvesterParams(
        url="http://example.test/stac",
        max_collections=5,
        max_items=100,
        with_assets=False,
        drivers="pg_es",
    )

    with patch(
        "dynastore.modules.processes.processes_module.execute_process",
        _fake_execute,
    ):
        await STAC_HARVESTER_PRESET.apply(params, "catalog:test-cat", ctx)

    inp = captured[0]["inputs"]
    assert inp["max_collections"] == 5
    assert inp["max_items"] == 100
    assert inp["with_assets"] is False
    assert inp["drivers"] == "pg_es"


@pytest.mark.asyncio
async def test_preset_apply_raises_without_db() -> None:
    """apply() must raise RuntimeError when ctx.db is None (engine absent)."""
    from dynastore.extensions.stac.presets.stac_harvester import (
        STAC_HARVESTER_PRESET,
        StacHarvesterParams,
    )

    ctx = _make_ctx(db=None)
    ctx.db = None  # explicit sentinel

    params = StacHarvesterParams(url="https://example.test/stac")

    with pytest.raises(RuntimeError, match="engine.*is None"):
        await STAC_HARVESTER_PRESET.apply(params, "catalog:test-cat", ctx)


# ---------------------------------------------------------------------------
# 1b. Preset apply() — bucket-free target_catalog creation (defer hint)
# ---------------------------------------------------------------------------


def _ready_model() -> MagicMock:
    return MagicMock(provisioning_status="ready")


def _provisioning_model() -> MagicMock:
    return MagicMock(provisioning_status="provisioning")


def _failed_model() -> MagicMock:
    return MagicMock(provisioning_status="failed")


@pytest.mark.asyncio
async def test_preset_apply_creates_missing_target_catalog_bucket_free() -> None:
    """When target_catalog does not exist yet, apply() creates it with
    hints=frozenset({Hint.DEFER}) so no GCS bucket is provisioned for a
    harvest-only catalog, then waits for it to reach 'ready' before
    submitting the harvest job."""
    from dynastore.extensions.stac.presets.stac_harvester import (
        STAC_HARVESTER_PRESET,
        StacHarvesterParams,
    )
    from dynastore.modules.storage.hints import Hint

    catalogs = MagicMock()
    # 1st call: existence check (absent). 2nd call: readiness poll (ready
    # immediately — no need to actually sleep in this test).
    catalogs.get_catalog_model = AsyncMock(side_effect=[None, _ready_model()])
    catalogs.create_catalog = AsyncMock(return_value=MagicMock())

    ctx = _make_ctx(catalogs=catalogs)
    params = StacHarvesterParams(
        url="https://example.test/stac", target_catalog="fresh-harvest-cat",
    )

    async def _fake_execute(process_id: str, exec_request: Any, **_kw: Any) -> MagicMock:
        return MagicMock(jobID="job-defer")

    with patch(
        "dynastore.modules.processes.processes_module.execute_process",
        _fake_execute,
    ):
        await STAC_HARVESTER_PRESET.apply(params, "catalog:test-cat", ctx)

    assert catalogs.get_catalog_model.await_count == 2
    for call in catalogs.get_catalog_model.await_args_list:
        assert call.args == ("fresh-harvest-cat",)
    catalogs.create_catalog.assert_awaited_once()
    call = catalogs.create_catalog.await_args
    assert call.args[0]["id"] == "fresh-harvest-cat"
    assert call.kwargs["hints"] == frozenset({Hint.DEFER})


@pytest.mark.asyncio
async def test_preset_apply_leaves_existing_target_catalog_untouched() -> None:
    """When target_catalog already exists (and is ready), apply() must not
    call create_catalog — an already-provisioned catalog's storage state is
    never changed by this preset."""
    from dynastore.extensions.stac.presets.stac_harvester import (
        STAC_HARVESTER_PRESET,
        StacHarvesterParams,
    )

    catalogs = MagicMock()
    catalogs.get_catalog_model = AsyncMock(return_value=_ready_model())  # already exists + ready
    catalogs.create_catalog = AsyncMock()

    ctx = _make_ctx(catalogs=catalogs)
    params = StacHarvesterParams(
        url="https://example.test/stac", target_catalog="existing-cat",
    )

    async def _fake_execute(process_id: str, exec_request: Any, **_kw: Any) -> MagicMock:
        return MagicMock(jobID="job-existing")

    with patch(
        "dynastore.modules.processes.processes_module.execute_process",
        _fake_execute,
    ):
        await STAC_HARVESTER_PRESET.apply(params, "catalog:test-cat", ctx)

    catalogs.create_catalog.assert_not_awaited()


@pytest.mark.asyncio
async def test_preset_apply_waits_for_catalog_ready_before_submitting_harvest() -> None:
    """apply() must poll get_catalog_model until provisioning_status=='ready'
    — and NOT submit the harvest job while the newly-created catalog's tenant
    schema (catalog_core) is still an in-flight async task.  Regression guard
    for the create/harvest race: a harvest that wins the race would otherwise
    "succeed" silently with zero items."""
    from dynastore.extensions.stac.presets.stac_harvester import (
        STAC_HARVESTER_PRESET,
        StacHarvesterParams,
    )

    execute_calls: list[str] = []

    catalogs = MagicMock()
    # absent -> still provisioning -> still provisioning -> ready.
    catalogs.get_catalog_model = AsyncMock(
        side_effect=[None, _provisioning_model(), _provisioning_model(), _ready_model()]
    )
    catalogs.create_catalog = AsyncMock(return_value=MagicMock())

    ctx = _make_ctx(catalogs=catalogs)
    params = StacHarvesterParams(
        url="https://example.test/stac", target_catalog="slow-provision-cat",
    )

    async def _fake_execute(process_id: str, exec_request: Any, **_kw: Any) -> MagicMock:
        execute_calls.append(process_id)
        return MagicMock(jobID="job-waited")

    import dynastore.extensions.stac.presets.stac_harvester as harvester_mod

    with patch(
        "dynastore.modules.processes.processes_module.execute_process",
        _fake_execute,
    ), patch.object(harvester_mod, "_CATALOG_READY_POLL_INTERVAL_S", 0.01):
        await STAC_HARVESTER_PRESET.apply(params, "catalog:test-cat", ctx)

    # 1 existence check + 3 readiness polls (provisioning, provisioning, ready).
    assert catalogs.get_catalog_model.await_count == 4
    assert execute_calls == ["stac_harvest"]  # only submitted once ready


@pytest.mark.asyncio
async def test_preset_apply_raises_loudly_on_readiness_timeout() -> None:
    """apply() must raise RuntimeError (not silently proceed) when the target
    catalog never reaches 'ready' within the poll budget — never submits the
    harvest job against a catalog whose tenant schema may not exist."""
    from dynastore.extensions.stac.presets.stac_harvester import (
        STAC_HARVESTER_PRESET,
        StacHarvesterParams,
    )
    import dynastore.extensions.stac.presets.stac_harvester as harvester_mod

    execute_calls: list[str] = []

    catalogs = MagicMock()
    # Always absent-then-provisioning: existence check absent, then every
    # readiness poll reports 'provisioning' forever.
    catalogs.get_catalog_model = AsyncMock(
        side_effect=[None] + [_provisioning_model() for _ in range(1000)]
    )
    catalogs.create_catalog = AsyncMock(return_value=MagicMock())

    ctx = _make_ctx(catalogs=catalogs)
    params = StacHarvesterParams(
        url="https://example.test/stac", target_catalog="never-ready-cat",
    )

    async def _fake_execute(process_id: str, exec_request: Any, **_kw: Any) -> MagicMock:
        execute_calls.append(process_id)
        return MagicMock(jobID="should-not-happen")

    # Force the timeout branch to trip on the very first poll iteration
    # instead of a real 60s wait.
    with patch.object(harvester_mod, "_CATALOG_READY_TIMEOUT_S", -1.0), patch(
        "dynastore.modules.processes.processes_module.execute_process",
        _fake_execute,
    ):
        with pytest.raises(RuntimeError, match="did not reach 'ready'"):
            await STAC_HARVESTER_PRESET.apply(params, "catalog:test-cat", ctx)

    assert execute_calls == []  # harvest must never be submitted


@pytest.mark.asyncio
async def test_preset_apply_raises_loudly_when_catalog_provisioning_failed() -> None:
    """apply() must raise RuntimeError immediately (no need to wait out the
    full timeout) when the target catalog's provisioning reaches 'failed'."""
    from dynastore.extensions.stac.presets.stac_harvester import (
        STAC_HARVESTER_PRESET,
        StacHarvesterParams,
    )

    execute_calls: list[str] = []

    catalogs = MagicMock()
    catalogs.get_catalog_model = AsyncMock(side_effect=[None, _failed_model()])
    catalogs.create_catalog = AsyncMock(return_value=MagicMock())

    ctx = _make_ctx(catalogs=catalogs)
    params = StacHarvesterParams(
        url="https://example.test/stac", target_catalog="broken-cat",
    )

    async def _fake_execute(process_id: str, exec_request: Any, **_kw: Any) -> MagicMock:
        execute_calls.append(process_id)
        return MagicMock(jobID="should-not-happen")

    with patch(
        "dynastore.modules.processes.processes_module.execute_process",
        _fake_execute,
    ):
        with pytest.raises(RuntimeError, match="provisioning failed"):
            await STAC_HARVESTER_PRESET.apply(params, "catalog:test-cat", ctx)

    assert execute_calls == []


@pytest.mark.asyncio
async def test_preset_apply_continues_against_winner_on_concurrent_create_conflict() -> None:
    """When create_catalog raises UniqueViolationError (a peer apply won the
    create race for the same target_catalog), apply() must catch it, re-poll
    for readiness against the peer's catalog, and still submit the harvest
    job — never abort an idempotent apply on a benign create race."""
    from dynastore.extensions.stac.presets.stac_harvester import (
        STAC_HARVESTER_PRESET,
        StacHarvesterParams,
    )
    from dynastore.modules.db_config.exceptions import UniqueViolationError

    execute_calls: list[str] = []

    catalogs = MagicMock()
    # Existence check: absent (race not yet visible to us). Readiness poll
    # (after the conflict): the peer's catalog is already ready.
    catalogs.get_catalog_model = AsyncMock(side_effect=[None, _ready_model()])
    catalogs.create_catalog = AsyncMock(
        side_effect=UniqueViolationError("Catalog 'raced-cat' already exists")
    )

    ctx = _make_ctx(catalogs=catalogs)
    params = StacHarvesterParams(
        url="https://example.test/stac", target_catalog="raced-cat",
    )

    async def _fake_execute(process_id: str, exec_request: Any, **_kw: Any) -> MagicMock:
        execute_calls.append(process_id)
        return MagicMock(jobID="job-raced")

    with patch(
        "dynastore.modules.processes.processes_module.execute_process",
        _fake_execute,
    ):
        descriptor = await STAC_HARVESTER_PRESET.apply(params, "catalog:test-cat", ctx)

    catalogs.create_catalog.assert_awaited_once()
    assert execute_calls == ["stac_harvest"]
    assert descriptor.payload["job_id"] == "job-raced"


@pytest.mark.asyncio
async def test_preset_apply_skips_catalog_ensure_when_catalogs_protocol_absent() -> None:
    """When PresetContext.catalogs is None (e.g. a caller that never wired the
    catalogs service), apply() must still submit the harvest job — the
    bucket-free-create step is best-effort, not a hard dependency."""
    from dynastore.extensions.stac.presets.stac_harvester import (
        STAC_HARVESTER_PRESET,
        StacHarvesterParams,
    )

    ctx = _make_ctx(catalogs=None)
    params = StacHarvesterParams(url="https://example.test/stac")

    async def _fake_execute(process_id: str, exec_request: Any, **_kw: Any) -> MagicMock:
        return MagicMock(jobID="job-no-catalogs")

    with patch(
        "dynastore.modules.processes.processes_module.execute_process",
        _fake_execute,
    ):
        descriptor = await STAC_HARVESTER_PRESET.apply(params, "catalog:test-cat", ctx)

    assert descriptor.payload["job_id"] == "job-no-catalogs"


def test_preset_params_rejects_non_http_url() -> None:
    """StacHarvesterParams must reject URLs that are not http(s)."""
    from dynastore.extensions.stac.presets.stac_harvester import StacHarvesterParams
    import pydantic

    with pytest.raises(pydantic.ValidationError):
        StacHarvesterParams(url="ftp://example.com/stac")


def test_preset_params_strips_trailing_slash() -> None:
    """StacHarvesterParams normalises trailing slashes in the URL."""
    from dynastore.extensions.stac.presets.stac_harvester import StacHarvesterParams

    p = StacHarvesterParams(url="https://example.test/stac/")
    assert p.url == "https://example.test/stac"


@pytest.mark.asyncio
async def test_preset_revoke_is_noop() -> None:
    """revoke() must not raise and must not call any write method."""
    from dynastore.extensions.stac.presets.stac_harvester import STAC_HARVESTER_PRESET
    from dynastore.modules.storage.presets.preset import AppliedDescriptor

    ctx = _make_ctx()
    descriptor = AppliedDescriptor(payload={
        "catalog_url": "https://example.test/stac",
        "target_catalog": "test-cat",
        "job_id": "job-abc",
    })
    # Should complete without raising.
    await STAC_HARVESTER_PRESET.revoke(descriptor, ctx)


def test_preset_registered_in_registry() -> None:
    """After importing the STAC extension's presets package the preset is in the registry."""
    import dynastore.extensions.stac.presets  # noqa: F401 — side-effect import
    from dynastore.modules.storage.presets.registry import get_preset

    preset = get_preset("stac_harvester")
    assert preset is not None
    assert preset.name == "stac_harvester"


def test_preset_dry_run_returns_trigger_task_entry() -> None:
    """dry_run() returns a PresetPlan with a trigger_task entry for stac_harvest,
    plus a create_catalog entry disclosing the bucket-free-create side effect."""
    import asyncio
    from dynastore.extensions.stac.presets.stac_harvester import (
        STAC_HARVESTER_PRESET,
        StacHarvesterParams,
    )

    ctx = _make_ctx()
    params = StacHarvesterParams(url="https://example.test/stac")

    plan = asyncio.get_event_loop().run_until_complete(
        STAC_HARVESTER_PRESET.dry_run(params, "catalog:test-cat", ctx)
    )

    assert plan.preset_name == "stac_harvester"
    assert len(plan.entries) == 2

    create_entry = next(e for e in plan.entries if e.kind == "create_catalog")
    assert create_entry.target == "test-cat"
    assert create_entry.detail["if_absent"] is True
    assert create_entry.detail["hints"] == ["defer"]

    task_entry = next(e for e in plan.entries if e.kind == "trigger_task")
    assert task_entry.target == "stac_harvest"
    assert task_entry.detail["inputs"]["catalog_url"] == "https://example.test/stac"
    assert task_entry.detail["inputs"]["target_catalog"] == "test-cat"


# ---------------------------------------------------------------------------
# 3. drivers field — defaults, and legacy storage_backend mapping
# ---------------------------------------------------------------------------


def test_harvest_request_default_drivers_is_es() -> None:
    """StacHarvestRequest defaults drivers to 'es'."""
    from dynastore.tasks.stac_harvest.models import StacHarvestRequest
    from dynastore.modules.storage.presets.routing import RoutingDrivers

    req = StacHarvestRequest(
        catalog_url="https://example.test/stac",
        target_catalog="my-cat",
    )
    assert req.drivers == RoutingDrivers.ES


def test_harvest_request_legacy_storage_backend_maps_to_drivers() -> None:
    """Legacy storage_backend on StacHarvestRequest maps onto drivers."""
    from dynastore.tasks.stac_harvest.models import StacHarvestRequest
    from dynastore.modules.storage.presets.routing import RoutingDrivers

    req = StacHarvestRequest(
        catalog_url="https://example.test/stac",
        target_catalog="my-cat",
        storage_backend="es_pg",
    )
    assert req.drivers == RoutingDrivers.PG_ES


def test_preset_params_default_drivers_is_es() -> None:
    """StacHarvesterParams defaults drivers to 'es'."""
    from dynastore.extensions.stac.presets.stac_harvester import StacHarvesterParams
    from dynastore.modules.storage.presets.routing import RoutingDrivers

    p = StacHarvesterParams(url="https://example.test/stac")
    assert p.drivers == RoutingDrivers.ES


@pytest.mark.asyncio
async def test_preset_apply_forwards_drivers() -> None:
    """apply() forwards resolved drivers (legacy storage_backend mapped) to inputs."""
    from dynastore.extensions.stac.presets.stac_harvester import (
        STAC_HARVESTER_PRESET,
        StacHarvesterParams,
    )

    captured: list[dict] = []

    async def _fake_execute(process_id: str, exec_request: Any, **_kw: Any) -> MagicMock:
        captured.append({"inputs": dict(exec_request.inputs)})
        return MagicMock(jobID="job-be")

    ctx = _make_ctx()
    # Legacy storage_backend still accepted and mapped to drivers=pg_es.
    params = StacHarvesterParams(url="https://example.test/stac", storage_backend="es_pg")

    with patch(
        "dynastore.modules.processes.processes_module.execute_process",
        _fake_execute,
    ):
        await STAC_HARVESTER_PRESET.apply(params, "catalog:test-cat", ctx)

    inp = captured[0]["inputs"]
    assert inp["drivers"] == "pg_es"
    assert "storage_backend" not in inp
    assert "es_only" not in inp


# ---------------------------------------------------------------------------
# 2. Worker mapping — map_collection and map_item
# ---------------------------------------------------------------------------


def test_map_collection_normalises_id_and_sets_defaults() -> None:
    """map_collection lowercases the id and inserts fallback extent/description."""
    from dynastore.tasks.stac_harvest.task import map_collection

    raw = {
        "id": "MyCollection",
        "title": "My Collection",
        "links": [{"rel": "self", "href": "https://example.test"}],
        "assets": {"thumbnail": {"href": "https://thumb.example.test/t.png"}},
    }
    result = map_collection(raw)

    assert result["id"] == "mycollection", "id must be lowercased"
    assert "links" not in result, "links must be stripped"
    assert "assets" not in result, "collection-level assets must be stripped"
    assert result["type"] == "Collection"
    assert "extent" in result
    assert "description" in result


def test_map_collection_preserves_existing_extent() -> None:
    """map_collection does not overwrite an extent that is already present."""
    from dynastore.tasks.stac_harvest.task import map_collection

    custom_extent = {
        "spatial": {"bbox": [[10.0, 20.0, 30.0, 40.0]]},
        "temporal": {"interval": [["2020-01-01T00:00:00Z", None]]},
    }
    raw = {"id": "col1", "extent": custom_extent}
    result = map_collection(raw)

    assert result["extent"] == custom_extent


def test_map_item_sets_collection_and_strips_links() -> None:
    """map_item rewrites collection reference and drops navigation links."""
    from dynastore.tasks.stac_harvest.task import map_item

    raw = {
        "id": "item-001",
        "type": "Feature",
        "collection": "original-collection",
        "links": [{"rel": "self", "href": "https://example.test/item-001"}],
        "geometry": {"type": "Point", "coordinates": [12.0, 41.0]},
        "properties": {"datetime": "2024-01-01T00:00:00Z"},
        "assets": {
            "data": {"href": "https://data.example.test/item-001.tif", "type": "image/tiff"}
        },
    }
    result = map_item(raw, "target-collection")

    assert result["type"] == "Feature"
    assert result["collection"] == "target-collection", "collection must be rewritten"
    assert "links" not in result, "links must be stripped"
    # Assets are preserved on items.
    assert "assets" in result
    assert result["id"] == "item-001"


def test_virtual_assets_for_yields_raster_asset() -> None:
    """virtual_assets_for yields a RASTER entry for tiff assets."""
    from dynastore.tasks.stac_harvest.task import virtual_assets_for

    feature = {
        "id": "item-001",
        "assets": {
            "visual": {
                "href": "https://data.example.test/item-001.tif",
                "type": "image/tiff; application=geotiff",
                "roles": ["data"],
                "title": "Visual band",
            }
        },
    }
    results = list(virtual_assets_for(feature))

    assert len(results) == 1
    va = results[0]
    assert va["asset_id"] == "item-001.visual"
    assert va["href"] == "https://data.example.test/item-001.tif"
    assert va["asset_type"] == "RASTER"
    assert va["metadata"]["source_asset_key"] == "visual"


def test_virtual_assets_for_skips_missing_href() -> None:
    """virtual_assets_for skips assets with no href."""
    from dynastore.tasks.stac_harvest.task import virtual_assets_for

    feature = {
        "id": "item-002",
        "assets": {
            "no_href": {"type": "application/json"},
        },
    }
    results = list(virtual_assets_for(feature))
    assert results == []


def test_virtual_assets_for_gcs_owned_by() -> None:
    """virtual_assets_for marks GCS hrefs as owned_by='gcs'."""
    from dynastore.tasks.stac_harvest.task import virtual_assets_for

    feature = {
        "id": "item-003",
        "assets": {
            "data": {
                "href": "https://storage.googleapis.com/my-bucket/item-003.tif",
                "type": "image/tiff",
            }
        },
    }
    results = list(virtual_assets_for(feature))
    assert results[0]["owned_by"] == "gcs"


# ---------------------------------------------------------------------------
# _ensure_collection — write-language + resilience
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_ensure_collection_creates_with_concrete_write_lang() -> None:
    """New collections are created with a concrete lang, never the '*' wildcard."""
    from dynastore.tasks.stac_harvest.task import _WRITE_LANG, _ensure_collection

    catalogs = MagicMock()
    catalogs.get_collection = AsyncMock(return_value=None)  # does not exist yet
    catalogs.create_collection = AsyncMock(return_value=object())
    catalogs.update_collection = AsyncMock()

    ok = await _ensure_collection(catalogs, "cat", {"id": "col"})

    assert ok is True
    assert _WRITE_LANG != "*"
    catalogs.create_collection.assert_awaited_once()
    assert catalogs.create_collection.await_args.kwargs["lang"] == _WRITE_LANG
    catalogs.update_collection.assert_not_awaited()


@pytest.mark.asyncio
async def test_ensure_collection_updates_existing_with_concrete_write_lang() -> None:
    """Existing collections are updated with a concrete lang, never '*'."""
    from dynastore.tasks.stac_harvest.task import _WRITE_LANG, _ensure_collection

    catalogs = MagicMock()
    catalogs.get_collection = AsyncMock(return_value=object())  # already exists
    catalogs.create_collection = AsyncMock()
    catalogs.update_collection = AsyncMock(return_value=object())

    ok = await _ensure_collection(catalogs, "cat", {"id": "col"})

    assert ok is True
    catalogs.update_collection.assert_awaited_once()
    assert catalogs.update_collection.await_args.kwargs["lang"] == _WRITE_LANG
    catalogs.create_collection.assert_not_awaited()


@pytest.mark.asyncio
async def test_ensure_collection_resilient_when_write_raises_but_row_lands() -> None:
    """A post-write hook raise must not abort item ingestion if the row exists."""
    from dynastore.tasks.stac_harvest.task import _ensure_collection

    catalogs = MagicMock()
    # First existence check: absent → take create path. Create raises (e.g. a
    # best-effort async indexer). Re-check then finds the row present.
    catalogs.get_collection = AsyncMock(side_effect=[None, object()])
    catalogs.create_collection = AsyncMock(side_effect=RuntimeError("indexer boom"))
    catalogs.update_collection = AsyncMock()

    ok = await _ensure_collection(catalogs, "cat", {"id": "col"})

    assert ok is True
    assert catalogs.get_collection.await_count == 2


@pytest.mark.asyncio
async def test_ensure_collection_returns_false_when_row_absent_after_raise() -> None:
    """A genuine write failure (row never lands) returns False."""
    from dynastore.tasks.stac_harvest.task import _ensure_collection

    catalogs = MagicMock()
    catalogs.get_collection = AsyncMock(side_effect=[None, None])  # absent, still absent
    catalogs.create_collection = AsyncMock(side_effect=RuntimeError("write rejected"))
    catalogs.update_collection = AsyncMock()

    ok = await _ensure_collection(catalogs, "cat", {"id": "col"})

    assert ok is False


# ---------------------------------------------------------------------------
# iter_items — adaptive page-size retry
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_iter_items_retries_first_page_with_smaller_limit() -> None:
    """A first-page fetch failure (e.g. limit too large) retries with a halved limit."""
    from dynastore.tasks.stac_harvest import task as harvest_task

    calls: list[str] = []

    def fake_get(url: str, *a: Any, **k: Any) -> dict:
        calls.append(url)
        if "limit=100" in url:  # over-large page rejected by source
            raise RuntimeError("HTTP Error 502: Bad Gateway")
        return {"features": [{"id": "i1"}, {"id": "i2"}], "links": []}

    with patch.object(harvest_task, "_http_get_json", side_effect=fake_get):
        items = [x async for x in harvest_task.iter_items("https://src/v1", "col")]

    assert [i["id"] for i in items] == ["i1", "i2"]
    assert any("limit=100" in u for u in calls)  # tried large first
    assert any("limit=50" in u for u in calls)   # then halved and succeeded


@pytest.mark.asyncio
async def test_iter_items_gives_up_after_min_limit() -> None:
    """If even the minimum page size fails, iter_items yields nothing (no crash)."""
    from dynastore.tasks.stac_harvest import task as harvest_task

    with patch.object(
        harvest_task, "_http_get_json", side_effect=RuntimeError("boom")
    ):
        items = [x async for x in harvest_task.iter_items("https://src/v1", "col")]

    assert items == []


# ---------------------------------------------------------------------------
# _upsert_items_batch — Feature parsing + error surfacing
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_upsert_items_batch_passes_feature_objects_not_dicts() -> None:
    """The batch is parsed into Feature objects (with .id) before upsert.

    The ES-primary write path returns the input entities and the service reads
    result.id, so raw dicts crash. Guards against regressing to dict payloads.
    """
    from dynastore.models.ogc import Feature
    from dynastore.tasks.stac_harvest.task import _upsert_items_batch

    catalogs = MagicMock()
    catalogs.upsert = AsyncMock(return_value=[])

    batch = [
        {"type": "Feature", "id": "i1", "geometry": None, "properties": {}},
        {"type": "Feature", "id": "i2", "geometry": None, "properties": {}},
    ]
    written, err = await _upsert_items_batch(catalogs, "cat", "col", batch)

    assert written == 2
    assert err is None
    sent = catalogs.upsert.await_args.args[2]
    assert all(isinstance(f, Feature) for f in sent)
    assert all(hasattr(f, "id") for f in sent)


@pytest.mark.asyncio
async def test_upsert_items_batch_surfaces_write_error() -> None:
    """A write exception is returned as a short error string, written=0."""
    from dynastore.tasks.stac_harvest.task import _upsert_items_batch

    catalogs = MagicMock()
    catalogs.upsert = AsyncMock(side_effect=RuntimeError("ES down"))

    batch = [{"type": "Feature", "id": "i1", "geometry": None, "properties": {}}]
    written, err = await _upsert_items_batch(catalogs, "cat", "col", batch)

    assert written == 0
    assert err is not None and "RuntimeError" in err and "ES down" in err
