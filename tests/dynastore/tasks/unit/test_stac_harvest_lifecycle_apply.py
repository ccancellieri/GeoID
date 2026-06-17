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

"""Unit tests for ``_apply_stac_presets`` lifecycle routing.

Verifies that:
1. ``backend="es"`` routes through ``apply_preset("items_es_public", ...)``
   AND ``apply_preset("stac_storage", ...)``, not raw ``set_config``.
2. ``backend="es_pg"`` routes ``stac_routing`` + ``stac_storage`` through
   ``apply_preset``.
3. When engine / IAM is unavailable, falls back to the direct
   ``preset.apply`` / ``ctx.config.set_config`` path (no raise).
4. ``PresetConflictError`` from ``apply_preset`` is swallowed; harvest continues.

No live DB, network, or OGC process engine is touched — all collaborators
are mocked.
"""
from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_ctx(db: Any = None) -> Any:
    from dynastore.modules.storage.presets.preset import PresetContext

    return PresetContext(
        db=db or MagicMock(),
        iam=None,
        policy=None,
        config=MagicMock(),
        tasks=None,
        cron=None,
        libs=None,
        principal=None,
        scope="catalog:test-cat",
        catalogs=None,
    )


# ---------------------------------------------------------------------------
# 1. backend="es" routes through apply_preset for both stac_storage and the
#    cumulative stac_routing (so collection + catalog STAC metadata is routed
#    to Elasticsearch); does NOT call ctx.config.set_config directly.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_es_backend_routes_via_lifecycle() -> None:
    """backend='es' must call apply_preset with 'stac_storage' and 'stac_routing'
    (full per-tier ES routing), NOT raw set_config, and NOT items_es_public."""
    from dynastore.tasks.stac_harvest import task as harvest_task

    apply_calls: list[tuple] = []

    async def fake_apply_preset(name: str, scope: str, params: Any, ctx: Any,
                                 engine: Any, audit: Any, **_kw: Any) -> dict:
        apply_calls.append((name, scope, params))
        return {"state": "applied"}

    fake_engine = MagicMock()
    fake_db_proto = MagicMock()
    fake_db_proto.engine = fake_engine
    ctx = _make_ctx(db=fake_engine)

    with (
        patch("dynastore.modules.storage.presets.lifecycle.apply_preset", fake_apply_preset),
        patch("dynastore.modules.get_protocol", return_value=fake_db_proto),
        patch(
            "dynastore.modules.iam.applied_presets_service.AppliedPresetsService",
            return_value=MagicMock(),
        ),
    ):
        await harvest_task._apply_stac_presets(ctx, "catalog:test-cat", "test-cat", "es")

    preset_names = [name for name, _, _ in apply_calls]
    assert "stac_storage" in preset_names, f"stac_storage not applied; calls={apply_calls}"
    assert "stac_routing" in preset_names, f"stac_routing not applied; calls={apply_calls}"
    # The items-only preset is no longer used by the harvest (it left collection
    # + catalog on the PG-default routing, dropping the STAC slice for ES).
    assert "items_es_public" not in preset_names, (
        f"items_es_public must not be applied; calls={apply_calls}"
    )
    # stac_routing for an ES harvest must carry the ES backend.
    from dynastore.modules.stac.stac_storage_config import StacStorageBackend
    routing_params = next(p for n, _, p in apply_calls if n == "stac_routing")
    assert routing_params.stac_storage == StacStorageBackend.ES
    # ctx.config.set_config must NOT have been called (audited path).
    ctx.config.set_config.assert_not_called()


@pytest.mark.asyncio
async def test_es_backend_scope_forwarded_correctly() -> None:
    """apply_preset calls for backend='es' must use the supplied scope string."""
    from dynastore.tasks.stac_harvest import task as harvest_task

    apply_calls: list[tuple] = []

    async def fake_apply_preset(name: str, scope: str, params: Any, ctx: Any,
                                 engine: Any, audit: Any, **_kw: Any) -> dict:
        apply_calls.append((name, scope))
        return {"state": "applied"}

    fake_engine = MagicMock()
    fake_db_proto = MagicMock()
    fake_db_proto.engine = fake_engine
    ctx = _make_ctx(db=fake_engine)

    with (
        patch("dynastore.modules.storage.presets.lifecycle.apply_preset", fake_apply_preset),
        patch("dynastore.modules.get_protocol", return_value=fake_db_proto),
        patch(
            "dynastore.modules.iam.applied_presets_service.AppliedPresetsService",
            return_value=MagicMock(),
        ),
    ):
        await harvest_task._apply_stac_presets(ctx, "catalog:my-harvest", "my-harvest", "es")

    for name, scope in apply_calls:
        assert scope == "catalog:my-harvest", (
            f"preset {name!r} was called with wrong scope {scope!r}"
        )


# ---------------------------------------------------------------------------
# 2. backend="es_pg" routes stac_routing + stac_storage via apply_preset.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_es_pg_backend_routes_stac_routing_via_lifecycle() -> None:
    """backend='es_pg' must call apply_preset for 'stac_storage' and 'stac_routing'."""
    from dynastore.tasks.stac_harvest import task as harvest_task

    apply_calls: list[tuple] = []

    async def fake_apply_preset(name: str, scope: str, params: Any, ctx: Any,
                                 engine: Any, audit: Any, **_kw: Any) -> dict:
        apply_calls.append((name, scope))
        return {"state": "applied"}

    fake_engine = MagicMock()
    fake_db_proto = MagicMock()
    fake_db_proto.engine = fake_engine
    ctx = _make_ctx(db=fake_engine)

    with (
        patch("dynastore.modules.storage.presets.lifecycle.apply_preset", fake_apply_preset),
        patch("dynastore.modules.get_protocol", return_value=fake_db_proto),
        patch(
            "dynastore.modules.iam.applied_presets_service.AppliedPresetsService",
            return_value=MagicMock(),
        ),
    ):
        await harvest_task._apply_stac_presets(ctx, "catalog:test-cat", "test-cat", "es_pg")

    preset_names = [name for name, _ in apply_calls]
    assert "stac_storage" in preset_names, f"stac_storage not applied; calls={apply_calls}"
    assert "stac_routing" in preset_names, f"stac_routing not applied; calls={apply_calls}"
    assert "items_es_public" not in preset_names, (
        f"items_es_public must not be applied for es_pg; calls={apply_calls}"
    )


@pytest.mark.asyncio
async def test_pg_backend_routes_stac_routing_via_lifecycle() -> None:
    """backend='pg' must call apply_preset for 'stac_storage' and 'stac_routing'."""
    from dynastore.tasks.stac_harvest import task as harvest_task

    apply_calls: list[tuple] = []

    async def fake_apply_preset(name: str, scope: str, params: Any, ctx: Any,
                                 engine: Any, audit: Any, **_kw: Any) -> dict:
        apply_calls.append((name, scope))
        return {"state": "applied"}

    fake_engine = MagicMock()
    fake_db_proto = MagicMock()
    fake_db_proto.engine = fake_engine
    ctx = _make_ctx(db=fake_engine)

    with (
        patch("dynastore.modules.storage.presets.lifecycle.apply_preset", fake_apply_preset),
        patch("dynastore.modules.get_protocol", return_value=fake_db_proto),
        patch(
            "dynastore.modules.iam.applied_presets_service.AppliedPresetsService",
            return_value=MagicMock(),
        ),
    ):
        await harvest_task._apply_stac_presets(ctx, "catalog:test-cat", "test-cat", "pg")

    preset_names = [name for name, _ in apply_calls]
    assert "stac_storage" in preset_names
    assert "stac_routing" in preset_names
    assert "items_es_public" not in preset_names


# ---------------------------------------------------------------------------
# 3. IAM-optional fallback: engine None → direct path, no raise.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_iam_optional_fallback_when_engine_none() -> None:
    """When engine is None, fall back to direct preset.apply / set_config; no raise."""
    from dynastore.tasks.stac_harvest import task as harvest_task

    direct_apply_calls: list[str] = []

    mock_preset = MagicMock()
    mock_preset.apply = AsyncMock(return_value=MagicMock())

    def fake_find_preset(name: str) -> MagicMock:
        direct_apply_calls.append(f"find:{name}")
        return mock_preset

    fake_config = AsyncMock()
    ctx = _make_ctx(db=None)
    ctx.db = None
    ctx.config = fake_config

    # get_protocol returns a proto with engine=None.
    fake_db_proto = MagicMock()
    fake_db_proto.engine = None

    with (
        patch("dynastore.modules.get_protocol", return_value=fake_db_proto),
        patch(
            "dynastore.modules.storage.presets.registry.find_preset",
            side_effect=fake_find_preset,
        ),
    ):
        # Must complete without raising.
        await harvest_task._apply_stac_presets(ctx, "catalog:test-cat", "test-cat", "es")

    # Direct path: find_preset was called for stac_storage (and possibly items routing).
    assert any("stac_storage" in c for c in direct_apply_calls), (
        f"expected find_preset('stac_storage') on direct path; calls={direct_apply_calls}"
    )
    # apply_preset (lifecycle) must NOT have been called (there's no engine).
    # We verify by checking that the lifecycle import path was avoided — the mock
    # preset's apply was called instead.
    assert mock_preset.apply.call_count >= 1


@pytest.mark.asyncio
async def test_iam_optional_fallback_when_get_protocol_raises() -> None:
    """When get_protocol raises, still fall back to direct path without raising."""
    from dynastore.tasks.stac_harvest import task as harvest_task

    mock_preset = MagicMock()
    mock_preset.apply = AsyncMock(return_value=MagicMock())
    fake_config = AsyncMock()
    ctx = _make_ctx(db=None)
    ctx.db = None
    ctx.config = fake_config

    with (
        patch("dynastore.modules.get_protocol", side_effect=RuntimeError("IAM not loaded")),
        patch(
            "dynastore.modules.storage.presets.registry.find_preset",
            return_value=mock_preset,
        ),
    ):
        # Must complete without raising.
        await harvest_task._apply_stac_presets(ctx, "catalog:test-cat", "test-cat", "es")

    assert mock_preset.apply.call_count >= 1


# ---------------------------------------------------------------------------
# 4. PresetConflictError from apply_preset is swallowed; no raise.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_preset_conflict_error_is_swallowed_for_stac_storage() -> None:
    """PresetConflictError on stac_storage apply must be swallowed; function returns normally."""
    from dynastore.tasks.stac_harvest import task as harvest_task
    from dynastore.modules.storage.presets.errors import PresetConflictError

    call_log: list[str] = []

    async def fake_apply_preset(name: str, scope: str, params: Any, ctx: Any,
                                 engine: Any, audit: Any, **_kw: Any) -> dict:
        call_log.append(name)
        if name == "stac_storage":
            raise PresetConflictError("already applied with same params")
        return {"state": "applied"}

    fake_engine = MagicMock()
    fake_db_proto = MagicMock()
    fake_db_proto.engine = fake_engine
    ctx = _make_ctx(db=fake_engine)

    with (
        patch("dynastore.modules.storage.presets.lifecycle.apply_preset", fake_apply_preset),
        patch("dynastore.modules.get_protocol", return_value=fake_db_proto),
        patch(
            "dynastore.modules.iam.applied_presets_service.AppliedPresetsService",
            return_value=MagicMock(),
        ),
    ):
        # Must NOT raise.
        await harvest_task._apply_stac_presets(ctx, "catalog:test-cat", "test-cat", "es")

    # stac_storage was attempted; conflict swallowed.
    assert "stac_storage" in call_log


@pytest.mark.asyncio
async def test_preset_conflict_error_is_swallowed_for_stac_routing_es() -> None:
    """PresetConflictError on the es-backend stac_routing apply must be swallowed."""
    from dynastore.tasks.stac_harvest import task as harvest_task
    from dynastore.modules.storage.presets.errors import PresetConflictError

    call_log: list[str] = []

    async def fake_apply_preset(name: str, scope: str, params: Any, ctx: Any,
                                 engine: Any, audit: Any, **_kw: Any) -> dict:
        call_log.append(name)
        if name == "stac_routing":
            raise PresetConflictError({"message": "in_progress"})
        return {"state": "applied"}

    fake_engine = MagicMock()
    fake_db_proto = MagicMock()
    fake_db_proto.engine = fake_engine
    ctx = _make_ctx(db=fake_engine)

    with (
        patch("dynastore.modules.storage.presets.lifecycle.apply_preset", fake_apply_preset),
        patch("dynastore.modules.get_protocol", return_value=fake_db_proto),
        patch(
            "dynastore.modules.iam.applied_presets_service.AppliedPresetsService",
            return_value=MagicMock(),
        ),
    ):
        # Must NOT raise.
        await harvest_task._apply_stac_presets(ctx, "catalog:test-cat", "test-cat", "es")

    assert "stac_routing" in call_log


# ---------------------------------------------------------------------------
# 5. items_es_public preset registration and bundle shape
# ---------------------------------------------------------------------------


def test_items_es_public_registered_as_catalog_scopable_items_tier() -> None:
    from dynastore.modules.storage.presets import get_preset, PresetTier

    p = get_preset("items_es_public")
    assert p.name == "items_es_public"
    assert p.tier == PresetTier.ITEMS
    assert p.catalog_scopable is True
    assert p.description, "preset must carry a non-empty description"


def test_items_es_public_bundle_has_single_items_routing_entry() -> None:
    from dynastore.modules.storage.presets import get_preset
    from dynastore.modules.storage.routing_config import ItemsRoutingConfig

    bundle = get_preset("items_es_public").build()
    entries = list(bundle.iter_apply())
    assert len(entries) == 1
    entry = entries[0]
    assert entry.slot == "items_template"
    assert entry.config_cls is ItemsRoutingConfig
    assert isinstance(entry.instance, ItemsRoutingConfig)
    # Scope left empty — admin endpoint layers catalog_id / collection_id.
    assert dict(entry.scope) == {}


def test_items_es_public_bundle_matches_items_routing_es() -> None:
    from dynastore.modules.storage.presets import get_preset
    from dynastore.modules.storage.presets.stac import _items_routing_es
    from dynastore.modules.storage.routing_config import ItemsRoutingConfig

    bundle = get_preset("items_es_public").build()
    entry = list(bundle.iter_apply())[0]
    expected = _items_routing_es()

    assert isinstance(entry.instance, ItemsRoutingConfig)
    assert entry.instance.model_dump() == expected.model_dump(), (
        "items_es_public bundle entry must equal _items_routing_es()"
    )


def test_items_es_public_pins_public_es_driver_not_private() -> None:
    from dynastore.modules.storage.presets import get_preset

    bundle = get_preset("items_es_public").build()
    items_template = list(bundle.iter_apply())[0].instance
    refs = [
        e.driver_ref
        for entries in items_template.operations.values()
        for e in entries
    ]
    assert "items_elasticsearch_driver" in refs
    assert "items_elasticsearch_private_driver" not in refs
    assert "items_postgresql_driver" not in refs
