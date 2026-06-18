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

"""Unit tests for ``_apply_harvest_presets`` lifecycle routing.

Verifies that:
1. Every drivers combination routes through ``apply_preset("routing", ...)``
   AND ``apply_preset("stac_storage", ...)``, not raw ``set_config``.
2. The ``routing`` preset carries the harvest's ``drivers``; ``stac_storage``
   carries the derived backend.
3. When engine / IAM is unavailable, falls back to the direct
   ``preset.apply`` path (no raise).
4. ``PresetConflictError`` from ``apply_preset`` is swallowed; harvest continues.

No live DB, network, or OGC process engine is touched — all collaborators
are mocked.
"""
from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from dynastore.modules.storage.presets.routing import RoutingDrivers


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


def _audited_patches(fake_apply_preset, fake_db_proto):
    return (
        patch("dynastore.modules.storage.presets.lifecycle.apply_preset", fake_apply_preset),
        patch("dynastore.modules.get_protocol", return_value=fake_db_proto),
        patch(
            "dynastore.modules.iam.applied_presets_service.AppliedPresetsService",
            return_value=MagicMock(),
        ),
    )


# ---------------------------------------------------------------------------
# 1. drivers=es routes routing + stac_storage through apply_preset; no set_config.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_es_drivers_route_via_lifecycle() -> None:
    """drivers=es must call apply_preset for 'routing' and 'stac_storage',
    NOT raw set_config."""
    from dynastore.tasks.stac_harvest import task as harvest_task
    from dynastore.modules.stac.stac_storage_config import StacStorageBackend

    apply_calls: list[tuple] = []

    async def fake_apply_preset(name: str, scope: str, params: Any, ctx: Any,
                                 engine: Any, audit: Any, **_kw: Any) -> dict:
        apply_calls.append((name, scope, params))
        return {"state": "applied"}

    fake_engine = MagicMock()
    fake_db_proto = MagicMock()
    fake_db_proto.engine = fake_engine
    ctx = _make_ctx(db=fake_engine)

    p1, p2, p3 = _audited_patches(fake_apply_preset, fake_db_proto)
    with p1, p2, p3:
        await harvest_task._apply_harvest_presets(
            ctx, "catalog:test-cat", "test-cat", RoutingDrivers.ES
        )

    preset_names = [name for name, _, _ in apply_calls]
    assert "routing" in preset_names, f"routing not applied; calls={apply_calls}"
    assert "stac_storage" in preset_names, f"stac_storage not applied; calls={apply_calls}"
    # The items-only preset is not used by the harvest.
    assert "items_es_public" not in preset_names
    assert "stac_routing" not in preset_names, "stac_routing was renamed to routing"
    # routing carries the es drivers; stac_storage carries the derived ES backend.
    routing_params = next(p for n, _, p in apply_calls if n == "routing")
    assert routing_params.drivers == RoutingDrivers.ES
    storage_params = next(p for n, _, p in apply_calls if n == "stac_storage")
    assert storage_params.stac_storage == StacStorageBackend.ES
    ctx.config.set_config.assert_not_called()


@pytest.mark.asyncio
async def test_drivers_scope_forwarded_correctly() -> None:
    """apply_preset calls must use the supplied scope string verbatim."""
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

    p1, p2, p3 = _audited_patches(fake_apply_preset, fake_db_proto)
    with p1, p2, p3:
        await harvest_task._apply_harvest_presets(
            ctx, "catalog:my-harvest/collection:col-9", "my-harvest", RoutingDrivers.ES
        )

    for name, scope in apply_calls:
        assert scope == "catalog:my-harvest/collection:col-9", (
            f"preset {name!r} was called with wrong scope {scope!r}"
        )


# ---------------------------------------------------------------------------
# 2. es_pg and pg drivers route routing + stac_storage via apply_preset.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "drivers,expected_backend",
    [
        (RoutingDrivers.PG_ES, "ES_PG"),
        (RoutingDrivers.PG, "PG"),
        (RoutingDrivers.PG_PES, "PG"),
    ],
)
async def test_other_drivers_route_via_lifecycle(drivers, expected_backend) -> None:
    """Each drivers combo applies 'routing' and 'stac_storage' with the right backend."""
    from dynastore.tasks.stac_harvest import task as harvest_task

    apply_calls: list[tuple] = []

    async def fake_apply_preset(name: str, scope: str, params: Any, ctx: Any,
                                 engine: Any, audit: Any, **_kw: Any) -> dict:
        apply_calls.append((name, params))
        return {"state": "applied"}

    fake_engine = MagicMock()
    fake_db_proto = MagicMock()
    fake_db_proto.engine = fake_engine
    ctx = _make_ctx(db=fake_engine)

    p1, p2, p3 = _audited_patches(fake_apply_preset, fake_db_proto)
    with p1, p2, p3:
        await harvest_task._apply_harvest_presets(
            ctx, "catalog:test-cat", "test-cat", drivers
        )

    names = [n for n, _ in apply_calls]
    assert "routing" in names and "stac_storage" in names
    routing_params = next(p for n, p in apply_calls if n == "routing")
    assert routing_params.drivers == drivers
    storage_params = next(p for n, p in apply_calls if n == "stac_storage")
    assert storage_params.stac_storage.value == expected_backend


# ---------------------------------------------------------------------------
# 3. IAM-optional fallback: engine None → direct path, no raise.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_iam_optional_fallback_when_engine_none() -> None:
    """When engine is None, fall back to direct preset.apply; no raise."""
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

    fake_db_proto = MagicMock()
    fake_db_proto.engine = None

    with (
        patch("dynastore.modules.get_protocol", return_value=fake_db_proto),
        patch(
            "dynastore.modules.storage.presets.registry.find_preset",
            side_effect=fake_find_preset,
        ),
    ):
        await harvest_task._apply_harvest_presets(
            ctx, "catalog:test-cat", "test-cat", RoutingDrivers.ES
        )

    # Direct path: find_preset called for both routing and stac_storage.
    assert any("routing" in c for c in direct_apply_calls), direct_apply_calls
    assert any("stac_storage" in c for c in direct_apply_calls), direct_apply_calls
    assert mock_preset.apply.call_count >= 2


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
        await harvest_task._apply_harvest_presets(
            ctx, "catalog:test-cat", "test-cat", RoutingDrivers.ES
        )

    assert mock_preset.apply.call_count >= 2


# ---------------------------------------------------------------------------
# 4. PresetConflictError from apply_preset is swallowed; no raise.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
@pytest.mark.parametrize("conflicting", ["routing", "stac_storage"])
async def test_preset_conflict_error_is_swallowed(conflicting) -> None:
    """A PresetConflictError on either apply must be swallowed; function returns."""
    from dynastore.tasks.stac_harvest import task as harvest_task
    from dynastore.modules.storage.presets.errors import PresetConflictError

    call_log: list[str] = []

    async def fake_apply_preset(name: str, scope: str, params: Any, ctx: Any,
                                 engine: Any, audit: Any, **_kw: Any) -> dict:
        call_log.append(name)
        if name == conflicting:
            raise PresetConflictError("already applied with same params")
        return {"state": "applied"}

    fake_engine = MagicMock()
    fake_db_proto = MagicMock()
    fake_db_proto.engine = fake_engine
    ctx = _make_ctx(db=fake_engine)

    p1, p2, p3 = _audited_patches(fake_apply_preset, fake_db_proto)
    with p1, p2, p3:
        await harvest_task._apply_harvest_presets(
            ctx, "catalog:test-cat", "test-cat", RoutingDrivers.ES
        )

    assert conflicting in call_log


# ---------------------------------------------------------------------------
# 5. items_es_public preset registration and bundle shape (unchanged preset).
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
    assert dict(entry.scope) == {}


def test_items_es_public_bundle_matches_items_routing_es() -> None:
    from dynastore.modules.storage.presets import get_preset
    from dynastore.modules.storage.presets.routing import _items_routing_es
    from dynastore.modules.storage.routing_config import ItemsRoutingConfig

    bundle = get_preset("items_es_public").build()
    entry = list(bundle.iter_apply())[0]
    expected = _items_routing_es()

    assert isinstance(entry.instance, ItemsRoutingConfig)
    assert entry.instance.model_dump() == expected.model_dump()


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
