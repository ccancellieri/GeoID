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

"""Regression test — router falls back to model defaults when stored config has no entries.

Phase H deploy 4 (image :860, 2026-04-29) surfaced a regression where
both `ingestion` (collection routing READ) and `gdal` (asset routing
READ) failed with `ValueError: No collection/asset driver found for
operation='READ'`. The catalog API service had stored ItemsRoutingConfig /
AssetRoutingConfig rows with empty `operations: {}` (left over from a
parallel-session config-refactor PR that rewrote stored config shape).

Pre-fix behaviour: `_resolve_driver_ids_cached` reads
`routing_config.operations.get(operation, [])` → empty → caller raises
`ValueError`. Worse than having no stored row at all (which would have
let `configs.get_config` return `cls()` and fire `default_factory`).

Post-fix behaviour: when entries are empty, fall back to
`routing_plugin_cls().operations.get(operation, [])` — same defaults
that fire when no row exists. This restores the canonical PostgreSQL
driver as a safety net.
"""
from unittest.mock import patch

import pytest

from dynastore.modules.storage.router import _resolve_driver_ids_cached
from dynastore.modules.storage.routing_config import (
    AssetRoutingConfig,
    ItemsRoutingConfig,
    Operation,
    OperationDriverEntry,
)


class _MockConfigsProtocol:
    """Returns whatever stored_config we hand it, ignoring scope kwargs."""

    def __init__(self, stored_config):
        self._cfg = stored_config

    async def get_config(self, cls, **kwargs):
        return self._cfg


def _patch_configs(stored_config):
    """Patch get_protocol(ConfigsProtocol) to return our mock."""
    return patch(
        "dynastore.tools.discovery.get_protocol",
        return_value=_MockConfigsProtocol(stored_config),
    )


@pytest.mark.asyncio
async def test_collection_routing_falls_back_to_defaults_when_empty() -> None:
    """Stored ItemsRoutingConfig with empty operations should fall
    back to default_factory entries (READ → items_elasticsearch_driver
    primary, items_postgresql_driver secondary — see PR #185)."""
    empty_config = ItemsRoutingConfig(operations={})

    # Cache is keyed on (cls, catalog_id, collection_id, op, hint) —
    # use a unique id so each test gets a fresh resolution.
    with _patch_configs(empty_config):
        result = await _resolve_driver_ids_cached(
            ItemsRoutingConfig,
            "test_cat_empty_collection_routing",
            "test_col",
            Operation.READ,
            None,
        )

    assert result, (
        "Expected fallback to default_factory entries (READ → "
        "items_elasticsearch_driver) when stored config has empty operations. "
        "Got: empty list."
    )
    driver_ids = [entry[0] for entry in result]
    assert driver_ids[0] == "items_elasticsearch_driver", (
        f"Expected first driver_ref to be items_elasticsearch_driver from "
        f"ItemsRoutingConfig.default_factory READ order, got {driver_ids[0]!r}."
    )
    assert "items_postgresql_driver" in driver_ids, (
        f"Expected items_postgresql_driver also in fallback list (geometry_exact "
        f"hint), got {driver_ids!r}."
    )


@pytest.mark.asyncio
async def test_asset_routing_falls_back_to_defaults_when_empty() -> None:
    """Asset READ falls back to PG-only: asset_postgresql_driver
    (FATAL + GEOMETRY_EXACT). The ES asset driver is no longer a
    hardcoded default — operators pin it explicitly when desired."""
    empty_config = AssetRoutingConfig(operations={})

    with _patch_configs(empty_config):
        result = await _resolve_driver_ids_cached(
            AssetRoutingConfig,
            "test_cat_empty_asset_routing",
            "test_col",
            Operation.READ,
            None,
        )

    assert result, "Expected fallback for AssetRoutingConfig empty operations."
    driver_ids = [entry[0] for entry in result]
    assert driver_ids == ["asset_postgresql_driver"], (
        f"Expected PG-only fallback for AssetRoutingConfig READ, got {driver_ids!r}."
    )


@pytest.mark.asyncio
async def test_explicit_entries_are_not_overridden() -> None:
    """Negative case: when stored config HAS explicit entries, the
    fallback must not fire — we must respect operator-set routing.
    The fallback only fires for the NO-entries case."""
    explicit_config = ItemsRoutingConfig(
        operations={
            Operation.READ: [OperationDriverEntry(driver_ref="explicitly_chosen_driver")],
        }
    )

    with _patch_configs(explicit_config):
        result = await _resolve_driver_ids_cached(
            ItemsRoutingConfig,
            "test_cat_explicit_routing",
            "test_col",
            Operation.READ,
            None,
        )

    assert result, "Expected explicit entries to be returned."
    assert result[0][0] == "explicitly_chosen_driver", (
        f"Fallback fired even when explicit entries were present. "
        f"Got {result[0][0]!r} instead of explicitly_chosen_driver."
    )


@pytest.mark.asyncio
async def test_resolve_drivers_logs_selection_at_debug(monkeypatch, caplog):
    """After a successful resolve_drivers call a DEBUG record must exist
    whose message starts with 'router-resolve' and contains the selected
    driver's class name."""
    import logging

    from dynastore.modules.storage import router as router_mod
    from dynastore.modules.storage.routing_config import ItemsRoutingConfig, Operation

    # Fake _resolve_driver_ids_cached: return one (driver_ref, on_failure)
    # tuple so resolve_drivers assembles a ResolvedDriver from the driver_index.
    async def _fake_ids(routing_plugin_cls, catalog_id, collection_id, operation, hint):
        return [("items_postgresql_driver", "fatal")]

    class _FakePostgresDriver:
        pass

    # Patch the cached async function with our fake.
    monkeypatch.setattr(router_mod, "_resolve_driver_ids_cached", _fake_ids)

    # Patch DriverRegistry.collection_index to return our fake driver instance.
    monkeypatch.setattr(
        router_mod.DriverRegistry,
        "collection_index",
        classmethod(lambda cls: {"items_postgresql_driver": _FakePostgresDriver()}),
    )

    # Bypass the L4 request cache so we always go through resolution logic.
    monkeypatch.setattr(router_mod, "get_request_driver_cache", lambda: {})

    with caplog.at_level(logging.DEBUG, logger=router_mod.__name__):
        result = await router_mod.resolve_drivers(
            Operation.READ,
            "test_cat_debug_log",
            "test_col",
            routing_plugin_cls=ItemsRoutingConfig,
        )

    assert result, "Expected at least one resolved driver."
    # driver_ref = type(driver).__name__ = "_FakePostgresDriver" for the patched driver
    expected_driver_name = _FakePostgresDriver.__name__
    assert any(
        "router-resolve" in r.message and expected_driver_name in r.message
        for r in caplog.records
    ), (
        f"Expected a DEBUG record containing 'router-resolve' and "
        f"{expected_driver_name!r}. Records: {[r.message for r in caplog.records]!r}"
    )
