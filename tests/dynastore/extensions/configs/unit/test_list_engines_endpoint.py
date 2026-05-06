"""Cycle F.4c.0 — `/configs/engines` discovery endpoint.

Pure-unit assertion that the handler returns one entry per registered
``EngineConfig`` subclass and the per-engine ``compatible_driver_classes``
list is populated from the F.2 ``required_engine_class`` discriminator
on every concrete driver config.
"""
from __future__ import annotations

import pytest

# Ensure every driver module's class-creation side-effects fire so
# ``_registered_pairs()`` returns the production driver-config list.
import dynastore.modules.storage.driver_config  # noqa: F401
import dynastore.modules.storage.drivers.catalog_postgresql  # noqa: F401
import dynastore.modules.storage.drivers.collection_postgresql  # noqa: F401
import dynastore.modules.storage.drivers.postgresql  # noqa: F401
import dynastore.modules.storage.drivers.duckdb  # noqa: F401
import dynastore.modules.storage.drivers.iceberg  # noqa: F401
import dynastore.modules.storage.drivers.elasticsearch  # noqa: F401
import dynastore.modules.storage.drivers.elasticsearch_private.driver  # noqa: F401
import dynastore.modules.storage.drivers.core_postgresql  # noqa: F401
import dynastore.modules.stac.drivers.postgresql  # noqa: F401
import dynastore.modules.elasticsearch.collection_es_driver  # noqa: F401
import dynastore.modules.elasticsearch.catalog_es_driver  # noqa: F401
import dynastore.modules.catalog.drivers.pg_asset_driver  # noqa: F401

from unittest.mock import MagicMock

from fastapi import FastAPI

from dynastore.extensions.configs.service import ConfigsService


def _make_service() -> ConfigsService:
    """Build a ConfigsService with a stub FastAPI host.

    ``list_engines`` reads only from the registry helpers — no DB call,
    no app_state coupling.  The FastAPI placeholder satisfies the
    constructor signature without triggering any router-config side
    effects we care about for this unit test.
    """
    return ConfigsService(app=FastAPI())


@pytest.mark.asyncio
async def test_list_engines_returns_all_four_engine_kinds():
    """Every concrete ``EngineConfig`` subclass surfaces under
    ``engines.<class_key>`` with its ``engine_class`` discriminator."""
    svc = _make_service()
    out = await svc.list_engines()

    engines = out["engines"]
    assert "postgresql_engine_config" in engines
    assert "elasticsearch_engine_config" in engines
    assert "duckdb_engine_config" in engines
    assert "iceberg_engine_config" in engines

    pg = engines["postgresql_engine_config"]
    assert pg["class_key"] == "postgresql_engine_config"
    assert pg["engine_class"] == "postgresql_engine"


@pytest.mark.asyncio
async def test_list_engines_compatible_driver_classes_sourced_from_required_engine_class():
    """Each engine's ``compatible_driver_classes`` lists every driver
    config whose ``required_engine_class`` discriminator matches.

    The PG engine is consumed by the largest set: items, catalog,
    collection, asset, plus the four PG sub-driver configs (Stac/Core
    × Catalog/Collection) added in F.2-fixup.  ES + DuckDB + Iceberg
    each consume their own subset.
    """
    svc = _make_service()
    out = await svc.list_engines()
    engines = out["engines"]

    pg_compat = set(engines["postgresql_engine_config"]["compatible_driver_classes"])
    # Spot-check the F.1 compatibility table pins.
    assert "items_postgresql_driver" in pg_compat
    assert "catalog_postgresql_driver" in pg_compat
    assert "collection_postgresql_driver" in pg_compat

    es_compat = set(engines["elasticsearch_engine_config"]["compatible_driver_classes"])
    assert "items_elasticsearch_driver" in es_compat
    assert "items_elasticsearch_private_driver" in es_compat
    assert "catalog_elasticsearch_driver" in es_compat

    duckdb_compat = set(engines["duckdb_engine_config"]["compatible_driver_classes"])
    assert "items_duckdb_driver" in duckdb_compat

    iceberg_compat = set(engines["iceberg_engine_config"]["compatible_driver_classes"])
    assert "items_iceberg_driver" in iceberg_compat


@pytest.mark.asyncio
async def test_list_engines_compat_is_sorted_for_stable_output():
    """Driver class_keys appear sorted under each engine to keep the
    response deterministic for clients that diff snapshots."""
    svc = _make_service()
    out = await svc.list_engines()
    for entry in out["engines"].values():
        compat = entry["compatible_driver_classes"]
        assert compat == sorted(compat)
