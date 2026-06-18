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

"""StacStoragePreset + StacPreset composite + StacStorageConfig no-DB unit tests.

Routing-bundle shape is covered by ``test_preset_routing.py`` (the ``routing``
preset owns routing now); this file verifies:
- StacPreset is registered as the composite of ``routing`` + ``stac_storage``.
- ``stac_storage`` writes ONLY the StacStorageConfig SSOT.
- Sidecar resolution gate: StacItemsSidecar.get_default_config injects iff
  stac_items_pg=True in context (and collection_type != RECORDS).
- default_catalog_sidecars / default_sidecars return CORE only by default.
- _resolve_stac_items_pg helper.
"""
from __future__ import annotations

import pytest

from dynastore.modules.stac.stac_storage_config import (
    StacLevel,
    StacStorageBackend,
    StacStorageConfig,
    catalog_stac_enabled,
    collection_stac_enabled,
    es_stac,
    items_stac_enabled,
    pg_stac,
)
from dynastore.modules.storage.presets import get_preset
from dynastore.modules.storage.presets.preset import CompositePreset
from dynastore.modules.storage.presets.stac import (
    StacPreset,
    StacPresetParams,
    StacStoragePreset,
    _build_stac_storage_bundle,
)


def _storage(params: StacPresetParams):
    """The SSOT-only bundle for ``params`` (single StacStorageConfig)."""
    return _build_stac_storage_bundle(params, catalog_id="cat-1")


# ---------------------------------------------------------------------------
# StacStorageConfig helpers
# ---------------------------------------------------------------------------


def test_stac_level_helpers_none():
    assert not catalog_stac_enabled(StacLevel.NONE)
    assert not collection_stac_enabled(StacLevel.NONE)
    assert not items_stac_enabled(StacLevel.NONE)


def test_stac_level_helpers_catalog():
    assert catalog_stac_enabled(StacLevel.CATALOG)
    assert not collection_stac_enabled(StacLevel.CATALOG)
    assert not items_stac_enabled(StacLevel.CATALOG)


def test_stac_level_helpers_collection():
    assert catalog_stac_enabled(StacLevel.COLLECTION)
    assert collection_stac_enabled(StacLevel.COLLECTION)
    assert not items_stac_enabled(StacLevel.COLLECTION)


def test_stac_level_helpers_items():
    assert catalog_stac_enabled(StacLevel.ITEMS)
    assert collection_stac_enabled(StacLevel.ITEMS)
    assert items_stac_enabled(StacLevel.ITEMS)


def test_stac_backend_helpers():
    assert pg_stac(StacStorageBackend.PG)
    assert pg_stac(StacStorageBackend.ES_PG)
    assert not pg_stac(StacStorageBackend.ES)
    assert es_stac(StacStorageBackend.ES)
    assert es_stac(StacStorageBackend.ES_PG)
    assert not es_stac(StacStorageBackend.PG)


# ---------------------------------------------------------------------------
# Preset registration
# ---------------------------------------------------------------------------


def test_stac_preset_registered_as_routing_storage_composite():
    p = get_preset("stac")
    assert p.name == "stac"
    assert p.description, "preset must carry a non-empty description"
    assert isinstance(p, StacPreset)
    assert isinstance(p, CompositePreset)
    # ``stac`` composes the parametric ``routing`` preset (decide where data
    # lives) then ``stac_storage`` (enable STAC), applied in that order.
    assert p.compose == ("routing", "stac_storage")


def test_stac_storage_child_registered():
    sp = get_preset("stac_storage")
    assert isinstance(sp, StacStoragePreset)
    assert sp.params_model is StacPresetParams
    assert sp.catalog_scopable


def test_routing_child_registered():
    # The other composite child is the parametric ``routing`` preset.
    rp = get_preset("routing")
    assert rp.name == "routing"
    assert rp.catalog_scopable


# ---------------------------------------------------------------------------
# stac_storage carries ONLY the SSOT
# ---------------------------------------------------------------------------


def test_storage_bundle_is_single_ssot_entry():
    params = StacPresetParams(stac_level=StacLevel.ITEMS, stac_storage=StacStorageBackend.ES_PG)
    sb = _storage(params)
    assert len(sb.entries) == 1
    assert sb.entries[0].config_cls is StacStorageConfig
    assert sb.entries[0].instance.stac_level == StacLevel.ITEMS
    assert sb.entries[0].instance.stac_storage == StacStorageBackend.ES_PG


def test_storage_bundle_none_carries_none_level():
    params = StacPresetParams(stac_level=StacLevel.NONE, stac_storage=StacStorageBackend.ES_PG)
    sb = _storage(params)
    assert len(sb.entries) == 1
    entry = sb.entries[0]
    assert entry.config_cls is StacStorageConfig
    assert entry.instance.stac_level == StacLevel.NONE


# ---------------------------------------------------------------------------
# Sidecar resolution gate — StacItemsSidecar.get_default_config
# ---------------------------------------------------------------------------


def test_stac_items_sidecar_not_injected_by_default():
    """No stac_items_pg in context => no sidecar injected (opt-in default)."""
    from dynastore.extensions.stac.stac_items_sidecar import StacItemsSidecar

    result = StacItemsSidecar.get_default_config({})
    assert result is None


def test_stac_items_sidecar_not_injected_when_false():
    from dynastore.extensions.stac.stac_items_sidecar import StacItemsSidecar

    result = StacItemsSidecar.get_default_config({"stac_items_pg": False})
    assert result is None


def test_stac_items_sidecar_injected_when_stac_items_pg_true():
    from dynastore.extensions.stac.stac_items_sidecar import StacItemsSidecar
    from dynastore.extensions.stac.stac_metadata_config import StacItemsSidecarConfig

    result = StacItemsSidecar.get_default_config({"stac_items_pg": True})
    assert isinstance(result, StacItemsSidecarConfig)


def test_stac_items_sidecar_skipped_for_records_even_when_pg_true():
    """RECORDS guard takes priority regardless of stac_items_pg."""
    from dynastore.extensions.stac.stac_items_sidecar import StacItemsSidecar

    result = StacItemsSidecar.get_default_config(
        {"stac_items_pg": True, "collection_type": "RECORDS"}
    )
    assert result is None


# ---------------------------------------------------------------------------
# default_catalog_sidecars — CORE only by default
# ---------------------------------------------------------------------------


def test_default_catalog_sidecars_core_only():
    from dynastore.modules.storage.drivers.pg_sidecars.registry import SidecarRegistry

    SidecarRegistry.clear_catalog_registry()
    sidecars = SidecarRegistry.default_catalog_sidecars()
    types = [getattr(s, "sidecar_type", None) for s in sidecars]
    assert "catalog_core" in types
    assert "catalog_stac" not in types, (
        "catalog_stac must NOT be in the default list (opt-in via StacPreset)"
    )


def test_default_collection_sidecars_core_only():
    from dynastore.modules.storage.drivers.collection_postgresql import (
        CollectionPgSidecarRegistry,
    )

    CollectionPgSidecarRegistry.clear()
    sidecars = CollectionPgSidecarRegistry.default_sidecars()
    types = [getattr(s, "sidecar_type", None) for s in sidecars]
    assert "collection_core" in types
    assert "collection_stac" not in types, (
        "collection_stac must NOT be in the default list (opt-in via StacPreset)"
    )


# ---------------------------------------------------------------------------
# _resolve_stac_items_pg — config-driven gate
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_resolve_stac_items_pg_no_protocol_returns_false():
    from dynastore.modules.storage.drivers.postgresql import _resolve_stac_items_pg

    result = await _resolve_stac_items_pg("cat-1", "coll-1", configs=None)
    assert result is False


@pytest.mark.asyncio
async def test_resolve_stac_items_pg_with_none_config_returns_false():
    from dynastore.modules.storage.drivers.postgresql import _resolve_stac_items_pg

    class FakeConfigs:
        async def get_config(self, cls, **kwargs):
            return None

    result = await _resolve_stac_items_pg("cat-1", "coll-1", configs=FakeConfigs())
    assert result is False


@pytest.mark.asyncio
async def test_resolve_stac_items_pg_with_items_level_pg_returns_true():
    from dynastore.modules.storage.drivers.postgresql import _resolve_stac_items_pg

    class FakeConfigs:
        async def get_config(self, cls, **kwargs):
            if cls is StacStorageConfig:
                return StacStorageConfig(
                    stac_level=StacLevel.ITEMS,
                    stac_storage=StacStorageBackend.ES_PG,
                )
            return None

    result = await _resolve_stac_items_pg("cat-1", "coll-1", configs=FakeConfigs())
    assert result is True


@pytest.mark.asyncio
async def test_resolve_stac_items_pg_with_collection_level_returns_false():
    from dynastore.modules.storage.drivers.postgresql import _resolve_stac_items_pg

    class FakeConfigs:
        async def get_config(self, cls, **kwargs):
            if cls is StacStorageConfig:
                return StacStorageConfig(
                    stac_level=StacLevel.COLLECTION,
                    stac_storage=StacStorageBackend.PG,
                )
            return None

    result = await _resolve_stac_items_pg("cat-1", "coll-1", configs=FakeConfigs())
    assert result is False


@pytest.mark.asyncio
async def test_resolve_stac_items_pg_with_items_es_only_returns_false():
    from dynastore.modules.storage.drivers.postgresql import _resolve_stac_items_pg

    class FakeConfigs:
        async def get_config(self, cls, **kwargs):
            if cls is StacStorageConfig:
                return StacStorageConfig(
                    stac_level=StacLevel.ITEMS,
                    stac_storage=StacStorageBackend.ES,
                )
            return None

    result = await _resolve_stac_items_pg("cat-1", "coll-1", configs=FakeConfigs())
    assert result is False
