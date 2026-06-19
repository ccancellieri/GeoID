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

"""No-DB unit tests for the parametric ``routing`` preset.

Covers: registration + scope; scope-aware tier selection (catalog scope writes
collection + items templates, collection scope writes items only); the four
driver combinations (pg / es / pg_es / pg_pes) and their per-tier driver_refs;
the pg_pes private-ES wiring; backend↔drivers mapping; and the composite
param-coercion helper.
"""
from __future__ import annotations

from dynastore.modules.stac.stac_storage_config import StacStorageBackend
from dynastore.modules.storage.presets import get_preset, PresetTier
from dynastore.modules.storage.presets.routing import (
    RoutingDrivers,
    RoutingPreset,
    RoutingPresetParams,
    _build_routing_bundle,
    backend_from_drivers,
    coerce_routing_params,
    drivers_from_backend,
)
from dynastore.modules.storage.presets.stac import StacPresetParams
from dynastore.modules.storage.routing_config import (
    CollectionRoutingConfig,
    ItemsRoutingConfig,
    Operation,
)


def _refs(bundle) -> set:
    refs = set()
    for e in bundle.entries:
        cfg = e.instance
        if hasattr(cfg, "operations"):
            for entries in cfg.operations.values():
                for op_entry in entries:
                    refs.add(op_entry.driver_ref)
    return refs


def _slots(bundle) -> list:
    return [e.slot for e in bundle.entries]


def _slot_cfg(bundle, slot):
    for e in bundle.entries:
        if e.slot == slot:
            return e.instance
    return None


def _op_refs(cfg, op) -> set:
    return {e.driver_ref for e in cfg.operations.get(op, [])}


# ---------------------------------------------------------------------------
# Registration
# ---------------------------------------------------------------------------


def test_routing_preset_registered():
    p = get_preset("routing")
    assert p.name == "routing"
    assert isinstance(p, RoutingPreset)
    assert p.tier == PresetTier.CATALOG
    assert p.catalog_scopable is True
    assert p.params_model is RoutingPresetParams
    assert p.description


def test_routing_default_drivers_is_pg_es():
    assert RoutingPresetParams().drivers == RoutingDrivers.PG_ES


# ---------------------------------------------------------------------------
# Scope-aware tier selection
# ---------------------------------------------------------------------------


def test_catalog_scope_writes_collection_and_items():
    b = _build_routing_bundle(RoutingPresetParams(drivers=RoutingDrivers.ES), catalog_id="c1")
    assert _slots(b) == ["collection_template", "items_template"]
    assert b.collection_template is not None
    assert b.items_template is not None
    # rollback priority: items (10) removed before collection (20).
    prio = {e.slot: e.rollback_priority for e in b.entries}
    assert prio["items_template"] < prio["collection_template"]


def test_collection_scope_writes_items_only():
    b = _build_routing_bundle(
        RoutingPresetParams(drivers=RoutingDrivers.ES), catalog_id="c1", collection_id="col1"
    )
    assert _slots(b) == ["items_template"]
    assert b.collection_template is None
    assert b.items_template is not None


# ---------------------------------------------------------------------------
# Driver combinations
# ---------------------------------------------------------------------------


def test_pg_drivers_pg_only():
    b = _build_routing_bundle(RoutingPresetParams(drivers=RoutingDrivers.PG), catalog_id="c1")
    refs = _refs(b)
    assert refs == {"collection_postgresql_driver", "items_postgresql_driver"}


def test_es_drivers_items_es_only_metadata_stays_pg():
    """drivers=es → items ES-only; collection metadata PG-authoritative.

    The ES collection READ entry is hint-gated (geometry_simplified, on_failure=WARN):
    it is only activated when a request explicitly requests the simplified geometry path.
    PG remains the authoritative READ store with on_failure=FATAL as the second
    (fallback) entry.  Collection WRITE is PG-only (system of record).  ES is
    appended as the secondary index + SEARCH by self-registration at apply time.
    """
    b = _build_routing_bundle(RoutingPresetParams(drivers=RoutingDrivers.ES), catalog_id="c1")

    # Items tier is genuinely ES-only (items ES driver is a full store).
    items = _slot_cfg(b, "items_template")
    assert _op_refs(items, Operation.WRITE) == {"items_elasticsearch_driver"}
    assert _op_refs(items, Operation.READ) == {"items_elasticsearch_driver"}

    # Collection WRITE stays PG-only (system of record).
    coll = _slot_cfg(b, "collection_template")
    assert _op_refs(coll, Operation.WRITE) == {"collection_postgresql_driver"}
    # Collection READ has ES (hint-gated, WARN) + PG (authoritative, FATAL).
    read_refs = _op_refs(coll, Operation.READ)
    assert "collection_postgresql_driver" in read_refs, (
        "PG must always be in collection READ (authoritative system of record)"
    )
    # ES is present for hint-gated geometry_simplified reads; verify failure policy.
    from dynastore.modules.storage.routing_config import FailurePolicy
    read_entries = {e.driver_ref: e for e in coll.operations.get(Operation.READ, [])}
    pg_entry = read_entries.get("collection_postgresql_driver")
    assert pg_entry is not None and pg_entry.on_failure == FailurePolicy.FATAL, (
        "PG READ entry must be FATAL (authoritative)"
    )
    if "collection_elasticsearch_driver" in read_entries:
        es_entry = read_entries["collection_elasticsearch_driver"]
        assert es_entry.on_failure == FailurePolicy.WARN, (
            "ES READ entry must be WARN-only (hint-gated, non-authoritative)"
        )
    # ES remains the collection SEARCH backend.
    assert "collection_elasticsearch_driver" in _op_refs(coll, Operation.SEARCH)


def test_pg_es_drivers_both_tiers():
    b = _build_routing_bundle(RoutingPresetParams(drivers=RoutingDrivers.PG_ES), catalog_id="c1")
    refs = _refs(b)
    assert "collection_postgresql_driver" in refs
    assert "collection_elasticsearch_driver" in refs
    assert "items_postgresql_driver" in refs
    assert "items_elasticsearch_driver" in refs


def test_pg_pes_drivers_private_items_pg_collection():
    """pg_pes: items use PG + private ES; collection metadata stays in PG only."""
    b = _build_routing_bundle(RoutingPresetParams(drivers=RoutingDrivers.PG_PES), catalog_id="c1")
    refs = _refs(b)
    assert "items_postgresql_driver" in refs
    assert "items_elasticsearch_private_driver" in refs
    # Private ES has no collection-tier index — collection stays PG, no public ES.
    assert "collection_postgresql_driver" in refs
    assert "collection_elasticsearch_driver" not in refs
    assert "items_elasticsearch_driver" not in refs


def test_pg_pes_collection_scope_items_only_private():
    b = _build_routing_bundle(
        RoutingPresetParams(drivers=RoutingDrivers.PG_PES),
        catalog_id="c1",
        collection_id="col1",
    )
    assert _slots(b) == ["items_template"]
    assert isinstance(b.items_template, ItemsRoutingConfig)
    refs = _refs(b)
    assert "items_elasticsearch_private_driver" in refs


def test_config_classes_per_tier():
    b = _build_routing_bundle(RoutingPresetParams(drivers=RoutingDrivers.PG_ES), catalog_id="c1")
    by_slot = {e.slot: e.config_cls for e in b.entries}
    assert by_slot["collection_template"] is CollectionRoutingConfig
    assert by_slot["items_template"] is ItemsRoutingConfig


# ---------------------------------------------------------------------------
# backend ↔ drivers mapping
# ---------------------------------------------------------------------------


def test_backend_from_drivers():
    assert backend_from_drivers(RoutingDrivers.ES) == StacStorageBackend.ES
    assert backend_from_drivers(RoutingDrivers.PG) == StacStorageBackend.PG
    assert backend_from_drivers(RoutingDrivers.PG_ES) == StacStorageBackend.ES_PG
    # Private ES is not a public STAC route — PG sidecar only.
    assert backend_from_drivers(RoutingDrivers.PG_PES) == StacStorageBackend.PG


def test_drivers_from_backend():
    assert drivers_from_backend(StacStorageBackend.ES) == RoutingDrivers.ES
    assert drivers_from_backend(StacStorageBackend.PG) == RoutingDrivers.PG
    assert drivers_from_backend(StacStorageBackend.ES_PG) == RoutingDrivers.PG_ES
    assert drivers_from_backend(None) == RoutingDrivers.PG_ES


# ---------------------------------------------------------------------------
# coerce_routing_params — supports the composite's StacPresetParams
# ---------------------------------------------------------------------------


def test_coerce_native_params_passthrough():
    p = RoutingPresetParams(drivers=RoutingDrivers.PG)
    assert coerce_routing_params(p) is p


def test_coerce_stac_params_derives_drivers_from_backend():
    # No explicit drivers on the composite params → derive from stac_storage.
    p = StacPresetParams(stac_storage=StacStorageBackend.ES_PG)
    assert coerce_routing_params(p).drivers == RoutingDrivers.PG_ES


def test_coerce_stac_params_explicit_drivers_wins():
    p = StacPresetParams(stac_storage=StacStorageBackend.ES, drivers=RoutingDrivers.PG_PES)
    assert coerce_routing_params(p).drivers == RoutingDrivers.PG_PES
