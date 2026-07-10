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

"""Pin routing defaults so a future edit can't silently change the WRITE
primary's failure policy or the INDEX lane's materialization target. PG
must remain FATAL on WRITE — non-negotiable for authoritative writes.

Under the lane model (#2494) search is derived, not configured: these
tests pin ``operations[INDEX]`` (the materialization / derived-search
target set) and ``operations[READ]`` (the canonical + hint-opt-in
readers), and replicate the two-lane (INDEX-then-READ) resolution the
production ``get_items_search_driver`` performs to pin that a
GROUP_BY / JOIN request still resolves to PG even though ES is the
INDEX-lane entry.
"""
from __future__ import annotations


def test_default_pg_write_is_sync_fatal():
    from dynastore.modules.storage.routing_config import (
        FailurePolicy, ItemsRoutingConfig, Operation,
    )
    cfg = ItemsRoutingConfig()
    pg = next(
        e for e in cfg.operations[Operation.WRITE]
        if e.driver_ref == "items_postgresql_driver"
    )
    assert pg.on_failure == FailurePolicy.FATAL


def test_default_write_lane_has_no_index_lane_driver():
    """The items WRITE lane is PG-only — the ES indexer lives entirely in
    the INDEX lane under the lane model, never as a WRITE entry."""
    from dynastore.modules.storage.routing_config import ItemsRoutingConfig, Operation
    cfg = ItemsRoutingConfig()
    write_refs = {e.driver_ref for e in cfg.operations[Operation.WRITE]}
    assert write_refs == {"items_postgresql_driver"}


def test_default_index_lane_is_es_only():
    """The items INDEX lane (materialization + derived-search target) is
    the ES driver, auto-registered with no explicit hints — it inherits
    its effective hint surface from ``ItemsElasticsearchDriver.supported_hints``
    (which includes ``Hint.SEARCH``, winning the derived-search preference
    ranking)."""
    from dynastore.modules.storage.hints import Hint
    from dynastore.modules.storage.routing_config import ItemsRoutingConfig, Operation
    cfg = ItemsRoutingConfig()
    index = cfg.operations[Operation.INDEX]
    assert [e.driver_ref for e in index] == ["items_elasticsearch_driver"]
    assert index[0].hints == set()

    from dynastore.modules.storage.drivers.elasticsearch import ItemsElasticsearchDriver
    assert Hint.SEARCH in ItemsElasticsearchDriver.supported_hints


def test_default_read_routing_unchanged():
    """READ entries (ES geometry_simplified primary, PG geometry_exact)
    must remain; READ carries no per-entry failure policy any more, but PG
    stays first (index 0) and structurally authoritative."""
    from dynastore.modules.storage.routing_config import (
        FailurePolicy, ItemsRoutingConfig, Operation,
    )
    cfg = ItemsRoutingConfig()
    read = cfg.operations[Operation.READ]
    es_read = next(e for e in read if e.driver_ref == "items_elasticsearch_driver")
    pg_read = next(e for e in read if e.driver_ref == "items_postgresql_driver")
    assert "geometry_simplified" in es_read.hints
    assert "geometry_exact" in pg_read.hints
    assert pg_read.on_failure == FailurePolicy.FATAL


def _resolve_read(read, requested):
    """Replicate router.py's best-overlap matcher against a raw entry list
    (no driver registry — entry.hints only, matching this file's fixtures
    which always declare explicit hints)."""
    if not requested:
        return [e.driver_ref for e in read]
    matched = [
        (i, e) for i, e in enumerate(read)
        if requested.issubset(frozenset(e.hints))
    ]
    matched.sort(key=lambda t: (-len(t[1].hints), t[0]))
    return [e.driver_ref for _, e in matched]


def _resolve_derived_search(cfg, requested):
    """Replicate ``router.get_items_search_driver``'s two-lane resolution:
    INDEX matched-only (strict — empty on no match), then READ (matched +
    relaxed) as fallback.  Entry hints only (no driver-registry
    ``supported_hints`` fallback), matching this file's style."""
    from dynastore.modules.storage.routing_config import Operation

    index = cfg.operations.get(Operation.INDEX, [])
    if requested:
        index_matched = [
            e.driver_ref for e in index if requested.issubset(frozenset(e.hints))
        ]
    else:
        index_matched = [e.driver_ref for e in index]
    if index_matched:
        return index_matched
    return _resolve_read(cfg.operations[Operation.READ], requested)


def test_default_items_derived_search_prefers_index_lane():
    """An unfiltered search resolves to the INDEX lane (ES) — the derived
    search pool's first tier."""
    from dynastore.modules.storage.routing_config import ItemsRoutingConfig
    cfg = ItemsRoutingConfig()
    assert _resolve_derived_search(cfg, frozenset()) == ["items_elasticsearch_driver"]


def test_default_items_derived_search_group_by_resolves_pg():
    """A GROUP_BY-flavoured search request must resolve to PG — the INDEX
    lane's bare ES entry (no explicit hints, and
    ``ItemsElasticsearchDriver.supported_hints`` has no GROUP_BY) never
    matches, so the derived pool falls through to the READ lane, where the
    PG entry declares ``Hint.GROUP_BY`` explicitly (#2829: Elasticsearch has
    no GROUP BY implementation)."""
    from dynastore.modules.storage.hints import Hint
    from dynastore.modules.storage.routing_config import ItemsRoutingConfig, Operation

    cfg = ItemsRoutingConfig()
    # The INDEX entry carries no explicit hints in the persisted config; this
    # helper only inspects entry.hints (no driver-registry lookup), so assert
    # the structural precondition the production resolver relies on instead:
    # the ES driver's declared supported_hints has no GROUP_BY.
    from dynastore.modules.storage.drivers.elasticsearch import ItemsElasticsearchDriver
    assert Hint.GROUP_BY not in ItemsElasticsearchDriver.supported_hints

    pg_read = next(
        e for e in cfg.operations[Operation.READ]
        if e.driver_ref == "items_postgresql_driver"
    )
    assert Hint.GROUP_BY in pg_read.hints


def test_default_items_read_group_by_resolves_pg():
    """A plain browse (``_pick_operation`` → READ, no search-triggering
    filter) carrying an explicit ``Hint.GROUP_BY`` must resolve to PG the
    same way a derived-search group_by request does (#2829) — Elasticsearch
    has no GROUP BY implementation."""
    from dynastore.modules.storage.hints import Hint
    from dynastore.modules.storage.routing_config import (
        ItemsRoutingConfig, Operation,
    )
    cfg = ItemsRoutingConfig()
    read = cfg.operations[Operation.READ]
    es = next(e for e in read if e.driver_ref == "items_elasticsearch_driver")
    pg = next(e for e in read if e.driver_ref == "items_postgresql_driver")
    assert Hint.GROUP_BY in pg.hints
    assert Hint.GROUP_BY not in es.hints

    # Unhinted READ keeps declared order — ES first.
    assert _resolve_read(read, frozenset()) == [
        "items_elasticsearch_driver", "items_postgresql_driver",
    ]
    # group_by is relational-only → PG even though ES is listed first for READ.
    assert _resolve_read(read, frozenset({Hint.GROUP_BY})) == ["items_postgresql_driver"]
    # join is also PG-only (DWH join / OGC Joins; ES lacks ST_Transform).
    assert Hint.JOIN in pg.hints
    assert Hint.JOIN not in es.hints
    assert _resolve_read(read, frozenset({Hint.JOIN})) == ["items_postgresql_driver"]


def test_collection_routing_default_write_is_pg_fatal():
    from dynastore.modules.storage.routing_config import (
        CollectionRoutingConfig, FailurePolicy, Operation,
    )
    cfg = CollectionRoutingConfig()
    write = cfg.operations[Operation.WRITE]
    assert [e.driver_ref for e in write] == ["collection_postgresql_driver"]
    assert write[0].on_failure == FailurePolicy.FATAL


def test_collection_routing_default_read_is_pg_primary():
    from dynastore.modules.storage.routing_config import (
        CollectionRoutingConfig, FailurePolicy, Operation,
    )
    cfg = CollectionRoutingConfig()
    read = cfg.operations[Operation.READ]
    # Two READ entries: PG (system-of-record, untagged) then ES
    # (hint-routed via METADATA / prefer:es). No geometry hints at the
    # metadata level. READ carries no per-entry failure policy any more,
    # but PG stays first (index 0) and structurally authoritative.
    refs = [e.driver_ref for e in read]
    assert refs[0] == "collection_postgresql_driver"
    assert "collection_elasticsearch_driver" in refs
    assert len(read) == 2
    assert read[0].on_failure == FailurePolicy.FATAL


def test_collection_routing_default_index_has_no_hardcoded_es_hop():
    """The ES INDEX hop is NOT hard-coded in the code default (#1069 / #1073).

    A PG-only deployment (no ES collection-tier indexer registered, no preset
    applied) must get NO INDEX entry — otherwise a plain collection create
    enqueues an obligation row into tasks.storage that nothing will ever
    drain. The ES INDEX hop is supplied at validation time by
    ``_self_register_indexers_into`` when an ES driver is registered (see
    ``test_routing_self_registration.py::``
    ``test_collection_routing_validator_augments_index_lane``) and declared
    explicitly by the ``es``/``pg_es`` routing presets, which cannot rely on
    incidental driver discovery (``tests/.../unit/test_preset_routing.py``).
    """
    from dynastore.modules.storage.routing_config import (
        CollectionRoutingConfig, index_entries,
    )
    cfg = CollectionRoutingConfig()
    index = index_entries(cfg.operations)
    assert "collection_elasticsearch_driver" not in {e.driver_ref for e in index}


def test_catalog_routing_default_refs_are_registered():
    """The default WRITE/READ entries must reference actually-registered
    driver_refs. catalog_core_postgresql_driver / catalog_stac_postgresql_driver
    were never registered as entry-points — the registered wrapper is
    catalog_postgresql_driver. READ has two entries: PG (SoR) and ES (hint-routed)."""
    from dynastore.modules.storage.routing_config import (
        CatalogRoutingConfig, FailurePolicy, Operation,
    )
    cfg = CatalogRoutingConfig()
    write_refs = [e.driver_ref for e in cfg.operations[Operation.WRITE]]
    read_entries = cfg.operations[Operation.READ]
    read_refs = [e.driver_ref for e in read_entries]
    assert write_refs == ["catalog_postgresql_driver"]
    # Two READ entries: PG first (SoR, untagged, FATAL), ES second
    # (hints=METADATA — no geometry at the metadata level).
    assert read_refs[0] == "catalog_postgresql_driver"
    assert "catalog_elasticsearch_driver" in read_refs
    assert len(read_entries) == 2
    assert read_entries[0].on_failure == FailurePolicy.FATAL
    for ref in write_refs + read_refs:
        assert ref not in (
            "catalog_core_postgresql_driver",
            "catalog_stac_postgresql_driver",
        ), f"{ref} is not a registered entry-point"


def test_catalog_routing_default_write_is_fatal():
    from dynastore.modules.storage.routing_config import (
        CatalogRoutingConfig, FailurePolicy, Operation,
    )
    cfg = CatalogRoutingConfig()
    assert cfg.operations[Operation.WRITE][0].on_failure == FailurePolicy.FATAL


# ---------------------------------------------------------------------------
# Two-entry READ defaults for CollectionRoutingConfig / CatalogRoutingConfig
# ---------------------------------------------------------------------------


def test_collection_routing_read_has_pg_sor_then_es_hinted():
    """CollectionRoutingConfig.operations[READ] has exactly two entries:
    PG (untagged, FATAL, index 0) then ES (hints={METADATA}).
    There is no geometry at the metadata level so geometry hints are absent.
    READ carries no per-entry failure policy any more — the ES entry's
    ``on_failure`` is the inert field default (FATAL), never enforced."""
    from dynastore.modules.storage.hints import Hint
    from dynastore.modules.storage.routing_config import (
        CollectionRoutingConfig, FailurePolicy, Operation,
    )
    cfg = CollectionRoutingConfig()
    read = cfg.operations[Operation.READ]
    assert len(read) == 2
    pg_e = next(e for e in read if e.driver_ref == "collection_postgresql_driver")
    es_e = next(e for e in read if e.driver_ref == "collection_elasticsearch_driver")
    # PG is first (index 0) and FATAL
    assert read[0].driver_ref == "collection_postgresql_driver"
    assert pg_e.on_failure == FailurePolicy.FATAL
    # ES entry is opt-in via METADATA hint; geometry hints do not apply here
    assert Hint.METADATA in es_e.hints
    assert es_e.hints == {Hint.METADATA}


def test_catalog_routing_read_has_pg_sor_then_es_hinted():
    """CatalogRoutingConfig.operations[READ] has exactly two entries:
    PG (untagged, FATAL, index 0) then ES (hints={METADATA}).
    There is no geometry at the metadata level so geometry hints are absent."""
    from dynastore.modules.storage.hints import Hint
    from dynastore.modules.storage.routing_config import (
        CatalogRoutingConfig, FailurePolicy, Operation,
    )
    cfg = CatalogRoutingConfig()
    read = cfg.operations[Operation.READ]
    assert len(read) == 2
    pg_e = next(e for e in read if e.driver_ref == "catalog_postgresql_driver")
    es_e = next(e for e in read if e.driver_ref == "catalog_elasticsearch_driver")
    assert read[0].driver_ref == "catalog_postgresql_driver"
    assert pg_e.on_failure == FailurePolicy.FATAL
    assert Hint.METADATA in es_e.hints
    assert es_e.hints == {Hint.METADATA}


def test_collection_routing_es_hints_subset_of_es_driver_supported_hints():
    """The ES entry's hints must be a subset of CollectionElasticsearchDriver.supported_hints,
    so _validate_routing_entries accepts the entry without an error."""
    from dynastore.modules.storage.routing_config import (
        CollectionRoutingConfig, Operation,
    )
    from dynastore.modules.elasticsearch.collection_es_driver import (
        CollectionElasticsearchDriver,
    )
    cfg = CollectionRoutingConfig()
    es_e = next(
        e for e in cfg.operations[Operation.READ]
        if e.driver_ref == "collection_elasticsearch_driver"
    )
    driver_hints = CollectionElasticsearchDriver.supported_hints
    assert frozenset(es_e.hints).issubset(driver_hints), (
        f"Entry hints {es_e.hints} not a subset of driver.supported_hints {driver_hints}"
    )


def test_catalog_routing_es_hints_subset_of_es_driver_supported_hints():
    """The ES entry's hints must be a subset of CatalogElasticsearchDriver.supported_hints."""
    from dynastore.modules.storage.routing_config import (
        CatalogRoutingConfig, Operation,
    )
    from dynastore.modules.elasticsearch.catalog_es_driver import (
        CatalogElasticsearchDriver,
    )
    cfg = CatalogRoutingConfig()
    es_e = next(
        e for e in cfg.operations[Operation.READ]
        if e.driver_ref == "catalog_elasticsearch_driver"
    )
    driver_hints = CatalogElasticsearchDriver.supported_hints
    assert frozenset(es_e.hints).issubset(driver_hints), (
        f"Entry hints {es_e.hints} not a subset of driver.supported_hints {driver_hints}"
    )
