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

"""Pin routing defaults so a future edit can't silently flip ES back
from OUTBOX without explicit test update. PG must remain FATAL —
non-negotiable for authoritative writes."""
from __future__ import annotations


def test_default_pg_write_is_sync_fatal():
    from dynastore.modules.storage.routing_config import (
        FailurePolicy, ItemsRoutingConfig, Operation, WriteMode,
    )
    cfg = ItemsRoutingConfig()
    pg = next(
        e for e in cfg.operations[Operation.WRITE]
        if e.driver_ref == "items_postgresql_driver"
    )
    assert pg.on_failure == FailurePolicy.FATAL
    assert pg.write_mode == WriteMode.SYNC


def test_default_es_write_is_async_outbox():
    from dynastore.modules.storage.routing_config import (
        FailurePolicy, ItemsRoutingConfig, Operation, WriteMode,
    )
    cfg = ItemsRoutingConfig()
    es = next(
        e for e in cfg.operations[Operation.WRITE]
        if e.driver_ref == "items_elasticsearch_driver"
    )
    assert es.on_failure == FailurePolicy.OUTBOX
    assert es.write_mode == WriteMode.ASYNC


def test_default_read_routing_unchanged():
    """Sanity guard — Task 11 only touches WRITE; READ entries
    (ES geometry_simplified primary, PG geometry_exact) must remain."""
    from dynastore.modules.storage.routing_config import (
        FailurePolicy, ItemsRoutingConfig, Operation,
    )
    cfg = ItemsRoutingConfig()
    read = cfg.operations[Operation.READ]
    es_read = next(e for e in read if e.driver_ref == "items_elasticsearch_driver")
    pg_read = next(e for e in read if e.driver_ref == "items_postgresql_driver")
    assert "geometry_simplified" in es_read.hints
    assert "geometry_exact" in pg_read.hints
    # READ defaults retain their existing failure semantics — flag if changed.
    assert pg_read.on_failure == FailurePolicy.FATAL


def test_default_items_search_declares_op_appropriate_hints():
    """SEARCH entries declare the search-flavour hints each driver serves so a
    resolved config is self-documenting. ES carries the search engine flavours
    (search/fulltext/attribute_filter/...); PG carries the relational ones
    (attribute_filter/group_by/geometry_exact/...)."""
    from dynastore.modules.storage.hints import Hint
    from dynastore.modules.storage.routing_config import (
        ItemsRoutingConfig, Operation,
    )
    cfg = ItemsRoutingConfig()
    search = cfg.operations[Operation.SEARCH]
    es = next(e for e in search if e.driver_ref == "items_elasticsearch_driver")
    pg = next(e for e in search if e.driver_ref == "items_postgresql_driver")
    # both serve the common filter/sort flavours
    for h in (Hint.ATTRIBUTE_FILTER, Hint.SPATIAL_FILTER, Hint.SORT):
        assert h in es.hints and h in pg.hints
    # engine-only vs relational-only
    assert Hint.FULLTEXT in es.hints and Hint.SEARCH in es.hints
    assert Hint.GROUP_BY in pg.hints
    assert Hint.GEOMETRY_SIMPLIFIED in es.hints
    assert Hint.GEOMETRY_EXACT in pg.hints


def test_default_items_search_excludes_read_only_hints():
    """``tiles``, ``write``, and ``metadata`` only make sense on READ — they
    must NOT appear in any SEARCH entry.  ``join`` IS declared on the PG SEARCH
    entry so that a JOIN request with a CQL filter (_pick_operation → SEARCH)
    still routes to PG rather than relaxing to ES."""
    from dynastore.modules.storage.hints import Hint
    from dynastore.modules.storage.routing_config import (
        ItemsRoutingConfig, Operation,
    )
    cfg = ItemsRoutingConfig()
    for e in cfg.operations[Operation.SEARCH]:
        assert Hint.TILES not in e.hints
        assert Hint.WRITE not in e.hints
        assert Hint.METADATA not in e.hints
    # JOIN must NOT appear on the ES SEARCH entry (ES cannot serve join queries).
    es_search = next(
        e for e in cfg.operations[Operation.SEARCH]
        if e.driver_ref == "items_elasticsearch_driver"
    )
    assert Hint.JOIN not in es_search.hints
    # JOIN MUST appear on the PG SEARCH entry (for CQL-filtered JOIN requests).
    pg_search = next(
        e for e in cfg.operations[Operation.SEARCH]
        if e.driver_ref == "items_postgresql_driver"
    )
    assert Hint.JOIN in pg_search.hints
    # tiles is still declared where it belongs: the PG READ entry.
    read = cfg.operations[Operation.READ]
    pg_read = next(e for e in read if e.driver_ref == "items_postgresql_driver")
    assert Hint.TILES in pg_read.hints
    # JOIN is also declared on the PG READ entry (DWH join / OGC Joins).
    assert Hint.JOIN in pg_read.hints


def test_default_items_filtered_search_resolves_es_first():
    """A filtered/sorted search routes to ES first (the search engine): the
    best-overlap matcher's longest-effective tiebreak ranks ES above PG when
    their hint surfaces are equal in size (ES wins via entry-order index 0).
    Unfiltered search keeps declared order (ES then PG)."""
    from dynastore.modules.storage.hints import Hint
    from dynastore.modules.storage.routing_config import (
        ItemsRoutingConfig, Operation,
    )
    cfg = ItemsRoutingConfig()
    search = cfg.operations[Operation.SEARCH]
    es = next(e for e in search if e.driver_ref == "items_elasticsearch_driver")
    pg = next(e for e in search if e.driver_ref == "items_postgresql_driver")

    def resolve(requested):
        if not requested:
            return [e.driver_ref for e in search]
        matched = [
            (i, e) for i, e in enumerate(search)
            if requested.issubset(frozenset(e.hints))
        ]
        matched.sort(key=lambda t: (-len(t[1].hints), t[0]))
        return [e.driver_ref for _, e in matched]

    # ES and PG now have equal surface size; ES wins common-flavour requests via
    # entry-order tiebreak (index 0), not by being strictly larger.
    assert len(es.hints) >= len(pg.hints)
    assert resolve(frozenset()) == [
        "items_elasticsearch_driver", "items_postgresql_driver",
    ]
    for flavour in (Hint.ATTRIBUTE_FILTER, Hint.SPATIAL_FILTER, Hint.SORT):
        assert resolve(frozenset({flavour}))[0] == "items_elasticsearch_driver"
    # group_by is relational-only → PG even though ES is listed first
    assert resolve(frozenset({Hint.GROUP_BY})) == ["items_postgresql_driver"]
    # join routes to PG (ES does not declare it).
    assert resolve(frozenset({Hint.JOIN})) == ["items_postgresql_driver"]


def test_default_items_read_group_by_resolves_pg():
    """A plain browse (``_pick_operation`` → READ, no search-triggering
    filter) carrying an explicit ``Hint.GROUP_BY`` must resolve to PG the
    same way a SEARCH-routed group_by request does (#2829) — Elasticsearch
    has no GROUP BY implementation. Mirrors
    ``test_default_items_filtered_search_resolves_es_first``'s matcher but
    against the real ``Operation.READ`` entries (ES listed first by default)."""
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

    def resolve(requested):
        if not requested:
            return [e.driver_ref for e in read]
        matched = [
            (i, e) for i, e in enumerate(read)
            if requested.issubset(frozenset(e.hints))
        ]
        matched.sort(key=lambda t: (-len(t[1].hints), t[0]))
        return [e.driver_ref for _, e in matched]

    # Unhinted READ keeps declared order — ES first.
    assert resolve(frozenset()) == [
        "items_elasticsearch_driver", "items_postgresql_driver",
    ]
    # group_by is relational-only → PG even though ES is listed first for READ.
    assert resolve(frozenset({Hint.GROUP_BY})) == ["items_postgresql_driver"]


def test_collection_routing_default_write_is_pg_fatal():
    from dynastore.modules.storage.routing_config import (
        CollectionRoutingConfig, FailurePolicy, Operation, WriteMode,
    )
    cfg = CollectionRoutingConfig()
    write = cfg.operations[Operation.WRITE]
    assert [e.driver_ref for e in write] == ["collection_postgresql_driver"]
    assert write[0].on_failure == FailurePolicy.FATAL
    assert write[0].write_mode == WriteMode.SYNC


def test_collection_routing_default_read_is_pg_primary():
    from dynastore.modules.storage.routing_config import (
        CollectionRoutingConfig, FailurePolicy, Operation,
    )
    cfg = CollectionRoutingConfig()
    read = cfg.operations[Operation.READ]
    # Two READ entries: PG (system-of-record, untagged, FATAL) then ES
    # (hint-routed via METADATA / prefer:es, WARN). No geometry hints at
    # the metadata level.
    refs = [e.driver_ref for e in read]
    assert refs[0] == "collection_postgresql_driver"
    assert "collection_elasticsearch_driver" in refs
    assert len(read) == 2
    assert read[0].on_failure == FailurePolicy.FATAL


def test_collection_routing_default_index_has_no_hardcoded_es_hop():
    """The ES secondary-index hop is NOT hard-coded in the code default
    (#1069 / #1073).

    A PG-only deployment (no ES CollectionIndexer registered, no preset
    applied) must get NO secondary-index WRITE entry — otherwise a plain
    collection create enqueues an OUTBOX row into tasks.tasks that nothing
    will ever drain, which poisons the create transaction when the outbox
    table is absent. The ES secondary-index hop (ASYNC + OUTBOX) is supplied
    at validation time by ``_self_register_indexers_into`` when an ES driver
    is registered (see test_collection_routing_validator_augments_write_index_and_search)
    and by the routing presets (see test_preset_public_catalog)."""
    from dynastore.modules.storage.routing_config import (
        CollectionRoutingConfig, secondary_index_entries,
    )
    cfg = CollectionRoutingConfig()
    index = secondary_index_entries(cfg.operations)
    assert "collection_elasticsearch_driver" not in {e.driver_ref for e in index}


def test_collection_routing_default_search_is_es_first_pg_fallback():
    from dynastore.modules.storage.routing_config import (
        CollectionRoutingConfig, Operation,
    )
    cfg = CollectionRoutingConfig()
    search = cfg.operations[Operation.SEARCH]
    refs = [e.driver_ref for e in search]
    assert refs[0] == "collection_elasticsearch_driver"
    assert "collection_postgresql_driver" in refs


def test_collection_routing_default_search_carries_geometry_hints():
    """SEARCH entries declare which geometry precision they serve so a
    consumer can route to PG via hint='geometry_exact'. ES carries
    GEOMETRY_SIMPLIFIED (fast, lossy); PG carries GEOMETRY_EXACT (full WKB).
    Mirrors ItemsRoutingConfig READ defaults."""
    from dynastore.modules.storage.hints import Hint
    from dynastore.modules.storage.routing_config import (
        CollectionRoutingConfig, Operation,
    )
    cfg = CollectionRoutingConfig()
    search = cfg.operations[Operation.SEARCH]
    es = next(e for e in search if e.driver_ref == "collection_elasticsearch_driver")
    pg = next(e for e in search if e.driver_ref == "collection_postgresql_driver")
    assert Hint.GEOMETRY_SIMPLIFIED in es.hints
    assert Hint.GEOMETRY_EXACT in pg.hints


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
    # (hints=METADATA, WARN — no geometry at the metadata level).
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
# Task D.2 — two-entry READ defaults for CollectionRoutingConfig / CatalogRoutingConfig
# ---------------------------------------------------------------------------


def test_collection_routing_read_has_pg_sor_then_es_hinted():
    """CollectionRoutingConfig.operations[READ] has exactly two entries:
    PG (untagged, FATAL) then ES (hints={METADATA}, WARN).
    There is no geometry at the metadata level so geometry hints are absent."""
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
    assert es_e.on_failure == FailurePolicy.WARN


def test_catalog_routing_read_has_pg_sor_then_es_hinted():
    """CatalogRoutingConfig.operations[READ] has exactly two entries:
    PG (untagged, FATAL) then ES (hints={METADATA}, WARN).
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
    assert es_e.on_failure == FailurePolicy.WARN


def test_collection_routing_es_hints_subset_of_es_driver_supported_hints():
    """The ES entry's hints must be a subset of CollectionElasticsearchDriver.supported_hints,
    so _validate_routing_entries accepts the entry without an error."""
    from dynastore.modules.storage.hints import Hint
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
    from dynastore.modules.storage.hints import Hint
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
