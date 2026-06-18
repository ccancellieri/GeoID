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

"""Routing-config self-registration: every installed store driver
appears in ``operations[WRITE]`` / ``operations[READ]`` after the
auto-append step fires — closes the "implicit fan-out invisible to
operators" antipattern.
"""

from __future__ import annotations

from dynastore.models.protocols.storage_driver import Capability
from dynastore.modules.storage.routing_config import (
    CatalogRoutingConfig,
    CollectionRoutingConfig,
    ItemsRoutingConfig,
    FailurePolicy,
    Operation,
    OperationDriverEntry,
    _self_register_store_drivers,
)


class _FakeStore:
    """A store-driver stand-in declaring capabilities.

    The store auto-append is capability-gated (#1179): a driver is only
    appended to an operation its capabilities support. Real store drivers
    declare WRITE/READ; a capability-less stand-in (the default ``object()``
    used previously) is correctly skipped, so stand-ins must carry caps.
    """

    def __init__(self, capabilities=frozenset({Capability.WRITE, Capability.READ})):
        self.capabilities = capabilities


def test_collection_self_registers_missing_store_drivers():
    """Empty metadata.operations + 2 installed drivers → both auto-appended
    to WRITE and READ."""
    cfg = CollectionRoutingConfig()
    cfg.operations.clear()

    metadata_index = {"pg_core_meta": _FakeStore(), "pg_stac_meta": _FakeStore()}
    _self_register_store_drivers(cfg, metadata_index)

    write_ids = {e.driver_ref for e in cfg.operations[Operation.WRITE]}
    read_ids = {e.driver_ref for e in cfg.operations[Operation.READ]}
    assert write_ids == {"pg_core_meta", "pg_stac_meta"}
    assert read_ids == {"pg_core_meta", "pg_stac_meta"}


def test_collection_operator_managed_list_locks_out_auto_augment():
    """Option A (#792 / #889): once an operator has touched the list,
    the self-register helpers do not augment it.  Other discoverable
    drivers stay out until the operator either lists them explicitly
    or drops the list back to defaults.
    """
    cfg = CollectionRoutingConfig()
    cfg.operations.clear()
    cfg.operations[Operation.WRITE] = [
        OperationDriverEntry(
            driver_ref="pg_core_meta", on_failure=FailurePolicy.WARN,
            # Field default is source="operator"; explicit here for clarity.
            source="operator",
        ),
    ]

    metadata_index = {"pg_core_meta": _FakeStore(), "pg_stac_meta": _FakeStore()}
    _self_register_store_drivers(cfg, metadata_index)

    write_entries = {
        e.driver_ref: e for e in cfg.operations[Operation.WRITE]
    }
    assert write_entries["pg_core_meta"].on_failure == FailurePolicy.WARN
    assert write_entries["pg_core_meta"].source == "operator"
    # PgStacMeta was missing and stays missing — operator-managed list.
    assert "pg_stac_meta" not in write_entries


def test_collection_no_op_when_all_drivers_already_listed():
    """All installed drivers already present → no duplicates appended."""
    cfg = CollectionRoutingConfig()
    cfg.operations.clear()
    cfg.operations[Operation.WRITE] = [
        OperationDriverEntry(driver_ref="pg_core_meta"),
        OperationDriverEntry(driver_ref="pg_stac_meta"),
    ]
    cfg.operations[Operation.READ] = [
        OperationDriverEntry(driver_ref="pg_core_meta"),
        OperationDriverEntry(driver_ref="pg_stac_meta"),
    ]

    metadata_index = {"pg_core_meta": _FakeStore(), "pg_stac_meta": _FakeStore()}
    _self_register_store_drivers(cfg, metadata_index)

    assert len(cfg.operations[Operation.WRITE]) == 2
    assert len(cfg.operations[Operation.READ]) == 2


def test_catalog_self_registers_missing_drivers():
    """Catalog tier: same self-registration shape on the top-level operations."""
    cfg = CatalogRoutingConfig()
    cfg.operations.clear()

    metadata_index = {"catalog_pg_core": _FakeStore(), "catalog_pg_stac": _FakeStore()}
    _self_register_store_drivers(cfg, metadata_index)

    write_ids = {e.driver_ref for e in cfg.operations[Operation.WRITE]}
    read_ids = {e.driver_ref for e in cfg.operations[Operation.READ]}
    assert write_ids == {"catalog_pg_core", "catalog_pg_stac"}
    assert read_ids == {"catalog_pg_core", "catalog_pg_stac"}


def test_capability_less_store_driver_is_not_auto_appended():
    """#1179: a driver that satisfies the *Store protocol structurally but
    declares no capabilities (e.g. the diagnostic LogCatalogIndexer, which is
    discoverable as a CatalogStore) must NOT be auto-appended. Previously it
    was injected here and then rejected by the capability gate in
    ``_validate_routing_entries`` — making the routing config impossible to
    PUT at all (even a no-op round-trip of the default config returned 400).
    """
    cfg = CatalogRoutingConfig()
    cfg.operations.clear()

    index = {
        "catalog_pg_core": _FakeStore(),
        "log_catalog_indexer": _FakeStore(capabilities=frozenset()),
    }
    _self_register_store_drivers(cfg, index)

    write_ids = {e.driver_ref for e in cfg.operations.get(Operation.WRITE, [])}
    read_ids = {e.driver_ref for e in cfg.operations.get(Operation.READ, [])}
    # The real store driver is auto-appended …
    assert "catalog_pg_core" in write_ids
    assert "catalog_pg_core" in read_ids
    # … the capability-less indexer is not.
    assert "log_catalog_indexer" not in write_ids
    assert "log_catalog_indexer" not in read_ids


def test_partial_capability_store_driver_only_lands_in_supported_ops():
    """A driver with only WRITE capability auto-appends to WRITE, not READ."""
    cfg = CatalogRoutingConfig()
    cfg.operations.clear()

    index = {"write_only": _FakeStore(capabilities=frozenset({Capability.WRITE}))}
    _self_register_store_drivers(cfg, index)

    write_ids = {e.driver_ref for e in cfg.operations.get(Operation.WRITE, [])}
    read_ids = {e.driver_ref for e in cfg.operations.get(Operation.READ, [])}
    assert write_ids == {"write_only"}
    assert read_ids == set()


def test_validate_catalog_routing_handler_tolerates_capability_less_store():
    """#1179 end-to-end: ``_validate_catalog_routing_config`` must NOT raise when
    a capability-less store driver (LogCatalogIndexer) is discoverable.

    Before the fix, the un-gated store auto-append injected
    ``log_catalog_indexer`` into WRITE/READ and the capability gate then raised
    ``ValueError: ... does not support operation 'WRITE'`` — so even a no-op
    round-trip PUT of the default catalog routing config returned HTTP 400. This
    pins the auto-append + capability-gate interaction the live failure exercised.
    """
    import asyncio
    from unittest.mock import patch

    from dynastore.models.protocols.entity_store import CatalogStore
    from dynastore.modules.storage.routing_config import (
        _validate_catalog_routing_config,
    )

    # Distinct class names so _to_snake() yields the expected driver_refs.
    class CatalogPostgresqlDriver:
        capabilities = frozenset({Capability.WRITE, Capability.READ})
        supported_hints: frozenset = frozenset()

    class LogCatalogIndexer:  # structurally a CatalogStore, but capability-less
        capabilities: frozenset = frozenset()
        supported_hints: frozenset = frozenset()

    pool = [CatalogPostgresqlDriver(), LogCatalogIndexer()]

    cfg = CatalogRoutingConfig()
    cfg.operations.clear()
    # source="auto" → the list is NOT operator-managed, so the store
    # auto-append fires (this is what injected log_catalog_indexer pre-fix).
    cfg.operations[Operation.WRITE] = [
        OperationDriverEntry(driver_ref="catalog_postgresql_driver", source="auto"),
    ]
    cfg.operations[Operation.READ] = [
        OperationDriverEntry(driver_ref="catalog_postgresql_driver", source="auto"),
    ]

    def _fake_get_protocols(proto):
        return pool if proto is CatalogStore else []

    with patch("dynastore.tools.discovery.get_protocols", _fake_get_protocols):
        # Must complete without raising (pre-#1179 this raised ValueError).
        asyncio.run(
            _validate_catalog_routing_config(cfg, None, None, None)
        )

    write_ids = {e.driver_ref for e in cfg.operations.get(Operation.WRITE, [])}
    read_ids = {e.driver_ref for e in cfg.operations.get(Operation.READ, [])}
    assert "catalog_postgresql_driver" in write_ids
    assert "log_catalog_indexer" not in write_ids
    assert "log_catalog_indexer" not in read_ids


def test_self_registration_skips_zero_drivers():
    """Empty driver index → no entries appended (no spurious empty list creation
    for ops that were already absent)."""
    cfg = CollectionRoutingConfig()
    cfg.operations.clear()

    _self_register_store_drivers(cfg, store_driver_index={})

    # Operations stay empty — auto-append only adds for present drivers.
    assert cfg.operations.get(Operation.WRITE, []) == []
    assert cfg.operations.get(Operation.READ, []) == []


# ---------------------------------------------------------------------------
# Per-tier indexer marker self-registration
# ---------------------------------------------------------------------------


def test_indexer_marker_lands_in_write_with_async_outbox_defaults():
    """A driver opting in to a tier indexer marker auto-registers under
    operations[WRITE] as a secondary index (secondary_index=True) with
    write_mode=async, on_failure=outbox — sourced from the per-tier
    marker, not from generic capability discovery.  OUTBOX (not WARN) is
    the default so transient indexer failures enqueue a durable retry row
    instead of dropping silently.
    """
    from typing import ClassVar
    from unittest.mock import patch

    from dynastore.models.protocols.indexer import CollectionIndexer
    from dynastore.modules.storage.routing_config import (
        WriteMode,
        _self_register_indexers_into,
        secondary_index_entries,
    )

    class _CollectionES:
        is_collection_indexer: ClassVar[bool] = True
        auto_register_for_routing: ClassVar = frozenset({Operation.WRITE})

    class _NotAnIndexer:
        pass

    target_ops: dict = {}
    fake_pool = [_CollectionES(), _NotAnIndexer()]

    def _fake_get_protocols(proto):
        return [d for d in fake_pool if isinstance(d, proto)]

    with patch("dynastore.tools.discovery.get_protocols", _fake_get_protocols):
        _self_register_indexers_into(target_ops, CollectionIndexer)

    entries = secondary_index_entries(target_ops)
    assert len(entries) == 1
    assert entries[0].driver_ref == "_collection_es"
    assert entries[0].on_failure == FailurePolicy.OUTBOX
    assert entries[0].write_mode == WriteMode.ASYNC
    assert entries[0].secondary_index is True


def test_validate_handlers_invoke_indexer_self_registration():
    """Each routing-config validate handler MUST invoke
    ``_self_register_indexers_into`` against the matching tier marker.
    Pins the wiring against accidental drop in a future refactor.

    Self-registration moved apply→validate in #738/#747 — it must run
    pre-persist so the auto-registered ``source="auto"`` entries are
    actually serialized into the stored config.
    """
    import asyncio
    from unittest.mock import patch

    from dynastore.modules.storage.routing_config import (
        AssetRoutingConfig,
        _validate_asset_routing_config,
        _validate_catalog_routing_config,
        _validate_collection_routing_config,
        _validate_items_routing_config,
    )

    calls: list[type] = []

    def _spy(target_ops, marker_proto, **_kwargs):
        # Accept arbitrary kwargs (e.g. ``search_caps`` on the searcher
        # helper) so the spy works for both helper signatures.
        calls.append(marker_proto)

    # Empty operations so _validate_routing_entries has nothing to check
    # against the (stubbed-empty) driver registry.
    items = ItemsRoutingConfig()
    items.operations.clear()
    coll = CollectionRoutingConfig()
    coll.operations.clear()
    asset = AssetRoutingConfig()
    asset.operations.clear()
    cat = CatalogRoutingConfig()
    cat.operations.clear()

    with patch(
        "dynastore.modules.storage.routing_config._self_register_indexers_into",
        _spy,
    ), patch(
        "dynastore.tools.discovery.get_protocols",
        lambda proto: [],
    ):
        asyncio.run(_validate_items_routing_config(
            items, catalog_id=None, collection_id=None, db_resource=None,
        ))
        asyncio.run(_validate_collection_routing_config(
            coll, catalog_id=None, collection_id=None, db_resource=None,
        ))
        asyncio.run(_validate_asset_routing_config(
            asset, catalog_id=None, collection_id=None, db_resource=None,
        ))
        asyncio.run(_validate_catalog_routing_config(
            cat, catalog_id=None, collection_id=None, db_resource=None,
        ))

    from dynastore.models.protocols.indexer import (
        AssetIndexer,
        CatalogIndexer,
        CollectionIndexer,
        ItemIndexer,
    )
    # Each tier's validate handler invokes indexer registration with its own
    # marker Protocol. Order matches the invocation order above.
    assert calls == [ItemIndexer, CollectionIndexer, AssetIndexer, CatalogIndexer]


def test_end_to_end_marker_to_write_index_entry_via_real_validate_handler():
    """End-to-end: register a real driver opting in to ``CatalogIndexer``,
    invoke ``_validate_catalog_routing_config`` against a fresh
    ``CatalogRoutingConfig``, assert the driver lands in
    ``operations[WRITE]`` as a secondary index with the marker's defaults
    (async + outbox).

    Validates the full chain: marker discovery → helper invocation →
    entry with correct durable-retry policy.  Self-registration moved
    apply→validate in #738/#747.
    """
    import asyncio
    from typing import ClassVar

    from dynastore.models.protocols.indexer import CatalogIndexer
    from dynastore.modules.storage.routing_config import (
        WriteMode,
        _validate_catalog_routing_config,
        secondary_index_entries,
    )
    from dynastore.tools.discovery import register_plugin, unregister_plugin

    class _DummyCatalogIndexer:
        is_catalog_indexer: ClassVar[bool] = True
        auto_register_for_routing: ClassVar = frozenset({Operation.WRITE})
        # Minimal CatalogStore surface — enough for
        # _validate_routing_entries to accept it under operations[WRITE]
        # if it were referenced (it isn't pre-apply; the marker self-
        # registration appends it).  We avoid populating WRITE/READ to
        # skip validation for those op-keys.
        capabilities = frozenset()

    instance = _DummyCatalogIndexer()
    register_plugin(instance)
    try:
        cfg = CatalogRoutingConfig()
        cfg.operations.clear()  # skip default validation against unregistered drivers

        asyncio.run(_validate_catalog_routing_config(
            cfg, catalog_id=None, collection_id=None, db_resource=None,
        ))

        index_entries = secondary_index_entries(cfg.operations)
        assert any(
            e.driver_ref == "_dummy_catalog_indexer"
            and e.on_failure == FailurePolicy.OUTBOX
            and e.write_mode == WriteMode.ASYNC
            for e in index_entries
        ), f"_DummyCatalogIndexer not auto-registered: {index_entries!r}"
    finally:
        unregister_plugin(instance)
        # Sanity: ensure cleanup so other tests don't see this stub.
        from dynastore.tools.discovery import get_protocols
        assert not any(
            isinstance(d, CatalogIndexer) and type(d).__name__ == "_dummy_catalog_indexer"
            for d in get_protocols(CatalogIndexer)
        )


def test_indexer_marker_skips_already_listed_driver():
    """Operator-supplied WRITE secondary-index entry survives — only missing
    drivers get appended."""
    from typing import ClassVar
    from unittest.mock import patch

    from dynastore.models.protocols.indexer import AssetIndexer
    from dynastore.modules.storage.routing_config import _self_register_indexers_into

    class _AssetES:
        is_asset_indexer: ClassVar[bool] = True
        auto_register_for_routing: ClassVar = frozenset({Operation.WRITE})

    operator_entry = OperationDriverEntry(
        driver_ref="_asset_es", on_failure=FailurePolicy.FATAL,
    )
    target_ops: dict = {Operation.WRITE: [operator_entry]}

    with patch("dynastore.tools.discovery.get_protocols",
               lambda proto: [_AssetES()] if proto is AssetIndexer else []):
        _self_register_indexers_into(target_ops, AssetIndexer)

    # No duplicate; operator-supplied on_failure=FATAL preserved. The
    # discovered indexer marker stamps secondary_index=True upward.
    assert len(target_ops[Operation.WRITE]) == 1
    assert target_ops[Operation.WRITE][0].on_failure == FailurePolicy.FATAL
    assert target_ops[Operation.WRITE][0].secondary_index is True


# ---------------------------------------------------------------------------
# SEARCH self-registration helper
# ---------------------------------------------------------------------------


def test_searcher_helper_picks_up_drivers_opting_into_search():
    """Drivers declaring ``Operation.SEARCH`` in
    ``auto_register_for_routing`` land in operations[SEARCH].  Drivers
    that don't opt in stay out.

    The previous cap-based gate (``EntityStoreCapability.SEARCH`` /
    ``Capability.FULLTEXT`` / …) was retired alongside the migration to
    the per-Operation auto-default set: capabilities are structural facts
    only; per-request flavours (fulltext/aggregation/count) live in the
    ``Hint`` catalogue.  Auto-augmentation gates purely on the Op-set.
    """
    from typing import ClassVar
    from unittest.mock import patch

    from dynastore.models.protocols.entity_store import CatalogStore
    from dynastore.modules.storage.routing_config import (
        _self_register_searchers_into,
    )

    class _ESCat:
        auto_register_for_routing: ClassVar = frozenset({Operation.SEARCH})

    class _IndexerOnly:
        # Opts into WRITE (secondary index) but not SEARCH — should NOT land
        # in SEARCH.
        auto_register_for_routing: ClassVar = frozenset({Operation.WRITE})

    class _NotOptedIn:
        # No declaration → defaults to frozenset() → not auto-augmented.
        pass

    target_ops: dict = {}
    fake_pool = [_ESCat(), _IndexerOnly(), _NotOptedIn()]

    with patch("dynastore.tools.discovery.get_protocols",
               lambda proto: fake_pool):
        _self_register_searchers_into(target_ops, CatalogStore)

    ids = {e.driver_ref for e in target_ops.get(Operation.SEARCH, [])}
    assert ids == {"_es_cat"}


def test_searcher_helper_skips_drivers_without_search_optin():
    """Driver without ``Operation.SEARCH`` in its Op-set is not added
    to SEARCH, regardless of which capabilities it declares.

    Capabilities are structural facts; they do not gate auto-augmentation
    under the new model.
    """
    from unittest.mock import patch

    from dynastore.models.protocols.entity_store import (
        CatalogStore,
        EntityStoreCapability,
    )
    from dynastore.modules.storage.routing_config import (
        _self_register_searchers_into,
    )

    class _PgPrimary:
        # Has lots of caps but no Op-set → no auto-default into SEARCH.
        capabilities = frozenset({
            EntityStoreCapability.READ, EntityStoreCapability.WRITE,
            EntityStoreCapability.SEARCH,  # cap is no longer the gate
        })

    target_ops: dict = {}
    with patch("dynastore.tools.discovery.get_protocols",
               lambda proto: [_PgPrimary()]):
        _self_register_searchers_into(target_ops, CatalogStore)
    assert target_ops.get(Operation.SEARCH, []) == []


def test_searcher_helper_idempotent():
    """Repeated calls don't add duplicates; operator-supplied entry survives."""
    from typing import ClassVar
    from unittest.mock import patch

    from dynastore.models.protocols.entity_store import CatalogStore
    from dynastore.modules.storage.routing_config import (
        _self_register_searchers_into,
    )

    class _ESCat:
        auto_register_for_routing: ClassVar = frozenset({Operation.SEARCH})

    from dynastore.modules.storage.hints import Hint
    op_entry = OperationDriverEntry(driver_ref="_es_cat", hints={Hint.METADATA})
    target_ops: dict = {Operation.SEARCH: [op_entry]}

    with patch("dynastore.tools.discovery.get_protocols",
               lambda proto: [_ESCat()]):
        _self_register_searchers_into(target_ops, CatalogStore)
        _self_register_searchers_into(target_ops, CatalogStore)

    assert len(target_ops[Operation.SEARCH]) == 1
    assert target_ops[Operation.SEARCH][0].hints == {Hint.METADATA}


# ---------------------------------------------------------------------------
# Read-time model_validator augmentation
# ---------------------------------------------------------------------------


def test_catalog_routing_validator_augments_write_index_and_search():
    """Constructing a default CatalogRoutingConfig must fold in
    discoverable CatalogIndexer (as a WRITE secondary index) + SEARCH-capable
    drivers."""
    from typing import ClassVar
    from unittest.mock import patch

    from dynastore.models.protocols.entity_store import EntityStoreCapability
    from dynastore.modules.storage.routing_config import secondary_index_entries

    class _CatES:
        is_catalog_indexer: ClassVar[bool] = True
        auto_register_for_routing: ClassVar = frozenset({Operation.WRITE, Operation.SEARCH})

    instance = _CatES()

    def _fake_get_protocols(proto):
        # Marker check + capability check both rely on isinstance —
        # the fake instance has the right ClassVar + duck-typed
        # `capabilities` set so it satisfies both runtime_checkable
        # Protocols (CatalogIndexer + CatalogStore via
        # the SEARCH cap predicate inside _self_register_searchers_into).
        return [instance]

    with patch("dynastore.tools.discovery.get_protocols", _fake_get_protocols):
        cfg = CatalogRoutingConfig()

    index_ids = {e.driver_ref for e in secondary_index_entries(cfg.operations)}
    search_ids = {e.driver_ref for e in cfg.operations.get(Operation.SEARCH, [])}
    assert "_cat_es" in index_ids
    assert "_cat_es" in search_ids
    # Primary WRITE entry unchanged — the registered CatalogStore is the
    # ``catalog_postgresql_driver`` composition wrapper (#732), which fans
    # CRUD across the catalog_core + catalog_stac sidecars internally. The
    # discovered ES indexer joins the same WRITE list as a secondary index
    # (role distinguished by ``secondary_index``), not as a primary store.
    primary_write_ids = {
        e.driver_ref for e in cfg.operations[Operation.WRITE]
        if not e.secondary_index
    }
    assert primary_write_ids == {"catalog_postgresql_driver"}


def test_catalog_routing_validator_no_op_when_no_indexers_discoverable():
    """No discoverable indexer/searcher → operations stays at the
    default-factory shape: primary WRITE+READ only. Secondary-index and
    SEARCH are both discovery-driven, so with nothing discoverable the ES
    secondary-index hop is NOT present (#1069 / #1073) — a PG-only deployment
    must not pin an undrainable OUTBOX hop into tasks.tasks."""
    from unittest.mock import patch

    from dynastore.modules.storage.routing_config import secondary_index_entries

    with patch("dynastore.tools.discovery.get_protocols", lambda proto: []):
        cfg = CatalogRoutingConfig()

    # Nothing discoverable → no ES secondary-index hop folded in.
    index_ids = {e.driver_ref for e in secondary_index_entries(cfg.operations)}
    assert "catalog_elasticsearch_driver" not in index_ids
    # SEARCH is purely discovery-driven — empty when nothing is discoverable.
    assert Operation.SEARCH not in cfg.operations or cfg.operations[Operation.SEARCH] == []


def test_collection_routing_validator_augments_write_index_and_search():
    """ItemsRoutingConfig validator augments metadata.operations
    (not top-level operations)."""
    from typing import ClassVar
    from unittest.mock import patch

    from dynastore.models.protocols.entity_store import EntityStoreCapability
    from dynastore.modules.storage.routing_config import secondary_index_entries

    class _ColES:
        is_collection_indexer: ClassVar[bool] = True
        auto_register_for_routing: ClassVar = frozenset({Operation.WRITE, Operation.SEARCH})

    with patch("dynastore.tools.discovery.get_protocols",
               lambda proto: [_ColES()]):
        cfg = CollectionRoutingConfig()

    index_ids = {e.driver_ref for e in secondary_index_entries(cfg.operations)}
    search_ids = {e.driver_ref for e in cfg.operations.get(Operation.SEARCH, [])}
    assert "_col_es" in index_ids
    assert "_col_es" in search_ids


def test_items_routing_validator_augments_write_index_and_search():
    """The model_validator on ItemsRoutingConfig augments its `operations`
    with discoverable ItemIndexer drivers (→ WRITE secondary index) and
    CollectionItemsStore drivers declaring storage SEARCH-family caps
    (FULLTEXT / SPATIAL_FILTER / ATTRIBUTE_FILTER) (→ SEARCH).
    """
    from typing import ClassVar
    from unittest.mock import patch

    from dynastore.models.protocols.storage_driver import Capability
    from dynastore.modules.storage.routing_config import secondary_index_entries

    class _ItemsES:
        is_item_indexer: ClassVar[bool] = True
        auto_register_for_routing: ClassVar = frozenset({Operation.WRITE, Operation.SEARCH})

    with patch("dynastore.tools.discovery.get_protocols",
               lambda proto: [_ItemsES()]):
        cfg = ItemsRoutingConfig()

    top_index = {e.driver_ref for e in secondary_index_entries(cfg.operations)}
    top_search = {e.driver_ref for e in cfg.operations.get(Operation.SEARCH, [])}
    assert "_items_es" in top_index
    assert "_items_es" in top_search
    # Primary PG WRITE entry retained; the indexer is appended as a
    # secondary index alongside it.
    write_ids = {e.driver_ref for e in cfg.operations[Operation.WRITE]}
    assert write_ids == {"items_postgresql_driver", "_items_es"} or \
           "items_postgresql_driver" in write_ids


def test_items_routing_search_optin_gate():
    """ItemsRoutingConfig SEARCH gate is the per-Operation opt-in set.
    The cap-based gate (storage ``Capability.{FULLTEXT, SPATIAL_FILTER,
    ATTRIBUTE_FILTER}`` vs metadata ``EntityStoreCapability.SEARCH``)
    was retired alongside PR #3a — capabilities are structural facts
    only.  A driver lands in items SEARCH iff its class declares
    ``Operation.SEARCH`` in ``auto_register_for_routing``.
    """
    from typing import ClassVar
    from unittest.mock import patch

    class _OptedInSearcher:
        is_item_indexer: ClassVar[bool] = False
        auto_register_for_routing: ClassVar = frozenset({Operation.SEARCH})

    class _OptedOutSearcher:
        # Capabilities are irrelevant under the new model — only the
        # Op-set decides auto-augmentation.
        is_item_indexer: ClassVar[bool] = False

    with patch("dynastore.tools.discovery.get_protocols",
               lambda proto: [_OptedInSearcher(), _OptedOutSearcher()]):
        cfg = ItemsRoutingConfig()

    top_search = {e.driver_ref for e in cfg.operations.get(Operation.SEARCH, [])}
    assert "_opted_in_searcher" in top_search
    assert "_opted_out_searcher" not in top_search


def test_asset_routing_validator_augments_write_index_only():
    """AssetRoutingConfig validator augments WRITE with a secondary index
    but NOT SEARCH — assets aren't search-addressable the way collection
    items are."""
    from typing import ClassVar
    from unittest.mock import patch

    from dynastore.modules.storage.routing_config import (
        AssetRoutingConfig,
        secondary_index_entries,
    )

    class _AssetES:
        is_asset_indexer: ClassVar[bool] = True
        auto_register_for_routing: ClassVar = frozenset({Operation.WRITE})

    with patch("dynastore.tools.discovery.get_protocols",
               lambda proto: [_AssetES()]):
        cfg = AssetRoutingConfig()

    index_ids = {e.driver_ref for e in secondary_index_entries(cfg.operations)}
    assert "_asset_es" in index_ids
    assert Operation.SEARCH not in cfg.operations or cfg.operations[Operation.SEARCH] == []


def test_validator_failure_in_discovery_does_not_break_construction():
    """If `get_protocols` raises (e.g. discovery not ready during test
    fixture loading), the validator must not propagate — a debug log is
    enough; the apply-handler path is the safety net."""
    from unittest.mock import patch

    def _boom(proto):
        raise RuntimeError("discovery not ready")

    with patch("dynastore.tools.discovery.get_protocols", _boom):
        cfg = CatalogRoutingConfig()  # must not raise

    # Default WRITE/READ unaffected — the registered CatalogStore is the
    # ``catalog_postgresql_driver`` composition wrapper (#732).
    write_ids = {e.driver_ref for e in cfg.operations[Operation.WRITE]}
    assert write_ids == {"catalog_postgresql_driver"}
    # Discovery augmentation was skipped, and the ES secondary-index hop is no
    # longer hard-coded (#1069 / #1073) — so there is no ES secondary index;
    # SEARCH is discovery-only, so it stays absent.
    from dynastore.modules.storage.routing_config import secondary_index_entries
    index_ids = {e.driver_ref for e in secondary_index_entries(cfg.operations)}
    assert "catalog_elasticsearch_driver" not in index_ids
    assert Operation.SEARCH not in cfg.operations or cfg.operations[Operation.SEARCH] == []


# ---------------------------------------------------------------------------
# Apply-handler parity for SEARCH
# ---------------------------------------------------------------------------


# Imports needed for the source-provenance test block below.
from dynastore.modules.storage.routing_config import (  # noqa: E402
    _self_register_store_drivers,
)


def test_default_entry_source_is_operator():
    """An entry constructed without ``source`` defaults to ``operator`` —
    the assumption is that any explicit construction is operator-driven
    unless an auto helper marks it otherwise."""
    e = OperationDriverEntry(driver_ref="X")
    assert e.source == "operator"


def test_indexer_helper_marks_entries_as_auto():
    """Entries created by `_self_register_indexers_into` carry
    `source="auto"` so operators can distinguish them in the API
    response."""
    from typing import ClassVar
    from unittest.mock import patch

    from dynastore.models.protocols.indexer import CollectionIndexer
    from dynastore.modules.storage.routing_config import (
        _self_register_indexers_into,
    )

    class _ColES:
        is_collection_indexer: ClassVar[bool] = True
        auto_register_for_routing: ClassVar = frozenset({Operation.WRITE})

    target_ops: dict = {}
    with patch("dynastore.tools.discovery.get_protocols",
               lambda proto: [_ColES()]):
        _self_register_indexers_into(target_ops, CollectionIndexer)

    assert len(target_ops[Operation.WRITE]) == 1
    assert target_ops[Operation.WRITE][0].source == "auto"


def test_searcher_helper_marks_entries_as_auto():
    """Entries created by `_self_register_searchers_into` also carry
    `source="auto"`."""
    from typing import ClassVar
    from unittest.mock import patch

    from dynastore.models.protocols.entity_store import CatalogStore
    from dynastore.modules.storage.routing_config import (
        _self_register_searchers_into,
    )

    class _ESCat:
        auto_register_for_routing: ClassVar = frozenset({Operation.SEARCH})

    target_ops: dict = {}
    with patch("dynastore.tools.discovery.get_protocols",
               lambda proto: [_ESCat()]):
        _self_register_searchers_into(target_ops, CatalogStore)

    assert len(target_ops[Operation.SEARCH]) == 1
    assert target_ops[Operation.SEARCH][0].source == "auto"


def test_store_driver_helper_marks_entries_as_auto():
    """`_self_register_store_drivers` also marks new entries as auto."""
    cfg = CollectionRoutingConfig()
    cfg.operations.clear()

    metadata_index = {"pg_core_meta": _FakeStore()}
    _self_register_store_drivers(cfg, metadata_index)

    for op in (Operation.WRITE, Operation.READ):
        entries = cfg.operations[op]
        assert len(entries) == 1
        assert entries[0].source == "auto"


def test_operator_managed_list_locks_out_auto_augment():
    """Option A (#792 / #889): a single operator-source entry locks the
    whole operation's list.  Discoverable drivers do NOT get auto-appended
    onto the side of an operator-managed entry — that semantic gave rise
    to #792 where deleted SEARCH/secondary-index drivers came back on every
    read.

    Inverse shape pin (do not collapse to a single assertion): the test
    constructs the SAME mixed-discovery surface the old behaviour relied
    on, then asserts only the operator entry survives — guarding against
    a future "simplify" patch that drops the operator-managed gate.
    """
    from typing import ClassVar
    from unittest.mock import patch

    from dynastore.models.protocols.indexer import CollectionIndexer
    from dynastore.modules.storage.routing_config import (
        _self_register_indexers_into,
    )

    class _AutoDriver:
        is_collection_indexer: ClassVar[bool] = True
        auto_register_for_routing: ClassVar = frozenset({Operation.WRITE})

    class _OpDriver:
        is_collection_indexer: ClassVar[bool] = True
        auto_register_for_routing: ClassVar = frozenset({Operation.WRITE})

    operator_entry = OperationDriverEntry(
        driver_ref="_op_driver", on_failure=FailurePolicy.FATAL,
    )
    target_ops: dict = {Operation.WRITE: [operator_entry]}

    with patch("dynastore.tools.discovery.get_protocols",
               lambda proto: [_AutoDriver(), _OpDriver()]):
        _self_register_indexers_into(target_ops, CollectionIndexer)

    by_id = {e.driver_ref: e for e in target_ops[Operation.WRITE]}
    assert set(by_id) == {"_op_driver"}
    assert by_id["_op_driver"].source == "operator"
    assert by_id["_op_driver"].on_failure == FailurePolicy.FATAL


def test_source_field_serialises_in_model_dump():
    """The new field appears in `model_dump()` output so it surfaces in
    the configs API response without any endpoint-side changes."""
    e_op = OperationDriverEntry(driver_ref="X")
    e_auto = OperationDriverEntry(driver_ref="Y", source="auto")
    assert e_op.model_dump()["source"] == "operator"
    assert e_auto.model_dump()["source"] == "auto"


def test_source_field_round_trips_via_model_validate():
    """Persisted JSONB rows that include `source` deserialise correctly.
    Rows that DON'T include it (older persisted data) get the default
    `operator` — backwards-compatible."""
    e_new = OperationDriverEntry.model_validate({"driver_ref": "X", "source": "auto"})
    assert e_new.source == "auto"
    e_legacy = OperationDriverEntry.model_validate({"driver_ref": "X"})
    assert e_legacy.source == "operator"


def test_source_field_rejects_invalid_value():
    """Literal[\"operator\", \"auto\"] is enforced — typos fail validation."""
    import pytest as _pytest
    from pydantic import ValidationError

    with _pytest.raises(ValidationError):
        OperationDriverEntry(driver_ref="X", source="bogus")  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# Apply-handler parity for SEARCH (existing test below)
# ---------------------------------------------------------------------------


def test_validate_handlers_invoke_searcher_self_registration():
    """The items-tier, collection-tier and catalog-tier validate handlers MUST
    each call `_self_register_searchers_into` (asset tier doesn't, by design —
    no SEARCH op for assets).  Self-registration moved apply→validate in
    #738/#747."""
    import asyncio
    from unittest.mock import patch

    from dynastore.modules.storage.routing_config import (
        AssetRoutingConfig,
        _validate_asset_routing_config,
        _validate_catalog_routing_config,
        _validate_collection_routing_config,
        _validate_items_routing_config,
    )

    calls: list[type] = []

    def _spy(target_ops, marker_proto, **_kwargs):
        # Accept arbitrary kwargs (e.g. ``search_caps`` on the searcher
        # helper) so the spy works for both helper signatures.
        calls.append(marker_proto)

    items = ItemsRoutingConfig()
    items.operations.clear()
    coll = CollectionRoutingConfig()
    coll.operations.clear()
    asset = AssetRoutingConfig()
    asset.operations.clear()
    cat = CatalogRoutingConfig()
    cat.operations.clear()

    with patch(
        "dynastore.modules.storage.routing_config._self_register_searchers_into",
        _spy,
    ), patch(
        "dynastore.tools.discovery.get_protocols",
        lambda proto: [],
    ):
        asyncio.run(_validate_items_routing_config(
            items, catalog_id=None, collection_id=None, db_resource=None,
        ))
        asyncio.run(_validate_collection_routing_config(
            coll, catalog_id=None, collection_id=None, db_resource=None,
        ))
        asyncio.run(_validate_asset_routing_config(
            asset, catalog_id=None, collection_id=None, db_resource=None,
        ))
        asyncio.run(_validate_catalog_routing_config(
            cat, catalog_id=None, collection_id=None, db_resource=None,
        ))

    from dynastore.models.protocols.entity_store import (
        CatalogStore,
        CollectionStore,
    )
    from dynastore.models.protocols.storage_driver import CollectionItemsStore
    # Each tier's validate handler invokes searcher registration once with its
    # own marker Protocol. Order matches invocation order above.
    assert calls == [
        CollectionItemsStore,
        CollectionStore,
        CatalogStore,
    ]


# ---------------------------------------------------------------------------
# Transformer self-registration parity (sister to indexer / searcher helpers)
# ---------------------------------------------------------------------------


def test_transformer_helper_picks_up_entity_transform_protocol_implementers():
    """Any registered EntityTransformProtocol implementer lands in the
    ``transformers`` registry keyed by class name (matching indexer/searcher
    convention)."""
    from unittest.mock import patch

    from dynastore.modules.storage.routing_config import (
        _self_register_transformers_into,
    )

    class TransformerOne:
        async def transform_for_index(self, entity, **_): return entity
        async def restore_from_index(self, doc, **_): return doc

    class TransformerTwo:
        async def transform_for_index(self, entity, **_): return entity
        async def restore_from_index(self, doc, **_): return doc

    registry: list = []
    fake_pool = [TransformerOne(), TransformerTwo()]
    with patch("dynastore.tools.discovery.get_protocols",
               lambda proto: fake_pool):
        _self_register_transformers_into(registry)

    ids = {e.driver_ref for e in registry}
    assert ids == {"transformer_one", "transformer_two"}
    assert all(e.source == "auto" for e in registry)


def test_transformer_helper_idempotent_and_preserves_operator_entry():
    """An operator-authored registry is invariant under auto-augmentation."""
    from unittest.mock import patch

    from dynastore.modules.storage.routing_config import (
        _self_register_transformers_into,
        TransformerEntry,
    )

    class CustomTransformer:
        async def transform_for_index(self, entity, **_): return entity
        async def restore_from_index(self, doc, **_): return doc

    op_entry = TransformerEntry(driver_ref="custom_transformer", source="operator")
    registry: list = [op_entry]

    with patch("dynastore.tools.discovery.get_protocols",
               lambda proto: [CustomTransformer()]):
        _self_register_transformers_into(registry)
        _self_register_transformers_into(registry)

    assert len(registry) == 1
    assert registry[0].driver_ref == "custom_transformer"
    assert registry[0].source == "operator"


def test_transformer_helper_no_op_when_no_implementers():
    """Empty discovery → the registry stays empty."""
    from unittest.mock import patch

    from dynastore.modules.storage.routing_config import (
        _self_register_transformers_into,
    )

    registry: list = []
    with patch("dynastore.tools.discovery.get_protocols",
               lambda proto: []):
        _self_register_transformers_into(registry)
    assert registry == []


# ---------------------------------------------------------------------------
# Option A regression suite (#792 / #889) — list-level operator override
# ---------------------------------------------------------------------------


def test_option_a_is_operator_managed_predicate_basic():
    """``_is_operator_managed`` returns True iff any entry in ``operations[op]``
    has ``source='operator'``.  Foundation for the list-level lock."""
    from dynastore.modules.storage.routing_config import _is_operator_managed

    op_entry = OperationDriverEntry(driver_ref="x", source="operator")
    au_entry = OperationDriverEntry(driver_ref="y", source="auto")

    assert _is_operator_managed({Operation.SEARCH: [op_entry]}, Operation.SEARCH)
    assert not _is_operator_managed({Operation.SEARCH: [au_entry]}, Operation.SEARCH)
    assert _is_operator_managed({Operation.SEARCH: [au_entry, op_entry]}, Operation.SEARCH)
    assert not _is_operator_managed({Operation.SEARCH: []}, Operation.SEARCH)
    assert not _is_operator_managed({}, Operation.SEARCH)


def test_option_a_indexer_helper_skips_operator_managed_list():
    """Indexer self-register helper is a no-op for an operation whose
    list contains any ``source='operator'`` entry (#792 / #889 — the
    deletion-comes-back symptom)."""
    from typing import ClassVar
    from unittest.mock import patch

    from dynastore.models.protocols.indexer import CollectionIndexer
    from dynastore.modules.storage.routing_config import (
        _self_register_indexers_into,
    )

    class _NewIndexer:
        is_collection_indexer: ClassVar[bool] = True
        auto_register_for_routing: ClassVar = frozenset({Operation.WRITE})

    target_ops: dict = {
        Operation.WRITE: [
            OperationDriverEntry(
                driver_ref="some_other_driver", source="operator",
            ),
        ],
    }
    with patch("dynastore.tools.discovery.get_protocols",
               lambda proto: [_NewIndexer()]):
        _self_register_indexers_into(target_ops, CollectionIndexer)

    refs = {e.driver_ref for e in target_ops[Operation.WRITE]}
    assert refs == {"some_other_driver"}


def test_option_a_searcher_helper_skips_operator_managed_list():
    """Searcher self-register helper: same lock-out shape."""
    from typing import ClassVar
    from unittest.mock import patch

    from dynastore.models.protocols.entity_store import CatalogStore
    from dynastore.modules.storage.routing_config import (
        _self_register_searchers_into,
    )

    class _NewSearcher:
        auto_register_for_routing: ClassVar = frozenset({Operation.SEARCH})

    target_ops: dict = {
        Operation.SEARCH: [
            OperationDriverEntry(driver_ref="es_cat", source="operator"),
        ],
    }
    with patch("dynastore.tools.discovery.get_protocols",
               lambda proto: [_NewSearcher()]):
        _self_register_searchers_into(target_ops, CatalogStore)

    refs = {e.driver_ref for e in target_ops[Operation.SEARCH]}
    assert refs == {"es_cat"}


def test_option_a_store_drivers_helper_locks_per_operation():
    """``_self_register_store_drivers`` iterates op_keys; the operator lock
    must apply per-operation independently (so an operator-managed WRITE
    list locks WRITE without blocking auto-augment on READ)."""
    cfg = CollectionRoutingConfig()
    cfg.operations.clear()
    cfg.operations[Operation.WRITE] = [
        OperationDriverEntry(driver_ref="pg_core_meta", source="operator"),
    ]
    cfg.operations[Operation.READ] = []  # empty → auto-augmentable

    metadata_index = {"pg_core_meta": _FakeStore(), "pg_stac_meta": _FakeStore()}
    _self_register_store_drivers(cfg, metadata_index)

    write_refs = {e.driver_ref for e in cfg.operations[Operation.WRITE]}
    read_refs = {e.driver_ref for e in cfg.operations[Operation.READ]}
    # WRITE locked: pg_stac_meta NOT appended.
    assert write_refs == {"pg_core_meta"}
    # READ free: both auto-appended with source=auto.
    assert read_refs == {"pg_core_meta", "pg_stac_meta"}
    for entry in cfg.operations[Operation.READ]:
        assert entry.source == "auto"


def test_option_a_upload_helper_skips_operator_managed_list():
    """Upload self-register helper: same lock-out shape."""
    from typing import ClassVar
    from unittest.mock import patch

    from dynastore.models.protocols.asset_upload import AssetUploadProtocol
    from dynastore.modules.storage.routing_config import (
        _self_register_upload_into,
    )

    class _NewUploader:
        auto_register_for_routing: ClassVar = frozenset({Operation.UPLOAD})

    target_ops: dict = {
        Operation.UPLOAD: [
            OperationDriverEntry(driver_ref="gcs_upload", source="operator"),
        ],
    }
    with patch("dynastore.tools.discovery.get_protocols",
               lambda proto: [_NewUploader()]):
        _self_register_upload_into(target_ops, AssetUploadProtocol)

    refs = {e.driver_ref for e in target_ops[Operation.UPLOAD]}
    assert refs == {"gcs_upload"}


def test_option_a_transformer_helper_skips_operator_managed_list():
    """Transformer self-register helper: same lock-out shape."""
    from unittest.mock import patch

    from dynastore.modules.storage.routing_config import (
        _self_register_transformers_into,
        TransformerEntry,
    )

    class _NewTransformer:
        async def transform_for_index(self, entity, **_): return entity
        async def restore_from_index(self, doc, **_): return doc

    registry: list = [
        TransformerEntry(driver_ref="pinned_tf", source="operator"),
    ]
    with patch("dynastore.tools.discovery.get_protocols",
               lambda proto: [_NewTransformer()]):
        _self_register_transformers_into(registry)

    refs = {e.driver_ref for e in registry}
    assert refs == {"pinned_tf"}


def test_option_a_default_factory_entries_are_auto_sourced():
    """Default-factory operations entries must carry ``source='auto'``
    so a fresh boot stays augmentable under Option A.  An old default of
    ``source='operator'`` would lock auto-registration out at first read
    — the #792 deletion semantic must NOT apply to boot defaults."""
    from dynastore.modules.storage.routing_config import AssetRoutingConfig

    cfg_items = ItemsRoutingConfig.model_construct(
        operations=ItemsRoutingConfig.model_fields["operations"].default_factory(),
    )
    cfg_coll = CollectionRoutingConfig.model_construct(
        operations=CollectionRoutingConfig.model_fields["operations"].default_factory(),
    )
    cfg_asset = AssetRoutingConfig.model_construct(
        operations=AssetRoutingConfig.model_fields["operations"].default_factory(),
    )
    cfg_cat = CatalogRoutingConfig.model_construct(
        operations=CatalogRoutingConfig.model_fields["operations"].default_factory(),
    )
    for label, cfg in (
        ("ItemsRoutingConfig", cfg_items),
        ("CollectionRoutingConfig", cfg_coll),
        ("AssetRoutingConfig", cfg_asset),
        ("CatalogRoutingConfig", cfg_cat),
    ):
        for op, entries in cfg.operations.items():
            for entry in entries:
                assert entry.source == "auto", (
                    f"{label}.operations[{op}] entry '{entry.driver_ref}' "
                    f"has source={entry.source!r} — must be 'auto' for "
                    f"Option A boot-time augmentation to work"
                )


def test_option_a_fresh_construct_still_auto_augments():
    """End-to-end: constructing a config with no operator overrides
    still picks up discoverable drivers — boot defaults (source='auto')
    do not lock the helper."""
    from typing import ClassVar
    from unittest.mock import patch

    class _DiscoverableIndexer:
        is_collection_indexer: ClassVar[bool] = True
        auto_register_for_routing: ClassVar = frozenset({Operation.WRITE})

    with patch("dynastore.tools.discovery.get_protocols",
               lambda proto: [_DiscoverableIndexer()]):
        cfg = CollectionRoutingConfig()

    from dynastore.modules.storage.routing_config import secondary_index_entries
    index_refs = {e.driver_ref for e in secondary_index_entries(cfg.operations)}
    assert "_discoverable_indexer" in index_refs


def _items_pg_only_body():
    """A GET→edit→PUT body: PG-only lists whose entries carry ``source='auto'``
    exactly as the configs API serialises persisted self-registered/boot
    entries back to the operator. This is the round-trip that the bug rode in
    on (#792/#889)."""
    return {
        "operations": {
            Operation.WRITE: [
                {"driver_ref": "items_postgresql_driver", "source": "auto",
                 "on_failure": "fatal", "write_mode": "sync"},
            ],
            Operation.READ: [
                {"driver_ref": "items_postgresql_driver", "source": "auto"},
            ],
            Operation.SEARCH: [
                {"driver_ref": "items_postgresql_driver", "source": "auto"},
            ],
        },
    }


def test_external_write_context_stamps_operator_provenance():
    """The configs-API deserialisation boundary passes
    ``context={'dynastore_external_write': True}`` to ``model_validate``; the
    routing validator then stamps ``source='operator'`` on every operation list
    the operator explicitly sent — the API-boundary half of Option A
    (#792/#889) that engages ``_is_operator_managed``.
    """
    cfg = ItemsRoutingConfig.model_validate(
        _items_pg_only_body(),
        context={"dynastore_external_write": True},
    )

    assert all(
        e.source == "operator"
        for entries in cfg.operations.values()
        for e in entries
    )


def test_external_write_context_blocks_driver_reinjection_on_removal():
    """End-to-end of the reported bug through the REAL deserialisation path.

    With a discoverable ES indexer/searcher registered, an operator round-trips
    a PG-only routing config (entries carry ``source='auto'`` from the GET):

    * WITHOUT the external-write context the validator's self-register pass
      re-appends ES — this is the bug, and the assertion proves the fixture
      genuinely reproduces it.
    * WITH the context the validator stamps the lists operator-authored BEFORE
      self-registration, so ES is NOT re-appended — the operator's deletion
      sticks.
    """
    from typing import ClassVar
    from unittest.mock import patch

    class _ItemsES:
        is_item_indexer: ClassVar[bool] = True
        auto_register_for_routing: ClassVar = frozenset(
            {Operation.WRITE, Operation.SEARCH}
        )

    # Bug path: no external-write context -> ES is re-appended.
    with patch("dynastore.tools.discovery.get_protocols",
               lambda proto: [_ItemsES()]):
        bug = ItemsRoutingConfig.model_validate(_items_pg_only_body())
    bug_write = {e.driver_ref for e in bug.operations[Operation.WRITE]}
    assert "_items_es" in bug_write, (
        "fixture must reproduce the re-injection without the context flag"
    )

    # Fix path: external-write context -> ES stays removed.
    with patch("dynastore.tools.discovery.get_protocols",
               lambda proto: [_ItemsES()]):
        fixed = ItemsRoutingConfig.model_validate(
            _items_pg_only_body(),
            context={"dynastore_external_write": True},
        )
    fixed_write = {e.driver_ref for e in fixed.operations[Operation.WRITE]}
    fixed_search = {e.driver_ref for e in fixed.operations[Operation.SEARCH]}
    assert fixed_write == {"items_postgresql_driver"}
    assert fixed_search == {"items_postgresql_driver"}


def test_internal_construct_without_context_still_auto_augments():
    """No external-write context (internal DB-load / boot-default construction)
    => discoverable drivers still auto-register. Guards against the stamp
    over-reaching and freezing internal augmentation."""
    from typing import ClassVar
    from unittest.mock import patch

    from dynastore.modules.storage.routing_config import secondary_index_entries

    class _ItemsES:
        is_item_indexer: ClassVar[bool] = True
        auto_register_for_routing: ClassVar = frozenset(
            {Operation.WRITE, Operation.SEARCH}
        )

    with patch("dynastore.tools.discovery.get_protocols",
               lambda proto: [_ItemsES()]):
        cfg = ItemsRoutingConfig.model_validate(_items_pg_only_body())

    index_refs = {e.driver_ref for e in secondary_index_entries(cfg.operations)}
    assert "_items_es" in index_refs


def test_is_external_operator_write_reads_context():
    """The context predicate is True only for the explicit external-write flag
    and False for missing/empty/None context (internal paths)."""
    from types import SimpleNamespace

    from typing import Any, cast

    from dynastore.modules.storage.routing_config import _is_external_operator_write

    def _info(ctx):
        # Duck-typed stand-in for ValidationInfo (only .context is read).
        return cast(Any, SimpleNamespace(context=ctx))

    assert _is_external_operator_write(_info({"dynastore_external_write": True}))
    assert not _is_external_operator_write(_info(None))
    assert not _is_external_operator_write(_info({}))
    assert not _is_external_operator_write(_info({"dynastore_external_write": False}))


def test_operations_field_is_mutable_so_operators_can_edit_driver_list():
    """The ``operations`` field must be Mutable, not Immutable: an operator
    must be able to change the driver mapping (e.g. remove ES) even after the
    tier is materialized. Pins the #792/#889 follow-up that an Immutable
    ``operations`` made a genuine driver-list change 409 once any catalog
    existed."""
    from dynastore.models.mutability import is_immutable_field
    from dynastore.modules.storage.routing_config import (
        AssetRoutingConfig,
        CatalogRoutingConfig,
    )

    for cls in (
        ItemsRoutingConfig,
        CollectionRoutingConfig,
        AssetRoutingConfig,
        CatalogRoutingConfig,
    ):
        field_info = cls.model_fields["operations"]
        assert not is_immutable_field(field_info), (
            f"{cls.__name__}.operations must be Mutable so operators can edit "
            f"the driver list post-materialization"
        )


# ---------------------------------------------------------------------------
# #1865 — scope operator-provenance lock to changed operation lists only
# ---------------------------------------------------------------------------


def test_1865_compute_changed_op_keys_detects_changes():
    """``_compute_changed_op_keys`` returns only the operation keys whose
    driver-ref sets differ from the stored config.  Order-insensitive."""
    from dynastore.modules.storage.routing_config import _compute_changed_op_keys

    incoming = {
        Operation.WRITE: [
            {"driver_ref": "pg_driver"},
        ],
        Operation.READ: [
            {"driver_ref": "pg_driver"},
        ],
        Operation.SEARCH: [
            {"driver_ref": "pg_driver"},
        ],
    }
    # Stored has WRITE with BOTH pg + es; READ and SEARCH identical to incoming.
    stored_raw = {
        "operations": {
            Operation.WRITE: [
                {"driver_ref": "pg_driver", "source": "auto"},
                {"driver_ref": "es_driver", "source": "auto"},
            ],
            Operation.READ: [{"driver_ref": "pg_driver", "source": "auto"}],
            Operation.SEARCH: [{"driver_ref": "pg_driver", "source": "auto"}],
        }
    }
    changed = _compute_changed_op_keys(incoming, stored_raw)
    # Only WRITE changed (es_driver removed).
    assert changed == {Operation.WRITE}


def test_1865_compute_changed_op_keys_returns_none_on_create():
    """When there is no stored config (create path), returns ``None`` so the
    validator stamps all present lists."""
    from dynastore.modules.storage.routing_config import _compute_changed_op_keys

    incoming = {
        Operation.WRITE: [{"driver_ref": "pg_driver"}],
    }
    assert _compute_changed_op_keys(incoming, None) is None


def test_1865_compute_changed_op_keys_new_op_key_is_changed():
    """An operation key present in incoming but absent in stored is 'changed'."""
    from dynastore.modules.storage.routing_config import _compute_changed_op_keys

    incoming = {
        Operation.WRITE: [{"driver_ref": "pg_driver"}],
        Operation.SEARCH: [{"driver_ref": "es_driver"}],  # new — absent in stored
    }
    stored_raw = {
        "operations": {
            Operation.WRITE: [{"driver_ref": "pg_driver", "source": "auto"}],
        }
    }
    changed = _compute_changed_op_keys(incoming, stored_raw)
    assert Operation.SEARCH in changed
    assert Operation.WRITE not in changed


def test_1865_stamp_scoped_to_changed_ops_only():
    """``_stamp_operator_provenance`` with ``changed_op_keys={WRITE}`` stamps
    WRITE entries but leaves READ/SEARCH entries with their existing source."""
    cfg = ItemsRoutingConfig.model_construct(
        operations={
            Operation.WRITE: [
                OperationDriverEntry(driver_ref="pg", source="auto"),
            ],
            Operation.READ: [
                OperationDriverEntry(driver_ref="pg", source="auto"),
            ],
            Operation.SEARCH: [
                OperationDriverEntry(driver_ref="pg", source="auto"),
            ],
        }
    )
    cfg._stamp_operator_provenance(changed_op_keys={Operation.WRITE})

    assert cfg.operations[Operation.WRITE][0].source == "operator"
    assert cfg.operations[Operation.READ][0].source == "auto"
    assert cfg.operations[Operation.SEARCH][0].source == "auto"


def test_1865_stamp_with_none_changed_keys_stamps_all():
    """``_stamp_operator_provenance(None)`` stamps all operations (create path /
    legacy behaviour)."""
    cfg = ItemsRoutingConfig.model_construct(
        operations={
            Operation.WRITE: [OperationDriverEntry(driver_ref="pg", source="auto")],
            Operation.READ: [OperationDriverEntry(driver_ref="pg", source="auto")],
        }
    )
    cfg._stamp_operator_provenance(changed_op_keys=None)

    assert cfg.operations[Operation.WRITE][0].source == "operator"
    assert cfg.operations[Operation.READ][0].source == "operator"


def test_1865_unchanged_op_allows_driver_auto_augmentation():
    """End-to-end: operator PUT changes only WRITE (removes ES); READ and
    SEARCH are unchanged.  After validation with the scoped context:

    * WRITE is locked — ES stays removed.
    * READ and SEARCH remain auto-managed — a newly discovered ES driver
      would be auto-appended to those lists on the next write.

    This is the core semantic of #1865: scope the provenance lock to the
    lists the operator actually changed.
    """
    from typing import ClassVar
    from unittest.mock import patch

    class _ItemsES:
        is_item_indexer: ClassVar[bool] = True
        auto_register_for_routing: ClassVar = frozenset(
            {Operation.WRITE, Operation.SEARCH}
        )

    # The stored config has WRITE=[pg, es], READ=[pg], SEARCH=[pg, es].
    # The operator sends WRITE=[pg] (removes es), READ=[pg], SEARCH=[pg, es].
    # Only WRITE changed.
    incoming_body = {
        "operations": {
            Operation.WRITE: [
                {"driver_ref": "items_postgresql_driver", "source": "auto",
                 "on_failure": "fatal", "write_mode": "sync"},
            ],
            Operation.READ: [
                {"driver_ref": "items_postgresql_driver", "source": "auto"},
            ],
            Operation.SEARCH: [
                {"driver_ref": "items_postgresql_driver", "source": "auto"},
                {"driver_ref": "_items_es", "source": "auto"},
            ],
        },
    }
    # changed_op_keys = {WRITE} (only that list differs from stored)
    with patch("dynastore.tools.discovery.get_protocols",
               lambda proto: [_ItemsES()]):
        cfg = ItemsRoutingConfig.model_validate(
            incoming_body,
            context={
                "dynastore_external_write": True,
                "dynastore_changed_operation_keys": {Operation.WRITE},
            },
        )

    write_refs = {e.driver_ref for e in cfg.operations[Operation.WRITE]}
    read_refs = {e.driver_ref for e in cfg.operations[Operation.READ]}
    search_refs = {e.driver_ref for e in cfg.operations[Operation.SEARCH]}

    # WRITE locked: ES removed, stays removed even though ES is discoverable.
    assert "_items_es" not in write_refs
    assert "items_postgresql_driver" in write_refs
    # WRITE entries have source=operator (locked).
    assert all(
        e.source == "operator"
        for e in cfg.operations[Operation.WRITE]
    )
    # READ: still auto-managed — the validator did not stamp operator on READ.
    # The existing [pg] auto entry survives; no additional es entry (discovery
    # did not add one since READ was already present).
    assert "items_postgresql_driver" in read_refs
    assert all(
        e.source == "auto"
        for e in cfg.operations[Operation.READ]
    )
    # SEARCH: operator sent [pg, es] and it was NOT in changed_op_keys, so
    # entries stay auto-sourced and are not locked.
    assert "items_postgresql_driver" in search_refs
    assert "_items_es" in search_refs
    assert all(
        e.source == "auto"
        for e in cfg.operations[Operation.SEARCH]
    )


def test_1865_write_lock_preserved_when_read_search_open():
    """Guard: the #1863 guarantee is intact — when WRITE is in changed_op_keys
    and the operator removes a driver from WRITE, that driver does NOT come
    back even though READ/SEARCH are left open."""
    from typing import ClassVar
    from unittest.mock import patch

    class _ItemsES:
        is_item_indexer: ClassVar[bool] = True
        auto_register_for_routing: ClassVar = frozenset(
            {Operation.WRITE, Operation.SEARCH}
        )

    pg_only_write_body = {
        "operations": {
            Operation.WRITE: [
                {"driver_ref": "items_postgresql_driver", "source": "auto",
                 "on_failure": "fatal", "write_mode": "sync"},
            ],
            Operation.READ: [
                {"driver_ref": "items_postgresql_driver", "source": "auto"},
            ],
        },
    }
    with patch("dynastore.tools.discovery.get_protocols",
               lambda proto: [_ItemsES()]):
        cfg = ItemsRoutingConfig.model_validate(
            pg_only_write_body,
            context={
                "dynastore_external_write": True,
                "dynastore_changed_operation_keys": {Operation.WRITE},
            },
        )

    write_refs = {e.driver_ref for e in cfg.operations[Operation.WRITE]}
    assert "_items_es" not in write_refs, (
        "#1863 regression: removed ES driver re-appeared in WRITE despite lock"
    )


def test_option_a_792_reproducer_search_index_lock():
    """End-to-end reproducer for #792: operator PUTs an explicit SEARCH
    list (single entry, source='operator'), then a downstream code path
    runs the search self-register helper — the missing driver MUST stay
    missing (the symptom on #792 was the missing driver re-appearing).
    """
    from typing import ClassVar
    from unittest.mock import patch

    from dynastore.models.protocols.storage_driver import (
        CollectionItemsStore,
    )
    from dynastore.modules.storage.routing_config import (
        _self_register_searchers_into,
    )

    # Driver pool: ES is operator-pinned; PG offers SEARCH opt-in but is
    # explicitly omitted from the operator's list.
    class _ItemsES:
        is_item_indexer: ClassVar[bool] = False
        auto_register_for_routing: ClassVar = frozenset({Operation.SEARCH})

    class _ItemsPG:
        is_item_indexer: ClassVar[bool] = False
        auto_register_for_routing: ClassVar = frozenset({Operation.SEARCH})

    target_ops: dict = {
        Operation.SEARCH: [
            OperationDriverEntry(
                driver_ref="items_elasticsearch_driver",
                source="operator",
            ),
        ],
    }
    with patch("dynastore.tools.discovery.get_protocols",
               lambda proto: [_ItemsES(), _ItemsPG()]):
        _self_register_searchers_into(target_ops, CollectionItemsStore)

    refs = {e.driver_ref for e in target_ops[Operation.SEARCH]}
    # The exact #792 symptom — missing driver re-appearing — does not occur.
    assert refs == {"items_elasticsearch_driver"}


def test_validate_collection_routing_skips_secondary_index_write_entry():
    """A ``secondary_index=True`` WRITE entry whose driver_ref is not a
    registered ``CollectionStore`` must NOT fail collection routing validation.

    Regression for the ES-catalog routing preset apply: the default
    ``CollectionRoutingConfig`` self-registers the ES ``CollectionIndexer``
    into ``operations[WRITE]`` (``secondary_index=True``).  The ES driver is an
    indexer, not a ``CollectionStore``, so validating it against the store
    registry rejected it and rolled back the whole routing bundle, dropping an
    ES catalog to the PG+ES default.  Secondary-index sinks are skipped here;
    they are validated by their own discovery-driven self-registration and the
    runtime router skips any unregistered entry at dispatch.
    """
    import asyncio
    from unittest.mock import patch

    from dynastore.modules.storage.routing_config import (
        WriteMode,
        _validate_collection_routing_config,
    )

    cfg = CollectionRoutingConfig()
    cfg.operations.clear()
    cfg.operations[Operation.WRITE] = [
        OperationDriverEntry(
            driver_ref="collection_elasticsearch_driver",
            secondary_index=True,
            write_mode=WriteMode.ASYNC,
            on_failure=FailurePolicy.OUTBOX,
            source="auto",
        ),
    ]
    cfg.operations[Operation.READ] = []

    # No collection drivers registered → store registry is empty.  Pre-fix this
    # raised ValueError on the secondary-index ES entry; post-fix it is skipped.
    with patch("dynastore.tools.discovery.get_protocols", lambda proto: []):
        asyncio.run(_validate_collection_routing_config(
            cfg, catalog_id=None, collection_id=None, db_resource=None,
        ))

    write_refs = {e.driver_ref for e in cfg.operations[Operation.WRITE]}
    assert "collection_elasticsearch_driver" in write_refs


def test_validate_collection_routing_still_rejects_unknown_primary_store():
    """A non-secondary (primary) WRITE entry with an unregistered driver_ref
    still hard-fails — the secondary-index skip must not weaken typo
    protection for the primary metadata store.
    """
    import asyncio
    from unittest.mock import patch

    import pytest

    from dynastore.modules.storage.routing_config import (
        _validate_collection_routing_config,
    )

    cfg = CollectionRoutingConfig()
    cfg.operations.clear()
    cfg.operations[Operation.WRITE] = [
        OperationDriverEntry(driver_ref="collection_postgres_typo", source="auto"),
    ]
    cfg.operations[Operation.READ] = []

    with patch("dynastore.tools.discovery.get_protocols", lambda proto: []):
        with pytest.raises(ValueError, match="operations\\[WRITE\\] driver"):
            asyncio.run(_validate_collection_routing_config(
                cfg, catalog_id=None, collection_id=None, db_resource=None,
            ))


def test_validate_collection_routing_skips_stale_indexer_in_read():
    """A routing config persisted before the ES-only collection routing fix could
    list the ES ``collection_elasticsearch_driver`` as a READ *primary*
    (``secondary_index`` unset).  The ES driver is a ``CollectionIndexer``, never
    a READ-capable ``CollectionStore`` — validating it against the store registry
    hard-raised ``operations[READ] driver ... is not registered`` and blocked the
    catalog.  It must now be warn-skipped (the runtime router relaxes READ to an
    available store), so a stale catalog stays readable and self-heals on the
    next apply.  Parity with the operations[WRITE] secondary-index skip (#2267).
    """
    import asyncio
    from typing import ClassVar
    from unittest.mock import patch

    from dynastore.models.protocols.entity_store import CollectionStore
    from dynastore.models.protocols.indexer import CollectionIndexer
    from dynastore.modules.storage.routing_config import (
        _validate_collection_routing_config,
    )

    class CollectionPostgresqlDriver:  # the sole registered CollectionStore
        capabilities = frozenset({Capability.WRITE, Capability.READ})
        supported_hints: frozenset = frozenset()

    class CollectionElasticsearchDriver:  # a CollectionIndexer, NOT a store
        is_collection_indexer: ClassVar[bool] = True
        auto_register_for_routing: ClassVar = frozenset(
            {Operation.SEARCH, Operation.WRITE}
        )

    pg = CollectionPostgresqlDriver()
    es = CollectionElasticsearchDriver()

    def _fake_get_protocols(proto):
        if proto is CollectionStore:
            return [pg]
        if proto is CollectionIndexer:
            return [es]
        return []

    cfg = CollectionRoutingConfig()
    cfg.operations.clear()
    cfg.operations[Operation.READ] = [
        OperationDriverEntry(
            driver_ref="collection_postgresql_driver", source="auto",
        ),
        # Stale ES-as-READ-primary entry (secondary_index unset).
        OperationDriverEntry(
            driver_ref="collection_elasticsearch_driver", source="auto",
        ),
    ]
    cfg.operations[Operation.WRITE] = []

    # Pre-fix this raised ValueError on the ES READ entry; post-fix it is skipped.
    with patch("dynastore.tools.discovery.get_protocols", _fake_get_protocols):
        asyncio.run(_validate_collection_routing_config(
            cfg, catalog_id=None, collection_id=None, db_resource=None,
        ))

    read_refs = {e.driver_ref for e in cfg.operations[Operation.READ]}
    assert "collection_postgresql_driver" in read_refs


def test_validate_collection_routing_still_rejects_unknown_read_driver():
    """A READ entry whose driver_ref is neither a registered ``CollectionStore``
    nor a known ``CollectionIndexer`` (a genuine typo) still hard-fails — the
    indexer skip must not weaken typo protection on the READ side.
    """
    import asyncio
    from unittest.mock import patch

    import pytest

    from dynastore.modules.storage.routing_config import (
        _validate_collection_routing_config,
    )

    cfg = CollectionRoutingConfig()
    cfg.operations.clear()
    cfg.operations[Operation.READ] = [
        OperationDriverEntry(driver_ref="collection_postgres_typo", source="auto"),
    ]
    cfg.operations[Operation.WRITE] = []

    with patch("dynastore.tools.discovery.get_protocols", lambda proto: []):
        with pytest.raises(ValueError, match="operations\\[READ\\] driver"):
            asyncio.run(_validate_collection_routing_config(
                cfg, catalog_id=None, collection_id=None, db_resource=None,
            ))
