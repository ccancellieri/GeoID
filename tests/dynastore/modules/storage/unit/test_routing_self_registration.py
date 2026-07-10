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

Lane model (#2494): indexers self-register into ``operations[INDEX]``
(never a distinct SEARCH operation — search is derived from INDEX-then-READ
at query time).  ``_self_register_indexers_into`` gates on the WRITE lane's
operator-managed status (not INDEX's own), so an operator who has taken
explicit ownership of an entity's WRITE lane is treated as having taken
ownership of the whole routing config — INDEX auto-augmentation backs off
too.  This is what keeps every "PG-only" preset (which pins WRITE
explicitly) free of a silently-injected ES indexer without each preset
needing its own INDEX-lane lock.
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
# Per-tier indexer marker self-registration (INDEX lane, #2494)
# ---------------------------------------------------------------------------


def test_indexer_marker_lands_in_index_lane():
    """A driver claiming a tier in ``index_tiers`` auto-registers under
    ``operations[INDEX]`` — sourced from the tier value, not from
    generic capability discovery.  Lane membership IS the role: there is
    no per-entry write_mode/on_failure/secondary_index flag any more.
    """
    from typing import ClassVar, FrozenSet
    from unittest.mock import patch

    from dynastore.modules.storage.routing_config import (
        _self_register_indexers_into,
        index_entries,
    )

    class _CollectionES:
        index_tiers: ClassVar[FrozenSet[str]] = frozenset({"collection"})
        auto_register_for_routing: ClassVar = frozenset({Operation.INDEX})

    class _NotAnIndexer:
        pass

    target_ops: dict = {}
    fake_pool = [_CollectionES(), _NotAnIndexer()]

    def _fake_get_protocols(proto):
        return [d for d in fake_pool if isinstance(d, proto)]

    with patch("dynastore.tools.discovery.get_protocols", _fake_get_protocols):
        _self_register_indexers_into(target_ops, "collection")

    entries = index_entries(target_ops)
    assert len(entries) == 1
    assert entries[0].driver_ref == "_collection_es"
    assert entries[0].source == "auto"


def test_indexer_seeding_checked_by_value_not_by_marker_presence():
    """A driver claiming only the ``catalog`` tier is discovered when
    seeding items (structurally satisfies ``IndexTierDriver``), but is NOT
    seeded into the items INDEX lane — tier membership is checked BY VALUE,
    not by marker presence.  A multi-tier driver seeds into every tier it
    claims.  Pins the exact GOTCHA this design fixes: `get_protocols`
    isinstance checks only test that ``index_tiers`` exists, never its
    contents."""
    from typing import ClassVar, FrozenSet
    from unittest.mock import patch

    from dynastore.modules.storage.routing_config import (
        _self_register_indexers_into,
        index_entries,
    )

    class _CatalogOnlyES:
        index_tiers: ClassVar[FrozenSet[str]] = frozenset({"catalog"})
        auto_register_for_routing: ClassVar = frozenset({Operation.INDEX})

    class _MultiTierES:
        index_tiers: ClassVar[FrozenSet[str]] = frozenset({"catalog", "item"})
        auto_register_for_routing: ClassVar = frozenset({Operation.INDEX})

    fake_pool = [_CatalogOnlyES(), _MultiTierES()]

    def _fake_get_protocols(proto):
        return [d for d in fake_pool if isinstance(d, proto)]

    item_ops: dict = {}
    with patch("dynastore.tools.discovery.get_protocols", _fake_get_protocols):
        _self_register_indexers_into(item_ops, "item")

    item_refs = {e.driver_ref for e in index_entries(item_ops)}
    # The catalog-only driver structurally satisfies IndexTierDriver (it HAS
    # index_tiers) but must not be seeded for "item" — value, not presence.
    assert "_catalog_only_es" not in item_refs
    assert "_multi_tier_es" in item_refs

    catalog_ops: dict = {}
    with patch("dynastore.tools.discovery.get_protocols", _fake_get_protocols):
        _self_register_indexers_into(catalog_ops, "catalog")

    catalog_refs = {e.driver_ref for e in index_entries(catalog_ops)}
    assert catalog_refs == {"_catalog_only_es", "_multi_tier_es"}


def test_validate_handlers_invoke_indexer_self_registration():
    """Each routing-config validate handler MUST invoke
    ``_self_register_indexers_into`` against its own tier string.
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

    calls: list[str] = []

    def _spy(target_ops, tier, **_kwargs):
        calls.append(tier)

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

    # Each tier's validate handler invokes indexer registration with its own
    # tier string. Order matches the invocation order above.
    assert calls == ["item", "collection", "asset", "catalog"]


def test_end_to_end_marker_to_index_entry_via_real_validate_handler():
    """End-to-end: register a real driver claiming the ``catalog`` tier,
    invoke ``_validate_catalog_routing_config`` against a fresh
    ``CatalogRoutingConfig``, assert the driver lands in
    ``operations[INDEX]``.

    Validates the full chain: marker discovery → helper invocation →
    entry landing in the INDEX lane.  Self-registration moved
    apply→validate in #738/#747.
    """
    import asyncio
    from typing import ClassVar, FrozenSet

    from dynastore.models.protocols.indexer import IndexTierDriver
    from dynastore.modules.storage.routing_config import (
        _validate_catalog_routing_config,
        index_entries,
    )
    from dynastore.tools.discovery import register_plugin, unregister_plugin

    class _DummyCatalogIndexer:
        index_tiers: ClassVar[FrozenSet[str]] = frozenset({"catalog"})
        auto_register_for_routing: ClassVar = frozenset({Operation.INDEX})
        # Minimal CatalogStore surface — enough for
        # _validate_routing_entries to accept it under operations[WRITE]
        # if it were referenced (it isn't pre-apply; the tier self-
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

        entries = index_entries(cfg.operations)
        assert any(
            e.driver_ref == "_dummy_catalog_indexer" for e in entries
        ), f"_DummyCatalogIndexer not auto-registered: {entries!r}"
    finally:
        unregister_plugin(instance)
        # Sanity: ensure cleanup so other tests don't see this stub.
        from dynastore.tools.discovery import get_protocols
        assert not any(
            isinstance(d, IndexTierDriver)
            and "catalog" in d.index_tiers
            and type(d).__name__ == "_dummy_catalog_indexer"
            for d in get_protocols(IndexTierDriver)
        )


def test_indexer_helper_blocked_when_write_lane_is_operator_managed():
    """A discoverable indexer is NOT appended to INDEX when the entity's
    WRITE lane already carries an operator-source entry.

    ``_self_register_indexers_into`` gates on WRITE's operator-managed
    status (not INDEX's own) — an operator who has taken explicit
    ownership of WRITE is treated as having taken ownership of the whole
    routing config.  This is what keeps a "PG-only" preset (which pins
    WRITE explicitly, with no INDEX entry at all) free of a silently
    injected ES indexer.
    """
    from typing import ClassVar, FrozenSet
    from unittest.mock import patch

    from dynastore.models.protocols.indexer import IndexTierDriver
    from dynastore.modules.storage.routing_config import _self_register_indexers_into

    class _AssetES:
        index_tiers: ClassVar[FrozenSet[str]] = frozenset({"asset"})
        auto_register_for_routing: ClassVar = frozenset({Operation.INDEX})

    target_ops: dict = {
        Operation.WRITE: [
            OperationDriverEntry(
                driver_ref="asset_postgresql_driver",
                on_failure=FailurePolicy.FATAL,
                source="operator",
            ),
        ],
    }

    with patch("dynastore.tools.discovery.get_protocols",
               lambda proto: [_AssetES()] if proto is IndexTierDriver else []):
        _self_register_indexers_into(target_ops, "asset")

    # WRITE is operator-managed → INDEX auto-registration is blocked.
    assert target_ops.get(Operation.INDEX, []) == []


def test_indexer_helper_fires_when_write_lane_is_auto_sourced():
    """Inverse of the above: a WRITE lane with only ``source="auto"``
    entries (boot defaults) does not block INDEX auto-registration."""
    from typing import ClassVar, FrozenSet
    from unittest.mock import patch

    from dynastore.models.protocols.indexer import IndexTierDriver
    from dynastore.modules.storage.routing_config import (
        _self_register_indexers_into,
        index_entries,
    )

    class _AssetES:
        index_tiers: ClassVar[FrozenSet[str]] = frozenset({"asset"})
        auto_register_for_routing: ClassVar = frozenset({Operation.INDEX})

    target_ops: dict = {
        Operation.WRITE: [
            OperationDriverEntry(
                driver_ref="asset_postgresql_driver",
                on_failure=FailurePolicy.FATAL,
                source="auto",
            ),
        ],
    }

    with patch("dynastore.tools.discovery.get_protocols",
               lambda proto: [_AssetES()] if proto is IndexTierDriver else []):
        _self_register_indexers_into(target_ops, "asset")

    refs = {e.driver_ref for e in index_entries(target_ops)}
    assert refs == {"_asset_es"}


def test_indexer_marker_skips_already_listed_driver():
    """An indexer already present in operations[INDEX] is not duplicated;
    only missing drivers get appended."""
    from typing import ClassVar, FrozenSet
    from unittest.mock import patch

    from dynastore.models.protocols.indexer import IndexTierDriver
    from dynastore.modules.storage.routing_config import _self_register_indexers_into

    class _AssetES:
        index_tiers: ClassVar[FrozenSet[str]] = frozenset({"asset"})
        auto_register_for_routing: ClassVar = frozenset({Operation.INDEX})

    operator_entry = OperationDriverEntry(driver_ref="_asset_es", source="operator")
    target_ops: dict = {Operation.INDEX: [operator_entry]}

    with patch("dynastore.tools.discovery.get_protocols",
               lambda proto: [_AssetES()] if proto is IndexTierDriver else []):
        _self_register_indexers_into(target_ops, "asset")

    # No duplicate; operator-supplied entry preserved as-is.
    assert len(target_ops[Operation.INDEX]) == 1
    assert target_ops[Operation.INDEX][0].source == "operator"


def test_indexer_helper_blocked_when_index_lane_has_operator_entry():
    """An operator-sourced INDEX entry blocks ALL auto-append — not just a
    dedup of the entry that happens to share its driver_ref. Two installed
    indexers: one matches the existing operator entry's ref, one is a
    genuinely different, not-yet-listed driver. Neither gets appended."""
    from typing import ClassVar, FrozenSet
    from unittest.mock import patch

    from dynastore.models.protocols.indexer import IndexTierDriver
    from dynastore.modules.storage.routing_config import _self_register_indexers_into

    class _AssetEsPublic:
        index_tiers: ClassVar[FrozenSet[str]] = frozenset({"asset"})
        auto_register_for_routing: ClassVar = frozenset({Operation.INDEX})

    class _AssetEsPrivate:
        index_tiers: ClassVar[FrozenSet[str]] = frozenset({"asset"})
        auto_register_for_routing: ClassVar = frozenset({Operation.INDEX})

    operator_entry = OperationDriverEntry(driver_ref="_asset_es_public", source="operator")
    target_ops: dict = {Operation.INDEX: [operator_entry]}

    with patch(
        "dynastore.tools.discovery.get_protocols",
        lambda proto: [_AssetEsPublic(), _AssetEsPrivate()] if proto is IndexTierDriver else [],
    ):
        _self_register_indexers_into(target_ops, "asset")

    # Only the original operator entry — the second, distinct installed
    # indexer must NOT be silently appended alongside it.
    assert len(target_ops[Operation.INDEX]) == 1
    assert target_ops[Operation.INDEX][0].driver_ref == "_asset_es_public"
    assert target_ops[Operation.INDEX][0].source == "operator"


def test_explicit_empty_index_survives_validate_round_trip():
    """A PUT carrying an explicit ``INDEX: []`` (an operator opt-out — no
    materialization target at all) must survive
    ``_validate_items_routing_config`` untouched, even with an indexer
    installed and discoverable in the SAME process. An empty list carries
    no entry to stamp with ``source``, so this must be gated independently
    of the operator-sourced-entry check (#3232 review)."""
    import asyncio
    from typing import ClassVar, FrozenSet
    from unittest.mock import patch

    from dynastore.modules.storage.routing_config import (
        _validate_items_routing_config,
        index_entries,
    )

    class _ItemsES:
        index_tiers: ClassVar[FrozenSet[str]] = frozenset({"item"})
        auto_register_for_routing: ClassVar = frozenset({Operation.INDEX})

    items = ItemsRoutingConfig(
        operations={
            Operation.WRITE: [
                OperationDriverEntry(driver_ref="items_postgresql_driver", source="auto"),
            ],
            Operation.INDEX: [],
        },
    )

    with patch("dynastore.tools.discovery.get_protocols", lambda proto: [_ItemsES()]):
        asyncio.run(_validate_items_routing_config(
            items, catalog_id=None, collection_id=None, db_resource=None,
        ))

    assert items.operations[Operation.INDEX] == []
    assert index_entries(items.operations) == []


def test_absent_index_key_still_seeds_on_fresh_config():
    """Existing behavior pinned: an ABSENT INDEX key (no operator intent
    expressed at all) still defers to discovery and seeds INDEX."""
    from typing import ClassVar, FrozenSet
    from unittest.mock import patch

    from dynastore.models.protocols.indexer import IndexTierDriver
    from dynastore.modules.storage.routing_config import _self_register_indexers_into

    class _AssetES:
        index_tiers: ClassVar[FrozenSet[str]] = frozenset({"asset"})
        auto_register_for_routing: ClassVar = frozenset({Operation.INDEX})

    target_ops: dict = {}  # INDEX key absent entirely

    with patch(
        "dynastore.tools.discovery.get_protocols",
        lambda proto: [_AssetES()] if proto is IndexTierDriver else [],
    ):
        _self_register_indexers_into(target_ops, "asset")

    assert [e.driver_ref for e in target_ops[Operation.INDEX]] == ["_asset_es"]
    assert target_ops[Operation.INDEX][0].source == "auto"


def test_auto_only_index_entries_reregistration_is_idempotent():
    """INDEX present with only ``source="auto"`` entries still defers to
    discovery (gate 2/3 don't fire), and re-running self-registration
    against the same discoverable set never duplicates."""
    from typing import ClassVar, FrozenSet
    from unittest.mock import patch

    from dynastore.models.protocols.indexer import IndexTierDriver
    from dynastore.modules.storage.routing_config import _self_register_indexers_into

    class _AssetES:
        index_tiers: ClassVar[FrozenSet[str]] = frozenset({"asset"})
        auto_register_for_routing: ClassVar = frozenset({Operation.INDEX})

    target_ops: dict = {
        Operation.INDEX: [OperationDriverEntry(driver_ref="_asset_es", source="auto")],
    }

    with patch(
        "dynastore.tools.discovery.get_protocols",
        lambda proto: [_AssetES()] if proto is IndexTierDriver else [],
    ):
        _self_register_indexers_into(target_ops, "asset")
        _self_register_indexers_into(target_ops, "asset")

    assert len(target_ops[Operation.INDEX]) == 1
    assert target_ops[Operation.INDEX][0].source == "auto"


# ---------------------------------------------------------------------------
# Read-time model_validator augmentation
# ---------------------------------------------------------------------------


def test_catalog_routing_validator_augments_index_lane():
    """Constructing a default CatalogRoutingConfig must fold in a
    discoverable catalog-tier indexer into the INDEX lane."""
    from typing import ClassVar, FrozenSet
    from unittest.mock import patch

    from dynastore.modules.storage.routing_config import index_entries

    class _CatES:
        index_tiers: ClassVar[FrozenSet[str]] = frozenset({"catalog"})
        auto_register_for_routing: ClassVar = frozenset({Operation.INDEX})

    instance = _CatES()

    def _fake_get_protocols(proto):
        return [instance]

    with patch("dynastore.tools.discovery.get_protocols", _fake_get_protocols):
        cfg = CatalogRoutingConfig()

    index_ids = {e.driver_ref for e in index_entries(cfg.operations)}
    assert "_cat_es" in index_ids
    # Primary WRITE entry unchanged — the registered CatalogStore is the
    # ``catalog_postgresql_driver`` composition wrapper (#732), which fans
    # CRUD across the catalog_core + catalog_stac sidecars internally.
    # Under the lane model an indexer can never appear in WRITE at all.
    write_ids = {e.driver_ref for e in cfg.operations[Operation.WRITE]}
    assert write_ids == {"catalog_postgresql_driver"}


def test_catalog_routing_validator_no_op_when_no_indexers_discoverable():
    """No discoverable indexer → operations stays at the default-factory
    shape: primary WRITE+READ only. The INDEX lane is discovery-driven, so
    with nothing discoverable the ES materialization hop is NOT present
    (#1069 / #1073) — a PG-only deployment must not pin an undrainable
    obligation into tasks.storage."""
    from unittest.mock import patch

    from dynastore.modules.storage.routing_config import index_entries

    with patch("dynastore.tools.discovery.get_protocols", lambda proto: []):
        cfg = CatalogRoutingConfig()

    # Nothing discoverable → no ES INDEX entry folded in.
    index_ids = {e.driver_ref for e in index_entries(cfg.operations)}
    assert "catalog_elasticsearch_driver" not in index_ids


def test_collection_routing_validator_augments_index_lane():
    """CollectionRoutingConfig validator augments operations[INDEX]."""
    from typing import ClassVar, FrozenSet
    from unittest.mock import patch

    from dynastore.modules.storage.routing_config import index_entries

    class _ColES:
        index_tiers: ClassVar[FrozenSet[str]] = frozenset({"collection"})
        auto_register_for_routing: ClassVar = frozenset({Operation.INDEX})

    with patch("dynastore.tools.discovery.get_protocols",
               lambda proto: [_ColES()]):
        cfg = CollectionRoutingConfig()

    index_ids = {e.driver_ref for e in index_entries(cfg.operations)}
    assert "_col_es" in index_ids


def test_items_routing_validator_augments_index_lane():
    """The model_validator on ItemsRoutingConfig augments its `operations`
    with discoverable item-tier indexer drivers into the INDEX lane.
    """
    from typing import ClassVar, FrozenSet
    from unittest.mock import patch

    from dynastore.modules.storage.routing_config import index_entries

    class _ItemsES:
        index_tiers: ClassVar[FrozenSet[str]] = frozenset({"item"})
        auto_register_for_routing: ClassVar = frozenset({Operation.INDEX})

    with patch("dynastore.tools.discovery.get_protocols",
               lambda proto: [_ItemsES()]):
        cfg = ItemsRoutingConfig()

    top_index = {e.driver_ref for e in index_entries(cfg.operations)}
    assert "_items_es" in top_index
    # Primary PG WRITE entry retained; the indexer never lands in WRITE.
    write_ids = {e.driver_ref for e in cfg.operations[Operation.WRITE]}
    assert write_ids == {"items_postgresql_driver"}


def test_items_routing_index_optin_gate():
    """ItemsRoutingConfig INDEX gate is the per-Operation opt-in set.  A
    driver lands in the items INDEX lane iff its class declares
    ``Operation.INDEX`` in ``auto_register_for_routing`` AND ``"item"`` is
    in its ``index_tiers``.
    """
    from typing import ClassVar, FrozenSet
    from unittest.mock import patch

    class _OptedInIndexer:
        index_tiers: ClassVar[FrozenSet[str]] = frozenset({"item"})
        auto_register_for_routing: ClassVar = frozenset({Operation.INDEX})

    class _OptedOutIndexer:
        # Tier claimed but no Op-set declared → not auto-augmented.
        index_tiers: ClassVar[FrozenSet[str]] = frozenset({"item"})

    with patch("dynastore.tools.discovery.get_protocols",
               lambda proto: [_OptedInIndexer(), _OptedOutIndexer()]):
        cfg = ItemsRoutingConfig()

    from dynastore.modules.storage.routing_config import index_entries
    top_index = {e.driver_ref for e in index_entries(cfg.operations)}
    assert "_opted_in_indexer" in top_index
    assert "_opted_out_indexer" not in top_index


def test_asset_routing_validator_augments_index_lane():
    """AssetRoutingConfig validator augments the INDEX lane."""
    from typing import ClassVar, FrozenSet
    from unittest.mock import patch

    from dynastore.modules.storage.routing_config import (
        AssetRoutingConfig,
        index_entries,
    )

    class _AssetES:
        index_tiers: ClassVar[FrozenSet[str]] = frozenset({"asset"})
        auto_register_for_routing: ClassVar = frozenset({Operation.INDEX})

    with patch("dynastore.tools.discovery.get_protocols",
               lambda proto: [_AssetES()]):
        cfg = AssetRoutingConfig()

    index_ids = {e.driver_ref for e in index_entries(cfg.operations)}
    assert "_asset_es" in index_ids


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
    # Discovery augmentation was skipped, and the ES INDEX hop is no longer
    # hard-coded (#1069 / #1073) — so there is no ES INDEX entry.
    from dynastore.modules.storage.routing_config import index_entries
    index_ids = {e.driver_ref for e in index_entries(cfg.operations)}
    assert "catalog_elasticsearch_driver" not in index_ids


# ---------------------------------------------------------------------------
# Provenance ("source") field
# ---------------------------------------------------------------------------


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
    from typing import ClassVar, FrozenSet
    from unittest.mock import patch

    from dynastore.modules.storage.routing_config import (
        _self_register_indexers_into,
    )

    class _ColES:
        index_tiers: ClassVar[FrozenSet[str]] = frozenset({"collection"})
        auto_register_for_routing: ClassVar = frozenset({Operation.INDEX})

    target_ops: dict = {}
    with patch("dynastore.tools.discovery.get_protocols",
               lambda proto: [_ColES()]):
        _self_register_indexers_into(target_ops, "collection")

    assert len(target_ops[Operation.INDEX]) == 1
    assert target_ops[Operation.INDEX][0].source == "auto"


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
# Transformer self-registration parity (sister to the indexer helper)
# ---------------------------------------------------------------------------


def test_transformer_helper_picks_up_entity_transform_protocol_implementers():
    """Any registered EntityTransformProtocol implementer lands in the
    ``transformers`` registry keyed by class name (matching the indexer
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

    assert _is_operator_managed({Operation.INDEX: [op_entry]}, Operation.INDEX)
    assert not _is_operator_managed({Operation.INDEX: [au_entry]}, Operation.INDEX)
    assert _is_operator_managed({Operation.INDEX: [au_entry, op_entry]}, Operation.INDEX)
    assert not _is_operator_managed({Operation.INDEX: []}, Operation.INDEX)
    assert not _is_operator_managed({}, Operation.INDEX)


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
    from typing import ClassVar, FrozenSet
    from unittest.mock import patch

    class _DiscoverableIndexer:
        index_tiers: ClassVar[FrozenSet[str]] = frozenset({"collection"})
        auto_register_for_routing: ClassVar = frozenset({Operation.INDEX})

    with patch("dynastore.tools.discovery.get_protocols",
               lambda proto: [_DiscoverableIndexer()]):
        cfg = CollectionRoutingConfig()

    from dynastore.modules.storage.routing_config import index_entries
    index_refs = {e.driver_ref for e in index_entries(cfg.operations)}
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
                 "on_failure": "fatal"},
            ],
            Operation.READ: [
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


def test_external_write_context_blocks_indexer_reinjection_on_removal():
    """End-to-end of the reported bug through the REAL deserialisation path.

    With a discoverable ES indexer registered, an operator round-trips a
    PG-only routing config (entries carry ``source='auto'`` from the GET):

    * WITHOUT the external-write context, WRITE stays ``source='auto'`` —
      not operator-managed — so the validator's self-register pass appends
      ES to INDEX.  This is the bug-reproduction baseline: the assertion
      proves the fixture genuinely exercises the gate.
    * WITH the context the validator stamps every present list
      operator-authored BEFORE self-registration, so WRITE becomes
      operator-managed and INDEX auto-registration is blocked — the
      operator's deletion sticks.
    """
    from typing import ClassVar, FrozenSet
    from unittest.mock import patch

    from dynastore.modules.storage.routing_config import index_entries

    class _ItemsES:
        index_tiers: ClassVar[FrozenSet[str]] = frozenset({"item"})
        auto_register_for_routing: ClassVar = frozenset({Operation.INDEX})

    # Baseline: no external-write context -> WRITE stays auto-sourced ->
    # INDEX auto-registration fires.
    with patch("dynastore.tools.discovery.get_protocols",
               lambda proto: [_ItemsES()]):
        bug = ItemsRoutingConfig.model_validate(_items_pg_only_body())
    bug_index = {e.driver_ref for e in index_entries(bug.operations)}
    assert "_items_es" in bug_index, (
        "fixture must reproduce INDEX auto-registration without the context flag"
    )

    # Fix path: external-write context -> WRITE locked -> INDEX stays empty.
    with patch("dynastore.tools.discovery.get_protocols",
               lambda proto: [_ItemsES()]):
        fixed = ItemsRoutingConfig.model_validate(
            _items_pg_only_body(),
            context={"dynastore_external_write": True},
        )
    fixed_write = {e.driver_ref for e in fixed.operations[Operation.WRITE]}
    fixed_index = {e.driver_ref for e in index_entries(fixed.operations)}
    assert fixed_write == {"items_postgresql_driver"}
    assert fixed_index == set()


def test_internal_construct_without_context_still_auto_augments():
    """No external-write context (internal DB-load / boot-default construction)
    => discoverable drivers still auto-register. Guards against the stamp
    over-reaching and freezing internal augmentation."""
    from typing import ClassVar, FrozenSet
    from unittest.mock import patch

    from dynastore.modules.storage.routing_config import index_entries

    class _ItemsES:
        index_tiers: ClassVar[FrozenSet[str]] = frozenset({"item"})
        auto_register_for_routing: ClassVar = frozenset({Operation.INDEX})

    with patch("dynastore.tools.discovery.get_protocols",
               lambda proto: [_ItemsES()]):
        cfg = ItemsRoutingConfig.model_validate(_items_pg_only_body())

    index_refs = {e.driver_ref for e in index_entries(cfg.operations)}
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
        Operation.INDEX: [
            {"driver_ref": "pg_driver"},
        ],
    }
    # Stored has WRITE with BOTH pg + es; READ and INDEX identical to incoming.
    stored_raw = {
        "operations": {
            Operation.WRITE: [
                {"driver_ref": "pg_driver", "source": "auto"},
                {"driver_ref": "es_driver", "source": "auto"},
            ],
            Operation.READ: [{"driver_ref": "pg_driver", "source": "auto"}],
            Operation.INDEX: [{"driver_ref": "pg_driver", "source": "auto"}],
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
        Operation.INDEX: [{"driver_ref": "es_driver"}],  # new — absent in stored
    }
    stored_raw = {
        "operations": {
            Operation.WRITE: [{"driver_ref": "pg_driver", "source": "auto"}],
        }
    }
    changed = _compute_changed_op_keys(incoming, stored_raw)
    assert Operation.INDEX in changed
    assert Operation.WRITE not in changed


def test_1865_stamp_scoped_to_changed_ops_only():
    """``_stamp_operator_provenance`` with ``changed_op_keys={WRITE}`` stamps
    WRITE entries but leaves READ/INDEX entries with their existing source."""
    cfg = ItemsRoutingConfig.model_construct(
        operations={
            Operation.WRITE: [
                OperationDriverEntry(driver_ref="pg", source="auto"),
            ],
            Operation.READ: [
                OperationDriverEntry(driver_ref="pg", source="auto"),
            ],
            Operation.INDEX: [
                OperationDriverEntry(driver_ref="pg", source="auto"),
            ],
        }
    )
    cfg._stamp_operator_provenance(changed_op_keys={Operation.WRITE})

    assert cfg.operations[Operation.WRITE][0].source == "operator"
    assert cfg.operations[Operation.READ][0].source == "auto"
    assert cfg.operations[Operation.INDEX][0].source == "auto"


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


def test_1865_write_lock_also_gates_index_auto_registration():
    """Deviation from the pre-lane-model #1865 semantic (documented): since
    indexers now self-register into a SEPARATE ``operations[INDEX]`` list,
    ``_self_register_indexers_into`` cannot gate on INDEX's own
    operator-managed status the way ``_self_register_store_drivers`` gates
    WRITE vs READ independently — INDEX has no operator-authored anchor of
    its own when a "PG-only" config never mentions it at all.  It gates on
    WRITE instead, so locking WRITE (even when INDEX itself is untouched /
    not in ``changed_op_keys``) also blocks INDEX auto-registration.  This
    is the safe-by-default posture: an operator who explicitly manages
    WRITE for an entity does not get a silently-injected ES indexer.
    """
    from typing import ClassVar, FrozenSet
    from unittest.mock import patch

    from dynastore.modules.storage.routing_config import index_entries

    class _ItemsES:
        index_tiers: ClassVar[FrozenSet[str]] = frozenset({"item"})
        auto_register_for_routing: ClassVar = frozenset({Operation.INDEX})

    # Stored config has WRITE=[pg], READ=[pg]. Operator PUTs the same
    # WRITE=[pg] (unchanged) plus READ=[pg] (unchanged) — only WRITE is
    # in changed_op_keys per the scoping contract exercised here.
    incoming_body = {
        "operations": {
            Operation.WRITE: [
                {"driver_ref": "items_postgresql_driver", "source": "auto",
                 "on_failure": "fatal"},
            ],
            Operation.READ: [
                {"driver_ref": "items_postgresql_driver", "source": "auto"},
            ],
        },
    }
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
    index_refs = {e.driver_ref for e in index_entries(cfg.operations)}

    assert write_refs == {"items_postgresql_driver"}
    assert all(e.source == "operator" for e in cfg.operations[Operation.WRITE])
    # INDEX stays empty — locked alongside WRITE even though INDEX itself
    # carried no operator-source entry to lock on its own.
    assert index_refs == set()


def test_validate_collection_routing_accepts_index_lane_entry_for_unregistered_driver():
    """An INDEX-lane entry whose driver_ref is not currently registered must
    NOT fail collection routing validation.

    Regression for the ES-catalog routing preset apply: the default
    ``CollectionRoutingConfig`` self-registers the ES collection-tier
    indexer into ``operations[INDEX]``.  A deployment where the ES driver isn't
    locally installed must not roll back the whole routing bundle — the
    runtime router / drain skip any unregistered entry at dispatch.
    """
    import asyncio
    from unittest.mock import patch

    from dynastore.modules.storage.routing_config import (
        _validate_collection_routing_config,
    )

    cfg = CollectionRoutingConfig()
    cfg.operations.clear()
    cfg.operations[Operation.INDEX] = [
        OperationDriverEntry(
            driver_ref="collection_elasticsearch_driver",
            source="auto",
        ),
    ]
    cfg.operations[Operation.READ] = []

    # No collection drivers registered → store registry is empty. The
    # unregistered INDEX entry must not raise.
    with patch("dynastore.tools.discovery.get_protocols", lambda proto: []):
        asyncio.run(_validate_collection_routing_config(
            cfg, catalog_id=None, collection_id=None, db_resource=None,
        ))

    index_refs = {e.driver_ref for e in cfg.operations[Operation.INDEX]}
    assert "collection_elasticsearch_driver" in index_refs


def test_validate_collection_routing_still_rejects_unknown_primary_store():
    """A primary WRITE entry with an unregistered driver_ref still hard-fails
    — the INDEX-lane tolerance must not weaken typo protection for the
    primary metadata store.
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
    list the ES ``collection_elasticsearch_driver`` as a READ *primary*.  The ES
    driver claims the ``collection`` INDEX tier, never a READ-capable
    ``CollectionStore`` — validating it against the store registry hard-raised
    ``operations[READ] driver ... is not registered`` and blocked the catalog.
    It must be warn-skipped (the runtime router relaxes READ to an available
    store), so a stale catalog stays readable and self-heals on the next apply.
    """
    import asyncio
    from typing import ClassVar, FrozenSet
    from unittest.mock import patch

    from dynastore.models.protocols.entity_store import CollectionStore
    from dynastore.models.protocols.indexer import IndexTierDriver
    from dynastore.modules.storage.routing_config import (
        _validate_collection_routing_config,
    )

    class CollectionPostgresqlDriver:  # the sole registered CollectionStore
        capabilities = frozenset({Capability.WRITE, Capability.READ})
        supported_hints: frozenset = frozenset()

    class CollectionElasticsearchDriver:  # claims "collection", NOT a store
        index_tiers: ClassVar[FrozenSet[str]] = frozenset({"collection"})
        auto_register_for_routing: ClassVar = frozenset(
            {Operation.INDEX, Operation.READ}
        )

    pg = CollectionPostgresqlDriver()
    es = CollectionElasticsearchDriver()

    def _fake_get_protocols(proto):
        if proto is CollectionStore:
            return [pg]
        if proto is IndexTierDriver:
            return [es]
        return []

    cfg = CollectionRoutingConfig()
    cfg.operations.clear()
    cfg.operations[Operation.READ] = [
        OperationDriverEntry(
            driver_ref="collection_postgresql_driver", source="auto",
        ),
        # Stale ES-as-READ-primary entry.
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
    nor a known collection-tier indexer (a genuine typo) still hard-fails —
    the indexer skip must not weaken typo protection on the READ side.
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


# ---------------------------------------------------------------------------
# Structural validation — a tier-claiming class without index_bulk
# ---------------------------------------------------------------------------


def test_validate_routing_entries_rejects_index_driver_without_index_bulk():
    """A driver claiming an index tier (``index_tiers``) is not, by itself,
    enough to serve as an INDEX-lane materialization target: it must also
    structurally provide ``index_bulk`` (the :class:`Indexer` protocol
    surface the drain consumes).  Claiming a tier without wiring the drain
    surface is rejected with a clear error naming the class and the
    missing method — checked independently of ``index_tiers``, which is a
    discovery/seeding concern, not a capability guarantee."""
    import pytest

    from dynastore.modules.storage.routing_config import (
        _validate_routing_entries,
    )

    class _ClaimsItemTierButNoIndexBulk:
        """Claims the ``item`` tier but never implements ``ensure_indexer``
        / ``index`` / ``index_bulk`` — fails the structural Indexer check."""

        index_tiers = frozenset({"item"})
        supported_hints: frozenset = frozenset()
        capabilities: frozenset = frozenset()

    driver = _ClaimsItemTierButNoIndexBulk()
    cfg = ItemsRoutingConfig()
    cfg.operations.clear()
    cfg.operations[Operation.INDEX] = [
        OperationDriverEntry(driver_ref="fake_index_driver", source="auto"),
    ]

    driver_index = {"fake_index_driver": driver}

    with pytest.raises(
        ValueError, match="does not implement the Indexer protocol",
    ):
        _validate_routing_entries(cfg, driver_index, "Items routing config")
