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

"""Unit tests for ABAC PG-sidecar write-path wiring (#1457 G4 + G1).

Covers:

G4 — ``_collection_uses_access_aware_driver`` detects a PG driver whose
     per-collection config declares an ``access_envelope`` sidecar.

G1 — ``upsert`` (Branch B / PG primary) pre-resolves the access envelope
     once and injects it into every per-item ``item_context`` so that
     ``AccessEnvelopeSidecar.prepare_upsert_payload`` receives
     ``context["_access_envelope"]`` and can write the sub-table row.

These are pure unit tests: no DB, no asyncpg, no managed_transaction.
Stubs wire only the edges under test.
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional

from dynastore.modules.catalog.item_service import ItemService
from dynastore.modules.storage.drivers.pg_sidecars.access_envelope_config import (
    AccessEnvelopeSidecarConfig,
)


# ---------------------------------------------------------------------------
# Shared stubs
# ---------------------------------------------------------------------------


class _StubResolved:
    """Minimal ResolvedDriver-shaped stub."""

    def __init__(self, driver: Any) -> None:
        self.driver = driver


class _PgDriverWithEnvelopeSidecar:
    """Simulates an ``ItemsPostgresqlDriver`` whose collection config
    carries an ``AccessEnvelopeSidecarConfig``."""

    async def get_driver_config(
        self, catalog_id: str, collection_id: Optional[str] = None, **_kw: Any
    ) -> Any:
        cfg = _StubConfig()
        cfg.sidecars = [AccessEnvelopeSidecarConfig()]
        return cfg


class _StubConfig:
    sidecars: List[Any] = []


class _PgDriverWithoutSidecar:
    """PG driver whose collection config has no access_envelope sidecar."""

    async def get_driver_config(
        self, catalog_id: str, collection_id: Optional[str] = None, **_kw: Any
    ) -> Any:
        return _StubConfig()


class _ESEnvelopeDriver:
    """ES private driver — detected via the class-level applies_access_filter."""

    applies_access_filter = True


class _PublicDriver:
    applies_access_filter = False


def _wire_write_drivers(monkeypatch: Any, resolved: List[Any]) -> None:
    async def _get_write_drivers(catalog_id: str, collection_id: Any) -> List[Any]:
        return resolved

    monkeypatch.setattr(
        "dynastore.modules.storage.router.get_write_drivers",
        _get_write_drivers,
    )


# ---------------------------------------------------------------------------
# G4 — _collection_uses_access_aware_driver
# ---------------------------------------------------------------------------


async def test_g4_detects_pg_sidecar_access_envelope(monkeypatch: Any) -> None:
    """G4: returns True when the PG driver config contains access_envelope sidecar."""
    svc = ItemService()
    _wire_write_drivers(
        monkeypatch,
        [_StubResolved(_PgDriverWithEnvelopeSidecar())],
    )
    assert await svc._collection_uses_access_aware_driver("cat1", "col1") is True


async def test_g4_false_when_pg_sidecar_has_no_access_envelope(monkeypatch: Any) -> None:
    """G4: returns False when the PG driver config has no access_envelope sidecar."""
    svc = ItemService()
    _wire_write_drivers(
        monkeypatch,
        [_StubResolved(_PgDriverWithoutSidecar())],
    )
    assert await svc._collection_uses_access_aware_driver("cat1", "col1") is False


async def test_g4_still_detects_es_envelope_driver(monkeypatch: Any) -> None:
    """G4: existing ES-envelope detection is unaffected by the PG branch."""
    svc = ItemService()
    _wire_write_drivers(
        monkeypatch,
        [_StubResolved(_ESEnvelopeDriver())],
    )
    assert await svc._collection_uses_access_aware_driver("cat1", "col1") is True


async def test_g4_detects_mixed_pg_plus_es_resolved(monkeypatch: Any) -> None:
    """G4: mixed routing — PG primary (no sidecar) + ES envelope secondary → True."""
    svc = ItemService()
    _wire_write_drivers(
        monkeypatch,
        [
            _StubResolved(_PgDriverWithoutSidecar()),
            _StubResolved(_ESEnvelopeDriver()),
        ],
    )
    assert await svc._collection_uses_access_aware_driver("cat1", "col1") is True


async def test_g4_fails_open_when_get_driver_config_raises(monkeypatch: Any) -> None:
    """G4: if get_driver_config raises for the PG driver, the branch is skipped
    (fail-open), and the method continues to the next resolved driver."""

    class _BrokenPgDriver:
        async def get_driver_config(self, *_a: Any, **_kw: Any) -> Any:
            raise RuntimeError("DB unavailable")

    svc = ItemService()
    _wire_write_drivers(
        monkeypatch,
        [_StubResolved(_BrokenPgDriver())],
    )
    # No ES envelope driver → False even though the PG check errored.
    assert await svc._collection_uses_access_aware_driver("cat1", "col1") is False


# ---------------------------------------------------------------------------
# G1 — access envelope injected into item_context for PG-sidecar collections
# ---------------------------------------------------------------------------


async def test_g1_access_envelope_injected_into_item_context(
    monkeypatch: Any,
) -> None:
    """G1: when _resolve_access_envelope returns an envelope, it is present in
    every item_context passed to sidecar.prepare_upsert_payload."""

    _EXPECTED_ENVELOPE = {
        "_visibility": "private",
        "_owner": "alice",
    }

    captured_contexts: List[Dict[str, Any]] = []

    class _CapturingSidecar:
        sidecar_id = "access_envelope"

        def is_mandatory(self) -> bool:
            return False

        def validate_insert(self, feature: Any, context: Any) -> Any:
            class _OK:
                valid = True
                error = None

            return _OK()

        def prepare_upsert_payload(
            self, feature: Any, context: Dict[str, Any]
        ) -> Optional[Dict[str, Any]]:
            captured_contexts.append(dict(context))
            return None  # write not needed for this test

        def get_partition_keys(self) -> List[str]:
            return []

    svc = ItemService()

    # Patch _resolve_access_envelope to return the fixed envelope.
    async def _fake_resolve(cat: str, col: str, pc: Any, feature: Any = None) -> Dict[str, Any]:
        return _EXPECTED_ENVELOPE

    monkeypatch.setattr(svc, "_resolve_access_envelope", _fake_resolve)

    # Wire all Branch B dependencies: managed_transaction, configs, sidecars,
    # insert_or_update_distributed, etc.  _patch_branch_b also sets
    # get_write_drivers to return a single PG stub so Branch B fires.
    _patch_branch_b(monkeypatch, svc, [_CapturingSidecar()])

    items = [{"type": "Feature", "id": "item1", "geometry": None, "properties": {}}]
    await svc.upsert(
        "cat1", "col1", items,
        processing_context={"owner": "alice"},
    )

    assert len(captured_contexts) == 1, "sidecar.prepare_upsert_payload called once"
    ctx = captured_contexts[0]
    assert ctx.get("_access_envelope") == _EXPECTED_ENVELOPE, (
        f"expected _access_envelope in item_context, got: {ctx}"
    )


async def test_g1_no_envelope_when_collection_not_access_aware(
    monkeypatch: Any,
) -> None:
    """G1: when _resolve_access_envelope returns None (not an access-aware
    collection), _access_envelope is absent from item_context."""

    captured_contexts: List[Dict[str, Any]] = []

    class _CapturingSidecar:
        sidecar_id = "attributes"

        def is_mandatory(self) -> bool:
            return False

        def validate_insert(self, feature: Any, context: Any) -> Any:
            class _OK:
                valid = True
                error = None

            return _OK()

        def prepare_upsert_payload(
            self, feature: Any, context: Dict[str, Any]
        ) -> Optional[Dict[str, Any]]:
            captured_contexts.append(dict(context))
            return None

        def get_partition_keys(self) -> List[str]:
            return []

    svc = ItemService()

    async def _no_envelope(cat: str, col: str, pc: Any, feature: Any = None) -> None:
        return None

    monkeypatch.setattr(svc, "_resolve_access_envelope", _no_envelope)
    _patch_branch_b(monkeypatch, svc, [_CapturingSidecar()])

    items = [{"type": "Feature", "id": "item2", "geometry": None, "properties": {}}]
    await svc.upsert("cat1", "col1", items)

    assert len(captured_contexts) == 1
    assert "_access_envelope" not in captured_contexts[0], (
        "_access_envelope must NOT appear when collection is not access-aware"
    )


async def test_pg_primary_upsert_persists_write_id_in_hub_payload(
    monkeypatch: Any,
) -> None:
    """PG-primary item writes stamp the canonical batch write id on hub rows."""

    svc = ItemService()

    async def _no_envelope(cat: str, col: str, pc: Any, feature: Any = None) -> None:
        return None

    monkeypatch.setattr(svc, "_resolve_access_envelope", _no_envelope)
    _patch_branch_b(monkeypatch, svc, [])

    captured_hub_payloads: List[Dict[str, Any]] = []

    async def _capture_insert(
        conn: Any,
        cat: str,
        col: str,
        hub_payload: Dict[str, Any],
        sidecar_payloads: Any,
        **_kw: Any,
    ) -> Dict[str, Any]:
        captured_hub_payloads.append(dict(hub_payload))
        return {"geoid": hub_payload["geoid"]}

    async def _noop(*_a: Any, **_kw: Any) -> None:
        pass

    monkeypatch.setattr(svc, "insert_or_update_distributed", _capture_insert)
    monkeypatch.setattr(svc, "_dispatch_index_upsert", _noop)

    items = [{"type": "Feature", "id": "item3", "geometry": None, "properties": {}}]
    await svc.upsert(
        "cat1",
        "col1",
        items,
        processing_context={"write_id": "w-upsert-1"},
    )

    assert captured_hub_payloads
    assert {p.get("write_id") for p in captured_hub_payloads} == {"w-upsert-1"}


async def test_pg_primary_upsert_persists_access_owner_when_principal_present(
    monkeypatch: Any,
) -> None:
    """#2687: the write-time owner is stamped onto every hub row whenever a
    principal is present in ``processing_context`` — UNGATED, regardless of
    whether the collection routes to an access-aware driver (mirrors the
    write_id rollout shape: a cheap nullable column, no access-aware gate)."""

    svc = ItemService()

    async def _no_envelope(cat: str, col: str, pc: Any, feature: Any = None) -> None:
        return None  # not an access-aware collection

    monkeypatch.setattr(svc, "_resolve_access_envelope", _no_envelope)
    _patch_branch_b(monkeypatch, svc, [])

    captured_hub_payloads: List[Dict[str, Any]] = []

    async def _capture_insert(
        conn: Any,
        cat: str,
        col: str,
        hub_payload: Dict[str, Any],
        sidecar_payloads: Any,
        **_kw: Any,
    ) -> Dict[str, Any]:
        captured_hub_payloads.append(dict(hub_payload))
        return {"geoid": hub_payload["geoid"]}

    async def _noop(*_a: Any, **_kw: Any) -> None:
        pass

    monkeypatch.setattr(svc, "insert_or_update_distributed", _capture_insert)
    monkeypatch.setattr(svc, "_dispatch_index_upsert", _noop)

    items = [{"type": "Feature", "id": "item4", "geometry": None, "properties": {}}]
    await svc.upsert(
        "cat1",
        "col1",
        items,
        processing_context={"owner": "alice"},
    )

    assert captured_hub_payloads
    assert {p.get("access_owner") for p in captured_hub_payloads} == {"alice"}


async def test_pg_primary_upsert_omits_access_owner_without_principal(
    monkeypatch: Any,
) -> None:
    """No principal in ``processing_context`` → ``access_owner`` key is
    absent from the hub payload entirely (not stamped as ``None``), so the
    dynamic batch-insert column union never gains the column for a batch
    with no principal at all."""

    svc = ItemService()

    async def _no_envelope(cat: str, col: str, pc: Any, feature: Any = None) -> None:
        return None

    monkeypatch.setattr(svc, "_resolve_access_envelope", _no_envelope)
    _patch_branch_b(monkeypatch, svc, [])

    captured_hub_payloads: List[Dict[str, Any]] = []

    async def _capture_insert(
        conn: Any,
        cat: str,
        col: str,
        hub_payload: Dict[str, Any],
        sidecar_payloads: Any,
        **_kw: Any,
    ) -> Dict[str, Any]:
        captured_hub_payloads.append(dict(hub_payload))
        return {"geoid": hub_payload["geoid"]}

    async def _noop(*_a: Any, **_kw: Any) -> None:
        pass

    monkeypatch.setattr(svc, "insert_or_update_distributed", _capture_insert)
    monkeypatch.setattr(svc, "_dispatch_index_upsert", _noop)

    items = [{"type": "Feature", "id": "item5", "geometry": None, "properties": {}}]
    await svc.upsert("cat1", "col1", items)

    assert captured_hub_payloads
    assert all("access_owner" not in p for p in captured_hub_payloads)


# ---------------------------------------------------------------------------
# Branch B infrastructure helpers
# ---------------------------------------------------------------------------


class _PgBranchBDriver:
    """Minimal stub that makes ItemService.upsert take Branch B (PG primary).

    Branch B fires when ``Capability.QUERY_FALLBACK_SOURCE`` IS in the primary
    driver's ``capabilities`` set — the code's check for non-PG primary is
    ``QUERY_FALLBACK_SOURCE not in primary.driver.capabilities`` (→ Branch A).
    A driver with ``QUERY_FALLBACK_SOURCE`` therefore falls through to Branch B.
    """

    from dynastore.models.protocols.storage_driver import Capability

    capabilities = {Capability.QUERY_FALLBACK_SOURCE}

    async def get_driver_config(
        self, catalog_id: str, collection_id: Optional[str] = None, **_kw: Any
    ) -> Any:
        from dynastore.modules.storage.driver_config import ItemsPostgresqlDriverConfig

        return ItemsPostgresqlDriverConfig()


def _patch_branch_b(
    monkeypatch: Any,
    svc: ItemService,
    sidecars_ordered: List[Any],
) -> None:
    """Wire the Branch B dependencies so ``upsert`` reaches the per-item loop."""
    from contextlib import asynccontextmanager

    # Return a single PG-type stub so primary = resolved_drivers[0] succeeds
    # and Branch B fires (QUERY_FALLBACK_SOURCE is in capabilities).
    pg_stub = _StubResolved(_PgBranchBDriver())

    async def _pg_drivers(cat: str, col: str) -> List[Any]:
        return [pg_stub]

    monkeypatch.setattr(
        "dynastore.modules.storage.router.get_write_drivers",
        _pg_drivers,
    )

    # Provide a fake engine so managed_transaction has something to work with.
    class _FakeEngine:
        pass

    svc.engine = _FakeEngine()  # type: ignore[assignment]

    # Patch managed_transaction so no real DB is needed.
    # Yield None — DriverContext(db_resource=None) is a valid Pydantic value and
    # all Phase 1 / Phase 4 helpers in the stubs ignore the conn argument anyway.
    @asynccontextmanager
    async def _fake_tx(engine: Any):
        yield None

    monkeypatch.setattr(
        "dynastore.modules.catalog.item_service.managed_transaction",
        _fake_tx,
    )

    # Patch _load_branch_b_phase1 — instead of hitting DB, return the pre-built
    # col_config + sidecars.  We do this by patching the individual async helpers
    # that Phase 1 calls.
    _patch_phase1_helpers(monkeypatch, svc, sidecars_ordered)


def _patch_phase1_helpers(
    monkeypatch: Any,
    svc: ItemService,
    sidecars_ordered: List[Any],
) -> None:
    """Stub all the async DB helpers that Phase 1 of Branch B calls."""
    # CatalogsProtocol — ensure_alive + is_active + activate_collection.
    class _FakeCatalogs:
        async def ensure_alive(self, *_a: Any, **_kw: Any) -> None:
            return None

        async def is_active(self, *_a: Any, **_kw: Any) -> bool:
            return True

        async def activate_collection(self, *_a: Any, **_kw: Any) -> None:
            pass

    monkeypatch.setattr(
        "dynastore.modules.catalog.item_service.get_protocol",
        lambda p, *a, **kw: (
            _FakeCatalogs()
            if p.__name__ in ("CatalogsProtocol",)
            else None
        ),
    )

    # _resolve_physical_schema / _resolve_physical_table
    async def _fake_schema(cat: str, **_kw: Any) -> str:
        return "public"

    async def _fake_table(cat: str, col: str, **_kw: Any) -> str:
        return "items_test"

    monkeypatch.setattr(svc, "_resolve_physical_schema", _fake_schema)
    monkeypatch.setattr(svc, "_resolve_physical_table", _fake_table)

    # ensure_physical_table_exists — no-op
    async def _noop_ensure(*_a: Any, **_kw: Any) -> None:
        pass

    monkeypatch.setattr(svc, "ensure_physical_table_exists", _noop_ensure)

    # primary.driver.get_driver_config — needed only when primary is not None;
    # we pass [] so primary is None and this path doesn't run.

    # Phase 1 config resolvers: _effective_sidecars + SidecarRegistry
    # Patch at the call sites inside upsert so sidecars_ordered is injected.
    import dynastore.modules.storage.drivers.pg_sidecars as _pg_sc

    def _fake_effective(col_cfg: Any, **_kw: Any) -> List[Any]:
        # Return the test-injected sidecars as their configs (identity mapping).
        return sidecars_ordered  # type: ignore[return-value]

    monkeypatch.setattr(_pg_sc, "_effective_sidecars", _fake_effective)

    # SidecarRegistry.get_sidecar — return the sidecar as-is
    # (our stubs ARE the sidecar instances already).
    def _identity_sidecar(cfg: Any) -> Any:
        return cfg  # cfg is already the sidecar instance in our stubs

    monkeypatch.setattr(_pg_sc.SidecarRegistry, "get_sidecar", staticmethod(_identity_sidecar))

    # CollectionPluginConfig — return a stub with ingest_chunk_size.
    class _FakePartitioning:
        enabled = False

    class _FakeCollectionConfig:
        max_bulk_features = 1000
        ingest_chunk_size = 50
        partitioning = _FakePartitioning()

    # Unified get_protocol stub — handles ConfigsProtocol (all get_config calls)
    # and CatalogsProtocol (is_active / activate_collection).  Returns a single
    # _FakeConfigsProto for both so Phase 1's `assert _configs is not None` passes.
    class _FakeConfigsProto:
        """Handles ConfigsProtocol.get_config for any class asked during Phase 1."""

        async def get_config(self, cls: Any, *args: Any, **kw: Any) -> Any:
            from dynastore.modules.storage.driver_config import (
                ItemsPostgresqlDriverConfig,
            )

            name = getattr(cls, "__name__", "") or ""
            if name == "CollectionPluginConfig":
                return _FakeCollectionConfig()
            if cls is ItemsPostgresqlDriverConfig or name == "ItemsPostgresqlDriverConfig":
                return ItemsPostgresqlDriverConfig()
            if name == "CollectionInfo":
                from dynastore.modules.catalog.catalog_config import CollectionInfo
                return CollectionInfo()
            # For unknown classes return a default-safe None; callers that
            # assert non-None will catch the error explicitly.
            return None

        # CatalogsProtocol interface (Phase 1 calls ensure_alive / is_active /
        # activate_collection).
        async def ensure_alive(self, *_a: Any, **_kw: Any) -> None:
            return None

        async def is_active(self, *_a: Any, **_kw: Any) -> bool:
            return True

        async def activate_collection(self, *_a: Any, **_kw: Any) -> None:
            pass

    _fake_proto_instance = _FakeConfigsProto()
    monkeypatch.setattr(
        "dynastore.modules.catalog.item_service.get_protocol",
        lambda p, *a, **kw: _fake_proto_instance,
    )

    # insert_or_update_distributed — return a fake hub row (geoid required).
    async def _fake_insert(
        conn: Any,
        cat: str,
        col: str,
        hub_payload: Any,
        sidecar_payloads: Any,
        **_kw: Any,
    ) -> Dict[str, Any]:
        return {"geoid": hub_payload.get("geoid", "stub-geoid")}

    monkeypatch.setattr(svc, "insert_or_update_distributed", _fake_insert)

    # _dispatch_index_upsert + _dispatch_tile_cache_invalidation — no-ops.
    async def _noop(*_a: Any, **_kw: Any) -> None:
        pass

    monkeypatch.setattr(svc, "_dispatch_index_upsert", _noop)
    monkeypatch.setattr(svc, "_dispatch_tile_cache_invalidation", _noop)
    monkeypatch.setattr(svc, "_enforce_strict_unknown_fields", _noop)
    monkeypatch.setattr(svc, "_fan_out_to_secondary_drivers", _noop)

    async def _passthrough_geometry(
        cat: str,
        col: str,
        items: List[Any],
        drivers: Any,
        **_kw: Any,
    ) -> List[Any]:
        return items

    monkeypatch.setattr(svc, "_enforce_es_geometry_size_limit", _passthrough_geometry)

    # Phase 5: fetch_features_bulk reads back written items.  Return a minimal
    # Feature list so the caller has something to work with without hitting DB.
    async def _fake_fetch_bulk(
        conn: Any,
        schema: str,
        table: str,
        geoids: List[str],
        col_config: Any,
        **_kw: Any,
    ) -> List[Any]:
        from dynastore.models.ogc import Feature

        return [
            Feature(type="Feature", id=g, geometry=None, properties={})
            for g in geoids
        ]

    monkeypatch.setattr(svc, "fetch_features_bulk", _fake_fetch_bulk)

    # _resolve_stac_items_pg — returns False (no STAC items sidecar needed).
    # STAC is opt-in: the items sidecar is injected only when the scope's
    # StacStorageConfig has items-tier + PG; this fake pins the "off" branch.
    import dynastore.modules.storage.drivers.postgresql as _pg_drv
    monkeypatch.setattr(
        _pg_drv, "_resolve_stac_items_pg",
        lambda *a, **k: _async_false(),
    )


async def _async_false() -> bool:
    return False
