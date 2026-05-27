"""Unit tests for cascade_registry.py — Chunk 1 of Resource Cascade Cleanup.

Covers:
- register + get + owners_for_scope happy path.
- Duplicate owner_id raises ValueError.
- Unknown owner_id raises KeyError.
- owners_for_scope returns stable registration order.
- owners_for_scope returns a copy (mutating result does not affect registry).
- freeze blocks register / unregister.
- is_frozen reflects state.
- Thread-safety smoke test: concurrent register from multiple threads.
- Multi-scope owner: owner registered under both CATALOG and COLLECTION.
"""

from __future__ import annotations

import threading
from typing import Any, Iterable

import pytest

from dynastore.modules.catalog.cascade_registry import CascadeCleanupRegistry
from dynastore.modules.catalog.resource_owner import (
    CleanupMode,
    CleanupOutcome,
    ResourceOwnerProtocol,
    CleanupRef,
    ResourceScope,
    ScopeRef,
)


# ---------------------------------------------------------------------------
# Fake owners
# ---------------------------------------------------------------------------


def _make_owner(
    owner_id: str,
    scopes: tuple[ResourceScope, ...] = (ResourceScope.CATALOG, ResourceScope.COLLECTION),
) -> ResourceOwnerProtocol:
    """Build a minimal compliant owner instance with the given owner_id and scopes."""

    class _FakeOwner:
        pass

    _FakeOwner.owner_id = owner_id  # type: ignore[attr-defined]

    def supported_scopes(self: Any) -> Iterable[ResourceScope]:
        return scopes

    async def describe_scope(
        self: Any, scope_ref: ScopeRef, conn: Any
    ) -> list[CleanupRef]:
        return []

    async def cleanup_one(
        self: Any,
        ref: CleanupRef,
        mode: CleanupMode,
        *,
        dry_run: bool = False,
    ) -> CleanupOutcome:
        return CleanupOutcome.DONE

    _FakeOwner.supported_scopes = supported_scopes  # type: ignore[attr-defined]
    _FakeOwner.describe_scope = describe_scope  # type: ignore[attr-defined]
    _FakeOwner.cleanup_one = cleanup_one  # type: ignore[attr-defined]

    return _FakeOwner()  # type: ignore[return-value]


# ---------------------------------------------------------------------------
# Happy path
# ---------------------------------------------------------------------------


class TestRegistryHappyPath:
    def test_register_and_get(self) -> None:
        reg = CascadeCleanupRegistry()
        owner = _make_owner("es.items")
        reg.register(owner)
        assert reg.get("es.items") is owner

    def test_owners_for_scope_catalog(self) -> None:
        reg = CascadeCleanupRegistry()
        owner = _make_owner("es.items", (ResourceScope.CATALOG,))
        reg.register(owner)
        result = reg.owners_for_scope(ResourceScope.CATALOG)
        assert owner in result

    def test_owners_for_scope_collection(self) -> None:
        reg = CascadeCleanupRegistry()
        owner = _make_owner("es.items", (ResourceScope.COLLECTION,))
        reg.register(owner)
        result = reg.owners_for_scope(ResourceScope.COLLECTION)
        assert owner in result

    def test_owners_for_empty_scope_returns_empty_list(self) -> None:
        reg = CascadeCleanupRegistry()
        assert reg.owners_for_scope(ResourceScope.ASSET) == []

    def test_len_reflects_registered_count(self) -> None:
        reg = CascadeCleanupRegistry()
        reg.register(_make_owner("a", (ResourceScope.CATALOG,)))
        reg.register(_make_owner("b", (ResourceScope.CATALOG,)))
        assert len(reg) == 2


# ---------------------------------------------------------------------------
# Error cases
# ---------------------------------------------------------------------------


class TestRegistryErrorCases:
    def test_duplicate_owner_id_raises_value_error(self) -> None:
        reg = CascadeCleanupRegistry()
        reg.register(_make_owner("es.items"))
        with pytest.raises(ValueError, match="es.items"):
            reg.register(_make_owner("es.items"))

    def test_unknown_owner_id_raises_key_error(self) -> None:
        reg = CascadeCleanupRegistry()
        with pytest.raises(KeyError):
            reg.get("nonexistent.owner")


# ---------------------------------------------------------------------------
# Stable order + copy semantics
# ---------------------------------------------------------------------------


class TestRegistryOrderAndCopy:
    def test_owners_for_scope_stable_order(self) -> None:
        reg = CascadeCleanupRegistry()
        a = _make_owner("owner-a", (ResourceScope.CATALOG,))
        b = _make_owner("owner-b", (ResourceScope.CATALOG,))
        c = _make_owner("owner-c", (ResourceScope.CATALOG,))
        reg.register(a)
        reg.register(b)
        reg.register(c)
        result = reg.owners_for_scope(ResourceScope.CATALOG)
        assert result == [a, b, c]

    def test_owners_for_scope_returns_copy(self) -> None:
        reg = CascadeCleanupRegistry()
        owner = _make_owner("es.items", (ResourceScope.CATALOG,))
        reg.register(owner)
        result = reg.owners_for_scope(ResourceScope.CATALOG)
        result.clear()  # mutate the returned list
        # Registry internal state is unaffected
        assert reg.owners_for_scope(ResourceScope.CATALOG) == [owner]


# ---------------------------------------------------------------------------
# unregister
# ---------------------------------------------------------------------------


class TestRegistryUnregister:
    def test_unregister_removes_from_get(self) -> None:
        reg = CascadeCleanupRegistry()
        reg.register(_make_owner("es.items"))
        reg.unregister("es.items")
        with pytest.raises(KeyError):
            reg.get("es.items")

    def test_unregister_removes_from_scope_index(self) -> None:
        reg = CascadeCleanupRegistry()
        reg.register(_make_owner("es.items", (ResourceScope.CATALOG,)))
        reg.unregister("es.items")
        assert reg.owners_for_scope(ResourceScope.CATALOG) == []

    def test_unregister_nonexistent_is_noop(self) -> None:
        reg = CascadeCleanupRegistry()
        reg.unregister("ghost")  # should not raise


# ---------------------------------------------------------------------------
# freeze
# ---------------------------------------------------------------------------


class TestRegistryFreeze:
    def test_is_frozen_false_initially(self) -> None:
        reg = CascadeCleanupRegistry()
        assert reg.is_frozen is False

    def test_freeze_sets_is_frozen(self) -> None:
        reg = CascadeCleanupRegistry()
        reg.freeze()
        assert reg.is_frozen is True

    def test_register_after_freeze_raises(self) -> None:
        reg = CascadeCleanupRegistry()
        reg.freeze()
        with pytest.raises(RuntimeError, match="frozen"):
            reg.register(_make_owner("es.items"))

    def test_unregister_after_freeze_raises(self) -> None:
        reg = CascadeCleanupRegistry()
        reg.register(_make_owner("es.items"))
        reg.freeze()
        with pytest.raises(RuntimeError, match="frozen"):
            reg.unregister("es.items")

    def test_freeze_is_idempotent(self) -> None:
        reg = CascadeCleanupRegistry()
        reg.freeze()
        reg.freeze()  # second call must not raise
        assert reg.is_frozen is True

    def test_get_works_after_freeze(self) -> None:
        reg = CascadeCleanupRegistry()
        owner = _make_owner("es.items")
        reg.register(owner)
        reg.freeze()
        assert reg.get("es.items") is owner

    def test_owners_for_scope_works_after_freeze(self) -> None:
        reg = CascadeCleanupRegistry()
        owner = _make_owner("es.items", (ResourceScope.CATALOG,))
        reg.register(owner)
        reg.freeze()
        assert owner in reg.owners_for_scope(ResourceScope.CATALOG)


# ---------------------------------------------------------------------------
# Multi-scope owner
# ---------------------------------------------------------------------------


class TestMultiScopeOwner:
    def test_owner_found_via_catalog_and_collection(self) -> None:
        reg = CascadeCleanupRegistry()
        owner = _make_owner("es.items", (ResourceScope.CATALOG, ResourceScope.COLLECTION))
        reg.register(owner)
        assert owner in reg.owners_for_scope(ResourceScope.CATALOG)
        assert owner in reg.owners_for_scope(ResourceScope.COLLECTION)

    def test_owner_not_found_via_unregistered_scope(self) -> None:
        reg = CascadeCleanupRegistry()
        reg.register(_make_owner("es.items", (ResourceScope.CATALOG,)))
        assert reg.owners_for_scope(ResourceScope.COLLECTION) == []


# ---------------------------------------------------------------------------
# Thread-safety smoke test
# ---------------------------------------------------------------------------


class TestThreadSafety:
    def test_concurrent_register_from_multiple_threads(self) -> None:
        reg = CascadeCleanupRegistry()
        errors: list[Exception] = []
        owner_ids = [f"owner-{i}" for i in range(20)]

        def _register(oid: str) -> None:
            try:
                reg.register(_make_owner(oid, (ResourceScope.CATALOG,)))
            except Exception as exc:
                errors.append(exc)

        threads = [threading.Thread(target=_register, args=(oid,)) for oid in owner_ids]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert errors == [], f"Unexpected errors during concurrent register: {errors}"
        assert len(reg) == len(owner_ids)
        catalog_owners = reg.owners_for_scope(ResourceScope.CATALOG)
        assert len(catalog_owners) == len(owner_ids)
