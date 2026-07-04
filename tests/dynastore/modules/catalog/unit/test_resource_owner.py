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

"""Unit tests for resource_owner.py — Chunk 1 of Resource Cascade Cleanup.

Covers:
- ScopeRef and CleanupRef equality, hashability, frozen-ness.
- CleanupRef.to_json round-trip including nested metadata.
- CleanupRef.from_json error on missing required keys.
- Enum string values (must match JSONB storage).
- ResourceOwnerProtocol runtime isinstance checks.
- BaseResourceOwner default supported_scopes.
"""

from __future__ import annotations

import dataclasses
import json
from typing import Any, Iterable

import pytest

from dynastore.modules.catalog.resource_owner import (
    BaseResourceOwner,
    CleanupMode,
    CleanupOutcome,
    ResourceOwnerProtocol,
    CleanupRef,
    ResourceScope,
    ScopeRef,
)


# ---------------------------------------------------------------------------
# Enum string values
# ---------------------------------------------------------------------------


class TestEnumStringValues:
    """Enum values hit asyncpg JSONB — must be exact."""

    def test_cleanup_mode_soft(self) -> None:
        assert CleanupMode.SOFT == "soft"

    def test_cleanup_mode_hard(self) -> None:
        assert CleanupMode.HARD == "hard"

    def test_resource_scope_catalog(self) -> None:
        assert ResourceScope.CATALOG == "catalog"

    def test_resource_scope_collection(self) -> None:
        assert ResourceScope.COLLECTION == "collection"

    def test_resource_scope_asset(self) -> None:
        assert ResourceScope.ASSET == "asset"

    def test_resource_scope_item(self) -> None:
        assert ResourceScope.ITEM == "item"

    def test_cleanup_outcome_done(self) -> None:
        assert CleanupOutcome.DONE == "done"

    def test_cleanup_outcome_retry(self) -> None:
        assert CleanupOutcome.RETRY == "retry"

    def test_cleanup_outcome_dead(self) -> None:
        assert CleanupOutcome.DEAD == "dead"


# ---------------------------------------------------------------------------
# ScopeRef
# ---------------------------------------------------------------------------


class TestScopeRef:
    def test_equality_same_values(self) -> None:
        a = ScopeRef(scope=ResourceScope.CATALOG, catalog_id="cat-1")
        b = ScopeRef(scope=ResourceScope.CATALOG, catalog_id="cat-1")
        assert a == b

    def test_equality_with_optional_fields(self) -> None:
        a = ScopeRef(
            scope=ResourceScope.COLLECTION,
            catalog_id="cat-1",
            collection_id="col-1",
        )
        b = ScopeRef(
            scope=ResourceScope.COLLECTION,
            catalog_id="cat-1",
            collection_id="col-1",
        )
        assert a == b

    def test_inequality_different_catalog(self) -> None:
        a = ScopeRef(scope=ResourceScope.CATALOG, catalog_id="cat-1")
        b = ScopeRef(scope=ResourceScope.CATALOG, catalog_id="cat-2")
        assert a != b

    def test_hashable(self) -> None:
        ref = ScopeRef(scope=ResourceScope.CATALOG, catalog_id="cat-1")
        s = {ref}
        assert ref in s

    def test_frozen_mutation_raises(self) -> None:
        ref = ScopeRef(scope=ResourceScope.CATALOG, catalog_id="cat-1")
        with pytest.raises((dataclasses.FrozenInstanceError, AttributeError)):
            ref.catalog_id = "mutated"  # type: ignore[misc]

    def test_optional_fields_default_none(self) -> None:
        ref = ScopeRef(scope=ResourceScope.CATALOG, catalog_id="cat-1")
        assert ref.collection_id is None
        assert ref.asset_id is None
        assert ref.item_id is None


# ---------------------------------------------------------------------------
# CleanupRef
# ---------------------------------------------------------------------------


class TestCleanupRef:
    def test_equality(self) -> None:
        a = CleanupRef(kind="es_index", locator="cat-items", owner_id="es.items")
        b = CleanupRef(kind="es_index", locator="cat-items", owner_id="es.items")
        assert a == b

    def test_inequality_different_locator(self) -> None:
        a = CleanupRef(kind="es_index", locator="cat-a", owner_id="es.items")
        b = CleanupRef(kind="es_index", locator="cat-b", owner_id="es.items")
        assert a != b

    def test_hashable(self) -> None:
        ref = CleanupRef(kind="es_index", locator="cat-items", owner_id="es.items")
        assert ref in {ref}

    def test_frozen_mutation_raises(self) -> None:
        ref = CleanupRef(kind="es_index", locator="cat-items", owner_id="es.items")
        with pytest.raises((dataclasses.FrozenInstanceError, AttributeError)):
            ref.kind = "mutated"  # type: ignore[misc]

    def test_metadata_defaults_to_empty_dict(self) -> None:
        ref = CleanupRef(kind="es_index", locator="idx", owner_id="es.items")
        assert ref.metadata == {}

    def test_to_json_round_trip_simple(self) -> None:
        ref = CleanupRef(kind="es_index", locator="fao-public-items", owner_id="es.items")
        d = ref.to_json()
        assert d == {
            "kind": "es_index",
            "locator": "fao-public-items",
            "owner_id": "es.items",
            "metadata": {},
        }
        restored = CleanupRef.from_json(d)
        assert restored == ref

    def test_to_json_round_trip_with_nested_metadata(self) -> None:
        meta: dict[str, Any] = {
            "index_prefix": "fao-public",
            "is_private": True,
            "replicas": 2,
            "tags": ["a", "b"],
            "nested": {"x": None, "y": 3.14},
        }
        ref = CleanupRef(
            kind="es_index",
            locator="fao-public-items",
            owner_id="es.items",
            metadata=meta,
        )
        serialized = json.dumps(ref.to_json())
        restored = CleanupRef.from_json(json.loads(serialized))
        assert restored == ref

    def test_to_json_round_trip_none_in_metadata(self) -> None:
        ref = CleanupRef(
            kind="gcs_prefix",
            locator="gs://bucket/prefix/",
            owner_id="gcp.gcs.prefix",
            metadata={"deleted_at": None},
        )
        assert CleanupRef.from_json(json.loads(json.dumps(ref.to_json()))) == ref

    def test_from_json_missing_kind_raises(self) -> None:
        with pytest.raises(KeyError):
            CleanupRef.from_json({"locator": "x", "owner_id": "y"})

    def test_from_json_missing_locator_raises(self) -> None:
        with pytest.raises(KeyError):
            CleanupRef.from_json({"kind": "es_index", "owner_id": "y"})

    def test_from_json_missing_owner_id_raises(self) -> None:
        with pytest.raises(KeyError):
            CleanupRef.from_json({"kind": "es_index", "locator": "x"})

    def test_from_json_missing_metadata_uses_empty_dict(self) -> None:
        ref = CleanupRef.from_json(
            {"kind": "es_index", "locator": "x", "owner_id": "es.items"}
        )
        assert ref.metadata == {}


# ---------------------------------------------------------------------------
# ResourceOwnerProtocol runtime isinstance
# ---------------------------------------------------------------------------


class TestResourceOwnerProtocolRuntimeCheck:
    def test_compliant_class_passes_isinstance(self) -> None:
        class GoodOwner:
            owner_id = "good.owner"

            def supported_scopes(self) -> Iterable[ResourceScope]:
                return (ResourceScope.CATALOG,)

            async def describe_scope(self, scope_ref: ScopeRef, conn: Any) -> list[CleanupRef]:
                return []

            async def cleanup_one(
                self,
                ref: CleanupRef,
                mode: CleanupMode,
                *,
                dry_run: bool = False,
            ) -> CleanupOutcome:
                return CleanupOutcome.DONE

        assert isinstance(GoodOwner(), ResourceOwnerProtocol)

    def test_missing_describe_scope_fails_isinstance(self) -> None:
        class BadOwner:
            owner_id = "bad.owner"

            def supported_scopes(self) -> Iterable[ResourceScope]:
                return (ResourceScope.CATALOG,)

            # missing describe_scope

            async def cleanup_one(
                self,
                ref: CleanupRef,
                mode: CleanupMode,
                *,
                dry_run: bool = False,
            ) -> CleanupOutcome:
                return CleanupOutcome.DONE

        assert not isinstance(BadOwner(), ResourceOwnerProtocol)

    def test_missing_cleanup_one_fails_isinstance(self) -> None:
        class BadOwner:
            owner_id = "bad.owner2"

            def supported_scopes(self) -> Iterable[ResourceScope]:
                return (ResourceScope.CATALOG,)

            async def describe_scope(self, scope_ref: ScopeRef, conn: Any) -> list[CleanupRef]:
                return []

            # missing cleanup_one

        assert not isinstance(BadOwner(), ResourceOwnerProtocol)

    def test_missing_supported_scopes_fails_isinstance(self) -> None:
        class BadOwner:
            owner_id = "bad.owner3"

            # missing supported_scopes

            async def describe_scope(self, scope_ref: ScopeRef, conn: Any) -> list[CleanupRef]:
                return []

            async def cleanup_one(
                self,
                ref: CleanupRef,
                mode: CleanupMode,
                *,
                dry_run: bool = False,
            ) -> CleanupOutcome:
                return CleanupOutcome.DONE

        assert not isinstance(BadOwner(), ResourceOwnerProtocol)


# ---------------------------------------------------------------------------
# BaseResourceOwner default supported_scopes
# ---------------------------------------------------------------------------


class TestBaseResourceOwnerDefaultScopes:
    def test_default_scopes_contains_catalog_and_collection(self) -> None:
        class ConcreteOwner(BaseResourceOwner):
            owner_id = "concrete.owner"

            async def describe_scope(
                self, scope_ref: ScopeRef, conn: Any
            ) -> list[CleanupRef]:
                return []

            async def cleanup_one(
                self,
                ref: CleanupRef,
                mode: CleanupMode,
                *,
                dry_run: bool = False,
            ) -> CleanupOutcome:
                return CleanupOutcome.DONE

        owner = ConcreteOwner()
        scopes = list(owner.supported_scopes())
        assert ResourceScope.CATALOG in scopes
        assert ResourceScope.COLLECTION in scopes

    def test_override_scopes(self) -> None:
        class NarrowOwner(BaseResourceOwner):
            owner_id = "narrow.owner"

            def supported_scopes(self) -> Iterable[ResourceScope]:
                return (ResourceScope.COLLECTION,)

            async def describe_scope(
                self, scope_ref: ScopeRef, conn: Any
            ) -> list[CleanupRef]:
                return []

            async def cleanup_one(
                self,
                ref: CleanupRef,
                mode: CleanupMode,
                *,
                dry_run: bool = False,
            ) -> CleanupOutcome:
                return CleanupOutcome.DONE

        scopes = list(NarrowOwner().supported_scopes())
        assert scopes == [ResourceScope.COLLECTION]
        assert ResourceScope.CATALOG not in scopes
