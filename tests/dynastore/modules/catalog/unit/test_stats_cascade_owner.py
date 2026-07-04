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

"""Unit tests for StatsTelemetryOwner.

Covers:
- describe_scope: CATALOG scope emits one CleanupRef with correct index name
  and catalog_id metadata.
- describe_scope: COLLECTION scope returns empty list (no per-collection index).
- describe_scope: ASSET scope returns empty list.
- cleanup_one: SOFT mode returns DONE without touching ES.
- cleanup_one: HARD dry_run returns DONE without touching ES.
- cleanup_one: HARD, ES client unavailable returns RETRY.
- cleanup_one: HARD, index absent returns DONE (idempotent).
- cleanup_one: HARD, index present — deletes it and returns DONE.
- cleanup_one: HARD, ES raises exception returns RETRY.
- register_owners: adds StatsTelemetryOwner to registry under CATALOG scope.

All tests use mocks only — no live ES or DB connection.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from dynastore.modules.catalog.cascade_registry import CascadeCleanupRegistry
from dynastore.modules.catalog.resource_owner import (
    CleanupMode,
    CleanupOutcome,
    CleanupRef,
    ResourceScope,
    ScopeRef,
)

_CATALOG_ID = "my-catalog-001"
_COLLECTION_ID = "my-collection-abc"
_ASSET_ID = "asset-xyz"
_PREFIX = "fao"
# Expected index name: prefix + "_access_logs_" + safe_catalog_id
_EXPECTED_INDEX = f"{_PREFIX}_access_logs_{_CATALOG_ID.lower().replace(' ', '_')}"


def _make_owner():
    from dynastore.modules.stats.cascade_owner import StatsTelemetryOwner
    return StatsTelemetryOwner()


class TestStatsTelemetryOwnerDescribeScope:
    @pytest.mark.asyncio
    async def test_catalog_scope_returns_one_ref(self) -> None:
        owner = _make_owner()
        scope_ref = ScopeRef(scope=ResourceScope.CATALOG, catalog_id=_CATALOG_ID)

        with patch(
            "dynastore.modules.elasticsearch.client.get_index_prefix",
            return_value=_PREFIX,
        ):
            refs = await owner.describe_scope(scope_ref, MagicMock())

        assert len(refs) == 1
        ref = refs[0]
        assert ref.kind == "stats_es_index"
        assert ref.locator == _EXPECTED_INDEX
        assert ref.owner_id == "stats.telemetry"
        assert ref.metadata["catalog_id"] == _CATALOG_ID
        assert ref.metadata["index_name"] == _EXPECTED_INDEX

    @pytest.mark.asyncio
    async def test_catalog_scope_index_name_sanitises_spaces(self) -> None:
        owner = _make_owner()
        catalog_with_spaces = "my catalog"
        scope_ref = ScopeRef(scope=ResourceScope.CATALOG, catalog_id=catalog_with_spaces)

        with patch(
            "dynastore.modules.elasticsearch.client.get_index_prefix",
            return_value=_PREFIX,
        ):
            refs = await owner.describe_scope(scope_ref, MagicMock())

        assert len(refs) == 1
        assert refs[0].locator == f"{_PREFIX}_access_logs_my_catalog"

    @pytest.mark.asyncio
    async def test_collection_scope_returns_empty(self) -> None:
        owner = _make_owner()
        scope_ref = ScopeRef(
            scope=ResourceScope.COLLECTION,
            catalog_id=_CATALOG_ID,
            collection_id=_COLLECTION_ID,
        )
        refs = await owner.describe_scope(scope_ref, MagicMock())
        assert refs == []

    @pytest.mark.asyncio
    async def test_asset_scope_returns_empty(self) -> None:
        owner = _make_owner()
        scope_ref = ScopeRef(
            scope=ResourceScope.ASSET,
            catalog_id=_CATALOG_ID,
            collection_id=_COLLECTION_ID,
            asset_id=_ASSET_ID,
        )
        refs = await owner.describe_scope(scope_ref, MagicMock())
        assert refs == []


class TestStatsTelemetryOwnerCleanupOne:
    def _make_ref(self) -> CleanupRef:
        return CleanupRef(
            kind="stats_es_index",
            locator=_EXPECTED_INDEX,
            owner_id="stats.telemetry",
            metadata={"catalog_id": _CATALOG_ID, "index_name": _EXPECTED_INDEX},
        )

    @pytest.mark.asyncio
    async def test_soft_mode_returns_done_without_es(self) -> None:
        owner = _make_owner()
        ref = self._make_ref()
        # ES client must never be called in SOFT mode
        with patch(
            "dynastore.modules.elasticsearch.client.get_client",
            side_effect=AssertionError("ES must not be called in SOFT mode"),
        ):
            outcome = await owner.cleanup_one(ref, CleanupMode.SOFT)
        assert outcome == CleanupOutcome.DONE

    @pytest.mark.asyncio
    async def test_dry_run_hard_returns_done_without_es(self) -> None:
        owner = _make_owner()
        ref = self._make_ref()
        with patch(
            "dynastore.modules.elasticsearch.client.get_client",
            side_effect=AssertionError("ES must not be called in dry-run"),
        ):
            outcome = await owner.cleanup_one(ref, CleanupMode.HARD, dry_run=True)
        assert outcome == CleanupOutcome.DONE

    @pytest.mark.asyncio
    async def test_hard_no_es_client_returns_retry(self) -> None:
        owner = _make_owner()
        ref = self._make_ref()
        with patch(
            "dynastore.modules.elasticsearch.client.get_client",
            return_value=None,
        ):
            outcome = await owner.cleanup_one(ref, CleanupMode.HARD)
        assert outcome == CleanupOutcome.RETRY

    @pytest.mark.asyncio
    async def test_hard_index_absent_returns_done(self) -> None:
        owner = _make_owner()
        ref = self._make_ref()

        mock_es = MagicMock()
        mock_es.indices = MagicMock()
        mock_es.indices.exists = AsyncMock(return_value=False)
        mock_es.indices.delete = AsyncMock(
            side_effect=AssertionError("delete must not be called when index absent")
        )

        with patch(
            "dynastore.modules.elasticsearch.client.get_client",
            return_value=mock_es,
        ):
            outcome = await owner.cleanup_one(ref, CleanupMode.HARD)

        assert outcome == CleanupOutcome.DONE
        mock_es.indices.exists.assert_awaited_once_with(index=_EXPECTED_INDEX)

    @pytest.mark.asyncio
    async def test_hard_index_present_deletes_and_returns_done(self) -> None:
        owner = _make_owner()
        ref = self._make_ref()

        mock_es = MagicMock()
        mock_es.indices = MagicMock()
        mock_es.indices.exists = AsyncMock(return_value=True)
        mock_es.indices.delete = AsyncMock(return_value={"acknowledged": True})

        with patch(
            "dynastore.modules.elasticsearch.client.get_client",
            return_value=mock_es,
        ):
            outcome = await owner.cleanup_one(ref, CleanupMode.HARD)

        assert outcome == CleanupOutcome.DONE
        mock_es.indices.delete.assert_awaited_once_with(index=_EXPECTED_INDEX)

    @pytest.mark.asyncio
    async def test_hard_es_raises_returns_retry(self) -> None:
        owner = _make_owner()
        ref = self._make_ref()

        mock_es = MagicMock()
        mock_es.indices = MagicMock()
        mock_es.indices.exists = AsyncMock(side_effect=OSError("connection refused"))

        with patch(
            "dynastore.modules.elasticsearch.client.get_client",
            return_value=mock_es,
        ):
            outcome = await owner.cleanup_one(ref, CleanupMode.HARD)

        assert outcome == CleanupOutcome.RETRY

    @pytest.mark.asyncio
    async def test_hard_ref_uses_locator_when_metadata_absent(self) -> None:
        """cleanup_one falls back to ref.locator when metadata.index_name is absent."""
        owner = _make_owner()
        ref = CleanupRef(
            kind="stats_es_index",
            locator=_EXPECTED_INDEX,
            owner_id="stats.telemetry",
            metadata={"catalog_id": _CATALOG_ID},  # no index_name key
        )

        mock_es = MagicMock()
        mock_es.indices = MagicMock()
        mock_es.indices.exists = AsyncMock(return_value=True)
        mock_es.indices.delete = AsyncMock(return_value={"acknowledged": True})

        with patch(
            "dynastore.modules.elasticsearch.client.get_client",
            return_value=mock_es,
        ):
            outcome = await owner.cleanup_one(ref, CleanupMode.HARD)

        assert outcome == CleanupOutcome.DONE
        mock_es.indices.delete.assert_awaited_once_with(index=_EXPECTED_INDEX)


class TestStatsTelemetryOwnerRegistration:
    def test_register_owners_adds_to_registry(self) -> None:
        from dynastore.modules.stats.cascade_owner import (
            StatsTelemetryOwner,
            register_owners,
        )

        reg = CascadeCleanupRegistry()
        register_owners(reg)

        assert reg.get(StatsTelemetryOwner.owner_id) is not None
        catalog_owners = [o.owner_id for o in reg.owners_for_scope(ResourceScope.CATALOG)]
        assert StatsTelemetryOwner.owner_id in catalog_owners

    def test_register_owners_not_in_collection_scope(self) -> None:
        from dynastore.modules.stats.cascade_owner import (
            StatsTelemetryOwner,
            register_owners,
        )

        reg = CascadeCleanupRegistry()
        register_owners(reg)

        collection_owners = [
            o.owner_id for o in reg.owners_for_scope(ResourceScope.COLLECTION)
        ]
        assert StatsTelemetryOwner.owner_id not in collection_owners

    def test_owner_id_is_stable(self) -> None:
        from dynastore.modules.stats.cascade_owner import StatsTelemetryOwner
        assert StatsTelemetryOwner.owner_id == "stats.telemetry"
