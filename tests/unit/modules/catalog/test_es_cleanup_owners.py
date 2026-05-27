"""Unit tests for EsItemsIndexOwner — Chunk 2.

Covers:
- describe_scope returns expected CleanupRef shape for CATALOG scope.
- describe_scope returns [] for COLLECTION / ASSET / ITEM scopes.
- cleanup_one HARD calls es.indices.delete with correct index name.
- cleanup_one treats 404 (ignore_unavailable) as DONE (no exception raised).
- cleanup_one SOFT returns DONE and does not call delete.
- cleanup_one HARD on ES exception returns RETRY.
- register_owners adds the owner to the registry.
"""

from __future__ import annotations

from typing import Any
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
from dynastore.modules.storage.drivers.elasticsearch_private.cascade_owners import (
    EsItemsIndexOwner,
    register_owners,
)


# ---------------------------------------------------------------------------
# Fixtures / helpers
# ---------------------------------------------------------------------------


_PREFIX = "test-prefix"
_CATALOG_ID = "cat-abc"
_EXPECTED_INDEX = f"{_PREFIX}-{_CATALOG_ID}-private-items"


def _mock_es_client(raises: Exception | None = None) -> Any:
    """Return a mock ES client whose indices.delete is async."""
    client = MagicMock()
    if raises is not None:
        client.indices.delete = AsyncMock(side_effect=raises)
    else:
        client.indices.delete = AsyncMock(return_value=None)
    return client


def _patch_index_name(index_name: str):
    """Context manager that patches _get_index_name to return *index_name*."""
    return patch(
        "dynastore.modules.storage.drivers.elasticsearch_private.cascade_owners._get_index_name",
        return_value=index_name,
    )


def _patch_es_client(client: Any):
    return patch(
        "dynastore.modules.storage.drivers.elasticsearch_private.cascade_owners._get_es_client",
        return_value=client,
    )


# ---------------------------------------------------------------------------
# EsItemsIndexOwner.describe_scope
# ---------------------------------------------------------------------------


class TestEsItemsIndexOwnerDescribeScope:
    @pytest.mark.asyncio
    async def test_catalog_scope_returns_one_ref(self) -> None:
        owner = EsItemsIndexOwner()
        scope_ref = ScopeRef(scope=ResourceScope.CATALOG, catalog_id=_CATALOG_ID)

        with _patch_index_name(_EXPECTED_INDEX):
            refs = await owner.describe_scope(scope_ref, MagicMock())

        assert len(refs) == 1
        ref = refs[0]
        assert ref.kind == "es_index"
        assert ref.locator == _EXPECTED_INDEX
        assert ref.owner_id == EsItemsIndexOwner.owner_id
        assert ref.metadata["catalog_id"] == _CATALOG_ID

    @pytest.mark.asyncio
    async def test_collection_scope_returns_empty(self) -> None:
        owner = EsItemsIndexOwner()
        scope_ref = ScopeRef(
            scope=ResourceScope.COLLECTION,
            catalog_id=_CATALOG_ID,
            collection_id="col-1",
        )
        refs = await owner.describe_scope(scope_ref, MagicMock())
        assert refs == []

    @pytest.mark.asyncio
    async def test_asset_scope_returns_empty(self) -> None:
        owner = EsItemsIndexOwner()
        scope_ref = ScopeRef(scope=ResourceScope.ASSET, catalog_id=_CATALOG_ID)
        refs = await owner.describe_scope(scope_ref, MagicMock())
        assert refs == []

    @pytest.mark.asyncio
    async def test_item_scope_returns_empty(self) -> None:
        owner = EsItemsIndexOwner()
        scope_ref = ScopeRef(scope=ResourceScope.ITEM, catalog_id=_CATALOG_ID)
        refs = await owner.describe_scope(scope_ref, MagicMock())
        assert refs == []


# ---------------------------------------------------------------------------
# EsItemsIndexOwner.cleanup_one
# ---------------------------------------------------------------------------


class TestEsItemsIndexOwnerCleanupOne:
    @pytest.mark.asyncio
    async def test_hard_delete_calls_indices_delete(self) -> None:
        es = _mock_es_client()
        ref = CleanupRef(kind="es_index", locator=_EXPECTED_INDEX, owner_id=EsItemsIndexOwner.owner_id)

        with _patch_es_client(es):
            outcome = await EsItemsIndexOwner().cleanup_one(ref, CleanupMode.HARD)

        es.indices.delete.assert_awaited_once_with(
            index=_EXPECTED_INDEX, params={"ignore_unavailable": "true"}
        )
        assert outcome == CleanupOutcome.DONE

    @pytest.mark.asyncio
    async def test_hard_delete_404_is_done(self) -> None:
        """ES treats ignore_unavailable=true as success on 404 — so no exception."""
        # The driver already passes ignore_unavailable=true, meaning no exception
        # is raised for 404. Simulate a clean return.
        es = _mock_es_client(raises=None)
        ref = CleanupRef(kind="es_index", locator="nonexistent-idx", owner_id=EsItemsIndexOwner.owner_id)

        with _patch_es_client(es):
            outcome = await EsItemsIndexOwner().cleanup_one(ref, CleanupMode.HARD)

        assert outcome == CleanupOutcome.DONE

    @pytest.mark.asyncio
    async def test_hard_delete_es_exception_returns_retry(self) -> None:
        # Use a generic connection-style error — the owner catches all exceptions.
        class _FakeConnectionError(OSError):
            pass

        es = _mock_es_client(raises=_FakeConnectionError("connection refused"))
        ref = CleanupRef(kind="es_index", locator=_EXPECTED_INDEX, owner_id=EsItemsIndexOwner.owner_id)

        with _patch_es_client(es):
            outcome = await EsItemsIndexOwner().cleanup_one(ref, CleanupMode.HARD)

        assert outcome == CleanupOutcome.RETRY

    @pytest.mark.asyncio
    async def test_hard_delete_generic_exception_returns_retry(self) -> None:
        es = _mock_es_client(raises=RuntimeError("boom"))
        ref = CleanupRef(kind="es_index", locator=_EXPECTED_INDEX, owner_id=EsItemsIndexOwner.owner_id)

        with _patch_es_client(es):
            outcome = await EsItemsIndexOwner().cleanup_one(ref, CleanupMode.HARD)

        assert outcome == CleanupOutcome.RETRY

    @pytest.mark.asyncio
    async def test_soft_delete_returns_done_without_calling_delete(self) -> None:
        es = _mock_es_client()
        ref = CleanupRef(kind="es_index", locator=_EXPECTED_INDEX, owner_id=EsItemsIndexOwner.owner_id)

        with _patch_es_client(es):
            outcome = await EsItemsIndexOwner().cleanup_one(ref, CleanupMode.SOFT)

        es.indices.delete.assert_not_awaited()
        assert outcome == CleanupOutcome.DONE

    @pytest.mark.asyncio
    async def test_dry_run_returns_done_without_calling_delete(self) -> None:
        es = _mock_es_client()
        ref = CleanupRef(kind="es_index", locator=_EXPECTED_INDEX, owner_id=EsItemsIndexOwner.owner_id)

        with _patch_es_client(es):
            outcome = await EsItemsIndexOwner().cleanup_one(ref, CleanupMode.HARD, dry_run=True)

        es.indices.delete.assert_not_awaited()
        assert outcome == CleanupOutcome.DONE


# ---------------------------------------------------------------------------
# EsItemsIndexOwner.cleanup_one — DENY policy revocation (issue #1467)
# ---------------------------------------------------------------------------


class TestEsItemsIndexOwnerDenyPolicyRevocation:
    """Verify that cleanup_one revokes the DENY policy on HARD delete.

    The cascade path (EsItemsIndexOwner.cleanup_one) must call
    _revoke_deny_policy so orphan IAM DENY policies are not left behind
    after a catalog hard-delete.  This mirrors the legacy drop_storage
    ordering: delete index first, then revoke policy.
    """

    @pytest.mark.asyncio
    async def test_hard_delete_calls_revoke_deny_policy(self) -> None:
        """HARD mode: _revoke_deny_policy called after successful ES delete."""
        es = _mock_es_client()
        ref = CleanupRef(
            kind="es_index",
            locator=_EXPECTED_INDEX,
            owner_id=EsItemsIndexOwner.owner_id,
            metadata={"catalog_id": _CATALOG_ID},
        )

        with (
            _patch_es_client(es),
            patch(
                "dynastore.modules.storage.drivers.elasticsearch_private.cascade_owners._revoke_deny_policy",
                new_callable=AsyncMock,
            ) as mock_revoke,
        ):
            outcome = await EsItemsIndexOwner().cleanup_one(ref, CleanupMode.HARD)

        mock_revoke.assert_awaited_once_with(_CATALOG_ID)
        assert outcome == CleanupOutcome.DONE

    @pytest.mark.asyncio
    async def test_hard_delete_revoke_raises_still_returns_done(self) -> None:
        """HARD mode: IAM revoke failure is best-effort — outcome is DONE, not RETRY."""
        es = _mock_es_client()
        ref = CleanupRef(
            kind="es_index",
            locator=_EXPECTED_INDEX,
            owner_id=EsItemsIndexOwner.owner_id,
            metadata={"catalog_id": _CATALOG_ID},
        )

        with (
            _patch_es_client(es),
            patch(
                "dynastore.modules.storage.drivers.elasticsearch_private.cascade_owners._revoke_deny_policy",
                new_callable=AsyncMock,
                side_effect=RuntimeError("IAM unavailable"),
            ),
        ):
            outcome = await EsItemsIndexOwner().cleanup_one(ref, CleanupMode.HARD)

        assert outcome == CleanupOutcome.DONE

    @pytest.mark.asyncio
    async def test_soft_delete_does_not_call_revoke_deny_policy(self) -> None:
        """SOFT mode: _revoke_deny_policy must never be called."""
        es = _mock_es_client()
        ref = CleanupRef(
            kind="es_index",
            locator=_EXPECTED_INDEX,
            owner_id=EsItemsIndexOwner.owner_id,
            metadata={"catalog_id": _CATALOG_ID},
        )

        with (
            _patch_es_client(es),
            patch(
                "dynastore.modules.storage.drivers.elasticsearch_private.cascade_owners._revoke_deny_policy",
                new_callable=AsyncMock,
            ) as mock_revoke,
        ):
            outcome = await EsItemsIndexOwner().cleanup_one(ref, CleanupMode.SOFT)

        mock_revoke.assert_not_awaited()
        assert outcome == CleanupOutcome.DONE

    @pytest.mark.asyncio
    async def test_hard_delete_missing_catalog_id_still_returns_done(self) -> None:
        """HARD mode with no catalog_id in metadata: revoke skipped, DONE returned."""
        es = _mock_es_client()
        # CleanupRef without catalog_id in metadata
        ref = CleanupRef(
            kind="es_index",
            locator=_EXPECTED_INDEX,
            owner_id=EsItemsIndexOwner.owner_id,
        )

        with (
            _patch_es_client(es),
            patch(
                "dynastore.modules.storage.drivers.elasticsearch_private.cascade_owners._revoke_deny_policy",
                new_callable=AsyncMock,
            ) as mock_revoke,
        ):
            outcome = await EsItemsIndexOwner().cleanup_one(ref, CleanupMode.HARD)

        mock_revoke.assert_not_awaited()
        assert outcome == CleanupOutcome.DONE


# ---------------------------------------------------------------------------
# register_owners
# ---------------------------------------------------------------------------


class TestRegisterOwners:
    def test_register_adds_items_owner(self) -> None:
        reg = CascadeCleanupRegistry()
        register_owners(reg)

        assert reg.get(EsItemsIndexOwner.owner_id) is not None

    def test_register_items_owner_in_catalog_scope(self) -> None:
        reg = CascadeCleanupRegistry()
        register_owners(reg)
        catalog_owners = [o.owner_id for o in reg.owners_for_scope(ResourceScope.CATALOG)]
        assert EsItemsIndexOwner.owner_id in catalog_owners

    def test_double_register_raises_value_error(self) -> None:
        reg = CascadeCleanupRegistry()
        register_owners(reg)
        with pytest.raises(ValueError, match=EsItemsIndexOwner.owner_id):
            register_owners(reg)
