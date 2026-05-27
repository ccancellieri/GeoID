"""Tests for the iam_baseline backfill migration.

Three paths:
1. Happy path — clean DB, migration inserts the audit row with correct descriptor.
2. Divergence path — DB has modified policies, descriptor reflects actual state.
3. Idempotency — second run is a no-op.
"""
from __future__ import annotations

import json
from typing import Any, Dict, List, Optional
from unittest.mock import AsyncMock, MagicMock, patch, call
import pytest


# ---------------------------------------------------------------------------
# Mock infrastructure
# ---------------------------------------------------------------------------

def _mock_row(**fields: Any) -> MagicMock:
    row = MagicMock()
    row._mapping = fields
    return row


def _make_mocks(
    *,
    existing_row: Optional[Any] = None,
    present_policy_ids: Optional[List[str]] = None,
    present_role_names: Optional[List[str]] = None,
):
    """Build the mocked engine, policy_storage, iam_storage."""
    if present_policy_ids is None:
        present_policy_ids = [
            "admin_authorization_api",
            "self_service_authorization_api",
            "admin_catalog_access",
            "catalog_preset_delegation",
        ]
    if present_role_names is None:
        present_role_names = ["admin", "sysadmin", "user"]

    engine = MagicMock()

    policy_storage = MagicMock()
    iam_storage = MagicMock()

    async def _get_policy(pid: str, schema: str, conn: Any, partition_key: str) -> Optional[MagicMock]:
        if pid in present_policy_ids:
            return MagicMock()
        return None

    async def _get_role(rname: str, schema: str, conn: Any) -> Optional[MagicMock]:
        if rname in present_role_names:
            return MagicMock()
        return None

    policy_storage.get_policy = _get_policy
    iam_storage.get_role = _get_role

    return engine, policy_storage, iam_storage


# ---------------------------------------------------------------------------
# Test helpers for patching the DQLQuery calls
# ---------------------------------------------------------------------------

class _FakeConn:
    """Fake async connection with call tracking."""

    def __init__(self, *, existing_row: Optional[Any]):
        self._existing = existing_row
        self.inserts: List[Dict[str, Any]] = []

    async def execute(self, stmt: Any, **kwargs: Any) -> Any:
        return None


def _make_conn(existing_row: Optional[Any]) -> _FakeConn:
    return _FakeConn(existing_row=existing_row)


# ---------------------------------------------------------------------------
# Tests — mock at the DQLQuery level
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_backfill_inserts_on_clean_db():
    """Happy path: no existing row → inserts applied row with full descriptor."""
    from dynastore.modules.iam.migrations import backfill_iam_baseline_audit as m

    engine, policy_storage, iam_storage = _make_mocks()

    inserted: Dict[str, Any] = {}

    async def _select(resource: Any, **kw: Any) -> Optional[Any]:
        return None  # No existing row.

    async def _insert(resource: Any, **kw: Any) -> Optional[Any]:
        inserted.update(kw)
        return None

    with patch.object(m, "managed_transaction") as mock_txn, \
         patch.object(m, "DQLQuery") as mock_dql:
        # Patch managed_transaction to yield a fake conn directly.
        from contextlib import asynccontextmanager

        @asynccontextmanager
        async def _ctx_manager(eng: Any):
            yield MagicMock()

        mock_txn.side_effect = _ctx_manager

        # The function creates its own SELECT_EXISTING and INSERT_APPLIED queries.
        # We can't easily intercept DQLQuery creation after the fact — the queries
        # are module-level objects. Instead, patch the execute calls directly on
        # the module's query objects after import.

    # Re-approach: patch the module-level query objects directly.
    async def _select_none(resource: Any, **kw: Any) -> Optional[Any]:
        return None

    async def _insert_capture(resource: Any, **kw: Any) -> Optional[Any]:
        inserted.update(kw)
        return None

    with patch(
        "dynastore.modules.iam.migrations.backfill_iam_baseline_audit.managed_transaction",
    ) as mock_txn:
        from contextlib import asynccontextmanager

        @asynccontextmanager
        async def _ctx(eng: Any):
            yield MagicMock()

        mock_txn.side_effect = _ctx

        with patch(
            "dynastore.modules.iam.migrations.backfill_iam_baseline_audit.SELECT_EXISTING"
        ) as mock_select, patch(
            "dynastore.modules.iam.migrations.backfill_iam_baseline_audit.INSERT_APPLIED"
        ) as mock_insert:
            mock_select.execute = _select_none
            mock_insert.execute = _insert_capture

            await m.run_backfill(engine, policy_storage, iam_storage)

    assert "preset_name" in inserted
    assert inserted["preset_name"] == "iam_baseline"
    revoke = json.loads(inserted["revoke_descriptor"])
    # All standard policies present.
    assert "admin_authorization_api" in revoke["policy_ids"]
    assert "self_service_authorization_api" in revoke["policy_ids"]


@pytest.mark.asyncio
async def test_backfill_idempotent_when_row_exists():
    """When audit row already exists, migration is a no-op."""
    from dynastore.modules.iam.migrations import backfill_iam_baseline_audit as m

    engine, policy_storage, iam_storage = _make_mocks()
    inserted: Dict[str, Any] = {}

    existing_row = _mock_row(preset_name="iam_baseline", scope_key="platform", state="applied")

    async def _select_found(resource: Any, **kw: Any) -> Optional[Any]:
        return existing_row

    async def _insert_capture(resource: Any, **kw: Any) -> Optional[Any]:
        inserted.update(kw)
        return None

    with patch(
        "dynastore.modules.iam.migrations.backfill_iam_baseline_audit.managed_transaction"
    ) as mock_txn:
        from contextlib import asynccontextmanager

        @asynccontextmanager
        async def _ctx(eng: Any):
            yield MagicMock()

        mock_txn.side_effect = _ctx

        with patch(
            "dynastore.modules.iam.migrations.backfill_iam_baseline_audit.SELECT_EXISTING"
        ) as mock_select, patch(
            "dynastore.modules.iam.migrations.backfill_iam_baseline_audit.INSERT_APPLIED"
        ) as mock_insert:
            mock_select.execute = _select_found
            mock_insert.execute = _insert_capture

            await m.run_backfill(engine, policy_storage, iam_storage)

    # INSERT must not have been called.
    assert inserted == {}, "INSERT was called despite existing row"


@pytest.mark.asyncio
async def test_backfill_divergence_path():
    """If DB has fewer policies than canonical, descriptor reflects actual state."""
    from dynastore.modules.iam.migrations import backfill_iam_baseline_audit as m

    # Simulated divergence: admin_authorization_api is missing from DB.
    engine, policy_storage, iam_storage = _make_mocks(
        present_policy_ids=[
            "self_service_authorization_api",
            # admin_authorization_api intentionally absent
        ],
        present_role_names=["admin"],
    )
    inserted: Dict[str, Any] = {}

    async def _select_none(resource: Any, **kw: Any) -> Optional[Any]:
        return None

    async def _insert_capture(resource: Any, **kw: Any) -> Optional[Any]:
        inserted.update(kw)
        return None

    with patch(
        "dynastore.modules.iam.migrations.backfill_iam_baseline_audit.managed_transaction"
    ) as mock_txn:
        from contextlib import asynccontextmanager

        @asynccontextmanager
        async def _ctx(eng: Any):
            yield MagicMock()

        mock_txn.side_effect = _ctx

        with patch(
            "dynastore.modules.iam.migrations.backfill_iam_baseline_audit.SELECT_EXISTING"
        ) as mock_select, patch(
            "dynastore.modules.iam.migrations.backfill_iam_baseline_audit.INSERT_APPLIED"
        ) as mock_insert:
            mock_select.execute = _select_none
            mock_insert.execute = _insert_capture

            await m.run_backfill(engine, policy_storage, iam_storage)

    revoke = json.loads(inserted["revoke_descriptor"])
    # Only the policies actually in DB should appear.
    assert "self_service_authorization_api" in revoke["policy_ids"]
    assert "admin_authorization_api" not in revoke["policy_ids"]
    # Roles reflect actual state too.
    assert "admin" in revoke["role_names"]
    assert "sysadmin" not in revoke["role_names"]
    assert "user" not in revoke["role_names"]
