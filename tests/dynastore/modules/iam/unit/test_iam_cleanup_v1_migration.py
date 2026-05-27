"""Tests for the IAM cleanup v1 migration (#1345).

Covers:
1. Migration happy path — DDL drops are executed and sentinel is written.
2. Idempotency — second run is a no-op (sentinel already present).
3. ``_derive_partition_key`` never returns ``policies_sysadmin`` or ``policies_auth``.
4. ``LIST_ROLES`` orders by ``name ASC`` and contains no ``level`` reference.
5. ``Role`` model has no ``level`` field.
6. ``parent_roles`` deprecation marker is present on the ``Role`` model.
"""
from __future__ import annotations

import inspect
from contextlib import asynccontextmanager
from typing import Any, Dict, List
from unittest.mock import AsyncMock, MagicMock, patch

import pytest


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------

def _make_async_conn() -> AsyncMock:
    """Return an async-capable fake connection."""
    conn = AsyncMock()
    conn.execute = AsyncMock(return_value=None)
    return conn


@asynccontextmanager
async def _txn(_engine: Any):
    yield _make_async_conn()


# ---------------------------------------------------------------------------
# 1. Migration happy path
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_migration_runs_ddl_and_writes_sentinel():
    """Happy path: DDL statements are executed and the sentinel is inserted."""
    from dynastore.modules.iam.migrations import iam_cleanup_v1 as m

    insert_calls: List[Dict[str, Any]] = []

    async def _select_sentinel(_resource: Any, **kw: Any) -> Any:
        return None  # not yet applied

    async def _insert_sentinel(_resource: Any, **kw: Any) -> Any:
        insert_calls.append(kw)
        return None

    with patch.object(m, "managed_transaction", side_effect=_txn), \
         patch.object(m.SELECT_SENTINEL, "execute", _select_sentinel), \
         patch.object(m.INSERT_SENTINEL, "execute", _insert_sentinel), \
         patch.object(m, "_run_ddl_steps") as mock_ddl:
        mock_ddl.return_value = None

        await m.run_migration(MagicMock())

    mock_ddl.assert_called_once()
    assert len(insert_calls) == 1
    assert insert_calls[0]["preset_name"] == m._SENTINEL_KEY
    assert insert_calls[0]["scope_key"] == m._SCOPE_KEY


# ---------------------------------------------------------------------------
# 2. Idempotency — second run is a no-op
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_migration_is_no_op_when_sentinel_present():
    """When the sentinel row exists, migration skips all DDL and no new row is inserted."""
    from dynastore.modules.iam.migrations import iam_cleanup_v1 as m

    insert_calls: List[Dict[str, Any]] = []

    async def _select_sentinel(_resource: Any, **kw: Any) -> Any:
        return (1,)  # already applied

    async def _insert_sentinel(_resource: Any, **kw: Any) -> Any:
        insert_calls.append(kw)
        return None

    with patch.object(m, "managed_transaction", side_effect=_txn), \
         patch.object(m.SELECT_SENTINEL, "execute", _select_sentinel), \
         patch.object(m.INSERT_SENTINEL, "execute", _insert_sentinel), \
         patch.object(m, "_run_ddl_steps") as mock_ddl:
        mock_ddl.return_value = None

        await m.run_migration(MagicMock())

    mock_ddl.assert_not_called()
    assert insert_calls == [], "INSERT must not fire on repeat run"


# ---------------------------------------------------------------------------
# 3. _derive_partition_key never returns policies_sysadmin or policies_auth
# ---------------------------------------------------------------------------

def test_derive_partition_key_never_returns_dead_partitions():
    """``_derive_partition_key`` must never emit the dead partition names."""
    from dynastore.modules.iam.policies import PolicyService

    # Build a minimal PolicyService (no DB needed — testing a pure function).
    svc = PolicyService.__new__(PolicyService)
    svc._role_config = MagicMock()

    dead_partitions = {"policies_sysadmin", "policies_auth"}

    # Probe many paths; none should yield a dead partition name.
    probe_paths = [
        "/sysadmin/resources",
        "/auth/tokens",
        "/admin/catalogs/foo",
        "/docs/openapi.json",
        "/",
        "/features/catalogs/bar/collections/baz",
        "/sysadmin",
        "/auth",
        "sysadmin",
        "auth",
        "/some/deeply/nested/path",
    ]
    for path in probe_paths:
        key = svc._derive_partition_key(path)
        assert key not in dead_partitions, (
            f"_derive_partition_key({path!r}) returned {key!r}, "
            f"which is a dead partition name"
        )


# ---------------------------------------------------------------------------
# 4. LIST_ROLES pins ORDER BY name ASC and contains no level reference
# ---------------------------------------------------------------------------

def test_list_roles_orders_by_name_asc():
    """LIST_ROLES SQL must use ORDER BY name ASC (not level)."""
    from dynastore.modules.iam.iam_queries import LIST_ROLES

    sql: str = LIST_ROLES.template.upper()
    assert "ORDER BY NAME ASC" in sql, (
        f"LIST_ROLES must order by name ASC; got: {LIST_ROLES.template!r}"
    )


def test_list_roles_contains_no_level_reference():
    """LIST_ROLES SQL must not reference the dropped level column."""
    from dynastore.modules.iam.iam_queries import LIST_ROLES

    sql: str = LIST_ROLES.template.lower()
    assert "level" not in sql, (
        f"LIST_ROLES must not reference the dropped level column; got: {LIST_ROLES.template!r}"
    )


# ---------------------------------------------------------------------------
# 5. Role model has no level field
# ---------------------------------------------------------------------------

def test_role_model_has_no_level_field():
    """The Role model must not declare a ``level`` field."""
    from dynastore.models.auth_models import Role

    assert "level" not in Role.model_fields, (
        "Role.level has been removed from the DB schema; "
        "it must also be absent from the Pydantic model"
    )


# ---------------------------------------------------------------------------
# 6. parent_roles deprecation marker is present on Role model
# ---------------------------------------------------------------------------

def test_role_parent_roles_marked_deprecated():
    """``parent_roles`` on Role must be marked deprecated (not removed)."""
    from dynastore.models.auth_models import Role

    # The field must still exist (not yet removed).
    assert "parent_roles" in Role.model_fields, (
        "parent_roles must still be present on Role (deprecation, not removal)"
    )

    # Pydantic v2 stores deprecation on the FieldInfo object.
    field_info = Role.model_fields["parent_roles"]
    deprecated_flag = getattr(field_info, "deprecated", None)
    assert deprecated_flag, (
        "Role.parent_roles must carry a deprecation marker "
        "(Field(deprecated=True) or a non-empty deprecation string)"
    )
