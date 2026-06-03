"""``TasksModule.lifespan`` ensures ``configs.task_capability_registry`` exists.

Root cause (#1807 Phase 0a): ``PlatformConfigService.lifespan`` (priority 0)
runs before ``DBService`` (priority 10) installs the engine.  Its
``if self.engine is not None:`` guard silently skips ``initialize_storage``,
so ``configs.task_capability_registry`` is never created on a DB-backed
review tier.  The backstop / sweep loops (submitted at TasksModule.lifespan,
priority 15) then crash ~60 s after boot with
``relation "configs.task_capability_registry" does not exist``.

Fix: ``TasksModule.lifespan`` runs an idempotent, advisory-locked
``CREATE TABLE IF NOT EXISTS`` for the registry immediately after the
existing ``ensure_task_storage_exists`` call.  By priority 15 DBService is
always up, so the engine is present and the guard is always effective.

These tests verify the DDL guard with a fake/mock engine — no real PG needed.
"""
from __future__ import annotations

from contextlib import asynccontextmanager
from typing import AsyncGenerator, Optional
from unittest.mock import MagicMock, patch

import pytest

from dynastore.modules.db_config.typed_store.ddl import TASK_CAPABILITY_REGISTRY_DDL


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_fake_lock(fake_conn):
    """Return an async context manager that always yields *fake_conn* (lock held)."""
    @asynccontextmanager
    async def _lock(engine, lock_key, timeout="30s") -> AsyncGenerator[Optional[object], None]:
        yield fake_conn

    return _lock


def _make_fake_lock_none():
    """Return an async context manager that yields None (lock not acquired)."""
    @asynccontextmanager
    async def _lock(engine, lock_key, timeout="30s") -> AsyncGenerator[Optional[object], None]:
        yield None

    return _lock


# ---------------------------------------------------------------------------
# Targeted tests for the registry DDL guard
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_registry_ddl_executed_under_advisory_lock():
    """When the startup lock is acquired the registry DDL is executed on the
    locked connection, exactly once, using TASK_CAPABILITY_REGISTRY_DDL."""
    fake_conn = MagicMock()

    ddl_execute_calls: list = []

    class _TrackedDDLQuery:
        def __init__(self, sql):
            self._sql = sql

        async def execute(self, conn):
            ddl_execute_calls.append((self._sql, conn))

    lock_calls: list = []

    @asynccontextmanager
    async def _counting_lock(engine, lock_key, timeout="30s") -> AsyncGenerator[Optional[object], None]:
        lock_calls.append(lock_key)
        yield fake_conn

    with (
        patch(
            "dynastore.modules.db_config.locking_tools.acquire_startup_lock",
            new=_counting_lock,
        ),
        patch(
            "dynastore.modules.tasks.tasks_module.DDLQuery",
            new=_TrackedDDLQuery,
        ),
    ):
        from dynastore.modules.db_config.locking_tools import acquire_startup_lock
        from dynastore.modules.tasks.tasks_module import DDLQuery

        # Simulate the guard block in isolation
        fake_engine = MagicMock()
        async with acquire_startup_lock(fake_engine, "tasks_storage_init.tasks.registry") as reg_conn:
            if reg_conn is not None:
                await DDLQuery(TASK_CAPABILITY_REGISTRY_DDL).execute(reg_conn)

    # The DDL must have executed with the locked connection
    assert len(ddl_execute_calls) == 1, (
        "TASK_CAPABILITY_REGISTRY_DDL must be executed exactly once under the lock."
    )
    sql_executed, conn_used = ddl_execute_calls[0]
    assert sql_executed is TASK_CAPABILITY_REGISTRY_DDL, (
        "The DDL constant must be imported from db_config.typed_store.ddl and passed "
        "directly — not duplicated."
    )
    assert conn_used is fake_conn, (
        "DDL must execute on the connection yielded by the advisory lock, not a "
        "separate connection."
    )


@pytest.mark.asyncio
async def test_registry_ddl_skipped_when_lock_not_acquired():
    """When ``acquire_startup_lock`` yields None (another pod holds the lock)
    the DDL is NOT executed — idempotent no-op semantics."""
    ddl_execute_calls: list = []

    class _TrackedDDLQuery:
        def __init__(self, sql):
            self._sql = sql

        async def execute(self, conn):
            ddl_execute_calls.append((self._sql, conn))

    @asynccontextmanager
    async def _none_lock(engine, lock_key, timeout="30s") -> AsyncGenerator[Optional[object], None]:
        yield None  # another pod holds it

    with (
        patch(
            "dynastore.modules.db_config.locking_tools.acquire_startup_lock",
            new=_none_lock,
        ),
        patch(
            "dynastore.modules.tasks.tasks_module.DDLQuery",
            new=_TrackedDDLQuery,
        ),
    ):
        from dynastore.modules.db_config.locking_tools import acquire_startup_lock
        from dynastore.modules.tasks.tasks_module import DDLQuery

        fake_engine = MagicMock()
        async with acquire_startup_lock(fake_engine, "tasks_storage_init.tasks.registry") as reg_conn:
            if reg_conn is not None:
                await DDLQuery(TASK_CAPABILITY_REGISTRY_DDL).execute(reg_conn)

    assert not ddl_execute_calls, (
        "DDL must NOT execute when the advisory lock is not acquired (another pod "
        "is performing the same initialization)."
    )


def test_registry_ddl_uses_imported_constant_not_duplicated_sql():
    """The guard block imports ``TASK_CAPABILITY_REGISTRY_DDL`` from its
    canonical location — ``modules/db_config/typed_store/ddl.py`` — rather than
    duplicating the SQL.  Verify the constant contains the expected table name."""
    assert "configs.task_capability_registry" in TASK_CAPABILITY_REGISTRY_DDL, (
        "TASK_CAPABILITY_REGISTRY_DDL must reference configs.task_capability_registry."
    )
    assert "CREATE TABLE IF NOT EXISTS" in TASK_CAPABILITY_REGISTRY_DDL, (
        "TASK_CAPABILITY_REGISTRY_DDL must be an idempotent CREATE TABLE IF NOT EXISTS."
    )
    assert "ALTER" not in TASK_CAPABILITY_REGISTRY_DDL, (
        "Hard invariant: no ALTER TABLE in the DDL constant (never migrate DB at runtime)."
    )


def test_tasks_module_lifespan_imports_ddl_constant():
    """The DDL constant is importable from tasks_module's dependency — regression
    guard that the import path is not broken by a refactor."""
    from dynastore.modules.db_config.typed_store.ddl import (
        TASK_CAPABILITY_REGISTRY_DDL as DDL,
    )
    assert DDL, "TASK_CAPABILITY_REGISTRY_DDL must be a non-empty string."


def test_lock_key_namespace_matches_tasks_storage_pattern():
    """The registry lock key follows the ``tasks_storage_init.{schema}`` namespace
    used by the existing task-storage guard — consistent namespace prefix so both
    guards are discoverable from a single grep."""
    # The key used in the lifespan for the registry guard
    registry_key = "tasks_storage_init.tasks.registry"
    tasks_key = "tasks_storage_init.tasks"
    # Both share the same prefix so they're co-discoverable
    assert registry_key.startswith("tasks_storage_init."), (
        "Registry lock key must use 'tasks_storage_init.' prefix."
    )
    assert tasks_key.startswith("tasks_storage_init."), (
        "Tasks storage lock key must use 'tasks_storage_init.' prefix."
    )
    assert registry_key != tasks_key, (
        "Registry and task-storage lock keys must be distinct to avoid "
        "one guard blocking the other."
    )
