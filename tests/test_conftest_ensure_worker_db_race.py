"""Regression tests for #894 — _ensure_worker_db source-DB race.

The defect: pg_terminate_backend is one-shot, so any external client
(dev stack, pgAdmin, leftover sessions) reconnecting between the
terminate and the CREATE DATABASE TEMPLATE call crashed the whole pytest
session with asyncpg.ObjectInUseError. The fix wraps the terminate+create
in a bounded retry loop and, on exhaustion, raises a RuntimeError that
points the operator at the existing clear-DB workflows (no source-DB
mutation in the fixture itself — operator clears state with
tests/dynastore/test_utils/cleanup_db.py locally or with
packages/core/src/dynastore/scripts/db_reset.sh in review env, then
re-runs).
"""

from __future__ import annotations

import inspect

import asyncpg
import pytest

from tests import conftest as ensure_mod


@pytest.mark.asyncio
async def test_retry_succeeds_after_two_object_in_use_then_creates():
    """First two CREATE calls raise ObjectInUseError, third succeeds —
    helper must retry, terminate before each CREATE, and return."""
    calls: list[tuple] = []
    create_attempts = {"n": 0}

    async def execute(query: str, *args):
        calls.append((query, args))
        if query.startswith("SELECT pg_terminate_backend"):
            return None
        if query.startswith("CREATE DATABASE"):
            create_attempts["n"] += 1
            if create_attempts["n"] < 3:
                raise asyncpg.exceptions.ObjectInUseError(
                    "source database \"src\" is being accessed by other users"
                )
            return None
        return None

    await ensure_mod._create_db_template_with_retry(
        execute, "src", "src_gw0", attempts=5, backoff=0.0,
    )

    terminate_calls = [c for c in calls if c[0].startswith("SELECT pg_terminate_backend")]
    create_calls = [c for c in calls if c[0].startswith("CREATE DATABASE")]
    assert len(terminate_calls) == 3, "must re-terminate before each CREATE retry"
    assert len(create_calls) == 3
    assert create_attempts["n"] == 3


@pytest.mark.asyncio
async def test_retry_exhausts_and_raises_runtimeerror_with_operator_hints():
    """When every CREATE attempt fails, helper raises RuntimeError that
    names both DBs, the attempt count, pg_stat_activity, and points at
    the local + review-env clear-DB workflows."""
    async def execute(query: str, *args):
        if query.startswith("CREATE DATABASE"):
            raise asyncpg.exceptions.ObjectInUseError(
                "source database \"src\" is being accessed by other users"
            )
        return None

    with pytest.raises(RuntimeError) as exc_info:
        await ensure_mod._create_db_template_with_retry(
            execute, "src", "src_gw0", attempts=3, backoff=0.0,
        )
    msg = str(exc_info.value)
    assert "src" in msg and "src_gw0" in msg
    assert "3 attempts" in msg
    assert "pg_stat_activity" in msg
    # Resolution path must point at the existing clear-DB workflows.
    assert "cleanup_db" in msg, "must point local operators at cleanup_db.py"
    assert "db_reset.sh" in msg, "must point review-env operators at db_reset.sh"


def test_ensure_worker_db_does_not_mutate_source_db_settings():
    """Source-level guard: _ensure_worker_db must NOT issue ALTER DATABASE
    on the source DB. The fix relies on bounded retry + operator-driven
    cleanup of external clients, not on toggling source-DB settings (#894
    direction per maintainer: use the existing clear-DB workflows)."""
    src = inspect.getsource(ensure_mod._ensure_worker_db)
    assert "_create_db_template_with_retry" in src, (
        "must call the bounded retry helper, not a single CREATE"
    )
    assert "ALTER DATABASE" not in src, (
        "must not ALTER DATABASE on the source — operator clears state via "
        "cleanup_db.py / db_reset.sh before launching the session"
    )
    assert "ALLOW_CONNECTIONS" not in src, (
        "must not toggle ALLOW_CONNECTIONS on the source DB"
    )
