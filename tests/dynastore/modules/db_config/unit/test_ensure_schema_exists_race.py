"""Regression pin (un-fao/GeoID#821): ``ensure_schema_exists`` swallows
the ``pg_namespace_nspname_index`` UniqueViolation that fires when a peer
worker wins the cold-start ``CREATE SCHEMA`` race after the DDLExecutor's
post-wait re-check misses the peer's commit (suspected MVCC snapshot age).

The schema-already-exists post-condition is satisfied either way; the
defensive swallow keeps multi-worker startup deterministic. Any *other*
UniqueViolation (different constraint name) must still propagate —
swallowing too widely would mask genuine schema-init bugs.
"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from dynastore.modules.db_config.exceptions import UniqueViolationError
from dynastore.modules.db_config.maintenance_tools import ensure_schema_exists


def _unique_violation(constraint: str) -> UniqueViolationError:
    inner = SimpleNamespace()
    setattr(inner, "constraint_name", constraint)
    return UniqueViolationError(
        f"duplicate key value violates unique constraint \"{constraint}\"",
        original_exception=inner,
    )


@pytest.mark.asyncio
async def test_swallows_pg_namespace_nspname_index_uniqueviolation() -> None:
    """Cold-start race: peer created the schema mid-flight. We treat as success."""
    conn = MagicMock()
    with patch(
        "dynastore.modules.db_config.maintenance_tools.DDLQuery"
    ) as mock_ddl_cls:
        execute_mock = AsyncMock(side_effect=_unique_violation("pg_namespace_nspname_index"))
        mock_ddl_cls.return_value.execute = execute_mock

        # Must NOT raise — the schema's post-condition is satisfied by the peer.
        await ensure_schema_exists(conn, "iam")

        execute_mock.assert_awaited_once_with(conn)


@pytest.mark.asyncio
async def test_propagates_other_uniqueviolations() -> None:
    """A UniqueViolation on a different constraint is NOT the race we tolerate;
    it must surface so the underlying schema-init bug is visible."""
    conn = MagicMock()
    with patch(
        "dynastore.modules.db_config.maintenance_tools.DDLQuery"
    ) as mock_ddl_cls:
        mock_ddl_cls.return_value.execute = AsyncMock(
            side_effect=_unique_violation("some_other_unique_index")
        )

        with pytest.raises(UniqueViolationError):
            await ensure_schema_exists(conn, "iam")


@pytest.mark.asyncio
async def test_propagates_uniqueviolation_with_no_constraint_metadata() -> None:
    """If the original exception lacks ``constraint_name`` (untyped driver
    error), we don't know it's the safe race — re-raise."""
    conn = MagicMock()
    bad = UniqueViolationError("opaque")  # no original_exception attached
    with patch(
        "dynastore.modules.db_config.maintenance_tools.DDLQuery"
    ) as mock_ddl_cls:
        mock_ddl_cls.return_value.execute = AsyncMock(side_effect=bad)

        with pytest.raises(UniqueViolationError):
            await ensure_schema_exists(conn, "iam")


@pytest.mark.asyncio
async def test_happy_path_no_exception() -> None:
    """When DDLQuery.execute succeeds, ensure_schema_exists returns cleanly."""
    conn = MagicMock()
    with patch(
        "dynastore.modules.db_config.maintenance_tools.DDLQuery"
    ) as mock_ddl_cls:
        mock_ddl_cls.return_value.execute = AsyncMock(return_value=None)

        await ensure_schema_exists(conn, "iam")
