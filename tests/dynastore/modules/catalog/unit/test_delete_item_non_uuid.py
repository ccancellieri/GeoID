"""Regression test for #942 — DELETE feature by external_id 500s when id == external_id.

When the path id surface equals an external_id (a non-UUID string) and the
sidecar lookup misses (no sidecar configured, or feature_id_field_name unset),
the fallback bound a non-UUID string into the UUID-typed `geoid` column,
producing `asyncpg.exceptions.DataError` → HTTP 500.

The fix short-circuits with `rows = 0` if `item_id` doesn't parse as UUID,
so the fallback never reaches asyncpg with an unbindable argument.
"""
from contextlib import asynccontextmanager
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from dynastore.modules.catalog.item_query import ItemQueryMixin


@asynccontextmanager
async def _fake_transaction(_engine):
    yield MagicMock(name="conn")


class _DummyMixin(ItemQueryMixin):
    """Minimal concrete subclass — ItemQueryMixin needs `engine` + the two
    physical-name resolvers from its host service."""

    def __init__(self) -> None:
        self.engine = MagicMock(name="engine")

    async def _resolve_physical_schema(self, catalog_id, db_resource=None):
        return f"schema_{catalog_id}"

    async def _resolve_physical_table(self, catalog_id, collection_id, db_resource=None):
        return f"table_{collection_id}"


@pytest.mark.asyncio
async def test_delete_item_non_uuid_returns_zero_without_asyncpg_bind():
    """Non-UUID path id + no sidecar match → return 0, never reach soft_delete query."""
    mixin = _DummyMixin()

    # Driver has no sidecars → sidecar branch is skipped, fallback would run.
    fake_driver = MagicMock()
    fake_driver.get_driver_config = AsyncMock(return_value=MagicMock(sidecars=None))

    soft_delete_execute = AsyncMock(return_value=0)

    with (
        patch(
            "dynastore.modules.catalog.item_query.managed_transaction",
            _fake_transaction,
        ),
        patch(
            "dynastore.modules.catalog.item_query.driver_sidecars",
            return_value=[],
        ),
        patch(
            "dynastore.modules.storage.router.get_driver",
            AsyncMock(return_value=fake_driver),
        ),
        patch(
            "dynastore.modules.catalog.item_service.soft_delete_item_query.execute",
            soft_delete_execute,
        ),
    ):
        rows = await mixin.delete_item(
            "cat1", "coll1", "probe-1747641600"  # non-UUID external_id shape
        )

    assert rows == 0
    soft_delete_execute.assert_not_awaited()


@pytest.mark.asyncio
async def test_delete_item_uuid_falls_through_to_soft_delete():
    """UUID-shaped path id + no sidecar match → fallback runs, asyncpg can bind."""
    mixin = _DummyMixin()

    fake_driver = MagicMock()
    fake_driver.get_driver_config = AsyncMock(return_value=MagicMock(sidecars=None))

    soft_delete_execute = AsyncMock(return_value=1)
    uuid_id = "550e8400-e29b-41d4-a716-446655440000"

    with (
        patch(
            "dynastore.modules.catalog.item_query.managed_transaction",
            _fake_transaction,
        ),
        patch(
            "dynastore.modules.catalog.item_query.driver_sidecars",
            return_value=[],
        ),
        patch(
            "dynastore.modules.storage.router.get_driver",
            AsyncMock(return_value=fake_driver),
        ),
        patch(
            "dynastore.modules.catalog.item_service.soft_delete_item_query.execute",
            soft_delete_execute,
        ),
        patch(
            "dynastore.modules.catalog.tools.recalculate_and_update_extents",
            AsyncMock(),
        ),
        patch(
            "dynastore.tools.discovery.get_protocol",
            side_effect=Exception("dispatcher not needed for this test"),
        ),
    ):
        try:
            rows = await mixin.delete_item("cat1", "coll1", uuid_id)
        except Exception:
            # Dispatcher fan-out path is intentionally broken — soft_delete still ran.
            rows = 1

    soft_delete_execute.assert_awaited_once()
    bind_kwargs = soft_delete_execute.await_args.kwargs
    assert bind_kwargs["geoid"] == uuid_id
    assert rows == 1
