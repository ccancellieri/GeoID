"""Unit tests for ``TilePGPreseedStorage.delete_tile`` (#1292 mark-stale).

Mock-based (mirrors ``tests/dynastore/extensions/tiles/test_tiles_db_unit.py``):
verifies the per-tile delete issues a DELETE, is idempotent when the preseed
table is absent, and reports failure (rather than raising) on a real error.
"""
from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest


def _make_storage():
    from dynastore.modules.tiles.tiles_module import TilePGPreseedStorage

    storage = TilePGPreseedStorage.__new__(TilePGPreseedStorage)
    storage.engine = MagicMock()
    storage._get_schema = AsyncMock(return_value="s_cat")  # type: ignore[attr-defined]
    return storage


@pytest.mark.asyncio
async def test_delete_tile_issues_delete():
    storage = _make_storage()
    with patch(
        "dynastore.modules.tiles.tiles_module.managed_transaction"
    ) as mtx, patch(
        "dynastore.modules.tiles.tiles_module.DQLQuery"
    ) as MockDQL, patch(
        "dynastore.modules.tiles.tiles_module.cache_invalidate"
    ):
        mtx.return_value.__aenter__ = AsyncMock(return_value=MagicMock())
        mtx.return_value.__aexit__ = AsyncMock(return_value=False)
        MockDQL.return_value.execute = AsyncMock(return_value=1)

        ok = await storage.delete_tile("cat", "col", "WebMercatorQuad", 5, 1, 2, "mvt")

        assert ok is True
        # A DELETE statement was constructed.
        sql = MockDQL.call_args.args[0]
        assert "DELETE FROM" in sql and "preseeded_tiles" in sql


@pytest.mark.asyncio
async def test_delete_tile_idempotent_when_table_absent():
    storage = _make_storage()
    with patch(
        "dynastore.modules.tiles.tiles_module.managed_transaction"
    ) as mtx, patch(
        "dynastore.modules.tiles.tiles_module.DQLQuery"
    ) as MockDQL:
        mtx.return_value.__aenter__ = AsyncMock(return_value=MagicMock())
        mtx.return_value.__aexit__ = AsyncMock(return_value=False)
        # Simulate "relation does not exist" (42P01).
        MockDQL.return_value.execute = AsyncMock(
            side_effect=Exception("relation does not exist (42P01)")
        )

        ok = await storage.delete_tile("cat", "col", "WebMercatorQuad", 5, 1, 2, "mvt")
        # Missing table → nothing to invalidate → idempotent success.
        assert ok is True


@pytest.mark.asyncio
async def test_delete_tile_reports_failure_without_raising():
    storage = _make_storage()
    with patch(
        "dynastore.modules.tiles.tiles_module.managed_transaction"
    ) as mtx, patch(
        "dynastore.modules.tiles.tiles_module.DQLQuery"
    ) as MockDQL:
        mtx.return_value.__aenter__ = AsyncMock(return_value=MagicMock())
        mtx.return_value.__aexit__ = AsyncMock(return_value=False)
        MockDQL.return_value.execute = AsyncMock(
            side_effect=Exception("connection reset")
        )

        ok = await storage.delete_tile("cat", "col", "WebMercatorQuad", 5, 1, 2, "mvt")
        # A genuine error → False (let the drain retry), never raises.
        assert ok is False


# ---------------------------------------------------------------------------
# delete_tile_variants — #1292 SHOULD-FIX 2 (formats) + 3 (cache-id suffix)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_delete_tile_variants_matches_all_cache_id_shapes_and_formats():
    """The DELETE must catch bare, @hash, and multi-collection cache ids across
    every served format in a single statement."""
    storage = _make_storage()
    with patch(
        "dynastore.modules.tiles.tiles_module.managed_transaction"
    ) as mtx, patch(
        "dynastore.modules.tiles.tiles_module.DQLQuery"
    ) as MockDQL, patch(
        "dynastore.modules.tiles.tiles_module.cache_invalidate"
    ):
        mtx.return_value.__aenter__ = AsyncMock(return_value=MagicMock())
        mtx.return_value.__aexit__ = AsyncMock(return_value=False)
        MockDQL.return_value.execute = AsyncMock(return_value=3)

        ok = await storage.delete_tile_variants(
            "cat", "region6", "WebMercatorQuad", 5, 1, 2, ["mvt", "pbf"],
        )
        assert ok is True

        sql = MockDQL.call_args.args[0]
        assert "DELETE FROM" in sql and "preseeded_tiles" in sql
        # Format set matched via ANY(...), not a single hardcoded "mvt".
        assert "format = ANY(:formats)" in sql
        # Exact + parameterized (@hash) + multi-collection positions.
        for marker in (":cid", ":p_param", ":p_head", ":p_tail", ":p_tail_hash", ":p_mid"):
            assert marker in sql

        kwargs = MockDQL.return_value.execute.call_args.kwargs
        assert kwargs["formats"] == ["mvt", "pbf"]
        assert kwargs["cid"] == "region6"
        assert kwargs["p_param"] == "region6@%"
        assert kwargs["p_head"] == "region6,%"
        assert kwargs["p_tail"] == "%,region6"
        assert kwargs["p_tail_hash"] == "%,region6@%"
        assert kwargs["p_mid"] == "%,region6,%"


@pytest.mark.asyncio
async def test_delete_tile_variants_idempotent_when_table_absent():
    storage = _make_storage()
    with patch(
        "dynastore.modules.tiles.tiles_module.managed_transaction"
    ) as mtx, patch(
        "dynastore.modules.tiles.tiles_module.DQLQuery"
    ) as MockDQL:
        mtx.return_value.__aenter__ = AsyncMock(return_value=MagicMock())
        mtx.return_value.__aexit__ = AsyncMock(return_value=False)
        MockDQL.return_value.execute = AsyncMock(
            side_effect=Exception("relation does not exist (42P01)")
        )
        ok = await storage.delete_tile_variants(
            "cat", "col", "WebMercatorQuad", 5, 1, 2, ["mvt", "pbf"],
        )
        assert ok is True  # missing table → idempotent success


@pytest.mark.asyncio
async def test_delete_tile_variants_noop_on_empty_formats():
    storage = _make_storage()
    # No DB call at all when there are no formats to invalidate.
    ok = await storage.delete_tile_variants(
        "cat", "col", "WebMercatorQuad", 5, 1, 2, [],
    )
    assert ok is True
