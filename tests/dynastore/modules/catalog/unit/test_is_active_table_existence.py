"""Unit tests for CollectionService.is_active table-existence probe (#1875).

Self-healing guard: when a physical_table pin is present but the table has
been dropped out-of-band (diverged infra state), is_active must return False
so the caller's lazy-activation path re-provisions via ensure_storage.

Scenarios tested
----------------
- No pin → False immediately (no DB probe).
- Pin present + table absent → False (probe fires; re-provisioning triggered).
- Pin present + table present (cold process, no confirmed set) → True (probe fires).
- Pin present + already in confirmed-active set → True (fast path, no probe).
- activate_collection success → marks confirmed-active.
- Schema resolution failure → falls back to pin-only (True) with a warning.
- Probe exception → falls back to pin-only (True) with a warning.
"""
from __future__ import annotations

from typing import TYPE_CHECKING

import pytest
from unittest.mock import AsyncMock, patch

if TYPE_CHECKING:
    from dynastore.modules.catalog.collection_service import CollectionService


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_service() -> "CollectionService":
    """Construct a CollectionService without running __init__."""
    from dynastore.modules.catalog.collection_service import CollectionService

    svc = CollectionService.__new__(CollectionService)
    svc.engine = AsyncMock()
    return svc


def _clear_confirmed_set() -> None:
    """Drain the module-level confirmed-active set between tests."""
    from dynastore.modules.catalog import collection_service

    collection_service._confirmed_active.clear()


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestIsActiveTableExistenceProbe:
    """is_active must probe the DB when pin is present but table confirmation
    is absent from the process-local confirmed-active set."""

    @pytest.mark.asyncio
    async def test_no_pin_returns_false_without_probe(self):
        """When physical_table is None, is_active returns False immediately
        without touching the DB (no schema resolution, no to_regclass)."""
        _clear_confirmed_set()
        svc = _make_service()

        with patch.object(
            svc, "resolve_physical_table", new=AsyncMock(return_value=None)
        ) as mock_resolve:
            with patch.object(
                svc, "_resolve_physical_schema", new=AsyncMock()
            ) as mock_schema:
                result = await svc.is_active("cat1", "col1")

        assert result is False
        mock_resolve.assert_awaited_once()
        mock_schema.assert_not_called()

    @pytest.mark.asyncio
    async def test_pin_present_table_absent_returns_false(self):
        """Pin present but to_regclass returns None → False (diverged state)."""
        _clear_confirmed_set()
        svc = _make_service()

        with patch.object(
            svc, "resolve_physical_table", new=AsyncMock(return_value="t_items_abc")
        ):
            with patch.object(
                svc, "_resolve_physical_schema", new=AsyncMock(return_value="cat_schema")
            ):
                with patch(
                    "dynastore.modules.db_config.locking_tools.check_table_exists",
                    new=AsyncMock(return_value=False),
                ) as mock_probe:
                    with patch(
                        "dynastore.modules.catalog.collection_service.managed_transaction"
                    ) as mock_txn:
                        mock_conn = AsyncMock()
                        mock_txn.return_value.__aenter__ = AsyncMock(return_value=mock_conn)
                        mock_txn.return_value.__aexit__ = AsyncMock(return_value=False)
                        result = await svc.is_active("cat1", "col1")

        assert result is False
        mock_probe.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_pin_present_table_present_cold_process_returns_true(self):
        """Pin present + to_regclass finds the table → True.

        Also checks that the (catalog_id, collection_id) pair is added to
        the confirmed-active set so the next call takes the fast path.
        """
        _clear_confirmed_set()
        svc = _make_service()

        with patch.object(
            svc, "resolve_physical_table", new=AsyncMock(return_value="t_items_abc")
        ):
            with patch.object(
                svc, "_resolve_physical_schema", new=AsyncMock(return_value="cat_schema")
            ):
                with patch(
                    "dynastore.modules.db_config.locking_tools.check_table_exists",
                    new=AsyncMock(return_value=True),
                ):
                    with patch(
                        "dynastore.modules.catalog.collection_service.managed_transaction"
                    ) as mock_txn:
                        mock_conn = AsyncMock()
                        mock_txn.return_value.__aenter__ = AsyncMock(return_value=mock_conn)
                        mock_txn.return_value.__aexit__ = AsyncMock(return_value=False)
                        result = await svc.is_active("cat1", "col1")

        assert result is True

        # Verify the pair was added to the confirmed-active set.
        from dynastore.modules.catalog import collection_service
        assert ("cat1", "col1") in collection_service._confirmed_active

    @pytest.mark.asyncio
    async def test_pin_present_confirmed_set_hit_skips_probe(self):
        """When (catalog_id, collection_id) is already in the confirmed-active
        set, is_active returns True without any DB probe."""
        _clear_confirmed_set()
        svc = _make_service()

        from dynastore.modules.catalog import collection_service
        collection_service._confirmed_active.add(("cat1", "col1"))

        with patch.object(
            svc, "resolve_physical_table", new=AsyncMock(return_value="t_items_abc")
        ):
            with patch.object(
                svc, "_resolve_physical_schema", new=AsyncMock()
            ) as mock_schema:
                with patch(
                    "dynastore.modules.db_config.locking_tools.check_table_exists",
                    new=AsyncMock(),
                ) as mock_probe:
                    result = await svc.is_active("cat1", "col1")

        assert result is True
        mock_schema.assert_not_called()
        mock_probe.assert_not_called()

    @pytest.mark.asyncio
    async def test_schema_resolution_none_falls_back_to_pin_only(self):
        """When _resolve_physical_schema returns None (transient failure),
        is_active falls back to True (pin-only) rather than raising or
        triggering spurious re-provisioning."""
        _clear_confirmed_set()
        svc = _make_service()

        with patch.object(
            svc, "resolve_physical_table", new=AsyncMock(return_value="t_items_abc")
        ):
            with patch.object(
                svc, "_resolve_physical_schema", new=AsyncMock(return_value=None)
            ):
                with patch(
                    "dynastore.modules.db_config.locking_tools.check_table_exists",
                    new=AsyncMock(),
                ) as mock_probe:
                    result = await svc.is_active("cat1", "col1")

        assert result is True
        mock_probe.assert_not_called()

    @pytest.mark.asyncio
    async def test_probe_exception_falls_back_to_pin_only(self):
        """A DB error during the to_regclass probe must not raise or block
        writes. is_active returns True (pin-only fallback) with a log warning."""
        _clear_confirmed_set()
        svc = _make_service()

        with patch.object(
            svc, "resolve_physical_table", new=AsyncMock(return_value="t_items_abc")
        ):
            with patch.object(
                svc, "_resolve_physical_schema", new=AsyncMock(return_value="cat_schema")
            ):
                with patch(
                    "dynastore.modules.db_config.locking_tools.check_table_exists",
                    new=AsyncMock(side_effect=Exception("connection reset")),
                ):
                    with patch(
                        "dynastore.modules.catalog.collection_service.managed_transaction"
                    ) as mock_txn:
                        mock_conn = AsyncMock()
                        mock_txn.return_value.__aenter__ = AsyncMock(return_value=mock_conn)
                        mock_txn.return_value.__aexit__ = AsyncMock(return_value=False)
                        result = await svc.is_active("cat1", "col1")

        assert result is True

    @pytest.mark.asyncio
    async def test_activate_collection_marks_confirmed(self):
        """activate_collection on success must add the pair to the confirmed-active set."""
        _clear_confirmed_set()
        svc = _make_service()

        with patch.object(
            svc, "_activate_collection", new=AsyncMock()
        ):
            with patch(
                "dynastore.modules.catalog.collection_service.managed_transaction"
            ) as mock_txn:
                mock_conn = AsyncMock()
                mock_txn.return_value.__aenter__ = AsyncMock(return_value=mock_conn)
                mock_txn.return_value.__aexit__ = AsyncMock(return_value=False)
                await svc.activate_collection("cat1", "col1")

        from dynastore.modules.catalog import collection_service
        assert ("cat1", "col1") in collection_service._confirmed_active

    @pytest.mark.asyncio
    async def test_diverged_collection_reprovisions_then_active(self):
        """End-to-end divergence scenario: pin present but table absent on
        first call → is_active returns False → simulate activate_collection →
        confirmed set populated → second is_active returns True (fast path)."""
        _clear_confirmed_set()
        svc = _make_service()

        # First call: table absent → False.
        with patch.object(
            svc, "resolve_physical_table", new=AsyncMock(return_value="t_items_abc")
        ):
            with patch.object(
                svc, "_resolve_physical_schema", new=AsyncMock(return_value="cat_schema")
            ):
                with patch(
                    "dynastore.modules.db_config.locking_tools.check_table_exists",
                    new=AsyncMock(return_value=False),
                ):
                    with patch(
                        "dynastore.modules.catalog.collection_service.managed_transaction"
                    ) as mock_txn:
                        mock_conn = AsyncMock()
                        mock_txn.return_value.__aenter__ = AsyncMock(return_value=mock_conn)
                        mock_txn.return_value.__aexit__ = AsyncMock(return_value=False)
                        first = await svc.is_active("cat1", "col1")

        assert first is False

        # Simulate activate_collection completing successfully.
        from dynastore.modules.catalog import collection_service
        collection_service._mark_confirmed_active("cat1", "col1")

        # Second call: fast path via confirmed set → True, no probe.
        with patch.object(
            svc, "resolve_physical_table", new=AsyncMock(return_value="t_items_abc")
        ):
            with patch.object(
                svc, "_resolve_physical_schema", new=AsyncMock()
            ) as mock_schema:
                with patch(
                    "dynastore.modules.db_config.locking_tools.check_table_exists",
                    new=AsyncMock(),
                ) as mock_probe:
                    second = await svc.is_active("cat1", "col1")

        assert second is True
        mock_schema.assert_not_called()
        mock_probe.assert_not_called()
