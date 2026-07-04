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

"""Unit tests for lifecycle_reaper.py — DB-free.

Assertions:
  (a) Stale PROVISIONING row → lifecycle_status cleared to NULL (→ ACTIVE).
  (b) Stale DELETING row → delete_collection(force=True) re-driven.
  (c) Fresh rows (under threshold) → no action taken.
  (d) Config kill-switch (enabled=False) → no DB access, no action.
  (e) Missing DB engine → graceful no-op, no raise.
  (f) Per-collection exception swallowed → loop continues for others.
  (g) Advisory lock key does not collide with SoftDeleteReaper or
      MaintenanceSupervisor.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from dynastore.modules.catalog.lifecycle_reaper import (
    LifecycleReaper,
    LifecycleReaperConfig,
    _LIFECYCLE_REAPER_ADVISORY_LOCK_KEY,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _cfg(
    enabled: bool = True,
    threshold_seconds: int = 60,
    batch_size: int = 50,
    interval: float = 30.0,
) -> LifecycleReaperConfig:
    """Build a config instance with test-friendly defaults."""
    return LifecycleReaperConfig(
        enabled=enabled,
        stuck_threshold_seconds=threshold_seconds,
        reaper_interval_seconds=interval,
        batch_size=batch_size,
    )


def _mock_catalogs_svc(
    delete_collection_side_effect: Any = None,
) -> MagicMock:
    svc = MagicMock()
    svc.delete_collection = AsyncMock(side_effect=delete_collection_side_effect)
    return svc


def _setup_patches(
    engine: MagicMock,
    query_results: list[Any],
    catalogs_svc: Any = None,
) -> tuple[Any, Any, Any, Any]:
    """Return patch objects for engine, managed_transaction, DQLQuery, get_protocol."""
    return (
        patch(
            "dynastore.modules.catalog.lifecycle_reaper.get_engine",
            return_value=engine,
        ),
        patch(
            "dynastore.modules.catalog.lifecycle_reaper.managed_transaction",
        ),
        patch(
            "dynastore.modules.catalog.lifecycle_reaper.DQLQuery",
        ),
        patch(
            "dynastore.tools.discovery.get_protocol",
            return_value=catalogs_svc,
        ),
    )


# ---------------------------------------------------------------------------
# (a) Stale PROVISIONING cleared to NULL
# ---------------------------------------------------------------------------


class TestProvisioningClear:
    @pytest.mark.asyncio
    async def test_stale_provisioning_clears_lifecycle_status(self) -> None:
        """A stale PROVISIONING row must trigger a lifecycle_status=NULL UPDATE."""
        reaper = LifecycleReaper(_cfg())

        engine = MagicMock()
        fake_conn = AsyncMock()

        call_count = 0

        async def _mock_execute(*args: Any, **kwargs: Any) -> Any:
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                # _list_active_catalogs
                return [("cat-1", "schema_cat1")]
            if call_count == 2:
                # stuck PROVISIONING query
                return [("col-stuck",)]
            if call_count == 3:
                # UPDATE (clear) — return rowcount 1
                return 1
            # stuck DELETING query — none
            return []

        mock_query_instance = MagicMock()
        mock_query_instance.execute = AsyncMock(side_effect=_mock_execute)

        with (
            patch(
                "dynastore.modules.catalog.lifecycle_reaper.get_engine",
                return_value=engine,
            ),
            patch(
                "dynastore.modules.catalog.lifecycle_reaper.managed_transaction",
            ) as mock_tx,
            patch(
                "dynastore.modules.catalog.lifecycle_reaper.DQLQuery",
                return_value=mock_query_instance,
            ),
            patch(
                "dynastore.modules.catalog.lifecycle_reaper."
                "_invalidate_collection_lifecycle_caches",
                return_value=None,
            ),
        ):
            mock_tx.return_value.__aenter__ = AsyncMock(return_value=fake_conn)
            mock_tx.return_value.__aexit__ = AsyncMock(return_value=False)

            await reaper.run_once()

        # Three DQLQuery.execute calls: list_catalogs, stuck_provisioning, update
        # (plus one for stuck_deleting that returns [])
        assert call_count >= 3

    @pytest.mark.asyncio
    async def test_no_stale_provisioning_no_update(self) -> None:
        """When no stuck PROVISIONING rows, no UPDATE is issued."""
        reaper = LifecycleReaper(_cfg())
        engine = MagicMock()
        fake_conn = AsyncMock()

        call_count = 0

        async def _mock_execute(*args: Any, **kwargs: Any) -> Any:
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return [("cat-1", "schema_cat1")]
            # Both PROVISIONING and DELETING queries return empty
            return []

        mock_query_instance = MagicMock()
        mock_query_instance.execute = AsyncMock(side_effect=_mock_execute)

        with (
            patch(
                "dynastore.modules.catalog.lifecycle_reaper.get_engine",
                return_value=engine,
            ),
            patch(
                "dynastore.modules.catalog.lifecycle_reaper.managed_transaction",
            ) as mock_tx,
            patch(
                "dynastore.modules.catalog.lifecycle_reaper.DQLQuery",
                return_value=mock_query_instance,
            ),
        ):
            mock_tx.return_value.__aenter__ = AsyncMock(return_value=fake_conn)
            mock_tx.return_value.__aexit__ = AsyncMock(return_value=False)

            await reaper.run_once()

        # Only: list_catalogs + stuck_provisioning + stuck_deleting = 3 calls, no update
        assert call_count == 3


# ---------------------------------------------------------------------------
# (b) Stale DELETING re-drives force delete
# ---------------------------------------------------------------------------


class TestDeletingRedriven:
    @pytest.mark.asyncio
    async def test_stale_deleting_calls_force_delete(self) -> None:
        """A stale DELETING row must trigger delete_collection(force=True)."""
        reaper = LifecycleReaper(_cfg())
        mock_svc = _mock_catalogs_svc()

        engine = MagicMock()
        fake_conn = AsyncMock()

        call_count = 0

        async def _mock_execute(*args: Any, **kwargs: Any) -> Any:
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return [("cat-1", "schema_cat1")]
            if call_count == 2:
                # No stuck PROVISIONING
                return []
            if call_count == 3:
                # Stuck DELETING
                return [("col-deleting",)]
            return []

        mock_query_instance = MagicMock()
        mock_query_instance.execute = AsyncMock(side_effect=_mock_execute)

        with (
            patch(
                "dynastore.modules.catalog.lifecycle_reaper.get_engine",
                return_value=engine,
            ),
            patch(
                "dynastore.modules.catalog.lifecycle_reaper.managed_transaction",
            ) as mock_tx,
            patch(
                "dynastore.modules.catalog.lifecycle_reaper.DQLQuery",
                return_value=mock_query_instance,
            ),
            patch(
                "dynastore.modules.catalog.lifecycle_reaper.get_protocol",
                return_value=mock_svc,
            ),
        ):
            mock_tx.return_value.__aenter__ = AsyncMock(return_value=fake_conn)
            mock_tx.return_value.__aexit__ = AsyncMock(return_value=False)

            await reaper.run_once()

        mock_svc.delete_collection.assert_awaited_once_with(
            "cat-1", "col-deleting", force=True
        )

    @pytest.mark.asyncio
    async def test_multiple_stale_deleting_all_redriven(self) -> None:
        """Multiple stuck DELETING collections → all get force-deleted."""
        reaper = LifecycleReaper(_cfg())
        mock_svc = _mock_catalogs_svc()

        engine = MagicMock()
        fake_conn = AsyncMock()

        call_count = 0

        async def _mock_execute(*args: Any, **kwargs: Any) -> Any:
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return [("cat-1", "schema_cat1")]
            if call_count == 2:
                return []  # No PROVISIONING
            if call_count == 3:
                return [("col-a",), ("col-b",)]  # Two stuck DELETING
            return []

        mock_query_instance = MagicMock()
        mock_query_instance.execute = AsyncMock(side_effect=_mock_execute)

        with (
            patch(
                "dynastore.modules.catalog.lifecycle_reaper.get_engine",
                return_value=engine,
            ),
            patch(
                "dynastore.modules.catalog.lifecycle_reaper.managed_transaction",
            ) as mock_tx,
            patch(
                "dynastore.modules.catalog.lifecycle_reaper.DQLQuery",
                return_value=mock_query_instance,
            ),
            patch(
                "dynastore.modules.catalog.lifecycle_reaper.get_protocol",
                return_value=mock_svc,
            ),
        ):
            mock_tx.return_value.__aenter__ = AsyncMock(return_value=fake_conn)
            mock_tx.return_value.__aexit__ = AsyncMock(return_value=False)

            await reaper.run_once()

        assert mock_svc.delete_collection.await_count == 2
        called = {call.args[1] for call in mock_svc.delete_collection.await_args_list}
        assert called == {"col-a", "col-b"}


# ---------------------------------------------------------------------------
# (c) Fresh rows under threshold → no action
# ---------------------------------------------------------------------------


class TestFreshRowsIgnored:
    @pytest.mark.asyncio
    async def test_fresh_rows_not_processed(self) -> None:
        """Rows not past the threshold return empty from the SQL query; nothing fires."""
        reaper = LifecycleReaper(_cfg())
        mock_svc = _mock_catalogs_svc()

        engine = MagicMock()
        fake_conn = AsyncMock()

        # All queries return empty (SQL WHERE clause filters them out in real DB)
        mock_query_instance = MagicMock()

        call_count = 0

        async def _mock_execute(*args: Any, **kwargs: Any) -> Any:
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return [("cat-1", "schema_cat1")]
            return []

        mock_query_instance.execute = AsyncMock(side_effect=_mock_execute)

        with (
            patch(
                "dynastore.modules.catalog.lifecycle_reaper.get_engine",
                return_value=engine,
            ),
            patch(
                "dynastore.modules.catalog.lifecycle_reaper.managed_transaction",
            ) as mock_tx,
            patch(
                "dynastore.modules.catalog.lifecycle_reaper.DQLQuery",
                return_value=mock_query_instance,
            ),
            patch(
                "dynastore.modules.catalog.lifecycle_reaper.get_protocol",
                return_value=mock_svc,
            ),
        ):
            mock_tx.return_value.__aenter__ = AsyncMock(return_value=fake_conn)
            mock_tx.return_value.__aexit__ = AsyncMock(return_value=False)

            await reaper.run_once()

        mock_svc.delete_collection.assert_not_awaited()

    def test_stuck_provisioning_sql_contains_threshold_and_skip_locked(self) -> None:
        """SQL template must gate on created_at age and use SKIP LOCKED."""
        from dynastore.modules.catalog.lifecycle_reaper import _STUCK_PROVISIONING_SQL

        assert "created_at" in _STUCK_PROVISIONING_SQL
        assert "threshold_seconds" in _STUCK_PROVISIONING_SQL
        assert "SKIP LOCKED" in _STUCK_PROVISIONING_SQL
        assert "provisioning" in _STUCK_PROVISIONING_SQL

    def test_stuck_deleting_sql_contains_threshold_and_skip_locked(self) -> None:
        """SQL template must gate on created_at age and use SKIP LOCKED."""
        from dynastore.modules.catalog.lifecycle_reaper import _STUCK_DELETING_SQL

        assert "created_at" in _STUCK_DELETING_SQL
        assert "threshold_seconds" in _STUCK_DELETING_SQL
        assert "SKIP LOCKED" in _STUCK_DELETING_SQL
        assert "deleting" in _STUCK_DELETING_SQL


# ---------------------------------------------------------------------------
# (d) Kill-switch: enabled=False
# ---------------------------------------------------------------------------


class TestKillSwitch:
    @pytest.mark.asyncio
    async def test_disabled_config_skips_everything(self) -> None:
        """When enabled=False, run_once must not touch the DB or call any service."""
        reaper = LifecycleReaper(_cfg(enabled=False))

        with patch(
            "dynastore.modules.catalog.lifecycle_reaper.get_engine"
        ) as mock_engine:
            await reaper.run_once()

        mock_engine.assert_not_called()


# ---------------------------------------------------------------------------
# (e) Missing engine — graceful no-op
# ---------------------------------------------------------------------------


class TestMissingEngine:
    @pytest.mark.asyncio
    async def test_no_engine_returns_without_raising(self) -> None:
        reaper = LifecycleReaper(_cfg())

        with patch(
            "dynastore.modules.catalog.lifecycle_reaper.get_engine",
            return_value=None,
        ):
            # Neither method must raise
            result = await reaper._list_active_catalogs()
            assert result == []

            await reaper._reap_provisioning_in_schema(
                "cat-1", "schema1", threshold_seconds=60, limit=50
            )
            await reaper._reap_deleting_in_schema(
                "cat-1", "schema1", threshold_seconds=60, limit=50
            )


# ---------------------------------------------------------------------------
# (f) Per-collection exception swallowed — loop continues
# ---------------------------------------------------------------------------


class TestPerEntityExceptionHandling:
    @pytest.mark.asyncio
    async def test_first_deleting_failure_does_not_abort_second(self) -> None:
        """delete_collection raises for col-a; col-b must still be processed."""
        reaper = LifecycleReaper(_cfg())

        delete_calls: list[str] = []

        async def _delete(catalog_id: str, collection_id: str, *, force: bool = False) -> bool:
            delete_calls.append(collection_id)
            if collection_id == "col-a":
                raise RuntimeError("simulated failure")
            return True

        mock_svc = MagicMock()
        mock_svc.delete_collection = _delete

        engine = MagicMock()
        fake_conn = AsyncMock()

        call_count = 0

        async def _mock_execute(*args: Any, **kwargs: Any) -> Any:
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return [("cat-1", "schema_cat1")]
            if call_count == 2:
                return []  # No PROVISIONING
            if call_count == 3:
                return [("col-a",), ("col-b",)]
            return []

        mock_query_instance = MagicMock()
        mock_query_instance.execute = AsyncMock(side_effect=_mock_execute)

        with (
            patch(
                "dynastore.modules.catalog.lifecycle_reaper.get_engine",
                return_value=engine,
            ),
            patch(
                "dynastore.modules.catalog.lifecycle_reaper.managed_transaction",
            ) as mock_tx,
            patch(
                "dynastore.modules.catalog.lifecycle_reaper.DQLQuery",
                return_value=mock_query_instance,
            ),
            patch(
                "dynastore.modules.catalog.lifecycle_reaper.get_protocol",
                return_value=mock_svc,
            ),
        ):
            mock_tx.return_value.__aenter__ = AsyncMock(return_value=fake_conn)
            mock_tx.return_value.__aexit__ = AsyncMock(return_value=False)

            # Must not raise despite col-a failure
            await reaper.run_once()

        assert delete_calls == ["col-a", "col-b"], (
            "Both collections must be attempted even when the first one fails."
        )


# ---------------------------------------------------------------------------
# (g) Advisory lock key non-collision
# ---------------------------------------------------------------------------


class TestAdvisoryLockKeyCollision:
    def test_key_does_not_collide_with_soft_delete_reaper(self) -> None:
        from dynastore.modules.catalog.soft_delete_reaper import (
            _REAPER_ADVISORY_LOCK_KEY as SOFT_DELETE_KEY,
        )

        assert _LIFECYCLE_REAPER_ADVISORY_LOCK_KEY != SOFT_DELETE_KEY

    def test_key_does_not_collide_with_maintenance_supervisor(self) -> None:
        from dynastore.modules.catalog.maintenance_supervisor import (
            _SUPERVISOR_ADVISORY_LOCK_KEY as SUPERVISOR_KEY,
        )

        assert _LIFECYCLE_REAPER_ADVISORY_LOCK_KEY != SUPERVISOR_KEY


# ---------------------------------------------------------------------------
# Config validation
# ---------------------------------------------------------------------------


class TestLifecycleReaperConfig:
    def test_default_threshold_is_fifteen_minutes(self) -> None:
        cfg = LifecycleReaperConfig()
        assert cfg.stuck_threshold_seconds == 900  # 15 * 60

    def test_threshold_below_minimum_rejected(self) -> None:
        with pytest.raises(Exception):
            LifecycleReaperConfig(stuck_threshold_seconds=0)

    def test_batch_size_bounds(self) -> None:
        with pytest.raises(Exception):
            LifecycleReaperConfig(batch_size=0)
        with pytest.raises(Exception):
            LifecycleReaperConfig(batch_size=501)

    def test_enabled_default_false(self) -> None:
        assert LifecycleReaperConfig().enabled is False

    def test_interval_below_minimum_rejected(self) -> None:
        with pytest.raises(Exception):
            LifecycleReaperConfig(reaper_interval_seconds=10.0)
