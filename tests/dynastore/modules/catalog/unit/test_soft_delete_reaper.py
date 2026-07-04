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

"""Unit tests for soft_delete_reaper.py — DB-free.

Assertions:
  (a) Reaper SKIPS entities not yet past the grace window.
  (b) Reaper ENQUEUES a hard cascade (via delete_catalog/delete_collection
      force=True) for past-grace entities.
  (c) Reaper respects in-flight-cascade dedup (entities already queued for
      cascade_cleanup are not hard-deleted again this tick).
  (d) Config kill-switch (enabled=False) suppresses all processing.
  (e) Missing DB engine returns gracefully without raising.
  (f) Per-entity exception is swallowed so the loop continues for others.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from dynastore.modules.catalog.soft_delete_reaper import (
    SoftDeleteReaper,
    SoftDeleteReaperConfig,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _cfg(
    enabled: bool = True,
    grace_seconds: int = 60,
    batch_size: int = 50,
    interval: float = 30.0,
) -> SoftDeleteReaperConfig:
    """Build a config instance with test-friendly defaults."""
    return SoftDeleteReaperConfig(
        enabled=enabled,
        soft_grace_period_seconds=grace_seconds,
        reaper_interval_seconds=interval,
        batch_size=batch_size,
    )


def _mock_catalogs_svc(
    delete_catalog_side_effect: Any = None,
    delete_collection_side_effect: Any = None,
) -> MagicMock:
    svc = MagicMock()
    svc.delete_catalog = AsyncMock(side_effect=delete_catalog_side_effect)
    svc.delete_collection = AsyncMock(side_effect=delete_collection_side_effect)
    return svc


# ---------------------------------------------------------------------------
# (a) skip entities not past grace
# ---------------------------------------------------------------------------


class TestSkipNotPastGrace:
    @pytest.mark.asyncio
    async def test_no_overdue_catalogs_does_not_call_delete(self) -> None:
        """When the query returns empty, delete_catalog must not be called."""
        reaper = SoftDeleteReaper(_cfg())
        mock_svc = _mock_catalogs_svc()

        engine = MagicMock()

        with (
            patch(
                "dynastore.modules.catalog.soft_delete_reaper.get_engine",
                return_value=engine,
            ),
            patch(
                "dynastore.modules.catalog.soft_delete_reaper.managed_transaction",
            ) as mock_tx,
            patch(
                "dynastore.modules.catalog.soft_delete_reaper.DQLQuery",
            ) as mock_query_cls,
            patch(
                "dynastore.modules.catalog.soft_delete_reaper.get_protocol",
                return_value=mock_svc,
            ),
        ):
            # managed_transaction returns a context manager
            fake_conn = AsyncMock()
            mock_tx.return_value.__aenter__ = AsyncMock(return_value=fake_conn)
            mock_tx.return_value.__aexit__ = AsyncMock(return_value=False)

            # DQLQuery(...).execute(...) returns empty list → no overdue catalogs
            mock_query_instance = MagicMock()
            mock_query_instance.execute = AsyncMock(return_value=[])
            mock_query_cls.return_value = mock_query_instance

            await reaper._reap_catalogs(grace_seconds=60, limit=50)

        mock_svc.delete_catalog.assert_not_awaited()


# ---------------------------------------------------------------------------
# (b) hard-delete called for past-grace entities
# ---------------------------------------------------------------------------


class TestHardDeletePastGrace:
    @pytest.mark.asyncio
    async def test_overdue_catalog_triggers_force_delete(self) -> None:
        """One overdue catalog → delete_catalog(catalog_id, force=True) called."""
        reaper = SoftDeleteReaper(_cfg())
        mock_svc = _mock_catalogs_svc()

        engine = MagicMock()
        with (
            patch(
                "dynastore.modules.catalog.soft_delete_reaper.get_engine",
                return_value=engine,
            ),
            patch(
                "dynastore.modules.catalog.soft_delete_reaper.managed_transaction",
            ) as mock_tx,
            patch(
                "dynastore.modules.catalog.soft_delete_reaper.DQLQuery",
            ) as mock_query_cls,
            patch(
                "dynastore.modules.catalog.soft_delete_reaper.get_protocol",
                return_value=mock_svc,
            ),
        ):
            fake_conn = AsyncMock()
            mock_tx.return_value.__aenter__ = AsyncMock(return_value=fake_conn)
            mock_tx.return_value.__aexit__ = AsyncMock(return_value=False)

            # Return one overdue catalog ID
            mock_query_instance = MagicMock()
            mock_query_instance.execute = AsyncMock(return_value=[("overdue-cat",)])
            mock_query_cls.return_value = mock_query_instance

            await reaper._reap_catalogs(grace_seconds=60, limit=50)

        mock_svc.delete_catalog.assert_awaited_once_with("overdue-cat", force=True)

    @pytest.mark.asyncio
    async def test_multiple_overdue_catalogs_all_deleted(self) -> None:
        """Multiple overdue catalogs → delete_catalog called for each."""
        reaper = SoftDeleteReaper(_cfg())
        mock_svc = _mock_catalogs_svc()

        engine = MagicMock()
        overdue = [("cat-1",), ("cat-2",), ("cat-3",)]
        with (
            patch(
                "dynastore.modules.catalog.soft_delete_reaper.get_engine",
                return_value=engine,
            ),
            patch(
                "dynastore.modules.catalog.soft_delete_reaper.managed_transaction",
            ) as mock_tx,
            patch(
                "dynastore.modules.catalog.soft_delete_reaper.DQLQuery",
            ) as mock_query_cls,
            patch(
                "dynastore.modules.catalog.soft_delete_reaper.get_protocol",
                return_value=mock_svc,
            ),
        ):
            fake_conn = AsyncMock()
            mock_tx.return_value.__aenter__ = AsyncMock(return_value=fake_conn)
            mock_tx.return_value.__aexit__ = AsyncMock(return_value=False)

            mock_query_instance = MagicMock()
            mock_query_instance.execute = AsyncMock(return_value=overdue)
            mock_query_cls.return_value = mock_query_instance

            await reaper._reap_catalogs(grace_seconds=60, limit=50)

        assert mock_svc.delete_catalog.await_count == 3
        called_ids = {call.args[0] for call in mock_svc.delete_catalog.await_args_list}
        assert called_ids == {"cat-1", "cat-2", "cat-3"}

    @pytest.mark.asyncio
    async def test_overdue_collection_triggers_force_delete(self) -> None:
        """One overdue collection → delete_collection(cat_id, col_id, force=True)."""
        reaper = SoftDeleteReaper(_cfg())
        mock_svc = _mock_catalogs_svc()

        engine = MagicMock()

        # _list_active_catalog_ids returns one catalog
        active_catalogs = [("my-cat", "my_schema")]
        # collection query returns one overdue collection
        overdue_collections = [("col-1",)]

        call_count = 0

        async def _mock_execute(*args: Any, **kwargs: Any) -> Any:
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return active_catalogs  # _list_active_catalog_ids
            return overdue_collections  # _reap_collections_in_schema

        mock_query_instance = MagicMock()
        mock_query_instance.execute = AsyncMock(side_effect=_mock_execute)

        fake_conn = AsyncMock()

        with (
            patch(
                "dynastore.modules.catalog.soft_delete_reaper.get_engine",
                return_value=engine,
            ),
            patch(
                "dynastore.modules.catalog.soft_delete_reaper.managed_transaction",
            ) as mock_tx,
            patch(
                "dynastore.modules.catalog.soft_delete_reaper.DQLQuery",
                return_value=mock_query_instance,
            ),
            patch(
                "dynastore.modules.catalog.soft_delete_reaper.get_protocol",
                return_value=mock_svc,
            ),
        ):
            mock_tx.return_value.__aenter__ = AsyncMock(return_value=fake_conn)
            mock_tx.return_value.__aexit__ = AsyncMock(return_value=False)

            await reaper._reap_collections(grace_seconds=60, limit=50)

        mock_svc.delete_collection.assert_awaited_once_with(
            "my-cat", "col-1", force=True
        )


# ---------------------------------------------------------------------------
# (c) in-flight dedup — SQL WHERE clause dedup check
# ---------------------------------------------------------------------------


class TestInflightDedup:
    """The in-flight dedup logic lives in SQL (NOT EXISTS sub-select).

    At the unit level we verify that the reaper's query passes the expected
    parameters and does NOT call delete for entities that the SQL filters out
    (simulated by returning an empty result from the mocked query).
    """

    @pytest.mark.asyncio
    async def test_entity_with_inflight_task_skipped(self) -> None:
        """SQL returns empty because the in-flight NOT EXISTS filter excluded
        the entity → delete_catalog must NOT be called."""
        reaper = SoftDeleteReaper(_cfg())
        mock_svc = _mock_catalogs_svc()

        engine = MagicMock()
        with (
            patch(
                "dynastore.modules.catalog.soft_delete_reaper.get_engine",
                return_value=engine,
            ),
            patch(
                "dynastore.modules.catalog.soft_delete_reaper.managed_transaction",
            ) as mock_tx,
            patch(
                "dynastore.modules.catalog.soft_delete_reaper.DQLQuery",
            ) as mock_query_cls,
            patch(
                "dynastore.modules.catalog.soft_delete_reaper.get_protocol",
                return_value=mock_svc,
            ),
        ):
            fake_conn = AsyncMock()
            mock_tx.return_value.__aenter__ = AsyncMock(return_value=fake_conn)
            mock_tx.return_value.__aexit__ = AsyncMock(return_value=False)

            # Empty result simulates the NOT EXISTS filter excluding the entity
            mock_query_instance = MagicMock()
            mock_query_instance.execute = AsyncMock(return_value=[])
            mock_query_cls.return_value = mock_query_instance

            await reaper._reap_catalogs(grace_seconds=60, limit=50)

        mock_svc.delete_catalog.assert_not_awaited()

    def test_overdue_catalog_query_contains_not_exists_clause(self) -> None:
        """The SQL template must include the in-flight dedup sub-select."""
        from dynastore.modules.catalog.soft_delete_reaper import _OVERDUE_CATALOGS_SQL

        assert "NOT EXISTS" in _OVERDUE_CATALOGS_SQL
        assert "cascade_cleanup" in _OVERDUE_CATALOGS_SQL
        assert "SKIP LOCKED" in _OVERDUE_CATALOGS_SQL


# ---------------------------------------------------------------------------
# (d) kill-switch: enabled=False
# ---------------------------------------------------------------------------


class TestKillSwitch:
    @pytest.mark.asyncio
    async def test_disabled_config_skips_everything(self) -> None:
        """When enabled=False, run_once must not touch the DB or call delete."""
        reaper = SoftDeleteReaper(_cfg(enabled=False))

        with patch(
            "dynastore.modules.catalog.soft_delete_reaper.get_engine"
        ) as mock_engine:
            await reaper.run_once()

        mock_engine.assert_not_called()


# ---------------------------------------------------------------------------
# (e) missing engine — graceful no-op
# ---------------------------------------------------------------------------


class TestMissingEngine:
    @pytest.mark.asyncio
    async def test_no_engine_returns_without_raising(self) -> None:
        reaper = SoftDeleteReaper(_cfg())

        with patch(
            "dynastore.modules.catalog.soft_delete_reaper.get_engine",
            return_value=None,
        ):
            # Must not raise
            await reaper._reap_catalogs(grace_seconds=60, limit=50)
            await reaper._reap_collections(grace_seconds=60, limit=50)


# ---------------------------------------------------------------------------
# (f) per-entity exception swallowed — loop continues
# ---------------------------------------------------------------------------


class TestPerEntityExceptionHandling:
    @pytest.mark.asyncio
    async def test_first_entity_failure_does_not_abort_second(self) -> None:
        """delete_catalog raises for cat-1; cat-2 must still be processed."""
        reaper = SoftDeleteReaper(_cfg())

        delete_calls: list[str] = []

        async def _delete(catalog_id: str, *, force: bool = False) -> None:
            delete_calls.append(catalog_id)
            if catalog_id == "cat-1":
                raise RuntimeError("simulated failure")

        mock_svc = MagicMock()
        mock_svc.delete_catalog = _delete

        engine = MagicMock()
        overdue = [("cat-1",), ("cat-2",)]

        with (
            patch(
                "dynastore.modules.catalog.soft_delete_reaper.get_engine",
                return_value=engine,
            ),
            patch(
                "dynastore.modules.catalog.soft_delete_reaper.managed_transaction",
            ) as mock_tx,
            patch(
                "dynastore.modules.catalog.soft_delete_reaper.DQLQuery",
            ) as mock_query_cls,
            patch(
                "dynastore.modules.catalog.soft_delete_reaper.get_protocol",
                return_value=mock_svc,
            ),
        ):
            fake_conn = AsyncMock()
            mock_tx.return_value.__aenter__ = AsyncMock(return_value=fake_conn)
            mock_tx.return_value.__aexit__ = AsyncMock(return_value=False)

            mock_query_instance = MagicMock()
            mock_query_instance.execute = AsyncMock(return_value=overdue)
            mock_query_cls.return_value = mock_query_instance

            # Must not raise despite the cat-1 failure
            await reaper._reap_catalogs(grace_seconds=60, limit=50)

        assert delete_calls == ["cat-1", "cat-2"], (
            "Both entities must be attempted even when the first one fails."
        )


# ---------------------------------------------------------------------------
# Config validation
# ---------------------------------------------------------------------------


class TestSoftDeleteReaperConfig:
    def test_default_grace_is_seven_days(self) -> None:
        cfg = SoftDeleteReaperConfig()
        assert cfg.soft_grace_period_seconds == 604800  # 7 * 86400

    def test_grace_below_minimum_rejected(self) -> None:
        with pytest.raises(Exception):
            SoftDeleteReaperConfig(soft_grace_period_seconds=0)

    def test_batch_size_bounds(self) -> None:
        with pytest.raises(Exception):
            SoftDeleteReaperConfig(batch_size=0)
        with pytest.raises(Exception):
            SoftDeleteReaperConfig(batch_size=501)

    def test_enabled_default_false(self) -> None:
        # The reaper hard-deletes (irreversible) — it must be opt-in, not
        # auto-active on first deploy. Operators enable it via the configs API.
        assert SoftDeleteReaperConfig().enabled is False
