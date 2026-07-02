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

"""Unit tests for ES log index retention selection + driver (#2797).

``parse_log_index_month`` / ``select_expired_log_indices`` are pure and
covered without any ES mocking; ``run_es_logs_retention`` is covered with a
mocked ``opensearchpy`` client (no live cluster).
"""
from __future__ import annotations

from datetime import date, datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from dynastore.modules.elasticsearch.log_retention import (
    parse_log_index_month,
    select_expired_log_indices,
    run_es_logs_retention,
)


PREFIX = "dynastore"


# ---------------------------------------------------------------------------
# parse_log_index_month
# ---------------------------------------------------------------------------


def test_parse_log_index_month_valid_suffix():
    assert parse_log_index_month("dynastore-logs-2026.01") == date(2026, 1, 1)


def test_parse_log_index_month_valid_suffix_december():
    assert parse_log_index_month("dynastore-logs-2025.12") == date(2025, 12, 1)


def test_parse_log_index_month_flat_index_returns_none():
    """The pre-#2797 flat index has no month suffix — never a delete candidate."""
    assert parse_log_index_month("dynastore-logs") is None


def test_parse_log_index_month_unrelated_index_returns_none():
    assert parse_log_index_month("dynastore-catalogs") is None
    assert parse_log_index_month("dynastore-items-mycatalog") is None


# ---------------------------------------------------------------------------
# select_expired_log_indices — cutoff selection
# ---------------------------------------------------------------------------


def test_select_expired_log_indices_drops_older_than_window():
    now = datetime(2026, 7, 15, tzinfo=timezone.utc)
    names = [
        "dynastore-logs-2025.11",  # 8 months old -> expired
        "dynastore-logs-2025.12",  # 7 months old -> expired
        "dynastore-logs-2026.01",  # exactly at cutoff -> kept (not strictly older)
        "dynastore-logs-2026.06",  # 1 month old -> kept
        "dynastore-logs-2026.07",  # current month -> kept
    ]
    expired = select_expired_log_indices(names, now, retention_months=6)
    assert expired == ["dynastore-logs-2025.11", "dynastore-logs-2025.12"]


def test_select_expired_log_indices_boundary_month_is_not_expired():
    """A month exactly `retention_months` back is kept (>=, not >)."""
    now = datetime(2026, 7, 1, tzinfo=timezone.utc)
    names = ["dynastore-logs-2026.01"]
    assert select_expired_log_indices(names, now, retention_months=6) == []


def test_select_expired_log_indices_skips_non_monthly_names():
    now = datetime(2030, 1, 1, tzinfo=timezone.utc)
    names = ["dynastore-logs", "dynastore-catalogs", "dynastore-logs-2020.01"]
    expired = select_expired_log_indices(names, now, retention_months=6)
    assert expired == ["dynastore-logs-2020.01"]


def test_select_expired_log_indices_empty_input():
    now = datetime(2026, 7, 1, tzinfo=timezone.utc)
    assert select_expired_log_indices([], now, retention_months=6) == []


def test_select_expired_log_indices_year_boundary_cutoff():
    """retention_months spanning a year boundary computes the correct cutoff."""
    now = datetime(2026, 2, 10, tzinfo=timezone.utc)  # cutoff = 2025-08
    names = ["dynastore-logs-2025.07", "dynastore-logs-2025.08", "dynastore-logs-2025.09"]
    expired = select_expired_log_indices(names, now, retention_months=6)
    assert expired == ["dynastore-logs-2025.07"]


# ---------------------------------------------------------------------------
# run_es_logs_retention — driver, mocked ES client
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_run_es_logs_retention_no_client_returns_zero():
    with patch(
        "dynastore.modules.elasticsearch.client.get_client", return_value=None,
    ):
        deleted = await run_es_logs_retention(retention_months=6)
    assert deleted == 0


@pytest.mark.asyncio
async def test_run_es_logs_retention_deletes_only_expired_indices():
    es = MagicMock()
    es.indices = MagicMock()
    existing = {
        "dynastore-logs-2025.01": {},
        "dynastore-logs-2026.06": {},
        "dynastore-logs-2026.07": {},
    }
    es.indices.get = AsyncMock(return_value=existing)
    es.indices.delete = AsyncMock()

    fixed_now = datetime(2026, 7, 10, tzinfo=timezone.utc)

    with (
        patch("dynastore.modules.elasticsearch.client.get_client", return_value=es),
        patch("dynastore.modules.elasticsearch.client.get_index_prefix", return_value=PREFIX),
        patch("dynastore.modules.elasticsearch.log_retention.datetime") as mock_dt,
    ):
        mock_dt.now.return_value = fixed_now
        deleted = await run_es_logs_retention(retention_months=6)

    assert deleted == 1
    es.indices.delete.assert_awaited_once_with(index="dynastore-logs-2025.01")


@pytest.mark.asyncio
async def test_run_es_logs_retention_indices_get_failure_returns_zero():
    es = MagicMock()
    es.indices = MagicMock()
    es.indices.get = AsyncMock(side_effect=RuntimeError("cluster unreachable"))

    with (
        patch("dynastore.modules.elasticsearch.client.get_client", return_value=es),
        patch("dynastore.modules.elasticsearch.client.get_index_prefix", return_value=PREFIX),
    ):
        deleted = await run_es_logs_retention(retention_months=6)

    assert deleted == 0


@pytest.mark.asyncio
async def test_run_es_logs_retention_one_delete_failure_does_not_block_others():
    es = MagicMock()
    es.indices = MagicMock()
    existing = {
        "dynastore-logs-2025.01": {},
        "dynastore-logs-2025.02": {},
    }
    es.indices.get = AsyncMock(return_value=existing)

    async def _delete(index):
        if index == "dynastore-logs-2025.01":
            raise RuntimeError("delete failed")

    es.indices.delete = AsyncMock(side_effect=_delete)

    fixed_now = datetime(2026, 7, 10, tzinfo=timezone.utc)

    with (
        patch("dynastore.modules.elasticsearch.client.get_client", return_value=es),
        patch("dynastore.modules.elasticsearch.client.get_index_prefix", return_value=PREFIX),
        patch("dynastore.modules.elasticsearch.log_retention.datetime") as mock_dt,
    ):
        mock_dt.now.return_value = fixed_now
        deleted = await run_es_logs_retention(retention_months=6)

    assert deleted == 1
    assert es.indices.delete.await_count == 2
