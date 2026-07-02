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

"""Unit tests for the shared ``list_page_with_count`` SQL pagination helper.

Covers the count-splitting contract that used to be copy-pasted at every
call site (issue #2699): a single merged ``COUNT(*) OVER()`` query returns
``(rows, total)`` with ``total_count`` stripped from each row, and an empty
page returns ``([], 0)`` rather than crashing on an index-0 lookup.
"""

from unittest.mock import AsyncMock, patch

import pytest

from dynastore.modules.db_config.query_executor import DQLQuery
from dynastore.modules.db_config.shared_queries import list_page_with_count


@pytest.mark.asyncio
async def test_list_page_with_count_splits_total_from_rows():
    conn = AsyncMock()
    fake_rows = [
        {"total_count": 3, "id": 1, "name": "a"},
        {"total_count": 3, "id": 2, "name": "b"},
    ]
    with patch.object(DQLQuery, "execute", new_callable=AsyncMock, return_value=fake_rows):
        rows, total = await list_page_with_count(
            conn, "SELECT COUNT(*) OVER() AS total_count, * FROM t LIMIT :limit OFFSET :offset;",
            {"extra": "param"}, limit=2, offset=0,
        )

    assert total == 3
    assert rows == [{"id": 1, "name": "a"}, {"id": 2, "name": "b"}]
    assert all("total_count" not in r for r in rows)


@pytest.mark.asyncio
async def test_list_page_with_count_empty_page_returns_zero_total():
    conn = AsyncMock()
    with patch.object(DQLQuery, "execute", new_callable=AsyncMock, return_value=[]):
        rows, total = await list_page_with_count(
            conn, "SELECT COUNT(*) OVER() AS total_count, * FROM t LIMIT :limit OFFSET :offset;",
            limit=10, offset=0,
        )

    assert rows == []
    assert total == 0


@pytest.mark.asyncio
async def test_list_page_with_count_passes_limit_offset_and_params():
    conn = AsyncMock()
    with patch.object(
        DQLQuery, "execute", new_callable=AsyncMock, return_value=[]
    ) as mock_execute:
        await list_page_with_count(
            conn,
            "SELECT COUNT(*) OVER() AS total_count, * FROM t WHERE catalog_id = :catalog_id "
            "LIMIT :limit OFFSET :offset;",
            {"catalog_id": "cat1"},
            limit=5,
            offset=10,
        )

    mock_execute.assert_awaited_once_with(conn, limit=5, offset=10, catalog_id="cat1")
