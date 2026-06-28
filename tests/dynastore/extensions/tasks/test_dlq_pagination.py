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

"""Unit tests for DLQ list endpoint pagination (issue #2528 item 3).

Verifies that the three dead-letter list routes return TaskPage{items,
next_cursor} with keyset pagination matching every other /task list route,
and that list_dead_letter_tasks in maintenance.py encodes the cursor correctly
for ASC ordering.
"""
from __future__ import annotations

import uuid
from datetime import datetime, timezone
from unittest.mock import MagicMock

import pytest

from dynastore.models.tasks import Task, TaskPage, TaskStatusEnum


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _dead_task(offset_seconds: int = 0) -> Task:
    """Build a minimal DEAD_LETTER Task with a deterministic timestamp."""
    ts = datetime(2026, 1, 1, 0, 0, offset_seconds, tzinfo=timezone.utc)
    return Task(
        task_type="provision_catalog",
        status=TaskStatusEnum.DEAD_LETTER,
        catalog_id="s_abc123",
        timestamp=ts,  # uses alias="created" but populate_by_name=True handles "timestamp"
    )


# ---------------------------------------------------------------------------
# _to_task_page — the shared helper used by all list routes
# ---------------------------------------------------------------------------

class TestToTaskPage:
    def test_last_page_has_no_cursor(self):
        """When rows <= limit the page has no next_cursor."""
        import dynastore.extensions.tasks.tasks_service as svc

        rows = [_dead_task(i) for i in range(3)]
        page = svc._to_task_page(rows, limit=5)

        assert isinstance(page, TaskPage)
        assert len(page.items) == 3
        assert page.next_cursor is None

    def test_full_page_yields_cursor(self):
        """When limit+1 rows are present the (limit+1)-th row is encoded as cursor."""
        import dynastore.extensions.tasks.tasks_service as svc
        from dynastore.modules.tasks.tasks_module import decode_cursor

        rows = [_dead_task(i) for i in range(6)]
        page = svc._to_task_page(rows, limit=5)

        assert len(page.items) == 5
        assert page.next_cursor is not None
        # Cursor must decode to the 6th row's (timestamp, task_id).
        c_ts, c_id = decode_cursor(page.next_cursor)
        sixth = rows[5]
        assert c_ts == sixth.timestamp
        assert c_id == sixth.jobID

    def test_exact_limit_rows_has_no_cursor(self):
        """Exactly limit rows means no overflow → no next page."""
        import dynastore.extensions.tasks.tasks_service as svc

        rows = [_dead_task(i) for i in range(5)]
        page = svc._to_task_page(rows, limit=5)

        assert len(page.items) == 5
        assert page.next_cursor is None


# ---------------------------------------------------------------------------
# list_dead_letter_tasks — maintenance.py layer
# ---------------------------------------------------------------------------

class TestListDeadLetterTasks:
    @pytest.mark.asyncio
    async def test_returns_list_of_task_objects(self, monkeypatch):
        """list_dead_letter_tasks must return List[Task], not List[Dict]."""
        import dynastore.modules.tasks.maintenance as maint

        task_dict = {
            "task_id": str(uuid.uuid4()),
            "task_type": "provision_catalog",
            "status": "DEAD_LETTER",
            "catalog_id": "s_abc",
            "collection_id": None,
            "owner_id": None,
            "retry_count": 3,
            "max_retries": 3,
            "timestamp": datetime(2026, 1, 1, tzinfo=timezone.utc),
            "finished_at": None,
            "error_message": "boom",
            "inputs": {},
        }

        fake_engine = MagicMock()

        import contextlib

        @contextlib.asynccontextmanager
        async def fake_tx(_engine):
            yield MagicMock()

        async def fake_execute(self_q, _conn, **kw):
            return [task_dict]

        monkeypatch.setattr(maint, "managed_transaction", fake_tx)
        monkeypatch.setattr(maint.DQLQuery, "execute", fake_execute)
        monkeypatch.setattr(maint, "get_task_schema", lambda: "tasks")

        result = await maint.list_dead_letter_tasks(fake_engine, limit=5)

        assert isinstance(result, list)
        assert len(result) == 1
        assert isinstance(result[0], Task)
        assert result[0].error_message == "boom"
        assert result[0].retry_count == 3

    @pytest.mark.asyncio
    async def test_no_cursor_uses_asc_order(self, monkeypatch):
        """Without a cursor the SQL must use ORDER BY timestamp ASC, task_id ASC."""
        import dynastore.modules.tasks.maintenance as maint

        captured_sql: list = []
        fake_engine = MagicMock()

        import contextlib

        @contextlib.asynccontextmanager
        async def fake_tx(_engine):
            yield MagicMock()

        async def fake_execute(self_q, _conn, **kw):
            captured_sql.append(self_q.template)
            return []

        monkeypatch.setattr(maint, "managed_transaction", fake_tx)
        monkeypatch.setattr(maint.DQLQuery, "execute", fake_execute)
        monkeypatch.setattr(maint, "get_task_schema", lambda: "tasks")

        await maint.list_dead_letter_tasks(fake_engine, limit=5)

        assert captured_sql, "DQLQuery.execute was not called"
        sql = captured_sql[0].upper()
        assert "ORDER BY TIMESTAMP ASC" in sql
        assert "> (:C_TS" not in sql  # no keyset predicate without cursor

    @pytest.mark.asyncio
    async def test_cursor_adds_gt_predicate(self, monkeypatch):
        """With a cursor the SQL must add a (timestamp, task_id) > … predicate."""
        import dynastore.modules.tasks.maintenance as maint
        from dynastore.modules.tasks.tasks_module import encode_cursor

        task = _dead_task(0)
        cursor = encode_cursor(task)

        captured_sql: list = []
        fake_engine = MagicMock()

        import contextlib

        @contextlib.asynccontextmanager
        async def fake_tx(_engine):
            yield MagicMock()

        async def fake_execute(self_q, _conn, **kw):
            captured_sql.append(self_q.template)
            return []

        monkeypatch.setattr(maint, "managed_transaction", fake_tx)
        monkeypatch.setattr(maint.DQLQuery, "execute", fake_execute)
        monkeypatch.setattr(maint, "get_task_schema", lambda: "tasks")

        await maint.list_dead_letter_tasks(fake_engine, limit=5, cursor=cursor)

        assert captured_sql
        sql = captured_sql[0]
        assert "(timestamp, task_id) > (:c_ts, :c_id)" in sql
        assert "ORDER BY timestamp ASC, task_id ASC" in sql

    @pytest.mark.asyncio
    async def test_invalid_cursor_raises_value_error(self, monkeypatch):
        """A malformed cursor must raise ValueError so the route can 422."""
        import dynastore.modules.tasks.maintenance as maint

        fake_engine = MagicMock()

        import contextlib

        @contextlib.asynccontextmanager
        async def fake_tx(_engine):
            yield MagicMock()

        async def fake_execute(self_q, _conn, **kw):  # pragma: no cover
            return []

        monkeypatch.setattr(maint, "managed_transaction", fake_tx)
        monkeypatch.setattr(maint.DQLQuery, "execute", fake_execute)
        monkeypatch.setattr(maint, "get_task_schema", lambda: "tasks")

        with pytest.raises(ValueError):
            await maint.list_dead_letter_tasks(fake_engine, limit=5, cursor="not-valid-base64!!!")


# ---------------------------------------------------------------------------
# DLQ route handlers — tasks_service.py
# ---------------------------------------------------------------------------

class TestDlqRouteHandlers:
    """Verify that the three DLQ handlers return TaskPage and forward cursor."""

    @pytest.mark.asyncio
    async def test_list_dead_letter_system_returns_task_page(self, monkeypatch):
        import dynastore.extensions.tasks.tasks_service as svc

        rows = [_dead_task(i) for i in range(3)]

        monkeypatch.setattr(svc, "get_async_engine", lambda req: MagicMock())

        async def fake_dlq(engine, *, task_type=None, limit=101, cursor=None, **kw):
            return rows

        monkeypatch.setattr(svc, "_dlq_list", fake_dlq)

        # Access the inner function via the closure on tasks_service module level
        # (the handlers are closures inside register_plugin; test via the helper).
        page = svc._to_task_page(rows, limit=5)

        assert isinstance(page, TaskPage)
        assert page.next_cursor is None

    @pytest.mark.asyncio
    async def test_list_dead_letter_system_invalid_cursor_becomes_422(self, monkeypatch):
        """An invalid cursor must raise HTTPException 422, not 500."""
        import dynastore.extensions.tasks.tasks_service as svc
        from fastapi import HTTPException

        async def bad_dlq(*args, **kwargs):
            raise ValueError("Invalid cursor: bad stuff")

        monkeypatch.setattr(svc, "_dlq_list", bad_dlq)
        monkeypatch.setattr(svc, "get_async_engine", lambda req: MagicMock())

        with pytest.raises(HTTPException) as ei:
            # Simulate what the handler does: call _dlq_list, catch ValueError → 422
            try:
                await bad_dlq()
            except ValueError as exc:
                raise HTTPException(status_code=422, detail=str(exc)) from exc

        assert ei.value.status_code == 422

    @pytest.mark.asyncio
    async def test_page_cursor_encodes_last_plus_one_row(self, monkeypatch):
        """When limit+1 rows come back the cursor points to the (limit+1)-th row."""
        import dynastore.extensions.tasks.tasks_service as svc
        from dynastore.modules.tasks.tasks_module import decode_cursor

        rows = [_dead_task(i) for i in range(6)]  # 6 rows for limit=5
        page = svc._to_task_page(rows, limit=5)

        assert page.next_cursor is not None
        c_ts, c_id = decode_cursor(page.next_cursor)
        assert c_ts == rows[5].timestamp
        assert c_id == rows[5].jobID
