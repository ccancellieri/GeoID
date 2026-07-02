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

"""#914 / #2666 — replay must raise on a silent no-op, not report "ok".

``IndexDispatcher._dispatch_bulk`` already guards against an indexer that
returns ``BulkResult(total=N, succeeded=0, failed=0)`` (call succeeded,
nothing raised, nothing written). ``IndexPropagationTask.run`` replays
those rows post-outbox and must apply the same guard, or a replayed
no-op is silently reported as success and the write is dropped for good.
"""
from __future__ import annotations

from typing import Sequence
from unittest.mock import patch
from uuid import uuid4

import pytest

from dynastore.models.protocols.indexer import (
    BulkResult, IndexContext, IndexOp,
)
from dynastore.modules.tasks.models import TaskPayload
from dynastore.tasks.index_propagation.task import IndexPropagationTask


class _StubIndexer:
    """Indexer stand-in that always returns a fixed ``BulkResult``."""

    def __init__(self, name: str, result: BulkResult) -> None:
        self.__class__.__name__ = name
        self._result = result

    async def ensure_indexer(self, ctx: IndexContext) -> None:  # noqa: D401
        return None

    async def index(self, ctx: IndexContext, op: IndexOp) -> None:  # noqa: D401
        raise AssertionError(
            "IndexPropagationTask.run must call index_bulk, never index"
        )

    async def index_bulk(
        self, ctx: IndexContext, ops: Sequence[IndexOp],
    ) -> BulkResult:
        return self._result


def _payload(ops: list) -> TaskPayload:
    return TaskPayload(task_id=uuid4(), caller_id="test", inputs={
        "indexer_id": "stub_indexer",
        "entity_type": "item",
        "catalog": "cat",
        "collection": "col",
        "ops": ops,
    })


_ONE_OP = [{"entity_id": "e1", "op_type": "upsert", "payload": {"x": 1}}]


@pytest.mark.asyncio
async def test_run_raises_on_silent_noop() -> None:
    """total>0, succeeded=0, failed=0 — the #914 trap — must raise, not
    report ``status="ok"``."""
    fake = _StubIndexer(
        "StubIndexer",
        BulkResult(total=1, succeeded=0, failed=0, failures=[]),
    )
    with patch(
        "dynastore.tasks.index_propagation.task.get_protocols",
        return_value=[fake],
    ):
        with pytest.raises(RuntimeError, match="silent no-op"):
            await IndexPropagationTask().run(_payload(_ONE_OP))


@pytest.mark.asyncio
async def test_run_reports_ok_on_real_success() -> None:
    fake = _StubIndexer(
        "StubIndexer",
        BulkResult(total=1, succeeded=1, failed=0, failures=[]),
    )
    with patch(
        "dynastore.tasks.index_propagation.task.get_protocols",
        return_value=[fake],
    ):
        result = await IndexPropagationTask().run(_payload(_ONE_OP))
    assert result["status"] == "ok"


@pytest.mark.asyncio
async def test_run_reports_partial_on_real_failure() -> None:
    fake = _StubIndexer(
        "StubIndexer",
        BulkResult(
            total=1, succeeded=0, failed=1,
            failures=[{"entity_id": "e1", "error": "mapping"}],
        ),
    )
    with patch(
        "dynastore.tasks.index_propagation.task.get_protocols",
        return_value=[fake],
    ):
        result = await IndexPropagationTask().run(_payload(_ONE_OP))
    assert result["status"] == "partial"


@pytest.mark.asyncio
async def test_run_empty_ops_is_not_a_false_positive() -> None:
    """total=0 (empty ops list) is a legitimate result, not a silent noop —
    must not raise."""
    fake = _StubIndexer(
        "StubIndexer",
        BulkResult(total=0, succeeded=0, failed=0, failures=[]),
    )
    with patch(
        "dynastore.tasks.index_propagation.task.get_protocols",
        return_value=[fake],
    ):
        result = await IndexPropagationTask().run(_payload([]))
    assert result["status"] == "ok"
    assert result["total"] == 0
