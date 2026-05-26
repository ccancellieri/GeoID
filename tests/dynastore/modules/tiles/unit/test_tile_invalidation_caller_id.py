"""Contract test: ``enqueue_tile_invalidation_task`` attributes the row to
the originating principal when provided, falls back to a system sentinel
otherwise.

Locks the wiring added so write-reactive tile-cache invalidation (#1296,
#1298) carries the user who triggered the write — not a hardcoded
``"tile_cache_invalidation"`` string that shadowed the real principal.
"""

from __future__ import annotations

from typing import Any, Dict
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from dynastore.modules.tiles import tile_cache_sync


class _FakeFeature:
    def __init__(self, bbox):
        self.bbox = bbox
        self.geometry = None
        self.id = "f1"


def _feature_with_bbox():
    return _FakeFeature([0.0, 0.0, 1.0, 1.0])


@pytest.mark.asyncio
async def test_uses_supplied_caller_id() -> None:
    captured: Dict[str, Any] = {}

    fake_task = MagicMock()
    fake_task.task_id = "t1"

    async def _fake_create_task(_engine, task_create, **kwargs):
        captured["task_create"] = task_create
        captured["kwargs"] = kwargs
        return fake_task

    with patch.object(
        tile_cache_sync, "is_tile_cache_active",
        new=AsyncMock(return_value=True),
    ), patch(
        "dynastore.modules.tasks.tasks_module.create_task",
        new=AsyncMock(side_effect=_fake_create_task),
    ), patch.object(
        tile_cache_sync, "feature_bbox", return_value=(0.0, 0.0, 1.0, 1.0),
    ):
        n = await tile_cache_sync.enqueue_tile_invalidation_task(
            "c1", "col1", [_feature_with_bbox()],
            engine=MagicMock(),
            schema="s_xyz",
            caller_id="user:alice",
        )

    assert n == 1
    tc = captured["task_create"]
    assert tc.caller_id == "user:alice"
    assert tc.collection_id == "col1"
    assert tc.task_type == "tiles_preseed"
    assert captured["kwargs"]["schema"] == "s_xyz"


@pytest.mark.asyncio
async def test_falls_back_to_system_sentinel_when_caller_unknown() -> None:
    """No principal in context → the enqueue still happens but the row is
    attributed to ``"system:tile_cache_invalidation"`` so audit queries on
    ``caller_id IS NULL`` cleanly flag any genuinely unattributed enqueue.
    """
    captured: Dict[str, Any] = {}

    fake_task = MagicMock()
    fake_task.task_id = "t1"

    async def _fake_create_task(_engine, task_create, **kwargs):
        captured["task_create"] = task_create
        return fake_task

    with patch.object(
        tile_cache_sync, "is_tile_cache_active",
        new=AsyncMock(return_value=True),
    ), patch(
        "dynastore.modules.tasks.tasks_module.create_task",
        new=AsyncMock(side_effect=_fake_create_task),
    ), patch.object(
        tile_cache_sync, "feature_bbox", return_value=(0.0, 0.0, 1.0, 1.0),
    ):
        n = await tile_cache_sync.enqueue_tile_invalidation_task(
            "c1", "col1", [_feature_with_bbox()],
            engine=MagicMock(),
            schema="s_xyz",
        )

    assert n == 1
    assert captured["task_create"].caller_id == "system:tile_cache_invalidation"
