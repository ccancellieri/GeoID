"""When a mandatory task regains a live correct-tier owner, its DEAD_LETTER rows
are auto-requeued (reusing requeue_dead_letter_tasks_by_type)."""
from __future__ import annotations

import pytest

from dynastore.modules.tasks import dispatcher


@pytest.mark.asyncio
async def test_owner_return_requeues_mandatory(monkeypatch):
    calls = {"requeued": []}

    async def _owners(_engine, task_key, _grace):
        return [{"service": "catalog", "affinity_tier": "catalog"}]  # owner back
    monkeypatch.setattr(dispatcher, "_live_owners_for", _owners)
    monkeypatch.setattr(dispatcher, "_mandatory_specs", lambda: [("cascade_cleanup", "catalog")])

    async def _requeue_by_type(_engine, task_type, **kw):
        calls["requeued"].append(task_type)
        return 2
    monkeypatch.setattr(dispatcher, "_requeue_dead_letter_tasks_by_type", _requeue_by_type)

    n = await dispatcher.auto_requeue_recovered_mandatory(engine=object(), ttl_grace_seconds=90)
    assert n == 2
    assert calls["requeued"] == ["cascade_cleanup"]


@pytest.mark.asyncio
async def test_no_requeue_when_owner_still_missing(monkeypatch):
    async def _no_owners(_engine, _task_key, _grace):
        return []
    monkeypatch.setattr(dispatcher, "_live_owners_for", _no_owners)
    monkeypatch.setattr(dispatcher, "_mandatory_specs", lambda: [("cascade_cleanup", "catalog")])

    async def _requeue_by_type(_engine, task_type, **kw):  # must NOT be called
        raise AssertionError("must not requeue when no correct-tier owner")
    monkeypatch.setattr(dispatcher, "_requeue_dead_letter_tasks_by_type", _requeue_by_type)

    n = await dispatcher.auto_requeue_recovered_mandatory(engine=object(), ttl_grace_seconds=90)
    assert n == 0
