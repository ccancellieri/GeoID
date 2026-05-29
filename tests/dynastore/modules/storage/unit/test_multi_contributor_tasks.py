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

"""Unit tests for the task-contributor kind of ``MultiContributorPreset``.

Pure-Python — ``execute_process`` is monkeypatched, so no DB, no runners, no
FastAPI. Covers the load-bearing invariants:

- apply submits each ``TaskSeed`` to the OGC Process engine and records the
  returned job id in the descriptor's ``tasks`` list;
- a dedup hit (``execute_process`` returns ``None``) is recorded as
  ``job_id=None`` / ``deduped=True``;
- a task contributor with no ``ctx.db`` raises a clear error;
- dry_run emits a ``trigger_task`` entry;
- revoke never re-submits the job (a triggered job cannot be un-run).
"""
from __future__ import annotations

import asyncio
from types import SimpleNamespace
from typing import Any, List

import pytest

from dynastore.modules.storage.presets.multi_contributor import MultiContributorPreset
from dynastore.modules.storage.presets.preset import AppliedDescriptor, NoParams, TaskSeed


def _ctx(db: Any) -> Any:
    """A PresetContext-shaped object — only ``db`` / ``principal`` matter here."""
    return SimpleNamespace(
        db=db, catalogs=None, config=None, iam=None, policy=None,
        principal=None, scope="platform",
    )


def _task_preset(seeds) -> MultiContributorPreset:
    contributor = SimpleNamespace(get_tasks=lambda: list(seeds))
    return MultiContributorPreset(
        name="_test_task_preset",
        description="test",
        keywords=("data",),
        contributors_factory=lambda: [contributor],
    )


def _patch_execute_process(monkeypatch, result, calls: List[dict]):
    async def _fake(process_id, execution_request, engine=None, caller_id=None,
                    preferred_mode=None, dedup_key=None, **kw):
        calls.append({
            "process_id": process_id,
            "inputs": dict(execution_request.inputs),
            "engine": engine,
            "caller_id": caller_id,
            "preferred_mode": preferred_mode,
            "dedup_key": dedup_key,
        })
        return result

    monkeypatch.setattr(
        "dynastore.modules.processes.processes_module.execute_process", _fake,
    )


def test_apply_submits_task_and_records_job_id(monkeypatch):
    calls: List[dict] = []
    _patch_execute_process(monkeypatch, SimpleNamespace(jobID="job-123"), calls)

    seed = TaskSeed(process_id="dimensions_materialize", inputs={}, async_mode=True,
                    dedup_key="dk")
    preset = _task_preset([seed])
    descriptor = asyncio.run(preset.apply(NoParams(), "platform", _ctx(object())))

    assert len(calls) == 1
    assert calls[0]["process_id"] == "dimensions_materialize"
    assert calls[0]["dedup_key"] == "dk"
    from dynastore.modules.processes import models as _m
    assert calls[0]["preferred_mode"] == _m.JobControlOptions.ASYNC_EXECUTE

    tasks = descriptor.payload["tasks"]
    assert tasks == [{
        "process_id": "dimensions_materialize",
        "job_id": "job-123",
        "deduped": False,
    }]


def test_apply_records_dedup_hit_as_none(monkeypatch):
    calls: List[dict] = []
    _patch_execute_process(monkeypatch, None, calls)  # None == dedup hit

    seed = TaskSeed(process_id="dimensions_materialize")
    preset = _task_preset([seed])
    descriptor = asyncio.run(preset.apply(NoParams(), "platform", _ctx(object())))

    rec = descriptor.payload["tasks"][0]
    assert rec["job_id"] is None
    assert rec["deduped"] is True


def test_sync_mode_seed_uses_sync_execute(monkeypatch):
    calls: List[dict] = []
    _patch_execute_process(monkeypatch, SimpleNamespace(task_id="t-9"), calls)

    seed = TaskSeed(process_id="some_proc", async_mode=False)
    preset = _task_preset([seed])
    asyncio.run(preset.apply(NoParams(), "platform", _ctx(object())))

    from dynastore.modules.processes import models as _m
    assert calls[0]["preferred_mode"] == _m.JobControlOptions.SYNC_EXECUTE


def test_apply_with_task_contributor_but_no_db_raises():
    seed = TaskSeed(process_id="dimensions_materialize")
    preset = _task_preset([seed])
    with pytest.raises(RuntimeError, match="db"):
        asyncio.run(preset.apply(NoParams(), "platform", _ctx(None)))


def test_dry_run_emits_trigger_task_entry():
    seed = TaskSeed(process_id="dimensions_materialize", inputs={"force": True})
    preset = _task_preset([seed])
    plan = asyncio.run(preset.dry_run(NoParams(), "platform", _ctx(None)))

    entries = [e for e in plan.entries if e.kind == "trigger_task"]
    assert len(entries) == 1
    assert entries[0].target == "dimensions_materialize"
    assert entries[0].detail["async"] is True
    assert entries[0].detail["inputs"] == {"force": True}


def test_revoke_does_not_resubmit_the_job(monkeypatch):
    calls: List[dict] = []
    _patch_execute_process(monkeypatch, SimpleNamespace(jobID="job-1"), calls)

    descriptor = AppliedDescriptor(payload={
        "tasks": [{"process_id": "dimensions_materialize", "job_id": "job-1"}],
        "data": [],
        "policy_ids": [], "role_names": [], "config_qualnames": [],
        "scope": "platform",
    })
    preset = _task_preset([TaskSeed(process_id="dimensions_materialize")])
    # Must not raise and must not submit anything during revoke.
    asyncio.run(preset.revoke(descriptor, _ctx(object())))
    assert calls == []
