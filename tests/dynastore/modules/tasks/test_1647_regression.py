"""Regression for the class of bug behind the cascade-cleanup starvation: a
mandatory, capability-less task with no live correct-tier owner must be flagged
unowned AND reported as unclaimable, so the backstop dead-letters it instead of
leaving it PENDING forever. Covers the subtle variant too: an owner exists, but
not at the correct tier. Pure-static — no DB.
"""
from __future__ import annotations

import pytest

import dynastore.tasks as tasks_pkg
from dynastore.modules.tasks import mandatory


class _FakeCls:
    """Stands in for a loaded, mandatory, catalog-tier system task."""
    mandatory = True
    affinity_tier = "catalog"


class _FakeCfg:
    cls = _FakeCls
    definition = None  # kind == "task"


@pytest.mark.asyncio
async def test_capabilityless_mandatory_is_flagged_unowned(monkeypatch):
    # No owner advertises the task at all -> violation (the starvation state).
    async def _no_owners(_engine, _task_key, _grace):
        return []
    monkeypatch.setattr(mandatory, "_live_owners_for", _no_owners)
    monkeypatch.setattr(mandatory, "_mandatory_specs", lambda: [("cascade_cleanup", "catalog")])

    violations = await mandatory.check_mandatory_ownership(engine=object(), ttl_grace_seconds=90)
    assert "cascade_cleanup" in violations


@pytest.mark.asyncio
async def test_capabilityless_mandatory_is_unclaimable(monkeypatch):
    # find_unclaimable_task_types walks the real task registry, so inject a fake
    # one holding the mandatory catalog-tier task; with no live owner it is a
    # backstop DLQ target (no silent starvation).
    async def _no_owners(_engine, _task_key, _grace):
        return []
    monkeypatch.setattr(mandatory, "_live_owners_for", _no_owners)
    monkeypatch.setattr(tasks_pkg, "_DYNASTORE_TASKS", {"cascade_cleanup": _FakeCfg()})

    unclaimable = await mandatory.find_unclaimable_task_types(engine=object(), ttl_grace_seconds=90)
    assert "cascade_cleanup" in unclaimable


@pytest.mark.asyncio
async def test_wrong_tier_owner_still_unclaimable(monkeypatch):
    # Advertised only by a wrong-tier service: "has an owner" but not the
    # correct-tier owner — the subtle variant of the starvation bug.
    async def _worker_only(_engine, _task_key, _grace):
        return [{"service": "worker", "affinity_tier": None}]
    monkeypatch.setattr(mandatory, "_live_owners_for", _worker_only)
    monkeypatch.setattr(mandatory, "_mandatory_specs", lambda: [("cascade_cleanup", "catalog")])

    violations = await mandatory.check_mandatory_ownership(engine=object(), ttl_grace_seconds=90)
    assert "cascade_cleanup" in violations
