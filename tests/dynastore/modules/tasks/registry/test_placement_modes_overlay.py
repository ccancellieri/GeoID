"""_overlay_routing_modes: routing config overrides observed modes, fail-open."""
from __future__ import annotations

import pytest

from dynastore.modules.tasks.routing.model import RunnerTarget
from dynastore.modules.tasks.routing.exec_hints import ExecHint
from dynastore.modules.tasks.routing import resolver as routing_resolver
from dynastore.modules.tasks.registry import publisher
from dynastore.modules.tasks.registry.model import CapabilityRow


def _row(task_key: str, modes):
    return CapabilityRow(
        service="catalog", task_key=task_key, kind="task", modes=list(modes),
        service_version="v", service_commit="c", version="c",
    )


@pytest.mark.asyncio
async def test_routing_offload_overrides_observed(monkeypatch):
    """A gcp_cloud_run target maps to off_load mode in the registry row."""
    target = RunnerTarget(service="worker", runner="gcp_cloud_run", hints={ExecHint.OFFLOAD})

    async def _targets(task_key):
        return [target]

    monkeypatch.setattr(routing_resolver, "resolved_targets_for", _targets)
    rows = [_row("reindex", ["async"])]
    await publisher._overlay_routing_modes(rows)
    assert rows[0].modes == ["off_load"]


@pytest.mark.asyncio
async def test_no_routing_entry_leaves_observed_modes(monkeypatch):
    """Empty target list leaves observed modes unchanged."""
    async def _none(task_key):
        return []

    monkeypatch.setattr(routing_resolver, "resolved_targets_for", _none)
    rows = [_row("reindex", ["async", "sync"])]
    await publisher._overlay_routing_modes(rows)
    assert rows[0].modes == ["async", "sync"]


@pytest.mark.asyncio
async def test_resolver_error_is_fail_open(monkeypatch):
    """Resolver exception leaves observed modes unchanged (fail-open)."""
    async def _boom(task_key):
        raise RuntimeError("routing config unreadable")

    monkeypatch.setattr(routing_resolver, "resolved_targets_for", _boom)
    rows = [_row("reindex", ["async"])]
    await publisher._overlay_routing_modes(rows)
    assert rows[0].modes == ["async"]  # untouched


@pytest.mark.asyncio
async def test_background_runner_maps_to_async(monkeypatch):
    """A background runner target maps to async mode."""
    target = RunnerTarget(service="catalog", runner="background", hints={ExecHint.BACKGROUND})

    async def _targets(task_key):
        return [target]

    monkeypatch.setattr(routing_resolver, "resolved_targets_for", _targets)
    rows = [_row("cleanup", ["async"])]
    await publisher._overlay_routing_modes(rows)
    assert rows[0].modes == ["async"]
