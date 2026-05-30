"""_overlay_placement_modes: placement config overrides observed modes, fail-open."""
from __future__ import annotations

import pytest

from dynastore.modules.tasks.placement.model import PlacementEntry
from dynastore.modules.tasks.placement import resolver as placement_resolver
from dynastore.modules.tasks.registry import publisher
from dynastore.modules.tasks.registry.model import CapabilityRow


def _row(task_key: str, modes):
    return CapabilityRow(
        service="catalog", task_key=task_key, kind="task", modes=list(modes),
        service_version="v", service_commit="c", version="c",
    )


@pytest.mark.asyncio
async def test_placement_mode_overrides_observed(monkeypatch):
    async def _fake_resolved_entry(task_key):
        return PlacementEntry(consumers=["worker"], mode="off_load")

    monkeypatch.setattr(placement_resolver, "resolved_entry", _fake_resolved_entry)
    rows = [_row("reindex", ["async"])]
    await publisher._overlay_placement_modes(rows)
    assert rows[0].modes == ["off_load"]


@pytest.mark.asyncio
async def test_no_placement_entry_leaves_observed_modes(monkeypatch):
    async def _none(task_key):
        return None

    monkeypatch.setattr(placement_resolver, "resolved_entry", _none)
    rows = [_row("reindex", ["async", "sync"])]
    await publisher._overlay_placement_modes(rows)
    assert rows[0].modes == ["async", "sync"]


@pytest.mark.asyncio
async def test_resolver_error_is_fail_open(monkeypatch):
    async def _boom(task_key):
        raise RuntimeError("placement config unreadable")

    monkeypatch.setattr(placement_resolver, "resolved_entry", _boom)
    rows = [_row("reindex", ["async"])]
    await publisher._overlay_placement_modes(rows)
    assert rows[0].modes == ["async"]  # untouched
