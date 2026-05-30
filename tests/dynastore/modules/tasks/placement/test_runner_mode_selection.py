"""off_load picks the preset runner_type; async uses top priority."""
from __future__ import annotations

import pytest

from dynastore.modules.tasks import execution


class _Runner:
    def __init__(self, runner_type, priority, can=True):
        self.runner_type = runner_type
        self.priority = priority
        self._can = can
    def can_handle(self, _t):
        return self._can


def _aval(value):
    async def _f(*a, **k):
        return value
    return _f


@pytest.mark.asyncio
async def test_off_load_prefers_preset_runner(monkeypatch):
    runners = [_Runner("background", 100), _Runner("gcp_cloud_run", 10)]
    monkeypatch.setattr(execution, "_candidate_runners", lambda mode: runners)
    monkeypatch.setattr(execution, "_resolved_mode", _aval("off_load"))
    monkeypatch.setattr(execution, "_off_load_runner_type", _aval("gcp_cloud_run"))

    chosen = await execution.select_runner_for("gdal")
    assert chosen.runner_type == "gcp_cloud_run"


@pytest.mark.asyncio
async def test_async_uses_top_priority(monkeypatch):
    runners = [_Runner("background", 100), _Runner("gcp_cloud_run", 10)]
    monkeypatch.setattr(execution, "_candidate_runners", lambda mode: runners)
    monkeypatch.setattr(execution, "_resolved_mode", _aval("async"))
    monkeypatch.setattr(execution, "_off_load_runner_type", _aval("gcp_cloud_run"))

    chosen = await execution.select_runner_for("gdal")
    assert chosen.runner_type == "background"  # top priority among async
