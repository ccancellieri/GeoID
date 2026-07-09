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

"""Unit tests for the cold-boot contributor registry and orchestrator.

No database or IAM imports required.  Each test re-imports the registry
module in a clean state by using a module-level patched ``_REGISTRY`` list.
"""
from __future__ import annotations

import logging
from typing import Any
from unittest.mock import AsyncMock, patch

import pytest


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_contributor(name: str, priority: int, side_effect: Any = None) -> object:
    """Return a minimal ColdBootContributor-compatible object."""

    class _C:
        pass

    c = _C()
    c.name = name  # type: ignore[attr-defined]
    c.priority = priority  # type: ignore[attr-defined]
    mock = AsyncMock(side_effect=side_effect)
    c.run = mock  # type: ignore[attr-defined]
    return c


# ---------------------------------------------------------------------------
# Registry ordering tests
# ---------------------------------------------------------------------------


def test_register_and_get_sorted_by_priority_desc():
    """Contributors are returned in descending priority order."""
    from dynastore.modules.presets.cold_boot import (
        get_cold_boot_contributors,
        register_cold_boot_contributor,
    )

    low = _make_contributor("low", 10)
    high = _make_contributor("high", 100)
    mid = _make_contributor("mid", 50)

    with patch("dynastore.modules.presets.cold_boot._REGISTRY", []):
        register_cold_boot_contributor(low)
        register_cold_boot_contributor(high)
        register_cold_boot_contributor(mid)

        result = get_cold_boot_contributors()

    assert [c.name for c in result] == ["high", "mid", "low"]


def test_tie_broken_by_registration_order():
    """Same-priority contributors keep insertion order (stable sort)."""
    from dynastore.modules.presets.cold_boot import (
        get_cold_boot_contributors,
        register_cold_boot_contributor,
    )

    a = _make_contributor("alpha", 50)
    b = _make_contributor("beta", 50)
    c = _make_contributor("gamma", 50)

    with patch("dynastore.modules.presets.cold_boot._REGISTRY", []):
        register_cold_boot_contributor(a)
        register_cold_boot_contributor(b)
        register_cold_boot_contributor(c)

        result = get_cold_boot_contributors()

    assert [x.name for x in result] == ["alpha", "beta", "gamma"]


def test_duplicate_name_raises():
    """Registering two contributors with the same name raises ValueError."""
    from dynastore.modules.presets.cold_boot import register_cold_boot_contributor

    a = _make_contributor("dup", 10)
    b = _make_contributor("dup", 20)

    with patch("dynastore.modules.presets.cold_boot._REGISTRY", []):
        register_cold_boot_contributor(a)
        with pytest.raises(ValueError, match="dup"):
            register_cold_boot_contributor(b)


# ---------------------------------------------------------------------------
# Orchestrator tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_run_cold_boot_calls_all_contributors_in_order():
    """run_cold_boot calls every registered contributor, highest priority first."""
    from dynastore.modules.presets.cold_boot import run_cold_boot

    calls: list[str] = []

    async def _run_a(engine: Any) -> None:
        calls.append("a")

    async def _run_b(engine: Any) -> None:
        calls.append("b")

    low = _make_contributor("low", 10)
    high = _make_contributor("high", 100)
    low.run = AsyncMock(side_effect=_run_b)
    high.run = AsyncMock(side_effect=_run_a)

    sentinel_engine = object()

    with patch("dynastore.modules.presets.cold_boot._REGISTRY", [low, high]):
        await run_cold_boot(sentinel_engine)

    # High priority must run first → "a" then "b"
    assert calls == ["a", "b"]

    # Engine is forwarded to each contributor
    high.run.assert_awaited_once_with(sentinel_engine)
    low.run.assert_awaited_once_with(sentinel_engine)


@pytest.mark.asyncio
async def test_run_cold_boot_notifies_probe_around_each_contributor():
    """A probe sees contributor start/end timing without changing execution."""
    from dynastore.modules.presets.cold_boot import run_cold_boot

    events: list[tuple[str, str, float | None, BaseException | None]] = []

    def _probe(event: str, contributor: Any, elapsed: float | None, error: BaseException | None) -> None:
        events.append((event, contributor.name, elapsed, error))

    contributor = _make_contributor("diag", 10)

    with patch("dynastore.modules.presets.cold_boot._REGISTRY", [contributor]):
        await run_cold_boot(object(), probe=_probe)

    assert events[0] == ("before", "diag", None, None)
    assert events[1][0:2] == ("after", "diag")
    assert events[1][2] is not None
    assert events[1][2] >= 0
    assert events[1][3] is None


@pytest.mark.asyncio
async def test_run_cold_boot_notifies_probe_after_failure():
    """Probe failures are diagnostic only; contributor failures are still reported."""
    from dynastore.modules.presets.cold_boot import run_cold_boot

    events: list[tuple[str, str, BaseException | None]] = []

    def _probe(event: str, contributor: Any, elapsed: float | None, error: BaseException | None) -> None:
        events.append((event, contributor.name, error))

    boom = _make_contributor("boom", 100)
    boom.run = AsyncMock(side_effect=RuntimeError("cold-boot explosion"))

    with patch("dynastore.modules.presets.cold_boot._REGISTRY", [boom]):
        await run_cold_boot(None, probe=_probe)

    assert events[0] == ("before", "boom", None)
    assert events[1][0:2] == ("after", "boom")
    assert isinstance(events[1][2], RuntimeError)


@pytest.mark.asyncio
async def test_run_cold_boot_continues_after_failure(caplog):
    """A contributor that raises must not prevent subsequent contributors from running."""
    from dynastore.modules.presets.cold_boot import run_cold_boot

    calls: list[str] = []

    async def _run_ok(engine: Any) -> None:
        calls.append("ok")

    boom = _make_contributor("boom", 100)
    boom.run = AsyncMock(side_effect=RuntimeError("cold-boot explosion"))

    ok = _make_contributor("ok", 10)
    ok.run = AsyncMock(side_effect=_run_ok)

    with patch("dynastore.modules.presets.cold_boot._REGISTRY", [boom, ok]), caplog.at_level(
        logging.ERROR, logger="dynastore.modules.presets.cold_boot"
    ):
        await run_cold_boot(None)

    assert "ok" in calls, "The 'ok' contributor must run even after 'boom' failed"
    assert any(
        "boom" in rec.getMessage() for rec in caplog.records if rec.levelno >= logging.ERROR
    ), "Expected an ERROR log naming the failed contributor"


@pytest.mark.asyncio
async def test_run_cold_boot_no_contributors_is_noop(caplog):
    """run_cold_boot with an empty registry is a silent no-op."""
    from dynastore.modules.presets.cold_boot import run_cold_boot

    with patch("dynastore.modules.presets.cold_boot._REGISTRY", []):
        await run_cold_boot(None)  # must not raise


# ---------------------------------------------------------------------------
# FilePresetSeederContributor tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_file_preset_seeder_noop_when_dir_missing(tmp_path, caplog):
    """FilePresetSeederContributor.run is a no-op when PRESETS_DIR does not exist."""
    from dynastore.modules.presets.preset_seeder import FilePresetSeederContributor

    missing = tmp_path / "no_such_dir"
    contributor = FilePresetSeederContributor()

    with patch("dynastore.modules.presets.preset_seeder.PRESETS_DIR", missing):
        await contributor.run(None)  # must not raise


@pytest.mark.asyncio
async def test_file_preset_seeder_applies_json_files(tmp_path):
    """FilePresetSeederContributor enumerates JSON files and calls bootstrap_presets."""
    from dynastore.modules.presets.preset_seeder import FilePresetSeederContributor

    (tmp_path / "demo.json").write_text('{"preset_name": "demo", "params": {}}')

    applied_calls: list[tuple] = []

    async def _fake_bootstrap(engine: Any, payloads: Any, **kw: Any) -> dict:
        applied_calls.append((engine, payloads))
        return {"demo": True}

    # load_preset_payloads reads from its own PRESETS_DIR; patch both so
    # the seeder sees the tmp_path as its file root.
    def _fake_load_payloads(name: str) -> list | None:
        import json
        candidate = tmp_path / f"{name}.json"
        if not candidate.exists():
            return None
        raw = json.loads(candidate.read_text())
        return [raw]

    contributor = FilePresetSeederContributor()
    sentinel_engine = object()

    with (
        patch("dynastore.modules.presets.preset_seeder.PRESETS_DIR", tmp_path),
        patch(
            "dynastore.modules.presets.preset_seeder.load_preset_payloads",
            side_effect=_fake_load_payloads,
        ),
        patch(
            "dynastore.modules.presets.preset_seeder.bootstrap_presets",
            side_effect=_fake_bootstrap,
        ),
    ):
        await contributor.run(sentinel_engine)

    assert len(applied_calls) == 1
    engine_arg, payloads_arg = applied_calls[0]
    assert engine_arg is sentinel_engine
    # The payloads list must contain our demo entry.
    assert any(
        isinstance(p, dict) and p.get("preset_name") == "demo"
        for p in payloads_arg
    ), f"Expected 'demo' preset in payloads; got {payloads_arg}"


@pytest.mark.asyncio
async def test_file_preset_seeder_continues_after_file_error(tmp_path, caplog):
    """FilePresetSeederContributor logs ERROR and continues when bootstrap_presets raises."""
    from dynastore.modules.presets.preset_seeder import FilePresetSeederContributor

    (tmp_path / "bad.json").write_text('{"preset_name": "bad", "params": {}}')
    (tmp_path / "good.json").write_text('{"preset_name": "good", "params": {}}')

    good_applied: list[str] = []

    async def _fake_bootstrap(engine: Any, payloads: Any, **kw: Any) -> dict:
        for p in payloads:
            name = p.get("preset_name", "") if isinstance(p, dict) else ""
            if name == "bad":
                raise RuntimeError("bad preset exploded")
            good_applied.append(name)
        return {}

    def _fake_load(name: str) -> list | None:
        import json
        candidate = tmp_path / f"{name}.json"
        return [json.loads(candidate.read_text())] if candidate.exists() else None

    contributor = FilePresetSeederContributor()

    with (
        patch("dynastore.modules.presets.preset_seeder.PRESETS_DIR", tmp_path),
        patch("dynastore.modules.presets.preset_seeder.load_preset_payloads", side_effect=_fake_load),
        patch("dynastore.modules.presets.preset_seeder.bootstrap_presets", side_effect=_fake_bootstrap),
        caplog.at_level(logging.ERROR, logger="dynastore.modules.presets.preset_seeder"),
    ):
        await contributor.run(None)

    # "good" must still be processed even when "bad" raised
    assert "good" in good_applied, f"Expected 'good' in {good_applied}"
    # ERROR must be logged for the bad file
    assert any("bad" in rec.getMessage() for rec in caplog.records if rec.levelno >= logging.ERROR)
