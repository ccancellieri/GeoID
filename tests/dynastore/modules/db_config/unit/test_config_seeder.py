"""Unit tests for the JSON-defaults bootstrap (``config_seeder.py``).

Black-box: we patch ``DEFAULTS_DIR`` to a tmp folder, mock the advisory-lock
helper (no real PG), and assert the right ``set_config`` calls. Tests do NOT
reload the module — that mutates global state and breaks parallel test workers.
"""
from __future__ import annotations

import json
from contextlib import asynccontextmanager
from unittest.mock import AsyncMock, patch

import pytest

from dynastore.modules.db_config import config_seeder as seeder
from dynastore.modules.tasks.placement.model import TaskPlacementConfig


@asynccontextmanager
async def _fake_lock(_engine, _key):
    yield object()  # any non-None value = "we hold it"


def _write_seed(dir_, name, payload):
    dir_.mkdir(parents=True, exist_ok=True)
    (dir_ / name).write_text(json.dumps(payload))


@pytest.mark.asyncio
async def test_no_defaults_dir_is_noop(monkeypatch, tmp_path):
    monkeypatch.setattr(seeder, "DEFAULTS_DIR", tmp_path / "nope")
    # No defaults/ subfolder — silent no-op.
    await seeder.seed_default_configs(engine=object())


@pytest.mark.asyncio
async def test_no_files_is_noop(monkeypatch, tmp_path):
    (tmp_path / "defaults").mkdir()
    monkeypatch.setattr(seeder, "DEFAULTS_DIR", tmp_path / "defaults")
    await seeder.seed_default_configs(engine=object())


@pytest.mark.asyncio
async def test_applies_when_no_existing_config(monkeypatch, tmp_path):
    _write_seed(tmp_path / "defaults", "task-placement.json", {
        "class_key": "task_placement_config",
        "value": {"placements": {"t_a": {"consumers": ["catalog"], "mode": "off_load"}}},
    })
    monkeypatch.setattr(seeder, "DEFAULTS_DIR", tmp_path / "defaults")

    config_mgr = AsyncMock()
    config_mgr.list_configs = AsyncMock(return_value={})
    config_mgr.set_config = AsyncMock()

    with patch("dynastore.tools.discovery.get_protocol", return_value=config_mgr), \
         patch.object(seeder, "acquire_startup_lock", _fake_lock):
        await seeder.seed_default_configs(engine=object())

    config_mgr.set_config.assert_awaited_once()
    cls_arg, cfg_arg = config_mgr.set_config.await_args.args
    assert cls_arg is TaskPlacementConfig
    assert isinstance(cfg_arg, TaskPlacementConfig)
    assert cfg_arg.placements["t_a"].consumers == ["catalog"]


@pytest.mark.asyncio
async def test_skip_when_existing_no_override(monkeypatch, tmp_path):
    _write_seed(tmp_path / "defaults", "task-placement.json", {
        "class_key": "task_placement_config",
        "value": {"placements": {"t_a": {"consumers": ["maps"], "mode": "off_load"}}},
    })
    monkeypatch.setattr(seeder, "DEFAULTS_DIR", tmp_path / "defaults")

    config_mgr = AsyncMock()
    config_mgr.list_configs = AsyncMock(
        return_value={TaskPlacementConfig: TaskPlacementConfig()},
    )
    config_mgr.set_config = AsyncMock()

    with patch("dynastore.tools.discovery.get_protocol", return_value=config_mgr), \
         patch.object(seeder, "acquire_startup_lock", _fake_lock):
        await seeder.seed_default_configs(engine=object())

    config_mgr.set_config.assert_not_awaited()


@pytest.mark.asyncio
async def test_override_forces_apply(monkeypatch, tmp_path):
    _write_seed(tmp_path / "defaults", "task-placement.json", {
        "class_key": "task_placement_config",
        "value": {"placements": {"t_a": {"consumers": ["maps"], "mode": "off_load"}}},
        "override": True,
    })
    monkeypatch.setattr(seeder, "DEFAULTS_DIR", tmp_path / "defaults")

    config_mgr = AsyncMock()
    config_mgr.list_configs = AsyncMock(
        return_value={TaskPlacementConfig: TaskPlacementConfig()},
    )
    config_mgr.set_config = AsyncMock()

    with patch("dynastore.tools.discovery.get_protocol", return_value=config_mgr), \
         patch.object(seeder, "acquire_startup_lock", _fake_lock):
        await seeder.seed_default_configs(engine=object())

    config_mgr.set_config.assert_awaited_once()


@pytest.mark.asyncio
async def test_lexical_overlay_last_wins(monkeypatch, tmp_path):
    _write_seed(tmp_path / "defaults", "10-base.json", {
        "class_key": "task_placement_config",
        "value": {"placements": {"t_a": {"consumers": ["catalog"], "mode": "off_load"}}},
    })
    _write_seed(tmp_path / "defaults", "20-overlay.json", {
        "class_key": "task_placement_config",
        "value": {"placements": {"t_a": {"consumers": ["maps"], "mode": "off_load"}}},
    })
    monkeypatch.setattr(seeder, "DEFAULTS_DIR", tmp_path / "defaults")

    config_mgr = AsyncMock()
    config_mgr.list_configs = AsyncMock(return_value={})
    config_mgr.set_config = AsyncMock()

    with patch("dynastore.tools.discovery.get_protocol", return_value=config_mgr), \
         patch.object(seeder, "acquire_startup_lock", _fake_lock):
        await seeder.seed_default_configs(engine=object())

    config_mgr.set_config.assert_awaited_once()
    _, cfg_arg = config_mgr.set_config.await_args.args
    assert cfg_arg.placements["t_a"].consumers == ["maps"]


@pytest.mark.asyncio
@pytest.mark.parametrize("env_value", [None, "prod", "production", "PROD"])
async def test_unknown_class_key_warns_and_skips(
    monkeypatch, tmp_path, caplog, env_value,
):
    # An unknown class_key must never abort boot — it is tolerated in every
    # tier (warn+skip), so a stale seed from a not-yet-redeployed sibling repo
    # cannot crash startup. Models a cross-repo config rename.
    _write_seed(tmp_path / "defaults", "bogus.json", {
        "class_key": "NoSuchPluginConfig",
        "value": {"x": 1},
    })
    monkeypatch.setattr(seeder, "DEFAULTS_DIR", tmp_path / "defaults")
    if env_value is None:
        monkeypatch.delenv("DYNASTORE_ENV", raising=False)
        monkeypatch.delenv("ENVIRONMENT", raising=False)
    else:
        monkeypatch.setenv("DYNASTORE_ENV", env_value)

    config_mgr = AsyncMock()
    config_mgr.list_configs = AsyncMock(return_value={})
    config_mgr.set_config = AsyncMock()

    caplog.set_level("WARNING")
    with patch("dynastore.tools.discovery.get_protocol", return_value=config_mgr), \
         patch.object(seeder, "acquire_startup_lock", _fake_lock):
        # Must NOT raise in any tier.
        await seeder.seed_default_configs(engine=object())

    config_mgr.set_config.assert_not_awaited()
    assert any(
        "unknown class_key" in r.message.lower() and r.levelname == "WARNING"
        for r in caplog.records
    )


@pytest.mark.asyncio
async def test_unknown_class_coexists_with_valid_seed(monkeypatch, tmp_path, caplog):
    # The cross-repo rename window: a stale seed (old class, now unknown) sits
    # next to a valid seed (new class). The valid one is applied; the unknown
    # one warns+skips; boot does not abort. This is what makes the config
    # rename deploy order-independent across repos.
    _write_seed(tmp_path / "defaults", "00-stale.json", {
        "class_key": "task_routing_config",  # renamed away → unknown to this build
        "value": {"placements": {}},
    })
    _write_seed(tmp_path / "defaults", "10-current.json", {
        "class_key": "task_placement_config",
        "value": {"placements": {"t_a": {"consumers": ["catalog"], "mode": "async"}}},
    })
    monkeypatch.setattr(seeder, "DEFAULTS_DIR", tmp_path / "defaults")
    monkeypatch.delenv("DYNASTORE_ENV", raising=False)
    monkeypatch.delenv("ENVIRONMENT", raising=False)

    config_mgr = AsyncMock()
    config_mgr.list_configs = AsyncMock(return_value={})
    config_mgr.set_config = AsyncMock()

    caplog.set_level("WARNING")
    with patch("dynastore.tools.discovery.get_protocol", return_value=config_mgr), \
         patch.object(seeder, "acquire_startup_lock", _fake_lock):
        # The unknown stale seed must not abort the valid one.
        await seeder.seed_default_configs(engine=object())

    config_mgr.set_config.assert_awaited_once()
    cls_arg, _ = config_mgr.set_config.await_args.args
    assert cls_arg is TaskPlacementConfig
    assert any("unknown class_key" in r.message.lower() for r in caplog.records)


@pytest.mark.asyncio
async def test_missing_class_key_raises_in_non_prod(monkeypatch, tmp_path):
    _write_seed(tmp_path / "defaults", "no-key.json", {"value": {"x": 1}})
    monkeypatch.setattr(seeder, "DEFAULTS_DIR", tmp_path / "defaults")
    monkeypatch.delenv("DYNASTORE_ENV", raising=False)
    monkeypatch.delenv("ENVIRONMENT", raising=False)

    config_mgr = AsyncMock()
    config_mgr.list_configs = AsyncMock(return_value={})
    config_mgr.set_config = AsyncMock()

    with patch("dynastore.tools.discovery.get_protocol", return_value=config_mgr), \
         patch.object(seeder, "acquire_startup_lock", _fake_lock):
        with pytest.raises(seeder.ConfigSeederError, match="missing 'class_key'"):
            await seeder.seed_default_configs(engine=object())


@pytest.mark.asyncio
async def test_bad_value_raises_in_non_prod(monkeypatch, tmp_path):
    _write_seed(tmp_path / "defaults", "bad-value.json", {
        "class_key": "task_placement_config",
        "value": "not-an-object",
    })
    monkeypatch.setattr(seeder, "DEFAULTS_DIR", tmp_path / "defaults")
    monkeypatch.delenv("DYNASTORE_ENV", raising=False)
    monkeypatch.delenv("ENVIRONMENT", raising=False)

    config_mgr = AsyncMock()
    config_mgr.list_configs = AsyncMock(return_value={})
    config_mgr.set_config = AsyncMock()

    with patch("dynastore.tools.discovery.get_protocol", return_value=config_mgr), \
         patch.object(seeder, "acquire_startup_lock", _fake_lock):
        with pytest.raises(seeder.ConfigSeederError, match="must be a JSON object"):
            await seeder.seed_default_configs(engine=object())


@pytest.mark.asyncio
async def test_malformed_json_raises_in_non_prod(monkeypatch, tmp_path):
    (tmp_path / "defaults").mkdir()
    (tmp_path / "defaults" / "bad.json").write_text("{not json")
    monkeypatch.setattr(seeder, "DEFAULTS_DIR", tmp_path / "defaults")
    monkeypatch.delenv("DYNASTORE_ENV", raising=False)
    monkeypatch.delenv("ENVIRONMENT", raising=False)

    config_mgr = AsyncMock()
    config_mgr.list_configs = AsyncMock(return_value={})
    config_mgr.set_config = AsyncMock()

    with patch("dynastore.tools.discovery.get_protocol", return_value=config_mgr), \
         patch.object(seeder, "acquire_startup_lock", _fake_lock):
        with pytest.raises(seeder.ConfigSeederError, match="unreadable"):
            await seeder.seed_default_configs(engine=object())

    config_mgr.set_config.assert_not_awaited()


@pytest.mark.asyncio
async def test_no_protocol_registered_returns(monkeypatch, tmp_path, caplog):
    _write_seed(tmp_path / "defaults", "task-placement.json", {
        "class_key": "task_placement_config",
        "value": {"placements": {"t_a": {"consumers": ["catalog"], "mode": "off_load"}}},
    })
    monkeypatch.setattr(seeder, "DEFAULTS_DIR", tmp_path / "defaults")

    caplog.set_level("WARNING")
    with patch("dynastore.tools.discovery.get_protocol", return_value=None):
        await seeder.seed_default_configs(engine=object())

    assert any(
        "PlatformConfigsProtocol not registered" in r.message
        for r in caplog.records
    )
