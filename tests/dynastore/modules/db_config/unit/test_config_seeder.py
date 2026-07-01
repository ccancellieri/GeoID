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

"""Unit tests for the JSON-defaults bootstrap (``config_seeder.py``).

Black-box: we patch ``DEFAULTS_DIR`` to a tmp folder, mock the advisory-lock
helper (no real PG), and assert the right ``set_config`` calls. Tests do NOT
reload the module — that mutates global state and breaks parallel test workers.
"""
from __future__ import annotations

import json
from contextlib import asynccontextmanager
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from dynastore.modules.db_config import config_seeder as seeder
from dynastore.modules.tasks.routing.model import TaskRoutingConfig


@asynccontextmanager
async def _fake_lock(_engine, _key):
    yield object()  # any non-None value = "we hold it"


def _write_seed(dir_, name, payload):
    dir_.mkdir(parents=True, exist_ok=True)
    (dir_ / name).write_text(json.dumps(payload))


@pytest.mark.asyncio
async def test_no_defaults_dir_seeds_nothing(monkeypatch, tmp_path):
    # No defaults/ subfolder — seeding is a clean no-op (the protocol is never
    # fetched). The legacy outbox_drain key fixup was removed with the
    # migration apparatus (#1807 P4).
    monkeypatch.setattr(seeder, "DEFAULTS_DIR", tmp_path / "nope")
    get_protocol = MagicMock()
    with patch.object(seeder, "acquire_startup_lock", _fake_lock), \
         patch("dynastore.tools.discovery.get_protocol", get_protocol):
        await seeder.seed_default_configs(engine=object())
    get_protocol.assert_not_called()


@pytest.mark.asyncio
async def test_no_files_seeds_nothing(monkeypatch, tmp_path):
    (tmp_path / "defaults").mkdir()
    monkeypatch.setattr(seeder, "DEFAULTS_DIR", tmp_path / "defaults")
    get_protocol = MagicMock()
    with patch.object(seeder, "acquire_startup_lock", _fake_lock), \
         patch("dynastore.tools.discovery.get_protocol", get_protocol):
        await seeder.seed_default_configs(engine=object())
    get_protocol.assert_not_called()


@pytest.mark.asyncio
async def test_applies_when_no_existing_config(monkeypatch, tmp_path):
    _write_seed(tmp_path / "defaults", "task-routing.json", {
        "class_key": "task_routing_config",
        "value": {"tasks": {"t_a": [{"consumers": ["catalog"], "runner": "background"}]}},
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
    assert cls_arg is TaskRoutingConfig
    assert isinstance(cfg_arg, TaskRoutingConfig)
    assert cfg_arg.tasks["t_a"][0].consumers == ["catalog"]


@pytest.mark.asyncio
async def test_skip_when_existing_no_override(monkeypatch, tmp_path):
    _write_seed(tmp_path / "defaults", "task-routing.json", {
        "class_key": "task_routing_config",
        "value": {"tasks": {"t_a": [{"consumers": ["maps"], "runner": "background"}]}},
    })
    monkeypatch.setattr(seeder, "DEFAULTS_DIR", tmp_path / "defaults")

    config_mgr = AsyncMock()
    config_mgr.list_configs = AsyncMock(
        return_value={TaskRoutingConfig: TaskRoutingConfig()},
    )
    config_mgr.set_config = AsyncMock()

    with patch("dynastore.tools.discovery.get_protocol", return_value=config_mgr), \
         patch.object(seeder, "acquire_startup_lock", _fake_lock):
        await seeder.seed_default_configs(engine=object())

    config_mgr.set_config.assert_not_awaited()


@pytest.mark.asyncio
async def test_override_forces_apply(monkeypatch, tmp_path):
    _write_seed(tmp_path / "defaults", "task-routing.json", {
        "class_key": "task_routing_config",
        "value": {"tasks": {"t_a": [{"consumers": ["maps"], "runner": "background"}]}},
        "override": True,
    })
    monkeypatch.setattr(seeder, "DEFAULTS_DIR", tmp_path / "defaults")

    config_mgr = AsyncMock()
    config_mgr.list_configs = AsyncMock(
        return_value={TaskRoutingConfig: TaskRoutingConfig()},
    )
    config_mgr.set_config = AsyncMock()

    with patch("dynastore.tools.discovery.get_protocol", return_value=config_mgr), \
         patch.object(seeder, "acquire_startup_lock", _fake_lock):
        await seeder.seed_default_configs(engine=object())

    config_mgr.set_config.assert_awaited_once()


@pytest.mark.asyncio
async def test_lexical_overlay_last_wins(monkeypatch, tmp_path):
    _write_seed(tmp_path / "defaults", "10-base.json", {
        "class_key": "task_routing_config",
        "value": {"tasks": {"t_a": [{"consumers": ["catalog"], "runner": "background"}]}},
    })
    _write_seed(tmp_path / "defaults", "20-overlay.json", {
        "class_key": "task_routing_config",
        "value": {"tasks": {"t_a": [{"consumers": ["maps"], "runner": "background"}]}},
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
    assert cfg_arg.tasks["t_a"][0].consumers == ["maps"]


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
    # rename deploy order-independent across repos. ``task_placement_config``
    # is the retired class name (replaced by ``task_routing_config``), so it
    # now models the stale, unknown-to-this-build seed.
    _write_seed(tmp_path / "defaults", "00-stale.json", {
        "class_key": "task_placement_config",  # retired → unknown to this build
        "value": {"placements": {}},
    })
    _write_seed(tmp_path / "defaults", "10-current.json", {
        "class_key": "task_routing_config",
        "value": {"tasks": {"t_a": [{"consumers": ["catalog"], "runner": "background"}]}},
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
    assert cls_arg is TaskRoutingConfig
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
        "class_key": "task_routing_config",
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
    _write_seed(tmp_path / "defaults", "task-routing.json", {
        "class_key": "task_routing_config",
        "value": {"tasks": {"t_a": [{"consumers": ["catalog"], "runner": "background"}]}},
    })
    monkeypatch.setattr(seeder, "DEFAULTS_DIR", tmp_path / "defaults")

    caplog.set_level("WARNING")
    with patch("dynastore.tools.discovery.get_protocol", return_value=None), \
         patch.object(seeder, "acquire_startup_lock", _fake_lock):
        await seeder.seed_default_configs(engine=object())

    assert any(
        "PlatformConfigsProtocol not registered" in r.message
        for r in caplog.records
    )


# ---------------------------------------------------------------------------
# Bootstrap guard integration
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_guard_set_skips_non_override_seeds(monkeypatch, tmp_path):
    """When the bootstrap guard is set, non-override seeds are not applied."""
    _write_seed(tmp_path / "defaults", "task-routing.json", {
        "class_key": "task_routing_config",
        "value": {"tasks": {"t_a": [{"consumers": ["catalog"], "runner": "background"}]}},
    })
    monkeypatch.setattr(seeder, "DEFAULTS_DIR", tmp_path / "defaults")

    config_mgr = AsyncMock()
    config_mgr.list_configs = AsyncMock(return_value={})
    config_mgr.set_config = AsyncMock()

    with patch("dynastore.tools.discovery.get_protocol", return_value=config_mgr), \
         patch.object(seeder, "acquire_startup_lock", _fake_lock), \
         patch(
             "dynastore.modules.catalog.bootstrap_guard.is_initialized",
             AsyncMock(return_value=True),
         ):
        await seeder.seed_default_configs(engine=object())

    # Non-override seed must be skipped when guard is set.
    config_mgr.set_config.assert_not_awaited()


@pytest.mark.asyncio
async def test_guard_set_still_applies_override_seeds(monkeypatch, tmp_path):
    """override:true seeds always apply even when the bootstrap guard is set."""
    _write_seed(tmp_path / "defaults", "task-routing.json", {
        "class_key": "task_routing_config",
        "value": {"tasks": {"t_a": [{"consumers": ["catalog"], "runner": "background"}]}},
        "override": True,
    })
    monkeypatch.setattr(seeder, "DEFAULTS_DIR", tmp_path / "defaults")

    config_mgr = AsyncMock()
    config_mgr.list_configs = AsyncMock(return_value={})
    config_mgr.set_config = AsyncMock()

    with patch("dynastore.tools.discovery.get_protocol", return_value=config_mgr), \
         patch.object(seeder, "acquire_startup_lock", _fake_lock), \
         patch(
             "dynastore.modules.catalog.bootstrap_guard.is_initialized",
             AsyncMock(return_value=True),
         ):
        await seeder.seed_default_configs(engine=object())

    # override:true seed must apply despite the guard.
    config_mgr.set_config.assert_awaited_once()


# ---------------------------------------------------------------------------
# Cold-boot storage ensure (#2209)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_ensures_platform_storage_before_seeding(monkeypatch, tmp_path):
    """The seeder ensures configs.platform_configs exists before any set_config.

    Cold-boot regression #2209: PlatformConfigService (priority 0) skips its DDL
    when the engine is not yet up, so on a fresh DB the table can be absent when
    this priority-15 seeder runs — silently dropping idp_config and breaking
    auth. The seeder must self-heal by calling initialize_storage first.
    """
    _write_seed(tmp_path / "defaults", "task-routing.json", {
        "class_key": "task_routing_config",
        "value": {"tasks": {"t_a": [{"consumers": ["catalog"], "runner": "background"}]}},
    })
    monkeypatch.setattr(seeder, "DEFAULTS_DIR", tmp_path / "defaults")

    order: list[str] = []
    config_mgr = AsyncMock()
    config_mgr.list_configs = AsyncMock(return_value={})
    config_mgr.set_config = AsyncMock(side_effect=lambda *a, **k: order.append("set_config"))

    init_storage = AsyncMock(side_effect=lambda *a, **k: order.append("initialize_storage"))

    with patch("dynastore.tools.discovery.get_protocol", return_value=config_mgr), \
         patch.object(seeder, "acquire_startup_lock", _fake_lock), \
         patch(
             "dynastore.modules.db_config.platform_config_service."
             "PlatformConfigService.initialize_storage",
             init_storage,
         ):
        await seeder.seed_default_configs(engine=object())

    init_storage.assert_awaited_once()
    config_mgr.set_config.assert_awaited_once()
    # Storage must be ensured BEFORE the first config write, else the write
    # races the missing table on a fresh DB.
    assert order == ["initialize_storage", "set_config"]


@pytest.mark.asyncio
async def test_storage_ensure_failure_does_not_abort_seeding(
    monkeypatch, tmp_path, caplog,
):
    """A failed storage-ensure is non-fatal: seeding still proceeds (warn only).

    The ensure is a best-effort self-heal; if it fails (e.g. transient DB error)
    boot must not abort — the existing per-row failure handling still applies.
    """
    _write_seed(tmp_path / "defaults", "task-routing.json", {
        "class_key": "task_routing_config",
        "value": {"tasks": {"t_a": [{"consumers": ["catalog"], "runner": "background"}]}},
    })
    monkeypatch.setattr(seeder, "DEFAULTS_DIR", tmp_path / "defaults")

    config_mgr = AsyncMock()
    config_mgr.list_configs = AsyncMock(return_value={})
    config_mgr.set_config = AsyncMock()

    init_storage = AsyncMock(side_effect=RuntimeError("transient DB error"))

    caplog.set_level("WARNING")
    with patch("dynastore.tools.discovery.get_protocol", return_value=config_mgr), \
         patch.object(seeder, "acquire_startup_lock", _fake_lock), \
         patch(
             "dynastore.modules.db_config.platform_config_service."
             "PlatformConfigService.initialize_storage",
             init_storage,
         ):
        # Must NOT raise — ensure failure is best-effort.
        await seeder.seed_default_configs(engine=object())

    init_storage.assert_awaited_once()
    config_mgr.set_config.assert_awaited_once()
    assert any(
        "platform config storage ensure failed" in r.message.lower()
        for r in caplog.records
    )


@pytest.mark.asyncio
async def test_no_storage_ensure_when_no_seeds(monkeypatch, tmp_path):
    """No defaults → no storage-ensure attempt (nothing to write)."""
    monkeypatch.setattr(seeder, "DEFAULTS_DIR", tmp_path / "nope")

    init_storage = AsyncMock()
    with patch("dynastore.tools.discovery.get_protocol", MagicMock()), \
         patch.object(seeder, "acquire_startup_lock", _fake_lock), \
         patch(
             "dynastore.modules.db_config.platform_config_service."
             "PlatformConfigService.initialize_storage",
             init_storage,
         ):
        await seeder.seed_default_configs(engine=object())

    init_storage.assert_not_awaited()


@pytest.mark.asyncio
async def test_guard_unset_seeds_then_runs_normally(monkeypatch, tmp_path):
    """When the guard is not set, normal seeding logic runs unchanged."""
    _write_seed(tmp_path / "defaults", "task-routing.json", {
        "class_key": "task_routing_config",
        "value": {"tasks": {"t_a": [{"consumers": ["catalog"], "runner": "background"}]}},
    })
    monkeypatch.setattr(seeder, "DEFAULTS_DIR", tmp_path / "defaults")

    config_mgr = AsyncMock()
    config_mgr.list_configs = AsyncMock(return_value={})
    config_mgr.set_config = AsyncMock()

    with patch("dynastore.tools.discovery.get_protocol", return_value=config_mgr), \
         patch.object(seeder, "acquire_startup_lock", _fake_lock), \
         patch(
             "dynastore.modules.catalog.bootstrap_guard.is_initialized",
             AsyncMock(return_value=False),
         ):
        await seeder.seed_default_configs(engine=object())

    config_mgr.set_config.assert_awaited_once()


@pytest.mark.asyncio
async def test_lock_timeout_skips_without_raising(monkeypatch, tmp_path):
    """A lock-timeout on the seeder's advisory lock must be tolerated the
    same way as "another process holds it": skip seeding this boot instead
    of aborting startup (#2625).
    """
    _write_seed(tmp_path / "defaults", "task-routing.json", {
        "class_key": "task_routing_config",
        "value": {"tasks": {"t_a": [{"consumers": ["catalog"], "runner": "background"}]}},
    })
    monkeypatch.setattr(seeder, "DEFAULTS_DIR", tmp_path / "defaults")

    class _LockNotAvailableError(Exception):
        pgcode = "55P03"

    @asynccontextmanager
    async def _raising_lock(_engine, _key):
        raise _LockNotAvailableError("canceling statement due to lock timeout")
        yield None  # pragma: no cover - unreachable; keeps this a generator

    config_mgr = AsyncMock()
    config_mgr.set_config = AsyncMock()

    with patch("dynastore.tools.discovery.get_protocol", return_value=config_mgr), \
         patch.object(seeder, "acquire_startup_lock", _raising_lock):
        await seeder.seed_default_configs(engine=object())

    config_mgr.set_config.assert_not_awaited()
