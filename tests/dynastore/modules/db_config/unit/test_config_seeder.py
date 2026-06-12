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
from unittest.mock import AsyncMock, MagicMock, patch, call

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
async def test_no_defaults_dir_seeds_nothing_but_runs_fixup(monkeypatch, tmp_path):
    monkeypatch.setattr(seeder, "DEFAULTS_DIR", tmp_path / "nope")
    fixup = AsyncMock()
    # No defaults/ subfolder — nothing seeded, but the legacy-key fixup
    # still runs under the lock (stored rows never self-heal otherwise).
    with patch.object(seeder, "acquire_startup_lock", _fake_lock), \
         patch.object(seeder, "_fixup_legacy_outbox_drain_keys", fixup):
        await seeder.seed_default_configs(engine=object())
    fixup.assert_awaited_once()


@pytest.mark.asyncio
async def test_no_files_seeds_nothing_but_runs_fixup(monkeypatch, tmp_path):
    (tmp_path / "defaults").mkdir()
    monkeypatch.setattr(seeder, "DEFAULTS_DIR", tmp_path / "defaults")
    fixup = AsyncMock()
    with patch.object(seeder, "acquire_startup_lock", _fake_lock), \
         patch.object(seeder, "_fixup_legacy_outbox_drain_keys", fixup):
        await seeder.seed_default_configs(engine=object())
    fixup.assert_awaited_once()


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
         patch.object(seeder, "acquire_startup_lock", _fake_lock), \
         patch.object(seeder, "_fixup_legacy_outbox_drain_keys", AsyncMock()):
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


# ---------------------------------------------------------------------------
# Legacy "outbox_drain" key fixup
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_fixup_routing_config_renames_outbox_drain(monkeypatch):
    """_fixup_routing_config_rows rewrites outbox_drain → event_drain + index_drain."""
    rows = [
        {
            "ref_key": "platform",
            "config_data": json.dumps({
                "tasks": {
                    "outbox_drain": [{"consumers": ["worker"], "runner": "background"}],
                },
                "processes": {},
            }),
        }
    ]

    captured_updates: list = []

    async def _fake_select(conn):
        return rows

    async def _fake_update(conn, *, ref_key, new_data):
        captured_updates.append((ref_key, json.loads(new_data)))

    monkeypatch.setattr(seeder._q_select_routing_configs, "execute", _fake_select)
    monkeypatch.setattr(seeder._q_update_routing_config, "execute", _fake_update)

    await seeder._fixup_routing_config_rows(object())

    assert len(captured_updates) == 1
    ref_key, new_data = captured_updates[0]
    assert ref_key == "platform"
    tasks_map = new_data["tasks"]
    # Legacy key removed.
    assert "outbox_drain" not in tasks_map
    # Both new keys populated.
    assert "event_drain" in tasks_map
    assert "index_drain" in tasks_map
    assert tasks_map["event_drain"][0]["consumers"] == ["worker"]
    assert tasks_map["index_drain"][0]["consumers"] == ["worker"]


@pytest.mark.asyncio
async def test_fixup_routing_config_covers_processes_map(monkeypatch):
    """A legacy key under 'processes' is split exactly like one under 'tasks'."""
    rows = [
        {
            "ref_key": "platform",
            "config_data": json.dumps({
                "tasks": {},
                "processes": {
                    "outbox_drain": [{"consumers": ["worker"], "runner": "background"}],
                },
            }),
        }
    ]

    captured_updates: list = []

    async def _fake_select(conn):
        return rows

    async def _fake_update(conn, *, ref_key, new_data):
        captured_updates.append((ref_key, json.loads(new_data)))

    monkeypatch.setattr(seeder._q_select_routing_configs, "execute", _fake_select)
    monkeypatch.setattr(seeder._q_update_routing_config, "execute", _fake_update)

    await seeder._fixup_routing_config_rows(object())

    assert len(captured_updates) == 1
    _, new_data = captured_updates[0]
    proc_map = new_data["processes"]
    assert "outbox_drain" not in proc_map
    assert "event_drain" in proc_map and "index_drain" in proc_map


@pytest.mark.asyncio
async def test_fixup_routing_config_no_op_when_clean(monkeypatch):
    """_fixup_routing_config_rows is a no-op when no legacy key exists."""
    rows = [
        {
            "ref_key": "platform",
            "config_data": json.dumps({
                "tasks": {
                    "event_drain": [{"consumers": ["worker"], "runner": "background"}],
                    "index_drain": [{"consumers": ["worker"], "runner": "background"}],
                },
                "processes": {},
            }),
        }
    ]

    captured_updates: list = []

    async def _fake_select(conn):
        return rows

    async def _fake_update(conn, *, ref_key, new_data):
        captured_updates.append(ref_key)

    monkeypatch.setattr(seeder._q_select_routing_configs, "execute", _fake_select)
    monkeypatch.setattr(seeder._q_update_routing_config, "execute", _fake_update)

    await seeder._fixup_routing_config_rows(object())

    assert captured_updates == [], "No UPDATE should be issued when no legacy key exists."


@pytest.mark.asyncio
async def test_fixup_routing_config_does_not_overwrite_existing_new_keys(monkeypatch):
    """When event_drain or index_drain already exist, they are not overwritten."""
    rows = [
        {
            "ref_key": "platform",
            "config_data": json.dumps({
                "tasks": {
                    "outbox_drain": [{"consumers": ["old"], "runner": "legacy"}],
                    "event_drain":  [{"consumers": ["worker"], "runner": "background"}],
                    # index_drain absent → should be populated from outbox_drain
                },
                "processes": {},
            }),
        }
    ]

    captured_updates: list = []

    async def _fake_select(conn):
        return rows

    async def _fake_update(conn, *, ref_key, new_data):
        captured_updates.append((ref_key, json.loads(new_data)))

    monkeypatch.setattr(seeder._q_select_routing_configs, "execute", _fake_select)
    monkeypatch.setattr(seeder._q_update_routing_config, "execute", _fake_update)

    await seeder._fixup_routing_config_rows(object())

    assert len(captured_updates) == 1
    _, new_data = captured_updates[0]
    tasks_map = new_data["tasks"]
    # Existing event_drain must not be overwritten.
    assert tasks_map["event_drain"][0]["consumers"] == ["worker"]
    # index_drain was absent → populated from legacy value.
    assert tasks_map["index_drain"][0]["consumers"] == ["old"]
    assert "outbox_drain" not in tasks_map


@pytest.mark.asyncio
async def test_fixup_routing_config_tolerates_missing_table(monkeypatch):
    """_fixup_routing_config_rows is silent when configs table does not exist."""
    async def _raise(conn):
        raise Exception("relation configs.platform_configs does not exist")

    monkeypatch.setattr(seeder._q_select_routing_configs, "execute", _raise)

    # Must not propagate.
    await seeder._fixup_routing_config_rows(object())


@pytest.mark.asyncio
async def test_fixup_pending_task_rows_rewrites(monkeypatch, caplog):
    """_fixup_pending_task_rows issues an UPDATE for PENDING outbox_drain rows."""
    updated_rows = [{"task_id": "abc"}, {"task_id": "def"}]

    async def _fake_execute(conn):
        return updated_rows

    fake_q = AsyncMock()
    fake_q.execute = _fake_execute

    with patch.object(seeder, "DQLQuery", return_value=fake_q):
        caplog.set_level("INFO")
        await seeder._fixup_pending_task_rows(object())

    assert any("2" in r.message and "outbox_drain" in r.message for r in caplog.records)


@pytest.mark.asyncio
async def test_fixup_pending_task_rows_tolerates_missing_table(monkeypatch, caplog):
    """_fixup_pending_task_rows is silent when the tasks table does not exist."""
    async def _raise(conn):
        raise Exception("relation tasks.tasks does not exist")

    fake_q = MagicMock()
    fake_q.execute = _raise

    with patch.object(seeder, "DQLQuery", return_value=fake_q):
        caplog.set_level("DEBUG")
        # Must not propagate.
        await seeder._fixup_pending_task_rows(object())
