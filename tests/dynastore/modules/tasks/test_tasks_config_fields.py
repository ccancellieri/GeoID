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

import asyncio
from unittest.mock import MagicMock

from dynastore.modules.tasks.tasks_config import TasksPluginConfig


# ---------------------------------------------------------------------------
# queue_poll_interval env-derived default (geoid#2830 C6)
# ---------------------------------------------------------------------------
#
# ``default_factory`` (re-read on every bare construction) rather than a
# frozen-at-import ``default=os.getenv(...)`` — see the field's docstring
# comment in tasks_config.py for why this stays env-backed instead of
# migrating to a cold-boot DB-seed preset.


def test_queue_poll_interval_default_when_env_unset(monkeypatch):
    monkeypatch.delenv("DYNASTORE_QUEUE_POLL_INTERVAL", raising=False)
    cfg = TasksPluginConfig()
    assert cfg.queue_poll_interval == 30.0


def test_queue_poll_interval_default_reads_env_per_instantiation(monkeypatch):
    monkeypatch.setenv("DYNASTORE_QUEUE_POLL_INTERVAL", "0.5")
    cfg = TasksPluginConfig()
    assert cfg.queue_poll_interval == 0.5

    monkeypatch.setenv("DYNASTORE_QUEUE_POLL_INTERVAL", "12.0")
    cfg2 = TasksPluginConfig()
    assert cfg2.queue_poll_interval == 12.0


def test_runner_and_dispatcher_fields_present_with_defaults():
    cfg = TasksPluginConfig()
    assert cfg.background_runner_concurrency == 4
    assert cfg.dispatcher_batch_size == 10
    assert cfg.dispatcher_claim_reject_backoff_seconds == 30
    assert cfg.task_timeout_seconds == 3600


def test_in_task_run_inline_chunk_size_default_is_conservative():
    cfg = TasksPluginConfig()
    # Conservative default (#2716): well below the fixed serving-path
    # INLINE_DISPATCH_CHUNK_SIZE (500) so an absorbed in-run chunk never
    # builds one oversized driver call inside a memory-bounded job.
    assert 100 <= cfg.in_task_run_inline_chunk_size <= 200


def test_in_task_run_inline_chunk_size_rejects_non_positive():
    import pytest
    from pydantic import ValidationError

    with pytest.raises(ValidationError):
        TasksPluginConfig(in_task_run_inline_chunk_size=0)


# ---------------------------------------------------------------------------
# BackgroundRunner pool-aware concurrency clamp
# ---------------------------------------------------------------------------

def _make_app_state(pool_max_size: int) -> MagicMock:
    """Return a minimal app_state-like object carrying a db_config."""
    db_cfg = MagicMock()
    db_cfg.pool_max_size = pool_max_size
    app_state = MagicMock()
    app_state.db_config = db_cfg
    return app_state


def _run_lifespan_startup(pool_max_size: int, configured_concurrency: int) -> int:
    """
    Drive just the startup half of BackgroundRunner.lifespan with a mock
    app_state carrying the given pool_max_size, and a mock configs manager
    that returns the given configured_concurrency.

    Returns the effective _max_concurrency after lifespan setup completes.
    """
    from unittest.mock import AsyncMock, patch
    from dynastore.modules.tasks.runners import BackgroundRunner

    runner = BackgroundRunner()

    # Build a mock TasksPluginConfig carrying the desired concurrency.
    mock_cfg = MagicMock(spec=TasksPluginConfig)
    mock_cfg.background_runner_concurrency = configured_concurrency

    mock_config_mgr = MagicMock()
    mock_config_mgr.get_config = AsyncMock(return_value=mock_cfg)

    app_state = _make_app_state(pool_max_size)

    async def _drive():
        with patch(
            "dynastore.tools.discovery.get_protocol",
            return_value=mock_config_mgr,
        ):
            async with runner.lifespan(app_state):
                pass  # capture state after startup; teardown is a no-op here

    asyncio.run(_drive())
    return runner._max_concurrency


def test_config_default_is_safe_value():
    """The config default must be the new conservative value, not 100."""
    cfg = TasksPluginConfig()
    assert cfg.background_runner_concurrency == 4


def test_clamp_reduces_concurrency_when_above_pool_ceiling():
    """
    When configured_concurrency > pool_total - SERVING_RESERVE, the runner
    applies the pool-aware clamp.
    pool_total=10, SERVING_RESERVE=2 → ceiling=8; configured=20 → effective=8.
    """
    effective = _run_lifespan_startup(pool_max_size=10, configured_concurrency=20)
    assert effective == 8


def test_clamp_leaves_concurrency_when_below_pool_ceiling():
    """
    When configured_concurrency <= pool_total - SERVING_RESERVE, no clamping
    occurs and the configured value is used as-is.
    pool_total=20, SERVING_RESERVE=2 → ceiling=18; configured=4 → effective=4.
    """
    effective = _run_lifespan_startup(pool_max_size=20, configured_concurrency=4)
    assert effective == 4


def test_clamp_effective_concurrency_never_below_one():
    """
    With a tiny pool (pool_total=2, SERVING_RESERVE=2) ceiling=0, but the
    clamp floors at 1 so the runner always has at least one worker slot.
    """
    effective = _run_lifespan_startup(pool_max_size=2, configured_concurrency=10)
    assert effective == 1


def test_clamp_equals_ceiling_when_configured_exactly_at_ceiling():
    """
    Configured value exactly at pool_total - SERVING_RESERVE is used as-is
    (no clamping needed, no warning emitted).
    pool_total=10, SERVING_RESERVE=2 → ceiling=8; configured=8 → effective=8.
    """
    effective = _run_lifespan_startup(pool_max_size=10, configured_concurrency=8)
    assert effective == 8
