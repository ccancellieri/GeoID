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

"""Unit cover for the engine snapshot boot-order recovery (#818).

``DBConfigModule.lifespan`` runs at priority 0 — before ``DBService``
(priority 10) installs the connection pool — so the very first
``build_engine_snapshot`` call is guaranteed to fail with ``db_resource
is None``.  Without recovery the resolver would return KeyError forever
on this process and the ValkeyEngineConfig apply handler (gated on
engine_mode) would never register, silently breaking the runtime-tunable
contract advertised by #633 / #724 / #743.

The fix has two halves; this file pins the resolver half:

  1. ``build_engine_snapshot`` accepts an ``into=`` dict and mutates it
     in place, so a long-lived resolver closure observes successful
     entries from a later retry.
  2. ``refresh_snapshot_until_ready`` retries with exponential backoff
     until at least one engine loads; once that bounded budget is
     exhausted it degrades to an unbounded keep-alive retry every
     ``max_delay`` seconds rather than stranding the snapshot empty for
     the rest of the process's life (#2908).

The cache-module half (unconditional apply-handler registration) is
covered alongside the existing reconnect suite in
``tests/dynastore/modules/cache/unit/test_valkey_reconnect.py``.
"""

from __future__ import annotations

import asyncio

from unittest.mock import AsyncMock, MagicMock

import pytest

from dynastore.modules.db_config import engine_resolver as er
from dynastore.modules.db_config.engine_config import (
    EngineConfig,
    PostgresqlEngineConfig,
    ValkeyEngineConfig,
)


@pytest.fixture
def pcfg_stub():
    """A PlatformConfigService stub whose ``get_config`` is programmable."""
    stub = MagicMock()
    stub.get_config = AsyncMock()
    return stub


# --------------------------------------------------------------------------
# build_engine_snapshot mutates the ``into`` dict
# --------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_build_engine_snapshot_mutates_into_dict_in_place(pcfg_stub):
    """The same dict handed in should carry the new entries on return.

    The resolver closure built from this dict needs to see retry-populated
    entries without being rebuilt — that is the central contract.
    """
    snapshot: dict[str, EngineConfig] = {}
    pcfg_stub.get_config.return_value = PostgresqlEngineConfig()

    result = await er.build_engine_snapshot(pcfg_stub, into=snapshot)

    assert result is snapshot, "must return the same dict, not a copy"
    assert "postgresql_engine_config" in snapshot
    assert "postgresql_engine" in snapshot  # mirrored under engine_class


@pytest.mark.asyncio
async def test_build_engine_snapshot_skips_failing_engines_without_aborting(
    pcfg_stub,
):
    """A single failing engine must not strand the others — best-effort."""

    def _by_type(cls):
        if cls is ValkeyEngineConfig:
            raise RuntimeError("db_resource is None")
        return cls()

    pcfg_stub.get_config.side_effect = _by_type

    snapshot: dict[str, EngineConfig] = {}
    await er.build_engine_snapshot(pcfg_stub, into=snapshot)

    # Failing engine absent, others present.
    assert "valkey_engine_config" not in snapshot
    assert "postgresql_engine_config" in snapshot


# --------------------------------------------------------------------------
# refresh_snapshot_until_ready
# --------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_refresh_returns_true_when_db_becomes_ready(pcfg_stub, caplog):
    """Simulate the boot-order race: first attempt fails, second succeeds.

    Fast path (#2908 case d): success inside the bounded budget must not
    touch the exhaustion ``ERROR`` log at all.
    """
    state = {"calls": 0}

    def _gated(cls):
        state["calls"] += 1
        # First full pass (one call per engine kind) fails, second succeeds.
        # n_engines is at least 1, so calls<=n_engines means "first pass".
        # We use call count as the boundary so the test is independent of
        # how many EngineConfig subclasses are registered.
        if state["calls"] <= state.setdefault("n_engines", 0):
            raise RuntimeError("db_resource is None")
        return cls()

    # Pre-measure the engine kind count so the gate stays accurate.
    from dynastore.modules.db_config.engine_registry import (
        list_registered_engines,
    )

    state["n_engines"] = len(list_registered_engines())
    pcfg_stub.get_config.side_effect = _gated

    snapshot: dict[str, EngineConfig] = {}
    with caplog.at_level("ERROR", logger="dynastore.modules.db_config.engine_resolver"):
        ok = await er.refresh_snapshot_until_ready(
            snapshot,
            pcfg_stub,
            max_attempts=3,
            initial_delay=0.01,
            max_delay=0.01,
        )

    assert ok is True
    assert snapshot, "snapshot should be populated after retry"
    assert not any(
        "retry budget exhausted" in rec.getMessage() for rec in caplog.records
    ), "success inside the bounded budget must not log the exhaustion ERROR"


@pytest.mark.asyncio
async def test_refresh_keepalive_recovers_after_budget_exhausted(
    pcfg_stub, caplog
):
    """#2908 cases (a)+(b): bounded budget exhausts, then keep-alive phase
    recovers once the DB pool comes up — exactly one ERROR on exhaustion,
    then one INFO once the keep-alive phase loads the snapshot.
    """
    from dynastore.modules.db_config.engine_registry import (
        list_registered_engines,
    )

    n_engines = len(list_registered_engines())
    # Bounded budget: max_attempts=2 → exhausts after 2 failed passes, then
    # the keep-alive phase takes over with its own ``max_delay``-cadence
    # sleeps. Fail every call through the bounded budget plus one keep-alive
    # pass, then succeed.
    state = {"calls": 0, "fail_until": 2 * n_engines + n_engines}

    def _gated(cls):
        state["calls"] += 1
        if state["calls"] <= state["fail_until"]:
            raise RuntimeError("db_resource is None")
        return cls()

    pcfg_stub.get_config.side_effect = _gated

    snapshot: dict[str, EngineConfig] = {}
    with caplog.at_level("INFO", logger="dynastore.modules.db_config.engine_resolver"):
        ok = await er.refresh_snapshot_until_ready(
            snapshot,
            pcfg_stub,
            max_attempts=2,
            initial_delay=0.01,
            max_delay=0.01,
        )

    assert ok is True
    assert snapshot, "keep-alive phase must populate the snapshot on recovery"

    error_records = [
        rec for rec in caplog.records
        if rec.levelname == "ERROR" and "retry budget exhausted" in rec.getMessage()
    ]
    assert len(error_records) == 1, (
        "exhaustion must be logged exactly once, not on every keep-alive miss"
    )

    ready_records = [
        rec for rec in caplog.records
        if rec.levelname == "INFO" and "engine snapshot ready" in rec.getMessage()
    ]
    assert len(ready_records) == 1
    assert "recovered" in ready_records[0].getMessage(), (
        "the late-recovery INFO must say it recovered after exhaustion"
    )


@pytest.mark.asyncio
async def test_refresh_keepalive_cancellable_during_keepalive_phase(pcfg_stub):
    """#2908 case (c): once in the unbounded keep-alive phase, cancelling
    the task must raise ``CancelledError`` promptly — the coroutine is
    awaited-on-cancel at lifespan teardown and must never swallow it.
    """
    pcfg_stub.get_config.side_effect = RuntimeError("db_resource is None")

    snapshot: dict[str, EngineConfig] = {}
    task = asyncio.create_task(
        er.refresh_snapshot_until_ready(
            snapshot,
            pcfg_stub,
            max_attempts=2,
            initial_delay=0.01,
            max_delay=0.05,
        )
    )

    # Give the bounded budget time to exhaust and enter the keep-alive
    # ``asyncio.sleep(max_delay)`` — a handful of the 0.01s/0.05s delays.
    await asyncio.sleep(0.1)
    assert not task.done(), "task should still be alive in the keep-alive phase"

    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(task, timeout=1.0)


@pytest.mark.asyncio
async def test_refresh_returns_false_when_no_engines_registered(
    pcfg_stub, monkeypatch, caplog
):
    """Degenerate case: no registered engine kinds → nothing to ever wait
    for, so the keep-alive phase must not spin forever; return False.
    """
    monkeypatch.setattr(er, "list_registered_engines", lambda: {})

    snapshot: dict[str, EngineConfig] = {}
    with caplog.at_level("ERROR", logger="dynastore.modules.db_config.engine_resolver"):
        ok = await er.refresh_snapshot_until_ready(
            snapshot,
            pcfg_stub,
            max_attempts=2,
            initial_delay=0.01,
            max_delay=0.01,
        )

    assert ok is False
    assert snapshot == {}


# --------------------------------------------------------------------------
# try_refresh_snapshot_once — #2857 on-demand single attempt
# --------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_try_refresh_snapshot_once_returns_false_while_pool_not_ready(
    pcfg_stub, caplog,
):
    """A single on-demand attempt during the boot race returns False and
    must NOT log the bounded-retry-loop's terminal ERROR — that log is
    reserved for refresh_snapshot_until_ready's own budget exhaustion, not
    every miss from a caller with its own outer retry cadence.
    """
    pcfg_stub.get_config.side_effect = RuntimeError("db_resource is None")

    snapshot: dict[str, EngineConfig] = {}
    with caplog.at_level("ERROR", logger="dynastore.modules.db_config.engine_resolver"):
        ok = await er.try_refresh_snapshot_once(snapshot, pcfg_stub)

    assert ok is False
    assert snapshot == {}
    assert not any(
        "retry budget exhausted" in rec.getMessage() for rec in caplog.records
    )


@pytest.mark.asyncio
async def test_try_refresh_snapshot_once_populates_snapshot_once_ready(
    pcfg_stub,
):
    """Once the DB pool is up, a single attempt is enough to populate the
    snapshot — the contract a caller like CacheModule's boot-upgrade loop
    relies on when it calls this once per retry attempt.
    """
    pcfg_stub.get_config.return_value = PostgresqlEngineConfig()

    snapshot: dict[str, EngineConfig] = {}
    ok = await er.try_refresh_snapshot_once(snapshot, pcfg_stub)

    assert ok is True
    assert "postgresql_engine_config" in snapshot


@pytest.mark.asyncio
async def test_try_refresh_snapshot_once_mutates_same_dict_across_calls(
    pcfg_stub,
):
    """Repeated on-demand attempts, like refresh_snapshot_until_ready, must
    mutate the SAME dict in place so a resolver closure built once observes
    a later successful attempt without being rebuilt.
    """
    state = {"ready": False}

    def _gated(cls):
        if not state["ready"]:
            raise RuntimeError("db_resource is None")
        return cls()

    pcfg_stub.get_config.side_effect = _gated

    snapshot: dict[str, EngineConfig] = {}
    resolver = er.make_resolver(snapshot)

    assert await er.try_refresh_snapshot_once(snapshot, pcfg_stub) is False
    assert resolver("valkey_engine") is None

    state["ready"] = True
    assert await er.try_refresh_snapshot_once(snapshot, pcfg_stub) is True
    assert isinstance(resolver("valkey_engine"), ValkeyEngineConfig)


# --------------------------------------------------------------------------
# make_resolver observes mutation in place
# --------------------------------------------------------------------------


def test_make_resolver_observes_dict_mutation():
    """The resolver closure must see entries added to its captured dict."""
    snapshot: dict[str, EngineConfig] = {}
    resolver = er.make_resolver(snapshot)

    assert resolver("valkey_engine") is None  # empty boot snapshot

    cfg = ValkeyEngineConfig()
    snapshot["valkey_engine_config"] = cfg
    snapshot["valkey_engine"] = cfg  # mirror under engine_class

    assert resolver("valkey_engine") is cfg, (
        "resolver must return the engine added after closure construction"
    )


# --------------------------------------------------------------------------
# Refresh task wires up cleanly with the resolver closure
# --------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_resolver_observes_refresh_populated_entries(pcfg_stub):
    """End-to-end: empty boot snapshot + retry populates + resolver returns it."""
    state = {"first_pass": True}

    def _by_pass(cls):
        if state["first_pass"]:
            raise RuntimeError("db_resource is None")
        return cls()

    pcfg_stub.get_config.side_effect = _by_pass

    snapshot: dict[str, EngineConfig] = {}
    await er.build_engine_snapshot(pcfg_stub, into=snapshot)
    resolver = er.make_resolver(snapshot)
    assert resolver("valkey_engine") is None  # boot race

    state["first_pass"] = False
    await er.refresh_snapshot_until_ready(
        snapshot,
        pcfg_stub,
        max_attempts=2,
        initial_delay=0.01,
        max_delay=0.01,
    )

    cfg = resolver("valkey_engine")
    assert cfg is not None
    assert isinstance(cfg, ValkeyEngineConfig)
