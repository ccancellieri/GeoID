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

"""#2749 item 3 / #2832 — bounded idle transactions on task/ingestion connections.

Source-pin regression: ``idle_in_transaction_session_timeout`` (and
``lock_timeout``) are applied as ``server_settings`` on every connection
built anywhere in the tree, via ONE shared definition —
``lock_safety_server_settings`` / ``task_engine_connect_args`` in
``modules/db_config/db_timeout_config.py`` — rather than copy-pasted dicts.
The MAIN async engine (``modules/db/db_service.py``) resolves the pair via
``resolve_timeout_settings`` and merges ``lock_safety_server_settings(...)``
into its ``server_settings``. Every ad-hoc, short-lived task-side engine
(``create_async_engine(..., poolclass=NullPool)``) passes
``connect_args=task_engine_connect_args(DBConfig)`` for the same guarantee.

Without this, a task connection that freezes mid-transaction (asyncio stall,
OOM pause, network partition) can hold row/relation locks indefinitely — the
forensics on #2749 point at exactly this: the 15h wedge's blocking
transactions came from an ingestion-job connection with
``stmt_age ≈ xact_age``, invisible to any timeout because none was set on
that engine.

``test_bare_engine_sites_carry_lock_safety_settings`` asserts every
``create_async_engine()`` call site in the tree either goes through the
shared helper or is on the explicit allowlist below (a justified exception,
reviewed case by case) — so a newly added bare engine with no
``server_settings`` is caught instead of silently joining the gap.
"""

from __future__ import annotations

import pathlib


def _repo_root() -> pathlib.Path:
    return pathlib.Path(__file__).resolve().parents[5]


def _db_service_source() -> str:
    return (
        _repo_root() / "packages/core/src/dynastore/modules/db/db_service.py"
    ).read_text(encoding="utf-8")


def _db_timeout_config_source() -> str:
    return (
        _repo_root()
        / "packages/core/src/dynastore/modules/db_config/db_timeout_config.py"
    ).read_text(encoding="utf-8")


def _calls_create_async_engine(path: pathlib.Path) -> bool:
    for line in path.read_text(encoding="utf-8").splitlines():
        stripped = line.strip()
        if stripped.startswith("#"):
            continue  # ignore comment/docstring mentions
        if "create_async_engine(" in stripped:
            return True
    return False


def test_engine_server_settings_include_idle_in_transaction_timeout():
    source = _db_timeout_config_source()
    assert (
        '"idle_in_transaction_session_timeout": idle_in_transaction_session_timeout,'
        in source
    ), (
        "db_timeout_config.py must define lock_safety_server_settings() carrying "
        "idle_in_transaction_session_timeout — the safety net that lets a wedged "
        "transaction (task/job or interactive) self-clear instead of pinning a "
        "lock indefinitely (#2749, #2832)."
    )
    assert "resolve_timeout_settings(db_config)" in source, (
        "the timeout values must come from DBConfig via resolve_timeout_settings, "
        "not be hardcoded or duplicated inline."
    )
    assert "build_connection_server_settings(" in _db_service_source(), (
        "db_service.py must build its server_settings through the shared, "
        "pooler-aware build_connection_server_settings() helper (which merges "
        "lock_safety_server_settings) rather than inlining its own copy of the "
        "dict (#2832, #3081)."
    )


def test_engine_server_settings_include_lock_timeout():
    source = _db_timeout_config_source()
    assert '"lock_timeout": lock_timeout,' in source, (
        "db_timeout_config.py's lock_safety_server_settings() must pass "
        "lock_timeout — this bounds how long ANY statement (appender INSERT "
        "or DDL) waits to acquire a lock, appender queuing briefly instead "
        "of blocking unbounded behind a pending ACCESS EXCLUSIVE."
    )


def test_statement_timeout_is_clamped_before_reaching_server_settings():
    """#2898 source-pin: the shared serving engine's ``statement_timeout``
    must be run through ``clamp_serving_statement_timeout`` before it is fed
    into ``server_settings`` -- not the raw value from
    ``resolve_timeout_settings``. ``DBConfig.statement_timeout`` resolves to
    ``"0"`` (disabled, dev) or values like ``"90s"`` (prod) that sit above
    the 60s load-balancer/Cloud Run deadline; without the clamp, a stuck
    query holds its connection to that ceiling instead of being cancelled
    and reclaimed server-side. A future edit that drops the clamp call
    (e.g. reverting to the raw ``resolve_timeout_settings`` tuple) must fail
    this test.
    """
    source = _db_service_source()
    assert "clamp_serving_statement_timeout(" in source, (
        "db_service.py must call clamp_serving_statement_timeout() on the "
        "resolved statement_timeout for the shared serving engine (#2898)."
    )
    clamp_idx = source.index("clamp_serving_statement_timeout(")
    # The clamped value is now passed as the ``statement_timeout=`` keyword of
    # build_connection_server_settings() (#3081) rather than an inline dict key.
    settings_idx = source.index("statement_timeout=statement_timeout,")
    assert clamp_idx < settings_idx, (
        "clamp_serving_statement_timeout(...) must be assigned back to "
        "statement_timeout BEFORE it is passed into "
        "build_connection_server_settings(), so the clamped value (not the raw "
        "resolve_timeout_settings() value) is what the engine actually applies."
    )


def test_db_service_disables_asyncpg_driver_statement_cache():
    source = _db_service_source()
    assert '"prepared_statement_cache_size": 0,' in source
    assert '"statement_cache_size": 0,' in source, (
        "prepared_statement_cache_size=0 only disables SQLAlchemy's asyncpg "
        "prepared-statement cache; pool_pre_ping calls asyncpg.fetchrow() "
        "directly, so asyncpg's own statement_cache_size must also be disabled."
    )


# Bare ``create_async_engine()`` call sites. Each builds an engine directly
# and is justified — not a silent gap:
#
# - modules/db/db_service.py — the shared serving engine; builds its
#   server_settings via the pooler-aware build_connection_server_settings()
#   (see test_engine_server_settings_include_idle_in_transaction_timeout above).
# - modules/db_config/db_timeout_config.py — create_task_engine(), the single
#   factory every task/job entrypoint now uses; applies the task lock-safety
#   net via task_engine_connect_args() and re-applies it per transaction behind
#   a transaction pooler (#2749, #2832, #3081).
# - modules/db_config/typed_store/cli.py — a standalone CLI tool; its
#   ``_engine()`` helper still builds a one-off engine directly, but now passes
#   the shared pooler-safe connect args used by task engines.
_ALLOWLIST_BARE_CREATE_ASYNC_ENGINE = frozenset(
    {
        pathlib.Path("modules/db/db_service.py"),
        pathlib.Path("modules/db_config/db_timeout_config.py"),
        pathlib.Path("modules/db_config/typed_store/cli.py"),
    }
)

# Task/job entrypoints must build their ad-hoc engine through the shared
# create_task_engine() factory (#3057/#3081) rather than calling
# create_async_engine() by hand — that factory is what carries the lock-safety
# net, TCP keepalives, and pooler-safe per-transaction timeouts uniformly.
_TASK_ENTRYPOINTS_VIA_FACTORY = frozenset(
    {
        pathlib.Path("main_task.py"),
        pathlib.Path("tasks/ingestion/ingestion_task.py"),
        pathlib.Path("tasks/workclass_drain/event_drain_task.py"),
        pathlib.Path("tasks/workclass_drain/storage_drain_task.py"),
    }
)


def test_bare_engine_sites_carry_lock_safety_settings():
    """Every bare ``create_async_engine()`` call site is on the reviewed
    allowlist, and every task/job entrypoint builds its engine through the
    shared ``create_task_engine()`` factory.

    A NEW bare ``create_async_engine()`` site that skips the lock-safety net
    (and, since #3081, pooler-safety) is caught here instead of silently
    joining the gap: either build it through ``create_task_engine()`` /
    ``build_connection_server_settings()`` or add it to the allowlist with a
    one-line justification.
    """
    core_src = _repo_root() / "packages/core/src/dynastore"
    sites = sorted(
        path.relative_to(core_src)
        for path in core_src.rglob("*.py")
        if _calls_create_async_engine(path)
    )

    uncovered = set(sites) - _ALLOWLIST_BARE_CREATE_ASYNC_ENGINE
    assert not uncovered, (
        f"New bare create_async_engine() site(s) with no lock-safety coverage: "
        f"{sorted(uncovered)}. Either build the engine via create_task_engine() "
        "(task/job) or build_connection_server_settings() (serving), or add it "
        "to _ALLOWLIST_BARE_CREATE_ASYNC_ENGINE with a justification (#2832, #3081)."
    )

    # The task/job entrypoints must route through the shared factory and no
    # longer call create_async_engine() by hand.
    for site in _TASK_ENTRYPOINTS_VIA_FACTORY:
        source = (core_src / site).read_text(encoding="utf-8")
        assert "create_task_engine(" in source, (
            f"{site} must build its task engine via create_task_engine(DBConfig) "
            "so it carries the lock-safety net + TCP keepalives and stays "
            "pooler-safe (#2832, #3057, #3081)."
        )
        assert not _calls_create_async_engine(core_src / site), (
            f"{site} should no longer call create_async_engine() directly — "
            "build through the create_task_engine() factory instead."
        )

    # The factory itself must apply the task lock-safety settings.
    factory_source = (
        core_src / "modules/db_config/db_timeout_config.py"
    ).read_text(encoding="utf-8")
    assert "task_engine_connect_args(" in factory_source, (
        "create_task_engine() must apply task_engine_connect_args() so every "
        "task engine carries lock_timeout / idle_in_transaction_session_timeout "
        "(#2749, #2832)."
    )
