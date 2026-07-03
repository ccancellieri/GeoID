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
    assert "lock_safety_server_settings(" in _db_service_source(), (
        "db_service.py must consume the shared lock_safety_server_settings() "
        "helper rather than inlining its own copy of the dict."
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
    settings_idx = source.index('"statement_timeout": statement_timeout,')
    assert clamp_idx < settings_idx, (
        "clamp_serving_statement_timeout(...) must be assigned back to "
        "statement_timeout BEFORE it is placed in server_settings, so the "
        "clamped value (not the raw resolve_timeout_settings() value) is "
        "what the engine actually applies."
    )


# create_async_engine() sites that do NOT route through
# task_engine_connect_args() / lock_safety_server_settings() — a justified,
# reviewed exception, not a silent gap:
#
# - modules/db/db_service.py — the shared engine itself; it defines
#   lock_safety_server_settings() and merges it directly (see
#   test_engine_server_settings_include_idle_in_transaction_timeout above).
# - modules/db_config/typed_store/cli.py — a standalone CLI tool (not a
#   long-lived server/task process); its ``_engine()`` helper builds a
#   plain, default-pooled engine for one-off operator commands.
_ALLOWLIST_NO_TASK_HELPER = frozenset(
    {
        pathlib.Path("modules/db/db_service.py"),
        pathlib.Path("modules/db_config/typed_store/cli.py"),
    }
)


def test_bare_engine_sites_carry_lock_safety_settings():
    """Every ``create_async_engine()`` call site either uses the shared
    lock-safety helper or is on the explicit, reviewed allowlist above.

    Replaces the prior "known gap" inventory: the three task-side engines
    (ingestion / event_drain / storage_drain) now pass
    ``connect_args=task_engine_connect_args(DBConfig)`` and are asserted
    here, not merely documented. If this fails because a NEW bare
    ``create_async_engine()`` call site appeared, either wire it to
    ``task_engine_connect_args()`` or add it to
    ``_ALLOWLIST_NO_TASK_HELPER`` with a one-line justification.
    """
    core_src = _repo_root() / "packages/core/src/dynastore"
    sites = sorted(
        path.relative_to(core_src)
        for path in core_src.rglob("*.py")
        if _calls_create_async_engine(path)
    )

    covered = {
        pathlib.Path("tasks/ingestion/ingestion_task.py"),
        pathlib.Path("tasks/workclass_drain/event_drain_task.py"),
        pathlib.Path("tasks/workclass_drain/storage_drain_task.py"),
    }

    missing_from_tree = covered - set(sites)
    assert not missing_from_tree, (
        f"Expected task-side engine sites not found: {sorted(missing_from_tree)}. "
        "Update this test's `covered` set if these files moved."
    )

    uncovered = set(sites) - covered - _ALLOWLIST_NO_TASK_HELPER
    assert not uncovered, (
        f"New bare create_async_engine() site(s) with no lock-safety coverage: "
        f"{sorted(uncovered)}. Either pass "
        "connect_args=task_engine_connect_args(DBConfig) (see #2832) or add "
        "to _ALLOWLIST_NO_TASK_HELPER with a justification."
    )

    for site in covered:
        source = (core_src / site).read_text(encoding="utf-8")
        assert "task_engine_connect_args(" in source, (
            f"{site} calls create_async_engine() but does not pass "
            "connect_args=task_engine_connect_args(DBConfig) — it would build "
            "an engine with no lock_timeout / idle_in_transaction_session_timeout "
            "(#2749, #2832)."
        )
