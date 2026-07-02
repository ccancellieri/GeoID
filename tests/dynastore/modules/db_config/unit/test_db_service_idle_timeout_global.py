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

"""#2749 item 3 — bounded idle transactions on task/ingestion connections.

Source-pin regression: ``idle_in_transaction_session_timeout`` (and
``lock_timeout``) are applied as ``server_settings`` on every connection the
MAIN async engine hands out (``DBConfig.idle_in_transaction_session_timeout``
/ ``DBConfig.lock_timeout``, resolved once via ``resolve_timeout_settings``
at engine construction in ``modules/db/db_service.py``). That engine serves
both the API service and every background loop that resolves its connection
via ``get_engine()`` / ``DatabaseProtocol`` — which is the mechanism that
lets a wedged transaction self-clear (PostgreSQL terminates a backend
idle-in-transaction past the configured window, releasing its locks
server-side) instead of pinning a lock for hours.

KNOWN GAP (follow-up, not fixed here): a handful of task-side call sites
build their OWN short-lived ``create_async_engine(..., poolclass=NullPool)``
instead of resolving the shared engine, and pass no ``server_settings`` at
all — so those specific connections do NOT carry either timeout:

- tasks/ingestion/ingestion_task.py (`_maybe_enqueue_tile_preseed`)
- tasks/workclass_drain/event_drain_task.py
- tasks/workclass_drain/storage_drain_task.py
- modules/db_config/typed_store/cli.py (a CLI tool, not a server process)

The three task-side engines are lower-risk than the #2749 wedge itself —
each is built, used once, and ``.dispose()``-d in a ``finally`` within the
same function, so it never sits in a pool waiting to be reused — but they
are not covered by this test's guarantee. ``test_known_bare_engine_sites``
pins the current inventory so a newly added bare ``create_async_engine()``
call is caught for review instead of silently joining the gap.
"""

from __future__ import annotations

import pathlib


def _repo_root() -> pathlib.Path:
    return pathlib.Path(__file__).resolve().parents[5]


def _db_service_source() -> str:
    return (
        _repo_root() / "packages/core/src/dynastore/modules/db/db_service.py"
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
    source = _db_service_source()
    assert '"idle_in_transaction_session_timeout": (' in source, (
        "db_service.py must pass idle_in_transaction_session_timeout in "
        "server_settings for every connection the engine creates — this is "
        "the safety net that lets a wedged transaction (task/job or "
        "interactive) self-clear instead of pinning a lock indefinitely (#2749)."
    )
    assert "resolve_timeout_settings(db_config)" in source, (
        "the timeout values must come from DBConfig via resolve_timeout_settings, "
        "not be hardcoded or duplicated inline."
    )


def test_engine_server_settings_include_lock_timeout():
    source = _db_service_source()
    assert '"lock_timeout": lock_timeout,' in source, (
        "db_service.py must pass lock_timeout in server_settings for every "
        "connection — this bounds how long ANY statement (appender INSERT "
        "or DDL) waits to acquire a lock, appender queuing briefly instead "
        "of blocking unbounded behind a pending ACCESS EXCLUSIVE."
    )


def test_known_bare_engine_sites():
    """Inventory of ``create_async_engine()`` call sites that do NOT go
    through ``db_service.py`` and so do NOT carry ``lock_timeout`` /
    ``idle_in_transaction_session_timeout``.

    This is a known, tracked gap (see module docstring) — not asserting
    they're acceptable, just pinning the current set so a new bare engine
    doesn't join it unnoticed. If this test fails because the list shrank,
    update it (progress). If it grew, the new site needs the same review
    this docstring gives the existing four.
    """
    core_src = _repo_root() / "packages/core/src/dynastore"
    hits = sorted(
        path.relative_to(core_src)
        for path in core_src.rglob("*.py")
        if _calls_create_async_engine(path)
    )
    expected = sorted(
        pathlib.Path(p)
        for p in (
            "modules/db/db_service.py",
            "modules/db_config/typed_store/cli.py",
            "tasks/ingestion/ingestion_task.py",
            "tasks/workclass_drain/event_drain_task.py",
            "tasks/workclass_drain/storage_drain_task.py",
        )
    )
    assert hits == expected, (
        f"create_async_engine() call sites changed — expected {expected}, "
        f"found {hits}. Update this inventory (see module docstring) and, "
        "for any new task-side site, evaluate whether it needs the same "
        "server_settings as db_service.py."
    )
