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

"""Database timeout resolution helpers.

These timeouts are genuine infra dimensioning: lock_timeout,
statement_timeout, and idle_in_transaction_session_timeout are applied to
every PostgreSQL connection via asyncpg's ``server_settings``.  They are
configured via environment variables (DB_LOCK_TIMEOUT, DB_STATEMENT_TIMEOUT,
DB_IDLE_IN_TRANSACTION_TIMEOUT) and resolved once at startup by ``DBConfig``
using the env → db_config.json → code-default cascade defined there.
Task-side ad-hoc engines (see ``task_engine_connect_args`` below) use a
separate, more generous idle budget (DB_TASK_IDLE_IN_TRANSACTION_TIMEOUT)
since they interleave PG transactions with slower secondary-store I/O.

Related issues:
- #2343 — DB pool starvation (idle_in_txn)
- #2344 — Leader-loop lock hold
- #2340 — Async catalog hard-delete
- #2837 — task engines regressed onto the 10s serving budget
"""

from __future__ import annotations

from typing import Dict, Optional, Tuple


def _parse_pg_interval_seconds(value: str) -> Optional[int]:
    """Parse a small subset of PostgreSQL interval syntax into seconds.

    Understands a bare integer (seconds), an ``s`` suffix (seconds), and a
    ``min`` suffix (minutes). Returns ``None`` for anything else -- including
    the disabled sentinel ``"0"`` is left to the caller, since 0 parses fine
    as an integer but means "no timeout", not "0 seconds".
    """
    text = value.strip()
    if not text:
        return None
    try:
        return int(text)
    except ValueError:
        pass
    if text.endswith("min"):
        try:
            return int(text[: -len("min")]) * 60
        except ValueError:
            return None
    if text.endswith("s"):
        try:
            return int(text[: -len("s")])
        except ValueError:
            return None
    return None


def clamp_serving_statement_timeout(statement_timeout: str, ceiling_seconds: int) -> str:
    """Clamp a resolved ``statement_timeout`` to ``ceiling_seconds`` for the
    shared serving engine (#2898).

    ``DBConfig.statement_timeout`` is disabled ("0") in dev and set to "90s"
    in production -- both above the load balancer's 60s deadline, so a stuck
    interactive query on the shared serving engine holds its connection past
    the client's timeout instead of being cancelled and reclaimed server-side.
    This clamps the effective value below that ceiling without touching the
    configured ``DB_STATEMENT_TIMEOUT`` itself, so long-running task/job
    engines (which never call this) and ``SET LOCAL`` overrides within a
    transaction are unaffected.

    Fails safe: an unparseable, disabled ("0"), or non-positive value clamps
    to the ceiling; a value already at or below the ceiling is returned
    unchanged (reformatted as ``"<seconds>s"``); a value above the ceiling is
    clamped to it. Never raises -- a malformed input must not break engine
    construction.
    """
    try:
        parsed = _parse_pg_interval_seconds(statement_timeout)
        if parsed is None or parsed <= 0 or parsed > ceiling_seconds:
            return f"{ceiling_seconds}s"
        return f"{parsed}s"
    except Exception:
        return f"{ceiling_seconds}s"


def resolve_timeout_settings(
    db_config,
) -> Tuple[str, str, str]:
    """Return the effective timeout settings from ``DBConfig``.

    ``DBConfig`` already applies the env var → db_config.json → code-default
    cascade for each timeout key, so we delegate directly to it.

    Returns ``(lock_timeout, statement_timeout, idle_in_transaction_session_timeout)``.
    """
    return (
        db_config.lock_timeout,
        db_config.statement_timeout,
        db_config.idle_in_transaction_session_timeout,
    )


def lock_safety_server_settings(
    lock_timeout: str, idle_in_transaction_session_timeout: str
) -> Dict[str, str]:
    """Return the ``lock_timeout`` + ``idle_in_transaction_session_timeout``
    ``server_settings`` pair every PostgreSQL connection should carry.

    Single, reusable definition for the pair the shared engine
    (``modules/db/db_service.py``) has always applied — factored out so
    ad-hoc, short-lived engines (task/job code building their own
    NullPool-backed async engine) can carry the same lock-safety net
    instead of copy-pasting the dict. Without it, a task connection that
    freezes mid-transaction can hold row/relation locks indefinitely,
    invisible to any timeout (#2749, #2832).
    """
    return {
        "lock_timeout": lock_timeout,
        "idle_in_transaction_session_timeout": idle_in_transaction_session_timeout,
    }


def task_engine_connect_args(db_config) -> Dict[str, Dict[str, str]]:
    """``connect_args`` carrying the lock-safety ``server_settings`` for a
    task-side, NullPool-backed ad-hoc async engine.

    Resolves ``lock_timeout`` from ``db_config`` via
    :func:`resolve_timeout_settings` (same value as the shared engine), but
    the idle-in-transaction budget uses ``db_config.task_idle_in_transaction_session_timeout``
    instead of the shared ``idle_in_transaction_session_timeout``. Task/job
    engines routinely interleave a PG transaction with slow secondary-store
    I/O (e.g. ES bulk writes during reindex/drain) — the serving-tier 10s
    default kills them mid-write, which is what regressed in #2837. The
    task-tier value keeps the same lock-safety net (#2832) with a budget
    that actually fits that access pattern.

    Wraps the result via :func:`lock_safety_server_settings`. Intended for
    the short-lived, single-use engines built outside the shared engine —
    pass the result straight as the engine's ``connect_args`` keyword.

    Also stamps ``application_name`` with this process's service + instance
    id (geoid#2924) — these ad-hoc task engines previously carried no
    ``application_name`` at all, making their connections invisible to any
    per-instance monitoring/reaper query.
    """
    from dynastore.modules.db_config.instance import get_stamped_application_name

    lock_timeout, _statement_timeout, _idle_in_transaction_session_timeout = (
        resolve_timeout_settings(db_config)
    )
    return {
        "server_settings": {
            "application_name": get_stamped_application_name(),
            **lock_safety_server_settings(
                lock_timeout, db_config.task_idle_in_transaction_session_timeout
            ),
        },
    }
