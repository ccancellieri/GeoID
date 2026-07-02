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

Related issues:
- #2343 — DB pool starvation (idle_in_txn)
- #2344 — Leader-loop lock hold
- #2340 — Async catalog hard-delete
"""

from __future__ import annotations

from typing import Dict, Tuple


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

    Resolves ``lock_timeout`` / ``idle_in_transaction_session_timeout`` from
    ``db_config`` via :func:`resolve_timeout_settings` and wraps them via
    :func:`lock_safety_server_settings`. Intended for the short-lived,
    single-use engines built outside the shared engine (see #2832) — pass
    the result straight as the engine's ``connect_args`` keyword.
    """
    lock_timeout, _statement_timeout, idle_in_transaction_session_timeout = (
        resolve_timeout_settings(db_config)
    )
    return {
        "server_settings": lock_safety_server_settings(
            lock_timeout, idle_in_transaction_session_timeout
        ),
    }
