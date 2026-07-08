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

from typing import Any, Dict, Optional, Tuple
from uuid import uuid4

# Connection pooling mode values for ``DBConfig.db_pooling_mode`` (#3081).
POOLING_MODE_DIRECT = "direct"
POOLING_MODE_TRANSACTION_POOLER = "transaction_pooler"


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


def task_engine_connect_args(db_config) -> Dict[str, Any]:
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
    Disables both SQLAlchemy's asyncpg prepared-statement cache and asyncpg's
    own driver statement cache, matching the shared serving engine, so task
    engines stay safe if their DSN points at a pooler.

    Also stamps ``application_name`` with this process's service + instance
    id (geoid#2924) — these ad-hoc task engines previously carried no
    ``application_name`` at all, making their connections invisible to any
    per-instance monitoring/reaper query.

    Mode-aware (#3081): behind a transaction pooler
    (``DBConfig.db_pooling_mode == "transaction_pooler"``) only
    ``application_name`` rides the startup packet; the task lock-safety
    timeouts are re-applied per transaction by :func:`create_task_engine` via
    ``SET LOCAL``. TCP keepalives are also carried here in direct mode (#3057),
    so task/job connections share the serving engine's keepalive net.
    """
    from dynastore.modules.db_config.instance import get_stamped_application_name

    lock_timeout, _statement_timeout, _idle_in_transaction_session_timeout = (
        resolve_timeout_settings(db_config)
    )
    return {
        "prepared_statement_cache_size": 0,
        "statement_cache_size": 0,
        "prepared_statement_name_func": lambda: f"__asyncpg_{uuid4()}__",
        "server_settings": build_connection_server_settings(
            db_config,
            application_name=get_stamped_application_name(),
            lock_timeout=lock_timeout,
            idle_in_transaction_session_timeout=(
                db_config.task_idle_in_transaction_session_timeout
            ),
            # Task engines never bound statement_timeout — they interleave a PG
            # transaction with slow secondary-store I/O and scope their own
            # budget via SET LOCAL where needed.
            statement_timeout=None,
        ),
    }


def is_transaction_pooler(db_config) -> bool:
    """True when connections talk to a transaction-mode pooler (AlloyDB Managed
    Connection Pooling / PgBouncer, e.g. :6432) rather than a direct
    PostgreSQL/AlloyDB backend (#3081).

    Such poolers forward only an allowlist of startup parameters and reject the
    first unrecognized one, so the lock-safety GUCs + TCP keepalives this module
    otherwise sends via ``server_settings`` must not ride the startup packet in
    this mode. Any unset/unknown value falls back to direct.
    """
    return (
        getattr(db_config, "db_pooling_mode", POOLING_MODE_DIRECT)
        == POOLING_MODE_TRANSACTION_POOLER
    )


def tcp_keepalive_server_settings(db_config) -> Dict[str, str]:
    """Backend TCP-keepalive GUCs for a direct connection (#655). Empty behind a
    transaction pooler, which owns and manages the backend socket and rejects
    these as startup parameters (#3081)."""
    if is_transaction_pooler(db_config):
        return {}
    return {
        "tcp_keepalives_idle": str(db_config.tcp_keepalives_idle),
        "tcp_keepalives_interval": str(db_config.tcp_keepalives_interval),
        "tcp_keepalives_count": str(db_config.tcp_keepalives_count),
    }


def build_connection_server_settings(
    db_config,
    *,
    application_name: str,
    lock_timeout: str,
    idle_in_transaction_session_timeout: str,
    statement_timeout: Optional[str] = None,
) -> Dict[str, str]:
    """Assemble the asyncpg startup ``server_settings`` for one engine, honoring
    ``db_config.db_pooling_mode`` (#3081) — the single builder every engine
    (serving, per-catalog, task) routes through.

    Direct mode: ``application_name`` + TCP keepalives + the lock-safety timeout
    pair (and ``statement_timeout`` when given) — the full set every connection
    has historically carried.

    Transaction-pooler mode: ``application_name`` ONLY. A transaction-mode
    pooler aborts the connection on the first non-allowlisted startup parameter,
    so the timeout GUCs are instead re-applied per transaction via ``SET LOCAL``
    (see :func:`pooler_timeout_set_local_sql` / :func:`register_pooler_timeout_guard`)
    and the backend keepalives are dropped (the client→pooler socket keepalives
    are armed separately, client-side).
    """
    settings: Dict[str, str] = {"application_name": application_name}
    if is_transaction_pooler(db_config):
        return settings
    settings.update(tcp_keepalive_server_settings(db_config))
    settings.update(
        lock_safety_server_settings(lock_timeout, idle_in_transaction_session_timeout)
    )
    if statement_timeout is not None:
        settings["statement_timeout"] = statement_timeout
    return settings


def pooler_timeout_set_local_sql(
    db_config,
    *,
    lock_timeout: str,
    idle_in_transaction_session_timeout: str,
    statement_timeout: Optional[str] = None,
) -> Optional[str]:
    """A single SQL statement that re-applies the lock-safety timeouts inside a
    transaction, or ``None`` in direct mode (#3081).

    Behind a transaction pooler these GUCs cannot ride the startup packet, so
    they are re-applied per transaction. ``set_config(name, value, is_local =>
    true)`` is the function form of ``SET LOCAL`` — transaction-scoped, which is
    correct for a transaction-pooled backend that may be a different physical
    session each transaction. Emitted as ONE ``SELECT`` (not several ``SET
    LOCAL`` statements) so it is a single round-trip that also survives asyncpg's
    extended-query protocol, which rejects multi-statement strings.
    ``statement_timeout`` is included only when enabled (a non-"0" value).
    """
    if not is_transaction_pooler(db_config):
        return None
    calls = [
        f"set_config('lock_timeout', '{lock_timeout}', true)",
        (
            "set_config('idle_in_transaction_session_timeout', "
            f"'{idle_in_transaction_session_timeout}', true)"
        ),
    ]
    if statement_timeout is not None and str(statement_timeout) != "0":
        calls.append(f"set_config('statement_timeout', '{statement_timeout}', true)")
    return "SELECT " + ", ".join(calls)


def register_pooler_timeout_guard(engine: Any, set_local_sql: Optional[str]) -> None:
    """Register a SQLAlchemy ``begin`` listener that runs ``set_local_sql`` on
    every top-level transaction of ``engine`` (#3081). No-op when
    ``set_local_sql`` is falsy (direct mode) — the timeouts already ride the
    startup packet there.

    Fires only on real transactions: SAVEPOINTs emit the separate ``savepoint``
    event, and AUTOCOMMIT connections never emit BEGIN, so the timeouts are set
    exactly once per outermost transaction. Works for both sync and async
    engines — an async engine's transaction runs this sync ``begin`` event in
    the same greenlet, where ``exec_driver_sql`` executes synchronously.
    """
    if not set_local_sql:
        return
    from sqlalchemy import event

    sync_engine = getattr(engine, "sync_engine", engine)

    @event.listens_for(sync_engine, "begin")
    def _apply_pooler_timeouts(conn: Any) -> None:
        conn.exec_driver_sql(set_local_sql)


def create_task_engine(db_config) -> Any:
    """Build the short-lived, ``NullPool``-backed async engine that task/job
    code uses for its own DB work — mode-aware (#3081) and armed with the same
    lock-safety net + TCP keepalives as the shared serving engine (#3057).

    Direct mode: the startup ``server_settings`` carry ``lock_timeout``, the
    task idle-in-transaction budget, and the TCP keepalives. Transaction-pooler
    mode: only ``application_name`` rides the startup packet and the task
    lock-safety timeouts are re-applied per transaction via ``SET LOCAL``.
    Client→pooler (or →backend) socket keepalives are armed in both modes, since
    asyncpg exposes no libpq client keepalive params (#710) and the option is
    harmless in front of a pooler.

    Supersedes the inline ``create_async_engine(..., connect_args=
    task_engine_connect_args(...))`` the task entrypoints used to build by hand,
    so every task/job engine is constructed identically and stays pooler-safe.
    """
    from sqlalchemy.ext.asyncio import create_async_engine
    from sqlalchemy.pool import NullPool

    from dynastore.modules.db_config.tools import normalize_db_url

    lock_timeout, _statement_timeout, _idle = resolve_timeout_settings(db_config)
    task_idle = db_config.task_idle_in_transaction_session_timeout
    engine = create_async_engine(
        normalize_db_url(db_config.database_url, is_async=True),
        poolclass=NullPool,
        connect_args=task_engine_connect_args(db_config),
    )
    register_pooler_timeout_guard(
        engine,
        pooler_timeout_set_local_sql(
            db_config,
            lock_timeout=lock_timeout,
            idle_in_transaction_session_timeout=task_idle,
            statement_timeout=None,
        ),
    )
    # Arm client-side SO_KEEPALIVE on the app→(pooler|backend) socket. Local
    # import avoids a module-load cycle (db_service imports this module).
    from dynastore.modules.db.db_service import _arm_client_socket_keepalive

    _arm_client_socket_keepalive(engine, db_config)
    return engine
