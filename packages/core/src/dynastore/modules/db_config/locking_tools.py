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

import logging
import asyncio
import functools
import time
from contextlib import asynccontextmanager, contextmanager
from contextvars import ContextVar
from typing import Any, Optional, Callable, Awaitable, ClassVar, TypeVar, Dict, AsyncGenerator, Iterator, Set, Tuple, Union, cast, TYPE_CHECKING
from uuid import uuid4

from dynastore.modules.db_config.connection_health_config import (
    resolve_connection_retry_config,
    resolve_leadership_config,
    _leadership_config,
)
from dynastore.modules.db_config.exceptions import DatabaseConnectionError
from dynastore.modules.db_config.instance import get_service_name
from sqlalchemy import text, Engine
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine
from dynastore.modules.tasks.durable.locks import stable_lock_id_sha256 as _stable_lock_id_sha256
from dynastore.tools.async_utils import LoopLocalLock
from dynastore.modules.db_config.query_executor import (
    DQLQuery,
    DDLQuery,
    DbResource,
    ResultHandler,
    managed_transaction,
    sync_managed_transaction,
    retry_on_transient_connect,
)

if TYPE_CHECKING:
    # Imported lazily at runtime (see _get_lease_breaker): the storage package
    # __init__ pulls in db_config back-references, so a top-level import here
    # would create a circular import during module initialisation.
    from dynastore.modules.storage.circuit_breaker import CircuitBreaker

# Stable per-process identity for the leader_lease table.  Minted once at
# import time so every acquire/renew within this process uses the same owner
# string, while different pods always produce different strings (uuid4 suffix).
_LEASE_OWNER: str = f"{get_service_name() or 'unknown'}:{uuid4().hex}"

# CAS upsert for lease-table leader election.  Returns the winning row
# (owner, epoch) when this process acquires or renews the lease; returns zero
# rows when a live foreign owner holds the lease (the WHERE clause filters that
# case out, so the ON CONFLICT DO UPDATE never fires).
_LEASE_CAS_SQL = """\
INSERT INTO configs.leader_lease
       (lock_key, lock_name, owner, epoch, acquired_at, renewed_at, expires_at)
VALUES (:lock_key, :name, :owner, 1, now(), now(), now() + make_interval(secs => :ttl))
ON CONFLICT (lock_key) DO UPDATE
   SET owner       = EXCLUDED.owner,
       renewed_at  = now(),
       expires_at  = now() + make_interval(secs => :ttl),
       epoch       = CASE WHEN leader_lease.owner = EXCLUDED.owner
                          THEN leader_lease.epoch ELSE leader_lease.epoch + 1 END,
       acquired_at = CASE WHEN leader_lease.owner = EXCLUDED.owner
                          THEN leader_lease.acquired_at ELSE now() END
 WHERE leader_lease.expires_at < now()
    OR leader_lease.owner = EXCLUDED.owner
RETURNING owner, epoch\
"""

# Shared circuit breaker for the lease-election CAS. A SINGLE breaker (not
# per-lock) is correct: a failing leadership CAS is a DB-wide signal, so once
# tripped we stop hammering the database for every leader-elected service.
# Threshold/cooldown are the CircuitBreaker defaults (5 consecutive failures,
# 30 s cooldown) stated explicitly for self-documentation. is_open() advances
# OPEN -> HALF_OPEN after cooldown so the next tick probes for recovery.
#
# Constructed lazily on first use: CircuitBreaker lives under the storage
# package whose __init__ back-references db_config, so a module-level
# construction here would form a circular import.
_LEASE_BREAKER: Optional["CircuitBreaker"] = None
_LEASE_BREAKER_KEY = "leader_lease"


def _get_lease_breaker() -> "CircuitBreaker":
    """Return the process-wide lease circuit breaker, building it on first use."""
    global _LEASE_BREAKER
    if _LEASE_BREAKER is None:
        from dynastore.modules.storage.circuit_breaker import CircuitBreaker
        _LEASE_BREAKER = CircuitBreaker(failure_threshold=5, cooldown_seconds=30.0)
    return _LEASE_BREAKER

logger = logging.getLogger(__name__)

T = TypeVar("T")

# Tracks which advisory lock keys are already held in the current async call stack.
# This makes acquire_lock_if_needed re-entrant: if the same key is requested again
# within the same coroutine chain, we skip the lock attempt (PostgreSQL advisory
# xact locks are re-entrant at the DB level too).
_held_lock_keys: ContextVar[Optional[Set[str]]] = ContextVar("_held_lock_keys", default=None)


# Process-wide registry of SESSION-level advisory locks this pod currently holds
# (leadership tenures via ``pg_advisory_leadership``). Keyed by the 64-bit lock
# id; the value carries the human ``name`` and the ``time.monotonic()`` acquire
# stamp so release can log how long the lock was held. This is the observability
# the operator asked for: a long-lived advisory lock can no longer be acquired
# and released silently — both edges and the held-duration are logged, and the
# live set is queryable via :func:`held_advisory_locks` (used by the DB
# contention monitor to report "locks held by this pod").
_held_advisory_locks: Dict[int, Tuple[str, float]] = {}


def held_advisory_locks() -> Dict[int, Tuple[str, float]]:
    """Return a snapshot of session advisory locks held by THIS process.

    Maps ``lock_id`` -> ``(name, acquired_monotonic)``. Read-only copy; safe to
    iterate without racing the registry. Held seconds = ``time.monotonic() -
    acquired_monotonic``.
    """
    return dict(_held_advisory_locks)


def _get_probe_service_name() -> str:
    """Return the service identity for the probe log line.

    Resolution order mirrors the pattern used by the GCP liveness reconciler:
    instance.json → SERVICE_NAME env → literal "unknown".
    """
    import os
    try:
        from dynastore.modules.db_config.instance import get_service_name
        name = get_service_name()
        if name:
            return name
    except Exception:
        pass
    return os.getenv("SERVICE_NAME") or "unknown"


def _lease_breaker_record_success(lock_name: str) -> None:
    """Record a healthy CAS round-trip and log the breaker's recovery once.

    The breaker logs its own human-readable transitions; here we additionally
    emit a structured ``key=value`` line (matching the ``db_pool_acquire``
    idiom) only when the breaker actually closes, so log scrapers see a single
    recovery event rather than per-tick churn.
    """
    breaker = _get_lease_breaker()
    was_tripped = breaker.state_of(_LEASE_BREAKER_KEY) != "CLOSED"
    breaker.record_success(_LEASE_BREAKER_KEY)
    if was_tripped and breaker.state_of(_LEASE_BREAKER_KEY) == "CLOSED":
        logger.info(
            "leader_lease_breaker_closed service=%s lock_name=%s",
            _get_probe_service_name(),
            lock_name,
        )


def _lease_breaker_record_failure(lock_name: str) -> None:
    """Record a CAS failure and log once when it trips the breaker to OPEN.

    Only the CLOSED/HALF_OPEN -> OPEN transition is logged (structured
    ``key=value``); subsequent failures while already OPEN are silent to avoid
    per-tick churn.
    """
    breaker = _get_lease_breaker()
    was_open = breaker.state_of(_LEASE_BREAKER_KEY) == "OPEN"
    breaker.record_failure(_LEASE_BREAKER_KEY)
    if not was_open and breaker.state_of(_LEASE_BREAKER_KEY) == "OPEN":
        logger.warning(
            "leader_lease_breaker_open service=%s lock_name=%s",
            _get_probe_service_name(),
            lock_name,
        )


async def probe_lock_connection_liveness(
    conn: Any,
    *,
    timeout: float,
    name: str = "leader",
) -> None:
    """Check whether the advisory-lock connection is still alive.

    Runs a cheap ``SELECT 1`` on the provided connection under a bounded
    timeout.  If the wire has died (NAT idle reset, server restart), the
    query will hang or error rather than returning promptly.  Bounding it
    with ``asyncio.wait_for`` ensures detection within ``timeout`` seconds
    instead of waiting for the OS TCP timeout (often 75 s or more).

    This probe is intentionally NOT a re-lock attempt. PG session advisory
    locks are re-entrant on the same connection: calling
    ``pg_try_advisory_lock`` on the holder always returns ``true`` and
    leaks a lock level, making the lock harder to release. ``SELECT 1``
    tests wire liveness without touching the lock state.

    Raises
    ------
    asyncio.CancelledError
        Re-raised as-is; never treat shutdown/drain as a dead wire.
    DatabaseConnectionError
        On any other failure (including ``asyncio.TimeoutError``). The
        caller's leader loop catches this and resigns leadership so another
        pod can take over.
    """
    start = time.monotonic()
    try:
        await asyncio.wait_for(
            DQLQuery("SELECT 1", result_handler=ResultHandler.SCALAR).execute(conn),
            timeout=timeout,
        )
    except asyncio.CancelledError:
        raise
    except Exception as exc:
        elapsed_ms = int((time.monotonic() - start) * 1000)
        logger.warning(
            "leader_liveness_probe_failed service=%s name=%s elapsed_ms=%d err=%s",
            _get_probe_service_name(),
            name,
            elapsed_ms,
            exc,
        )
        # On a dead TCP socket the probe times out while asyncpg is still
        # awaiting the cancel-ack for the abandoned query. If we leave the
        # connection in that state, the pg_advisory_unlock issued by
        # pg_advisory_leadership's finally block blocks on the same dead wire
        # until the OS TCP timeout fires (~75s), delaying the lock handoff the
        # probe exists to accelerate. Invalidating the connection now tears the
        # transport down immediately so the unlock/close path returns promptly.
        # Best-effort: the connection is being resigned regardless.
        try:
            await conn.invalidate()
        except Exception:
            pass
        raise DatabaseConnectionError(
            f"leader liveness probe failed for {name!r}: {exc}"
        ) from exc


def retry_on_lock_conflict(max_retries: int | None = None, base_delay: float | None = None):
    """Decorator to retry database operations when encountering lock contention
    or asyncpg protocol 'operation in progress' errors.

    Configuration: Values are resolved from (1) explicit function parameters,
    else (2) the module-global ``ConnectionRetryConfig`` defaults.
    Resolved at CALL TIME. See :mod:`connection_health_config` for details.
    """

    def decorator(func):
        @functools.wraps(func)
        async def wrapper(*args, **kwargs):
            # Resolve config at call time for dynamic behavior
            cfg_max_retries, cfg_base_delay, _, _ = resolve_connection_retry_config()
            _max_retries = max_retries if max_retries is not None else cfg_max_retries
            _base_delay = base_delay if base_delay is not None else cfg_base_delay

            last_err = None
            for attempt in range(_max_retries):
                try:
                    return await func(*args, **kwargs)
                except Exception as e:
                    last_err = e
                    err_str = str(e).lower()
                    retryable = any(
                        x in err_str
                        for x in [
                            "55p03",
                            "40p01",
                            "lock_timeout",
                            "deadlock",
                            "operation is in progress",
                            "interfaceerror",
                            "connection does not exist",
                            "connection was closed",
                            "08003",
                            "databaseconnectionerror",
                            "connection is closed",
                            "connectionerror",
                        ]
                    )

                    if not retryable or attempt == _max_retries - 1:
                        raise

                    delay = _base_delay * (2**attempt)
                    logger.warning(
                        f"Conflict on wire/DB (attempt {attempt + 1}/{_max_retries}): {e}\nRetrying in {delay:.2f}s..."
                    )
                    await asyncio.sleep(delay)
            if last_err:
                raise last_err

        return wrapper

    return decorator


# Global coordinator to dedupe identical startup tasks within the same process.
class _StartupCoordinator:
    _tasks: ClassVar[Dict[str, asyncio.Future]] = {}
    _lock = LoopLocalLock()
    # Strong refs for the fire-and-forget ``_cleanup`` tasks. Without this,
    # asyncio only keeps weak refs and a GC sweep can collect the cleanup
    # mid-sleep — the cached future would never be evicted and a later
    # retry of the same key would see a "still in progress" hit instead of
    # re-running the coroutine.
    _cleanup_tasks: ClassVar[Set[asyncio.Task]] = set()

    @classmethod
    async def run_once(cls, key: str, coro_func: Callable[[], Awaitable[T]]) -> T:
        async with cls._lock:
            if key in cls._tasks:
                return await cls._tasks[key]

            future = asyncio.Future()
            cls._tasks[key] = future

        try:
            result = await coro_func()
            if not future.done():
                future.set_result(result)

            # Schedule cleanup for success case (keep result briefly)
            async def _cleanup():
                await asyncio.sleep(5)
                async with cls._lock:
                    # Only pop if it's still the SAME future
                    if cls._tasks.get(key) is future:
                        cls._tasks.pop(key, None)

            cleanup_task = asyncio.create_task(_cleanup())
            cls._cleanup_tasks.add(cleanup_task)
            cleanup_task.add_done_callback(cls._cleanup_tasks.discard)

            return result
        except Exception as e:
            if not future.done():
                future.set_exception(e)

            # Immediate cleanup on failure so retries can happen
            async with cls._lock:
                # Only pop if it's still the SAME future
                if cls._tasks.get(key) is future:
                    cls._tasks.pop(key, None)
            raise


def _get_stable_lock_id(key: str) -> int:
    """Generates a stable 64-bit integer from a string key for Postgres advisory locks.

    Alias of :func:`dynastore.modules.tasks.durable.locks.stable_lock_id_sha256`.
    The canonical home is the tasks durable submodule; the output is frozen.
    """
    return _stable_lock_id_sha256(key)


@asynccontextmanager
async def pg_advisory_leadership(
    engine: Optional[DbResource],
    key: Union[int, str],
    *,
    name: str = "leader",
) -> AsyncGenerator[Tuple[bool, Optional[AsyncConnection]], None]:
    """Non-blocking leadership election via a PG session advisory lock.

    Canonical leadership context manager for :func:`dynastore.tools.
    async_utils.run_leader_loop`. Yields ``(is_leader, lock_connection)`` where
    ``is_leader`` is ``True`` if this process became the leader, and
    ``lock_connection`` is the dedicated AUTOCOMMIT connection holding the
    advisory lock (or ``None`` if not leader).

    The lock connection can be reused by the leader for DB work during the
    tenure, avoiding the need to acquire a second connection from the pool.
    This is critical for pool-constrained environments (Cloud Run) where
    holding both a lock connection and a work connection can starve the pool.

    Design invariants (each one fixes a production failure mode):

    * The lock is taken on a **dedicated AUTOCOMMIT connection**, never on a
      pooled transaction. Session advisory locks belong to the connection; on
      a pooled one a failed unlock would leak the lock into pool inventory and
      permanently block leadership fleet-wide. Here the connection is closed
      in ``finally``, which releases the lock even if the explicit unlock
      fails — and a long leadership tenure never holds a transaction open.
    * Failures *before* leadership is yielded (connect, AUTOCOMMIT switch,
      acquire query) degrade to ``yield (False, None)``: the caller is simply
      not the leader this round.
    * Failures *after* ``yield (True, conn)`` (raised in the caller's body, or
      by the unlock/close steps) propagate so the loop can resign loudly and
      retry. Never ``yield`` from an ``except`` around the leadership
      ``yield`` — a second yield makes ``contextlib`` raise
      ``generator didn't stop``.

    ``key`` may be a 64-bit int used as-is, or a string folded to one via
    :func:`_get_stable_lock_id`. Requires an :class:`AsyncEngine`; any other
    engine (or ``None``) yields ``(False, None)`` with a warning, matching the
    events consumer precedent — single-process sync deployments get no election.

    Known property: if the lock connection dies mid-tenure PG releases the
    lock and another instance may become leader while this one finishes its
    tick. Leader-run jobs must stay idempotent under that overlap.
    """
    if not isinstance(engine, AsyncEngine):
        logger.warning(
            "%s: leadership requires AsyncEngine (got %s); not a leader.",
            name,
            type(engine).__name__ if engine is not None else "None",
        )
        yield (False, None)
        return
    lock_id = key if isinstance(key, int) else _get_stable_lock_id(key)
    conn_ctx = engine.connect()
    try:
        conn = await conn_ctx.__aenter__()
    except Exception as exc:
        logger.warning(
            "%s: leadership connect failed (%s); not a leader.", name, exc
        )
        yield (False, None)
        return
    try:
        try:
            conn = await conn.execution_options(isolation_level="AUTOCOMMIT")
            acquired = bool(
                await DQLQuery(
                    "SELECT pg_try_advisory_lock(:id)",
                    result_handler=ResultHandler.SCALAR,
                ).execute(conn, id=lock_id)
            )
        except Exception as exc:
            logger.warning(
                "%s: leadership acquisition failed (%s); not a leader.",
                name,
                exc,
            )
            acquired = False
        if not acquired:
            yield (False, None)
            return
        acquired_at = time.monotonic()
        _held_advisory_locks[lock_id] = (name, acquired_at)
        logger.info(
            "%s: leadership lock acquired (key=%s, advisory_locks_held_now=%d).",
            name,
            key,
            len(_held_advisory_locks),
        )
        try:
            yield (True, conn)
        finally:
            _held_advisory_locks.pop(lock_id, None)
            logger.info(
                "%s: leadership lock released (key=%s, held=%.1fs, "
                "advisory_locks_held_now=%d).",
                name,
                key,
                time.monotonic() - acquired_at,
                len(_held_advisory_locks),
            )
            try:
                await DQLQuery(
                    "SELECT pg_advisory_unlock(:id)",
                    result_handler=ResultHandler.NONE,
                ).execute(conn, id=lock_id)
            except Exception:
                pass  # closing the dedicated connection releases the lock
    finally:
        try:
            await conn_ctx.__aexit__(None, None, None)
        except Exception:
            pass


@asynccontextmanager
async def lease_leadership(
    engine: Optional[DbResource],
    key: Union[int, str],
    *,
    name: str = "leader",
) -> AsyncGenerator[Tuple[bool, Optional[AsyncConnection]], None]:
    """Non-blocking leadership election via a lease table CAS.

    Drop-in replacement for :func:`pg_advisory_leadership` that works under
    transaction-mode connection pooling (AlloyDB, PgBouncer transaction mode).
    Each call is a single INSERT … ON CONFLICT statement inside
    :func:`managed_transaction` — the connection is returned to the pool on
    COMMIT, so no connection pinning occurs.

    Yields ``(is_leader, None)``.  The second element is always ``None``
    because lease election does not hold a dedicated connection between
    yield and release.  Callers that need a DB connection for their tick
    work must acquire one from the pool via :func:`get_engine`.

    The lock identity is the same ``_get_stable_lock_id`` derivation used by
    :func:`pg_advisory_leadership`, so a cluster can migrate backends without
    key drift: ``key`` may be a 64-bit int (used as-is) or a string (folded
    via sha256).

    Design invariants:

    * Exactly ONE ``yield`` per code path — failures before the yield
      degrade to ``yield (False, None)``; a second yield would make
      ``contextlib`` raise ``RuntimeError: generator didn't stop``.
    * On WIN the lock_id is registered in ``_held_advisory_locks`` so
      :func:`held_advisory_locks` and the DB contention monitor keep
      reporting.  The entry is removed in ``finally``.
    * On WIN the ``finally`` block issues a best-effort expire UPDATE to
      release early; errors are swallowed because the lease expires
      naturally.
    * Per-tick model: this CM is entered/exited per tick by
      :class:`~dynastore.tools.background_service.BackgroundSupervisor`.
      No separate renewal task is needed.
    * Resilience: a circuit breaker short-circuits the CAS while the DB is
      down (decline immediately rather than hammer it); a modest transient
      retry covers a single backend drop mid-statement. Both reuse existing
      primitives (:class:`CircuitBreaker`, :func:`retry_on_transient_connect`).
    """
    if not isinstance(engine, AsyncEngine):
        logger.warning(
            "%s: leadership requires AsyncEngine (got %s); not a leader.",
            name,
            type(engine).__name__ if engine is not None else "None",
        )
        yield (False, None)
        return
    lock_id = key if isinstance(key, int) else _get_stable_lock_id(key)
    ttl = _leadership_config.lease_ttl_seconds

    # Circuit breaker: when OPEN the CAS has failed repeatedly, so decline this
    # tick without touching the DB rather than hammering a database that is
    # down. is_open() advances OPEN -> HALF_OPEN once the cooldown elapses, so
    # the next tick is let through as a recovery probe.
    if _get_lease_breaker().is_open(_LEASE_BREAKER_KEY):
        yield (False, None)
        return

    # The CAS runs at CM entry, NOT under the caller's tick_timeout, so retry a
    # transient backend drop (08003 under a transaction-mode pooler) here on a
    # fresh connection instead of silently skipping a maintenance tick. Budget
    # is deliberately MODEST (max_retries=3 -> ~0.5/1/2s, ≈3.5s worst case): it
    # MUST stay well under lease_ttl_seconds, otherwise a struggling DB would
    # delay every pod's election by the full default ~15s budget each tick.
    @retry_on_transient_connect(max_retries=3)
    async def _run_cas() -> Any:
        async with managed_transaction(engine) as _conn:
            aconn = cast(AsyncConnection, _conn)
            result = await aconn.execute(
                text(_LEASE_CAS_SQL),
                {"lock_key": lock_id, "name": name, "owner": _LEASE_OWNER, "ttl": ttl},
            )
            return result.fetchone()

    try:
        row = await _run_cas()
    except Exception as exc:
        # Retry budget exhausted: trip the breaker (logged once on transition)
        # and decline. Fail-safe — a failed election never crashes the loop.
        logger.warning(
            "%s: lease acquire failed (%s); not a leader.", name, exc
        )
        _lease_breaker_record_failure(name)
        yield (False, None)
        return
    # The CAS round-tripped (1 row = win, 0 rows = a live foreign owner); either
    # way the DB is healthy, so reset the breaker.
    _lease_breaker_record_success(name)
    # RETURNING yields a row iff our CAS WHERE matched (insert, takeover of an
    # expired lease, or renew of our own), and the upsert sets owner to ours —
    # so a non-None row always means we own the lease. Zero rows = a live
    # foreign owner blocked the update.
    won = row is not None
    if not won:
        yield (False, None)
        return
    acquired_at = time.monotonic()
    _held_advisory_locks[lock_id] = (name, acquired_at)
    logger.info(
        "%s: lease leadership acquired (key=%s, advisory_locks_held_now=%d).",
        name,
        key,
        len(_held_advisory_locks),
    )
    try:
        yield (True, None)
    finally:
        _held_advisory_locks.pop(lock_id, None)
        logger.info(
            "%s: lease leadership released (key=%s, held=%.1fs, "
            "advisory_locks_held_now=%d).",
            name,
            key,
            time.monotonic() - acquired_at,
            len(_held_advisory_locks),
        )
        try:
            async with managed_transaction(engine) as _rconn:
                arconn = cast(AsyncConnection, _rconn)
                await arconn.execute(
                    text(
                        "UPDATE configs.leader_lease"
                        " SET expires_at = now()"
                        " WHERE lock_key = :lock_key AND owner = :owner"
                    ),
                    {"lock_key": lock_id, "owner": _LEASE_OWNER},
                )
        except Exception:
            pass  # best-effort; lease expires naturally via expires_at


@contextmanager
def sync_acquire_startup_lock(
    conn: DbResource, lock_key: str, timeout: str | None = None
) -> "Iterator[Optional[DbResource]]":
    """Synchronous version of acquire_startup_lock for DDL coordination.

    Configuration: Timeout is resolved from (1) the function parameter, else
    (2) ``LeadershipConfig.lock_acquire_timeout_seconds`` (default 30s).
    """
    if timeout is None:
        timeout_secs, _, _, _, _ = resolve_leadership_config()
        timeout = f"{timeout_secs}s"

    if isinstance(conn, Engine):
        with sync_managed_transaction(conn) as tx_conn:
            with sync_acquire_startup_lock(tx_conn, lock_key, timeout) as active:
                yield active
        return

    lock_id = _get_stable_lock_id(lock_key)

    q_try = DQLQuery(
        "SELECT pg_try_advisory_xact_lock(:lock_id)",
        result_handler=ResultHandler.SCALAR,
    )
    acquired = q_try._executor._execute_sync(
        conn,
        q_try._executor.query_builder_strategy.build(conn, {"lock_id": lock_id})[0],
        {"lock_id": lock_id},
    )

    if not acquired:
        logger.debug(f"Lock {lock_key} busy, waiting up to {timeout}...")
        q_set = DDLQuery(f"SET LOCAL lock_timeout = '{timeout}'")
        q_set._executor._execute_sync(
            conn,
            q_set._executor.query_builder_strategy.build(conn, {})[0],
            {},
        )

        q_wait = DQLQuery(
            "SELECT pg_advisory_xact_lock(:lock_id)", result_handler=ResultHandler.NONE
        )
        try:
            q_wait._executor._execute_sync(
                conn,
                q_wait._executor.query_builder_strategy.build(
                    conn, {"lock_id": lock_id}
                )[0],
                {"lock_id": lock_id},
            )
            acquired = True
        except Exception as e:
            logger.warning(
                f"Failed to acquire advisory lock {lock_key} within {timeout}: {e}"
            )
            raise

    if acquired:
        logger.debug(f"Acquired advisory lock (sync): {lock_key}")
        yield conn
    else:
        yield None


@asynccontextmanager
async def acquire_startup_lock(
    conn: DbResource, lock_key: str, timeout: str | None = None
) -> AsyncGenerator[Optional[DbResource], None]:
    """Acquires an advisory lock for coordination.

    Serialization is handled internally by Query Executor.
    Ensures all operations happen on the same connection if an engine is provided.

    Configuration: Timeout is resolved from (1) the function parameter, else
    (2) ``LeadershipConfig.lock_acquire_timeout_seconds`` (default 30s).
    """
    if timeout is None:
        timeout_secs, _, _, _, _ = resolve_leadership_config()
        timeout = f"{timeout_secs}s"

    if isinstance(conn, (AsyncEngine, Engine)):
        async with managed_transaction(conn) as tx_conn:
            async with acquire_startup_lock(tx_conn, lock_key, timeout) as active:
                yield active
        return

    lock_id = _get_stable_lock_id(lock_key)

    q_try = DQLQuery(
        "SELECT pg_try_advisory_xact_lock(:lock_id)",
        result_handler=ResultHandler.SCALAR,
    )
    acquired = await q_try.execute(conn, lock_id=lock_id)

    if not acquired:
        logger.debug(f"Lock {lock_key} busy, waiting up to {timeout}...")
        await DDLQuery(f"SET LOCAL lock_timeout = '{timeout}'").execute(conn)

        q_wait = DQLQuery(
            "SELECT pg_advisory_xact_lock(:lock_id)", result_handler=ResultHandler.NONE
        )
        try:
            await q_wait.execute(conn, lock_id=lock_id)
            acquired = True
        except Exception as e:
            logger.warning(
                f"Failed to acquire advisory lock {lock_key} within {timeout}: {e}"
            )
            raise

    if acquired:
        logger.debug(f"Acquired advisory lock: {lock_key}")
        yield conn
    else:
        yield None


@asynccontextmanager
async def acquire_lock_if_needed(
    conn: DbResource, lock_key: str, check_fn: Callable[[], Awaitable[bool]]
):
    """
    Acquires an advisory lock only if the resource doesn't already exist.
    Uses acquire_startup_lock with the provided lock_key for correct per-resource
    lock coordination (avoids lock contention between different resources).

    Re-entrant: if the same lock_key is already held in the current async call
    stack (e.g. nested initialization), the lock attempt is skipped and the
    connection is yielded directly — matching PostgreSQL's own re-entrant
    advisory lock semantics.

    Yields the active connection if the lock was acquired (resource needs creation),
    or False if the resource already exists (no lock needed).
    """
    # Fast path: check without locking first
    if await check_fn():
        yield False
        return

    # Re-entrancy check: if this key is already held in the current call stack,
    # skip the lock attempt (PostgreSQL advisory xact locks are re-entrant at DB level).
    held = _held_lock_keys.get()
    if held is None:
        held = set()
        _held_lock_keys.set(held)

    if lock_key in held:
        logger.debug("Re-entrant lock request for key '%s' — skipping lock acquisition.", lock_key)
        yield conn
        return

    # Slow path: acquire the lock scoped to this specific resource key,
    # then re-check inside the lock to avoid TOCTOU races.
    held.add(lock_key)
    try:
        async with acquire_startup_lock(conn, lock_key) as active_conn:
            if active_conn is None:
                # Lock timed out — another process likely holds it.
                # Re-check: if it now exists, we can skip.
                if await check_fn():
                    yield False
                    return
                raise RuntimeError(f"Failed to acquire advisory lock for key: {lock_key}")

            # Re-check inside the lock (double-checked locking pattern)
            if await check_fn():
                yield False
                return

            yield active_conn
    finally:
        held.discard(lock_key)


async def check_table_exists(
    conn: DbResource, table_name: str, schema: str = "platform"
) -> bool:
    """Checks if a table (or partition) exists in the given schema.

    Uses to_regclass() which is transaction-visible and handles concurrent DDL
    correctly — unlike pg_tables which can briefly lag after a COMMIT.
    Returns False if the relation is absent; propagates connection errors.
    """
    from dynastore.modules.db_config.maintenance_tools import DQLQuery, ResultHandler

    fq = f'"{schema}"."{table_name}"'
    res = await DQLQuery(
        "SELECT to_regclass(:fq)",
        result_handler=ResultHandler.SCALAR,
    ).execute(conn, fq=fq)
    return res is not None


async def check_schema_exists(conn: DbResource, schema_name: str) -> bool:
    """Checks if a schema exists.

    Uses to_regnamespace() for the same transaction-visibility guarantees as
    check_table_exists. Propagates connection errors instead of masking them.
    """
    from dynastore.modules.db_config.maintenance_tools import DQLQuery, ResultHandler

    res = await DQLQuery(
        "SELECT to_regnamespace(:schema)",
        result_handler=ResultHandler.SCALAR,
    ).execute(conn, schema=schema_name)
    return res is not None


async def check_extension_exists(conn: DbResource, extension_name: str) -> bool:
    """Checks if an extension is installed."""
    from dynastore.modules.db_config.maintenance_tools import DQLQuery, ResultHandler

    query = DQLQuery(
        "SELECT 1 FROM pg_extension WHERE extname = :extension",
        result_handler=ResultHandler.SCALAR,
    )
    try:
        return await query.execute(conn, extension=extension_name) is not None
    except Exception:
        return False


async def check_trigger_exists(
    conn: DbResource,
    trigger_name: str,
    schema: str = "platform",
    table: Optional[str] = None,
) -> bool:
    """Checks if a trigger exists.

    When *table* is provided, the match is scoped to that specific relation —
    required when the same trigger name is applied per-table across a schema
    (e.g. ``trg_asset_cleanup`` on every tenant asset/sidecar table).
    """
    from dynastore.modules.db_config.maintenance_tools import DQLQuery, ResultHandler

    if table is None:
        sql = (
            "SELECT 1 FROM pg_trigger t "
            "JOIN pg_class c ON c.oid = t.tgrelid "
            "JOIN pg_namespace n ON n.oid = c.relnamespace "
            "WHERE n.nspname = :schema AND t.tgname = :name"
        )
        params = {"schema": schema, "name": trigger_name}
    else:
        sql = (
            "SELECT 1 FROM pg_trigger t "
            "JOIN pg_class c ON c.oid = t.tgrelid "
            "JOIN pg_namespace n ON n.oid = c.relnamespace "
            "WHERE n.nspname = :schema AND c.relname = :table AND t.tgname = :name"
        )
        params = {"schema": schema, "table": table, "name": trigger_name}

    query = DQLQuery(sql, result_handler=ResultHandler.SCALAR)
    try:
        return await query.execute(conn, **params) is not None
    except Exception:
        return False


async def check_cron_job_exists(conn: DbResource, job_name: str) -> bool:
    """Checks if a pg_cron job exists.

    Returns ``False`` when the ``cron.job`` table is absent (pg_cron not
    installed), mirroring the graceful-false behaviour of
    :func:`check_extension_exists`.
    """
    from dynastore.modules.db_config.maintenance_tools import DQLQuery, ResultHandler

    if not await check_extension_exists(conn, "pg_cron"):
        return False
    query = DQLQuery(
        "SELECT 1 FROM cron.job WHERE jobname = :job_name",
        result_handler=ResultHandler.SCALAR,
    )
    try:
        return await query.execute(conn, job_name=job_name) is not None
    except Exception:
        return False


async def check_function_exists(
    conn: DbResource, function_name: str, schema: str = "platform"
) -> bool:
    """Checks if a function exists."""
    from dynastore.modules.db_config.maintenance_tools import DQLQuery, ResultHandler

    query = DQLQuery(
        "SELECT 1 FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace WHERE n.nspname = :schema AND p.proname = :name",
        result_handler=ResultHandler.SCALAR,
    )
    try:
        return await query.execute(conn, schema=schema, name=function_name) is not None
    except Exception:
        return False


# --- Termination Helpers ---


async def terminate_backends_locking_schema(conn: DbResource, schema_name: str) -> int:
    """
    Terminates all backend processes holding locks on any object within a schema.
    Excludes the current connection's backend.
    """
    sql = """
    SELECT pg_terminate_backend(pid)
    FROM pg_locks l
    JOIN pg_class c ON l.relation = c.oid
    JOIN pg_namespace n ON c.relnamespace = n.oid
    WHERE n.nspname = :schema
      AND pid <> pg_backend_pid();
    """
    q = DQLQuery(sql, result_handler=ResultHandler.ALL_SCALARS)
    try:
        results = await q.execute(conn, schema=schema_name)
        count = len(results)
        if count > 0:
            logger.warning(
                f"Terminated {count} backends locking objects in schema '{schema_name}'"
            )
        return count
    except Exception as e:
        logger.error(f"Failed to terminate backends for schema '{schema_name}': {e}")
        return 0


async def terminate_backends_locking_table(
    conn: DbResource, schema_name: str, table_name: str
) -> int:
    """
    Terminates all backend processes holding locks on a specific table.
    Excludes the current connection's backend.
    """
    sql = """
    SELECT pg_terminate_backend(pid)
    FROM pg_locks l
    JOIN pg_class c ON l.relation = c.oid
    JOIN pg_namespace n ON c.relnamespace = n.oid
    WHERE n.nspname = :schema
      AND c.relname = :table
      AND pid <> pg_backend_pid();
    """
    q = DQLQuery(sql, result_handler=ResultHandler.ALL_SCALARS)
    try:
        results = await q.execute(conn, schema=schema_name, table=table_name)
        count = len(results)
        if count > 0:
            logger.warning(
                f"Terminated {count} backends locking table '{schema_name}.{table_name}'"
            )
        return count
    except Exception as e:
        logger.error(
            f"Failed to terminate backends for table '{schema_name}.{table_name}': {e}"
        )
        return 0


async def force_truncate_table(conn: DbResource, schema_name: str, table_name: str):
    """
    Forcefully clears a table using DELETE instead of TRUNCATE to avoid deadlocks.
    """
    await terminate_backends_locking_table(conn, schema_name, table_name)
    # Give a small window for backends to actually exit
    await asyncio.sleep(0.1)
    await DDLQuery(f'DELETE FROM "{schema_name}"."{table_name}";').execute(
        conn
    )


async def force_drop_schema(conn: DbResource, schema_name: str):
    """
    Forcefully drops a schema by terminating any blocking backends first.
    """
    await terminate_backends_locking_schema(conn, schema_name)
    # Give a small window for backends to actually exit
    await asyncio.sleep(0.1)
    await DDLQuery(f'DROP SCHEMA "{schema_name}" CASCADE;').execute(conn)


# --- Safe DROP for hot relations ---


async def safe_drop_relation(
    conn: DbResource,
    schema: str,
    relation: str,
    kind: str = "table",
    *,
    cascade: bool = False,
    lock_timeout: str = "5s",
    max_retries: int = 3,
    on_table: Optional[str] = None,
) -> None:
    """Drop a relation under a bounded ``lock_timeout`` with retries.

    ``DROP`` on a hot relation takes ``AccessExclusiveLock`` and will deadlock
    against concurrent DML. This helper runs ``SET LOCAL lock_timeout`` before
    the DROP so a blocked statement fails fast (SQLSTATE 55P03) and retries
    on transient lock / deadlock codes via :func:`retry_on_lock_conflict`.

    Parameters
    ----------
    conn : DbResource
        Active connection or engine.
    schema : str
        Target schema (unquoted).
    relation : str
        Target relation name (unquoted).
    kind : {"table", "index", "trigger", "schema"}
        Kind of object. For ``trigger``, ``on_table`` is required.
    cascade : bool
        Append ``CASCADE`` to the DROP.
    lock_timeout : str
        PostgreSQL lock_timeout string; default ``5s``.
    max_retries : int
        Max retries on 55P03 / 40P01.
    on_table : str | None
        For ``kind='trigger'``: the table the trigger is attached to.
    """
    kind_lower = kind.lower()
    tail = " CASCADE" if cascade else ""
    if kind_lower == "table":
        sql = f'DROP TABLE IF EXISTS "{schema}"."{relation}"{tail};'
    elif kind_lower == "index":
        sql = f'DROP INDEX IF EXISTS "{schema}"."{relation}"{tail};'
    elif kind_lower == "trigger":
        if not on_table:
            raise ValueError("on_table is required when kind='trigger'")
        sql = f'DROP TRIGGER IF EXISTS "{relation}" ON "{schema}"."{on_table}"{tail};'
    elif kind_lower == "schema":
        sql = f'DROP SCHEMA IF EXISTS "{schema}"{tail};'
    else:
        raise ValueError(f"unsupported kind: {kind!r}")

    @retry_on_lock_conflict(max_retries=max_retries)
    async def _drop():
        async with managed_transaction(conn) as tx:
            atx = cast(AsyncConnection, tx)
            await atx.execute(text(f"SET LOCAL lock_timeout = '{lock_timeout}'"))
            await atx.execute(text(sql))

    await _drop()
