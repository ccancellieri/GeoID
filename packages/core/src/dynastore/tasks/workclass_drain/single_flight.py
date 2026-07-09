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

# dynastore/tasks/workclass_drain/single_flight.py

"""Session-scoped advisory single-flight gate for in-process drains.

Claim-version fencing (#1945) keeps duplicate drains *correct* — a stale
owner's terminal CAS misses — but not *cheap*: when heartbeat writes lag
behind a congested pooler, the reaper resets lapsed rows and a second pod
re-drains work the original owner is still processing, doubling the
memory/decode transient inside an API-serving container.  In-process drains
are a safety valve, not the throughput path (the offloaded Cloud Run Job
flavors are), so serializing them platform-wide costs nothing that is
actually relied on.

The gate is a PostgreSQL *session*-scoped ``pg_try_advisory_lock`` held for
the full duration of a drain run.  A transaction-scoped lock cannot close
this window: the duplicate claim happens in a *later* transaction, long
after the first pod's claim transaction (and any xact-scoped lock) is gone.

Session state is exactly what a transaction-mode pooler does not preserve,
so the lock rides a dedicated direct (non-pooled) connection:

* ``DBConfig.listen_database_url`` when configured — the direct lane that
  already exists because LISTEN has the same session-affinity requirement;
* else ``DBConfig.database_url`` when it is not behind a transaction-mode
  pooler;
* else there is no trustworthy session lane and the gate **fails open** —
  the drain proceeds ungated, exactly as it behaved before the gate existed.

Fail-open is the posture throughout (connect failure, lock-query failure,
timeout): the gate must never turn a drain that would have run into one
that silently never runs.  If the gate connection dies mid-run PostgreSQL
releases the lock server-side and another pod may start a run — the same
wedge tolerance the offload-liveness probe applies, and the reclaim grace
on drain workclasses bounds the damage of that window.

Only in-process runs consult the gate; the offloaded job flavors never do,
so an offload run cannot fence itself out (same shape as the in-process
budget and the offload-liveness probe).
"""

import asyncio
import logging
from typing import Any, Optional

from dynastore.modules.tasks.durable.locks import stable_lock_id_blake2b

logger = logging.getLogger(__name__)

# Bounds on the gate's own DB operations. The gate is an optimization guard —
# it must never hold up (or wedge) the drain it protects, so every step is
# individually bounded and any overrun fails open.
_GATE_CONNECT_TIMEOUT_SECONDS: float = 5.0
_GATE_QUERY_TIMEOUT_SECONDS: float = 5.0

# First :func:`stable_lock_id_blake2b` part — namespaces the drain gate keys
# away from the dispatcher's xact-scoped serialization guards, which share
# the same PostgreSQL advisory-lock space regardless of lock scope.
_LOCK_NAMESPACE: str = "workclass_drain_single_flight"


def _direct_lane_dsn() -> Optional[str]:
    """DSN of a lane whose sessions are trustworthy for session-scoped locks.

    Returns ``None`` when every configured lane multiplexes sessions (a
    transaction-mode pooler with no direct LISTEN lane configured) — the
    caller must fail open rather than take a lock whose release could land
    on a different backend.
    """
    from dynastore.modules.db_config.db_config import DBConfig
    from dynastore.modules.db_config.db_timeout_config import is_transaction_pooler

    listen_url = str(getattr(DBConfig, "listen_database_url", "") or "").strip()
    if listen_url:
        return listen_url.replace("postgresql+asyncpg://", "postgresql://")
    if not is_transaction_pooler(DBConfig):
        dsn = str(DBConfig.database_url or "").strip()
        if dsn:
            return dsn.replace("postgresql+asyncpg://", "postgresql://")
    return None


class DrainSingleFlightGate:
    """One in-process drain per workclass, platform-wide.

    Usage (mirrors both drain tasks' ``run()``)::

        gate = DrainSingleFlightGate("storage")
        try:
            if not await gate.acquire():
                return <skip report>          # another pod's run is active
            ...drain...
        finally:
            await gate.release()              # idempotent

    ``acquire()`` returns ``False`` only on a *positive* signal that another
    holder exists; every failure mode returns ``True`` (fail open).
    """

    def __init__(self, workclass: str) -> None:
        self._workclass = workclass
        self._lock_key = stable_lock_id_blake2b(_LOCK_NAMESPACE, workclass)
        self._conn: Optional[Any] = None
        self._held = False

    async def acquire(self) -> bool:
        """Try to take the workclass gate; ``False`` means skip this run."""
        dsn = _direct_lane_dsn()
        if dsn is None:
            logger.debug(
                "DrainSingleFlightGate(%s): no direct session lane configured — "
                "running ungated.",
                self._workclass,
            )
            return True

        import asyncpg  # local import: keeps module import light

        from dynastore.modules.db_config.db_config import DBConfig
        from dynastore.modules.db_config.instance import get_stamped_application_name

        try:
            configured_connect_timeout = int(
                getattr(DBConfig, "connect_timeout", 0)
                or _GATE_CONNECT_TIMEOUT_SECONDS
            )
            conn = await asyncpg.connect(
                dsn,
                timeout=min(
                    int(_GATE_CONNECT_TIMEOUT_SECONDS), configured_connect_timeout
                ),
                statement_cache_size=0,
                server_settings={
                    "application_name": get_stamped_application_name(),
                },
            )
        except Exception as exc:  # noqa: BLE001 — the gate is best-effort by design
            logger.debug(
                "DrainSingleFlightGate(%s): gate connection failed (%s) — "
                "running ungated.",
                self._workclass,
                exc,
            )
            return True

        try:
            got = await asyncio.wait_for(
                conn.fetchval("SELECT pg_try_advisory_lock($1)", self._lock_key),
                timeout=_GATE_QUERY_TIMEOUT_SECONDS,
            )
        except Exception as exc:  # noqa: BLE001 — fail open, never block the drain
            logger.debug(
                "DrainSingleFlightGate(%s): lock acquisition failed (%s) — "
                "running ungated.",
                self._workclass,
                exc,
            )
            await self._close(conn)
            return True

        if not got:
            await self._close(conn)
            return False

        self._conn = conn
        self._held = True
        return True

    async def release(self) -> None:
        """Release the gate. Idempotent; safe when acquire() failed or skipped.

        The explicit unlock is a courtesy for observability (the lock row
        disappears immediately instead of at TCP teardown); closing the
        session releases the advisory lock server-side regardless, so a
        failed unlock is never an error.
        """
        conn, self._conn = self._conn, None
        held, self._held = self._held, False
        if conn is None:
            return
        if held:
            try:
                await asyncio.wait_for(
                    conn.fetchval("SELECT pg_advisory_unlock($1)", self._lock_key),
                    timeout=_GATE_QUERY_TIMEOUT_SECONDS,
                )
            except Exception:  # noqa: BLE001 — session close releases it anyway
                pass
        await self._close(conn)

    @staticmethod
    async def _close(conn: Any) -> None:
        try:
            await asyncio.wait_for(
                conn.close(), timeout=_GATE_QUERY_TIMEOUT_SECONDS
            )
        except Exception:  # noqa: BLE001 — a dead wire tears down on GC
            pass
