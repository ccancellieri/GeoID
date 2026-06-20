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

"""Read-only DB contention monitor — observability for locks vs. slow queries.

Why this exists
---------------
When the shared Postgres instance is implicated in an incident, the first
question is always "is this lock contention or just slow queries / an
overloaded box?". Today that question is hard to answer after the fact: the
app already bounds lock *waits* (``lock_timeout``), but a genuinely slow query
runs until the asyncpg ``command_timeout`` cancels it CLIENT-side, leaving no
server log line — the request just returns a 504 with no attributable cause.

This monitor closes that gap with a periodic, **read-only** snapshot of
``pg_stat_activity`` / ``pg_locks``:

* a one-line INFO heartbeat (connection counts vs. ``max_connections``, active /
  idle / idle-in-transaction, queries waiting on a lock, longest active query,
  advisory locks held instance-wide, and the advisory locks held by THIS pod);
* escalated WARNING detail when something is actually wrong — every query
  blocked on a lock (with its blocker PIDs, via ``pg_blocking_pids``), every
  long-running active query, and connection-pool pressure.

It distinguishes the two failure modes directly: ``waiting_on_lock`` + blocked
rows ⇒ locking; ``longest_active`` high with ``waiting_on_lock`` zero ⇒ slow
queries / DB resource contention.

Runs leader-elected (one pod per fleet) on its own advisory lock, mirroring the
maintenance supervisor / reaper loops. It only reads catalog/stats views and
never mutates anything.
"""

from __future__ import annotations

import asyncio
import logging
import os
import time
from dataclasses import dataclass
from typing import Any, Optional, Union

from dynastore.modules.db_config.locking_tools import (
    held_advisory_locks,
)
from dynastore.modules.db_config.query_executor import (
    DQLQuery,
    ResultHandler,
    managed_transaction,
)
from dynastore.tools.background_service import (
    Leadership,
    PeriodicService,
    PodPolicy,
    ServiceContext,
)
from dynastore.tools.protocol_helpers import get_engine

logger = logging.getLogger(__name__)

# Advisory lock key for leader election — must not collide with other loops
# (supervisor 0x4D41494E_54454E41, reaper 0x5D3A7E1F_C2B84961, lifecycle reaper).
# ASCII "LOCKMONI"; a deterministic constant inside the signed bigint range.
_CONTENTION_MONITOR_LOCK_KEY = 0x4C4F434B_4D4F4E49


def _env_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in ("1", "true", "yes", "on")


def _env_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None or not raw.strip():
        return default
    try:
        return int(raw)
    except ValueError:
        return default


def _env_float(name: str, default: float) -> float:
    raw = os.getenv(name)
    if raw is None or not raw.strip():
        return default
    try:
        return float(raw)
    except ValueError:
        return default


@dataclass
class DbContentionMonitorConfig:
    """Env-driven configuration. Read once at startup (restart to change)."""

    enabled: bool = True
    interval_seconds: int = 30
    # Active query age (s) above which a query is reported as slow (WARNING).
    slow_query_seconds: int = 10
    # Lock-wait age (s) above which a blocked query is reported (WARNING).
    lock_wait_seconds: int = 2
    # Max detail rows logged per category per tick.
    detail_limit: int = 10
    # total/max_connections ratio above which pool pressure is WARNINGed.
    conn_pressure_ratio: float = 0.8

    @classmethod
    def from_env(cls) -> "DbContentionMonitorConfig":
        return cls(
            enabled=_env_bool("DB_CONTENTION_MONITOR_ENABLED", True),
            interval_seconds=_env_int("DB_CONTENTION_MONITOR_INTERVAL_SECONDS", 30),
            slow_query_seconds=_env_int("DB_CONTENTION_SLOW_QUERY_SECONDS", 10),
            lock_wait_seconds=_env_int("DB_CONTENTION_LOCK_WAIT_SECONDS", 2),
            detail_limit=_env_int("DB_CONTENTION_DETAIL_LIMIT", 10),
            conn_pressure_ratio=_env_float("DB_CONTENTION_CONN_PRESSURE_RATIO", 0.8),
        )


# --- SQL (read-only) ---

# Single-row aggregate snapshot of this database's backends.
_SNAPSHOT_SQL = """
SELECT
  current_setting('max_connections')::int                                   AS max_connections,
  count(*)::int                                                             AS total,
  count(*) FILTER (WHERE state = 'active')::int                             AS active,
  count(*) FILTER (WHERE state = 'idle')::int                               AS idle,
  count(*) FILTER (WHERE state = 'idle in transaction')::int                AS idle_in_txn,
  count(*) FILTER (WHERE wait_event_type = 'Lock')::int                     AS waiting_on_lock,
  COALESCE(ROUND(EXTRACT(EPOCH FROM max(clock_timestamp() - query_start)
                 FILTER (WHERE state = 'active')))::int, 0)                 AS longest_active_secs,
  COALESCE(ROUND(EXTRACT(EPOCH FROM max(clock_timestamp() - xact_start)
                 FILTER (WHERE state = 'idle in transaction')))::int, 0)    AS longest_idle_txn_secs,
  (SELECT count(*)::int FROM pg_locks WHERE locktype = 'advisory')          AS advisory_locks
FROM pg_stat_activity
WHERE datname = current_database()
"""

# Queries currently waiting to ACQUIRE a lock, with the PIDs blocking them.
_BLOCKED_SQL = """
SELECT
  a.pid                                                          AS blocked_pid,
  ROUND(EXTRACT(EPOCH FROM clock_timestamp() - a.query_start))::int AS blocked_secs,
  a.wait_event_type                                              AS wait_event_type,
  a.wait_event                                                   AS wait_event,
  a.application_name                                             AS application_name,
  left(a.query, 200)                                            AS blocked_query,
  pg_blocking_pids(a.pid)                                        AS blocker_pids
FROM pg_stat_activity a
WHERE a.wait_event_type = 'Lock'
  AND a.datname = current_database()
  AND a.pid <> pg_backend_pid()
ORDER BY blocked_secs DESC NULLS LAST
LIMIT :limit
"""

# Long-running ACTIVE queries (executing, not blocked) — the slow-query signal.
_SLOW_ACTIVE_SQL = """
SELECT
  pid                                                           AS pid,
  ROUND(EXTRACT(EPOCH FROM clock_timestamp() - query_start))::int AS active_secs,
  wait_event_type                                               AS wait_event_type,
  wait_event                                                    AS wait_event,
  application_name                                              AS application_name,
  left(query, 200)                                             AS query
FROM pg_stat_activity
WHERE state = 'active'
  AND datname = current_database()
  AND pid <> pg_backend_pid()
  AND query_start IS NOT NULL
  AND clock_timestamp() - query_start > make_interval(secs => :threshold)
ORDER BY active_secs DESC NULLS LAST
LIMIT :limit
"""


class DbContentionMonitor(PeriodicService):
    """Leader-elected periodic sampler of DB lock / slow-query contention.

    Implements ``PeriodicService``: ``BackgroundSupervisor`` handles leadership
    election via ``_CONTENTION_MONITOR_LOCK_KEY`` and the configured cadence.
    Each tick calls ``run_once()`` which takes a read-only snapshot of
    ``pg_stat_activity`` / ``pg_locks`` and logs the result.
    """

    name = "db_contention_monitor"
    leadership = Leadership.LEADER_ONLY
    pod_policy = PodPolicy.SKIP_EPHEMERAL

    def __init__(self, config: DbContentionMonitorConfig) -> None:
        self._config = config
        self.cadence_seconds = float(config.interval_seconds)
        self.lock_key: Optional[Union[int, str]] = _CONTENTION_MONITOR_LOCK_KEY

    async def tick(self, ctx: ServiceContext) -> None:
        """Take one snapshot and log it."""
        await self.run_once()

    async def run_once(self) -> Optional[dict]:
        """Take one snapshot and log it. Returns the snapshot dict (or None).

        Never raises for ordinary query/visibility issues — observability must
        not become its own incident. The outer leader loop still sees
        CancelledError so shutdown is honoured.
        """
        if not self._config.enabled:
            return None
        engine = get_engine()
        if engine is None:
            logger.debug("db_contention_monitor: no DB engine — skipping tick.")
            return None
        try:
            async with managed_transaction(engine) as conn:
                snapshot = await self._collect(conn)
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.warning(
                "db_contention_monitor: snapshot failed (best-effort).",
                exc_info=True,
            )
            return None
        self._emit(snapshot)
        return snapshot

    async def _collect(self, conn: Any) -> dict:
        """Run the read-only queries and assemble a structured snapshot."""
        agg_rows = await DQLQuery(
            _SNAPSHOT_SQL, result_handler=ResultHandler.ALL_DICTS
        ).execute(conn)
        agg = dict(agg_rows[0]) if agg_rows else {}

        blocked = []
        slow = []
        # Only pay for the detail queries when the cheap aggregate says there is
        # something to look at — keeps the steady-state tick to one query.
        if agg.get("waiting_on_lock", 0):
            blocked = await DQLQuery(
                _BLOCKED_SQL, result_handler=ResultHandler.ALL_DICTS
            ).execute(conn, limit=self._config.detail_limit)
        if agg.get("longest_active_secs", 0) >= self._config.slow_query_seconds:
            slow = await DQLQuery(
                _SLOW_ACTIVE_SQL, result_handler=ResultHandler.ALL_DICTS
            ).execute(
                conn,
                threshold=float(self._config.slow_query_seconds),
                limit=self._config.detail_limit,
            )
        return {
            "agg": agg,
            "blocked": [dict(r) for r in (blocked or [])],
            "slow_active": [dict(r) for r in (slow or [])],
        }

    def _emit(self, snapshot: dict) -> None:
        """Log the heartbeat (INFO) and escalate to WARNING on contention."""
        agg = snapshot.get("agg", {})
        blocked = snapshot.get("blocked", [])
        slow = snapshot.get("slow_active", [])

        total = agg.get("total", 0)
        max_conns = agg.get("max_connections", 0) or 0
        waiting = agg.get("waiting_on_lock", 0)
        longest_active = agg.get("longest_active_secs", 0)
        idle_in_txn = agg.get("idle_in_txn", 0)
        longest_idle_txn = agg.get("longest_idle_txn_secs", 0)

        # Advisory locks held by THIS pod (the locks WE create), with held time.
        now = time.monotonic()
        pod_locks = held_advisory_locks()
        pod_locks_desc = ", ".join(
            f"{name}={now - t0:.0f}s" for name, t0 in pod_locks.values()
        ) or "none"

        cfg = self._config
        conn_pressure = (total / max_conns) if max_conns else 0.0
        contended = bool(
            blocked
            or slow
            or waiting
            or longest_active >= cfg.slow_query_seconds
            or conn_pressure >= cfg.conn_pressure_ratio
        )

        summary = (
            "db_contention: conns=%d/%d active=%d idle=%d idle_in_txn=%d "
            "waiting_on_lock=%d longest_active=%ds longest_idle_txn=%ds "
            "advisory_locks(instance)=%d advisory_locks(this_pod)=%d[%s]"
        )
        args = (
            total,
            max_conns,
            agg.get("active", 0),
            agg.get("idle", 0),
            idle_in_txn,
            waiting,
            longest_active,
            longest_idle_txn,
            agg.get("advisory_locks", 0),
            len(pod_locks),
            pod_locks_desc,
        )
        if contended:
            logger.warning(summary, *args)
        else:
            logger.info(summary, *args)

        # The disambiguating detail: blockers prove LOCKING; long active queries
        # with no blockers point at SLOW QUERIES / resource contention instead.
        for r in blocked:
            if (r.get("blocked_secs") or 0) < cfg.lock_wait_seconds:
                continue
            logger.warning(
                "db_contention[LOCK-WAIT]: pid=%s waited=%ss on %s/%s app=%r "
                "blocked_by=%s query=%r",
                r.get("blocked_pid"),
                r.get("blocked_secs"),
                r.get("wait_event_type"),
                r.get("wait_event"),
                r.get("application_name"),
                r.get("blocker_pids"),
                r.get("blocked_query"),
            )
        for r in slow:
            logger.warning(
                "db_contention[SLOW-QUERY]: pid=%s running=%ss wait=%s/%s app=%r "
                "query=%r",
                r.get("pid"),
                r.get("active_secs"),
                r.get("wait_event_type"),
                r.get("wait_event"),
                r.get("application_name"),
                r.get("query"),
            )


def load_db_contention_monitor_config() -> DbContentionMonitorConfig:
    """Build the monitor config from the environment."""
    return DbContentionMonitorConfig.from_env()
