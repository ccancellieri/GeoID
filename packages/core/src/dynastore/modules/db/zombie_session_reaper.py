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

"""Zombie-session reaper (geoid#2924).

Cloud Run kills instances without a clean TCP FIN (SIGKILL after the 10s grace
period, crash, scale-down). The server keeps their sockets alive indefinitely:
``idle_in_transaction_session_timeout`` does not apply to a plain ``idle``
session, and nothing else notices. A session that happened to be holding a
transaction-scoped advisory lock (``pg_try_advisory_xact_lock`` — every
advisory-lock call site in this codebase today, see the audit in the PR body)
keeps that lock held for as long as the zombie session lives, wedging
whichever role the lock arbitrates fleet-wide.

Self-healing, not blind cleanup
--------------------------------
This reaper only terminates a session when it can PROVE the session belongs
to a dead peer. "Proof" is the conjunction of three independent, strict
conditions — failing any one of them leaves the session untouched:

1. **Recognizably ours.** ``application_name`` must match the exact
   ``"{service}:{instance_id}"`` shape every engine in this codebase stamps
   (see ``dynastore.modules.db_config.instance.get_stamped_application_name``).
   A session from ``psql``, another application, or a pre-upgrade process
   that never carried this stamp does not match and is never touched.
2. **Provably idle.** ``state <> 'active'`` and idle
   (``clock_timestamp() - state_change``) past ``idle_threshold_seconds``
   (default 30 minutes — deliberately generous). A session mid-query, or one
   that has been idle for only a few minutes, is left alone regardless of
   what the liveness table says.
3. **Provably dead.** The ``instance_id`` parsed out of ``application_name``
   has no row in ``configs.instance_liveness`` — scoped to that instance's own
   ``service`` (see "Per-service safety valve" below) — renewed within
   ``liveness_stale_after_seconds``.

A fleet-wide (per-service, see below) safety valve backstops condition 3: if
the reaper cannot find *any* fresh row at all for a service among the
candidates, that is a sign the heartbeat mechanism itself is broken for that
service (not that every one of its candidate instances died simultaneously) —
that service's candidates are skipped for the tick with a loud warning.

Disabled by default (``ZombieSessionReaperConfig.enabled = False``); an
operator opts in explicitly via the configs API after reviewing the
instance-liveness table on their environment.

Shadow mode
-----------
Flipping ``enabled`` to True does not, by itself, terminate anything:
``zombie_reaper_shadow_mode`` defaults to True, so the reaper runs its full
detection pipeline — candidate scan, per-service liveness resolution, and the
TOCTOU recheck — exactly as it would to reap a session, but stops short of
calling ``pg_terminate_backend`` and instead logs a ``lock_reaped_shadow``
warning for every session it would have reaped (see ``_reap_session``). This
lets an operator watch the reaper's targeting against real traffic on an
environment before trusting it to actually evict a session. Only once that
log has been reviewed and looks correct should ``zombie_reaper_shadow_mode``
be flipped to False via the configs API.

Known correlated-failure mode: CPU throttling
-----------------------------------------------
Idleness and liveness-staleness are meant to be *independent* evidence, but on
a Cloud Run service running with the default request-scoped CPU allocation
(CPU throttled to ~0 between requests), they are NOT independent: with no
inbound traffic, the instance's asyncio event loop itself stalls, so its
heartbeat loop and its idle DB sessions freeze in lockstep. A genuinely alive
but throttled instance can look exactly like a dead one on both signals at
once. Mitigations, layered (none alone is sufficient):

* ``liveness_stale_after_seconds`` is validated to be **>= idle_threshold_seconds**
  (enforced — see ``ZombieSessionReaperConfig``) so the liveness signal is
  never more trigger-happy than the idle signal it is supposed to corroborate.
* **Operational precondition**: only enable this reaper on services that run
  with ``--no-cpu-throttling`` (CPU always allocated, so the heartbeat loop
  keeps ticking regardless of traffic), OR tune both thresholds well above
  the longest idle-throttle window actually observed for the service. Enabling
  it on a throttle-tolerant, bursty-traffic service without doing either is
  the false-positive trap this reaper must not recreate.
* A cheap TOCTOU re-check (see ``_reap_session``) immediately before
  ``pg_terminate_backend`` re-confirms, in the same transaction, that the
  session is still non-active and still idle past the threshold — catching
  the case where the instance woke back up between the scan and the act.

Per-service safety valve
--------------------------
The "any fresh liveness row" check is scoped to the ``service`` embedded in
each candidate's own ``application_name`` (not fleet-wide): a heartbeat wiring
bug affecting only one service must not let that service's candidates get
100% reaped just because *other*, healthy services still have fresh rows.

Interrupted-operation tracking (geoid#2924 leg 3)
--------------------------------------------------
Every advisory lock in this codebase today is transaction-scoped
(``pg_try_advisory_xact_lock`` / ``pg_advisory_xact_lock``) — Postgres already
releases those the instant the backend's last transaction ends, so a
genuinely ``idle`` (not ``idle in transaction``) zombie should hold none.  If
one still does (or the session is ``idle in transaction`` and never
committed), this reaper logs it loudly, with the raw advisory lock ids, as a
structured ``lock_reaped`` event before terminating the backend — so the loss
is visible, not silent. There is no reverse mapping from a lock id (a
one-way hash — see ``modules.tasks.durable.locks``) back to the operation it
guarded; recovering that mapping is filed as a follow-up in the PR body, not
solved here. Every operation this codebase currently guards with an advisory
lock is either itself idempotent and safely re-attempted by its own regular
cadence (leader-elected background loops), or backed by the ``configs.leader_lease``
TTL (which already self-expires independent of this reaper) — so today,
"log loudly and free the lock" is sufficient; nothing is silently dropped.
"""

from __future__ import annotations

import logging
import re
from typing import Any, ClassVar, Dict, List, Optional, Tuple, Union

from pydantic import Field, model_validator

from dynastore.models.mutability import Mutable
from dynastore.models.plugin_config import PluginConfig
from dynastore.modules.db_config.query_executor import (
    DQLQuery,
    ResultHandler,
    background_managed_transaction,
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
# (SoftDeleteReaper 0x5D3A7E1F_C2B84961, MaintenanceSupervisor
# 0x4D41494E_54454E41, LifecycleReaper 0x4C494643_52454150, DbContentionMonitor
# 0x4C4F434B_4D4F4E49). ASCII "ZOMBIEP1".
_ZOMBIE_REAPER_ADVISORY_LOCK_KEY = 0x5A4F4D42_49455031

# Matches exactly the shape get_stamped_application_name() produces:
# "{service}:{32-char lowercase hex instance id}". Anything else (empty,
# unstamped client, pre-upgrade process) does not match and is left alone.
# Two groups: service, instance_id — the per-service safety valve (finding 3)
# needs the service half, not just the instance id.
_STAMPED_APP_NAME_RE = re.compile(r"^([A-Za-z0-9_.-]+):([0-9a-f]{32})$")


class ZombieSessionReaperConfig(PluginConfig):
    """Configuration for the zombie-session reaper background loop."""

    _address: ClassVar[Tuple[str, ...]] = ("platform", "modules", "db")

    enabled: Mutable[bool] = Field(
        default=False,
        description=(
            "Master switch for the zombie-session reaper. Defaults to False: "
            "the reaper calls pg_terminate_backend() on sessions it identifies "
            "as belonging to a dead instance, so it is opt-in. Read live on "
            "every tick (and by the instance-liveness heartbeat, which only "
            "runs while this is True), so flipping it via the configs API "
            "takes effect immediately — no pod restart needed. Only enable on "
            "a service running with --no-cpu-throttling, or after confirming "
            "liveness_stale_after_seconds comfortably exceeds the service's "
            "longest observed idle-CPU-throttle window (see the module "
            "docstring's correlated-failure-mode section) — otherwise a "
            "throttled-but-alive instance can be misread as dead."
        ),
    )

    idle_threshold_seconds: Mutable[int] = Field(
        default=1800,  # 30 minutes
        ge=60,
        description=(
            "Seconds a session must have been idle (state <> 'active') before "
            "the reaper will even consider it. Deliberately generous — this is "
            "not a query timeout, it is the minimum age before checking whether "
            "the owning instance is provably dead. Default: 1800 (30 minutes). "
            "Read live on every tick."
        ),
    )

    liveness_stale_after_seconds: Mutable[int] = Field(
        default=1800,  # 30 minutes — floor is idle_threshold_seconds, see validator
        ge=60,
        description=(
            "An instance_liveness row older than this (or absent entirely) "
            "counts as proof the owning instance is dead. MUST be >= "
            "idle_threshold_seconds (enforced by a validator): on a "
            "CPU-throttled Cloud Run instance, an idle session and a stalled "
            "heartbeat freeze together, so a shorter staleness window would "
            "make the liveness check trigger-happier than the idle check it "
            "is supposed to corroborate, reintroducing the false-positive "
            "this reaper exists to avoid. Read live on every tick."
        ),
    )

    reaper_interval_seconds: Mutable[float] = Field(
        default=600.0,  # 10 minutes
        ge=60.0,
        description=(
            "How often (seconds) the reaper scans pg_stat_activity for zombie "
            "sessions. Default: 600 (10 minutes) — conservative by design; this "
            "is cleanup of a slow-forming problem (hours), not a hot path. "
            "Changes take effect on the next pod restart (this one field sets "
            "the background-loop scheduling cadence itself, unlike the other "
            "fields above which this reaper re-reads live every tick)."
        ),
    )

    batch_size: Mutable[int] = Field(
        default=20,
        ge=1,
        le=200,
        description=(
            "Maximum candidate sessions scanned/reaped per tick. Bounds the "
            "amount of pg_terminate_backend() activity and logging in a single "
            "pass when many zombies have accumulated at once (e.g. after a "
            "deploy storm). Read live on every tick."
        ),
    )

    zombie_reaper_shadow_mode: Mutable[bool] = Field(
        default=True,
        description=(
            "When True (the default), the reaper runs its full detection "
            "pipeline — candidate scan, per-service liveness resolution, TOCTOU "
            "recheck — exactly as it would to reap a session, but never calls "
            "pg_terminate_backend(): each session it would have reaped is "
            "instead logged as a 'lock_reaped_shadow' warning. This lets an "
            "operator turning enabled=True on for the first time on an "
            "environment observe what the reaper would do before trusting it "
            "to actually evict anything. Set to False once the shadow log has "
            "been reviewed and its targeting looks correct. Read live on "
            "every tick."
        ),
    )

    @model_validator(mode="after")
    def _stale_after_not_shorter_than_idle_threshold(self) -> "ZombieSessionReaperConfig":
        if self.liveness_stale_after_seconds < self.idle_threshold_seconds:
            raise ValueError(
                "ZombieSessionReaperConfig: liveness_stale_after_seconds "
                f"({self.liveness_stale_after_seconds}) must be >= "
                f"idle_threshold_seconds ({self.idle_threshold_seconds}) — a "
                "shorter staleness window lets a CPU-throttled-but-alive "
                "instance (whose heartbeat and idle sessions both freeze "
                "together) look dead before its own idle threshold would even "
                "fire, defeating the conservative-by-design intent of "
                "requiring both signals to agree."
            )
        return self


# ---------------------------------------------------------------------------
# Queries (all read-only except the final targeted pg_terminate_backend)
# ---------------------------------------------------------------------------

# Candidates: sessions recognizably ours (application_name matches our
# stamped shape) that are non-active and have been idle past the threshold.
_CANDIDATES_SQL = """
SELECT
    pid,
    application_name,
    state,
    EXTRACT(EPOCH FROM (clock_timestamp() - state_change))::bigint AS idle_secs,
    left(query, 200) AS last_query
FROM pg_stat_activity
WHERE datname = current_database()
  AND pid <> pg_backend_pid()
  AND state <> 'active'
  AND state_change IS NOT NULL
  AND application_name ~ '^[A-Za-z0-9_.-]+:[0-9a-f]{32}$'
  AND clock_timestamp() - state_change > make_interval(secs => :idle_threshold_seconds)
ORDER BY idle_secs DESC
LIMIT :limit
"""

# Per-service safety valve: does THIS service have ANY fresh liveness row at
# all? Zero here means "distrust the liveness signal for this service this
# tick" — not "every candidate instance of this service died" — so a
# heartbeat-wiring bug scoped to one service can't get its instances 100%
# reaped just because other services still look healthy.
_ANY_FRESH_LIVENESS_FOR_SERVICE_SQL = """
SELECT count(*)
FROM configs.instance_liveness
WHERE service = :service
  AND renewed_at > now() - make_interval(secs => :stale_after_seconds)
"""

# Which of the candidate instance_ids (all belonging to the same service) are
# FRESH (i.e. provably alive)? Any instance_id absent from this result —
# whether it has no row at all or only a stale one — is treated as dead.
_FRESH_INSTANCE_IDS_FOR_SERVICE_SQL = """
SELECT instance_id
FROM configs.instance_liveness
WHERE service = :service
  AND instance_id = ANY(:instance_ids)
  AND renewed_at > now() - make_interval(secs => :stale_after_seconds)
"""

# Advisory locks (transaction-scoped or otherwise) currently granted to a pid.
# Reconstructs the original 64-bit key from the (classid, objid) pair Postgres
# splits it into for the single-bigint advisory-lock form.
_PID_ADVISORY_LOCKS_SQL = """
SELECT ((classid::bigint << 32) | (objid::bigint & 4294967295)) AS lock_id
FROM pg_locks
WHERE pid = :pid AND locktype = 'advisory' AND granted
"""

# TOCTOU re-check (correlated-failure mitigation): re-confirm, in the SAME
# transaction as the terminate below, that the session is still non-active
# and still idle past the threshold. A throttled instance that woke back up
# between the scan and the act — or a pid Postgres has already recycled for
# an unrelated, freshly-connected backend — fails this check and is skipped.
_RECHECK_STILL_IDLE_SQL = """
SELECT 1
FROM pg_stat_activity
WHERE pid = :pid
  AND state <> 'active'
  AND state_change IS NOT NULL
  AND clock_timestamp() - state_change > make_interval(secs => :idle_threshold_seconds)
"""

_TERMINATE_SQL = "SELECT pg_terminate_backend(:pid) AS terminated"


class ZombieSessionReaper(PeriodicService):
    """Periodic reaper that terminates DB sessions proven to belong to dead instances.

    Implements ``PeriodicService``: ``BackgroundSupervisor`` handles leadership
    election via ``_ZOMBIE_REAPER_ADVISORY_LOCK_KEY`` and the configured
    cadence — exactly one pod scans and reaps per tick, avoiding duplicate
    (harmless but noisy) ``pg_terminate_backend`` calls from every replica.

    ``self._config`` is reloaded live at the top of every ``run_once()`` (see
    :func:`load_zombie_session_reaper_config`) rather than captured once at
    registration, so ``enabled`` and every other tunable except
    ``reaper_interval_seconds`` (which drives the loop's own scheduling — see
    its field description) respond to a live configs-API PATCH without a pod
    restart. The value passed to ``__init__`` only seeds the scheduling
    cadence and the pre-first-tick default.
    """

    name = "zombie_session_reaper"
    leadership = Leadership.LEADER_ONLY
    pod_policy = PodPolicy.SKIP_EPHEMERAL

    def __init__(self, config: ZombieSessionReaperConfig) -> None:
        self._config = config
        self.cadence_seconds = config.reaper_interval_seconds
        self.lock_key: Optional[Union[int, str]] = _ZOMBIE_REAPER_ADVISORY_LOCK_KEY

    async def tick(self, ctx: ServiceContext) -> None:
        await self.run_once()

    async def run_once(self) -> None:
        """Perform one full scan-and-reap pass.

        Safe to call directly in tests; raises on unexpected errors so the
        outer loop can log and retry.
        """
        self._config = await load_zombie_session_reaper_config()
        if not self._config.enabled:
            logger.debug("zombie_session_reaper: disabled via config — skipping scan.")
            return

        engine = get_engine()
        if engine is None:
            logger.warning("zombie_session_reaper: no DB engine — skipping scan.")
            return

        candidates = await self._find_candidates(engine)
        if not candidates:
            logger.debug("zombie_session_reaper: no idle stamped sessions this tick.")
            return

        parsed = [
            (row, m.group(1), m.group(2))  # (row, service, instance_id)
            for row in candidates
            if (m := _STAMPED_APP_NAME_RE.match(row.get("application_name") or ""))
        ]
        if not parsed:
            # Matched the SQL regex but not the Python one, or vice versa —
            # should not happen since both encode the same shape, but never
            # act on a session we can't parse an identity out of.
            return

        by_service: Dict[str, List[Tuple[dict, str]]] = {}
        for row, service, instance_id in parsed:
            by_service.setdefault(service, []).append((row, instance_id))

        for service, rows in by_service.items():
            instance_ids = sorted({instance_id for _row, instance_id in rows})
            dead_ids = await self._resolve_dead_instances(engine, service, instance_ids)
            if dead_ids is None:
                # Per-service safety valve tripped — see _resolve_dead_instances.
                continue
            if not dead_ids:
                logger.debug(
                    "zombie_session_reaper: service=%r %d candidate session(s), "
                    "none provably dead.",
                    service, len(rows),
                )
                continue
            for row, instance_id in rows:
                if instance_id not in dead_ids:
                    continue
                await self._reap_session(
                    engine,
                    row,
                    instance_id,
                    self._config.idle_threshold_seconds,
                    self._config.zombie_reaper_shadow_mode,
                )

    async def _find_candidates(self, engine: Any) -> List[dict]:
        try:
            async with background_managed_transaction(engine) as conn:
                rows = await DQLQuery(
                    _CANDIDATES_SQL, result_handler=ResultHandler.ALL_DICTS
                ).execute(
                    conn,
                    idle_threshold_seconds=self._config.idle_threshold_seconds,
                    limit=self._config.batch_size,
                )
        except Exception:
            logger.warning(
                "zombie_session_reaper: candidate scan failed (best-effort).",
                exc_info=True,
            )
            return []
        return [dict(r) for r in (rows or [])]

    async def _resolve_dead_instances(
        self, engine: Any, service: str, instance_ids: List[str]
    ) -> Optional[set]:
        """Return the subset of *instance_ids* (all belonging to *service*)
        provably dead, or ``None`` if the per-service safety valve tripped
        (liveness signal not trustworthy for this service this tick — caller
        must not reap any of its candidates)."""
        stale_after = self._config.liveness_stale_after_seconds
        try:
            async with background_managed_transaction(engine) as conn:
                any_fresh = await DQLQuery(
                    _ANY_FRESH_LIVENESS_FOR_SERVICE_SQL, result_handler=ResultHandler.SCALAR
                ).execute(conn, service=service, stale_after_seconds=stale_after)
                if not any_fresh:
                    logger.warning(
                        "zombie_session_reaper: configs.instance_liveness has no "
                        "fresh rows for service=%r (stale_after=%ds) — distrusting "
                        "the liveness signal for this service this tick and "
                        "skipping its %d candidate(s) rather than treating them "
                        "all as dead.",
                        service, stale_after, len(instance_ids),
                    )
                    return None

                fresh_rows = await DQLQuery(
                    _FRESH_INSTANCE_IDS_FOR_SERVICE_SQL, result_handler=ResultHandler.ALL_DICTS
                ).execute(
                    conn, service=service, instance_ids=instance_ids,
                    stale_after_seconds=stale_after,
                )
        except Exception:
            logger.warning(
                "zombie_session_reaper: liveness lookup failed for service=%r "
                "(best-effort) — skipping its reap pass.",
                service, exc_info=True,
            )
            return None

        fresh_ids = {r["instance_id"] for r in (fresh_rows or [])}
        return set(instance_ids) - fresh_ids

    async def _reap_session(
        self,
        engine: Any,
        row: dict,
        instance_id: str,
        idle_threshold_seconds: int,
        shadow_mode: bool,
    ) -> None:
        pid = row.get("pid")
        try:
            async with background_managed_transaction(engine) as conn:
                lock_rows = await DQLQuery(
                    _PID_ADVISORY_LOCKS_SQL, result_handler=ResultHandler.ALL_DICTS
                ).execute(conn, pid=pid)
        except Exception:
            logger.warning(
                "zombie_session_reaper: advisory-lock lookup failed for pid %s "
                "(best-effort; proceeding without lock detail).",
                pid, exc_info=True,
            )
            lock_rows = []
        lock_ids = [r["lock_id"] for r in (lock_rows or [])]

        if shadow_mode:
            # Shadow mode runs the identical TOCTOU recheck a real reap would,
            # but never terminates anything — only a candidate that survives
            # the recheck (i.e. one the real path would actually reap) gets
            # the distinct lock_reaped_shadow warning below.
            try:
                async with background_managed_transaction(engine) as conn:
                    still_idle = await DQLQuery(
                        _RECHECK_STILL_IDLE_SQL, result_handler=ResultHandler.SCALAR
                    ).execute(conn, pid=pid, idle_threshold_seconds=idle_threshold_seconds)
            except Exception:
                logger.warning(
                    "zombie_session_reaper: TOCTOU recheck failed for pid=%s "
                    "instance_id=%s (best-effort; shadow candidate not logged).",
                    pid, instance_id, exc_info=True,
                )
                return
            if not still_idle:
                logger.info(
                    "zombie_session_reaper: pid=%s instance_id=%s no longer "
                    "idle past threshold at reap time (recheck failed) — "
                    "skipping (shadow mode).",
                    pid, instance_id,
                )
                return
            logger.warning(
                "lock_reaped_shadow pid=%s service=%s instance_id=%s "
                "idle_secs=%s state=%r lock_ids=%s last_query=%r",
                pid, row.get("application_name"), instance_id,
                row.get("idle_secs"), row.get("state"), lock_ids, row.get("last_query"),
            )
            return

        # geoid#2924 leg 3: loud, structured record of what this reap
        # interrupted, BEFORE the terminate — so the signal survives even if
        # the terminate itself fails (or the TOCTOU recheck below skips it).
        # No reverse mapping from lock_id to the operation it guarded exists
        # today (see module docstring); the raw ids are the durable record
        # until that follow-up lands.
        logger.warning(
            "lock_reaped service=%s instance_id=%s pid=%s idle_secs=%s "
            "state=%r lock_ids=%s last_query=%r",
            row.get("application_name"), instance_id, pid,
            row.get("idle_secs"), row.get("state"), lock_ids, row.get("last_query"),
        )

        try:
            async with background_managed_transaction(engine) as conn:
                # TOCTOU recheck + terminate in the SAME transaction: a
                # throttled-but-alive instance that woke up between the scan
                # and here (or a pid Postgres already recycled) must not be
                # terminated — see the module docstring's correlated-failure
                # section.
                still_idle = await DQLQuery(
                    _RECHECK_STILL_IDLE_SQL, result_handler=ResultHandler.SCALAR
                ).execute(conn, pid=pid, idle_threshold_seconds=idle_threshold_seconds)
                if not still_idle:
                    logger.info(
                        "zombie_session_reaper: pid=%s instance_id=%s no longer "
                        "idle past threshold at reap time (recheck failed) — "
                        "skipping.",
                        pid, instance_id,
                    )
                    return
                terminated = await DQLQuery(
                    _TERMINATE_SQL, result_handler=ResultHandler.SCALAR
                ).execute(conn, pid=pid)
        except Exception:
            logger.exception(
                "zombie_session_reaper: pg_terminate_backend(%s) failed for "
                "instance %s — will retry next tick.",
                pid, instance_id,
            )
            return

        logger.warning(
            "zombie_session_reaper: terminated pid=%s instance_id=%s "
            "idle_secs=%s lock_ids=%s terminated=%s",
            pid, instance_id, row.get("idle_secs"), lock_ids, terminated,
        )


async def load_zombie_session_reaper_config() -> ZombieSessionReaperConfig:
    """Load ``ZombieSessionReaperConfig`` from the platform config store.

    Falls back to the default instance if the store is unavailable or the
    config has not been set.
    """
    try:
        from dynastore.models.protocols.platform_configs import PlatformConfigsProtocol
        from dynastore.tools.discovery import get_protocol

        config_mgr = get_protocol(PlatformConfigsProtocol)
        if config_mgr is not None:
            cfg = await config_mgr.get_config(ZombieSessionReaperConfig)
            if isinstance(cfg, ZombieSessionReaperConfig):
                return cfg
    except Exception as exc:
        logger.warning(
            "zombie_session_reaper: failed to load ZombieSessionReaperConfig "
            "(%s) — using defaults.", exc,
        )
    return ZombieSessionReaperConfig()
