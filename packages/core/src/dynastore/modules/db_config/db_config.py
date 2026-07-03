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
import os
from typing import Any, Mapping, Optional

import dynastore.tools.class_tools as class_tools
from dynastore.modules.db_config.instance import load_db_config

logger = logging.getLogger(__name__)

# DB connection / pool tunables resolve in the order:
#   valid env var → db_config.json → code default
# The file (loaded once at import) is the leak-proof deployment surface: a JSON
# *value* is never shell-substituted, so a missing key can never arrive as a
# literal ``${...}`` placeholder the way a templated env var can (#1581). An
# explicitly-set, valid env var still wins (dev / compose / deliberate
# override); the file fills the gap a deploy would otherwise template; the code
# default is the last resort. See ``instance.load_db_config``.
_FILE_VALUES: Mapping[str, Any] = load_db_config()

# Smallest persistent base (``pool_size``) a pool may hold. The review-env
# outage (dynastore #320) ran with ``DB_POOL_MIN_SIZE=2`` and timed out under
# concurrent load ("QueuePool limit of size 2 overflow 3 reached, connection
# timed out, timeout 60.00"). That failure was a *total-capacity* starvation —
# size 2 + overflow 3 = 5 connections could not serve the concurrency — NOT a
# shortage of persistent base connections. Concurrency is served by the total
# (``pool_size + max_overflow``), enforced by ``SAFE_POOL_TOTAL_FLOOR`` below;
# ``pool_size`` only controls how many connections stay warm between bursts.
#
# Because the base is held for the lifetime of every worker, ``pool_size ×
# GUNICORN_WORKERS × MIN_SCALE`` is the persistent connection footprint a
# deployment pins on the shared Postgres even at idle (dynastore #392). Forcing
# a large base onto light services (auth/tools) multiplied that idle footprint
# for no concurrency benefit. So we trust the operator's small ``pool_min_size``
# and floor it only at 1 — a pool needs at least one connection; everything
# above that is a deployment choice. Burst safety is the total floor's job.
#
# This is also the idle-cost lever (#2333): keep ``pool_min_size`` near this
# floor (1-2) and size ``pool_max_size`` for burst instead. SQLAlchemy's
# QueuePool already grows and shrinks around that small base on its own —
# overflow connections (anything above ``pool_size``) are opened on demand
# under load and CLOSED on check-in once the base is full again, rather than
# kept warm. So a small base + generous ``max_overflow`` is not just cheaper
# at idle, it is the mechanism that returns the pool to its cheap baseline
# automatically once load clears — no separate "shrink" actuator needed. The
# instance-count floor (``modules/scaling``) is the other half of that
# picture and must ratchet down just as promptly — see
# ``ScalingPolicyConfig.scale_in_cooldown_seconds`` / ``scale_in_step``.
SAFE_POOL_MIN_FLOOR: int = 1

# Smallest total pool capacity (``pool_size + max_overflow``) we consider safe
# under concurrent load. This is the real #320 invariant: regardless of how
# small the base is, an engine must be able to open this many connections at
# once or concurrent checkouts queue until they hit the QueuePool timeout. The
# dev auth/tools probe-timeout outage was the floor-collapse corner of this —
# base clamped to 5 against ``DB_POOL_MAX_SIZE=3`` left total 5 / overflow 0, a
# rigid pool that deadlocked startup consumers (engine-snapshot PG engine +
# event shards) needing more than the base at once. A total floor of 10 gives
# every pool real burst headroom; overflow connections are transient (opened on
# demand, returned to the pool), so the steady-state footprint stays the base.
SAFE_POOL_TOTAL_FLOOR: int = 10

# Smallest burst headroom (``max_overflow`` = max - base) we consider safe, so a
# pool whose base happens to sit near its max still keeps room to grow. Belt to
# the total floor's braces: ``SAFE_POOL_TOTAL_FLOOR`` guarantees the absolute
# ceiling, this guarantees the *gap* above whatever base the operator chose.
SAFE_POOL_MIN_OVERFLOW: int = 5


def _looks_unsubstituted(value: str) -> bool:
    """A value still carrying a ``${...}`` fragment is an unsubstituted deploy
    placeholder (#1581), not a real config value — never usable."""
    return "${" in value


def _cfg_int(
    name: str, default: int, *, file_values: Optional[Mapping[str, Any]] = None
) -> int:
    """Resolve an int tunable: valid env var → ``db_config.json`` → default.

    The failure mode behind #1581: a deploy templates the var through (e.g.
    ``iac.yml``) but leaves it undefined, so the container receives the literal
    ``${DB_POOL_RECYCLE}`` (or an empty string) rather than a number. A bare
    ``int()`` over ``os.getenv(name, "1800")`` only uses the default when the
    var is *unset* — a non-numeric literal makes ``int()`` raise ``ValueError`` at
    import, the gunicorn worker dies, and the Cloud Run startup probe fails the
    whole revision rollout.

    Each source is tried in order; an empty value is skipped silently (the
    default is intended), a non-numeric value is skipped with a WARN (so a
    mis-templated ``${...}`` can never crash startup and the operator sees it),
    and the next source is consulted. ``file_values`` defaults to the
    file loaded at import; tests pass an explicit mapping.
    """
    fv = _FILE_VALUES if file_values is None else file_values
    rejected = False
    for source, raw in (("env", os.getenv(name)), ("db_config.json", fv.get(name))):
        if raw is None:
            continue
        text = str(raw).strip()
        if text == "":
            continue
        try:
            return int(text)
        except (ValueError, TypeError):
            rejected = True
            logger.warning(
                "%s=%r from %s is not a valid integer (commonly an "
                "unsubstituted ${...} placeholder); ignoring this source.",
                name, raw, source,
            )
    if rejected:
        logger.warning(
            "%s: no usable value found; falling back to the default %d.",
            name, default,
        )
    return default


def _cfg_str(
    name: str, default: str, *, file_values: Optional[Mapping[str, Any]] = None
) -> str:
    """Resolve a string tunable: valid env var → ``db_config.json`` → default.

    The string analogue of :func:`_cfg_int`. ``int()`` already rejects a
    ``${...}`` placeholder; for free-form strings (``DATABASE_URL``,
    ``DB_LOCK_TIMEOUT``, …) an unsubstituted placeholder is a *valid* string
    that would silently mis-configure the connection (a broken DSN, a bad PG
    ``server_settings`` value), so it is detected and skipped explicitly. An
    empty value is skipped silently; the next source is consulted.
    """
    fv = _FILE_VALUES if file_values is None else file_values
    rejected = False
    for source, raw in (("env", os.getenv(name)), ("db_config.json", fv.get(name))):
        if raw is None:
            continue
        text = str(raw).strip()
        if text == "":
            continue
        if _looks_unsubstituted(text):
            rejected = True
            logger.warning(
                "%s=%r from %s looks like an unsubstituted ${...} placeholder; "
                "ignoring this source.",
                name, raw, source,
            )
            continue
        return text
    if rejected:
        logger.warning(
            "%s: no usable value found; falling back to the default %r.",
            name, default,
        )
    return default


_HARDCODED_DEV_DATABASE_URL = "postgresql://testuser:testpassword@db:5432/gis_dev"


def _resolve_database_url() -> str:
    """Resolve DATABASE_URL, gating the hardcoded dev fallback.

    Resolution order: valid env var → ``db_config.json`` → hardcoded dev
    default (gated behind ``DYNASTORE_ALLOW_DEV_SECRET=1``).

    An explicitly set ``DATABASE_URL`` env var or a value in ``db_config.json``
    is always accepted — only the hardcoded fallback is restricted.
    """
    url = _cfg_str("DATABASE_URL", "")
    if url:
        return url
    if os.getenv("DYNASTORE_ALLOW_DEV_SECRET") != "1":
        raise RuntimeError(
            "DATABASE_URL is not set and no db_config.json provides it. "
            "Set DATABASE_URL (or provide it via db_config.json) before "
            "starting in a non-development environment. "
            "To allow the hardcoded dev default, set DYNASTORE_ALLOW_DEV_SECRET=1."
        )
    logger.warning(
        "DATABASE_URL not configured; falling back to the hardcoded development "
        "database URL because DYNASTORE_ALLOW_DEV_SECRET=1. "
        "NEVER use this default in production."
    )
    return _HARDCODED_DEV_DATABASE_URL


class _LazyDatabaseUrl:
    """Descriptor deferring DATABASE_URL resolution to first access.

    Resolving at class-definition time would make ``import dynastore``
    raise on hosts with no DATABASE_URL and no dev flag (unit-test runs,
    CLI tooling). Deferring keeps the fail-fast guarantee — the
    RuntimeError fires at engine creation / first real use — without
    poisoning imports. Works for both ``DBConfig.database_url`` (class
    access) and instance access; the resolved value is cached.
    """

    _resolved: str | None = None

    def __get__(self, obj: object | None, objtype: type | None = None) -> str:
        if self._resolved is None:
            self._resolved = _resolve_database_url()
        return self._resolved


class DBConfig:
    database_url = _LazyDatabaseUrl()
    pool_min_size: int = _cfg_int("DB_POOL_MIN_SIZE", 5)
    pool_max_size: int = _cfg_int("DB_POOL_MAX_SIZE", 100)
    pool_max_queries: int = _cfg_int("DB_POOL_MAX_QUERIES", 50000)
    pool_command_timeout: int = _cfg_int("DB_POOL_COMMAND_TIMEOUT", 60)
    connect_timeout: int = _cfg_int("DB_CONNECT_TIMEOUT", 30)
    # SQLAlchemy retires a pooled connection once it reaches this age (#729).
    # On serverless deployments the VPC-egress path silently drops a TCP
    # connection that has been idle past its window; once dropped, the next
    # checkout's pool_pre_ping fails and the replacement handshake costs 8-22s.
    # Recycling proactively — while the path is still warm — keeps reconnects
    # sub-second. Keep this BELOW the deployment's idle-drop window (set a
    # lower DB_POOL_RECYCLE per-environment where idle periods are common).
    pool_recycle: int = _cfg_int("DB_POOL_RECYCLE", 1800)
    # TCP keepalive tunables (#655). The egress path silently drops the
    # established-connection mapping after an idle window; without keepalive
    # probes the pool hands out a dead-at-the-wire socket whose replacement
    # handshake costs 8-22s. As of #710 these values drive BOTH the
    # server-side GUC probes (passed via asyncpg server_settings) AND the
    # client-side SO_KEEPALIVE socket options that db_service arms on every
    # connection — asyncpg exposes no libpq client keepalive params, so the
    # client socket had to be armed directly. pool_recycle remains a backstop.
    # Keep idle BELOW the deployment's idle-drop window (lower it per-env where
    # idle periods are common) so a probe refreshes the mapping in time.
    tcp_keepalives_idle: int = _cfg_int("DB_TCP_KEEPALIVES_IDLE", 300)
    tcp_keepalives_interval: int = _cfg_int("DB_TCP_KEEPALIVES_INTERVAL", 30)
    tcp_keepalives_count: int = _cfg_int("DB_TCP_KEEPALIVES_COUNT", 5)
    # TCP_USER_TIMEOUT (ms) — caps how long transmitted data may stay
    # unacknowledged before the kernel tears the connection down (#710).
    # Armed on the client socket alongside SO_KEEPALIVE in db_service. This is
    # what bounds a pool_pre_ping probe that lands on a silently-dropped
    # socket: without it the probe blocks up to connect_timeout; with it the
    # dead connection is detected and replaced within this window. Healthy
    # queries are unaffected — every ACK resets the timer. Linux-only; ignored
    # where the socket option is unavailable (e.g. macOS dev).
    tcp_user_timeout_ms: int = _cfg_int("DB_TCP_USER_TIMEOUT_MS", 20000)
    # Lock-safety GUCs — applied as server_settings on EVERY connection (see
    # db_service). They make it impossible for one statement, or a leaked /
    # interrupted transaction, to freeze the whole application:
    #   * lock_timeout — the longest ANY statement will wait to acquire a
    #     lock. A pending lock request (e.g. an ALTER's AccessExclusive that
    #     queues ahead of every reader) can never convoy the application for
    #     longer than this window; on expiry the statement fails with 55P03
    #     (retried by retry_on_lock_conflict) instead of blocking forever.
    #   * idle_in_transaction_session_timeout — PostgreSQL terminates a
    #     backend that holds a transaction open while idle past this window,
    #     releasing its locks SERVER-side. This is the only guarantee that
    #     holds when a client is interrupted / OOM-killed mid-transaction and
    #     never runs ROLLBACK — the exact failure mode that pinned
    #     catalog.catalogs behind an idle-in-transaction reader while an
    #     ALTER waited on it. A DDL therefore can never leave a lock open.
    #
    #     Under burst load, connections can accumulate in idle_in_txn state
    #     if transactions span external calls (GCS/ES/Cloud Run API). A 10s
    #     default surfaces issues quickly in dev/staging while allowing
    #     production to tune higher via the configs API or env var.
    #   * statement_timeout — bounds total EXECUTION time of any single
    #     statement (distinct from lock_timeout, which only bounds the WAIT to
    #     acquire a lock). Disabled by default ("0") to preserve historical
    #     behaviour: a genuinely slow query currently runs unbounded until the
    #     asyncpg ``command_timeout`` (``pool_command_timeout``, 60s) cancels it
    #     CLIENT-side, which leaves no server log line — the query just vanishes
    #     and the request returns a 504 with no attributable cause. Setting this
    #     just UNDER pool_command_timeout (e.g. ``DB_STATEMENT_TIMEOUT=55s``)
    #     converts that silent wall into a logged server-side 57014 ("canceling
    #     statement due to statement timeout") naming the offending query — the
    #     difference between "we can't tell slow-query from lock-wait" and a
    #     self-diagnosing failure. Legitimate long operations (large ingests,
    #     DDL, maintenance jobs) already scope their own budget via SET LOCAL
    #     statement_timeout, which overrides this session default within their
    #     transaction, so a session-wide value is safe to enable.
    #
    # These are genuine infra dimensioning values. Configure them via env var
    # (or db_config.json) before startup; they are resolved once at import time.
    lock_timeout: str = _cfg_str("DB_LOCK_TIMEOUT", "5s")
    statement_timeout: str = _cfg_str("DB_STATEMENT_TIMEOUT", "0")
    idle_in_transaction_session_timeout: str = _cfg_str(
        "DB_IDLE_IN_TRANSACTION_TIMEOUT", "10s"
    )
    # Task-side counterpart of idle_in_transaction_session_timeout above,
    # applied only to the ad-hoc engines task/job code builds for itself
    # (see task_engine_connect_args in db_timeout_config.py). Those engines
    # routinely interleave a PG transaction with slow secondary-store I/O
    # (ES bulk writes during reindex/drain); the 10s serving-tier budget
    # kills them mid-write (#2837 regression). Still bounded — well short of
    # unbounded — so a genuinely frozen task can't hold locks forever, which
    # was the #2832 goal.
    task_idle_in_transaction_session_timeout: str = _cfg_str(
        "DB_TASK_IDLE_IN_TRANSACTION_TIMEOUT", "300s"
    )
    # How long SQLAlchemy's QueuePool will wait for a free connection before
    # raising ``sqlalchemy.exc.TimeoutError`` (fail-fast, not wedge).
    #
    # Background (#1894): prior code fed ``pool_command_timeout`` (60s) to
    # ``create_async_engine(pool_timeout=…)``, so the pool-acquire wait was
    # silently bound by the *statement* timeout budget instead of its own
    # tunable.  A saturated pool would block every waiting coroutine for up
    # to 60s before failing, wedging Cloud Run worker threads under provisioning
    # load spikes (incident #1895).
    #
    # A 30-second default matches ``connect_timeout`` (the companion "how long
    # to wait for the TCP handshake") and is fast enough to surface saturation
    # within the Cloud Run request deadline window, while generous enough not
    # to trip on a brief burst.  Operators can tighten it per-environment:
    #   DB_POOL_ACQUIRE_TIMEOUT=10  # review / staging — fail very fast
    #   DB_POOL_ACQUIRE_TIMEOUT=30  # production default
    pool_acquire_timeout: int = _cfg_int("DB_POOL_ACQUIRE_TIMEOUT", 30)
    # Ceiling (seconds) the shared SERVING engine's effective statement_timeout
    # is clamped to, regardless of what DB_STATEMENT_TIMEOUT resolves to
    # (see clamp_serving_statement_timeout in db_timeout_config.py). Exists
    # because DB_STATEMENT_TIMEOUT is disabled ("0") in dev and set to "90s"
    # in production -- both above the 60s load-balancer/Cloud Run deadline, so
    # a stuck interactive query holds its connection to that ceiling instead
    # of being cancelled and reclaimed server-side (#2898). 55s sits below the
    # 60s LB timeout and at/under pool_command_timeout. Task-side engines are
    # unaffected -- they never apply statement_timeout at all.
    serving_statement_timeout_ceiling_seconds: int = _cfg_int(
        "DB_SERVING_STATEMENT_TIMEOUT_CEILING", 55
    )

    def validate_pool_sizing(self) -> None:
        """Make a dangerously-small pool LOUD and SAFE at startup.

        The review-env outage (dynastore #320) timed out under concurrent load
        ("QueuePool limit of size 2 overflow 3 reached, connection timed out,
        timeout 60.00"), cascading into engine-snapshot retry exhaustion and
        100% memory / container restarts. The fix protects what actually
        starved: the *total capacity* a pool can open at once (``pool_size +
        max_overflow``), floored at ``SAFE_POOL_TOTAL_FLOOR``. The persistent
        base (``pool_size``) is left to the operator down to a floor of 1 —
        forcing a large base only inflates the idle connection footprint a
        deployment pins on the shared Postgres (``base × workers × MIN_SCALE``,
        dynastore #392) without adding burst capacity.

        This guard emits a clear WARNING naming the offending env var and the
        risk, then clamps up to the safe floors so a misconfigured tiny value
        can never silently strangle the service. Called once from the module
        lifespan after the config is built.
        """
        # A pool needs at least one connection; a 0/negative base is a misconfig
        # (not an intentional "small" value). Everything above 1 is a deployment
        # choice — burst safety is the total floor's job, not the base's.
        if self.pool_min_size < SAFE_POOL_MIN_FLOOR:
            logger.warning(
                "DB_POOL_MIN_SIZE (%d) is below the minimum of %d; a pool must "
                "hold at least one connection. Clamping pool_min_size up to %d "
                "— set a positive DB_POOL_MIN_SIZE in the environment.",
                self.pool_min_size,
                SAFE_POOL_MIN_FLOOR,
                SAFE_POOL_MIN_FLOOR,
            )
            self.pool_min_size = SAFE_POOL_MIN_FLOOR

        # The real #320 invariant: total connections an engine may open =
        # pool_size + overflow. Whatever the base, the pool must be able to
        # burst to this many at once or concurrent checkouts queue until they
        # hit the QueuePool timeout. A small DB_POOL_MAX_SIZE (the dev auth/tools
        # right-sizing) is lifted here so light services still get burst room.
        if self.pool_max_size < SAFE_POOL_TOTAL_FLOOR:
            logger.warning(
                "DB_POOL_MAX_SIZE (%d) is below the safe total-capacity floor of "
                "%d; the connections an engine can open at once are capped this "
                "low, which queues concurrent checkouts until they hit QueuePool "
                "timeouts (dynastore #320). Clamping pool_max_size up to %d — "
                "raise DB_POOL_MAX_SIZE in the environment to silence this.",
                self.pool_max_size,
                SAFE_POOL_TOTAL_FLOOR,
                SAFE_POOL_TOTAL_FLOOR,
            )
            self.pool_max_size = SAFE_POOL_TOTAL_FLOOR

        # Guard the *gap* above the base too: if the operator's ``pool_min_size``
        # sits close to ``pool_max_size`` the overflow collapses toward 0 — a
        # pool that cannot grow past its base and deadlocks when concurrent
        # startup consumers need more than the base at once (the size-5
        # overflow-0 starvation behind the dev auth/tools probe-timeout outage).
        # Lift ``pool_max_size`` so a minimum burst headroom always remains.
        min_max_for_overflow = self.pool_min_size + SAFE_POOL_MIN_OVERFLOW
        if self.pool_max_size < min_max_for_overflow:
            logger.warning(
                "DB_POOL_MAX_SIZE (%d) leaves the pool with max_overflow=%d on "
                "top of pool_size=%d; a zero/near-zero overflow cannot absorb "
                "concurrent checkouts and deadlocks startup under load "
                "(QueuePool limit reached). Clamping pool_max_size up to %d so "
                "at least %d overflow connections remain — raise "
                "DB_POOL_MAX_SIZE in the environment to silence this.",
                self.pool_max_size,
                max(self.pool_max_size - self.pool_min_size, 0),
                self.pool_min_size,
                min_max_for_overflow,
                SAFE_POOL_MIN_OVERFLOW,
            )
            self.pool_max_size = min_max_for_overflow

        if self.pool_acquire_timeout <= 0:
            logger.warning(
                "DB_POOL_ACQUIRE_TIMEOUT (%d) must be > 0; resetting to the "
                "default 30s so the pool-acquire wait is bounded (#1894).",
                self.pool_acquire_timeout,
            )
            self.pool_acquire_timeout = 30

        _LARGE_ACQUIRE_TIMEOUT_THRESHOLD = 120
        if self.pool_acquire_timeout > _LARGE_ACQUIRE_TIMEOUT_THRESHOLD:
            logger.warning(
                "DB_POOL_ACQUIRE_TIMEOUT (%ds) is unusually high (> %ds); "
                "a saturated pool will block callers for this long before "
                "failing fast — consider lowering it (#1894).",
                self.pool_acquire_timeout,
                _LARGE_ACQUIRE_TIMEOUT_THRESHOLD,
            )

    @property
    def pool_max_overflow(self) -> int:
        """SQLAlchemy ``max_overflow`` derived from the min/max pool bounds.

        Total connections an engine may open is ``pool_size + max_overflow``;
        with ``pool_size = pool_min_size`` the overflow that caps the total at
        ``pool_max_size`` is ``pool_max_size - pool_min_size``.

        Floored at 0 on purpose: SQLAlchemy reads a *negative* ``max_overflow``
        as **unbounded**, so a misconfigured env where the min exceeds the max
        would silently flip the pool from "too small" to "unlimited" and
        exhaust connections/memory (dynastore #320). Clamping keeps the total
        bounded at ``pool_min_size`` and warns instead.
        """
        gap = self.pool_max_size - self.pool_min_size
        if gap < 0:
            logger.warning(
                "DB_POOL_MAX_SIZE (%d) < DB_POOL_MIN_SIZE (%d); clamping pool "
                "overflow to 0 (total connections capped at the min). A "
                "negative overflow would make the pool unbounded — fix the "
                "env so max >= min.",
                self.pool_max_size,
                self.pool_min_size,
            )
            return 0
        return gap

    def __repr__(self) -> str:
        return class_tools.__repr__(self, sensitive_attrs=["database_url"])
