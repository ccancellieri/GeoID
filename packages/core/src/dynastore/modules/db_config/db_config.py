#    Copyright 2025 FAO
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
import dynastore.tools.class_tools as class_tools

logger = logging.getLogger(__name__)

# Smallest pool floor we consider safe under concurrent load. The review-env
# outage (dynastore #320) ran with ``DB_POOL_MIN_SIZE=2``: with only two base
# connections and an overflow exhausted by 3+ concurrent requests, surplus
# requests queued and timed out ("QueuePool limit of size 2 overflow 3
# reached, connection timed out, timeout 60.00"), cascading into engine-snapshot
# retry exhaustion and sustained 100% memory. A handful of base connections is
# the minimum that lets a few concurrent requests proceed without convoying.
SAFE_POOL_MIN_FLOOR: int = 5

# Smallest total pool capacity (base size + overflow) we consider safe. Even
# with a healthy ``pool_min_size`` a tiny ``DB_POOL_MAX_SIZE`` caps the total
# connections, so the floor is enforced on the effective total as well.
SAFE_POOL_TOTAL_FLOOR: int = 5


def _env_int(name: str, default: int) -> int:
    """Read an int from env var ``name``, tolerating mis-templated values.

    The failure mode behind #1581: a deploy templates the var through (e.g.
    ``iac.yml``) but leaves it undefined, so the container receives the literal
    ``${DB_POOL_RECYCLE}`` (or an empty string) rather than a number. A bare
    ``int(os.getenv(name, "1800"))`` only uses the default when the var is
    *unset* — a non-numeric literal makes ``int()`` raise ``ValueError`` at
    import, the gunicorn worker dies, and the Cloud Run startup probe fails the
    whole revision rollout. Treating any empty / non-numeric value as the
    default (with a WARN) makes a mis-templated env impossible to crash on.
    """
    raw = os.getenv(name)
    if raw is None or raw.strip() == "":
        return default
    try:
        return int(raw.strip())
    except (ValueError, TypeError):
        logger.warning(
            "%s=%r is not a valid integer (commonly an unsubstituted ${...} "
            "placeholder or an empty value from the deploy env); falling back "
            "to the default %d. Set %s to a numeric value to silence this.",
            name,
            raw,
            default,
            name,
        )
        return default


class DBConfig:
    database_url: str = os.getenv(
        "DATABASE_URL", "postgresql://testuser:testpassword@db:5432/gis_dev"
    )
    pool_min_size: int = _env_int("DB_POOL_MIN_SIZE", 5)
    pool_max_size: int = _env_int("DB_POOL_MAX_SIZE", 100)
    pool_max_queries: int = _env_int("DB_POOL_MAX_QUERIES", 50000)
    pool_command_timeout: int = _env_int("DB_POOL_COMMAND_TIMEOUT", 60)
    connect_timeout: int = _env_int("DB_CONNECT_TIMEOUT", 30)
    # SQLAlchemy retires a pooled connection once it reaches this age (#729).
    # On serverless deployments the VPC-egress path silently drops a TCP
    # connection that has been idle past its window; once dropped, the next
    # checkout's pool_pre_ping fails and the replacement handshake costs 8-22s.
    # Recycling proactively — while the path is still warm — keeps reconnects
    # sub-second. Keep this BELOW the deployment's idle-drop window (set a
    # lower DB_POOL_RECYCLE per-environment where idle periods are common).
    pool_recycle: int = _env_int("DB_POOL_RECYCLE", 1800)
    # TCP keepalive tunables (#655). The egress path silently drops the
    # established-connection mapping after an idle window; without keepalive
    # probes the pool hands out a dead-at-the-wire socket whose replacement
    # handshake costs 8-22s. NOTE (#710): these are server-side GUCs only —
    # they do not arm SO_KEEPALIVE on the client socket; pool_recycle above
    # is the load-bearing mitigation until client-side keepalives land.
    tcp_keepalives_idle: int = _env_int("DB_TCP_KEEPALIVES_IDLE", 300)
    tcp_keepalives_interval: int = _env_int("DB_TCP_KEEPALIVES_INTERVAL", 30)
    tcp_keepalives_count: int = _env_int("DB_TCP_KEEPALIVES_COUNT", 5)
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
    lock_timeout: str = os.getenv("DB_LOCK_TIMEOUT", "5s")
    idle_in_transaction_session_timeout: str = os.getenv(
        "DB_IDLE_IN_TRANSACTION_TIMEOUT", "30s"
    )

    def validate_pool_sizing(self) -> None:
        """Make a dangerously-small pool LOUD and SAFE at startup.

        The review-env outage (dynastore #320) was triggered by an env override
        ``DB_POOL_MIN_SIZE=2`` — well below the code default of 5. With only two
        base connections, 3+ concurrent requests exhausted the overflow and
        queued until they timed out ("QueuePool limit of size 2 overflow 3
        reached, connection timed out, timeout 60.00"), cascading into engine
        snapshot retry exhaustion and 100% memory / container restarts.

        This guard emits a clear WARNING naming the offending env var and the
        risk, then clamps ``pool_min_size`` / ``pool_max_size`` up to a safe
        floor so a misconfigured tiny value can never silently strangle the
        service. Called once from the module lifespan after the config is built.
        """
        if self.pool_min_size < SAFE_POOL_MIN_FLOOR:
            logger.warning(
                "DB_POOL_MIN_SIZE (%d) is below the safe floor of %d; under "
                "concurrent load a pool this small queues surplus requests "
                "until they hit QueuePool timeouts (dynastore #320). Clamping "
                "pool_min_size up to %d — raise DB_POOL_MIN_SIZE in the "
                "environment to silence this.",
                self.pool_min_size,
                SAFE_POOL_MIN_FLOOR,
                SAFE_POOL_MIN_FLOOR,
            )
            self.pool_min_size = SAFE_POOL_MIN_FLOOR

        # Effective total capacity an engine may open = pool_size + overflow.
        # Even with a healthy min, a tiny DB_POOL_MAX_SIZE caps the total.
        if self.pool_max_size < SAFE_POOL_TOTAL_FLOOR:
            logger.warning(
                "DB_POOL_MAX_SIZE (%d) is below the safe floor of %d; the total "
                "connections an engine can open is capped this low, which risks "
                "QueuePool timeouts under concurrent load (dynastore #320). "
                "Clamping pool_max_size up to %d — raise DB_POOL_MAX_SIZE in "
                "the environment to silence this.",
                self.pool_max_size,
                SAFE_POOL_TOTAL_FLOOR,
                SAFE_POOL_TOTAL_FLOOR,
            )
            self.pool_max_size = SAFE_POOL_TOTAL_FLOOR

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
