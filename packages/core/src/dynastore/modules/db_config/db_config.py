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


class DBConfig:
    database_url: str = os.getenv(
        "DATABASE_URL", "postgresql://testuser:testpassword@db:5432/gis_dev"
    )
    pool_min_size: int = int(os.getenv("DB_POOL_MIN_SIZE", "5"))
    pool_max_size: int = int(os.getenv("DB_POOL_MAX_SIZE", "100"))
    pool_max_queries: int = int(os.getenv("DB_POOL_MAX_QUERIES", "50000"))
    pool_command_timeout: int = int(os.getenv("DB_POOL_COMMAND_TIMEOUT", "60"))
    connect_timeout: int = int(os.getenv("DB_CONNECT_TIMEOUT", "30"))
    # SQLAlchemy retires a pooled connection once it reaches this age (#729).
    # On serverless deployments the VPC-egress path silently drops a TCP
    # connection that has been idle past its window; once dropped, the next
    # checkout's pool_pre_ping fails and the replacement handshake costs 8-22s.
    # Recycling proactively — while the path is still warm — keeps reconnects
    # sub-second. Keep this BELOW the deployment's idle-drop window (set a
    # lower DB_POOL_RECYCLE per-environment where idle periods are common).
    pool_recycle: int = int(os.getenv("DB_POOL_RECYCLE", "1800"))
    # TCP keepalive tunables (#655). The egress path silently drops the
    # established-connection mapping after an idle window; without keepalive
    # probes the pool hands out a dead-at-the-wire socket whose replacement
    # handshake costs 8-22s. NOTE (#710): these are server-side GUCs only —
    # they do not arm SO_KEEPALIVE on the client socket; pool_recycle above
    # is the load-bearing mitigation until client-side keepalives land.
    tcp_keepalives_idle: int = int(os.getenv("DB_TCP_KEEPALIVES_IDLE", "300"))
    tcp_keepalives_interval: int = int(os.getenv("DB_TCP_KEEPALIVES_INTERVAL", "30"))
    tcp_keepalives_count: int = int(os.getenv("DB_TCP_KEEPALIVES_COUNT", "5"))

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
