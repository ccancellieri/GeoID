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

"""
CacheModule — registers a shared Valkey cache backend when VALKEY_URL is set.

Falls back to local in-memory cache (LocalAsyncCacheBackend, priority=1000)
when Valkey is unavailable or VALKEY_URL is not configured.

Add ``module_cache`` to the deployment scope extras to activate::

    scope_catalog = ["dynastore[...,module_cache]"]
"""

from __future__ import annotations

import logging
import os
from contextlib import asynccontextmanager
from typing import AsyncGenerator

from dynastore.modules.protocols import ModuleProtocol

logger = logging.getLogger(__name__)


class CacheModule(ModuleProtocol):
    """SCOPE-controlled module that wires Valkey as the shared cache backend.

    Priority 9 — starts before DBService (10) so the backend is registered
    before any module that uses ``@cached`` in its lifespan.
    """

    priority: int = 9

    def __init__(self, app_state: object) -> None:
        self.app_state = app_state

    @asynccontextmanager
    async def lifespan(self, app_state: object) -> AsyncGenerator[None, None]:
        valkey_url = os.getenv("VALKEY_URL")
        if not valkey_url:
            logger.warning(
                "CACHE BACKEND: LOCAL (in-memory, per-instance) — "
                "VALKEY_URL not set; cross-instance consistency NOT guaranteed."
            )
            yield
            return

        # Mask credentials in logged URL (valkey://:pass@host → valkey://host)
        _safe_url = valkey_url.split("@")[-1] if "@" in valkey_url else valkey_url

        _tls = os.getenv("VALKEY_TLS", "").lower() in ("1", "true", "yes")
        _iam = os.getenv("VALKEY_IAM_AUTH", "").lower() in ("1", "true", "yes")
        _cluster = os.getenv("VALKEY_CLUSTER", "").lower() in ("1", "true", "yes")
        logger.info(
            "CacheModule: Connecting to Valkey at %s (tls=%s, iam_auth=%s, cluster=%s) …",
            _safe_url, _tls, _iam, _cluster,
        )
        try:
            from dynastore.tools.cache_valkey import ValkeyCacheBackend
            backend = ValkeyCacheBackend(url=valkey_url)
        except Exception as exc:
            logger.warning(
                "CacheModule: Cannot initialise Valkey backend (%s) — falling back to local cache.",
                exc,
            )
            logger.warning(
                "CACHE BACKEND: LOCAL (in-memory, per-instance) — "
                "Valkey unavailable; cross-instance consistency NOT guaranteed."
            )
            yield
            return

        try:
            info = await backend.info()
            version = info.get("server", {}).get("redis_version", "?")
            mode = info.get("server", {}).get("redis_mode", "standalone")
            used_mb = info.get("memory", {}).get("used_memory_human", "?")
            logger.info(
                "CacheModule: Valkey OK — version=%s mode=%s used_memory=%s host=%s",
                version, mode, used_mb, _safe_url,
            )
        except Exception as exc:
            logger.warning(
                "CacheModule: Valkey unreachable at %s (%s) — falling back to local cache.",
                _safe_url, exc,
            )
            logger.warning(
                "CACHE BACKEND: LOCAL (in-memory, per-instance) — "
                "Valkey connection failed; cross-instance consistency NOT guaranteed."
            )
            await backend.close()
            yield
            return

        from dynastore.tools.cache import _notify_backend_upgrade, get_cache_manager

        get_cache_manager().register_backend(backend)
        _notify_backend_upgrade()
        logger.info(
            "CACHE BACKEND: VALKEY (shared, cross-instance) — host=%s version=%s mode=%s used_memory=%s",
            _safe_url, version, mode, used_mb,
        )

        try:
            yield
        finally:
            await backend.close()
            logger.info("CacheModule: Valkey connection closed.")
