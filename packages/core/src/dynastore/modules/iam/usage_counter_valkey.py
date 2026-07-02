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

"""Valkey-backed driver for :class:`UsageCounterProtocol`.

Rides on the cache module's already-hardened Valkey pool — discovers
the backend via :func:`get_cache_manager().get_async_backend()` and
delegates to its :class:`CountingCacheBackend` primitives. No parallel
client, no extra config surface; IAM, TLS, socket timeouts, cluster
discovery, and the engine reconnect path are inherited from
``ValkeyEngineConfig``.

Selection: the layered composite (PR-A3) picks this driver only when
the active backend exposes :class:`CountingCacheBackend`. Standalone
deployments without Valkey fall through to the durable Postgres driver.
"""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Optional, Tuple

from dynastore.models.protocols.cache import CacheBackend, CountingCacheBackend
from dynastore.modules.iam.usage_counter_bucket import bucket_for, expires_for

logger = logging.getLogger(__name__)


def _key_for(policy_id: str, principal_key: str, bucket: datetime) -> str:
    """Cache-key shape — namespace-prefixed by ``ValkeyCacheBackend._key``."""
    # ISO-second precision matches the bucket math (windows align on
    # whole seconds). Keep the prefix short — every byte multiplies
    # across millions of keys.
    return f"uc:{policy_id}:{principal_key}:{int(bucket.timestamp())}"


def _ttl_seconds(window_seconds: Optional[int], bucket: datetime) -> Optional[float]:
    """Remaining grace TTL on the Valkey row.

    Mirrors :func:`expires_for` (PG): one window of grace past the
    bucket close. Returning ``None`` keeps the key resident — the right
    behavior for lifetime quotas (``max_count``); native TTL only kicks
    in for rate windows.
    """
    expires = expires_for(bucket, window_seconds)
    if expires is None:
        return None
    ttl = expires.timestamp() - datetime.now(tz=bucket.tzinfo).timestamp()
    # Floor to ``window_seconds`` so a freshly-created bucket gets the
    # full grace; never go negative.
    if window_seconds is not None and ttl < window_seconds:
        return float(window_seconds)
    return max(ttl, 0.0)


class ValkeyUsageCounter:
    """Hot-path Valkey driver for :class:`UsageCounterProtocol`."""

    name: str = "valkey"
    priority: int = 100  # wins over standalone Postgres (50), loses to layered (200)

    def __init__(self, backend: Optional[CountingCacheBackend] = None) -> None:
        # Construction is lazy — the cache manager isn't necessarily
        # initialized when the IAM module registers drivers at import
        # time. ``_get_backend`` resolves on first use.
        # An explicitly injected backend is pinned and never re-resolved.
        self._backend: Optional[CountingCacheBackend] = backend
        self._pinned: bool = backend is not None
        self._backend_gen: int = -1  # _backend_generation at resolution time

    def _get_backend(self) -> CountingCacheBackend:
        from dynastore.tools import cache as cache_tools

        # A live reconnect (configs PATCH) closes the old client and
        # registers a fresh backend under the same name, bumping the
        # generation counter. Re-resolve on generation change so this
        # driver doesn't keep calling a backend whose connection pool
        # has been aclose()'d — the same idiom @cached uses.
        if self._backend is not None and (
            self._pinned or self._backend_gen == cache_tools._backend_generation
        ):
            return self._backend

        generation = cache_tools._backend_generation
        active: CacheBackend = cache_tools.get_cache_manager().get_async_backend()
        if not isinstance(active, CountingCacheBackend):
            raise RuntimeError(
                f"ValkeyUsageCounter: active cache backend '{active.name}' does "
                "not implement CountingCacheBackend; layered selection should "
                "have skipped this driver."
            )
        self._backend = active
        self._backend_gen = generation
        return active

    # ------------------------------------------------------------------
    # UsageCounterProtocol
    # ------------------------------------------------------------------

    async def incr(
        self,
        policy_id: str,
        principal_key: str,
        *,
        window_seconds: Optional[int] = None,
        amount: int = 1,
    ) -> int:
        if amount <= 0:
            raise ValueError(f"amount must be > 0, got {amount}")
        bucket = bucket_for(window_seconds)
        key = _key_for(policy_id, principal_key, bucket)
        ttl = _ttl_seconds(window_seconds, bucket)
        backend = self._get_backend()
        return int(await backend.incr(key, amount, ttl=ttl))

    async def get(
        self,
        policy_id: str,
        principal_key: str,
        *,
        window_seconds: Optional[int] = None,
    ) -> int:
        bucket = bucket_for(window_seconds)
        key = _key_for(policy_id, principal_key, bucket)
        backend = self._get_backend()
        # ``get_count`` reads the raw integer — the generic ``get``
        # would try to msgpack-decode an ASCII digit and fail.
        value = await backend.get_count(key)
        return int(value) if value is not None else 0

    async def incr_if_below(
        self,
        policy_id: str,
        principal_key: str,
        limit: int,
        *,
        window_seconds: Optional[int] = None,
        amount: int = 1,
    ) -> Tuple[int, bool]:
        if amount <= 0:
            raise ValueError(f"amount must be > 0, got {amount}")
        bucket = bucket_for(window_seconds)
        key = _key_for(policy_id, principal_key, bucket)
        ttl = _ttl_seconds(window_seconds, bucket)
        backend = self._get_backend()
        return await backend.incr_if_below(key, limit, amount, ttl=ttl)

    async def reset(
        self,
        policy_id: str,
        principal_key: str,
        *,
        window_seconds: Optional[int] = None,
    ) -> None:
        bucket = bucket_for(window_seconds)
        key = _key_for(policy_id, principal_key, bucket)
        backend = self._get_backend()
        await backend.clear(key=key)

    async def reap_expired(self) -> int:
        # Valkey expires keys natively — no scan needed.
        return 0
