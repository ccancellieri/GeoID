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

"""Layered composite driver for :class:`UsageCounterProtocol`.

Valkey carries the hot atomic path (single round trip, native TTL,
shared across pods). Postgres remains the durable source of truth so
state survives a Valkey restart and operators can audit / list counts
from SQL.

Update flow
-----------

* Rate-limited windows (``window_seconds`` set) — Valkey is the live
  authority; PG receives batched write-throughs via
  :class:`AsyncBufferAggregator` (one write per flush window, deltas
  summed per ``(policy_id, principal_key, window_start)``). A pod
  crash loses at most one batch's worth of in-flight deltas in PG —
  acceptable for short rate windows.

* Lifetime quotas (``window_seconds=None``) — every increment is
  written through to PG synchronously inside the same await as the
  Valkey call. Losing a count after a pod crash would let a user
  exceed their lifetime cap, so durability beats throughput here.

* Valkey down — the driver returns whatever PG says. ``incr_if_below``
  falls through to :meth:`PostgresUsageCounter.incr_if_below` (its
  ``ON CONFLICT DO UPDATE`` is itself atomic), preserving correctness.

Selection
---------

Priority ``200`` wins over standalone Postgres (50) and Valkey (100).
The IAM module only registers this driver when both a
:class:`CountingCacheBackend` and a Postgres engine are available; in
single-backend deployments the standalone drivers take over.
"""

from __future__ import annotations

import logging
from typing import Optional, Tuple

from dynastore.modules.iam.usage_counter_pg import PostgresUsageCounter
from dynastore.modules.iam.usage_counter_valkey import ValkeyUsageCounter
from dynastore.tools.async_utils import AsyncBufferAggregator

logger = logging.getLogger(__name__)


# In-memory delta record buffered for batched PG write-through.
# (policy_id, principal_key, window_seconds, amount)
_Delta = Tuple[str, str, Optional[int], int]


class LayeredUsageCounter:
    """Composite Valkey-hot + PG-durable :class:`UsageCounterProtocol`."""

    name: str = "layered"
    priority: int = 200

    def __init__(
        self,
        valkey: Optional[ValkeyUsageCounter] = None,
        postgres: Optional[PostgresUsageCounter] = None,
        *,
        flush_threshold: int = 500,
        flush_interval: float = 5.0,
    ) -> None:
        self._valkey = valkey or ValkeyUsageCounter()
        self._postgres = postgres or PostgresUsageCounter()
        self._buffer: AsyncBufferAggregator = AsyncBufferAggregator(
            flush_callback=self._flush_to_postgres,
            threshold=flush_threshold,
            interval=flush_interval,
            name="usage_counter_layered",
        )

    # ------------------------------------------------------------------
    # Lifespan
    # ------------------------------------------------------------------

    async def start(self) -> None:
        await self._buffer.start()

    async def stop(self) -> None:
        await self._buffer.stop()

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
        try:
            new_count = await self._valkey.incr(
                policy_id,
                principal_key,
                window_seconds=window_seconds,
                amount=amount,
            )
        except Exception:
            logger.warning(
                "LayeredUsageCounter.incr: Valkey unavailable, falling back to PG",
                exc_info=True,
            )
            return await self._postgres.incr(
                policy_id,
                principal_key,
                window_seconds=window_seconds,
                amount=amount,
            )

        await self._record_pg_delta(
            policy_id, principal_key, window_seconds, amount
        )
        return new_count

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
        try:
            new_count, allowed = await self._valkey.incr_if_below(
                policy_id,
                principal_key,
                limit,
                window_seconds=window_seconds,
                amount=amount,
            )
        except Exception:
            logger.warning(
                "LayeredUsageCounter.incr_if_below: Valkey unavailable, "
                "falling back to PG",
                exc_info=True,
            )
            return await self._postgres.incr_if_below(
                policy_id,
                principal_key,
                limit,
                window_seconds=window_seconds,
                amount=amount,
            )

        if allowed:
            await self._record_pg_delta(
                policy_id, principal_key, window_seconds, amount
            )
        return (new_count, allowed)

    async def get(
        self,
        policy_id: str,
        principal_key: str,
        *,
        window_seconds: Optional[int] = None,
    ) -> int:
        # Valkey is the live authority within a window — read it first.
        # On miss (cold pod, key expired, Valkey down) fall back to PG.
        try:
            value = await self._valkey.get(
                policy_id, principal_key, window_seconds=window_seconds
            )
            if value > 0:
                return value
        except Exception:
            logger.debug(
                "LayeredUsageCounter.get: Valkey lookup failed", exc_info=True
            )
        return await self._postgres.get(
            policy_id, principal_key, window_seconds=window_seconds
        )

    async def reset(
        self,
        policy_id: str,
        principal_key: str,
        *,
        window_seconds: Optional[int] = None,
    ) -> None:
        # Drop both — PG is authoritative, but a stale Valkey row would
        # let the next request see an out-of-date count.
        errors: list = []
        try:
            await self._valkey.reset(
                policy_id, principal_key, window_seconds=window_seconds
            )
        except Exception as exc:
            errors.append(("valkey", exc))
        try:
            await self._postgres.reset(
                policy_id, principal_key, window_seconds=window_seconds
            )
        except Exception as exc:
            errors.append(("postgres", exc))
        if errors:
            logger.warning(
                "LayeredUsageCounter.reset: partial failure %s", errors
            )

    async def reap_expired(self) -> int:
        # Valkey expires natively; only PG needs the prune.
        return await self._postgres.reap_expired()

    # ------------------------------------------------------------------
    # Internal: PG write-through
    # ------------------------------------------------------------------

    async def _record_pg_delta(
        self,
        policy_id: str,
        principal_key: str,
        window_seconds: Optional[int],
        amount: int,
    ) -> None:
        # Lifetime quotas: write-through synchronously. Losing a delta
        # after a pod crash would let a user exceed their cap.
        if window_seconds is None or window_seconds <= 0:
            try:
                await self._postgres.incr(
                    policy_id,
                    principal_key,
                    window_seconds=window_seconds,
                    amount=amount,
                )
            except Exception:
                logger.warning(
                    "LayeredUsageCounter: PG sync write-through failed "
                    "(policy=%s principal=%s)",
                    policy_id,
                    principal_key,
                    exc_info=True,
                )
            return

        # Rate windows: buffer for batched flush.
        await self._buffer.add(
            (policy_id, principal_key, window_seconds, amount)
        )

    async def _flush_to_postgres(self, deltas: list) -> None:
        if not deltas:
            return
        # Collapse repeated (policy, principal, window) tuples — one PG
        # round trip per distinct bucket per flush, regardless of how
        # many in-window hits arrived.
        merged: dict = {}
        for policy_id, principal_key, window_seconds, amount in deltas:
            key = (policy_id, principal_key, window_seconds)
            merged[key] = merged.get(key, 0) + amount

        for (policy_id, principal_key, window_seconds), amount in merged.items():
            try:
                await self._postgres.incr(
                    policy_id,
                    principal_key,
                    window_seconds=window_seconds,
                    amount=amount,
                )
            except Exception:
                logger.warning(
                    "LayeredUsageCounter._flush_to_postgres failed "
                    "(policy=%s principal=%s window=%s amount=%s)",
                    policy_id,
                    principal_key,
                    window_seconds,
                    amount,
                    exc_info=True,
                )
