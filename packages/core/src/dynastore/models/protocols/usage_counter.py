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

from typing import Optional, Protocol, Tuple, runtime_checkable


@runtime_checkable
class UsageCounterProtocol(Protocol):
    """Shared atomic counter for rate limiting and quota enforcement.

    Read by the IAM policy condition handlers (``rate_limit``,
    ``max_count``) at evaluation time. Implementations:

      * :class:`PostgresUsageCounter` — durable single-table store,
        ``ON CONFLICT DO UPDATE`` CAS for atomic check-and-incr.
      * ``ValkeyUsageCounter`` (later slice) — atomic ``INCR``/``EXPIRE``
        on the platform Valkey, shared across pods, native TTL.
      * ``LayeredUsageCounter`` (later slice) — Valkey hot path with
        write-through deltas batched to Postgres for durability.

    Bucket semantics
    ----------------
    ``window_seconds`` truncates the counter into fixed time buckets:

      * ``window_seconds=60``  → rate-limit, separate row per minute,
        bucket starts at ``floor(now/60)*60``, ``expires_at`` set to
        twice the window so the reaper can drop it.
      * ``window_seconds=None`` → lifetime quota (``max_count``); single
        row per ``(policy_id, principal_key)``, ``expires_at`` NULL.

    ``principal_key`` is opaque — handlers may pass a principal id, a
    role name, ``"ip:1.2.3.4"`` for anonymous clients, or any scope
    discriminator declared by the policy condition.
    """

    @property
    def name(self) -> str:
        """Stable driver identifier used by the layered composite.

        Surfaced for the PR-A3 ``LayeredUsageCounter`` to log which
        tier serviced a call and to break ties between equal-priority
        drivers. Implementations return a short slug
        (``"postgres"``, ``"valkey"``, …) — not a fully-qualified
        class name.
        """
        ...

    @property
    def priority(self) -> int:
        """Driver-selection rank in the layered composite.

        Consumed by the PR-A3 ``LayeredUsageCounter`` when more than
        one driver can service a call. **Lower wins** — the composite
        tries drivers in ascending ``priority`` and falls through to
        the next on failure. ``name`` breaks ties.

        Convention: ``valkey`` ≪ ``postgres``. The Valkey driver is
        the hot read path (in-memory, native TTL); the PG driver is
        the durability tier and is picked only when the hot path is
        unavailable or for write-through delta flushes.
        """
        ...

    async def incr(
        self,
        policy_id: str,
        principal_key: str,
        *,
        window_seconds: Optional[int] = None,
        amount: int = 1,
    ) -> int:
        """Atomically increment the counter; return the new value.

        ``amount`` must be positive. Drivers raise ``ValueError`` on
        ``amount <= 0`` rather than silently no-op'ing (zero) or
        corrupting the counter (negative).
        """
        ...

    async def get(
        self,
        policy_id: str,
        principal_key: str,
        *,
        window_seconds: Optional[int] = None,
    ) -> int:
        """Return the current counter value (0 if no row)."""
        ...

    async def incr_if_below(
        self,
        policy_id: str,
        principal_key: str,
        limit: int,
        *,
        window_seconds: Optional[int] = None,
        amount: int = 1,
    ) -> Tuple[int, bool]:
        """Atomic check-and-incr.

        Returns ``(new_count, allowed)``. ``allowed`` is ``True`` iff the
        increment kept the counter at or below ``limit``. On denial the
        counter is left unchanged and the current value is returned.

        ``amount`` must be positive; drivers raise ``ValueError`` on
        ``amount <= 0``.
        """
        ...

    async def reset(
        self,
        policy_id: str,
        principal_key: str,
        *,
        window_seconds: Optional[int] = None,
    ) -> None:
        """Drop the counter row for one ``(policy, principal, bucket)``.

        ``window_seconds=None`` targets the lifetime bucket (``count``
        for the policy is wiped). Any other value targets the bucket
        containing ``now()`` for that window size — earlier buckets
        (already rolled over or expired) are left to the reap cron.

        At most one row is removed; the SQL keys on the full primary
        key. Use case is administrative reset, not bulk cleanup.
        """
        ...

    async def reap_expired(self) -> int:
        """Remove rows past ``expires_at``; return rows deleted.

        Drivers with native TTL (Valkey) return ``0``. The Postgres
        driver runs ``DELETE … WHERE expires_at < NOW()`` (the same
        pattern as the existing ``iam.refresh_tokens`` / ``iam.grants``
        nightly prune; usage counters join that cron job).
        """
        ...
