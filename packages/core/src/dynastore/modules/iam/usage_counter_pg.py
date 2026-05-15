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

"""Postgres driver for :class:`UsageCounterProtocol`.

The durable source of truth for rate-limit and lifetime-quota counters.
Backed by ``iam.usage_counters`` (DDL in :mod:`.iam_queries`,
bootstrapped by :class:`PostgresIamStorage`). A later slice adds the
Valkey hot-path driver and a layered composite; the policy condition
handlers consume only the protocol so the wiring stays unchanged.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional, Tuple

from dynastore.modules.db_config.query_executor import (
    DQLQuery,
    ResultHandler,
    managed_transaction,
)
from dynastore.tools.protocol_helpers import get_engine

# Fixed-bucket sentinel for lifetime counters. Postgres ``TIMESTAMPTZ``
# epoch is ``1970-01-01 UTC``; chosen so the PK index keeps lifetime
# rows clustered at the low end of the window_start range.
_LIFETIME_BUCKET = datetime.fromtimestamp(0, tz=timezone.utc)


def _bucket_for(window_seconds: Optional[int], now: Optional[datetime] = None) -> datetime:
    if window_seconds is None or window_seconds <= 0:
        return _LIFETIME_BUCKET
    moment = now or datetime.now(timezone.utc)
    floor_ts = (int(moment.timestamp()) // window_seconds) * window_seconds
    return datetime.fromtimestamp(floor_ts, tz=timezone.utc)


def _expires_for(
    bucket: datetime, window_seconds: Optional[int]
) -> Optional[datetime]:
    if window_seconds is None or window_seconds <= 0:
        return None
    # One window of grace past the bucket close — the nightly reaper
    # drops rows whose window has fully aged out. Short-lived in-flight
    # decisions during the trailing window still read a fresh count.
    return datetime.fromtimestamp(
        bucket.timestamp() + 2 * window_seconds, tz=timezone.utc
    )


# --- Queries --------------------------------------------------------------

# Plain incr (no cap predicate). Single round trip, single statement.
_INCR_AND_RETURN = DQLQuery(
    """
    INSERT INTO {schema}.usage_counters AS u
        (policy_id, principal_key, window_start, count, expires_at, last_seen_at)
    VALUES
        (:policy_id, :principal_key, :window_start, :amount, :expires_at, NOW())
    ON CONFLICT (policy_id, principal_key, window_start)
    DO UPDATE SET
        count        = u.count + EXCLUDED.count,
        last_seen_at = NOW(),
        expires_at   = COALESCE(u.expires_at, EXCLUDED.expires_at)
    RETURNING count;
    """,
    result_handler=ResultHandler.SCALAR_ONE,
)

# Atomic check-and-incr. Both branches of the upsert are gated by the
# cap predicate:
#   * INSERT path — the ``INSERT … SELECT … WHERE :amount <= :limit``
#     form rejects the very first hit when the requested amount already
#     exceeds the cap (a plain ``INSERT … VALUES`` would bypass the cap
#     because Postgres' ``ON CONFLICT DO UPDATE … WHERE`` predicate only
#     applies to the update branch, not the insert).
#   * UPDATE path — ``ON CONFLICT DO UPDATE … WHERE u.count +
#     EXCLUDED.count <= :limit`` rejects the increment once a row exists
#     and the sum would exceed the cap.
# The wrapping CTE falls back to ``SELECT count`` so the caller always
# sees the current value in one round trip, whether the CAS succeeded,
# was blocked by the predicate, or never ran (insert blocked).
_INCR_IF_BELOW = DQLQuery(
    """
    WITH cas AS (
        INSERT INTO {schema}.usage_counters AS u
            (policy_id, principal_key, window_start, count, expires_at, last_seen_at)
        SELECT :policy_id, :principal_key, :window_start, :amount, :expires_at, NOW()
         WHERE :amount <= :limit
        ON CONFLICT (policy_id, principal_key, window_start)
        DO UPDATE SET
            count        = u.count + EXCLUDED.count,
            last_seen_at = NOW(),
            expires_at   = COALESCE(u.expires_at, EXCLUDED.expires_at)
        WHERE u.count + EXCLUDED.count <= :limit
        RETURNING count
    ),
    fallback AS (
        SELECT count
          FROM {schema}.usage_counters
         WHERE policy_id = :policy_id
           AND principal_key = :principal_key
           AND window_start = :window_start
    )
    SELECT
        COALESCE((SELECT count FROM cas), (SELECT count FROM fallback), 0) AS count,
        EXISTS (SELECT 1 FROM cas)                                          AS allowed;
    """,
    result_handler=ResultHandler.ONE_DICT,
)

_GET = DQLQuery(
    """
    SELECT COALESCE(count, 0) AS count
      FROM {schema}.usage_counters
     WHERE policy_id = :policy_id
       AND principal_key = :principal_key
       AND window_start = :window_start;
    """,
    result_handler=ResultHandler.SCALAR_ONE_OR_NONE,
)

_RESET = DQLQuery(
    """
    DELETE FROM {schema}.usage_counters
     WHERE policy_id = :policy_id
       AND principal_key = :principal_key
       AND window_start = :window_start;
    """,
    result_handler=ResultHandler.ROWCOUNT,
)

_REAP_EXPIRED = DQLQuery(
    """
    DELETE FROM {schema}.usage_counters
     WHERE expires_at IS NOT NULL
       AND expires_at < NOW();
    """,
    result_handler=ResultHandler.ROWCOUNT,
)


# --- Driver ---------------------------------------------------------------

class PostgresUsageCounter:
    """Durable Postgres-backed :class:`UsageCounterProtocol` driver.

    Single-table store keyed by ``(policy_id, principal_key, window_start)``.
    All operations are single-round-trip and rely on the PK for atomicity;
    no row locks held outside the upsert statement.

    ``name`` / ``priority``
    -----------------------
    Class attributes that satisfy the protocol's selection surface. Not
    consumed by PR-A1 — the PR-A3 ``LayeredUsageCounter`` reads them to
    rank drivers when both can serve a call (lower ``priority`` wins,
    ``name`` breaks ties). The PG driver is the durability tier behind
    the Valkey hot path, so its ``priority`` is the highest of the
    in-tree drivers and ``name`` is the short slug ``"postgres"``.
    """

    name: str = "postgres"
    priority: int = 50

    def __init__(self, schema: str = "iam") -> None:
        self._schema = schema
        self._engine = get_engine()

    async def incr(
        self,
        policy_id: str,
        principal_key: str,
        *,
        window_seconds: Optional[int] = None,
        amount: int = 1,
    ) -> int:
        bucket = _bucket_for(window_seconds)
        async with managed_transaction(self._engine) as db:
            return await _INCR_AND_RETURN.execute(
                conn=db,
                schema=self._schema,
                policy_id=policy_id,
                principal_key=principal_key,
                window_start=bucket,
                amount=amount,
                expires_at=_expires_for(bucket, window_seconds),
            )

    async def get(
        self,
        policy_id: str,
        principal_key: str,
        *,
        window_seconds: Optional[int] = None,
    ) -> int:
        bucket = _bucket_for(window_seconds)
        async with managed_transaction(self._engine) as db:
            row = await _GET.execute(
                conn=db,
                schema=self._schema,
                policy_id=policy_id,
                principal_key=principal_key,
                window_start=bucket,
            )
        return int(row or 0)

    async def incr_if_below(
        self,
        policy_id: str,
        principal_key: str,
        limit: int,
        *,
        window_seconds: Optional[int] = None,
        amount: int = 1,
    ) -> Tuple[int, bool]:
        bucket = _bucket_for(window_seconds)
        async with managed_transaction(self._engine) as db:
            row = await _INCR_IF_BELOW.execute(
                conn=db,
                schema=self._schema,
                policy_id=policy_id,
                principal_key=principal_key,
                window_start=bucket,
                amount=amount,
                limit=limit,
                expires_at=_expires_for(bucket, window_seconds),
            )
        if not row:
            return (0, False)
        return (int(row["count"]), bool(row["allowed"]))

    async def reset(
        self,
        policy_id: str,
        principal_key: str,
        *,
        window_seconds: Optional[int] = None,
    ) -> None:
        bucket = _bucket_for(window_seconds)
        async with managed_transaction(self._engine) as db:
            await _RESET.execute(
                conn=db,
                schema=self._schema,
                policy_id=policy_id,
                principal_key=principal_key,
                window_start=bucket,
            )

    async def reap_expired(self) -> int:
        async with managed_transaction(self._engine) as db:
            return int(await _REAP_EXPIRED.execute(conn=db, schema=self._schema) or 0)
