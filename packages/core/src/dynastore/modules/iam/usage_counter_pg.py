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

from typing import Optional, Tuple

from dynastore.modules.db_config.query_executor import (
    DQLQuery,
    ResultHandler,
    managed_transaction,
)
from dynastore.modules.iam.iam_queries import (
    REAP_EXPIRED_USAGE_COUNTERS_SQL,
    REAP_ORPHAN_USAGE_COUNTERS_SQL,
)
from dynastore.modules.iam.usage_counter_bucket import (
    bucket_for as _bucket_for,
    expires_for as _expires_for,
)
from dynastore.tools.protocol_helpers import get_engine

__all__ = ["PostgresUsageCounter"]


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

# SQL shared with the pg_cron nightly prune function — see the SSOT
# block in `iam_queries.REAP_EXPIRED_USAGE_COUNTERS_SQL`.
_REAP_EXPIRED = DQLQuery(
    REAP_EXPIRED_USAGE_COUNTERS_SQL,
    result_handler=ResultHandler.ROWCOUNT,
)

# Safety-net reaper for orphan lifetime rows whose policy is gone.
# SSOT lives in `iam_queries.REAP_ORPHAN_USAGE_COUNTERS_SQL` and is the
# same string the pg_cron prune function interpolates — keep them
# aligned so a future tweak (e.g. archive-instead-of-delete) lands in
# both places at once.
_REAP_ORPHANS = DQLQuery(
    REAP_ORPHAN_USAGE_COUNTERS_SQL,
    result_handler=ResultHandler.ROWCOUNT,
)

_LIST_FOR_POLICY = DQLQuery(
    """
    SELECT principal_key, count, window_start, expires_at, last_seen_at
      FROM {schema}.usage_counters
     WHERE policy_id = :policy_id
     ORDER BY last_seen_at DESC NULLS LAST, principal_key
     LIMIT :limit OFFSET :offset;
    """,
    result_handler=ResultHandler.ALL_DICTS,
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

    @property
    def _engine(self):
        """Resolve the active DB engine lazily on every access.

        Capturing the engine in ``__init__`` was a latent ordering trap
        (gap #7 of #800): the trap doesn't bite today because
        ``IamModule.lifespan`` fetches ``DatabaseProtocol`` before
        instantiating the driver, but a future refactor that moves
        construction earlier — or a test fixture that swaps the engine
        mid-run — would silently strand the driver on a stale or
        ``None`` engine reference. Lazy resolution closes that class of
        regression at the cost of one provider-discovery lookup per
        operation; the lookup is O(small) and reads from an in-memory
        registry, so it does not add a DB round-trip.
        """
        return get_engine()

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
        if amount <= 0:
            raise ValueError(f"amount must be > 0, got {amount}")
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

    async def reap_orphans(self) -> int:
        """Safety-net reaper for lifetime rows whose parent policy is gone.

        Not part of :class:`UsageCounterProtocol` — the protocol exposes
        ``reap_expired`` only, mirroring TTL semantics common to both PG
        and Valkey. Orphan-policy cleanup is PG-specific (Valkey has no
        notion of "policy"), and the canonical site in production is the
        pg_cron prune function — this method is a local-dev / test
        affordance using the same SSOT string the cron job interpolates.
        """
        async with managed_transaction(self._engine) as db:
            return int(await _REAP_ORPHANS.execute(conn=db, schema=self._schema) or 0)

    async def list_for_policy(
        self,
        policy_id: str,
        *,
        limit: int = 100,
        offset: int = 0,
    ) -> list:
        """Paged listing of counter rows for one policy.

        Not part of :class:`UsageCounterProtocol` — admin-only side
        door for the usage panel. The layered driver delegates here
        because Valkey can't ``SCAN`` by policy prefix efficiently at
        scale; the durable table is the right source for inspection.
        """
        async with managed_transaction(self._engine) as db:
            rows = await _LIST_FOR_POLICY.execute(
                conn=db,
                schema=self._schema,
                policy_id=policy_id,
                limit=limit,
                offset=offset,
            )
        return list(rows or [])
