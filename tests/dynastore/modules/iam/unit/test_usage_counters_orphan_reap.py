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

"""Pin the SSOT for the *orphan lifetime* usage-counter reaper.

Gap #5 of issue #800: lifetime rows (``expires_at IS NULL``) are
intentionally skipped by the windowed-expiry reaper, so if a policy is
deleted by a non-transactional path (manual SQL, partial restore from
backup) its lifetime counters strand forever. The pg_cron prune
function now interpolates a second SSOT string —
``REAP_ORPHAN_USAGE_COUNTERS_SQL`` — that drops lifetime rows whose
``policy_id`` no longer exists in ``iam.policies``. The same string
backs the in-process ``PostgresUsageCounter.reap_orphans`` method.

These tests pin the contract so a future tweak (archive instead of
delete, grace window, etc.) lands in one place and is reflected on
both sides.
"""

from __future__ import annotations


def test_postgres_driver_uses_shared_orphan_reap_sql() -> None:
    """The ``_REAP_ORPHANS`` query template in the PG driver must be the
    same string as ``REAP_ORPHAN_USAGE_COUNTERS_SQL`` — otherwise the
    SSOT is fiction and drift is back on the table."""
    from dynastore.modules.iam.iam_queries import (
        REAP_ORPHAN_USAGE_COUNTERS_SQL,
    )
    from dynastore.modules.iam.usage_counter_pg import _REAP_ORPHANS

    assert _REAP_ORPHANS.template == REAP_ORPHAN_USAGE_COUNTERS_SQL


def test_orphan_reap_sql_targets_only_lifetime_rows_with_missing_policy() -> None:
    """Windowed rows (``expires_at IS NOT NULL``) must NOT be touched by
    this reaper — they're the other reaper's job. And the orphan
    predicate must be a ``NOT EXISTS`` against ``policies``, not a flat
    ``DELETE`` of all lifetime rows."""
    from dynastore.modules.iam.iam_queries import (
        REAP_ORPHAN_USAGE_COUNTERS_SQL,
    )

    sql = REAP_ORPHAN_USAGE_COUNTERS_SQL.lower()
    assert "delete from" in sql
    assert "usage_counters" in sql
    assert "expires_at is null" in sql
    assert "not exists" in sql
    assert "policies" in sql
    # Defensive: must not encode a ``last_seen_at`` predicate — using
    # stale-time as a reap criterion would silently break lifetime
    # semantics for quiet principals.
    assert "last_seen_at" not in sql


def test_plpgsql_prune_body_embeds_orphan_ssot() -> None:
    """The plpgsql prune body assembled in ``PostgresIamStorage`` must
    interpolate the SSOT constant, not a hand-typed copy. Guards against
    a future refactor that re-inlines the orphan WHERE clause."""
    import inspect

    from dynastore.modules.iam import postgres_iam_storage

    src = inspect.getsource(postgres_iam_storage)
    assert "REAP_ORPHAN_USAGE_COUNTERS_SQL" in src, (
        "plpgsql prune function must reference the orphan-reap SSOT "
        "constant; found no import / usage in postgres_iam_storage.py"
    )


def test_orphan_reap_and_expired_reap_are_distinct_sql() -> None:
    """The two reapers target different row sets — they must not share
    a single SQL string. If they did, gap #5 would just be a rename of
    gap #6 and one of the two cron lines is dead weight."""
    from dynastore.modules.iam.iam_queries import (
        REAP_EXPIRED_USAGE_COUNTERS_SQL,
        REAP_ORPHAN_USAGE_COUNTERS_SQL,
    )

    assert REAP_EXPIRED_USAGE_COUNTERS_SQL != REAP_ORPHAN_USAGE_COUNTERS_SQL
    # The expired reaper looks at expires_at, the orphan reaper looks at
    # policy existence — confirm the predicates are on different axes.
    assert "expires_at < NOW()" in REAP_EXPIRED_USAGE_COUNTERS_SQL
    assert "NOT EXISTS" in REAP_ORPHAN_USAGE_COUNTERS_SQL
