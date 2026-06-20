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

"""SQL IO for the task-capability registry.

UPSERT keyed on the (service, task_key) PK. The only per-tick write in steady
state is the cheap last_seen heartbeat. The build-keyed publish digest is NOT
stored here — the publisher gates the structural UPSERT with the shared
``@cached`` decorator (tools/cache.py), so no bespoke digest store exists.

Driver-agnostic IO
------------------
Every statement runs through ``managed_transaction`` + ``DQLQuery`` rather than
raw ``engine.begin()`` / ``engine.connect()``. The registry heartbeat publishes
from *every* pod — including background **job** processes that reach Postgres via
the datastore module's **sync** driver. ``managed_transaction`` accepts both an
async server engine (asyncpg) and a sync ``Engine``, and the ``DQLQuery``
executor drives either connection kind. ``async with engine.begin()`` was
async-only: under a job's sync engine ``engine.begin()`` returns a *sync*
``contextlib._GeneratorContextManager``, so ``async with`` raised
``'... does not support the asynchronous context manager protocol'`` and dropped
every heartbeat (stale ``last_seen`` → live owners look dead to the routing /
mandatory-ownership check).

Deadlock prevention
-------------------
Multiple gunicorn workers and job pods write to the same ``(service, *)`` rows
concurrently. Two different row-ordering strategies used to coexist:

* ``upsert_rows`` iterated ``rows`` in list order → lock order = list order.
* The old ``_HEARTBEAT_SQL`` bulk UPDATE locked all rows for a service in
  physical/heap-scan order — different from the UPSERT order → classic 40P01.

Both paths now acquire row locks in the same deterministic PK order
``(service, task_key)`` ASC:

* ``upsert_rows`` sorts ``rows`` before the loop (the caller also sorts at
  collection time, but sorting here is the hard guarantee).
* ``heartbeat`` uses a ``FOR UPDATE`` sub-select ordered by ``(service,
  task_key)`` so the UPDATE locks rows in the same order as the UPSERT loop.

Both public functions are also wrapped in ``retry_on_lock_conflict`` so a
deadlock victim retries instead of silently dropping liveness.
"""
from __future__ import annotations

import json
from typing import List

from dynastore.modules.db_config.locking_tools import retry_on_lock_conflict
from dynastore.modules.db_config.query_executor import (
    DQLQuery,
    ResultHandler,
    managed_transaction,
)
from dynastore.modules.tasks.registry.model import (
    TASK_CAPABILITY_REGISTRY_TABLE,
    CapabilityRow,
)

_UPSERT_SQL = f"""
INSERT INTO {TASK_CAPABILITY_REGISTRY_TABLE}
    (service, task_key, kind, required_capability, mandatory,
     affinity_tier, service_version, service_commit, version,
     description, payload_schema, last_seen, updated_at)
VALUES
    (:service, :task_key, :kind, :required_capability, :mandatory,
     :affinity_tier, :service_version, :service_commit, :version,
     :description, CAST(:payload_schema AS jsonb), now(), now())
ON CONFLICT (service, task_key) DO UPDATE SET
    kind = EXCLUDED.kind,
    required_capability = EXCLUDED.required_capability,
    mandatory = EXCLUDED.mandatory,
    affinity_tier = EXCLUDED.affinity_tier,
    service_version = EXCLUDED.service_version,
    service_commit = EXCLUDED.service_commit,
    version = EXCLUDED.version,
    description = EXCLUDED.description,
    payload_schema = EXCLUDED.payload_schema,
    last_seen = now(),
    updated_at = now()
"""

# Heartbeat UPDATE locks rows via a FOR UPDATE sub-select ordered by the PK
# (service, task_key) ASC — identical to the order the UPSERT loop uses.
# This eliminates the divergent acquisition order that caused 40P01 when
# multiple concurrent writers hit the same service's rows.
_HEARTBEAT_SQL = f"""
UPDATE {TASK_CAPABILITY_REGISTRY_TABLE} AS t
SET last_seen = now()
WHERE (t.service, t.task_key) IN (
    SELECT service, task_key
    FROM {TASK_CAPABILITY_REGISTRY_TABLE}
    WHERE service = :service
    ORDER BY service, task_key
    FOR UPDATE
)
"""

_LIST_SQL = f"""
SELECT service, task_key, kind, required_capability, mandatory,
       affinity_tier, service_version, service_commit, version,
       description, payload_schema, last_seen, updated_at
FROM {TASK_CAPABILITY_REGISTRY_TABLE}
ORDER BY service, task_key
"""

# Live correct-tier owners of a task_key (consumed by the mandatory-ownership check).
_LIVE_OWNERS_SQL = f"""
SELECT service, affinity_tier, last_seen
FROM {TASK_CAPABILITY_REGISTRY_TABLE}
WHERE task_key = :task_key
  AND last_seen > now() - make_interval(secs => :ttl_grace_seconds)
"""

# Batch: all live rows across every task_key in one round-trip.
_LIVE_OWNERS_ALL_SQL = f"""
SELECT task_key, service, affinity_tier, last_seen
FROM {TASK_CAPABILITY_REGISTRY_TABLE}
WHERE last_seen > now() - make_interval(secs => :ttl_grace_seconds)
"""


def _coerce_payload_schema(d: dict) -> dict:
    """asyncpg may hand back jsonb as a JSON string under a raw query; decode it
    to a dict so callers get structured data. Pass dicts/None through."""
    ps = d.get("payload_schema")
    if isinstance(ps, str):
        try:
            d["payload_schema"] = json.loads(ps)
        except (ValueError, TypeError):
            pass  # leave the raw string if it is not valid JSON
    return d


@retry_on_lock_conflict(max_retries=5, base_delay=0.1)
async def upsert_rows(engine, rows: List[CapabilityRow]) -> int:
    if not rows:
        return 0
    # Sort by PK before opening the transaction so every concurrent writer
    # acquires row locks in the same order — prerequisite for deadlock freedom.
    sorted_rows = sorted(rows, key=lambda r: (r.service, r.task_key))
    async with managed_transaction(engine) as conn:
        for r in sorted_rows:
            # JSON-encode the dict for the jsonb bind: the CAST(:payload_schema
            # AS jsonb) text bind won't auto-encode a Python dict, so serialize
            # it ourselves. One UPSERT per row keeps the path driver-agnostic
            # (no executemany); this runs ~once per deploy (digest-gated).
            p = r.model_dump()
            ps = p.get("payload_schema")
            p["payload_schema"] = json.dumps(ps) if ps is not None else None
            await DQLQuery(_UPSERT_SQL, result_handler=ResultHandler.NONE).execute(conn, **p)
    return len(rows)


@retry_on_lock_conflict(max_retries=5, base_delay=0.1)
async def heartbeat(engine, service: str) -> None:
    async with managed_transaction(engine) as conn:
        await DQLQuery(_HEARTBEAT_SQL, result_handler=ResultHandler.NONE).execute(
            conn, service=service,
        )


async def list_all(engine) -> List[dict]:
    async with managed_transaction(engine) as conn:
        rows = await DQLQuery(_LIST_SQL, result_handler=ResultHandler.ALL_DICTS).execute(conn)
    return [_coerce_payload_schema(dict(r)) for r in rows]


async def live_owners_for(engine, task_key: str, ttl_grace_seconds: float) -> List[dict]:
    async with managed_transaction(engine) as conn:
        return await live_owners_for_conn(conn, task_key, ttl_grace_seconds)


async def live_owners_for_conn(conn, task_key: str, ttl_grace_seconds: float) -> List[dict]:
    """Same as ``live_owners_for`` but uses a caller-supplied connection."""
    rows = await DQLQuery(_LIVE_OWNERS_SQL, result_handler=ResultHandler.ALL_DICTS).execute(
        conn, task_key=task_key, ttl_grace_seconds=ttl_grace_seconds,
    )
    return [dict(r) for r in rows]


async def live_owners_all(engine, ttl_grace_seconds: float) -> dict:
    """Return all live registry rows in one round-trip, grouped by task_key.

    Returns a ``{task_key: [row, ...]}`` dict; every row has the same
    shape as ``live_owners_for`` (service, affinity_tier, last_seen).
    """
    async with managed_transaction(engine) as conn:
        return await live_owners_all_conn(conn, ttl_grace_seconds)


async def live_owners_all_conn(conn, ttl_grace_seconds: float) -> dict:
    """Same as ``live_owners_all`` but uses a caller-supplied connection."""
    rows = await DQLQuery(_LIVE_OWNERS_ALL_SQL, result_handler=ResultHandler.ALL_DICTS).execute(
        conn, ttl_grace_seconds=ttl_grace_seconds,
    )
    grouped: dict = {}
    for row in rows:
        d = dict(row)
        key = d.pop("task_key")
        grouped.setdefault(key, []).append(d)
    return grouped
