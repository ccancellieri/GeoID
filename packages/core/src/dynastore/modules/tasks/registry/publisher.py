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

"""Version-gated self-publish of this pod's task inventory.

Structural PG writes happen only when the build-keyed digest changes
(~once per deploy). The version gate is the shared ``@cached`` decorator
(tools/cache.py): the UPSERT is wrapped in a function keyed on
``(service, digest)``, so a cache hit (this build already published
cluster-wide) skips the write while a miss runs it. A single cheap last_seen
heartbeat refreshes liveness every tick. Cadence piggybacks the
capability-publisher refresh interval.

In-process digest memo
----------------------
``_local_published`` is a process-wide ``{(service, digest): True}`` dict that
records digests published by this process during the current run. It is checked
before calling the shared ``@cached`` gate so that a Valkey outage (observed as
``ValkeyCacheBackend.set failed``) cannot turn the once-per-deploy UPSERT into a
per-tick storm. On a Valkey miss the ``@cached`` decorator would re-enter the
body on every tick; the in-process memo prevents that even when the distributed
cache is unavailable.
"""
from __future__ import annotations

import logging
from typing import Any, List, Optional, Set, Tuple, Union

import dynastore.tasks as tasks_pkg
from dynastore._version import get_git_commit, get_version
from dynastore.modules.db_config.instance import get_service_name
from dynastore.modules.tasks.registry import repository
from dynastore.modules.tasks.registry.model import CapabilityRow, compute_publish_digest
from dynastore.tasks import task_kind
from dynastore.tools.background_service import (
    Leadership,
    PeriodicService,
    PodPolicy,
    ServiceContext,
)
from dynastore.tools.cache import CacheIgnore, cached

logger = logging.getLogger(__name__)

# Re-assert the same build's rows at most this often per service (self-heals a
# row that was deleted out-of-band while the build digest is unchanged). A new
# build always changes the digest -> new key -> immediate re-publish regardless.
_PUBLISH_DIGEST_TTL_SECONDS = 3600.0

# In-process memo: set of (service, digest) pairs successfully published by
# this process during the current run. Consulted before hitting the shared
# @cached gate so a Valkey outage cannot convert the once-per-deploy UPSERT
# into a per-tick storm. Reset is intentionally NOT provided — a process
# restart clears it naturally, and a row deleted out-of-band will be
# self-healed on the next deploy (digest changes) or when the @cached TTL
# expires and the in-process memo alone is not enough to suppress a re-publish.
_local_published: Set[Tuple[str, str]] = set()


def _safe_describe(cls):
    """Return cls.describe() or None — self-description must never break publish."""
    fn = getattr(cls, "describe", None)
    if fn is None:
        return None
    try:
        return fn()
    except Exception as exc:  # noqa: BLE001
        logger.debug("task-registry: describe() failed for %r (%s)", cls, exc)
        return None


def _process_payload_schema(definition) -> Optional[dict]:
    """Best-effort JSON-Schema-ish view of a Process definition's inputs."""
    try:
        inputs = getattr(definition, "inputs", None)
        if not inputs:
            return None
        return {
            "type": "object",
            "properties": {name: inp.schema_ for name, inp in inputs.items()},
        }
    except Exception as exc:  # noqa: BLE001
        logger.debug("task-registry: process schema extract failed (%s)", exc)
        return None


def collect_local_inventory() -> Tuple[str, str, str, List[CapabilityRow]]:
    """Return (service, commit, version, rows) for this pod, or skip if no identity.

    Each row is built FROM the task's ``describe()`` self-description (the SSOT
    for code-level facets), with getattr fallbacks so a task without a working
    describe() still publishes. Process tasks have no payload model on the class
    — their schema is derived from the Process definition's inputs.

    Rows are returned sorted by ``(service, task_key)`` so that the UPSERT loop
    in ``repository.upsert_rows`` always acquires row locks in PK order regardless
    of the iteration order of ``_DYNASTORE_TASKS``. Deterministic order at the
    collection site and at the repository site together eliminate the
    lock-acquisition-order mismatch that caused 40P01 deadlocks.
    """
    service = get_service_name()
    commit = get_git_commit()
    version = get_version()
    rows: List[CapabilityRow] = []
    if not service:
        return ("", commit, version, rows)
    for task_key, cfg in tasks_pkg._DYNASTORE_TASKS.items():
        cls = cfg.cls
        kind = task_kind(cfg)
        descriptor = _safe_describe(cls)
        description = descriptor.description if descriptor is not None else ""
        payload_schema = descriptor.payload_schema if descriptor is not None else None
        if payload_schema is None and kind == "process":
            payload_schema = _process_payload_schema(cfg.definition)
        mandatory = (
            descriptor.mandatory if descriptor is not None
            else bool(getattr(cls, "mandatory", False))
        )
        affinity_tier = (
            descriptor.affinity_tier if descriptor is not None
            else getattr(cls, "affinity_tier", None)
        )
        rows.append(
            CapabilityRow(
                service=service,
                task_key=task_key,
                kind=kind,
                required_capability=None,  # payload-dependent; not summarizable per row
                mandatory=mandatory,
                affinity_tier=affinity_tier,
                service_version=version,
                service_commit=commit,
                version=commit,  # generic version == build commit for now
                description=description,
                payload_schema=payload_schema,
            )
        )
    # Sort by PK so every worker on this pod iterates in the same order.
    rows.sort(key=lambda r: (r.service, r.task_key))
    return (service, commit, version, rows)


@cached(ttl=_PUBLISH_DIGEST_TTL_SECONDS, namespace="task_registry")
async def _publish_if_new(
    service: str,
    digest: str,
    *,
    engine: CacheIgnore[Any],
    rows: CacheIgnore[List[CapabilityRow]],
) -> bool:
    """UPSERT this pod's inventory, gated by the shared cache on (service, digest).

    A cache HIT means this exact build inventory was already published
    cluster-wide for the service, so the body is skipped. A MISS (new build, or
    the gate entry expired) runs the structural UPSERT. ``engine`` and ``rows``
    are CacheIgnore-annotated, so the cache key is only (service, digest); the
    annotation stays correct across parameter renames where a string ignore
    list would silently break the gate.
    """
    await repository.upsert_rows(engine, rows)
    logger.info(
        "task-registry: published %d rows for service=%r (digest=%s)",
        len(rows), service, digest[:12],
    )
    return True


async def publish_inventory(engine) -> None:
    """Publish this pod's inventory (cache-gated UPSERT) and heartbeat liveness.

    Fail-soft: never raises into the caller's loop. The structural publish and
    the liveness heartbeat fail independently — a transient UPSERT error must
    not also drop the heartbeat, because the mandatory-ownership check reads
    last_seen to find live owners. A failed UPSERT is not cached, so the next
    tick retries it.

    The in-process memo (``_local_published``) is checked before the distributed
    cache so that a Valkey outage cannot degrade into a per-tick UPSERT storm.
    When Valkey is healthy the shared ``@cached`` gate also suppresses repeated
    writes cluster-wide; both guards complement each other.
    """
    try:
        service, commit, version, rows = collect_local_inventory()
    except Exception:  # fail-soft; registry must never brick startup
        logger.warning("task-registry: inventory collection failed (non-fatal)", exc_info=True)
        return
    if not service or not rows:
        return
    digest = compute_publish_digest(commit, version, rows)
    try:
        local_key = (service, digest)
        if local_key not in _local_published:
            await _publish_if_new(service, digest, engine=engine, rows=rows)
            _local_published.add(local_key)
        else:
            # In-process memo hit: UPSERT already ran in this process for this
            # (service, digest). Skip the distributed cache round-trip too — the
            # rows have not changed.
            logger.debug(
                "task-registry: in-process memo hit for service=%r (digest=%s); skipping upsert",
                service, digest[:12],
            )
    except Exception:
        logger.warning("task-registry: publish (upsert) failed (non-fatal)", exc_info=True)
    try:
        await repository.heartbeat(engine, service)
    except Exception:
        logger.warning("task-registry: heartbeat failed (non-fatal)", exc_info=True)


class RegistryHeartbeatService(PeriodicService):
    """Publishes this pod's task inventory + liveness heartbeat (issue #2271).

    Leadership election and ephemeral-pod gating are declared as policy and
    applied uniformly by ``BackgroundSupervisor``:

    * ``LEADER_ONLY`` — exactly one pod per service drives the registry writes;
      the supervisor wraps ``tick()`` in ``lease_leadership`` so followers
      skip. On a non-AsyncEngine (single-process / sync / test) deployment the
      supervisor auto-downgrades to run-everywhere.
    * ``SKIP_EPHEMERAL`` — ephemeral Cloud Run Job pods never start it. They run
      one task and exit, so they must not open registry connections or contend
      on the table at scale (the #2271 motivation; resolves #2279 for this loop).

    The advisory ``lock_key`` is ``task-registry-heartbeat:{service}`` so that
    during a rolling deploy pods electing this service use the same lock identity.
    """

    name = "task_registry_heartbeat"
    leadership = Leadership.LEADER_ONLY
    pod_policy = PodPolicy.SKIP_EPHEMERAL

    def __init__(self, *, refresh_seconds: float = 30.0) -> None:
        self.cadence_seconds = refresh_seconds
        service = get_service_name() or "unknown"
        self.lock_key: Optional[Union[int, str]] = f"task-registry-heartbeat:{service}"

    async def tick(self, ctx: ServiceContext) -> None:
        """Publish inventory using the lock connection when available.
        
        Uses ``ctx.lock_connection`` when available (LEADER_ONLY mode) to reuse
        the advisory-lock connection for DB work, avoiding a second pool checkout.
        Falls back to ``ctx.engine`` for RUN_EVERYWHERE mode or non-leader calls.
        """
        engine = ctx.lock_connection if ctx.lock_connection is not None else ctx.engine
        await publish_inventory(engine)
