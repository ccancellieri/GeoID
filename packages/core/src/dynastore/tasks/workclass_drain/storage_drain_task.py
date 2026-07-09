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

# dynastore/tasks/workclass_drain/storage_drain_task.py

"""``StorageDrainTask`` — control-plane-native drain for ``tasks.storage``.

Drains the GLOBAL ``tasks.storage`` table for ALL tenants (tenancy is the
``schema_name`` column, not the physical table). Uses ``task_type``
``"storage_drain"``.

Claim_version fencing (#1945)
-----------------------------
Every claim bumps ``claim_version = claim_version + 1`` on the row.  Terminal
writes (``mark_done`` / ``mark_retry`` / ``mark_dead``) are guarded by::

    AND claimed_by = :owner_id AND claim_version = :claim_version

If a stalled drain worker that was reclaimed by another pod (bumping
``claim_version`` again) later tries to finalize the row, the CAS predicate
matches 0 rows — the stale write is a no-op and the live owner retains
exclusive control.

Drain loop
----------
``run(payload)`` loops ``drain_once()`` until it returns 0, then exits
(one-shot drain-to-empty shape).  The
dispatcher restarts via LISTEN / periodic catch-up; a single ``run`` call
only needs to clear the current backlog.

In-process byte/wall-clock budget (#2732 step 4)
-------------------------------------------------
``StorageDrainTask`` always starts in-process (catalog API pod). If its
cumulative hydrated-document byte total or wall-clock elapsed crosses
``TasksPluginConfig.storage_drain_inprocess_max_bytes`` /
``storage_drain_inprocess_max_seconds`` with backlog rows still remaining,
``run()`` stops early and hands the remainder off to
:class:`StorageDrainOffloadTask` (``task_type`` ``"storage_drain_offload"``),
a thin subclass that drains to empty with no budget. Unlike the base task,
``StorageDrainOffloadTask`` carries the async-write workclass marker
(``dynastore.tasks.workclass_drain.AsyncWriteDrainTaskProtocol.is_async_write_workclass``,
#2782) — ``offload_required()`` therefore treats it exactly like
``event_drain``: it always prefers an external executor (the
``async_writer`` Cloud Run Job) when one advertises the task type, and
degrades to an in-process ``background`` run when none does (e.g. onprem).
"""
from __future__ import annotations

import asyncio
import json
import logging
import time
from dataclasses import replace as _dataclass_replace
from typing import Any, ClassVar, Dict, List, Optional, Sequence, Tuple, cast
from uuid import UUID, uuid4
from weakref import WeakKeyDictionary

from dynastore.models.protocols.indexing import (
    BulkIndexer,
    BulkIndexResult,
    IndexableOp,
)
from dynastore.models.tasks import TaskPayload
from dynastore.tasks.protocols import TaskProtocol
from dynastore.tasks.report import TaskReport
from dynastore.tasks.workclass_drain.single_flight import DrainSingleFlightGate
from dynastore.tools.db import validate_sql_identifier
from dynastore.tools.memory_trim import trim_malloc_arenas

logger = logging.getLogger(__name__)


# Per-attempt retry backoff in seconds.
# Index by ``attempts`` (0-based); the last entry caps the backoff at ~30 min.
_BACKOFF_SECONDS: List[int] = [1, 5, 30, 5 * 60, 30 * 60]

# Seconds before an in_flight row is considered stale and eligible for
# reclaim by any drain worker.
_DEFAULT_LEASE_SECONDS: int = 300

# Default claim batch size. Matches TasksPluginConfig.storage_drain_batch_size:
# it bounds ROW COUNT claimed per cycle (and the outer size of each id-only
# canonical-re-read group, #2494 P1) — see _DEFAULT_HYDRATION_BYTE_BUDGET
# below for the mechanism that actually bounds hydration memory (#2723).
# The previous default of 1500 OOM-killed 2Gi containers on MB-scale
# geometries before byte-budgeted sub-chunking existed.
_DEFAULT_BATCH_SIZE: int = 100

# Fallback size (bytes) attributed to a hydrated document that cannot be
# JSON-estimated (e.g. a non-JSON-serializable value slipped through). Large
# enough to force an isolating flush rather than silently accumulating an
# unmeasured object indefinitely.
_UNESTIMATED_DOC_BYTES: int = 8 * 1024 * 1024  # 8 MiB

# Default hydration byte budget (#2723). Matches
# TasksPluginConfig.storage_drain_hydration_byte_budget: bounds how much
# BUILT (hydrated) payload storage_drain accumulates before dispatching an
# index_bulk call, independent of storage_drain_batch_size (row count only).
# 16 MiB keeps a 2Gi container's peak hydration spike well under its memory
# limit even for MB-scale GAUL polygons, while still batching enough small
# documents per _bulk call for reasonable ES throughput.
_DEFAULT_HYDRATION_BYTE_BUDGET: int = 16 * 1024 * 1024

# Row-count CEILING on a single canonical-re-read SELECT for one id-only
# group (#2723): read_canonical_index_inputs batches ALL geoids handed to it
# into one query, materializing every row's raw geometry at once. A group
# can hold up to storage_drain_batch_size rows sharing one (catalog_id,
# collection_id) — for MB-scale GAUL polygons that alone can spike memory
# even though the group is already row-count-bounded. Chunk size within a
# group is ADAPTIVE (#3121, see _next_id_only_chunk_rows below): it starts
# at _ID_ONLY_READ_PROBE_ROWS and resizes from each chunk's measured
# hydrated byte cost, with this constant as the upper bound, so a single PG
# round trip's raw-row materialization — and asyncpg's json.loads decode
# transient on it, roughly an order of magnitude larger than the raw bytes —
# stays bounded regardless of per-row document size.
_ID_ONLY_READ_CHUNK_ROWS: int = 50

# First (probe) chunk size for an id-only group's adaptive re-read (#3121).
# Row sizes are unknown until the first chunk is hydrated, so the probe
# materializes exactly ONE row before the measured average takes over: a
# pathological row (a country-scale boundary's ST_AsGeoJSON output runs to
# tens of MB, and its json.loads decode transient roughly 10x that) must
# never be multiplied by a guessed row count the process cannot afford.
# The cost is one extra PG round trip per id-only group; the rows are
# hydrated either way.
_ID_ONLY_READ_PROBE_ROWS: int = 1

# Default in-process drain budget (#2732 step 4). Matches
# TasksPluginConfig.storage_drain_inprocess_max_bytes: the cumulative
# hydrated-document byte total one in-process StorageDrainTask.run() call
# will process before handing the remainder off to storage_drain_offload
# (see StorageDrainOffloadTask below), if the outbox has not already been
# drained to empty first.
_DEFAULT_INPROCESS_MAX_BYTES: int = 32 * 1024 * 1024

# Default in-process drain wall-clock budget (#2732 step 4), seconds. Matches
# TasksPluginConfig.storage_drain_inprocess_max_seconds — complements
# _DEFAULT_INPROCESS_MAX_BYTES so a run with many small documents (never
# crossing the byte budget) still hands off rather than holding the catalog
# API pod's request-serving capacity hostage indefinitely.
_DEFAULT_INPROCESS_MAX_SECONDS: float = 5.0


def _backoff(attempts: int) -> int:
    """Return the backoff in seconds for the given zero-based attempt count."""
    idx = min(attempts, len(_BACKOFF_SECONDS) - 1)
    return _BACKOFF_SECONDS[idx]


def _estimate_doc_bytes(doc: Dict[str, Any]) -> int:
    """Estimate a hydrated doc's wire size via the same JSON encoding the ES
    bulk indexer will eventually produce. Used only to decide sub-chunk
    flush boundaries (#2723) — not for exact accounting."""
    try:
        return len(json.dumps(doc, default=str).encode("utf-8"))
    except Exception:  # noqa: BLE001 — an unestimable doc still forces a flush
        return _UNESTIMATED_DOC_BYTES


def _next_id_only_chunk_rows(
    *, chunk_bytes: int, rows_read: int, byte_budget: int, current: int,
) -> int:
    """Size the next id-only re-read chunk from the previous chunk's measured
    hydrated byte cost (#3121).

    The OOM burst this bounds: one canonical re-read SELECT materializes
    every row's JSONB attributes and GeoJSON geometry through asyncpg's
    ``json.loads`` codec at once, a decode transient roughly an order of
    magnitude larger than the raw bytes — invisible to the hydration byte
    budget, which only measures BUILT documents after the fact. Hydrated
    document size tracks the decoded row size closely (same attributes, same
    GeoJSON), so the measured per-row average from the last chunk is a good
    predictor for the next: fit ``byte_budget`` worth of rows, floor 1 (an
    oversized single row is fetched alone rather than split), ceiling
    ``_ID_ONLY_READ_CHUNK_ROWS`` (the pre-#3121 fixed size).

    A chunk with no measurable cost (every geoid absent, or nothing read)
    carries ``current`` forward unchanged rather than guessing.
    """
    if rows_read <= 0 or chunk_bytes <= 0:
        return current
    avg_row_bytes = max(1, chunk_bytes // rows_read)
    return max(1, min(_ID_ONLY_READ_CHUNK_ROWS, byte_budget // avg_row_bytes))


# Per-event-loop storage-drain concurrency gates (#3121). Two storage_drain
# runs claimed and dispatched onto the same worker each materialize their
# own id-only decode transient; concurrent runs stack those spikes on one
# gunicorn worker's budget (observed: flat ~13% RSS to kernel OOM inside a
# single 60s metric sample while two drains ran). One gate per running loop
# — a worker process runs a single loop, so this is the per-process gate in
# production, shared by every task instance including
# StorageDrainOffloadTask — created lazily because an asyncio.Semaphore
# pins itself to the first loop that awaits it. Waiting runs are NOT lost
# work: rows stay claimed/fenced and the gated run drains whatever is still
# pending when it acquires.
_DRAIN_RUN_GATES: "WeakKeyDictionary[asyncio.AbstractEventLoop, asyncio.Semaphore]" = (
    WeakKeyDictionary()
)


def _drain_run_gate() -> asyncio.Semaphore:
    """Return this event loop's drain gate, creating it on first use."""
    loop = asyncio.get_running_loop()
    gate = _DRAIN_RUN_GATES.get(loop)
    if gate is None:
        gate = asyncio.Semaphore(1)
        _DRAIN_RUN_GATES[loop] = gate
    return gate


class StorageDrainTask(TaskProtocol):
    """One-shot drain for the global ``tasks.storage`` index outbox.

    Claims ready rows (and stale in_flight rows whose lease expired), fans
    them out to the appropriate ``BulkIndexer`` by ``driver_id``, and
    applies fenced terminal writes (done / retry / dead).  Drains to empty
    then exits; the dispatcher re-enters via NOTIFY.

    Routing: tier-agnostic (``affinity_tier = None``). Placement comes from
    the task routing config; with no override the default matrix routes a
    tier-less system task to the ``catalog`` tier — the service that
    co-locates the dispatcher and the secondary-write driver this drain pushes
    to (and where the legacy outbox drain already runs). An operator can
    repoint it via routing config without a code change.

    In-process-first placement (#2732 step 4): unlike ``EventDrainTask``,
    this task deliberately does NOT subclass
    ``dynastore.tasks.workclass_drain.AsyncWriteDrainTaskProtocol`` and does
    not carry its ``is_async_write_workclass`` marker — it always starts
    in-process, bounded by its own byte/wall-clock budget (see ``run()``),
    rather than unconditionally offloading. Once that budget is exhausted
    with backlog remaining, it hands the remainder off to
    :class:`StorageDrainOffloadTask`, which DOES carry the marker.
    """

    task_type: ClassVar[str] = "storage_drain"
    priority: int = 100
    affinity_tier: ClassVar[Optional[str]] = None
    # Explicit opt-out (see class docstring): a plain getattr(..., False) on
    # an absent attribute would have the same effect, but naming it here
    # documents the decision rather than leaving it to inference.
    is_async_write_workclass: ClassVar[bool] = False

    # In-process byte/wall-clock drain budget (#2732 step 4): True on the
    # base task, so ``run()`` self-escapes to ``storage_drain_offload`` once
    # the budget is exhausted with backlog remaining. ``StorageDrainOffloadTask``
    # overrides this to False — the offload variant always drains to empty.
    _inprocess_budget_enabled: ClassVar[bool] = True

    def __init__(
        self,
        app_state: object | None = None,
        *,
        batch_size: int = _DEFAULT_BATCH_SIZE,
        lease_seconds: int = _DEFAULT_LEASE_SECONDS,
        hydration_byte_budget: int = _DEFAULT_HYDRATION_BYTE_BUDGET,
        inprocess_max_bytes: int = _DEFAULT_INPROCESS_MAX_BYTES,
        inprocess_max_seconds: float = _DEFAULT_INPROCESS_MAX_SECONDS,
    ) -> None:
        self.app_state = app_state
        self.batch_size = batch_size
        self.lease_seconds = lease_seconds
        self.hydration_byte_budget = hydration_byte_budget
        self.inprocess_max_bytes = inprocess_max_bytes
        self.inprocess_max_seconds = inprocess_max_seconds
        # Cumulative hydrated bytes processed by the most recent drain_once()
        # call — a side channel that keeps drain_once's public return type
        # ``int`` (row count) unchanged for existing callers/tests, while
        # still letting run() read the byte cost of that batch (#2732 step 4).
        self._last_batch_bytes: int = 0
        # driver_id -> resolved BulkIndexer, memoised for this run.
        self._indexer_cache: Dict[str, BulkIndexer] = {}
        # catalog_id -> resolved known-fields set, memoised for this run
        # (#2494 P1 canonical re-read — shared by every id-only group under
        # the same catalog).
        self._known_fields_cache: Dict[str, Any] = {}
        # Split completion counters for this run (#2731): 'indexed' rows went
        # through a BulkIndexer and were reported passed; 'auto_done' rows were
        # resolved as done WITHOUT the indexer (canonical row verifiably absent
        # — a legitimate deleted-item skip); 'retried' rows were funnelled to
        # backoff for any reason (indexer error, unresolved driver, failed
        # canonical re-read, canonical doc build failure, an indexer-reported
        # transient result, or an indexer omission). Reset at the start of
        # every ``run()``; accumulated by ``drain_once()``.
        self._run_metrics: Dict[str, int] = {
            "indexed": 0, "auto_done": 0, "retried": 0,
        }

    async def run(self, payload: TaskPayload) -> TaskReport:
        """Drain ``tasks.storage``, then return.

        Serialized per worker process on its event loop's drain gate
        (``_drain_run_gate``, #3121): concurrent
        drain runs on one worker stack their hydration/decode memory spikes,
        which is what burst-OOMed the catalog pod. A run that arrives while
        another is active simply waits — its rows stay claimed and fenced,
        and it drains whatever is still pending once it acquires the gate.

        Loops ``drain_once()`` until it reports zero claimed rows (drain to
        empty) — the dispatcher re-enters via NOTIFY when new rows appear.

        In-process byte/wall-clock budget (#2732 step 4): when
        ``self._inprocess_budget_enabled`` (the base task; the
        ``storage_drain_offload`` subclass disables this and always drains to
        empty), the loop also tracks cumulative hydrated-document bytes
        (:attr:`_last_batch_bytes`, set by ``drain_once``) and elapsed
        wall-clock time. If either configured budget is crossed while rows
        still remain, the loop stops early and hands the remainder off to the
        marker-carrying ``storage_drain_offload`` task (see
        :class:`StorageDrainOffloadTask`) via :meth:`_handoff_to_offload_job`,
        rather than continuing to hold this request-serving pod's capacity
        indefinitely.

        Returns a :class:`~dynastore.tasks.report.TaskReport` so the runner
        persists structured metrics alongside the human-facing message
        (#1807 P2).  ``drain_once`` retains its ``int`` return type so internal
        callers and existing tests are unaffected.
        """
        gate = _drain_run_gate()
        if gate.locked():
            logger.info(
                "StorageDrainTask: another storage drain run is active in "
                "this process — waiting for it to finish."
            )
        async with gate:
            return await self._run_drain(payload)

    async def _run_drain(self, payload: TaskPayload) -> TaskReport:
        """Gate-held body of :meth:`run` — see its docstring."""
        from dynastore.modules.db_config.db_config import DBConfig
        from dynastore.modules.db_config.db_timeout_config import create_task_engine

        # One engine for the lifetime of this run — shared across all claim and
        # terminal-write statements so connection overhead is paid once, not
        # per-row. The factory normalizes the DSN (prefix + libpq ``sslmode=`` →
        # asyncpg ``ssl=``, so a Cloud SQL DSN doesn't raise "unexpected keyword
        # argument 'sslmode'"), carries the same lock_timeout /
        # idle_in_transaction_session_timeout the shared engine applies (#2749,
        # #2832) plus TCP keepalives (#3057), and is pooler-safe (#3081).
        engine = create_task_engine(DBConfig)
        # Stable owner_id for the lifetime of this run — used as the
        # ``claimed_by`` stamp and the CAS guard on terminal writes.
        owner_id = f"storage_drain:{uuid4()}"
        # Hot-reloaded claim size, resolved once per run — bounds ROW COUNT
        # claimed per cycle (#2726).
        batch_size = await self._resolve_batch_size()
        # Hot-reloaded hydration byte budget, resolved once per run — bounds
        # how much BUILT (hydrated) payload is held before an index_bulk
        # dispatch, independent of batch_size (#2723).
        hydration_byte_budget = await self._resolve_hydration_byte_budget()
        # Hot-reloaded in-process drain budget (#2732 step 4), resolved once
        # per run. Only consulted when ``_inprocess_budget_enabled`` (the
        # ``storage_drain_offload`` subclass never resolves or checks it).
        inprocess_max_bytes, inprocess_max_seconds = await self._resolve_inprocess_budget()
        # Reset the split counters for this run — drain_once accumulates
        # into self._run_metrics as it classifies each claimed batch (#2731).
        self._run_metrics = {"indexed": 0, "auto_done": 0, "retried": 0}
        # Cross-pod single-flight (#3144): at most one in-process storage
        # drain runs platform-wide, so a reclaim after lagging heartbeat
        # writes can no longer make two pods pay the same hydration transient
        # concurrently. Session-scoped advisory lock on a direct lane, held
        # for the whole run; fails open when no trustworthy lane exists. The
        # offload subclass never gates (``_inprocess_budget_enabled`` False).
        cross_pod_gate = (
            DrainSingleFlightGate("storage")
            if self._inprocess_budget_enabled
            else None
        )
        total = 0
        cumulative_bytes = 0
        start_time = time.monotonic()
        try:
            if cross_pod_gate is not None and not await cross_pod_gate.acquire():
                logger.info(
                    "StorageDrainTask: skipping in-process drain — another "
                    "in-process storage drain run holds the single-flight gate.",
                )
                return TaskReport.completed(
                    message=(
                        "storage drain skipped: another in-process storage "
                        "drain run is active"
                    ),
                    metrics={"drained": 0, **self._run_metrics},
                    correlation={"owner_id": owner_id},
                )
            # A live storage_drain_offload run already owns the backlog
            # (#3121): every byte this budgeted run would hydrate is a
            # redundant decode transient inside an API-serving container the
            # offload runner is about to process anyway — at a fraction of
            # this container's throughput. Skip without claiming a single
            # row; the offload lease expiring (completion, crash, reap)
            # re-opens the in-process fast path automatically. The offload
            # subclass never checks this (``_inprocess_budget_enabled`` is
            # False there), so the offload run can never fence itself out.
            if self._inprocess_budget_enabled and await self._offload_drain_is_active(
                engine
            ):
                logger.info(
                    "StorageDrainTask: skipping in-process drain — a live "
                    "storage_drain_offload run owns the backlog.",
                )
                return TaskReport.completed(
                    message=(
                        "storage drain skipped: a live storage_drain_offload "
                        "run owns the backlog"
                    ),
                    metrics={"drained": 0, **self._run_metrics},
                    correlation={"owner_id": owner_id},
                )
            while True:
                n = await self.drain_once(
                    engine=engine, owner_id=owner_id, batch_size=batch_size,
                    hydration_byte_budget=hydration_byte_budget,
                )
                total += n
                if n == 0:
                    break

                if self._inprocess_budget_enabled:
                    cumulative_bytes += self._last_batch_bytes
                    elapsed = time.monotonic() - start_time
                    over_bytes = (
                        inprocess_max_bytes > 0 and cumulative_bytes >= inprocess_max_bytes
                    )
                    over_seconds = (
                        inprocess_max_seconds > 0 and elapsed >= inprocess_max_seconds
                    )
                    if over_bytes or over_seconds:
                        await self._handoff_to_offload_job(engine)
                        break
        finally:
            if cross_pod_gate is not None:
                await cross_pod_gate.release()
            await engine.dispose()
            # The decode/hydration transients this run freed are retained in
            # glibc's malloc arenas — RSS stays pinned at the burst peak and
            # successive runs stack on top of it (#3121). Hand the pages back.
            trim_malloc_arenas()

        # 'drained' stays the total claimed count for backward compat; the
        # split counters distinguish rows actually written to the index from
        # rows resolved without ever reaching it, so a completion message can
        # no longer describe silently-dropped rows as uniformly "processed"
        # (#2731 — a drain once reported 6096 processed while ~5200 of them
        # were auto_done id-only rows whose canonical re-read had swallowed
        # an error, with zero WARNING/ERROR logs).
        report = TaskReport.completed(
            message=f"storage drain completed: {total} row(s) processed",
            metrics={"drained": total, **self._run_metrics},
            correlation={"owner_id": owner_id},
        )

        # Best-effort structured log via the catalog log manager.  No
        # catalog_id is available at the drain level (this task is global,
        # not tenant-scoped), so we skip the tenant log and rely on the
        # standard logger for observability.
        logger.info(
            "StorageDrainTask finished",
            extra={"task_report": report.log_details()},
        )

        return report

    async def _resolve_batch_size(self) -> int:
        """Resolve ``TasksPluginConfig.storage_drain_batch_size``, hot-reloaded.

        Mirrors the resolution pattern of
        ``index_dispatcher._storage_plane_routing_enabled``: falls back to
        the instance default (constructor value, itself matching the field
        default) when the platform configs protocol is unavailable — early
        startup, lightweight worker contexts, tests.
        """
        try:
            from dynastore.models.protocols.platform_configs import (
                PlatformConfigsProtocol,
            )
            from dynastore.modules.tasks.tasks_config import TasksPluginConfig
            from dynastore.tools.discovery import get_protocol

            config_mgr = get_protocol(PlatformConfigsProtocol)
            if config_mgr is not None:
                cfg = await config_mgr.get_config(TasksPluginConfig)
                if isinstance(cfg, TasksPluginConfig):
                    return int(cfg.storage_drain_batch_size)
        except Exception:  # noqa: BLE001 — config read is best-effort
            logger.debug(
                "StorageDrainTask: storage_drain_batch_size unavailable — "
                "falling back to the instance default (%d).",
                self.batch_size,
                exc_info=True,
            )
        return self.batch_size

    async def _resolve_hydration_byte_budget(self) -> int:
        """Resolve ``TasksPluginConfig.storage_drain_hydration_byte_budget``,
        hot-reloaded (#2723).

        Mirrors ``_resolve_batch_size``'s fallback pattern: falls back to the
        instance default (constructor value, itself matching the field
        default) when the platform configs protocol is unavailable — early
        startup, lightweight worker contexts, tests.
        """
        try:
            from dynastore.models.protocols.platform_configs import (
                PlatformConfigsProtocol,
            )
            from dynastore.modules.tasks.tasks_config import TasksPluginConfig
            from dynastore.tools.discovery import get_protocol

            config_mgr = get_protocol(PlatformConfigsProtocol)
            if config_mgr is not None:
                cfg = await config_mgr.get_config(TasksPluginConfig)
                if isinstance(cfg, TasksPluginConfig):
                    return int(cfg.storage_drain_hydration_byte_budget)
        except Exception:  # noqa: BLE001 — config read is best-effort
            logger.debug(
                "StorageDrainTask: storage_drain_hydration_byte_budget "
                "unavailable — falling back to the instance default (%d).",
                self.hydration_byte_budget,
                exc_info=True,
            )
        return self.hydration_byte_budget

    async def _resolve_inprocess_budget(self) -> Tuple[int, float]:
        """Resolve the ``TasksPluginConfig.storage_drain_inprocess_max_bytes``
        / ``storage_drain_inprocess_max_seconds`` pair, hot-reloaded (#2732
        step 4).

        Mirrors ``_resolve_batch_size``'s fallback pattern: falls back to the
        instance defaults (constructor values, themselves matching the field
        defaults) when the platform configs protocol is unavailable — early
        startup, lightweight worker contexts, tests.
        """
        try:
            from dynastore.models.protocols.platform_configs import (
                PlatformConfigsProtocol,
            )
            from dynastore.modules.tasks.tasks_config import TasksPluginConfig
            from dynastore.tools.discovery import get_protocol

            config_mgr = get_protocol(PlatformConfigsProtocol)
            if config_mgr is not None:
                cfg = await config_mgr.get_config(TasksPluginConfig)
                if isinstance(cfg, TasksPluginConfig):
                    return (
                        int(cfg.storage_drain_inprocess_max_bytes),
                        float(cfg.storage_drain_inprocess_max_seconds),
                    )
        except Exception:  # noqa: BLE001 — config read is best-effort
            logger.debug(
                "StorageDrainTask: storage_drain_inprocess_max_bytes/"
                "storage_drain_inprocess_max_seconds unavailable — falling "
                "back to the instance defaults (%d, %.1f).",
                self.inprocess_max_bytes,
                self.inprocess_max_seconds,
                exc_info=True,
            )
        return self.inprocess_max_bytes, self.inprocess_max_seconds

    async def _handoff_to_offload_job(self, engine: Any) -> None:
        """Enqueue the ``storage_drain_offload`` trigger (#2732 step 4).

        Called by ``run()`` once the in-process byte/wall-clock drain budget
        is exhausted with backlog rows still remaining. Uses its own dedup
        key so this handoff is never blocked by, and never blocks, a live
        ``storage_drain`` trigger — the two are independent triggers for the
        same underlying ``tasks.storage`` outbox, distinguished only by which
        task claims them next (in-process vs the offloaded job).
        """
        from dynastore.modules.db_config.query_executor import managed_transaction
        from dynastore.modules.storage.storage_emit import _enqueue_drain_trigger

        async with managed_transaction(engine) as conn:
            await _enqueue_drain_trigger(
                conn,
                task_type="storage_drain_offload",
                dedup_key="storage_drain_offload",
            )
        logger.info(
            "StorageDrainTask: in-process drain budget exhausted with "
            "backlog remaining — handed off remainder to the "
            "storage_drain_offload job.",
        )

    async def _offload_drain_is_active(self, engine: Any) -> bool:
        """True when a live ``storage_drain_offload`` run owns the backlog.

        "Live" mirrors the wedge-tolerance rule in
        ``storage_emit._enqueue_drain_trigger``: an ACTIVE trigger row whose
        claim lease (``locked_until``) has not yet expired. A wedged row —
        owner died mid-run, lease lapsed, reaper not yet caught up — does
        NOT count, so a crashed offload run can never permanently fence out
        the in-process path (#2715's lesson applied in reverse).

        Fail-open: any error reads as False so the in-process drain proceeds
        exactly as it did before this gate existed — environments without
        the tasks table (storage-only test fixtures) or with a transiently
        unreachable DB must not lose the drain that was about to run.
        """
        from dynastore.modules.db_config.query_executor import (
            DQLQuery,
            ResultHandler,
            managed_transaction,
        )
        from dynastore.modules.tasks.tasks_module import get_task_schema

        try:
            task_schema = get_task_schema()
            probe_sql = (
                f"SELECT 1 FROM {task_schema}.tasks"
                f" WHERE dedup_key = :dedup_key"
                f"   AND catalog_id = 'platform'"
                f"   AND status = 'ACTIVE'"
                f"   AND locked_until > now()"
                f" LIMIT 1"
            )
            async with managed_transaction(engine) as conn:
                row = await DQLQuery(
                    probe_sql,
                    result_handler=ResultHandler.SCALAR,
                ).execute(conn, dedup_key="storage_drain_offload")
            return row is not None
        except Exception:  # noqa: BLE001 — the gate is best-effort by design
            logger.debug(
                "StorageDrainTask: offload-liveness probe failed — "
                "proceeding with the in-process drain.",
                exc_info=True,
            )
            return False

    async def drain_once(
        self, *, engine: Any, owner_id: str, batch_size: Optional[int] = None,
        hydration_byte_budget: Optional[int] = None,
    ) -> int:
        """Claim one batch, process, apply fenced outcomes; return rows handled.

        ``batch_size`` overrides the instance default for this cycle (the
        hot-reloaded ``TasksPluginConfig.storage_drain_batch_size`` value,
        resolved once per run); ``None`` keeps ``self.batch_size`` so
        internal callers and existing tests are unaffected. ``hydration_byte_budget``
        does the same for ``TasksPluginConfig.storage_drain_hydration_byte_budget``
        (#2723) — the byte budget bounding hydrated-payload sub-chunks; ``None``
        keeps ``self.hydration_byte_budget``.

        Sub-chunk indexer exception: a sub-chunk that raises funnels only ITS
        rows to the retry path (#2723) — a flaky indexer can never lose data,
        and one bad sub-chunk no longer forces a retry of rows that would
        otherwise have indexed successfully.  Per-row poison classification
        is the indexer's responsibility.

        If a ``driver_id`` in the claimed batch cannot be resolved to a
        ``BulkIndexer``, those rows are treated as transient-retry so
        they are not dropped; they will be retried once a capable pod
        becomes available or the issue is resolved.
        """
        from dynastore.modules.tasks.tasks_module import get_task_schema

        task_schema = get_task_schema()
        validate_sql_identifier(task_schema)

        rows = await self._claim_batch(
            engine=engine,
            task_schema=task_schema,
            owner_id=owner_id,
            batch_size=batch_size if batch_size is not None else self.batch_size,
        )
        if not rows:
            self._last_batch_bytes = 0
            return 0

        effective_byte_budget = (
            hydration_byte_budget
            if hydration_byte_budget is not None
            else self.hydration_byte_budget
        )

        # Group claimed rows by driver_id for bulk dispatch.
        by_driver: Dict[str, List[Dict[str, Any]]] = {}
        for row in rows:
            by_driver.setdefault(row["driver_id"], []).append(row)

        # Per-batch classification counters (#2731): logged below and folded
        # into self._run_metrics so a drain's completion report can never
        # again describe auto_done/retried rows as uniformly "processed".
        batch_indexed = 0
        batch_auto_done = 0
        batch_retried = 0
        # Cumulative hydrated-document bytes across every driver in this
        # batch (#2732 step 4) — the run()-level in-process drain budget
        # signal, surfaced via self._last_batch_bytes below.
        batch_bytes = 0

        for driver_id, driver_rows in by_driver.items():
            indexer = await self._resolve_indexer(driver_id)
            if indexer is None:
                logger.warning(
                    "StorageDrainTask: driver_id=%r is not registered — "
                    "%d row(s) queued for retry. Registration is required: "
                    "the owning driver's module must be installed and "
                    "instantiated for this runtime's SCOPE so it registers "
                    "into the storage driver registry.",
                    driver_id,
                    len(driver_rows),
                )
                await self._apply_retry_all(
                    engine=engine,
                    task_schema=task_schema,
                    rows=driver_rows,
                    owner_id=owner_id,
                    error=f"indexer not registered: {driver_id}",
                )
                batch_retried += len(driver_rows)
                continue

            # Streams hydration + dispatch in byte-budgeted sub-chunks
            # (#2723) — never materializes more hydrated payload than
            # ``effective_byte_budget`` at once, regardless of how many
            # rows this driver claimed.
            counts = await self._process_driver_rows(
                engine=engine,
                task_schema=task_schema,
                driver_id=driver_id,
                indexer=indexer,
                driver_rows=driver_rows,
                owner_id=owner_id,
                byte_budget=effective_byte_budget,
            )
            batch_indexed += counts["indexed"]
            batch_auto_done += counts["auto_done"]
            batch_retried += counts["retried"]
            batch_bytes += counts.get("bytes", 0)

        self._run_metrics["indexed"] += batch_indexed
        self._run_metrics["auto_done"] += batch_auto_done
        self._run_metrics["retried"] += batch_retried
        # Side channel for run()'s in-process drain budget (#2732 step 4) —
        # drain_once's public return type stays the row count.
        self._last_batch_bytes = batch_bytes
        logger.info(
            "StorageDrainTask: batch claimed=%d indexed=%d auto_done=%d retried=%d",
            len(rows), batch_indexed, batch_auto_done, batch_retried,
        )

        return len(rows)

    # ------------------------------------------------------------------
    # Claim
    # ------------------------------------------------------------------

    async def _claim_batch(
        self,
        *,
        engine: Any,
        task_schema: str,
        owner_id: str,
        batch_size: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        """Claim a batch of ready/stale rows; return them as raw dicts.

        ``FOR UPDATE SKIP LOCKED`` lets multiple worker pods claim disjoint
        batches concurrently.  Bumps ``claim_version = claim_version + 1``
        on every (re)claim — this is the fence that prevents a stalled drain
        from finalising a row after it has been reclaimed by another worker.
        """
        from dynastore.modules.db_config.query_executor import (
            DQLQuery,
            ResultHandler,
            managed_transaction,
        )

        claim_sql = (
            f"WITH claimed AS ("
            f"    SELECT day, op_id"
            f"    FROM {task_schema}.storage"
            f"    WHERE (status = 'ready'     AND ready_at <= now())"
            f"       OR (status = 'in_flight' AND claimed_at < now() - make_interval(secs => :lease_seconds))"
            f"    ORDER BY ready_at, op_id"
            f"    LIMIT :batch_size"
            f"    FOR UPDATE SKIP LOCKED"
            f")"
            f" UPDATE {task_schema}.storage w"
            f" SET status = 'in_flight', claimed_at = now(), claimed_by = :owner_id,"
            f"     claim_version = w.claim_version + 1"
            f" FROM claimed"
            f" WHERE w.day = claimed.day AND w.op_id = claimed.op_id"
            f" RETURNING w.day, w.op_id, w.driver_id, w.catalog_id, w.collection_id,"
            f"           w.op, w.entity_id, w.write_id, w.idempotency_key,"
            f"           w.attempts, w.claim_version, w.claimed_by"
        )

        async with managed_transaction(engine) as conn:
            rows = await DQLQuery(
                claim_sql,
                result_handler=ResultHandler.ALL_DICTS,
            ).execute(
                conn,
                lease_seconds=self.lease_seconds,
                batch_size=batch_size if batch_size is not None else self.batch_size,
                owner_id=owner_id,
            )

        return rows or []

    # ------------------------------------------------------------------
    # Indexer resolution
    # ------------------------------------------------------------------

    async def _resolve_indexer(self, driver_id: str) -> Optional[BulkIndexer]:
        """Resolve a :class:`BulkIndexer` for ``driver_id``; cached per run.

        Resolution is config-scoped via the storage driver registry
        (``DriverRegistry.collection_index()``), which is populated from
        per-collection routing configs at startup.  The snake_case ``driver_id``
        stamped on each ``tasks.storage`` row matches the registry key
        (``_to_snake(type(driver).__name__)``), so any driver registered under
        its config-scoped id resolves automatically — the ES adapter is just
        one concrete implementation.

        The drain MUST use the :class:`BulkIndexer` protocol
        (``index_bulk(ops) -> BulkIndexResult``), NOT the distinct
        :class:`Indexer` protocol (``index_bulk(ctx, ops) -> BulkResult``) —
        they are different types with different signatures.

        Resolution order
        ----------------
        1. Per-run cache (``self._indexer_cache``) — avoids repeated registry
           lookups within a single drain cycle.
        2. ``DriverRegistry.collection_index().get(driver_id)`` — the process-
           wide L0 registry built from protocol discovery at startup.  Covers
           any registered ``CollectionItemsStore`` driver, not just ES.
        3. ``DriverRegistry.asset_index().get(driver_id)`` — fallback for
           drivers that live in the asset tier rather than the items tier.

        There is no construction fallback: a driver that is not registered
        (module not installed for this runtime's SCOPE, or discovery/lifespan
        hasn't populated it yet) resolves to ``None``. Registration is the
        single, symmetric contract every storage driver must satisfy to
        participate in drain — see module entry points under
        ``[project.entry-points."dynastore.modules"]`` (e.g.
        ``storage_elasticsearch = "...:ItemsElasticsearchDriver"``).

        ``driver_id``\\s that cannot be resolved through any of the above paths
        return ``None``; the caller funnels those rows to retry — they are
        never dropped.

        Config-scope gating (grouping rows by ``(catalog_id, collection_id,
        driver_id)`` and skipping rows for driver_ids not present in that
        collection's resolved WRITE drivers) is deferred.  The live-PG dispatch
        tests seed rows in a throwaway schema with no routing config, so a hard
        gate would break them.  See issue #1807 (P1.3) to add the gate once
        test fixtures carry per-collection routing config or the gate is guarded
        on a non-empty ``resolve_drivers`` result.
        """
        cached = self._indexer_cache.get(driver_id)
        if cached is not None:
            return cached

        from dynastore.modules.storage.drivers.elasticsearch import (
            ItemsElasticsearchDriver,
        )
        from dynastore.modules.storage.driver_registry import DriverRegistry
        from dynastore.tasks.workclass_drain.es_indexer_adapter import ESBulkIndexer

        # --- Registry-driven resolution ---
        driver = (
            DriverRegistry.collection_index().get(driver_id)
            or DriverRegistry.asset_index().get(driver_id)
        )

        if driver is not None:
            if isinstance(driver, ItemsElasticsearchDriver):
                if not driver.is_available():
                    logger.warning(
                        "StorageDrainTask: ES driver unavailable (opensearch-py "
                        "missing from worker extras) — rows for driver_id=%r will "
                        "retry until a capable pod drains them.",
                        driver_id,
                    )
                    return None
                indexer = cast(BulkIndexer, ESBulkIndexer(driver))
                self._indexer_cache[driver_id] = indexer
                return indexer
            # Any other driver found in the registry but without a known
            # BulkIndexer adapter yet: treat as unresolved (retry).  Adding
            # new adapters here as they are developed will extend support
            # without touching the drain loop.
            logger.debug(
                "StorageDrainTask: driver_id=%r resolved from registry but "
                "no BulkIndexer adapter is registered for type %r — rows will retry.",
                driver_id,
                type(driver).__name__,
            )
            return None

        # Unregistered: no construction fallback. The driver's module must
        # register itself (its entry point instantiated for this runtime's
        # SCOPE) for drain to find it — see the resolution-order note above.
        return None

    # ------------------------------------------------------------------
    # Row-to-op conversion
    # ------------------------------------------------------------------

    @staticmethod
    def _row_to_op(row: Dict[str, Any]) -> IndexableOp:
        """Convert a raw ``tasks.storage`` row dict to an ``IndexableOp``.

        ``driver_instance_id`` is derived deterministically from the
        ``(driver_id, catalog_id, collection_id)`` triple — the ``tasks.storage``
        table has no ``driver_instance_id`` column; it is derived here.

        Column mapping: ``entity_id`` (storage table) → ``IndexableOp.item_id``.
        # TODO(#1807 P1.3): branch on entity_kind for collection/catalog/asset tiers
        """
        from dynastore.modules.storage.driver_instance_id import (
            compute_driver_instance_id,
        )

        catalog_id = row["catalog_id"]
        collection_id = row.get("collection_id") or ""
        driver_id = row["driver_id"]

        return IndexableOp(
            op_id=UUID(str(row["op_id"])),
            op=row["op"],  # type: ignore[arg-type]
            catalog_id=catalog_id,
            collection_id=collection_id,
            driver_instance_id=compute_driver_instance_id(
                driver_id, catalog_id, collection_id,
            ),
            item_id=row.get("entity_id"),  # entity_id column → IndexableOp.item_id
            payload={},  # tasks.storage carries no payloads — ids only
            idempotency_key=row.get("idempotency_key") or "",
        )

    # ------------------------------------------------------------------
    # Canonical re-read for id-only rows (#2494 P1)
    # ------------------------------------------------------------------

    async def _process_driver_rows(
        self,
        *,
        engine: Any,
        task_schema: str,
        driver_id: str,
        indexer: BulkIndexer,
        driver_rows: List[Dict[str, Any]],
        owner_id: str,
        byte_budget: int,
    ) -> Dict[str, int]:
        """Hydrate and dispatch one driver's claimed rows in byte-budgeted
        sub-chunks (#2723).

        Streams rather than batch-building: every row is hydrated to one or
        more ``IndexableOp``\\s (delete rows directly; id-only upsert rows
        via the canonical re-read + doc build below, #2494 P1; write-id
        rows via primary-driver chunk reads) and appended to a pending
        sub-chunk. As soon as the pending sub-chunk's estimated
        JSON-encoded size reaches ``byte_budget``, it is dispatched to
        ``indexer.index_bulk`` and its outcomes applied immediately —
        BEFORE any further row in this driver's claimed batch is hydrated.
        Peak resident hydrated payload is therefore bounded by
        ``byte_budget``, independent of how many rows were claimed
        (``storage_drain_batch_size``, #2726 — row count only) or how many
        MB-scale documents (e.g. GAUL polygons) they hydrate to.

        ``tasks.storage`` carries no payloads, so classification is
        structural: a row with ``write_id`` set references a whole primary
        write batch (hydrated via the primary driver's write-id chunk
        reads); otherwise ``entity_id`` must be set — ``op='delete'`` rows
        convert to ``IndexableOp`` directly (:meth:`_row_to_op`; an id is
        all the indexer needs), and ``op='upsert'`` rows are id-only
        obligations written by ``IndexDispatcher._enqueue_storage_plane_ids``
        when ``TasksPluginConfig.items_secondary_via_storage_plane`` is
        enabled, grouped by ``(catalog_id, collection_id)``. A row with
        neither ``write_id`` nor ``entity_id`` can never hydrate and is
        marked dead. Each id-only group's canonical re-read
        (:func:`read_canonical_index_inputs`) stays batched, but in
        byte-adaptive read chunks (#3121, ``_next_id_only_chunk_rows``;
        row-count ceiling ``_ID_ONLY_READ_CHUNK_ROWS``) rather than the
        whole group at once, so a single PG round trip never materializes
        an unbounded number of raw (pre-JSON) geometry rows either:

        * a resolved geoid becomes an ``IndexableOp`` carrying the
          freshly-built canonical document;
        * a geoid absent from PG is treated as a deleted item and marked
          done directly (last-write-wins — the item is gone, indexing it
          would be wrong and re-trying it would never succeed);
        * a read chunk whose re-read itself raises funnels every id-only
          row in that chunk to retry (a transient infra failure, not a
          poison classification — the geoid's existence is simply
          unknown).

        Crash/partial-failure safety: a sub-chunk that fails
        ``index_bulk`` funnels only ITS rows to retry; rows in a sub-chunk
        already flushed keep their (already applied) outcome, and rows not
        yet reached when this coroutine is cancelled or the process dies
        simply stay claimed 'in_flight' — fenced by (claimed_by,
        claim_version) — until the lease expires and a future drain cycle
        reclaims and re-hydrates them. No row is ever double marked-done
        (the CAS fence in :meth:`_mark_done`/:meth:`_mark_retry`) or
        silently skipped (an unflushed row is simply never marked, so it
        falls back to the normal lease-expiry reclaim path).

        Returns ``{"indexed": n, "auto_done": n, "retried": n, "bytes": n}`` —
        the same split classification as the pre-#2723 single-shot pipeline,
        plus the cumulative hydrated-document byte total (#2732 step 4) for
        run()'s in-process drain budget, independent of sub-chunk flush
        boundaries.
        """
        counts = {"indexed": 0, "auto_done": 0, "retried": 0, "bytes": 0}

        pending_ops: List[IndexableOp] = []
        pending_rows: List[Dict[str, Any]] = []
        pending_bytes = 0

        async def _flush() -> None:
            nonlocal pending_ops, pending_rows, pending_bytes
            if not pending_ops:
                return
            ops, rows = pending_ops, pending_rows
            pending_ops, pending_rows, pending_bytes = [], [], 0
            try:
                result = await indexer.index_bulk(ops)
            except Exception as exc:  # noqa: BLE001 — surface every failure
                logger.warning(
                    "StorageDrainTask[%s]: sub-chunk error (%d row(s)): %s",
                    driver_id, len(rows), exc,
                )
                await self._apply_retry_all(
                    engine=engine, task_schema=task_schema, rows=rows,
                    owner_id=owner_id, error=str(exc),
                )
                counts["retried"] += len(rows)
                return
            outcome_counts = await self._apply_outcomes(
                engine=engine, task_schema=task_schema, rows=rows,
                result=result, owner_id=owner_id,
            )
            counts["indexed"] += outcome_counts["indexed"]
            counts["retried"] += outcome_counts["retried"]

        async def _append(
            op: IndexableOp, row: Dict[str, Any], doc: Dict[str, Any],
        ) -> None:
            nonlocal pending_bytes
            doc_bytes = _estimate_doc_bytes(doc)
            pending_ops.append(op)
            pending_rows.append(row)
            pending_bytes += doc_bytes
            counts["bytes"] += doc_bytes
            if pending_bytes >= byte_budget:
                await _flush()

        id_only_by_group: Dict[Tuple[str, str], List[Dict[str, Any]]] = {}
        write_id_by_group: Dict[Tuple[str, str, str, str], List[Dict[str, Any]]] = {}
        for row in driver_rows:
            # Structural classification — ``tasks.storage`` carries no
            # payloads. ``write_id`` set means a write-id batch reference;
            # otherwise ``entity_id`` must be set (id-only upsert re-read,
            # or a direct delete). A row with neither can never hydrate.
            write_id = row.get("write_id")
            if (
                row["op"] in {"upsert", "delete"}
                and isinstance(write_id, str)
                and write_id
            ):
                key = (
                    row["catalog_id"],
                    row.get("collection_id") or "",
                    row["op"],
                    write_id,
                )
                write_id_by_group.setdefault(key, []).append(row)
                continue
            if not row.get("entity_id"):
                logger.warning(
                    "StorageDrainTask: driver_id=%r row op_id=%s (%s) has "
                    "neither write_id nor entity_id — unhydratable, marking "
                    "dead.",
                    driver_id, row.get("op_id"), row["op"],
                )
                await self._mark_dead(
                    engine=engine,
                    task_schema=task_schema,
                    row=row,
                    owner_id=owner_id,
                )
                continue
            if row["op"] == "upsert":
                key = (row["catalog_id"], row.get("collection_id") or "")
                id_only_by_group.setdefault(key, []).append(row)
                continue
            # Delete rows replay directly — an id is all the indexer needs.
            await _append(self._row_to_op(row), row, {})

        for (catalog_id, collection_id, op, write_id), group_rows in write_id_by_group.items():
            ledger_row = group_rows[-1]
            try:
                hydrated_ops = list(
                    await self._read_primary_write_batch(
                        catalog_id=catalog_id,
                        collection_id=collection_id,
                        driver_id=driver_id,
                        write_id=write_id,
                        op=op,
                        engine=engine,
                    ),
                )
            except Exception as exc:  # noqa: BLE001 — missing protocol / lookup errors retry
                logger.warning(
                    "StorageDrainTask: write-id hydration failed for %s/%s "
                    "(driver=%s write_id=%s): %s — funnelling row to retry.",
                    catalog_id, collection_id, driver_id, write_id, exc,
                )
                await self._mark_retry(
                    engine=engine,
                    task_schema=task_schema,
                    row=ledger_row,
                    owner_id=owner_id,
                    error=f"write_id_hydration_failed: {exc}",
                )
                counts["retried"] += 1
                continue

            if not hydrated_ops:
                await self._mark_done(
                    engine=engine,
                    task_schema=task_schema,
                    row=ledger_row,
                    owner_id=owner_id,
                )
                counts["auto_done"] += 1
                continue

            pending_chunk: List[IndexableOp] = []
            pending_chunk_bytes = 0

            async def _flush_write_id_chunk() -> tuple[bool, Optional[str], Optional[str]]:
                nonlocal pending_chunk, pending_chunk_bytes
                if not pending_chunk:
                    return True, None, None
                ops_chunk = pending_chunk
                pending_chunk, pending_chunk_bytes = [], 0
                try:
                    result = await indexer.index_bulk(ops_chunk)
                except Exception as exc:  # noqa: BLE001 — retry whole ledger row
                    return False, "retry", str(exc)
                if result.transient:
                    return False, "retry", result.transient[0][1]
                if result.poison:
                    return False, "dead", result.poison[0][1]
                if len(result.passed) != len(ops_chunk):
                    return False, "retry", (
                        "indexer omitted op_id from grouped write_id batch"
                    )
                return True, None, None

            outcome: tuple[str, Optional[str]] = ("done", None)
            for op_row in hydrated_ops:
                doc_bytes = _estimate_doc_bytes(op_row.payload)
                counts["bytes"] += doc_bytes
                pending_chunk.append(op_row)
                pending_chunk_bytes += doc_bytes
                if pending_chunk_bytes >= byte_budget:
                    ok, action, reason = await _flush_write_id_chunk()
                    if not ok:
                        outcome = (cast(str, action), reason)
                        break
            else:
                ok, action, reason = await _flush_write_id_chunk()
                if not ok:
                    outcome = (cast(str, action), reason)

            if outcome[0] == "done":
                await self._mark_done(
                    engine=engine,
                    task_schema=task_schema,
                    row=ledger_row,
                    owner_id=owner_id,
                )
                counts["indexed"] += 1
            elif outcome[0] == "dead":
                logger.error(
                    "StorageDrainTask: write-id batch poison failure for "
                    "%s/%s (driver=%s write_id=%s): %s — marking ledger row "
                    "dead.",
                    catalog_id, collection_id, driver_id, write_id,
                    outcome[1] or "unclassified poison failure",
                )
                await self._mark_dead(
                    engine=engine,
                    task_schema=task_schema,
                    row=ledger_row,
                    owner_id=owner_id,
                )
            else:
                await self._mark_retry(
                    engine=engine,
                    task_schema=task_schema,
                    row=ledger_row,
                    owner_id=owner_id,
                    error=outcome[1] or "grouped write-id batch failed",
                )
                counts["retried"] += 1

        for (catalog_id, collection_id), group_rows in id_only_by_group.items():
            group_auto_done = 0
            # Adaptive re-read chunking (#3121): start each group with a
            # small probe, then let the measured hydrated byte cost of each
            # chunk size the next via _next_id_only_chunk_rows — bounding the
            # raw-row decode transient a single SELECT can materialize.
            row_idx = 0
            chunk_rows = min(_ID_ONLY_READ_PROBE_ROWS, _ID_ONLY_READ_CHUNK_ROWS)
            while row_idx < len(group_rows):
                row_chunk = group_rows[row_idx : row_idx + chunk_rows]
                row_idx += len(row_chunk)
                bytes_before_chunk = counts["bytes"]
                geoids = [r["entity_id"] for r in row_chunk if r.get("entity_id")]
                try:
                    inputs = await self._read_canonical_inputs(
                        engine=engine, catalog_id=catalog_id,
                        collection_id=collection_id, geoids=geoids,
                    )
                except Exception as exc:  # noqa: BLE001 — funnel to retry, never drop
                    logger.warning(
                        "StorageDrainTask: canonical re-read failed for %s/%s "
                        "(%d id-only row(s)): %s — funnelling to retry.",
                        catalog_id, collection_id, len(row_chunk), exc,
                    )
                    for row in row_chunk:
                        await self._mark_retry(
                            engine=engine, task_schema=task_schema, row=row,
                            owner_id=owner_id,
                            error=f"canonical_reread_failed: {exc}",
                        )
                    counts["retried"] += len(row_chunk)
                    continue

                for row in row_chunk:
                    geoid = row.get("entity_id")
                    ci = inputs.get(geoid) if geoid else None
                    if ci is None:
                        # No PG row — item deleted after the obligation was
                        # enqueued (or the geoid never existed). Skip as
                        # success rather than indexing a stale/absent doc.
                        await self._mark_done(
                            engine=engine, task_schema=task_schema,
                            row=row, owner_id=owner_id,
                        )
                        counts["auto_done"] += 1
                        group_auto_done += 1
                        continue
                    try:
                        doc = await self._build_canonical_doc(
                            catalog_id=catalog_id, collection_id=collection_id, ci=ci,
                        )
                    except Exception as exc:  # noqa: BLE001 — funnel to retry
                        logger.warning(
                            "StorageDrainTask: canonical doc build failed for "
                            "%s/%s/%s: %s — funnelling to retry.",
                            catalog_id, collection_id, geoid, exc,
                        )
                        await self._mark_retry(
                            engine=engine, task_schema=task_schema, row=row,
                            owner_id=owner_id,
                            error=f"canonical_doc_build_failed: {exc}",
                        )
                        counts["retried"] += 1
                        continue
                    op = _dataclass_replace(self._row_to_op(row), payload=doc)
                    await _append(op, row, doc)

                chunk_rows = _next_id_only_chunk_rows(
                    chunk_bytes=counts["bytes"] - bytes_before_chunk,
                    rows_read=len(row_chunk),
                    byte_budget=byte_budget,
                    current=chunk_rows,
                )

            # Observability (#2731): auto_done is a legitimate outcome (the
            # canonical row is verifiably absent, not merely unreadable), but
            # it must never be silent — the drain that lost ~5200 items on
            # 2026-07-02 produced zero WARNING/ERROR logs for the entire run.
            if group_auto_done:
                logger.info(
                    "StorageDrainTask: %d id-only row(s) auto_done (canonical "
                    "row absent) for %s/%s",
                    group_auto_done, catalog_id, collection_id,
                )

        await _flush()
        return counts

    async def _read_canonical_inputs(
        self, *, engine: Any, catalog_id: str, collection_id: str,
        geoids: List[str],
    ) -> Dict[str, Any]:
        """Batch-read canonical PG state for ``geoids`` (test seam)."""
        from dynastore.modules.catalog.canonical_index_read import (
            read_canonical_index_inputs,
        )

        return await read_canonical_index_inputs(
            catalog_id, collection_id, geoids, db_resource=engine,
        )

    async def _build_canonical_doc(
        self, *, catalog_id: str, collection_id: str, ci: Any,
    ) -> Dict[str, Any]:
        """Assemble the canonical ES doc for one re-read row (test seam).

        ``known_fields`` is per-catalog and memoised for the lifetime of
        this drain run — every id-only group under the same catalog shares
        it instead of re-resolving per group.

        Couples this generically-named drain task to the ES-shaped
        canonical envelope (``build_canonical_index_doc``); today the only
        ``BulkIndexer`` the drain resolves is the ES adapter
        (``items_elasticsearch_driver``), so this is not a behaviour change —
        a future non-ES ``BulkIndexer`` would need its own re-read
        strategy here.
        """
        from dynastore.modules.elasticsearch.canonical_doc import (
            build_canonical_index_doc,
        )

        known_fields = self._known_fields_cache.get(catalog_id)
        if known_fields is None:
            from dynastore.modules.elasticsearch.items_projection import (
                resolve_catalog_known_fields,
            )

            known_fields = await resolve_catalog_known_fields(catalog_id)
            self._known_fields_cache[catalog_id] = known_fields

        return build_canonical_index_doc(
            ci.row,
            resolved_sidecars=ci.resolved_sidecars,
            known_fields=known_fields,
            catalog_id=catalog_id,
            collection_id=collection_id,
            geometry=ci.geometry,
            bbox=ci.bbox,
            user_properties=ci.user_properties,
            access=ci.access,
            stac_reserved_members=ci.stac_reserved_members,
        )

    async def _read_primary_write_batch(
        self,
        *,
        catalog_id: str,
        collection_id: str,
        driver_id: str,
        write_id: str,
        op: str,
        engine: Optional[Any] = None,
    ) -> Sequence[IndexableOp]:
        primary = await self._resolve_primary_write_source(
            catalog_id=catalog_id,
            collection_id=collection_id,
        )
        if primary is None:
            raise LookupError("primary driver does not expose write-id chunk reads")
        reader: Any = getattr(primary, "read_indexable_write_batch", None)
        if reader is not None:
            return await reader(
                catalog_id=catalog_id,
                collection_id=collection_id,
                write_id=write_id,
                target_driver_id=driver_id,
                op=op,
            )

        from dynastore.modules.storage.driver_instance_id import (
            compute_driver_instance_id,
        )

        driver_instance_id = compute_driver_instance_id(
            driver_id, catalog_id, collection_id,
        )
        ops: List[IndexableOp] = []
        after_geoid: Optional[str] = None

        if op == "upsert":
            active_reader: Any = getattr(primary, "read_active_rows_by_write_id", None)
            if active_reader is None:
                raise LookupError(
                    "primary driver does not expose read_active_rows_by_write_id"
                )
            while True:
                rows, next_after = await active_reader(
                    catalog_id,
                    collection_id,
                    write_id=write_id,
                    limit=_ID_ONLY_READ_CHUNK_ROWS,
                    after_geoid=after_geoid,
                    db_resource=engine,
                )
                geoids = [str(row["geoid"]) for row in rows if row.get("geoid")]
                if geoids:
                    inputs = await self._read_canonical_inputs(
                        engine=engine,
                        catalog_id=catalog_id,
                        collection_id=collection_id,
                        geoids=geoids,
                    )
                    for geoid in geoids:
                        ci = inputs.get(geoid)
                        if ci is None:
                            raise LookupError(
                                f"canonical row missing for write_id={write_id} geoid={geoid}"
                            )
                        doc = await self._build_canonical_doc(
                            catalog_id=catalog_id,
                            collection_id=collection_id,
                            ci=ci,
                        )
                        ops.append(IndexableOp(
                            op_id=uuid4(),
                            op="upsert",
                            catalog_id=catalog_id,
                            collection_id=collection_id,
                            driver_instance_id=driver_instance_id,
                            item_id=geoid,
                            payload=doc,
                            idempotency_key=geoid,
                        ))
                if next_after is None:
                    break
                after_geoid = next_after
            return ops

        if op == "delete":
            tombstone_reader: Any = getattr(
                primary, "read_tombstoned_ids_by_write_id", None,
            )
            if tombstone_reader is None:
                raise LookupError(
                    "primary driver does not expose read_tombstoned_ids_by_write_id"
                )
            while True:
                ids, next_after = await tombstone_reader(
                    catalog_id,
                    collection_id,
                    write_id=write_id,
                    limit=_ID_ONLY_READ_CHUNK_ROWS,
                    after_geoid=after_geoid,
                    db_resource=engine,
                )
                for geoid in ids:
                    gid = str(geoid)
                    ops.append(IndexableOp(
                        op_id=uuid4(),
                        op="delete",
                        catalog_id=catalog_id,
                        collection_id=collection_id,
                        driver_instance_id=driver_instance_id,
                        item_id=gid,
                        payload={},
                        idempotency_key=gid,
                    ))
                if next_after is None:
                    break
                after_geoid = next_after
            return ops

        raise ValueError(f"unsupported write-id op: {op}")

    async def _resolve_primary_write_source(
        self,
        *,
        catalog_id: str,
        collection_id: str,
    ) -> Optional[Any]:
        from dynastore.modules.storage.router import get_write_drivers

        try:
            resolved = await get_write_drivers(catalog_id, collection_id)
        except Exception as exc:  # noqa: BLE001 — routing unavailability retries the row
            logger.debug(
                "StorageDrainTask: primary-driver lookup failed for %s/%s: %s",
                catalog_id, collection_id, exc,
            )
            return None
        if not resolved:
            return None
        primary = getattr(resolved[0], "driver", None)
        if primary is None:
            return None
        return primary

    # ------------------------------------------------------------------
    # Outcome application
    # ------------------------------------------------------------------

    async def _apply_outcomes(
        self,
        *,
        engine: Any,
        task_schema: str,
        rows: Sequence[Dict[str, Any]],
        result: BulkIndexResult,
        owner_id: str,
    ) -> Dict[str, int]:
        """Partition rows per BulkIndexResult and apply fenced mark_*.

        Returns ``{"indexed": n, "retried": n}`` — counts of rows actually
        marked done via the indexer path / marked for retry, for the
        per-batch classification summary (#2731). Poison (dead) rows are not
        included; ``TaskReport.metrics`` only splits indexed/auto_done/retried.
        """
        rows_by_id: Dict[UUID, Dict[str, Any]] = {
            UUID(str(r["op_id"])): r for r in rows
        }
        indexed_count = 0
        retried_count = 0

        if result.passed:
            for op_id in result.passed:
                row = rows_by_id.get(op_id)
                if row is not None:
                    await self._mark_done(
                        engine=engine,
                        task_schema=task_schema,
                        row=row,
                        owner_id=owner_id,
                    )
                    indexed_count += 1

        if result.transient:
            for op_id, reason in result.transient:
                row = rows_by_id.get(op_id)
                if row is not None:
                    await self._mark_retry(
                        engine=engine,
                        task_schema=task_schema,
                        row=row,
                        owner_id=owner_id,
                        error=reason,
                    )
                    retried_count += 1

        if result.poison:
            for op_id, _reason in result.poison:
                row = rows_by_id.get(op_id)
                if row is not None:
                    await self._mark_dead(
                        engine=engine,
                        task_schema=task_schema,
                        row=row,
                        owner_id=owner_id,
                    )

        # Defence-in-depth: any claimed op_id the indexer omitted from all
        # three result lists would otherwise sit 'in_flight' until its lease
        # expires (up to ``lease_seconds``). Funnel those to retry so a
        # partial/buggy BulkIndexResult can never strand rows. A well-behaved
        # indexer reports every op, so this is normally a no-op.
        categorized: set[UUID] = set(result.passed)
        categorized.update(op_id for op_id, _ in result.transient)
        categorized.update(op_id for op_id, _ in result.poison)
        for op_id, row in rows_by_id.items():
            if op_id not in categorized:
                await self._mark_retry(
                    engine=engine,
                    task_schema=task_schema,
                    row=row,
                    owner_id=owner_id,
                    error="indexer omitted op_id from BulkIndexResult",
                )
                retried_count += 1

        return {"indexed": indexed_count, "retried": retried_count}

    async def _apply_retry_all(
        self,
        *,
        engine: Any,
        task_schema: str,
        rows: Sequence[Dict[str, Any]],
        owner_id: str,
        error: str,
    ) -> None:
        """Funnel all rows in the batch to retry (whole-batch error path)."""
        for row in rows:
            await self._mark_retry(
                engine=engine,
                task_schema=task_schema,
                row=row,
                owner_id=owner_id,
                error=error,
            )

    # ------------------------------------------------------------------
    # Fenced terminal writes (CAS on claimed_by + claim_version)
    # ------------------------------------------------------------------

    async def _mark_done(
        self,
        *,
        engine: Any,
        task_schema: str,
        row: Dict[str, Any],
        owner_id: str,
    ) -> None:
        """Mark a row done; CAS on (claimed_by, claim_version).

        If another worker reclaimed the row (bumping claim_version), this
        UPDATE matches 0 rows — the stale drain's finalization is a no-op.
        """
        from dynastore.modules.db_config.query_executor import (
            DQLQuery, ResultHandler, managed_transaction,
        )

        sql = (
            f"UPDATE {task_schema}.storage"
            f" SET status='done', finished_at=now()"
            f" WHERE day=:day AND op_id=:op_id"
            f"   AND claimed_by=:owner_id AND claim_version=:claim_version"
        )
        async with managed_transaction(engine) as conn:
            await DQLQuery(sql, result_handler=ResultHandler.NONE).execute(
                conn,
                day=row["day"],
                op_id=str(row["op_id"]),
                owner_id=owner_id,
                claim_version=row["claim_version"],
            )

    async def _mark_retry(
        self,
        *,
        engine: Any,
        task_schema: str,
        row: Dict[str, Any],
        owner_id: str,
        error: str,
    ) -> None:
        """Mark a row for retry with backoff; CAS on (claimed_by, claim_version).

        Bumps ``attempts`` here (not at claim) and pushes ``ready_at`` into
        the future by the backoff curve keyed on the current attempt count.
        If the CAS predicate misses (stale claim), the row is already owned
        by another worker — this is a safe no-op.
        """
        from dynastore.modules.db_config.query_executor import (
            DQLQuery, ResultHandler, managed_transaction,
        )

        attempts = int(row.get("attempts") or 0)
        backoff = _backoff(attempts)
        sql = (
            f"UPDATE {task_schema}.storage"
            f" SET status='ready', attempts=attempts+1,"
            f"     claimed_by=NULL, claimed_at=NULL,"
            f"     ready_at = now() + make_interval(secs => :backoff_seconds)"
            f" WHERE day=:day AND op_id=:op_id"
            f"   AND claimed_by=:owner_id AND claim_version=:claim_version"
        )
        async with managed_transaction(engine) as conn:
            await DQLQuery(sql, result_handler=ResultHandler.NONE).execute(
                conn,
                day=row["day"],
                op_id=str(row["op_id"]),
                owner_id=owner_id,
                claim_version=row["claim_version"],
                backoff_seconds=backoff,
            )

        logger.debug(
            "StorageDrainTask: retry op_id=%s attempts+1=%d backoff=%ds error=%r",
            row["op_id"],
            attempts + 1,
            backoff,
            error,
        )

    async def _mark_dead(
        self,
        *,
        engine: Any,
        task_schema: str,
        row: Dict[str, Any],
        owner_id: str,
    ) -> None:
        """Mark a poison row as dead (terminal); CAS on (claimed_by, claim_version)."""
        from dynastore.modules.db_config.query_executor import (
            DQLQuery, ResultHandler, managed_transaction,
        )

        sql = (
            f"UPDATE {task_schema}.storage"
            f" SET status='dead', finished_at=now()"
            f" WHERE day=:day AND op_id=:op_id"
            f"   AND claimed_by=:owner_id AND claim_version=:claim_version"
        )
        async with managed_transaction(engine) as conn:
            await DQLQuery(sql, result_handler=ResultHandler.NONE).execute(
                conn,
                day=row["day"],
                op_id=str(row["op_id"]),
                owner_id=owner_id,
                claim_version=row["claim_version"],
            )


class StorageDrainOffloadTask(StorageDrainTask):
    """Overflow drain for ``tasks.storage`` (#2732 step 4).

    ``StorageDrainTask`` always starts in-process and hands off to this task
    once its own in-process byte/wall-clock budget is exhausted with backlog
    rows still remaining — see ``StorageDrainTask._handoff_to_offload_job``.

    Carries ``is_async_write_workclass = True`` (#2782): unlike the base
    task, ``offload_required()`` treats this task exactly like
    ``event_drain`` — placement is unconditional whenever an offload-capable
    runner (``gcp_cloud_run`` / ``worker_queue``) advertises the task type,
    with no static routing hint required. Under the cloud preset this runs
    as the ``async_writer`` Cloud Run Job once one is deployed; with none
    deployed (or under onprem, where none exists), it degrades to an
    in-process ``background`` run like any other system task — the fail-open
    in ``execution._restrict_to_offload_runners``.

    Identical claim/hydrate/dispatch/fence logic to the base task — the only
    behavioural differences are ``_inprocess_budget_enabled = False`` (this
    variant never checks or re-triggers the in-process budget, so it always
    drains its claimed backlog to empty rather than escaping again) and the
    workclass marker itself.
    """

    task_type: ClassVar[str] = "storage_drain_offload"
    is_async_write_workclass: ClassVar[bool] = True
    _inprocess_budget_enabled: ClassVar[bool] = False
