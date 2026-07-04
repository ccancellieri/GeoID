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

import hashlib
import json
import logging
import re
import os
import asyncio
import itertools
from datetime import datetime
from typing import Any, Dict, Optional, Tuple

from dateutil import parser as _dateutil_parser

from dynastore.modules.catalog.asset_service import Asset, VirtualAssetCreate
from dynastore.modules.catalog.models import CoreAssetReferenceType

from dynastore.modules.catalog.tools import recalculate_and_update_extents
from dynastore.modules.db_config.query_executor import DbEngine
from dynastore.modules.storage.computed_fields import SYSTEM_FIELD_KEYS
from dynastore.models.driver_context import DriverContext

# Import Ingestion Configuration
from dynastore.tasks.ingestion.ingestion_config import IngestionPluginConfig
from dynastore.tasks.ingestion.ingestion_models import TaskIngestionRequest
from .reporters import _ingestion_reporter_registry
from dynastore.tasks.tools import initialize_reporters
from .operations import initialize_operations, run_pre_operations, run_post_operations

# Canonical task-enqueue path — imported at module level so unit tests can patch
# ``dynastore.tasks.ingestion.main_ingestion.create_task_for_catalog``.
from dynastore.modules.tasks.tasks_module import create_task_for_catalog
from dynastore.models.tasks import TaskCreate as _TaskCreate

logger = logging.getLogger(__name__)


class _IndexMissFailed(RuntimeError):
    """Sentinel raised after a total secondary-index miss.

    Propagates out of ``run_ingestion_task`` so the task runner knows the
    ingestion failed, without triggering the generic ``except Exception``
    handler that would call ``task_finished("FAILED")`` a second time.
    Callers should treat this identically to RuntimeError.
    """


class _RejectionFailed(_IndexMissFailed):
    """Sentinel raised after every row in the run was rejected by the upsert.

    A subclass of ``_IndexMissFailed`` so it is caught by the same
    ``except _IndexMissFailed`` guard below (task_finished("FAILED") was
    already called before this is raised — the guard prevents a second,
    conflicting FAILED notification from the generic exception handler).
    """


# ---------------------------------------------------------------------------
# Secondary-index health check (FIX 2)
# ---------------------------------------------------------------------------


def _check_index_health(
    rows_written: int,
    index_results: Dict[str, Any],
) -> Tuple[str, Optional[str]]:
    """Classify the ingestion outcome based on secondary-index results.

    Called after the write loop finishes.  Returns ``(status, message)``
    where ``status`` is either ``"COMPLETED"`` or ``"FAILED"`` and
    ``message`` is a human-readable explanation (or ``None`` on full
    success).

    Rules:
    - No secondary indexers configured (empty dict) → COMPLETED, no message.
    - All indexers succeeded for every written item → COMPLETED, no message.
    - Total miss: every indexer reported ``succeeded == 0`` for a non-zero
      ``total`` → FAILED with a structured message (retry re-attempts and,
      with a working DB engine, succeeds).
    - Partial miss: some items indexed, some not → COMPLETED with a warning
      message so the caller can surface it in task outputs.
    """
    if not index_results or rows_written == 0:
        return "COMPLETED", None

    total_succeeded = sum(r.succeeded for r in index_results.values())
    total_total = sum(r.total for r in index_results.values())
    total_failed = sum(r.failed for r in index_results.values())

    if total_total == 0:
        # Ops were empty (e.g. all id-less features) — not a miss.
        return "COMPLETED", None

    if total_succeeded == 0:
        # Complete secondary-index miss — mark FAILED so a retry re-attempts.
        msg = (
            f"Secondary index recorded 0 indexed items out of {rows_written} "
            f"written to the source store "
            f"(total_ops={total_total}, failed={total_failed}). "
            "A restore task has been enqueued. Retry this task to re-attempt indexing."
        )
        return "FAILED", msg

    if total_failed > 0 or total_succeeded < total_total:
        # Partial miss — keep COMPLETED but surface the counts.
        msg = (
            f"Partial secondary-index write: {total_succeeded} indexed, "
            f"{total_failed} failed out of {total_total} total ops "
            f"({rows_written} rows written to source store). "
            "Check driver logs for per-item failure reasons."
        )
        return "COMPLETED", msg

    return "COMPLETED", None


# ---------------------------------------------------------------------------
# Restore task enqueue (FIX 3)
# ---------------------------------------------------------------------------


async def enqueue_collection_reindex_task(
    catalog_id: str,
    collection_id: str,
    *,
    pg_conn: Optional[Any],
) -> None:
    """Enqueue an idempotent ``elasticsearch_bulk_reindex_collection`` task.

    Uses the canonical ``create_task`` path so application-layer dedup fires:
    if a non-terminal task with the same ``(schema_name, dedup_key)`` already
    exists, ``create_task`` returns ``None`` and no duplicate row is inserted.
    This prevents unbounded PENDING row growth when the root cause recurs across
    successive ingestion retries.

    ``pg_conn`` is accepted for API compatibility but the canonical path opens
    its own managed transaction via the engine.  When ``pg_conn`` is ``None``
    the engine is also unavailable; the function logs a warning and returns.

    The task re-streams the collection from the routing-resolved
    source-of-truth (PG primary, via GEOMETRY_EXACT hint) into the
    routing-resolved secondary-index writer — driver-agnostic.
    """
    if pg_conn is None:
        logger.warning(
            "enqueue_collection_reindex_task: pg_conn is None for %s/%s — "
            "skipping reindex enqueue. The secondary index may remain empty "
            "until a manual reindex or a future retry provides a live connection.",
            catalog_id, collection_id,
        )
        return

    try:
        import hashlib

        dedup_key = hashlib.sha256(
            f"reindex|{catalog_id}|{collection_id}".encode()
        ).hexdigest()[:64]

        task_data = _TaskCreate(
            task_type="elasticsearch_bulk_reindex_collection",
            caller_id="ingestion:index_restore",
            scope="CATALOG",
            execution_mode="ASYNCHRONOUS",
            inputs={"catalog_id": catalog_id, "collection_id": collection_id},
            collection_id=collection_id,
            dedup_key=dedup_key,
        )

        # create_task_for_catalog resolves the physical schema then calls
        # create_task which performs the dedup pre-check (SELECT … WHERE
        # dedup_key = … AND status NOT IN ('COMPLETED','FAILED','DEAD_LETTER'))
        # inside its own managed_transaction.  Returns None on dedup hit.
        task = await create_task_for_catalog(
            engine=pg_conn,
            task_data=task_data,
            catalog_id=catalog_id,
        )
        if task is None:
            logger.info(
                "enqueue_collection_reindex_task: dedup hit — a non-terminal "
                "reindex task already exists for %s/%s; skipping duplicate insert.",
                catalog_id, collection_id,
            )
        else:
            logger.info(
                "enqueue_collection_reindex_task: enqueued restore for %s/%s "
                "(task_id=%s)",
                catalog_id, collection_id, task.task_id,
            )
    except Exception as exc:
        logger.error(
            "enqueue_collection_reindex_task: failed to enqueue restore for "
            "%s/%s: %s",
            catalog_id, collection_id, exc,
        )


# Cap on the per-indexer failure-detail sample carried across batches, and
# the bounded merge itself, live in dynastore.models.protocols.indexer
# (shared with the other two accumulation sites — IndexDispatcher's
# per-chunk aggregation and item_service's per-batch aggregation — #2657).
# Re-exported under the historical private name so existing call sites and
# tests in this module are unaffected.
from dynastore.models.protocols.indexer import (
    MAX_ACCUMULATED_FAILURE_SAMPLES as _MAX_ACCUMULATED_FAILURE_SAMPLES,  # noqa: F401
    merge_bulk_results as _merge_bulk_results,
)


def _merge_index_results(
    accumulated: Dict[str, Any],
    batch_results: Dict[str, Any],
) -> None:
    """Merge per-batch BulkResult entries into the running totals in-place."""
    for indexer_id, bulk_res in batch_results.items():
        if indexer_id in accumulated:
            accumulated[indexer_id] = _merge_bulk_results(accumulated[indexer_id], bulk_res)
        else:
            accumulated[indexer_id] = bulk_res


async def _maybe_apply_ingest_backpressure() -> None:
    """Cooperative backpressure before a batch flush (#2494 P1).

    No-op unless ``TasksPluginConfig.items_secondary_via_storage_plane`` is
    enabled — with the flag off, ingestion behaviour is unchanged. When the
    flag is on and the aggregate tasks.storage/tasks.events outbox backlog
    is high (``async_writer_backlog.backlog_is_high()``), sleeps for
    ``TasksPluginConfig.ingest_backpressure_sleep_seconds`` before the
    caller flushes the next batch, so the storage_drain worker gets room to
    catch up instead of the backlog growing unbounded under a hot ingestion
    job. Best-effort: never raises, never blocks ingestion on a config or
    probe failure.
    """
    try:
        from dynastore.models.protocols.platform_configs import PlatformConfigsProtocol
        from dynastore.modules.tasks.async_writer_backlog import backlog_is_high
        from dynastore.modules.tasks.tasks_config import TasksPluginConfig
        from dynastore.tools.discovery import get_protocol

        config_mgr = get_protocol(PlatformConfigsProtocol)
        cfg = await config_mgr.get_config(TasksPluginConfig) if config_mgr else None
        if not isinstance(cfg, TasksPluginConfig) or not cfg.items_secondary_via_storage_plane:
            return
        if await backlog_is_high():
            logger.info(
                "ingestion: storage-plane outbox backlog high — sleeping "
                "%.1fs before the next batch flush.",
                cfg.ingest_backpressure_sleep_seconds,
            )
            await asyncio.sleep(cfg.ingest_backpressure_sleep_seconds)
    except Exception:  # noqa: BLE001 — backpressure is best-effort, never block ingestion
        logger.debug(
            "ingestion: backpressure check failed — proceeding without delay.",
            exc_info=True,
        )


# Per-batch memory budgeting -------------------------------------------------
#
# A batch is flushed when EITHER an explicit row cap (database_batch_size) OR an
# accumulated-geometry budget (max_batch_memory_mb) is reached — whichever comes
# first. The memory budget keeps a handful of very large geometries (e.g.
# administrative multipolygons) from exhausting the container before a fixed row
# count is ever hit. Cost is dominated by geometry coordinates, so a feature's
# footprint is approximated by counting its coordinate ordinates; each is carried
# as a Python float (~24 bytes) inside nested lists, plus a flat per-feature
# overhead for the properties dict.
_FEATURE_BASE_BYTES = 512
_BYTES_PER_COORD_ORDINATE = 24


def _count_coordinate_ordinates(value: Any) -> int:
    """Total scalar ordinates in a (possibly deeply nested) GeoJSON
    ``coordinates`` array. Iterative to stay cheap on dense geometries."""
    total = 0
    stack = [value]
    while stack:
        node = stack.pop()
        if isinstance(node, (list, tuple)):
            if node and isinstance(node[0], (int, float)):
                total += len(node)
            else:
                stack.extend(node)
        elif isinstance(node, (int, float)):
            total += 1
    return total


def _estimate_feature_bytes(feature: Any) -> int:
    """Rough, geometry-dominated estimate of a prepared feature's in-memory
    footprint, used to bound a batch by accumulated bytes so a few very large
    geometries cannot blow the container's memory before the row-count cap."""
    if not isinstance(feature, dict):
        return _FEATURE_BASE_BYTES
    geom = feature.get("geometry")
    ordinates = 0
    if isinstance(geom, dict):
        if geom.get("type") == "GeometryCollection":
            for g in geom.get("geometries") or ():
                if isinstance(g, dict):
                    ordinates += _count_coordinate_ordinates(g.get("coordinates"))
        else:
            ordinates += _count_coordinate_ordinates(geom.get("coordinates"))
    return _FEATURE_BASE_BYTES + ordinates * _BYTES_PER_COORD_ORDINATE


# Canonical items-schema data types that denote a temporal value. A property
# declared as one of these is coerced from common string representations to a
# canonical ISO-8601 string during ingestion — see ``apply_temporal_coercion``.
_TEMPORAL_DATA_TYPES = frozenset({"date", "time", "timestamp"})


def _coerce_temporal_value(value, data_type: str, parse_format: Optional[str] = None):
    """Best-effort coercion of a single value to canonical ISO-8601.

    The typed write path already accepts ISO-8601 strings for ``date`` /
    ``time`` / ``timestamp`` columns, but not arbitrary formats (e.g.
    ``31/12/2024`` or ``Jan 31 2024``). This normalises a parseable string to
    the ISO form the write path accepts.

    ``parse_format`` is the field's optional ``strptime`` hint (#1350). When
    set it is tried first, so ambiguous numeric formats are read exactly as the
    operator declared (``%d/%m/%Y`` reads ``01/02/2024`` as 1 Feb, not the
    month-first 2 Jan that ``dateutil`` auto-detection would pick). A value that
    does not match the explicit pattern is returned unchanged — it then falls
    through to the typed write, which rejects just that row via the 207
    IngestionReport rather than silently month-first-guessing against the
    operator's stated intent. With no hint, parsing falls back to
    ``dateutil`` auto-detection (the #1333 behaviour).

    Only strings are touched; a value that is already a non-string (e.g. a
    reader-decoded ``datetime``) or that cannot be parsed is returned
    unchanged. An unparseable value then falls through to the typed write,
    which rejects just that row via the 207 IngestionReport rather than
    failing the whole batch.
    """
    if not isinstance(value, str):
        return value
    text = value.strip()
    if not text:
        return value
    if parse_format:
        try:
            parsed = datetime.strptime(text, parse_format)
        except (ValueError, TypeError):
            # Explicit pattern set but this row doesn't match it: respect the
            # operator's intent and leave the row for the typed write to reject
            # (207), rather than auto-detecting a different interpretation.
            return value
    else:
        try:
            parsed = _dateutil_parser.parse(text)
        except (ValueError, OverflowError, TypeError):
            return value
    if data_type == "date":
        return parsed.date().isoformat()
    if data_type == "time":
        return parsed.time().isoformat()
    return parsed.isoformat()


def apply_temporal_coercion(properties: dict, temporal_fields: dict) -> dict:
    """Coerce every schema-declared temporal property in ``properties`` in place.

    ``temporal_fields`` maps a field name to a ``(data_type, parse_format)``
    pair — its canonical temporal ``data_type`` and the optional ``strptime``
    input hint (#1350; ``None`` when the field declares no hint). Properties not
    named there are left untouched, so this is a no-op for collections whose
    items-schema declares no temporal field. Returns ``properties`` for
    call-site convenience.
    """
    if not temporal_fields:
        return properties
    for name, (data_type, parse_format) in temporal_fields.items():
        if name in properties:
            properties[name] = _coerce_temporal_value(
                properties[name], data_type, parse_format
            )
    return properties


# Identity + lifecycle fields lifted from ``generated`` into ``record["system"]``.
# Each is omitted from the envelope when its value is None — absent values
# simply don't appear, so a reporter never sees a half-populated system bag.
# SSOT lives in ``modules/storage/computed_fields`` (imported at module top) so
# the read-side ``expose_all`` assembly groups the same keys into ``system``.
_SYSTEM_KEYS = SYSTEM_FIELD_KEYS


def _build_report_envelope(record, generated: dict | None) -> dict:
    """Reshape one ingestion outcome into the report envelope.

    Returns a dict with sibling keys ``properties`` (user attributes only —
    never mixed with platform-derived values), ``stats`` (flat dict of computed
    statistics), and ``system`` (identity + lifecycle fields, each present only
    when the ingestion actually produced a value). GeoJSON envelope keys
    (``type``/``id``/``geometry``) stay at the record top level.

    ``generated`` is the per-item entry from ``ctx.extensions["_generated_stats"]``
    or ``None`` on fallback / rejection paths where the upsert never produced one.
    A ``model_dump``-able ``record`` is normalised first so every reporter sees
    one shape.
    """
    if hasattr(record, "model_dump"):
        rec = record.model_dump(mode="json", exclude_none=True)
    elif isinstance(record, dict):
        rec = dict(record)
    else:
        return record

    props = rec.get("properties")
    rec["properties"] = dict(props) if isinstance(props, dict) else {}

    gen = generated or {}
    raw_stats = gen.get("stats") or {}
    rec["stats"] = dict(raw_stats) if isinstance(raw_stats, dict) else {}

    system: dict = {}
    for key in _SYSTEM_KEYS:
        value = gen.get(key)
        if value is not None:
            system[key] = value
    rec["system"] = system

    return rec


def _enrich_report_record(record, generated: dict) -> dict:
    """Build a SUCCESS-path report envelope. See ``_build_report_envelope``."""
    return _build_report_envelope(record, generated)


async def _broadcast_batch_outcome(
    reporters, batch: list, upsert_result, generated=None, rejections=None
):
    """Fan out per-row outcomes to every reporter after a batch upsert.

    Accepted rows are reported as SUCCESS and per-row ``rejections`` (the
    upsert's ``ctx.extensions["_rejections"]`` out-list) as FAILED, so a single
    bad row lands in the detailed report as a row failure instead of aborting
    the whole job. Contract for the payload shape lives on
    ``ReportingInterface.process_batch_outcome`` — each item is
    ``{"status", "message", "record"}``.

    With no rejections the upsert is transactional, so every input row
    persisted: the read-back ``upsert_result`` is the canonical record source
    (server-assigned fields), and if its shape doesn't line up 1:1 with the
    input batch we fall back to the input features. When there ARE rejections
    the batch is partial — the read-back holds only the accepted rows, so it is
    always the success source and the rejected rows are reported separately.

    ``generated`` is the upsert's ``ctx.extensions["_generated_stats"]`` — per
    accepted item the geoid, external_id, asset_id and full computed-statistics
    set, aligned with the read-back. It is applied only when its length matches
    the success records, else dropped rather than mis-zipped.
    """
    rejections = list(rejections or [])
    accepted = (
        upsert_result
        if isinstance(upsert_result, list)
        else ([upsert_result] if upsert_result else [])
    )
    # Legacy defensive fallback only applies when nothing was rejected: if the
    # read-back didn't align 1:1 with the input batch, report the input
    # features (server fields/stats unavailable) rather than under-reporting.
    if not rejections and len(accepted) != len(batch):
        records = batch
        generated = None
    else:
        records = accepted

    gen = (
        generated
        if isinstance(generated, list) and len(generated) == len(records)
        else None
    )
    outcomes = []
    for i, rec in enumerate(records):
        rec = _enrich_report_record(rec, gen[i] if gen is not None else {})
        outcomes.append({"status": "SUCCESS", "message": None, "record": rec})
    for rej in rejections:
        # Identity that the upsert managed to derive before the row was rejected.
        # Only keys the rejection carried surface in ``system`` (e.g. external_id
        # is known pre-write, geoid only when a sidecar minted one).
        rej_generated = {
            k: rej[k]
            for k in _SYSTEM_KEYS
            if rej.get(k) is not None
        }
        rec = _build_report_envelope(rej.get("record") or {}, rej_generated)
        outcomes.append({
            "status": "FAILED",
            "message": rej.get("message") or rej.get("reason") or "rejected",
            "record": rec,
        })
    if outcomes:
        await asyncio.gather(
            *(reporter.process_batch_outcome(outcomes) for reporter in reporters)
        )


# Top-level keys yielded by readers (e.g. GdalOsgeoReader) that are GeoJSON
# envelope markers or reader-internal geometry slots — never publisher DBF
# columns. Excluded from the merge into feature["properties"] so they don't
# pollute the attributes JSONB sidecar. Source columns with the same names
# (if any) reach properties through the inner properties dict, not the
# top-level merge, so this is safe.
_STRUCTURAL_RAW_KEYS = frozenset({
    "geometry", "properties", "id",
    "type",                          # GeoJSON Feature envelope marker
    "geometry_wkb", "geometry_wkt",  # reader-internal geometry slots
})


def _resolve_source_content_type(asset: Asset) -> Optional[str]:
    """Best-effort MIME-type lookup used by reader resolution.

    1. ``asset.metadata['content_type']`` — populated by
       :meth:`GcpStorageOpsMixin.initiate_upload` for every new GCS
       upload, so the happy path is one in-memory dict read.
    2. For legacy assets whose metadata pre-dates the injection (or
       non-GCS uploads), do a single ``storage.objects.get`` to read
       the blob's native ``contentType`` header.  Only attempted for
       ``gs://`` URIs and behind a try/except so a bad path / missing
       creds never blocks the task.
    """
    md = asset.metadata or {}
    ct = md.get("content_type") or md.get("contentType")
    if ct:
        return ct
    source = asset.uri or asset.href
    if not source or not source.startswith("gs://"):
        return None
    try:
        from google.cloud import storage

        bucket_name, _, object_name = source[len("gs://"):].partition("/")
        if not bucket_name or not object_name:
            return None
        client = storage.Client()
        blob = client.bucket(bucket_name).get_blob(object_name)
        if blob is None:
            return None
        return blob.content_type
    except Exception as exc:  # noqa: BLE001
        logger.warning(
            "ingestion: GCS HEAD for %r failed (%s); reader resolution will "
            "rely on URI suffix only.", asset.uri, exc,
        )
        return None


# ---------------------------------------------------------------------------
# Deterministic per-feature identity (#2709)
# ---------------------------------------------------------------------------
#
# Re-running the same vector ingest must CONVERGE (upsert) rather than
# duplicate every feature. ``prepare_record_for_upsert`` resolves the
# GeoJSON-style ``feature["id"]`` through a three-tier precedence — each
# tier only runs when the previous one produced nothing:
#
#   1. A configurable source field (``column_mapping.external_id``, e.g.
#      GAUL's ``GAUL1_CODE``) or the source's own natural ``id``.
#   2. The reader-surfaced OGR feature id (FID) — stable across re-reads of
#      the SAME unmodified source (``GdalOsgeoReader`` now emits it as the
#      record's top-level ``"id"``, so tier 1's "id" lookup already covers
#      it when no explicit field is configured).
#   3. A content hash of the feature (geometry + attributes) — the final
#      safety net for sources where neither of the above resolved anything.
#
# ``feature["id"]`` then feeds ``ItemsWritePolicy.resolve_external_id`` (see
# ``modules/storage/driver_config.py``), which falls back to the feature's
# top-level ``id`` when no ``derive.external_id`` path is configured on the
# collection — so no downstream write-path change is required here.


def _resolve_raw_identity(raw: dict, ext_id_field: str) -> Any:
    """Look up *ext_id_field* on a raw reader record.

    Top-level key first, then ``properties[ext_id_field]`` — mirrors
    ``prepare_record_for_upsert``'s private ``_get_raw_val`` for the
    identity field specifically, factored out so tiers 1/2 of the #2709
    identity precedence are unit-testable without a live reader or DB.
    """
    if not ext_id_field:
        return None
    return raw[ext_id_field] if ext_id_field in raw else raw.get("properties", {}).get(ext_id_field)


def _content_hash_feature_id(geometry: Optional[dict], properties: dict) -> str:
    """Tier-3 identity fallback (#2709): a stable hash of the feature.

    Used only when neither a configured id field nor a natural id/OGR FID
    resolved (tiers 1-2). Deterministic for byte-identical geometry +
    properties, so a retried/re-run ingest of the SAME source converges on
    the same external_id instead of appending a duplicate row.
    """
    canonical = json.dumps(
        {"geometry": geometry, "properties": properties},
        sort_keys=True,
        default=str,
    )
    return "sha256:" + hashlib.sha256(canonical.encode("utf-8")).hexdigest()


async def run_ingestion_task(
    engine: DbEngine,
    task_id: str,
    catalog_id: str,
    collection_id: str,
    task_request: TaskIngestionRequest,
    caller_id: Optional[str] = None,
):
    from dynastore.tools.discovery import get_protocol
    from dynastore.models.protocols import CatalogsProtocol

    catalog_module = get_protocol(CatalogsProtocol)
    if not catalog_module:
        raise RuntimeError("CatalogsProtocol implementation not found.")

    if task_request.asset is None:
        raise ValueError("task_request.asset is required for ingestion.")
    req_asset = task_request.asset

    pre_ops = []
    post_ops = []

    logger.info(
        f"Starting Ingestion Task '{task_id}' for collection '{catalog_id}:{collection_id}'. Source SRID: {task_request.source_srid}"
    )

    # Resolve physical schema for task storage
    phys_schema = await catalog_module.resolve_physical_schema(
        catalog_id, ctx=DriverContext(db_resource=engine) if engine else None
    )
    if phys_schema is None:
        raise RuntimeError(f"Cannot resolve physical schema for catalog {catalog_id!r}.")

    reporters = initialize_reporters(
        engine,
        task_id,
        task_request,
        task_request.reporting,
        registry=_ingestion_reporter_registry,
        schema=phys_schema,
        catalog_id=catalog_id,
        collection_id=collection_id,
    )

    await asyncio.gather(
        *(
            reporter.task_started(
                task_id,
                collection_id,
                catalog_id,
                req_asset.asset_id or req_asset.uri or "",
            )
            for reporter in reporters
        )
    )

    # --- Ensure Logical Collection Exists ---
    await catalog_module.ensure_collection_exists(
        catalog_id, collection_id, lang=task_request.lang, ctx=DriverContext(db_resource=engine)
    )

    # Best-effort reap of orphaned extraction dirs left by prior crashed tasks
    # on this shared temp volume.  A failure here must never abort ingestion.
    try:
        from dynastore.tasks.ingestion.temp_reaper import reap_orphan_task_dirs
        await reap_orphan_task_dirs(engine)
    except Exception:
        logger.warning("temp_reaper: sweep failed — continuing ingestion", exc_info=True)

    logger.info(f"Task '{task_id}': Beginning main ingestion process.")
    try:
        # --- Fetch Physical Configuration (Immutable Storage) ---
        catalog_config = await catalog_module.get_collection_config(
            catalog_id, collection_id
        )

        # --- Fetch Ingestion Configuration (Mutable Logic) ---
        ingestion_config = await catalog_module.configs.get_config(
            IngestionPluginConfig, catalog_id, collection_id
        )

        # --- Resolve temporal fields for string -> ISO-8601 coercion (#1333) ---
        # A property whose items-schema field is declared date/time/timestamp is
        # normalised from common date string formats to canonical ISO-8601 at
        # ingestion time, since the typed write path accepts ISO-8601 but not
        # arbitrary formats. A resolution failure (or a schema with no temporal
        # field) simply degrades to "no coercion".
        temporal_fields: dict = {}
        # #1216: propose-only schema derivation. When the collection has no
        # items_schema yet, derive one from the source asset's OGR/gdalinfo and
        # surface it in the report tail for an admin to apply — never written to
        # config here. Set once below; emitted via task_finished(summary=...).
        items_schema_present = False
        proposed_items_schema: Optional[dict] = None
        try:
            from dynastore.modules.storage.driver_config import ItemsSchema

            items_schema = await catalog_module.configs.get_config(
                ItemsSchema, catalog_id, collection_id
            )
            if items_schema and items_schema.fields:
                items_schema_present = True
                temporal_fields = {
                    name: (
                        fdef.data_type,
                        getattr(fdef, "parse_format", None),
                    )
                    for name, fdef in items_schema.fields.items()
                    if getattr(fdef, "data_type", None) in _TEMPORAL_DATA_TYPES
                }
        except Exception as exc:
            logger.debug(
                "Task '%s': items-schema temporal resolution skipped (%s)",
                task_id, exc,
            )
        if temporal_fields:
            logger.info(
                "Task '%s': temporal coercion active for fields: %s",
                task_id, sorted(temporal_fields),
            )

        # Initialize Operations
        pre_ops = initialize_operations(
            engine,
            task_id,
            task_request,
            task_request.pre_operations,
            catalog_config=catalog_config,
            ingestion_config=ingestion_config,
        )
        post_ops = initialize_operations(
            engine,
            task_id,
            task_request,
            task_request.post_operations,
            catalog_config=catalog_config,
            ingestion_config=ingestion_config,
        )

        # --- Ensure Storage Exists (all write drivers) ---
        from dynastore.modules.storage.router import get_write_drivers
        write_drivers = await get_write_drivers(catalog_id, collection_id)
        for resolved in write_drivers:
            await resolved.driver.ensure_storage(
                catalog_id, collection_id,
                db_resource=engine,
            )

        asset_manager = catalog_module.assets

        # --- Resolve or Create the Asset ---
        asset: Optional[Asset] = None
        if req_asset.asset_id:
            asset = await asset_manager.get_asset(
                catalog_id, req_asset.asset_id, collection_id
            )
            if not asset and not req_asset.uri:
                raise ValueError(
                    f"Asset with asset_id '{req_asset.asset_id}' not found and no URI provided."
                )

        if not asset and req_asset.uri:
            logger.info(f"Creating asset from URI: {req_asset.uri}")
            asset_id_for_creation = req_asset.asset_id or re.sub(
                r"[^a-zA-Z0-9_\-]", "_", os.path.basename(req_asset.uri)
            )

            # External-source ingestion: register as a virtual asset since we
            # don't manage the source bytes (the file lives in the caller's
            # storage). Stage 4 will replace this with the policy-driven
            # variant; today we keep the `Asset.uri` field populated by
            # storing the raw URI as the virtual `href`.
            asset_payload = VirtualAssetCreate(
                asset_id=asset_id_for_creation,
                href=req_asset.uri,
                metadata=req_asset.metadata or {},
            )
            asset = await asset_manager.create_asset(
                catalog_id, asset_payload, collection_id, ctx=DriverContext(db_resource=engine)
            )

        if not asset:
            raise ValueError("Could not find or create an asset.")

        # --- Run Pre-Operations ---
        if pre_ops:
            catalog = await catalog_module.get_catalog(catalog_id, ctx=DriverContext(db_resource=engine))
            collection = await catalog_module.get_collection(
                catalog_id, collection_id, ctx=DriverContext(db_resource=engine)
            )
            asset = await run_pre_operations(pre_ops, catalog, collection, asset)
            if asset is None:
                raise RuntimeError("Pre-operations returned no asset.")

        from dynastore.modules.catalog.asset_service import AssetStatus

        if asset.status == AssetStatus.PENDING:
            raise RuntimeError(
                f"Asset {asset.asset_id} is PENDING (kind={asset.kind}): "
                f"OBJECT_FINALIZE has not activated this asset yet. Either "
                f"upload completed but the GCS push event was lost / dropped "
                f"(check handle_asset_events logs for 'no physical schema' "
                f"or 'orphan finalize'), or upload is still in flight. "
                f"Re-submit ingestion only after status=='active'."
            )

        source_file_path = asset.uri or asset.href
        if source_file_path is None:
            raise RuntimeError(
                f"Asset {asset.asset_id} has no source URI: kind={asset.kind} status={asset.status}"
            )
        asset_id = asset.asset_id
        # MIME hint used by reader resolution when the URI itself carries
        # no recognisable suffix (legacy bare-filename uploads).  Source
        # of truth: the asset row's metadata; falls back to a single GCS
        # object HEAD for legacy rows whose metadata pre-dates the
        # ``content_type`` injection in ``GcpStorageOpsMixin.initiate_upload``.
        source_content_type = _resolve_source_content_type(asset)

        # #1216: derive a proposed items_schema when the collection has none.
        # Best-effort and propose-only — failures (no libgdal in this SCOPE,
        # unreadable source, raster asset) just skip; nothing is written to
        # config. The result rides in the report tail via task_finished(summary).
        if not items_schema_present:
            try:
                from dynastore.tasks.ingestion.schema_introspect import (
                    extract_ogr_schema,
                )

                derived = extract_ogr_schema(source_file_path)
                if derived:
                    proposed_items_schema = {
                        "class_key": "items_schema",
                        "fields": {
                            name: fd.model_dump(mode="json", exclude_none=True)
                            for name, fd in derived.items()
                        },
                        "note": (
                            "Derived from the source via OGR/gdalinfo because the "
                            "collection has no items_schema. PROPOSAL ONLY — not "
                            "applied. PATCH it to the ItemsSchema config "
                            "(items_schema) for this collection to adopt it."
                        ),
                    }
                    logger.info(
                        "Task '%s': proposed items_schema derived (%d fields) — "
                        "surfaced in report, not applied.",
                        task_id, len(derived),
                    )
            except Exception as exc:
                logger.debug(
                    "Task '%s': items_schema derivation skipped (%s).",
                    task_id, exc,
                )

        # --- Process and Ingest Features ---
        total_features = None
        try:
            from dynastore.tasks.ingestion.readers import resolve_reader

            _count_reader = resolve_reader(
                source_file_path, content_type=source_content_type,
                reader_id=task_request.reader,
            )()
            total_features = _count_reader.feature_count(
                source_file_path, content_type=source_content_type,
            )
            if total_features is not None:
                logger.info(f"Source file contains {total_features} features.")
                await asyncio.gather(
                    *(reporter.update_progress(0, total_features) for reporter in reporters)
                )
        except Exception as e:
            logger.warning(f"Could not determine total feature count: {e}")

        # Flush a batch on whichever limit is reached first: an explicit row cap
        # (database_batch_size, default 50) or an accumulated-geometry memory
        # budget (max_batch_memory_mb, default 32 MB). The memory budget is what
        # auto-shrinks batches for geometry-heavy sources (e.g. admin
        # multipolygons) so a fixed row count cannot exhaust the container; the
        # lowered row-cap default keeps light-attribute sources (where the byte
        # budget alone wouldn't trigger for a long time) from batching all the
        # way up to a size that only made sense before dense collections like
        # GAUL exposed the memory budget as the real constraint.
        row_cap = task_request.database_batch_size or 50
        mem_budget_bytes = max(1, task_request.max_batch_memory_mb) * 1024 * 1024
        current_batch = []
        current_batch_bytes = 0
        rows_ingested = 0
        # Rows actually persisted vs. rejected by the upsert (#2891) — distinct
        # from rows_ingested, which counts every row PROCESSED (batch size) and
        # gates progress/extent regardless of whether the upsert accepted it.
        rows_persisted = 0
        rows_rejected = 0
        first_rejection_message: Optional[str] = None
        # Accumulate per-indexer BulkResult totals across all batches so we can
        # classify secondary-index health at the end of the loop.
        accumulated_index_results: Dict[str, Any] = {}

        # Defer tile-cache invalidation: the per-batch write path would enqueue
        # one tiles_invalidate task per batch (hundreds for a large ingestion).
        # We suppress that here and enqueue ONE coalesced invalidation for the
        # whole ingested extent after the loop (see below).
        upsert_context = {"asset_id": asset_id, "defer_tile_invalidation": True}

        def prepare_record_for_upsert(raw: dict, request: TaskIngestionRequest) -> dict:
            mapping = request.column_mapping
            feature = {"properties": {}}

            def _get_raw_val(key):
                if not key:
                    return None
                return (
                    raw.get(key) if key in raw else raw.get("properties", {}).get(key)
                )

            # 1. Identity — tiers 1 & 2 of the #2709 deterministic-identity
            # precedence (see the module-level comment above
            # ``_resolve_raw_identity``): a configured field wins, else the
            # source's natural "id" (which GdalOsgeoReader now populates
            # from the OGR FID when the source has no native id — tier 2).
            # ``is not None`` — not truthy — so a legitimate FID/id of 0
            # is not dropped.
            ext_id_field = mapping.external_id or "id"
            ext_id = _resolve_raw_identity(raw, ext_id_field)
            if ext_id is not None and ext_id != "":
                feature["id"] = ext_id

            # 2. Geometry
            geometry = None
            if mapping.csv_lat_column and mapping.csv_lon_column:
                lat = _get_raw_val(mapping.csv_lat_column)
                lon = _get_raw_val(mapping.csv_lon_column)
                if lat is not None and lon is not None:
                    coords = [float(lon), float(lat)]
                    elev = _get_raw_val(mapping.csv_elevation_column)
                    if elev is not None:
                        coords.append(float(elev))
                    else:
                        if mapping.csv_elevation_column:
                            logger.warning(
                                f"Elevation column '{mapping.csv_elevation_column}' specified but not found/empty in record: {raw.keys()} props: {raw.get('properties', {}).keys()}"
                            )
                    geometry = {"type": "Point", "coordinates": coords}
            elif mapping.csv_wkt_column:
                wkt = _get_raw_val(mapping.csv_wkt_column)
                if wkt:
                    from shapely.wkt import loads
                    from shapely.geometry import mapping as shapely_mapping

                    try:
                        geometry = shapely_mapping(loads(wkt))
                    except Exception as exc:
                        logger.warning(
                            "Failed to parse WKT geometry; leaving feature geometry empty: %s",
                            exc,
                        )
            elif mapping.geometry_wkb:
                wkb = _get_raw_val(mapping.geometry_wkb)
                if wkb:
                    if isinstance(wkb, dict):
                        # GDAL/OGR already decoded the geometry to GeoJSON dict.
                        geometry = wkb
                    else:
                        from shapely.wkb import loads
                        from shapely.geometry import mapping as shapely_mapping

                        try:
                            geometry = shapely_mapping(loads(wkb))
                        except Exception as exc:
                            logger.warning(
                                "Failed to parse WKB geometry; leaving feature geometry empty: %s",
                                exc,
                            )
                # If geometry_wkb column produced nothing, fall through to the
                # standard ``geometry`` key (e.g. GDAL stores it there).
                if geometry is None:
                    geometry = raw.get("geometry")
            else:
                # Fallback: check if 'geometry' is already present (e.g. GeoJSON)
                geometry = raw.get("geometry")

            if geometry:
                feature["geometry"] = geometry

            # 3. Attributes
            raw_props = raw.get("properties", {})
            if (
                mapping.attributes_source_type == "explicit_list"
                and mapping.attribute_mapping
            ):
                for item in mapping.attribute_mapping:
                    if item.constant is not None:
                        val = item.constant
                    else:
                        val = _get_raw_val(item.source)
                    
                    if val is not None:
                        feature["properties"][item.map_to] = val
            else:
                # Geometry / CSV source columns are consumed into the feature
                # geometry and must not leak into the attribute set. The
                # external_id *source* field is deliberately NOT reserved: unlike
                # the geometry sources it is a genuine attribute — it backs its
                # own materialised column and the write policy's
                # ``derive.external_id`` reads it from ``properties`` at write
                # time — so stripping it here would null both that column and the
                # derived external_id. This is the ``"all"`` counterpart of
                # listing the external_id field explicitly in ``attribute_mapping``
                # (which already keeps it), so the two source modes now agree.
                reserved = {
                    mapping.csv_lat_column,
                    mapping.csv_lon_column,
                    mapping.csv_elevation_column,
                    mapping.csv_wkt_column,
                    mapping.geometry_wkb,
                }
                # Take from properties first
                for k, v in raw_props.items():
                    if k not in reserved:
                        feature["properties"][k] = v
                # Also take from top-level if not already taken and not reserved.
                # _STRUCTURAL_RAW_KEYS covers the GeoJSON envelope marker and the
                # reader's internal geometry slots — they're never DBF columns
                # and were leaking into the attributes JSONB when the user's
                # ColumnMappingConfig left mapping.geometry_wkb unset.
                for k, v in raw.items():
                    if (
                        k not in _STRUCTURAL_RAW_KEYS
                        and k not in reserved
                        and k not in feature["properties"]
                    ):
                        feature["properties"][k] = v

            # 3b. Coerce schema-declared temporal properties (date/time/timestamp)
            # from common string formats to canonical ISO-8601 (#1333). No-op
            # when the items-schema declares no temporal field.
            apply_temporal_coercion(feature["properties"], temporal_fields)

            # 4. Temporal
            valid_from = _get_raw_val(request.time_validity_start_column)
            if valid_from:
                feature["valid_from"] = valid_from
            valid_to = _get_raw_val(request.time_validity_end_column)
            if valid_to:
                feature["valid_to"] = valid_to

            # 5. Tier 3 of the #2709 deterministic-identity precedence:
            # neither a configured field nor a natural id/OGR FID resolved
            # in step 1 — hash the assembled feature so a retried/re-run
            # ingest of the SAME source still converges instead of
            # appending a duplicate row.
            if "id" not in feature:
                feature["id"] = _content_hash_feature_id(
                    feature.get("geometry"), feature["properties"]
                )

            return feature

        # Pluggable source reader.  ``ReaderRegistry.resolve`` picks the
        # highest-priority reader whose ``can_read(uri)`` matches —
        # GdalOsgeoReader (system libgdal, supports Parquet/FGB/SHP/CSV/…)
        # then PyogrioReader as a tail fallback.  Solves the
        # ``CPLE_OpenFailedError: not recognized as being in a supported
        # file format`` blocker when a PyPI-wheel-bundled libgdal lacks the
        # Arrow/Parquet driver.
        from dynastore.tasks.ingestion.readers import resolve_reader

        reader_cls = resolve_reader(
            source_file_path, content_type=source_content_type,
            reader_id=task_request.reader,
        )
        reader_inst = reader_cls()
        logger.info(
            "ingestion: source %r (content_type=%r) → reader '%s'%s",
            source_file_path, source_content_type,
            reader_cls.reader_id or reader_cls.__name__,
            " (explicit override)" if task_request.reader else "",
        )

        # Built as a dict (not passed as literal kwargs) so reader_options can
        # cleanly override a default (e.g. read_batch_size) instead of colliding
        # as a duplicate keyword argument.
        open_kwargs: Dict[str, Any] = {
            "encoding": task_request.encoding,
            "content_type": source_content_type,
            "task_id": task_id,
            "task_schema": phys_schema,
            "read_batch_size": task_request.read_batch_size,
        }
        # task_id/task_schema/content_type are the reader's own identity/
        # plumbing kwargs (e.g. used to name reaper-tracked scratch dirs) —
        # a caller has no legitimate reason to override them via
        # reader_options, so drop and warn on any collision instead of
        # silently shadowing them.
        reader_options = dict(task_request.reader_options or {})
        shadowed_keys = [
            key for key in ("task_id", "task_schema", "content_type")
            if key in reader_options
        ]
        if shadowed_keys:
            logger.warning(
                "ingestion: reader_options attempted to override structural "
                "kwarg(s) %s — ignoring, these are fixed by the ingestion "
                "task itself and are not user-tunable",
                shadowed_keys,
            )
            for key in shadowed_keys:
                del reader_options[key]
        open_kwargs.update(reader_options)

        with reader_inst.open(source_file_path, **open_kwargs) as reader:
            sliced_reader = itertools.islice(
                reader,
                task_request.offset,
                task_request.limit + task_request.offset
                if task_request.limit
                else None,
            )

            for idx, raw_record in enumerate(sliced_reader, start=task_request.offset):
                feature = prepare_record_for_upsert(dict(raw_record), task_request)
                current_batch.append(feature)
                current_batch_bytes += _estimate_feature_bytes(feature)

                if (
                    len(current_batch) >= row_cap
                    or current_batch_bytes >= mem_budget_bytes
                ):
                    await _maybe_apply_ingest_backpressure()
                    upsert_ctx = DriverContext(db_resource=engine)
                    upsert_result = await catalog_module.upsert(
                        catalog_id,
                        collection_id,
                        current_batch,
                        ctx=upsert_ctx,
                        processing_context=upsert_context,
                    )
                    rows_ingested += len(current_batch)
                    _batch_rejections = upsert_ctx.extensions.get("_rejections") or []
                    _batch_persisted = (
                        upsert_result
                        if isinstance(upsert_result, list)
                        else ([upsert_result] if upsert_result else [])
                    )
                    rows_persisted += len(_batch_persisted)
                    rows_rejected += len(_batch_rejections)
                    if first_rejection_message is None and _batch_rejections:
                        first_rejection_message = (
                            _batch_rejections[0].get("message")
                            or _batch_rejections[0].get("reason")
                            or "rejected"
                        )
                    _merge_index_results(
                        accumulated_index_results,
                        upsert_ctx.extensions.get("_index_results") or {},
                    )
                    await _broadcast_batch_outcome(
                        reporters, current_batch, upsert_result,
                        upsert_ctx.extensions.get("_generated_stats"),
                        rejections=_batch_rejections,
                    )
                    await asyncio.gather(
                        *(
                            reporter.update_progress(rows_ingested, total_features)
                            for reporter in reporters
                        )
                    )
                    current_batch = []
                    current_batch_bytes = 0

            if current_batch:
                await _maybe_apply_ingest_backpressure()
                upsert_ctx = DriverContext(db_resource=engine)
                upsert_result = await catalog_module.upsert(
                    catalog_id,
                    collection_id,
                    current_batch,
                    ctx=upsert_ctx,
                    processing_context=upsert_context,
                )
                rows_ingested += len(current_batch)
                _batch_rejections = upsert_ctx.extensions.get("_rejections") or []
                _batch_persisted = (
                    upsert_result
                    if isinstance(upsert_result, list)
                    else ([upsert_result] if upsert_result else [])
                )
                rows_persisted += len(_batch_persisted)
                rows_rejected += len(_batch_rejections)
                if first_rejection_message is None and _batch_rejections:
                    first_rejection_message = (
                        _batch_rejections[0].get("message")
                        or _batch_rejections[0].get("reason")
                        or "rejected"
                    )
                _merge_index_results(
                    accumulated_index_results,
                    upsert_ctx.extensions.get("_index_results") or {},
                )
                await _broadcast_batch_outcome(
                    reporters, current_batch, upsert_result,
                    upsert_ctx.extensions.get("_generated_stats"),
                    rejections=_batch_rejections,
                )
                await asyncio.gather(
                    *(
                        reporter.update_progress(rows_ingested, total_features)
                        for reporter in reporters
                    )
                )

        ingested_extent = await recalculate_and_update_extents(
            engine, catalog_id, collection_id
        )

        # Coalesced tile-cache invalidation (one task per ingestion, not per
        # batch). Per-batch invalidation was suppressed via
        # ``defer_tile_invalidation`` above; here we enqueue a SINGLE
        # tiles_invalidate covering the whole ingested extent. Degrade-safe:
        # capability-gated inside the enqueue and never fails the ingestion.
        if rows_ingested > 0 and ingested_extent:
            try:
                from dynastore.modules.tiles.tile_cache_sync import (
                    enqueue_tile_invalidation_task,
                )

                await enqueue_tile_invalidation_task(
                    catalog_id,
                    collection_id,
                    [],
                    engine=engine,
                    schema=phys_schema,
                    prior_bboxes=ingested_extent,
                    caller_id=f"ingestion:{task_id}",
                )
            except Exception as inv_err:  # noqa: BLE001 — cache upkeep never breaks ingest
                logger.warning(
                    "Task '%s': coalesced tile invalidation failed for %s/%s: %s",
                    task_id, catalog_id, collection_id, inv_err,
                )

        # Register an informational reference: this asset feeds collection_id.
        # cascade_delete=True because the DB trigger (trg_asset_cleanup) already
        # cascades row-level cleanup when the asset is deleted — this reference is
        # purely for discoverability and audit, not for blocking deletion.
        try:
            await asset_manager.add_asset_reference(
                asset_id=asset.asset_id,
                catalog_id=catalog_id,
                ref_type=CoreAssetReferenceType.COLLECTION,
                ref_id=collection_id,
                cascade_delete=True,
                ctx=DriverContext(db_resource=engine),
            )
        except Exception as ref_err:
            logger.warning(
                f"Task '{task_id}': Could not register asset reference "
                f"({asset.asset_id} → {collection_id}): {ref_err}"
            )

        # --- Full-rejection gate (#2891) ---
        # Every row the upsert saw was rejected (0 persisted, >0 rejected):
        # the run wrote nothing, so report FAILED rather than falling into
        # the index-health check below, which treats rows_written==0 as the
        # trivial "nothing to index" COMPLETED case. Not an index miss (the
        # rows never made it to the source store), so no restore task is
        # enqueued here.
        if rows_persisted == 0 and rows_rejected > 0:
            rej_msg = (
                f"All {rows_rejected} row(s) rejected; 0 persisted. "
                f"First rejection: {first_rejection_message}"
            )
            logger.error(
                "ingestion task %s: %s/%s — %s",
                task_id, catalog_id, collection_id, rej_msg,
            )
            await asyncio.gather(
                *(
                    reporter.task_finished("FAILED", error_message=rej_msg)
                    for reporter in reporters
                )
            )
            if post_ops:
                try:
                    _cat = await catalog_module.get_catalog(
                        catalog_id, ctx=DriverContext(db_resource=engine),
                    )
                    _coll = await catalog_module.get_collection(
                        catalog_id, collection_id, ctx=DriverContext(db_resource=engine),
                    )
                    await run_post_operations(
                        post_ops, _cat, _coll, asset, "FAILED",
                        error_message=rej_msg,
                    )
                except Exception as _post_err:
                    logger.warning(
                        "Post-operations for FAILED (full rejection) errored: %s",
                        _post_err,
                    )
            raise _RejectionFailed(rej_msg)

        # --- Secondary-index health check (FIX 2 + FIX 3) ---
        # Inspect the per-indexer BulkResult totals accumulated across all
        # batches.  A total miss (succeeded==0 on all indexers for a non-zero
        # write) marks the task FAILED and enqueues an automatic restore so a
        # retry self-heals.  A partial miss keeps COMPLETED but surfaces counts.
        final_status, index_msg = _check_index_health(
            rows_written=rows_persisted,
            index_results=accumulated_index_results,
        )

        if final_status == "FAILED":
            # Enqueue an idempotent collection reindex task.  The canonical
            # create_task path opens its own managed transaction internally,
            # so the restore row is committed independently and is not rolled
            # back if the FAILED outcome causes a raise below.
            try:
                await enqueue_collection_reindex_task(
                    catalog_id=catalog_id,
                    collection_id=collection_id,
                    pg_conn=engine,
                )
            except Exception as restore_enqueue_err:
                logger.error(
                    "ingestion: restore enqueue failed for %s/%s: %s",
                    catalog_id, collection_id, restore_enqueue_err,
                )
            logger.error(
                "ingestion task %s: secondary index total miss for %s/%s "
                "(%d rows written, 0 indexed). Marking FAILED. %s",
                task_id, catalog_id, collection_id, rows_ingested, index_msg,
            )
            await asyncio.gather(
                *(
                    reporter.task_finished("FAILED", error_message=index_msg)
                    for reporter in reporters
                )
            )
            # Run post-operations for FAILED state and surface the failure.
            if post_ops:
                try:
                    _cat = await catalog_module.get_catalog(
                        catalog_id, ctx=DriverContext(db_resource=engine),
                    )
                    _coll = await catalog_module.get_collection(
                        catalog_id, collection_id, ctx=DriverContext(db_resource=engine),
                    )
                    await run_post_operations(
                        post_ops, _cat, _coll, asset, "FAILED",
                        error_message=index_msg,
                    )
                except Exception as _post_err:
                    logger.warning(
                        "Post-operations for FAILED (index miss) errored: %s",
                        _post_err,
                    )
            raise _IndexMissFailed(index_msg)

        # ``summary`` carries an optional proposed_items_schema (#1216). Every
        # reporter's ``task_finished`` accepts ``summary`` (it is on the abstract
        # base), so pass it explicitly — ``None`` when there's nothing to carry —
        # rather than a ``**dict`` splat that pyright cannot type-check.
        summary: Optional[Dict[str, Any]] = (
            {"proposed_items_schema": proposed_items_schema}
            if proposed_items_schema
            else None
        )
        if index_msg:
            # Partial miss: surface counts in summary outputs without failing.
            logger.warning(
                "ingestion task %s: %s/%s partial index miss — %s",
                task_id, catalog_id, collection_id, index_msg,
            )
            if summary is None:
                summary = {}
            summary["index_warning"] = index_msg

        if rows_rejected > 0:
            # Partial rejection: some rows persisted, some didn't (#2891).
            # rows_persisted > 0 here — a full rejection already raised
            # _RejectionFailed above — so this stays COMPLETED with counts.
            if summary is None:
                summary = {}
            summary["rejection_summary"] = {
                "persisted": rows_persisted,
                "rejected": rows_rejected,
                "first_message": first_rejection_message,
            }

        await asyncio.gather(
            *(
                reporter.task_finished("COMPLETED", summary=summary)
                for reporter in reporters
            )
        )

        # --- Run Post-Operations ---
        if post_ops:
            catalog = await catalog_module.get_catalog(catalog_id, ctx=DriverContext(db_resource=engine))
            collection = await catalog_module.get_collection(
                catalog_id, collection_id, ctx=DriverContext(db_resource=engine)
            )
            await run_post_operations(post_ops, catalog, collection, asset, "COMPLETED")

    except _IndexMissFailed:
        # task_finished("FAILED") was already called in the FAILED branch above;
        # re-raise without calling it again so reporters are not notified twice.
        raise
    except Exception as e:
        logger.critical(f"Ingestion task {task_id} failed: {e}", exc_info=True)
        await asyncio.gather(
            *(
                reporter.task_finished("FAILED", error_message=str(e))
                for reporter in reporters
            )
        )
        if post_ops:
            try:
                catalog = await catalog_module.get_catalog(
                    catalog_id, ctx=DriverContext(db_resource=engine)
                )
                collection = await catalog_module.get_collection(
                    catalog_id, collection_id, ctx=DriverContext(db_resource=engine)
                )
                await run_post_operations(
                    post_ops, catalog, collection, asset, "FAILED", error_message=str(e)
                )
            except Exception as cleanup_exc:
                logger.warning(
                    "Post-operations for FAILED state errored "
                    "(original failure is preserved and re-raised below): %s",
                    cleanup_exc,
                )

        # Re-raise the exception to ensure the caller (and tests) know the task failed.
        # The database status has already been updated to FAILED above.
        raise e
