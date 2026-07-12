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

"""Bulk sync-write backlog offload (#3253).

``OGCTransactionMixin._ingest_items`` calls :func:`offload_bulk_remainder`
once a synchronous bulk POST crosses its collection's in-process byte/
wall-clock budget (``CollectionPluginConfig.sync_ingest_inprocess_max_bytes``
/ ``sync_ingest_inprocess_max_seconds``). This module owns the actual
spill-to-durable-storage + async-'ingestion'-process handoff so
``ogc_base.py`` stays thin.

The budget defaults to disabled (0/0 — see ``CollectionPluginConfig``): a
collection only offloads once an operator has explicitly opted it in with a
non-zero threshold. This is deliberate rather than an oversight — enabling
the budget means a large or slow item-write to that collection can spawn a
background 'ingestion' job, so operators must not turn it on for a
collection whose write policy accepts unauthenticated/anonymous callers.
Neither this module nor ``ogc_base.py`` performs any per-request
entitlement check (core stays authorization-agnostic — IAM, where
installed, is enforced solely by ``IamMiddleware``); the opt-in default is
the only gate, and it is an operator decision, not a runtime one.

Round-trip contract (load-bearing — read before touching the classifier)
--------------------------------------------------------------------------
The async 'ingestion' process reads its source through the shared GDAL/
pyogrio-backed reader pipeline (``tasks.ingestion.main_ingestion.
prepare_record_for_upsert``), which only extracts ``id`` / ``geometry`` /
``properties`` (plus ``valid_from``/``valid_to`` from request-level column
mapping) from a GeoJSON Feature — it has no extraction path for any other
top-level member. Verified empirically against pyogrio 0.13 (the reader
GeoJSON/JSON sources actually resolve to):

* A Feature's ``geometry`` and *scalar or single-level-nested-dict*
  ``properties`` values round-trip byte-for-byte.
* A **string** ``id`` does NOT survive via the reader's own top-level
  ``"id"`` — pyogrio/geopandas' ``iterfeatures()`` always overwrites that
  key with the OGR FID (an integer re-numbered from 0), even though the
  original string id is separately preserved as ``properties["id"]``. The
  identity must therefore be threaded through an explicit ``properties``
  column (:data:`_SPILL_ID_PROPERTY`) read back via
  ``column_mapping.external_id`` — never the default ``"id"`` field.
* Any OTHER top-level member (STAC's ``assets``, ``links``, ``bbox``,
  ``collection``, ``stac_version``, ``stac_extensions``, …) is silently
  flattened into ``properties`` by GDAL's GeoJSON driver — nested dicts of
  dicts (``assets``) are destructively flattened into dotted-path scalar
  columns and ``bbox`` is dropped entirely (treated as geometry metadata,
  never exposed as a field). None of this is recoverable by
  ``prepare_record_for_upsert``, which never looks past ``properties``.
* A list-of-scalars ``properties`` value round-trips as a numpy array via
  pyogrio rather than a native Python list — ``prepare_record_for_upsert``
  merges it unchanged, which is a value-type drift the write path might
  reject. Treated as not spill-safe here rather than silently accepted.

Net effect: this module can only offload items shaped like a plain OGC
Feature — ``{type, id, geometry, properties, valid_from, valid_to}`` with
JSON-scalar or single-level-nested-dict property values. STAC items (and
any Records item carrying extra top-level members) fail the classifier.
That is reported via ``BulkOffloadOutcome.shape_unsupported`` rather than a
rejection: the shape simply doesn't apply to this remainder, so
``_ingest_items`` falls back to writing it inline — exactly the pre-#3253
behaviour — instead of refusing data the collection would otherwise have
accepted. Operators of a collection whose items never qualify should still
leave (or set) ``sync_ingest_inprocess_max_bytes=0`` (the default) to skip
the wasted per-flush classification check entirely.
"""

from __future__ import annotations

import hashlib
import json
import logging
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, List, Optional
from uuid import uuid4

from dynastore.extensions.ogc_models_shared import SidecarRejection
from dynastore.tasks.ingestion.ingestion_models import TaskIngestionRequest

if TYPE_CHECKING:
    from dynastore.models.driver_context import DriverContext

logger = logging.getLogger(__name__)

# Reserved properties key carrying the item's original identity through the
# spill file. Never "id" — see the module docstring's round-trip contract.
_SPILL_ID_PROPERTY = "__dynastore_offload_id"

# Top-level GeoJSON-Feature members the ingestion reader pipeline actually
# extracts (see module docstring). Any item carrying a member outside this
# set cannot round-trip and is classified as not spill-safe.
_SPILL_SAFE_TOP_LEVEL_KEYS = frozenset(
    {"type", "id", "geometry", "properties", "valid_from", "valid_to"}
)

_JSON_SCALAR_TYPES = (str, int, float, bool)


@dataclass
class BulkOffloadOutcome:
    """Result of an :func:`offload_bulk_remainder` call — one of four
    outcomes:

    * **Success** — ``job_id is not None``: the remainder was durably
      spilled AND its 'ingestion' process job was enqueued. The
      acknowledged-set discipline (#2825) this module must honour: nothing
      is reported deferred until spill+enqueue actually happened.
    * **Not applicable** — ``shape_unsupported=True``: the remainder's
      item shape cannot round-trip through the ingestion reader (e.g.
      STAC's ``assets``/``links``/``bbox``). This is not a failure — the
      caller (``OGCTransactionMixin._ingest_items``) falls back to writing
      the remainder inline, exactly as it would have before #3253. No I/O
      is attempted and ``rejections`` stays empty.
    * **Already in flight** — ``dedup_hit=True``: the exact same remainder
      (by content hash — see the ``dedup_key`` note on
      :func:`_spill_and_enqueue`) already has a non-terminal 'ingestion'
      job enqueued, so this call's own spill file is discarded (best
      effort) and no second job is created. There is no new job handle to
      report, so the caller falls back to writing the remainder inline —
      identical to the ``shape_unsupported`` fallback, and safe for the
      same reason: the write path is an idempotent upsert, so this
      request's own inline write and the in-flight job's write of the
      same content simply converge on the same rows.
    * **Failure** — ``job_id is None`` and neither ``shape_unsupported``
      nor ``dedup_hit`` is set: a genuine spill-write or enqueue I/O
      failure. Every item in the attempted remainder is represented in
      ``rejections`` instead — never silently dropped, never silently
      claimed accepted.
    """

    job_id: Optional[str]
    monitor_url: Optional[str]
    count: int
    rejections: List[SidecarRejection] = field(default_factory=list)
    shape_unsupported: bool = False
    dedup_hit: bool = False


def _is_json_scalar(value: Any) -> bool:
    return value is None or isinstance(value, _JSON_SCALAR_TYPES)


def _properties_are_spill_safe(properties: Any) -> bool:
    """True iff every value in *properties* survives the reader round-trip.

    Scalars and single-level-nested dicts of scalars are safe (verified
    empirically — pyogrio decodes a JSON-subtype OGR field back to a native
    Python dict). A list value is NOT: it round-trips as a numpy array
    rather than a native list, which the reader's merge does not normalise
    back — treated as unsafe here rather than risking a write-path failure
    or a silently different value type downstream.
    """
    if properties is None:
        return True
    if not isinstance(properties, dict):
        return False
    for value in properties.values():
        if _is_json_scalar(value):
            continue
        if isinstance(value, dict):
            if not all(_is_json_scalar(v) for v in value.values()):
                return False
            continue
        return False
    return True


def _normalize_item(item: Any) -> Optional[dict]:
    """Return *item* as a plain dict (via ``model_dump`` for Pydantic
    models), or ``None`` when it is neither a dict nor dumpable."""
    dump = getattr(item, "model_dump", None)
    if callable(dump):
        try:
            data = dump(by_alias=True, exclude_none=True)
        except TypeError:
            data = dump()
    elif isinstance(item, dict):
        data = item
    else:
        return None
    return data if isinstance(data, dict) else None


def _classify(item: Any) -> Optional[dict]:
    """Return the normalised item dict when it can round-trip cleanly
    through the ingestion reader pipeline, else ``None``.

    See the module docstring for the exact round-trip contract this
    enforces. Rejects anything shaped like a STAC Item (``assets``,
    ``links``, ``bbox``, ``stac_extensions``, ``collection``,
    ``stac_version`` are all outside :data:`_SPILL_SAFE_TOP_LEVEL_KEYS`),
    anything with a missing/non-scalar ``id``, a non-dict ``geometry``, or
    list-valued properties.
    """
    data = _normalize_item(item)
    if data is None:
        return None
    if set(data.keys()) - _SPILL_SAFE_TOP_LEVEL_KEYS:
        return None
    item_id = data.get("id")
    if item_id is None or not isinstance(item_id, (str, int)):
        return None
    geometry = data.get("geometry")
    if geometry is not None and not isinstance(geometry, dict):
        return None
    if not _properties_are_spill_safe(data.get("properties")):
        return None
    return data


def _item_identity(item: Any) -> Optional[str]:
    """Best-effort id extraction for a rejection record — never raises."""
    if isinstance(item, dict):
        value = item.get("id")
    else:
        value = getattr(item, "id", None)
    return str(value) if value is not None else None


def _reject_all(items: List[Any], *, policy_source: str, reason: str, message: str) -> BulkOffloadOutcome:
    rejections = [
        SidecarRejection(
            external_id=_item_identity(item),
            reason=reason,
            message=message,
            policy_source=policy_source,
        )
        for item in items
    ]
    return BulkOffloadOutcome(job_id=None, monitor_url=None, count=0, rejections=rejections)


async def _delete_spill_best_effort(storage: Any, target_path: str, *, reason: str) -> None:
    """Best-effort removal of a spill file this call wrote but that
    nothing will ever read. Never raises: a leaked file is harmless
    clutter, never a correctness issue, so a cleanup failure must not mask
    (or replace) whatever real error the caller is already handling.
    """
    try:
        await storage.delete_file(target_path)
    except Exception:  # noqa: BLE001 — best-effort cleanup only
        logger.warning(
            "offload_bulk_remainder: failed to remove orphaned spill file "
            "%s (%s).",
            target_path, reason, exc_info=True,
        )


async def _spill_and_enqueue(
    catalog_id: str,
    collection_id: str,
    normalized_items: List[dict],
    *,
    ctx: "DriverContext",
    caller_id: str,
) -> "Optional[tuple[str, Optional[str]]]":
    """Write *normalized_items* to durable storage and enqueue the
    'ingestion' process against it. Raises on any failure — the caller
    (:func:`offload_bulk_remainder`) turns that into rejections.

    Returns ``None`` on a dedup hit (see the ``dedup_key`` note below):
    an equivalent job is already in flight, so this call's own spill file
    is removed (best effort) and there is no job handle to report.
    """
    from dynastore.models.protocols.storage import StorageProtocol
    from dynastore.tools.discovery import get_protocol

    storage = get_protocol(StorageProtocol)
    if storage is None:
        raise RuntimeError("StorageProtocol not available for bulk-offload spill.")

    base_path = await storage.get_collection_storage_path(catalog_id, collection_id)
    if not base_path:
        raise RuntimeError(
            f"No storage path resolved for {catalog_id}/{collection_id}."
        )

    features = []
    for data in normalized_items:
        props = dict(data.get("properties") or {})
        # Identity passthrough (see module docstring) — never rely on the
        # reader's own "id" extraction for a string id.
        props[_SPILL_ID_PROPERTY] = str(data["id"])
        features.append({
            "type": "Feature",
            "id": data["id"],
            "geometry": data.get("geometry"),
            "properties": props,
        })
    body = json.dumps({"type": "FeatureCollection", "features": features}).encode("utf-8")

    target_path = f"{base_path.rstrip('/')}/_sync_offload/{uuid4().hex}.geojson"
    await storage.upload_file_content(target_path, body, content_type="application/geo+json")

    # From here on, *target_path* is a durably-written file that nothing
    # references yet — a retry generates a fresh uuid4 path, so a failure
    # here never gets swept up by a later retry's cleanup. Whether it must
    # be deleted depends on whether a job could possibly still be reading
    # it:
    #   * ctx.db_resource missing, or execute_process raising outright —
    #     enqueue never happened (or definitively failed before one
    #     could), so nothing can be reading target_path: delete it.
    #   * execute_process returning a *non-None* result we merely failed
    #     to extract a job id from (below) is NOT in that bucket — a
    #     non-None return already proves a runner claimed the work, so a
    #     task may genuinely exist and be reading target_path right now.
    #     Deleting there would risk yanking the file out from under it —
    #     handled separately, after this try block, and does NOT delete.
    try:
        if ctx.db_resource is None:
            raise RuntimeError(
                "DriverContext.db_resource is required to enqueue the ingestion process."
            )

        from dynastore.modules.processes import models as proc_models
        from dynastore.modules.processes.processes_module import execute_process

        # Stable across retries of the identical remainder — catalog,
        # collection, and a hash of the exact spilled content — so a client
        # retry of the same bulk POST hashes to the same key. Deliberately
        # NOT derived from target_path, which carries a fresh uuid4 on
        # every call and would defeat dedup entirely. execute_process
        # forwards this to TaskCreate, where the DB's partial unique index
        # on (schema, dedup_key) for non-terminal tasks collapses a
        # redelivered retry into the existing job instead of spawning a
        # second one (#3253 Finding 3).
        dedup_key = (
            f"sync_ingest_offload:{catalog_id}:{collection_id}:"
            f"{hashlib.sha256(body).hexdigest()}"
        )

        # Built via model_validate (nested dicts), not direct field=...
        # kwargs, so a partial payload (only asset/column_mapping set,
        # every other TaskIngestionRequest field left at its declared
        # default) round-trips through pydantic's normal validation path.
        task_request = TaskIngestionRequest.model_validate({
            "asset": {"uri": target_path},
            "column_mapping": {"external_id": _SPILL_ID_PROPERTY},
        })
        exec_request = proc_models.ExecuteRequest(
            inputs={"ingestion_request": task_request.model_dump(mode="json", exclude_none=True)},
        )
        result = await execute_process(
            "ingestion",
            exec_request,
            # execute_process's DB write goes through managed_transaction,
            # which is re-entrant over an already-open connection (nested
            # SAVEPOINT), not just a bare engine — see
            # query_executor.managed_transaction. ctx.db_resource is
            # narrower at runtime than DbEngine's declared type here.
            engine=ctx.db_resource,  # type: ignore[arg-type]
            caller_id=caller_id,
            preferred_mode=proc_models.JobControlOptions.ASYNC_EXECUTE,
            catalog_id=catalog_id,
            collection_id=collection_id,
            dedup_key=dedup_key,
        )
    except Exception:
        await _delete_spill_best_effort(
            storage, target_path,
            reason="enqueue failed before any job could exist",
        )
        raise

    if result is None:
        # Dedup hit: execution_engine.execute() returns None when a
        # non-terminal task with this dedup_key already exists (a retry of
        # this exact remainder) rather than spawning a second job. There is
        # no new job to report — the file just spilled above is a
        # redundant duplicate of whatever the in-flight job is already
        # reading, so remove it (best effort) and tell the caller to fall
        # back to an inline write instead of fabricating a job handle that
        # does not exist.
        await _delete_spill_best_effort(
            storage, target_path, reason=f"dedup hit on {dedup_key}",
        )
        return None

    job_id: Optional[str] = None
    for attr in ("jobID", "job_id", "task_id", "id"):
        val = getattr(result, attr, None)
        if val is not None:
            job_id = str(val)
            break
    if job_id is None:
        # See the try block's comment above: a non-None result already
        # proves a runner claimed the work, so a task may genuinely exist
        # and be reading target_path right now — do NOT delete it here,
        # only report the failure to extract a usable job id from it.
        raise RuntimeError("ingestion process execution returned no job id.")

    monitor_url = f"/processes/catalogs/{catalog_id}/collections/{collection_id}/jobs/{job_id}"
    return job_id, monitor_url


async def offload_bulk_remainder(
    catalog_id: str,
    collection_id: str,
    items: List[Any],
    *,
    ctx: "DriverContext",
    policy_source: str,
    caller_id: Optional[str] = None,
) -> BulkOffloadOutcome:
    """Attempt to defer *items* (the un-flushed remainder of a bulk-ingest
    sub-batching loop) to the async 'ingestion' process.

    Returns a :class:`BulkOffloadOutcome`. On success every item is
    represented only by ``count`` (the caller never re-derives per-item
    ids for a deferred item — they are not committed yet). When the shape
    does not round-trip through the ingestion reader,
    ``shape_unsupported=True`` is returned with an empty ``rejections`` —
    this is not a failure, it tells the caller to fall back to writing
    *items* inline (#3253 Finding 2). Likewise, when an equivalent job for
    this exact remainder is already in flight (retried request, same
    ``dedup_key``), ``dedup_hit=True`` is returned with an empty
    ``rejections`` — same fallback, no second job. On a genuine I/O
    failure (spill write or enqueue error) every item in *items* is turned
    into a :class:`SidecarRejection` — acknowledged-set discipline
    (#2825): a remainder this function cannot durably hand off is never
    silently claimed accepted.
    """
    if not items:
        return BulkOffloadOutcome(job_id=None, monitor_url=None, count=0, rejections=[])

    normalized: List[dict] = []
    for item in items:
        safe = _classify(item)
        if safe is None:
            # Shape mismatch, not a failure — see BulkOffloadOutcome and
            # the module docstring's round-trip contract. No I/O has been
            # attempted yet, so there is nothing to unwind.
            return BulkOffloadOutcome(
                job_id=None, monitor_url=None, count=0, rejections=[],
                shape_unsupported=True,
            )
        normalized.append(safe)

    try:
        spilled = await _spill_and_enqueue(
            catalog_id, collection_id, normalized,
            ctx=ctx, caller_id=caller_id or "system:sync_ingest_offload",
        )
    except Exception as exc:  # noqa: BLE001 — surfaced as rejections, never raised
        logger.warning(
            "offload_bulk_remainder: spill/enqueue failed for %s/%s (%d item(s)): %s",
            catalog_id, collection_id, len(items), exc, exc_info=True,
        )
        return _reject_all(
            items,
            policy_source=policy_source,
            reason="async_offload_failed",
            message=f"Failed to defer to the async ingestion process: {exc}",
        )

    if spilled is None:
        # Dedup hit — see BulkOffloadOutcome. Not a failure: an equivalent
        # job for this exact remainder is already in flight.
        return BulkOffloadOutcome(
            job_id=None, monitor_url=None, count=0, rejections=[],
            dedup_hit=True,
        )

    job_id, monitor_url = spilled
    return BulkOffloadOutcome(
        job_id=job_id, monitor_url=monitor_url, count=len(items), rejections=[],
    )
