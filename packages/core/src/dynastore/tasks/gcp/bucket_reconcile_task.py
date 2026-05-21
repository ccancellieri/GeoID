#    Copyright 2025 FAO
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

"""Bucket↔DB reconciliation task — surfaces and repairs drift.

Enforces the 1:1 invariant between bucket blobs and ``assets`` rows:

* **Orphan blob** — file in the bucket with no matching ``(catalog_id,
  collection_id, filename)`` row at ``kind='physical'`` AND
  ``status<>'deleted'``.  Reconcile imports the blob as ACTIVE
  (``owned_by='gcs:reconcile_orphan'``).  This is the recovery surface
  for ``orphan_finalize`` events from the inline finalize activator: a
  Pub/Sub event that arrived without a matching PENDING row leaves the
  blob in the bucket; reconciliation then pulls it into the catalog.

* **Ghost row** — ``kind='physical' AND status='active' AND uri IS NOT
  NULL`` but the bucket blob is gone.  Reconcile flips
  ``status='failed'`` with a structured ``_reconcile`` reason in
  ``metadata`` so operators can audit the transition.

* **Stuck PENDING** — ``status='pending' AND created_at < NOW() - ttl``.
  An upload was abandoned (or the finalize event was never delivered).
  Reconcile flips the row to ``status='failed'`` with
  ``reason='upload_abandoned'``.

The task runs in two modes — :attr:`BucketReconcileInputs.apply` toggles
between dry-run (returns the diff) and apply (mutates rows in a single
transaction).  The drift endpoint surfaces the dry-run path read-only.

Bucket I/O is intentionally narrow: list the catalog (or collection)
prefix once via the GCS storage_client, materialise blob filenames into
a set, and diff against the SQL row set in memory.  Real-bucket
integration tests cannot run locally per
``feedback_local_pubsub_events_disable.md`` — unit tests mock the
listing helper at the boundary.
"""

from __future__ import annotations

import logging
import os
from datetime import datetime, timedelta, timezone
from enum import Enum
from typing import Any, Dict, FrozenSet, List, Optional, Set, Tuple

from pydantic import BaseModel, ConfigDict, Field
from sqlalchemy import text

from dynastore.modules import get_protocol
from dynastore.modules.concurrency import run_in_thread
from dynastore.modules.db_config.query_executor import (
    DQLQuery,
    ResultHandler,
    managed_transaction,
)
from dynastore.tasks.protocols import TaskProtocol
from dynastore.models.protocols import (
    CatalogsProtocol,
    StorageProtocol,
)
from dynastore.models.tasks import TaskPayload

logger = logging.getLogger(__name__)


class DriftKind(str, Enum):
    """Categorisation surfaced in :class:`DriftEntry`."""

    ORPHAN_BLOB = "orphan_blob"
    GHOST_ROW = "ghost_row"
    STUCK_PENDING = "stuck_pending"


class DriftEntry(BaseModel):
    """Per-row diff entry surfaced in :class:`BucketReconcileReport`."""

    model_config = ConfigDict(extra="forbid")

    kind: DriftKind
    catalog_id: str
    collection_id: Optional[str] = None
    asset_id: Optional[str] = None
    filename: Optional[str] = None
    uri: Optional[str] = None
    detail: Optional[str] = None


class BucketReconcileReport(BaseModel):
    """Result of one reconcile invocation (dry-run or apply)."""

    model_config = ConfigDict(extra="forbid")

    catalog_id: str
    collection_id: Optional[str] = None
    dry_run: bool
    pending_ttl_minutes: int
    total_bucket_blobs: int = 0
    total_db_rows: int = 0
    orphans_imported: int = 0
    ghosts_marked_failed: int = 0
    stuck_pending_failed: int = 0
    drift_details: List[DriftEntry] = Field(default_factory=list)


class BucketReconcileInputs(BaseModel):
    """Inputs for ``bucket_reconcile`` task / drift endpoint."""

    model_config = ConfigDict(extra="ignore")

    catalog_id: str
    collection_id: Optional[str] = None
    apply: bool = False
    pending_ttl_minutes: int = 60


# ---------------------------------------------------------------------------
# Bucket listing — narrow synchronous helper. Mocked in unit tests at the
# ``list_bucket_blobs`` boundary so we never touch a real GCS client.
# ---------------------------------------------------------------------------


def _list_bucket_blobs_sync(
    storage_client: Any,
    bucket_name: str,
    prefix: Optional[str],
) -> List[Tuple[str, str]]:
    """Synchronously list ``(blob_path, filename)`` pairs in the bucket.

    ``filename`` is the basename (last path segment) of the blob's
    object name.  Pseudo-folder markers (zero-byte blobs whose name
    ends with ``/``) are skipped.
    """
    bucket = storage_client.bucket(bucket_name)
    out: List[Tuple[str, str]] = []
    for blob in bucket.list_blobs(prefix=prefix or None):
        name = getattr(blob, "name", None)
        if not name or name.endswith("/"):
            continue
        filename = os.path.basename(name) or name
        out.append((name, filename))
    return out


async def list_bucket_blobs(
    storage_client: Any,
    bucket_name: str,
    prefix: Optional[str],
) -> List[Tuple[str, str]]:
    """Async wrapper around :func:`_list_bucket_blobs_sync`.

    Off-loads the blocking GCS list call to a thread.  Tests can monkey-
    patch this function directly (no real client needed).
    """
    return await run_in_thread(
        _list_bucket_blobs_sync, storage_client, bucket_name, prefix
    )


# ---------------------------------------------------------------------------
# SQL helpers
# ---------------------------------------------------------------------------


_SELECT_ASSETS_SQL = """
SELECT asset_id, collection_id, filename, status, uri, owned_by, kind, created_at, metadata
FROM "{schema}".assets
WHERE catalog_id = :catalog_id
  AND status <> 'deleted'
  {collection_clause}
""".strip()


def _build_select_sql(schema: str, *, scoped_collection: bool) -> str:
    clause = "AND collection_id = :collection_id" if scoped_collection else ""
    return _SELECT_ASSETS_SQL.format(schema=schema, collection_clause=clause)


_INSERT_ORPHAN_SQL = """
INSERT INTO "{schema}".assets
    (asset_id, catalog_id, collection_id, asset_type, kind, status,
     filename, uri, owned_by, created_at, updated_at, metadata)
VALUES
    (:asset_id, :catalog_id, :collection_id, :asset_type, :kind, :status,
     :filename, :uri, :owned_by, :created_at, :updated_at, CAST(:metadata AS jsonb))
""".strip()


_UPDATE_GHOST_SQL = """
UPDATE "{schema}".assets
SET status = 'failed',
    updated_at = NOW(),
    metadata = COALESCE(metadata, '{{}}'::jsonb) || CAST(:patch AS jsonb)
WHERE catalog_id = :catalog_id
  AND collection_id IS NOT DISTINCT FROM :collection_id
  AND asset_id = :asset_id
""".strip()


_UPDATE_STUCK_PENDING_SQL = """
UPDATE "{schema}".assets
SET status = 'failed',
    updated_at = NOW(),
    metadata = COALESCE(metadata, '{{}}'::jsonb) || CAST(:patch AS jsonb)
WHERE catalog_id = :catalog_id
  AND collection_id IS NOT DISTINCT FROM :collection_id
  AND asset_id = :asset_id
  AND status = 'pending'
""".strip()


# ---------------------------------------------------------------------------
# Bucket-prefix resolution
# ---------------------------------------------------------------------------


def _resolve_listing_prefix(collection_id: Optional[str]) -> Optional[str]:
    """Return the GCS object-name prefix to scan for the given scope.

    ``None`` means "scan the whole bucket" (catalog scope) — the catalog
    bucket holds both ``catalog/`` and ``collections/`` trees, so we
    cannot narrow further without losing catalog-tier rows.
    """
    if collection_id is None:
        return None
    from dynastore.modules.gcp.tools import bucket as bucket_tool

    return bucket_tool.get_blob_path_for_collection_folder(collection_id)


def _row_collection_id(row: Any) -> Optional[str]:
    """Return ``collection_id`` from a SQL row dict (handles None)."""
    if isinstance(row, dict):
        return row.get("collection_id")
    return getattr(row, "collection_id", None)


def _row_get(row: Any, key: str) -> Any:
    if isinstance(row, dict):
        return row.get(key)
    return getattr(row, key, None)


# ---------------------------------------------------------------------------
# Core diff/apply algorithm — exposed as a free function so the drift
# endpoint can invoke it with apply=False and the task wraps it with
# apply=True. Same code path, single source of truth for the diff shape.
# ---------------------------------------------------------------------------


async def reconcile_bucket(
    inputs: BucketReconcileInputs,
    *,
    schema: str,
    bucket_name: str,
    storage_client: Any,
    engine: Any,
) -> BucketReconcileReport:
    """Compute the bucket↔DB diff for ``inputs`` and (optionally) apply fixes.

    All collaborator boundaries (storage client, engine, schema, bucket
    name) are arguments so unit tests can drive the algorithm with
    pure-Python doubles.
    """
    catalog_id = inputs.catalog_id
    collection_id = inputs.collection_id

    # ---------------------------------------------------------------- bucket
    prefix = _resolve_listing_prefix(collection_id)
    blobs = await list_bucket_blobs(storage_client, bucket_name, prefix)
    # Map basename -> blob_path. Multiple blobs sharing a basename across
    # different prefixes is rare; we keep the first hit (stable for tests).
    blob_filenames: Dict[str, str] = {}
    for blob_path, filename in blobs:
        blob_filenames.setdefault(filename, blob_path)

    # --------------------------------------------------------------- db rows
    select_sql = _build_select_sql(
        schema, scoped_collection=collection_id is not None
    )
    sql_params: Dict[str, Any] = {"catalog_id": catalog_id}
    if collection_id is not None:
        sql_params["collection_id"] = collection_id

    async with managed_transaction(engine) as conn:
        rows = await DQLQuery(
            text(select_sql), result_handler=ResultHandler.ALL_DICTS
        ).execute(conn, **sql_params)

    rows = rows or []

    # Index DB rows by (collection_id, filename) so the diff is O(N+M).
    # collection_id is normalised to "" when NULL so set-membership works.
    db_filename_index: Set[Tuple[str, str]] = set()
    for row in rows:
        fn = _row_get(row, "filename")
        if not fn:
            continue
        db_filename_index.add((_row_collection_id(row) or "", fn))

    drift_details: List[DriftEntry] = []
    orphans_imported = 0
    ghosts_marked_failed = 0
    stuck_pending_failed = 0

    now = datetime.now(timezone.utc)
    pending_cutoff = now - timedelta(minutes=inputs.pending_ttl_minutes)

    # ----------------------------------------------------- ghost / stuck pass
    ghost_targets: List[Dict[str, Any]] = []
    stuck_targets: List[Dict[str, Any]] = []

    for row in rows:
        kind = _row_get(row, "kind")
        status = _row_get(row, "status")
        filename = _row_get(row, "filename")
        row_collection = _row_collection_id(row)
        asset_id = _row_get(row, "asset_id")
        uri = _row_get(row, "uri")

        # Stuck PENDING — applies to physical rows that never received a
        # finalize event.  Virtual rows never go through PENDING in the
        # current pipeline; we still scope by kind to be defensive.
        if kind == "physical" and status == "pending":
            created_at = _row_get(row, "created_at")
            if created_at is not None and created_at < pending_cutoff:
                stuck_pending_failed += 1
                drift_details.append(
                    DriftEntry(
                        kind=DriftKind.STUCK_PENDING,
                        catalog_id=catalog_id,
                        collection_id=row_collection,
                        asset_id=asset_id,
                        filename=filename,
                        uri=uri,
                        detail=(
                            f"created_at={created_at.isoformat()} "
                            f"older than ttl={inputs.pending_ttl_minutes}m"
                        ),
                    )
                )
                stuck_targets.append(row)
            # Stuck PENDING rows are NOT additionally classed as ghost rows.
            continue

        # Ghost row — physical+active rows whose URI no longer maps to a
        # bucket object.  We compare the file by basename within the
        # listed prefix (``filename`` column is canonical).
        if kind == "physical" and status == "active" and uri:
            if filename and filename not in blob_filenames:
                ghosts_marked_failed += 1
                drift_details.append(
                    DriftEntry(
                        kind=DriftKind.GHOST_ROW,
                        catalog_id=catalog_id,
                        collection_id=row_collection,
                        asset_id=asset_id,
                        filename=filename,
                        uri=uri,
                        detail="missing_blob",
                    )
                )
                ghost_targets.append(row)

    # -------------------------------------------------------- orphan-blob pass
    # Iterate listed blobs; any whose (collection_id, filename) is absent
    # from the DB index is an orphan that we WOULD import.
    orphan_blobs: List[Tuple[str, Optional[str], str]] = []
    for blob_path, filename in blobs:
        # Resolve the scope this blob belongs to from the prefix shape:
        # ``collections/{cid}/...``  → collection_id=cid
        # ``catalog/...``            → catalog-tier (collection_id=None)
        # other prefixes             → catalog-tier (defensive default)
        blob_collection_id: Optional[str] = collection_id
        if blob_collection_id is None:
            # bucket-wide scan: derive from the path itself
            parts = blob_path.split("/", 2)
            if len(parts) >= 3 and parts[0] == "collections":
                blob_collection_id = parts[1]
            else:
                blob_collection_id = None
        key = (blob_collection_id or "", filename)
        if key in db_filename_index:
            continue
        orphans_imported += 1
        orphan_blobs.append((blob_path, blob_collection_id, filename))
        drift_details.append(
            DriftEntry(
                kind=DriftKind.ORPHAN_BLOB,
                catalog_id=catalog_id,
                collection_id=blob_collection_id,
                asset_id=None,
                filename=filename,
                uri=f"gs://{bucket_name}/{blob_path}",
                detail="no_db_row",
            )
        )

    # ------------------------------------------------------------------ apply
    if inputs.apply and (orphan_blobs or ghost_targets or stuck_targets):
        await _apply_fixes(
            engine=engine,
            schema=schema,
            catalog_id=catalog_id,
            bucket_name=bucket_name,
            now=now,
            orphan_blobs=orphan_blobs,
            ghost_targets=ghost_targets,
            stuck_targets=stuck_targets,
        )

    return BucketReconcileReport(
        catalog_id=catalog_id,
        collection_id=collection_id,
        dry_run=not inputs.apply,
        pending_ttl_minutes=inputs.pending_ttl_minutes,
        total_bucket_blobs=len(blobs),
        total_db_rows=len(rows),
        orphans_imported=orphans_imported,
        ghosts_marked_failed=ghosts_marked_failed,
        stuck_pending_failed=stuck_pending_failed,
        drift_details=drift_details,
    )


async def _apply_fixes(
    *,
    engine: Any,
    schema: str,
    catalog_id: str,
    bucket_name: str,
    now: datetime,
    orphan_blobs: List[Tuple[str, Optional[str], str]],
    ghost_targets: List[Dict[str, Any]],
    stuck_targets: List[Dict[str, Any]],
) -> None:
    """Execute INSERT/UPDATE statements for the diff inside one transaction."""
    import json
    import uuid

    insert_sql = text(_INSERT_ORPHAN_SQL.format(schema=schema))
    ghost_sql = text(_UPDATE_GHOST_SQL.format(schema=schema))
    stuck_sql = text(_UPDATE_STUCK_PENDING_SQL.format(schema=schema))

    async with managed_transaction(engine) as conn:
        # Orphan blob → INSERT new ACTIVE row.
        for blob_path, blob_collection_id, filename in orphan_blobs:
            asset_id = f"reconcile-{uuid.uuid4().hex[:12]}"
            metadata = {
                "_reconcile": {
                    "imported_at": now.isoformat(),
                    "reason": "orphan_blob",
                    "blob_path": blob_path,
                }
            }
            await DQLQuery(
                insert_sql, result_handler=ResultHandler.ROWCOUNT
            ).execute(
                conn,
                asset_id=asset_id,
                catalog_id=catalog_id,
                collection_id=blob_collection_id,
                asset_type="ASSET",
                kind="physical",
                status="active",
                filename=filename,
                uri=f"gs://{bucket_name}/{blob_path}",
                owned_by="gcs:reconcile_orphan",
                created_at=now,
                updated_at=now,
                metadata=json.dumps(metadata),
            )

        # Ghost row → UPDATE status=failed with reason=missing_blob.
        for row in ghost_targets:
            patch = {
                "_reconcile": {
                    "reason": "missing_blob",
                    "detected_at": now.isoformat(),
                }
            }
            await DQLQuery(
                ghost_sql, result_handler=ResultHandler.ROWCOUNT
            ).execute(
                conn,
                catalog_id=catalog_id,
                collection_id=_row_collection_id(row),
                asset_id=_row_get(row, "asset_id"),
                patch=json.dumps(patch),
            )

        # Stuck PENDING → UPDATE status=failed with reason=upload_abandoned.
        for row in stuck_targets:
            patch = {
                "_reconcile": {
                    "reason": "upload_abandoned",
                    "detected_at": now.isoformat(),
                }
            }
            await DQLQuery(
                stuck_sql, result_handler=ResultHandler.ROWCOUNT
            ).execute(
                conn,
                catalog_id=catalog_id,
                collection_id=_row_collection_id(row),
                asset_id=_row_get(row, "asset_id"),
                patch=json.dumps(patch),
            )


# ---------------------------------------------------------------------------
# Resolver: map (catalog_id) -> (schema, bucket_name, storage_client) using
# the same Protocol surface every other GCP task uses. Surfaces clear
# RuntimeError messages when collaborators are missing — let the dispatcher
# retry instead of dead-lettering on transient unavailability.
# ---------------------------------------------------------------------------


async def resolve_reconcile_context(
    catalog_id: str,
) -> Tuple[str, str, Any, Any]:
    """Resolve ``(schema, bucket_name, storage_client, engine)`` from protocols.

    The drift endpoint and the task body share this resolver so the
    diff path is reachable from both surfaces with no duplicated wiring.
    """
    from dynastore.tools.protocol_helpers import get_engine

    engine = get_engine()
    if engine is None:
        raise RuntimeError("Database engine not available.")

    catalogs_svc = get_protocol(CatalogsProtocol)
    if catalogs_svc is None:
        raise RuntimeError("CatalogsProtocol not available.")
    schema = await catalogs_svc.resolve_physical_schema(catalog_id)
    if not schema:
        raise RuntimeError(
            f"No physical schema for catalog '{catalog_id}'."
        )

    storage = get_protocol(StorageProtocol)
    if storage is None:
        raise RuntimeError("StorageProtocol not available.")
    bucket_name = await storage.get_storage_identifier(catalog_id)
    if not bucket_name:
        raise RuntimeError(
            f"No storage identifier registered for catalog '{catalog_id}'."
        )

    # ``StorageProtocol`` is GCP-agnostic but the reconcile path needs a
    # ``storage.Client`` to enumerate blobs.  Resolve it lazily through
    # the GCP module — the same lookup ``GcpCatalogCleanupTask`` uses.
    storage_client: Any = None
    try:
        from dynastore.modules.gcp.bucket_service import BucketService

        bucket_svc = get_protocol(BucketService)
        if bucket_svc is not None:
            storage_client = bucket_svc.storage_client
    except Exception as exc:  # pragma: no cover - defensive
        logger.debug(
            "BucketReconcileTask: BucketService lookup failed: %s", exc
        )
    if storage_client is None:
        # Fall back to a default client; matches GcpCatalogCleanupTask's
        # collection-cleanup fallback. The caller may also inject a
        # client directly via the ``storage_client`` arg of
        # :func:`reconcile_bucket` (drift endpoint does this in tests).
        import google.cloud.storage as gcs

        storage_client = gcs.Client()

    return schema, bucket_name, storage_client, engine


# ---------------------------------------------------------------------------
# Task wrapper — registered as ``bucket_reconcile`` via TaskProtocol's
# ``__init_subclass__``.
# ---------------------------------------------------------------------------


class BucketReconcileTask(TaskProtocol):
    """Apply-mode wrapper around :func:`reconcile_bucket`.

    Submitted via the same task-execute surface as other GCP tasks; the
    drift endpoint calls :func:`reconcile_bucket` directly in dry-run
    mode without going through the task framework.
    """

    task_type = "bucket_reconcile"
    priority: int = 50

    capabilities: FrozenSet[str] = frozenset()

    async def run(
        self, payload: TaskPayload[BucketReconcileInputs]
    ) -> Dict[str, Any]:
        inputs = payload.inputs
        schema, bucket_name, storage_client, engine = await resolve_reconcile_context(
            inputs.catalog_id
        )
        report = await reconcile_bucket(
            inputs,
            schema=schema,
            bucket_name=bucket_name,
            storage_client=storage_client,
            engine=engine,
        )
        return report.model_dump(mode="json")
