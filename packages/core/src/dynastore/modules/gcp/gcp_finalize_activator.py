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

"""GCS OBJECT_FINALIZE inline activator.

Replaces the legacy ``GcsStorageEventTask`` hop. The Pub/Sub HTTP push
handler parses the envelope into a :class:`FinalizeEvent` and invokes
:func:`activate` directly inside ``managed_transaction`` — no task
enqueue, no second hop.

The activator is **UPDATE-only** at this layer. The ``assets`` row
must already exist in ``status='pending'`` (born during
``initiate_upload``). A finalize event with no matching PENDING row is
treated as an orphan: a warning is written via
:func:`dynastore.modules.catalog.log_manager.log_event` with
``event_type='orphan_finalize'`` and the message is acked (the bucket
still holds the blob; Stage 6 reconciliation owns recovery).

At-least-once Pub/Sub redelivery is the retry mechanism. Idempotency
contract:

* Concurrent redeliveries serialise on ``SELECT ... FOR UPDATE`` — the
  second waits for the first transaction to commit, then sees
  ``status='active'`` and short-circuits with
  ``action='already_active'`` (no UPDATE).
* When ``custom_metadata['ticket_id']`` is present, the activator
  prefers the row whose ``metadata._upload.ticket_id`` matches (rare
  race: multiple PENDING rows for the same filename, e.g. from a
  retried upload-initiation that surfaced before the first cleanup).

Failure mapping (the HTTP push handler maps these to Pub/Sub ack/nack):

* ``ActivationOutcome(action='activated' | 'already_active')`` → ack.
* :class:`OrphanFinalizeEvent` → ack (logged; non-retryable here).
* Any other exception (DB unreachable, lock timeout, asyncpg pool
  exhaustion) → propagated, the handler returns 5xx so Pub/Sub
  redelivers.
"""

from __future__ import annotations

import json
import logging
import os
from dataclasses import dataclass, field
from typing import Any, Dict, Literal, Optional

from dynastore.modules.db_config.query_executor import DQLQuery, ResultHandler

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class FinalizeEvent:
    """Synthetic-friendly representation of an OBJECT_FINALIZE GCS event.

    Constructed from a parsed Pub/Sub push body via
    :func:`finalize_event_from_pubsub` or directly in tests. Pure data,
    no I/O — keeps the activator unit-testable without spinning up a
    real Pub/Sub.
    """

    bucket: str
    object_name: str
    filename: str
    uri: str
    catalog_id: str
    collection_id: Optional[str] = None
    md5_hash: Optional[str] = None
    size_bytes: Optional[int] = None
    content_type: Optional[str] = None
    generation: Optional[str] = None
    custom_metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class ActivationOutcome:
    """Result of one finalize-event activation attempt.

    ``action`` values:

    * ``activated``       — found a PENDING row, transitioned to ACTIVE.
    * ``already_active``  — row exists at status='active' or 'deleted'
      (idempotent re-delivery — no UPDATE was performed).
    * ``orphan``          — paired with :class:`OrphanFinalizeEvent`.
    * ``mismatch``        — reserved for explicit-mismatch surfaces
      (e.g. size_bytes=0 with non-empty event); not currently raised
      by :func:`activate` itself but reserved so the handler surface
      can grow without changing callers.
    """

    asset_id: Optional[str]
    action: Literal["activated", "already_active", "orphan", "mismatch"]
    reason: Optional[str] = None


class OrphanFinalizeEvent(Exception):
    """No PENDING row matched a GCS OBJECT_FINALIZE event.

    Raised by :func:`activate` after the orphan was logged via
    ``log_event`` so the surface is observable. The blob remains in
    the bucket; Stage 6 reconciliation owns the recovery path.
    """

    def __init__(self, event: FinalizeEvent) -> None:
        self.event = event
        super().__init__(
            f"orphan_finalize: no PENDING asset for {event.catalog_id}:"
            f"{event.collection_id or '_'} filename='{event.filename}' "
            f"uri='{event.uri}'"
        )


def finalize_event_from_pubsub(
    gcs_event_payload: Dict[str, Any],
    *,
    catalog_id: str,
    collection_id: Optional[str],
) -> FinalizeEvent:
    """Build a :class:`FinalizeEvent` from a parsed Pub/Sub push body.

    ``gcs_event_payload`` is the inner ``storage#object`` resource — i.e.
    the JSON that was base64-encoded in ``message.data``. The Pub/Sub
    HTTP route is responsible for the b64 decode + JSON parse +
    catalog/collection resolution; this helper is the minimal shape
    reduction.
    """
    bucket = str(gcs_event_payload.get("bucket") or "")
    object_name = str(gcs_event_payload.get("name") or "")
    filename = os.path.basename(object_name.rstrip("/")) or object_name
    uri = f"gs://{bucket}/{object_name}" if bucket and object_name else ""

    raw_size = gcs_event_payload.get("size")
    size_bytes: Optional[int] = None
    if raw_size is not None:
        try:
            size_bytes = int(raw_size)
        except (TypeError, ValueError):
            size_bytes = None

    custom_metadata = gcs_event_payload.get("metadata") or {}
    if not isinstance(custom_metadata, dict):
        custom_metadata = {}

    generation = gcs_event_payload.get("generation")
    return FinalizeEvent(
        bucket=bucket,
        object_name=object_name,
        filename=filename,
        uri=uri,
        catalog_id=catalog_id,
        collection_id=collection_id,
        md5_hash=gcs_event_payload.get("md5Hash"),
        size_bytes=size_bytes,
        content_type=gcs_event_payload.get("contentType"),
        generation=str(generation) if generation is not None else None,
        custom_metadata=dict(custom_metadata),
    )


# SQL is module-level so unit tests can assert on its shape (e.g. that
# ``FOR UPDATE`` is present without poking at private internals).
_SELECT_PENDING_SQL = """
SELECT asset_id, status, metadata
FROM "{schema}".assets
WHERE catalog_id = :catalog_id
  AND collection_id IS NOT DISTINCT FROM :collection_id
  AND filename = :filename
  AND kind = 'physical'
  AND status IN ('pending', 'active', 'deleted')
ORDER BY (status = 'pending') DESC, created_at ASC
FOR UPDATE
""".strip()


_UPDATE_ACTIVATE_SQL = """
UPDATE "{schema}".assets
SET status = 'active',
    uri = :uri,
    content_hash = :content_hash,
    size_bytes = :size_bytes,
    owned_by = 'gcs',
    metadata = metadata - '_upload',
    updated_at = NOW()
WHERE catalog_id = :catalog_id
  AND collection_id IS NOT DISTINCT FROM :collection_id
  AND asset_id = :asset_id
""".strip()


def _select_sql(schema: str) -> str:
    return _SELECT_PENDING_SQL.format(schema=schema)


def _update_sql(schema: str) -> str:
    return _UPDATE_ACTIVATE_SQL.format(schema=schema)


def _row_get(row: Any, key: str) -> Any:
    """Read a column from either a SQLAlchemy Row or a dict.

    Allows tests to mock ``conn.execute`` with plain dicts without
    pulling in SQLAlchemy machinery.
    """
    if row is None:
        return None
    if isinstance(row, dict):
        return row.get(key)
    # SQLAlchemy Row has _mapping; some wrappers expose attributes too.
    mapping = getattr(row, "_mapping", None)
    if mapping is not None:
        return mapping.get(key)
    return getattr(row, key, None)


def _ticket_id_of(row: Any) -> Optional[str]:
    """Extract metadata._upload.ticket_id from a row, tolerating dict-or-JSON."""
    meta = _row_get(row, "metadata")
    if isinstance(meta, str):
        try:
            meta = json.loads(meta)
        except Exception:
            meta = None
    if not isinstance(meta, dict):
        return None
    upload = meta.get("_upload")
    if not isinstance(upload, dict):
        return None
    tid = upload.get("ticket_id")
    return str(tid) if tid is not None else None


async def activate(
    conn: Any,
    schema: str,
    event: FinalizeEvent,
) -> ActivationOutcome:
    """Transactionally promote a PENDING asset row to ACTIVE.

    ``conn`` must already be inside a ``managed_transaction`` so the
    ``FOR UPDATE`` lock is held until commit. We deliberately do NOT
    pass ``SKIP LOCKED`` — concurrent Pub/Sub redeliveries should wait
    for the first activation to commit and then observe the row as
    ``active`` (returning ``already_active`` cheaply).

    Match strategy:

    * Locate every non-deleted row with the same ``(catalog_id,
      collection_id, filename)`` triple at ``kind='physical'``.
    * If ``event.custom_metadata['ticket_id']`` is set, prefer the row
      whose ``metadata._upload.ticket_id`` matches it — covers the
      multi-PENDING race for the same filename.
    * Else pick the first PENDING row (oldest by ``created_at``); fall
      back to the first ACTIVE row (idempotent redelivery).

    On no match, log an ``orphan_finalize`` warning event and raise
    :class:`OrphanFinalizeEvent`. On match with status already
    ``active`` or ``deleted``, return ``already_active`` without
    issuing the UPDATE — Pub/Sub re-deliveries are normal.
    """
    select_sql = _select_sql(schema)
    rows = await DQLQuery(
        select_sql,
        result_handler=ResultHandler.ALL,
    ).execute(
        conn,
        catalog_id=event.catalog_id,
        collection_id=event.collection_id,
        filename=event.filename,
    )
    rows = rows or []

    if not rows:
        await _log_orphan(event)
        raise OrphanFinalizeEvent(event)

    # Disambiguate by ticket_id when the upload-initiation step stamped one.
    ticket_id = (event.custom_metadata or {}).get("ticket_id")
    chosen: Any = None
    if ticket_id is not None:
        for row in rows:
            if _ticket_id_of(row) == str(ticket_id):
                chosen = row
                break

    if chosen is None:
        # Prefer the first PENDING row; the SELECT's ORDER BY already put
        # PENDING ahead of ACTIVE/DELETED, so rows[0] is the right pick.
        chosen = rows[0]

    chosen_status = _row_get(chosen, "status")
    chosen_asset_id = _row_get(chosen, "asset_id")

    if chosen_status != "pending":
        # Already activated by an earlier delivery (or already deleted).
        # Either way, no UPDATE: idempotent ack.
        return ActivationOutcome(
            asset_id=str(chosen_asset_id) if chosen_asset_id is not None else None,
            action="already_active",
            reason=f"row status={chosen_status!r}",
        )

    update_sql = _update_sql(schema)
    # Tag the hash with its algorithm so cross-driver comparison via the
    # CONTENT_HASH matcher can disambiguate (GCS gives base64 MD5; future
    # backends will provide SHA-256 hex etc.). Untagged values from older
    # rows continue to compare equal as raw strings — see
    # _probe_by_content_hash for the comparison rules.
    tagged_hash = (
        f"md5:{event.md5_hash}" if event.md5_hash else None
    )
    await DQLQuery(
        update_sql,
        result_handler=ResultHandler.ROWCOUNT,
    ).execute(
        conn,
        uri=event.uri,
        content_hash=tagged_hash,
        size_bytes=event.size_bytes,
        catalog_id=event.catalog_id,
        collection_id=event.collection_id,
        asset_id=chosen_asset_id,
    )

    return ActivationOutcome(
        asset_id=str(chosen_asset_id),
        action="activated",
    )


async def _log_orphan(event: FinalizeEvent) -> None:
    """Surface an orphan finalize via the canonical log_event channel.

    Uses ``event_type='orphan_finalize'`` so operators can filter on
    ``GET /logs/catalogs/{cat}?event_type=orphan_finalize``. The log
    backend is a fire-and-forget side channel; we never let a logging
    failure derail the ack path.
    """
    try:
        from dynastore.modules.catalog.log_manager import log_event

        await log_event(
            catalog_id=event.catalog_id,
            event_type="orphan_finalize",
            level="WARNING",
            message=(
                f"GCS OBJECT_FINALIZE for '{event.uri}' has no matching "
                f"PENDING asset row — bucket retains the blob; reconciliation "
                f"will surface or sweep it."
            ),
            collection_id=event.collection_id,
            details={
                "bucket": event.bucket,
                "object_name": event.object_name,
                "filename": event.filename,
                "generation": event.generation,
                "size_bytes": event.size_bytes,
                "md5_hash": event.md5_hash,
                "ticket_id": (event.custom_metadata or {}).get("ticket_id"),
            },
        )
    except Exception as exc:  # pragma: no cover - log path is best-effort
        logger.warning(
            "Failed to record orphan_finalize log event for %s: %s",
            event.uri,
            exc,
        )
