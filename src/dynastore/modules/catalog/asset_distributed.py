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

"""Asset write-policy chain runner — evaluates :class:`AssetsWritePolicy`.

Mirrors :mod:`dynastore.modules.catalog.item_distributed` (items side):

1. Walks the ``policy.identity_matchers`` chain in order, first match wins.
2. Dispatches the configured :class:`AssetWriteConflictPolicy`:
   ``REFUSE_FAIL`` raises :class:`AssetSidecarRejectedError`, ``UPDATE`` /
   ``NEW_VERSION`` mutate / archive+insert, ``REFUSE`` / ``REFUSE_RETURN``
   short-circuit.

This module is **additive**: it owns the policy logic only, not the upload
flow. Stage 4.1 will route upload-create through :func:`upsert_asset` with
``initial_status=PENDING``; Stage 4.2 will route the GCS finalize event
through here too.

The PostgreSQL DDL in
:mod:`dynastore.modules.catalog.drivers.pg_asset_driver` declares the
partial unique indexes ``WHERE status <> 'deleted'``, so ``NEW_VERSION``'s
"archive" step simply sets ``status='deleted'`` on the existing row — the
new INSERT with the same ``asset_id`` no longer collides.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Awaitable, Callable, Dict, Literal, Optional, Tuple, Union

from dynastore.modules.catalog.asset_service import (
    AssetCreate,
    AssetKind,
    AssetStatus,
    VirtualAssetCreate,
)
from dynastore.modules.catalog.write_policy_assets import (
    AssetIdentityMatcher,
    AssetsWritePolicy,
    AssetWriteConflictPolicy,
)
from dynastore.modules.db_config.query_executor import (
    DbResource,
    DQLQuery,
    ResultHandler,
)

logger = logging.getLogger(__name__)


AssetCreatePayload = Union[AssetCreate, VirtualAssetCreate]
UpsertAction = Literal[
    "inserted_pending",
    "inserted_active",
    "updated",
    "new_version",
    "refused",
    "returned_existing",
]


# ---------------------------------------------------------------------------
# Public dataclasses & error type
# ---------------------------------------------------------------------------


@dataclass
class Scope:
    """Address of the (catalog, optional collection) the write targets.

    ``collection_id`` is ``None`` for catalog-tier assets — the partial
    unique indexes on ``assets`` use ``UNIQUE NULLS NOT DISTINCT`` so this
    NULL is meaningful (catalog-tier rows are unique on
    ``(catalog_id, NULL, asset_id)``).
    """

    schema: str
    catalog_id: str
    collection_id: Optional[str] = None


@dataclass
class UpsertResult:
    """Outcome of :func:`upsert_asset`.

    Carries the row dict (post-insert / post-update / matched-existing) and
    the matcher that triggered the conflict path, when any.
    """

    action: UpsertAction
    row: Dict[str, Any]
    matcher_hit: Optional[AssetIdentityMatcher] = None
    metadata: Dict[str, Any] = field(default_factory=dict)


class AssetSidecarRejectedError(Exception):
    """Raised when :class:`AssetsWritePolicy` refuses an incoming asset.

    Mirrors :class:`dynastore.modules.storage.errors.SidecarRejectedError`
    (items side) so bulk callers can build a unified
    :class:`dynastore.extensions.ogc_models_shared.IngestionReport` from
    both surfaces.

    ``reason`` is a short machine-readable code:

    * ``"conflict"`` — identity matched and ``policy.on_conflict ==
      REFUSE_FAIL``.
    * ``"versioning_unsupported"`` — ``NEW_VERSION`` requested but the
      driver lacks the capability (caller wraps this).
    """

    def __init__(
        self,
        message: str,
        *,
        asset_id: str,
        matcher: str,
        reason: str = "conflict",
        existing_id: Optional[str] = None,
    ) -> None:
        super().__init__(message)
        self.asset_id = asset_id
        self.matcher = matcher
        self.reason = reason
        self.message = message
        self.existing_id = existing_id


# ---------------------------------------------------------------------------
# SQL probes — one per matcher
# ---------------------------------------------------------------------------


_BASE_SELECT_COLS = (
    "asset_id, catalog_id, collection_id, asset_type, kind, status, "
    "filename, href, uri, content_hash, size_bytes, created_at, updated_at, "
    "metadata, owned_by"
)


async def _probe_by_asset_id(
    conn: DbResource,
    scope: Scope,
    payload: AssetCreatePayload,
    policy: AssetsWritePolicy,
) -> Optional[Dict[str, Any]]:
    """Match by the canonical (catalog_id, collection_id, asset_id) key."""
    sql = (
        f'SELECT {_BASE_SELECT_COLS} FROM "{scope.schema}".assets '
        "WHERE catalog_id = :catalog_id "
        "AND collection_id IS NOT DISTINCT FROM :collection_id "
        "AND asset_id = :asset_id "
        "AND status <> 'deleted' "
        "LIMIT 1"
    )
    row = await DQLQuery(sql, result_handler=ResultHandler.ONE_OR_NONE).execute(
        conn,
        catalog_id=scope.catalog_id,
        collection_id=scope.collection_id,
        asset_id=payload.asset_id,
    )
    return _row_to_dict(row)


async def _probe_by_filename(
    conn: DbResource,
    scope: Scope,
    payload: AssetCreatePayload,
    policy: AssetsWritePolicy,
) -> Optional[Dict[str, Any]]:
    """Match by ``filename`` within the same scope (physical assets only)."""
    filename = getattr(payload, "filename", None)
    if not filename:
        return None
    sql = (
        f'SELECT {_BASE_SELECT_COLS} FROM "{scope.schema}".assets '
        "WHERE catalog_id = :catalog_id "
        "AND collection_id IS NOT DISTINCT FROM :collection_id "
        "AND filename = :filename "
        "AND kind = 'physical' "
        "AND status <> 'deleted' "
        "LIMIT 1"
    )
    row = await DQLQuery(sql, result_handler=ResultHandler.ONE_OR_NONE).execute(
        conn,
        catalog_id=scope.catalog_id,
        collection_id=scope.collection_id,
        filename=filename,
    )
    return _row_to_dict(row)


async def _probe_by_url(
    conn: DbResource,
    scope: Scope,
    payload: AssetCreatePayload,
    policy: AssetsWritePolicy,
) -> Optional[Dict[str, Any]]:
    """Match by ``uri`` (physical/active) OR ``href`` (virtual).

    For ``AssetCreate`` (physical) we look at ``payload.uri`` — present only
    for storage-event creates that already know the URI; REST flows leave it
    None and the matcher returns no hit (correct: a REST upload-create has
    no URL to compare yet, the finalize event sets it).

    For ``VirtualAssetCreate`` we look at ``payload.href``.
    """
    incoming_uri: Optional[str] = None
    incoming_href: Optional[str] = None
    if isinstance(payload, AssetCreate):
        incoming_uri = payload.uri
    elif isinstance(payload, VirtualAssetCreate):
        incoming_href = payload.href

    if not incoming_uri and not incoming_href:
        return None

    if incoming_uri:
        sql = (
            f'SELECT {_BASE_SELECT_COLS} FROM "{scope.schema}".assets '
            "WHERE catalog_id = :catalog_id "
            "AND collection_id IS NOT DISTINCT FROM :collection_id "
            "AND kind = 'physical' AND status = 'active' AND uri = :uri "
            "LIMIT 1"
        )
        row = await DQLQuery(
            sql, result_handler=ResultHandler.ONE_OR_NONE
        ).execute(
            conn,
            catalog_id=scope.catalog_id,
            collection_id=scope.collection_id,
            uri=incoming_uri,
        )
        hit = _row_to_dict(row)
        if hit:
            return hit

    if incoming_href:
        sql = (
            f'SELECT {_BASE_SELECT_COLS} FROM "{scope.schema}".assets '
            "WHERE catalog_id = :catalog_id "
            "AND collection_id IS NOT DISTINCT FROM :collection_id "
            "AND kind = 'virtual' AND status <> 'deleted' AND href = :href "
            "LIMIT 1"
        )
        row = await DQLQuery(
            sql, result_handler=ResultHandler.ONE_OR_NONE
        ).execute(
            conn,
            catalog_id=scope.catalog_id,
            collection_id=scope.collection_id,
            href=incoming_href,
        )
        return _row_to_dict(row)

    return None


async def _probe_by_metadata_field(
    conn: DbResource,
    scope: Scope,
    payload: AssetCreatePayload,
    policy: AssetsWritePolicy,
) -> Optional[Dict[str, Any]]:
    """Match by a JSON-path lookup on ``metadata``.

    The path comes from :attr:`AssetsWritePolicy.metadata_match_path` and is
    a dot-notation expression (``"a.b.c"``).  We resolve the value from the
    incoming payload's metadata dict and compare with the persisted column
    via PostgreSQL's ``#>>`` operator.
    """
    path = policy.metadata_match_path
    if not path:
        return None

    incoming_meta = getattr(payload, "metadata", None) or {}
    incoming_value = _walk_dot_path(incoming_meta, path)
    if incoming_value is None:
        return None

    # Build the PG json-path array literal from the dot path: "a.b.c" → '{a,b,c}'.
    pg_path = "{" + ",".join(path.split(".")) + "}"
    sql = (
        f'SELECT {_BASE_SELECT_COLS} FROM "{scope.schema}".assets '
        "WHERE catalog_id = :catalog_id "
        "AND collection_id IS NOT DISTINCT FROM :collection_id "
        f"AND metadata #>> '{pg_path}' = :value "
        "AND status <> 'deleted' "
        "LIMIT 1"
    )
    row = await DQLQuery(sql, result_handler=ResultHandler.ONE_OR_NONE).execute(
        conn,
        catalog_id=scope.catalog_id,
        collection_id=scope.collection_id,
        value=str(incoming_value),
    )
    return _row_to_dict(row)


async def _probe_by_content_hash(
    conn: DbResource,
    scope: Scope,
    payload: AssetCreatePayload,
    policy: AssetsWritePolicy,
) -> Optional[Dict[str, Any]]:
    """Match by ``content_hash`` (physical assets only, post-finalize).

    Returns no hit for ``AssetCreate`` payloads at upload-create time —
    the hash is set by the finalize event in Stage 4.2, so until then the
    incoming payload has nothing to compare. The probe is still wired so
    Stage 4.2's finalize-event path benefits from it directly.
    """
    incoming_hash = getattr(payload, "content_hash", None)
    if not incoming_hash:
        return None
    sql = (
        f'SELECT {_BASE_SELECT_COLS} FROM "{scope.schema}".assets '
        "WHERE catalog_id = :catalog_id "
        "AND collection_id IS NOT DISTINCT FROM :collection_id "
        "AND kind = 'physical' "
        "AND content_hash = :content_hash "
        "AND status <> 'deleted' "
        "LIMIT 1"
    )
    row = await DQLQuery(sql, result_handler=ResultHandler.ONE_OR_NONE).execute(
        conn,
        catalog_id=scope.catalog_id,
        collection_id=scope.collection_id,
        content_hash=incoming_hash,
    )
    return _row_to_dict(row)


# Probe lookup map: matcher → coroutine.
ProbeFn = Callable[
    [DbResource, Scope, AssetCreatePayload, AssetsWritePolicy],
    Awaitable[Optional[Dict[str, Any]]],
]
PROBES: Dict[AssetIdentityMatcher, ProbeFn] = {
    AssetIdentityMatcher.ASSET_ID: _probe_by_asset_id,
    AssetIdentityMatcher.FILENAME: _probe_by_filename,
    AssetIdentityMatcher.URL: _probe_by_url,
    AssetIdentityMatcher.METADATA_FIELD: _probe_by_metadata_field,
    AssetIdentityMatcher.CONTENT_HASH: _probe_by_content_hash,
}


# ---------------------------------------------------------------------------
# Chain runner & action dispatcher
# ---------------------------------------------------------------------------


async def _resolve(
    conn: DbResource,
    scope: Scope,
    payload: AssetCreatePayload,
    policy: AssetsWritePolicy,
) -> Tuple[Optional[Dict[str, Any]], Optional[AssetIdentityMatcher]]:
    """Walk the policy chain; first matcher returning a row wins."""
    for matcher in policy.identity_matchers:
        probe = PROBES.get(matcher)
        if probe is None:
            # Unknown matcher: silently skip so a policy can opt into a
            # matcher that requires a backend not yet wired (mirrors items'
            # forgiving stance for forward compat).
            continue
        row = await probe(conn, scope, payload, policy)
        if row:
            return row, matcher
    return None, None


async def upsert_asset(
    conn: DbResource,
    scope: Scope,
    payload: AssetCreatePayload,
    policy: AssetsWritePolicy,
    *,
    initial_status: AssetStatus = AssetStatus.ACTIVE,
) -> UpsertResult:
    """Resolve identity via the policy chain; INSERT / UPDATE / archive per action.

    Stage 3 callers should default ``initial_status`` to ``ACTIVE``. Stage 4.1's
    upload-create will pass ``PENDING`` so the finalize event in Stage 4.2 can
    transition the row to ``ACTIVE`` once the blob lands.

    Raises :class:`AssetSidecarRejectedError` when the policy says
    ``REFUSE_FAIL`` (or when ``NEW_VERSION`` is requested without driver
    versioning support — the caller decides how to surface that as a typed
    response).
    """
    # 1. Resolve identity via the matcher chain (skipped entirely for
    #    NEW_VERSION since we always want a fresh row in that mode).
    existing: Optional[Dict[str, Any]] = None
    matcher_hit: Optional[AssetIdentityMatcher] = None
    if policy.on_conflict != AssetWriteConflictPolicy.NEW_VERSION:
        existing, matcher_hit = await _resolve(conn, scope, payload, policy)

    # 2. Hash gating — when enabled and the existing row's content_hash
    #    matches the incoming one, short-circuit the conflict action so
    #    re-finalising the same blob is a no-op.
    on_conflict = policy.on_conflict
    incoming_hash = getattr(payload, "content_hash", None)
    if (
        existing
        and policy.skip_if_unchanged_content_hash
        and incoming_hash
        and existing.get("content_hash") == incoming_hash
    ):
        if on_conflict == AssetWriteConflictPolicy.NEW_VERSION:
            on_conflict = AssetWriteConflictPolicy.REFUSE_RETURN
        elif on_conflict == AssetWriteConflictPolicy.UPDATE:
            on_conflict = AssetWriteConflictPolicy.REFUSE_RETURN

    # 3. NEW_VERSION special case: no matcher_hit yet (we skipped the chain
    #    for NEW_VERSION). Probe by asset_id specifically — versioning is
    #    only meaningful when there is a prior row to archive.
    if policy.on_conflict == AssetWriteConflictPolicy.NEW_VERSION:
        existing = await _probe_by_asset_id(conn, scope, payload, policy)
        if existing:
            matcher_hit = AssetIdentityMatcher.ASSET_ID

    # 4. No match → fresh INSERT.
    if existing is None:
        inserted = await _insert_new_row(conn, scope, payload, initial_status)
        action: UpsertAction = (
            "inserted_pending"
            if initial_status == AssetStatus.PENDING
            else "inserted_active"
        )
        return UpsertResult(action=action, row=inserted, matcher_hit=None)

    # 5. Dispatch the conflict action.
    matcher_str = (matcher_hit.value if matcher_hit else "unknown")
    existing_id = str(existing.get("asset_id"))

    if on_conflict == AssetWriteConflictPolicy.REFUSE_FAIL:
        logger.info(
            "AssetsWritePolicy REFUSE_FAIL via matcher=%s (asset_id=%s)",
            matcher_str,
            existing_id,
        )
        raise AssetSidecarRejectedError(
            f"Asset write refused: identity matched via {matcher_str} "
            f"(existing asset_id={existing_id}); policy=REFUSE_FAIL",
            asset_id=payload.asset_id,
            matcher=matcher_str,
            reason="conflict",
            existing_id=existing_id,
        )

    if on_conflict == AssetWriteConflictPolicy.REFUSE:
        logger.info(
            "AssetsWritePolicy REFUSE via matcher=%s (asset_id=%s); "
            "skipping write silently",
            matcher_str,
            existing_id,
        )
        return UpsertResult(action="refused", row=existing, matcher_hit=matcher_hit)

    if on_conflict == AssetWriteConflictPolicy.REFUSE_RETURN:
        logger.info(
            "AssetsWritePolicy REFUSE_RETURN via matcher=%s (asset_id=%s); "
            "echoing existing row",
            matcher_str,
            existing_id,
        )
        return UpsertResult(
            action="returned_existing", row=existing, matcher_hit=matcher_hit
        )

    if on_conflict == AssetWriteConflictPolicy.UPDATE:
        updated = await _update_row(conn, scope, payload, existing)
        return UpsertResult(action="updated", row=updated, matcher_hit=matcher_hit)

    if on_conflict == AssetWriteConflictPolicy.NEW_VERSION:
        if not policy.requires_driver_versioning():
            # Defensive — should never trip given the enum check above, but
            # keeps the contract explicit.
            raise AssetSidecarRejectedError(
                "NEW_VERSION requested but policy.requires_driver_versioning() "
                "is False — refusing to silently UPDATE.",
                asset_id=payload.asset_id,
                matcher=matcher_str,
                reason="versioning_unsupported",
                existing_id=existing_id,
            )
        await _archive_row(conn, scope, existing)
        inserted = await _insert_new_row(conn, scope, payload, initial_status)
        return UpsertResult(
            action="new_version", row=inserted, matcher_hit=matcher_hit
        )

    # Defensive: enum exhausted by the branches above.
    raise AssetSidecarRejectedError(
        f"Unhandled AssetWriteConflictPolicy {on_conflict!r}",
        asset_id=payload.asset_id,
        matcher=matcher_str,
        reason="conflict",
        existing_id=existing_id,
    )


# ---------------------------------------------------------------------------
# Mutation helpers — INSERT / UPDATE / archive
# ---------------------------------------------------------------------------


async def _insert_new_row(
    conn: DbResource,
    scope: Scope,
    payload: AssetCreatePayload,
    initial_status: AssetStatus,
) -> Dict[str, Any]:
    """INSERT a fresh row from the payload + chosen initial status."""
    now = datetime.now(timezone.utc)
    kind_val = payload.kind.value if hasattr(payload.kind, "value") else str(payload.kind)
    asset_type_val = (
        payload.asset_type.value
        if hasattr(payload.asset_type, "value")
        else str(payload.asset_type)
    )

    filename = getattr(payload, "filename", None)
    href = getattr(payload, "href", None)
    uri = getattr(payload, "uri", None) if isinstance(payload, AssetCreate) else None

    sql = (
        f'INSERT INTO "{scope.schema}".assets ('
        "asset_id, catalog_id, collection_id, asset_type, kind, status, "
        "filename, href, uri, content_hash, size_bytes, "
        "created_at, updated_at, metadata, owned_by"
        ") VALUES ("
        ":asset_id, :catalog_id, :collection_id, :asset_type, :kind, :status, "
        ":filename, :href, :uri, :content_hash, :size_bytes, "
        ":created_at, :updated_at, CAST(:metadata AS jsonb), :owned_by"
        ") RETURNING " + _BASE_SELECT_COLS
    )

    row = await DQLQuery(sql, result_handler=ResultHandler.ONE_OR_NONE).execute(
        conn,
        asset_id=payload.asset_id,
        catalog_id=scope.catalog_id,
        collection_id=scope.collection_id,
        asset_type=asset_type_val,
        kind=kind_val,
        status=initial_status.value,
        filename=filename,
        href=href,
        uri=uri,
        content_hash=None,
        size_bytes=None,
        created_at=now,
        updated_at=now,
        metadata=json.dumps(payload.metadata or {}),
        owned_by=getattr(payload, "owned_by", None),
    )
    return _row_to_dict(row) or {}


async def _update_row(
    conn: DbResource,
    scope: Scope,
    payload: AssetCreatePayload,
    existing: Dict[str, Any],
) -> Dict[str, Any]:
    """UPDATE the matched row's mutable fields (metadata only, per AssetUpdate)."""
    sql = (
        f'UPDATE "{scope.schema}".assets '
        "SET metadata = CAST(:metadata AS jsonb), updated_at = :updated_at "
        "WHERE catalog_id = :catalog_id "
        "AND collection_id IS NOT DISTINCT FROM :collection_id "
        "AND asset_id = :asset_id "
        "RETURNING " + _BASE_SELECT_COLS
    )
    row = await DQLQuery(sql, result_handler=ResultHandler.ONE_OR_NONE).execute(
        conn,
        metadata=json.dumps(payload.metadata or {}),
        updated_at=datetime.now(timezone.utc),
        catalog_id=scope.catalog_id,
        collection_id=scope.collection_id,
        asset_id=existing["asset_id"],
    )
    return _row_to_dict(row) or existing


async def _archive_row(
    conn: DbResource,
    scope: Scope,
    existing: Dict[str, Any],
) -> None:
    """Soft-delete the existing row so a fresh INSERT with the same asset_id
    is allowed by the partial unique index ``WHERE status <> 'deleted'``.

    The DDL in ``pg_asset_driver.py`` declares::

        CREATE UNIQUE INDEX assets_uq_filename_*
            ON assets (catalog_id, collection_id, filename)
            WHERE kind = 'physical' AND status <> 'deleted';

    so flipping the row to ``status='deleted'`` is sufficient for the new
    INSERT to land — no need to rewrite ``asset_id`` or stash an extra
    "archived" suffix.
    """
    sql = (
        f'UPDATE "{scope.schema}".assets '
        "SET status = 'deleted', updated_at = :updated_at "
        "WHERE catalog_id = :catalog_id "
        "AND collection_id IS NOT DISTINCT FROM :collection_id "
        "AND asset_id = :asset_id "
        "AND status <> 'deleted'"
    )
    await DQLQuery(sql, result_handler=ResultHandler.NONE).execute(
        conn,
        updated_at=datetime.now(timezone.utc),
        catalog_id=scope.catalog_id,
        collection_id=scope.collection_id,
        asset_id=existing["asset_id"],
    )


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _row_to_dict(row: Any) -> Optional[Dict[str, Any]]:
    """Coerce a SQLAlchemy / asyncpg row-like into a plain dict (or None)."""
    if row is None:
        return None
    if isinstance(row, dict):
        return dict(row)
    mapping = getattr(row, "_mapping", None)
    if mapping is not None:
        return dict(mapping)
    asdict = getattr(row, "_asdict", None)
    if callable(asdict):
        result: Any = asdict()
        if result is None:
            return None
        return dict(result)
    # asyncpg.Record exposes dict() directly.
    try:
        return dict(row)
    except Exception:
        return None


def _walk_dot_path(data: Dict[str, Any], path: str) -> Optional[Any]:
    """Resolve a dot-notation path into a nested dict; None on miss."""
    cur: Any = data
    for part in path.split("."):
        if not isinstance(cur, dict):
            return None
        cur = cur.get(part)
        if cur is None:
            return None
    return cur


__all__ = [
    "AssetSidecarRejectedError",
    "Scope",
    "UpsertResult",
    "upsert_asset",
    "PROBES",
]


# ---------------------------------------------------------------------------
# Avoid an unused-import warning for AssetKind — it is part of the public API
# of the underlying DTOs but only referenced in docstrings here.
# ---------------------------------------------------------------------------

_ = AssetKind
