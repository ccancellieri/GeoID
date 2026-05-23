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

1. Walks the ``policy.identity`` rules in order. Each rule's ``match_on``
   list is AND-composed over identity fields; the first rule whose
   conjunction matches an existing row wins (OR across rules).
2. Dispatches the rule's ``on_match`` action (or the policy-level
   :class:`AssetWriteConflictPolicy` when the rule doesn't override):
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
    AssetIdentityField,
    AssetIdentityKind,
    AssetsWritePolicy,
    AssetWriteConflictPolicy,
    ResolvedAssetIdentityRule,
)
from dynastore.modules.db_config.exceptions import UniqueViolationError
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
    the identity kind that triggered the conflict path, when any. The kind
    is taken from the first :class:`AssetIdentityField` of the winning
    :class:`AssetIdentityRule` — single-field rules (the only shape Stage 1
    emits) put the relevant kind there directly.
    """

    action: UpsertAction
    row: Dict[str, Any]
    matcher_hit: Optional[AssetIdentityKind] = None
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
    *,
    path: Optional[str] = None,
) -> Optional[Dict[str, Any]]:
    """Match by a JSON-path lookup on ``metadata``.

    The path comes from the :attr:`AssetIdentityField.path` of the field
    being evaluated and is a dot-notation expression (``"a.b.c"``).  We
    resolve the value from the incoming payload's metadata dict and compare
    with the persisted column via PostgreSQL's ``#>>`` operator.
    """
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
    the hash is set by the finalize event, so until then the incoming
    payload has nothing to compare. The probe is still wired so the
    finalize-event path benefits from it directly.

    The ``content_hash`` column stores values as ``"<algo>:<raw>"`` (e.g.
    ``"md5:abc=="``). Both sides of the comparison are tagged: the
    incoming payload carries the tagged form, the row stores the tagged
    form. A ``"sha256:X"`` payload never matches an ``"md5:Y"`` row even
    if raw bytes coincide — the algo prefix is part of the equality.
    """
    incoming_hash = getattr(payload, "content_hash", None)
    if not incoming_hash:
        return None
    sql = (
        f'SELECT {_BASE_SELECT_COLS} FROM "{scope.schema}".assets '
        "WHERE catalog_id = :catalog_id "
        "AND collection_id IS NOT DISTINCT FROM :collection_id "
        "AND kind = 'physical' "
        "AND status <> 'deleted' "
        "AND content_hash = :tagged "
        "LIMIT 1"
    )
    row = await DQLQuery(sql, result_handler=ResultHandler.ONE_OR_NONE).execute(
        conn,
        catalog_id=scope.catalog_id,
        collection_id=scope.collection_id,
        tagged=incoming_hash,
    )
    return _row_to_dict(row)


# Probe lookup map: identity kind → coroutine.
#
# Probes share a uniform (conn, scope, payload, policy) signature plus
# kwargs the rule-walker may pass (e.g. ``path`` for METADATA_FIELD). The
# **kwargs catch on the protocol keeps every probe trivially swappable from
# the dispatcher.
ProbeFn = Callable[..., Awaitable[Optional[Dict[str, Any]]]]
PROBES: Dict[AssetIdentityKind, ProbeFn] = {
    AssetIdentityKind.ASSET_ID: _probe_by_asset_id,
    AssetIdentityKind.FILENAME: _probe_by_filename,
    AssetIdentityKind.URL: _probe_by_url,
    AssetIdentityKind.METADATA_FIELD: _probe_by_metadata_field,
    AssetIdentityKind.CONTENT_HASH: _probe_by_content_hash,
}


# ---------------------------------------------------------------------------
# Constraint → matcher mapping for concurrent-INSERT race handling
# ---------------------------------------------------------------------------


def _constraint_name_from_exc(exc: BaseException) -> Optional[str]:
    """Extract the violated constraint name from a :class:`UniqueViolationError`.

    The dynastore ``UniqueViolationError`` wraps either an ``asyncpg`` exception
    (``constraint_name`` attr) or a SQLAlchemy ``IntegrityError`` (``orig`` attr
    pointing at the asyncpg exception). Returns ``None`` when the constraint
    name can't be recovered — caller falls back to a generic conflict matcher.
    """
    original = getattr(exc, "original_exception", None)
    if original is None:
        return None
    name = getattr(original, "constraint_name", None)
    if name:
        return name
    # SQLAlchemy IntegrityError nesting
    orig = getattr(original, "orig", None)
    if orig is not None:
        name = getattr(orig, "constraint_name", None)
        if name:
            return name
    # Fall back to message scraping — pgcode 23505 messages typically contain
    # `unique constraint "<name>"`.
    msg = str(original)
    import re
    m = re.search(r'unique constraint "([^"]+)"', msg)
    if m:
        return m.group(1)
    return None


def _matcher_for_constraint(constraint_name: Optional[str]) -> str:
    """Map a violated constraint name to the identity-kind value reported in
    :class:`AssetSidecarRejectedError`.

    Constraint names are namespaced by schema (e.g.
    ``assets_uq_filename_<schema_tag>``); this matches by prefix.
    """
    if not constraint_name:
        return "unknown"
    if constraint_name.startswith("assets_uq_filename"):
        return AssetIdentityKind.FILENAME.value
    if constraint_name.startswith("assets_uq_href"):
        return AssetIdentityKind.URL.value
    if constraint_name == "assets_identity_uq" or constraint_name.startswith(
        "assets_identity_uq"
    ):
        return AssetIdentityKind.ASSET_ID.value
    return "unknown"


# ---------------------------------------------------------------------------
# Chain runner & action dispatcher
# ---------------------------------------------------------------------------


async def _probe_field(
    conn: DbResource,
    scope: Scope,
    payload: AssetCreatePayload,
    policy: AssetsWritePolicy,
    field_spec: AssetIdentityField,
) -> Optional[Dict[str, Any]]:
    """Dispatch a single :class:`AssetIdentityField` to its probe.

    Kept as a thin shim so the rule walker stays focused on AND/OR
    composition rather than on per-kind kwargs.
    """
    probe = PROBES.get(field_spec.kind)
    if probe is None:
        # Unknown kind: silently skip so a policy can opt into a kind that
        # requires a backend not yet wired (mirrors items' forgiving stance
        # for forward compat).
        return None
    if field_spec.kind == AssetIdentityKind.METADATA_FIELD:
        return await probe(conn, scope, payload, policy, path=field_spec.path)
    return await probe(conn, scope, payload, policy)


async def _resolve(
    conn: DbResource,
    scope: Scope,
    payload: AssetCreatePayload,
    policy: AssetsWritePolicy,
) -> Tuple[
    Optional[Dict[str, Any]],
    Optional[AssetIdentityKind],
    Optional[ResolvedAssetIdentityRule],
]:
    """Walk the resolved identity rules in order; first matching rule wins.

    ``policy.identity`` rules reference derivations by name; we resolve them
    via :meth:`AssetsWritePolicy.resolved_identity` to the engine
    :class:`AssetIdentityField` objects (kind + optional metadata path) that
    the probe dispatcher consumes — mirroring how ``item_distributed`` consumes
    ``ItemsWritePolicy.resolved_identity()``.

    For each rule the ``match_on`` fields are evaluated as an AND: every
    field must independently match the SAME existing row for the rule to
    fire. Single-field rules (the only shape Stage 1 emits) trivially
    satisfy AND with the field's own probe result.

    Returns ``(row, winning_kind, winning_rule)``; the rule is forwarded so
    its ``on_match`` override can substitute the policy-level action.
    """
    for rule in policy.resolved_identity():
        winning_row: Optional[Dict[str, Any]] = None
        all_matched = True
        for field_spec in rule.match_on:
            row = await _probe_field(conn, scope, payload, policy, field_spec)
            if row is None:
                all_matched = False
                break
            if winning_row is None:
                winning_row = row
            elif winning_row.get("asset_id") != row.get("asset_id"):
                # AND-composition: every field must point at the same row.
                all_matched = False
                break
        if all_matched and winning_row is not None:
            # By convention the first field of the rule names the dispatch
            # kind reported back to callers (single-field rules trivially,
            # multi-field rules pick the lead).
            lead_kind = rule.match_on[0].kind
            return winning_row, lead_kind, rule
    return None, None, None


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
    # 1. Resolve identity via the rule chain (skipped entirely for
    #    NEW_VERSION since we always want a fresh row in that mode).
    existing: Optional[Dict[str, Any]] = None
    matcher_hit: Optional[AssetIdentityKind] = None
    winning_rule: Optional[ResolvedAssetIdentityRule] = None
    if policy.on_conflict != AssetWriteConflictPolicy.NEW_VERSION:
        existing, matcher_hit, winning_rule = await _resolve(
            conn, scope, payload, policy
        )

    # 2. Resolve the effective action for a matched row: the rule-level
    #    override when the winning rule provided one, otherwise the policy
    #    default. Idempotent "unchanged content" handling is no longer a
    #    special boolean — it is expressed declaratively as an identity rule
    #    matching on ``content_hash`` (matching on content_hash *is* the
    #    "incoming hash equals an existing row's hash" condition) with
    #    ``on_match=REFUSE_RETURN``, which lands in this same resolution and
    #    dispatches to the return-existing branch below.
    on_conflict = (
        winning_rule.on_match
        if (winning_rule is not None and winning_rule.on_match is not None)
        else policy.on_conflict
    )

    # 3. NEW_VERSION special case: no matcher_hit yet (we skipped the chain
    #    for NEW_VERSION). Probe by asset_id specifically — versioning is
    #    only meaningful when there is a prior row to archive.
    if policy.on_conflict == AssetWriteConflictPolicy.NEW_VERSION:
        existing = await _probe_by_asset_id(conn, scope, payload, policy)
        if existing:
            matcher_hit = AssetIdentityKind.ASSET_ID

    # 4. No match → fresh INSERT. The chain probe ran without a transaction
    #    barrier, so a concurrent caller can win the race and INSERT the same
    #    identity between our probe and our INSERT. The partial unique indexes
    #    catch that at the DB level; we re-raise as a typed rejection so the
    #    REST surface returns 409 (matching the application-level policy
    #    rejection) instead of bubbling a 500.
    if existing is None:
        # Assets is partitioned BY LIST (collection_id); ensure the per-collection
        # partition exists before INSERT. NULL collection_id rows land in the
        # default partition created by ensure_storage; only non-NULL values need
        # a per-value partition. Mirrors pg_asset_driver.index_asset.
        if scope.collection_id is not None:
            from dynastore.modules.db_config.partition_tools import (
                ensure_partition_exists,
            )
            await ensure_partition_exists(
                conn,
                table_name="assets",
                strategy="LIST",
                partition_value=scope.collection_id,
                schema=scope.schema,
                parent_table_name="assets",
                parent_table_schema=scope.schema,
            )
        try:
            inserted = await _insert_new_row(conn, scope, payload, initial_status)
        except UniqueViolationError as exc:
            constraint = _constraint_name_from_exc(exc)
            matcher_value = _matcher_for_constraint(constraint)
            logger.info(
                "AssetsWritePolicy concurrent-INSERT race lost: "
                "constraint=%s matcher=%s asset_id=%s",
                constraint,
                matcher_value,
                payload.asset_id,
            )
            raise AssetSidecarRejectedError(
                f"Asset write refused: concurrent INSERT lost the race "
                f"(unique constraint {constraint or 'unknown'}); policy=race",
                asset_id=payload.asset_id,
                matcher=matcher_value,
                reason="conflict",
                existing_id=None,
            ) from exc
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
        try:
            inserted = await _insert_new_row(conn, scope, payload, initial_status)
        except UniqueViolationError as exc:
            # NEW_VERSION raced with another writer that won the same archive +
            # re-INSERT cycle. Surface as a conflict rejection rather than 500.
            constraint = _constraint_name_from_exc(exc)
            matcher_value = _matcher_for_constraint(constraint)
            logger.info(
                "NEW_VERSION concurrent-INSERT race lost: "
                "constraint=%s matcher=%s asset_id=%s",
                constraint,
                matcher_value,
                payload.asset_id,
            )
            raise AssetSidecarRejectedError(
                f"NEW_VERSION write refused: concurrent INSERT lost the race "
                f"(unique constraint {constraint or 'unknown'})",
                asset_id=payload.asset_id,
                matcher=matcher_value,
                reason="conflict",
                existing_id=existing_id,
            ) from exc
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

    Also stamps any active ``asset_references`` for the archived asset_id
    so a future hard-delete of the successor row (re-using the same id)
    isn't blocked by a stale reference pointing at the version we just
    archived.
    """
    now = datetime.now(timezone.utc)
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
        updated_at=now,
        catalog_id=scope.catalog_id,
        collection_id=scope.collection_id,
        asset_id=existing["asset_id"],
    )

    refs_sql = (
        f'UPDATE "{scope.schema}".asset_references '
        "SET valid_until = :now "
        "WHERE catalog_id = :catalog_id "
        "AND asset_id = :asset_id "
        "AND valid_until IS NULL"
    )
    await DQLQuery(refs_sql, result_handler=ResultHandler.NONE).execute(
        conn,
        now=now,
        catalog_id=scope.catalog_id,
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
