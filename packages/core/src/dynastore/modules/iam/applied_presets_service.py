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

"""Service wrapping ``iam.applied_presets`` queries.

Provides typed methods for every state-machine transition. All writes
use ``managed_transaction``; callers may pass a ``conn`` to participate
in an outer transaction (used by the lifecycle layer for row-lock
semantics: SELECT … FOR UPDATE then transition in the same transaction).
"""
from __future__ import annotations

import base64
import json
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple
from uuid import UUID

from dynastore.modules.db_config.query_executor import DbResource

from . import applied_presets_queries as _q

# Type alias for an audit row returned as a plain dict.
AppliedRow = Dict[str, Any]

_VALID_STATES = frozenset({
    "pending", "in_progress", "applied",
    "revoke_pending", "revoke_in_progress", "revoked",
    "failed", "revoke_failed", "partial",
})


def _encode_cursor(applied_at_iso: Optional[str], preset_name: str) -> str:
    """Encode an opaque keyset cursor from ``(applied_at_iso, preset_name)``."""
    payload = json.dumps([applied_at_iso, preset_name])
    return base64.urlsafe_b64encode(payload.encode()).decode()


def _decode_cursor(cursor: str) -> Tuple[Optional[str], str]:
    """Decode a cursor produced by :func:`_encode_cursor`.

    Returns ``(applied_at_iso, preset_name)``.  Raises ``ValueError`` on
    malformed input so callers can surface a 400.
    """
    try:
        payload = base64.urlsafe_b64decode(cursor.encode()).decode()
        parts = json.loads(payload)
        if not isinstance(parts, list) or len(parts) != 2:
            raise ValueError("unexpected shape")
        applied_at_iso, preset_name = parts
        if not isinstance(preset_name, str):
            raise ValueError("preset_name must be a string")
        if applied_at_iso is not None:
            if not isinstance(applied_at_iso, str):
                raise ValueError("applied_at_iso must be a string or null")
            # Validate ISO timestamp shape so a crafted cursor cannot reach PG
            # as a malformed cast and surface as a 500.
            try:
                datetime.fromisoformat(applied_at_iso)
            except ValueError as ts_exc:
                raise ValueError(
                    f"applied_at_iso is not a valid ISO 8601 timestamp: {ts_exc}"
                ) from ts_exc
        return applied_at_iso, preset_name
    except Exception as exc:
        raise ValueError(f"invalid cursor: {exc}") from exc


class AppliedPresetsService:
    """CRUD + state transitions for the ``iam.applied_presets`` table."""

    def __init__(self, engine: Optional[DbResource]) -> None:
        self._engine = engine

    def _resource(self, conn: Optional[Any]) -> DbResource:
        """Return ``conn`` if provided, else the service engine; assert non-None."""
        resource = conn if conn is not None else self._engine
        assert resource is not None, "AppliedPresetsService: no DB resource available"
        return resource  # type: ignore[return-value]

    # ------------------------------------------------------------------
    # DDL bootstrap — called from the IAM module lifespan
    # ------------------------------------------------------------------

    async def ensure_table(self, conn: Optional[Any] = None) -> None:
        """Create the ``iam.applied_presets`` table and indexes if absent."""
        resource = self._resource(conn)
        await _q.CREATE_APPLIED_PRESETS_TABLE.execute(resource)
        await _q.CREATE_APPLIED_PRESETS_STATE_IDX.execute(resource)
        await _q.CREATE_APPLIED_PRESETS_SCOPE_IDX.execute(resource)

    # ------------------------------------------------------------------
    # Read
    # ------------------------------------------------------------------

    async def get(
        self,
        name: str,
        scope_key: str,
        conn: Optional[Any] = None,
    ) -> Optional[AppliedRow]:
        """Return the audit row for ``(name, scope_key)`` or ``None``."""
        resource = self._resource(conn)
        row = await _q.SELECT_ROW.execute(
            resource, preset_name=name, scope_key=scope_key
        )
        return dict(row._mapping) if row is not None else None

    async def get_for_update(
        self,
        name: str,
        scope_key: str,
        conn: Any,
    ) -> Optional[AppliedRow]:
        """Select the row with a ``FOR UPDATE`` lock.

        ``conn`` must be an active connection inside a transaction. The
        lifecycle layer holds this lock for the duration of the apply /
        revoke call.
        """
        row = await _q.SELECT_FOR_UPDATE.execute(
            conn, preset_name=name, scope_key=scope_key
        )
        return dict(row._mapping) if row is not None else None

    async def list_for_scope(
        self,
        *,
        scope_key: str,
        state: str = "applied",
        cursor: Optional[str] = None,
        limit: int = 50,
        conn: Optional[Any] = None,
    ) -> Tuple[List[AppliedRow], Optional[str]]:
        """Return rows for an exact ``scope_key``, keyset-paginated on ``(applied_at DESC, preset_name)``.

        ``limit`` is clamped to ``[1, 200]``.  ``state`` must be a valid
        state-machine value; callers should validate before calling.
        Returns ``(rows, next_cursor)`` where ``next_cursor`` is ``None``
        when no further pages exist.
        """
        limit = max(1, min(limit, 200))
        resource = self._resource(conn)
        if cursor is not None:
            applied_at_iso, cursor_name = _decode_cursor(cursor)
            rows = await _q.LIST_FOR_SCOPE_CURSOR.execute(
                resource,
                scope_key=scope_key,
                state=state,
                cursor_applied_at=applied_at_iso,
                cursor_preset_name=cursor_name,
                limit=limit,
            )
        else:
            rows = await _q.LIST_FOR_SCOPE.execute(
                resource,
                scope_key=scope_key,
                state=state,
                limit=limit,
            )
        rows = rows or []
        next_cursor: Optional[str] = None
        if len(rows) == limit:
            last = rows[-1]
            at_val = last.get("applied_at")
            at_iso = at_val.isoformat() if hasattr(at_val, "isoformat") else (str(at_val) if at_val is not None else None)
            next_cursor = _encode_cursor(at_iso, last["preset_name"])
        return rows, next_cursor

    async def list(
        self,
        *,
        name: Optional[str] = None,
        scope_key_prefix: Optional[str] = None,
        state: Optional[str] = None,
        limit: int = 50,
        cursor: Optional[str] = None,
        conn: Optional[Any] = None,
    ) -> List[AppliedRow]:
        """Paginated list of audit rows.

        ``scope_key_prefix`` is matched with ``LIKE`` — pass ``"catalog:%"``
        to list all catalog-scoped rows. ``cursor`` is a preset name for
        keyset pagination.
        """
        resource = self._resource(conn)
        sql_prefix = (scope_key_prefix or "%") + (
            "" if scope_key_prefix is None or scope_key_prefix.endswith("%") else "%"
        )
        kwargs = dict(
            scope_prefix=sql_prefix,
            preset_name=name,
            state=state,
            limit=limit,
        )
        if cursor is not None:
            rows = await _q.LIST_BY_SCOPE_PREFIX_CURSOR.execute(
                resource, cursor=cursor, **kwargs
            )
        else:
            rows = await _q.LIST_BY_SCOPE_PREFIX.execute(resource, **kwargs)
        return [dict(r) for r in (rows or [])]

    # ------------------------------------------------------------------
    # State transitions
    # ------------------------------------------------------------------

    async def insert_pending(
        self,
        name: str,
        scope_key: str,
        params_snapshot: Dict[str, Any],
        applied_by: Optional[UUID],
        conn: Optional[Any] = None,
    ) -> AppliedRow:
        """Insert or reset a row to ``pending`` state."""
        resource = self._resource(conn)
        row = await _q.UPSERT_PENDING.execute(
            resource,
            preset_name=name,
            scope_key=scope_key,
            applied_by=str(applied_by) if applied_by else None,
            params_snapshot=json.dumps(params_snapshot),
        )
        return dict(row._mapping)

    async def mark_in_progress(
        self,
        name: str,
        scope_key: str,
        task_id: Optional[UUID] = None,
        conn: Optional[Any] = None,
    ) -> Optional[AppliedRow]:
        resource = self._resource(conn)
        row = await _q.MARK_IN_PROGRESS.execute(
            resource,
            preset_name=name,
            scope_key=scope_key,
            task_id=str(task_id) if task_id else None,
        )
        return dict(row._mapping) if row is not None else None

    async def mark_applied(
        self,
        name: str,
        scope_key: str,
        revoke_descriptor: Dict[str, Any],
        conn: Optional[Any] = None,
    ) -> Optional[AppliedRow]:
        resource = self._resource(conn)
        row = await _q.MARK_APPLIED.execute(
            resource,
            preset_name=name,
            scope_key=scope_key,
            revoke_descriptor=json.dumps(revoke_descriptor),
        )
        return dict(row._mapping) if row is not None else None

    async def mark_failed(
        self,
        name: str,
        scope_key: str,
        last_error: str,
        conn: Optional[Any] = None,
    ) -> Optional[AppliedRow]:
        resource = self._resource(conn)
        row = await _q.MARK_FAILED.execute(
            resource,
            preset_name=name,
            scope_key=scope_key,
            last_error=last_error,
        )
        return dict(row._mapping) if row is not None else None

    async def mark_revoke_pending(
        self,
        name: str,
        scope_key: str,
        task_id: Optional[UUID] = None,
        conn: Optional[Any] = None,
    ) -> Optional[AppliedRow]:
        resource = self._resource(conn)
        row = await _q.MARK_REVOKE_PENDING.execute(
            resource,
            preset_name=name,
            scope_key=scope_key,
            task_id=str(task_id) if task_id else None,
        )
        return dict(row._mapping) if row is not None else None

    async def mark_revoked(
        self,
        name: str,
        scope_key: str,
        conn: Optional[Any] = None,
    ) -> Optional[AppliedRow]:
        resource = self._resource(conn)
        row = await _q.MARK_REVOKED.execute(
            resource, preset_name=name, scope_key=scope_key
        )
        return dict(row._mapping) if row is not None else None

    async def mark_revoke_failed(
        self,
        name: str,
        scope_key: str,
        last_error: str,
        conn: Optional[Any] = None,
    ) -> Optional[AppliedRow]:
        resource = self._resource(conn)
        row = await _q.MARK_REVOKE_FAILED.execute(
            resource,
            preset_name=name,
            scope_key=scope_key,
            last_error=last_error,
        )
        return dict(row._mapping) if row is not None else None

    async def mark_partial(
        self,
        name: str,
        scope_key: str,
        child_name: str,
        child_error: str,
        conn: Optional[Any] = None,
    ) -> Optional[AppliedRow]:
        resource = self._resource(conn)
        row = await _q.MARK_PARTIAL.execute(
            resource,
            preset_name=name,
            scope_key=scope_key,
            last_error=f"child {child_name!r} failed: {child_error}",
        )
        return dict(row._mapping) if row is not None else None
