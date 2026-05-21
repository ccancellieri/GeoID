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

"""Shared, PostgreSQL-backed registry of in-flight local-upload tickets.

Why a shared store
------------------
The platform runs as a distributed, stateless runtime — multiple Cloud Run
services and multiple pods, all stateless, sharing only PostgreSQL,
Elasticsearch and Valkey. A process-local ``ClassVar`` dict therefore loses
tickets across replicas: an ``initiate_upload`` handled by one process stamps a
ticket the other processes cannot see, so the matching
``POST /local-upload/{ticket_id}`` receive (or ``/upload/{id}/status`` poll)
that lands on a different process 404s.

The born-claimed PENDING ``assets`` row already lives in PG at initiate time,
but it sits in a *per-catalog* schema and the receive route at
``/local-upload/{ticket_id}`` only carries the bare ``ticket_id`` — it cannot
know which catalog schema to look in. This module co-locates the ticket
lifecycle with the catalog in a single global ``{schema}.local_upload_tickets``
table (the same global ``tasks`` schema that backs the cross-replica task
queue), keyed by ``ticket_id`` so any process resolves any ticket regardless of
which one initiated it.

TTL semantics match the previous in-memory store: tickets carry an
``expires_at`` and are treated as absent once past it. A lightweight
self-prune deletes expired rows opportunistically so the table does not grow
without bound (the global cron cleanup would otherwise be the only reaper).
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import Any, Dict, Optional

from dynastore.modules.db_config.maintenance_tools import ensure_schema_exists
from dynastore.modules.db_config.query_executor import (
    DDLBatch,
    DDLQuery,
    DQLQuery,
    ResultHandler,
    managed_transaction,
)
from dynastore.modules.tasks.tasks_module import get_task_schema

logger = logging.getLogger(__name__)


# The ticket payload (``data``) is a JSON object holding exactly the fields the
# receive / status paths need: asset_id, catalog_id, collection_id, filename,
# content_type, asset_def (the dumped AssetUploadDefinition), status,
# asset_id_result, error. ``expires_at`` is promoted to a column so expiry is a
# cheap indexed predicate and the self-prune is a single DELETE.
_TABLE_DDL = """
CREATE TABLE IF NOT EXISTS {schema}.local_upload_tickets (
    ticket_id   UUID         NOT NULL PRIMARY KEY,
    data        JSONB        NOT NULL,
    expires_at  TIMESTAMPTZ  NOT NULL,
    created_at  TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    updated_at  TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);
"""

_INDEX_DDL = """
CREATE INDEX IF NOT EXISTS idx_local_upload_tickets_expires_at
    ON {schema}.local_upload_tickets (expires_at);
"""


async def ensure_local_upload_tickets_table(engine: Any) -> None:
    """Create the global ``local_upload_tickets`` table + index if absent.

    Idempotent (``IF NOT EXISTS``). Called from ``LocalUploadModule.lifespan``
    once the database engine is up; never inlined into ``__init__``.
    """
    if engine is None:
        return
    schema = get_task_schema()
    # The index is the sentinel: when it exists the whole batch is skipped in
    # one round-trip on warm starts.
    batch = DDLBatch(
        sentinel=DDLQuery(_INDEX_DDL),
        steps=[DDLQuery(_TABLE_DDL), DDLQuery(_INDEX_DDL)],
    )
    async with managed_transaction(engine) as conn:
        await ensure_schema_exists(conn, schema)
        await batch.execute(conn, schema=schema)


class LocalUploadTicketStore:
    """Cross-replica ticket registry backing :class:`LocalUploadModule`.

    Replaces the former process-local ``ClassVar`` dict. The public surface
    mirrors a mapping so the module's create / look-up / expire flow is a
    near drop-in:

    * :meth:`put` — insert or overwrite a ticket.
    * :meth:`get` — fetch a non-expired ticket (returns ``None`` when missing
      or expired; expired rows are pruned on read).
    * :meth:`update` — merge fields into an existing ticket's payload.
    * :meth:`delete` — remove a ticket.
    """

    def __init__(self, engine: Any) -> None:
        self._engine = engine
        self._schema = get_task_schema()

    # -- helpers -------------------------------------------------------------

    @staticmethod
    def _now() -> datetime:
        return datetime.now(timezone.utc)

    @staticmethod
    def _coerce_expires_at(value: Any) -> datetime:
        if isinstance(value, datetime):
            return value
        return datetime.fromisoformat(str(value))

    # -- CRUD ----------------------------------------------------------------

    async def put(self, ticket_id: str, ticket: Dict[str, Any]) -> None:
        """Insert (or overwrite) ``ticket`` under ``ticket_id``.

        ``ticket["expires_at"]`` (a ``datetime``) is lifted into the indexed
        column; the whole dict is JSON-serialised into ``data``.
        """
        expires_at = self._coerce_expires_at(ticket["expires_at"])
        sql = (
            f'INSERT INTO "{self._schema}".local_upload_tickets '
            "(ticket_id, data, expires_at, created_at, updated_at) "
            "VALUES (CAST(:ticket_id AS uuid), CAST(:data AS jsonb), :expires_at, NOW(), NOW()) "
            "ON CONFLICT (ticket_id) DO UPDATE "
            "SET data = EXCLUDED.data, "
            "    expires_at = EXCLUDED.expires_at, "
            "    updated_at = NOW()"
        )
        async with managed_transaction(self._engine) as conn:
            await DQLQuery(sql, result_handler=ResultHandler.NONE).execute(
                conn,
                ticket_id=ticket_id,
                data=json.dumps(self._encode(ticket)),
                expires_at=expires_at,
            )

    async def get(self, ticket_id: str) -> Optional[Dict[str, Any]]:
        """Return the live ticket payload, or ``None`` when absent/expired.

        An expired row is deleted before returning ``None`` so a subsequent
        read does not keep paying for it (matches the old in-memory ``pop``).
        """
        select_sql = (
            "SELECT data, expires_at "
            f'FROM "{self._schema}".local_upload_tickets '
            "WHERE ticket_id = CAST(:ticket_id AS uuid)"
        )
        async with managed_transaction(self._engine) as conn:
            row = await DQLQuery(
                select_sql, result_handler=ResultHandler.ONE_DICT
            ).execute(conn, ticket_id=ticket_id)
        if not row:
            return None
        expires_at = self._coerce_expires_at(row["expires_at"])
        if self._now() > expires_at:
            await self.delete(ticket_id)
            return None
        return self._decode(row["data"])

    async def update(self, ticket_id: str, fields: Dict[str, Any]) -> None:
        """Merge ``fields`` into the stored payload (e.g. status transitions).

        A JSONB ``||`` shallow merge keeps the row addressable by the same
        ``ticket_id`` without a read-modify-write race window.
        """
        sql = (
            f'UPDATE "{self._schema}".local_upload_tickets '
            "SET data = data || CAST(:patch AS jsonb), updated_at = NOW() "
            "WHERE ticket_id = CAST(:ticket_id AS uuid)"
        )
        async with managed_transaction(self._engine) as conn:
            await DQLQuery(sql, result_handler=ResultHandler.NONE).execute(
                conn,
                ticket_id=ticket_id,
                patch=json.dumps(self._encode(fields)),
            )

    async def delete(self, ticket_id: str) -> None:
        """Remove the ticket (no-op when already gone)."""
        sql = (
            f'DELETE FROM "{self._schema}".local_upload_tickets '
            "WHERE ticket_id = CAST(:ticket_id AS uuid)"
        )
        async with managed_transaction(self._engine) as conn:
            await DQLQuery(sql, result_handler=ResultHandler.NONE).execute(
                conn, ticket_id=ticket_id
            )

    async def prune_expired(self) -> None:
        """Delete every ticket past its TTL. Cheap (indexed) and idempotent."""
        sql = (
            f'DELETE FROM "{self._schema}".local_upload_tickets '
            "WHERE expires_at < NOW()"
        )
        async with managed_transaction(self._engine) as conn:
            await DQLQuery(sql, result_handler=ResultHandler.NONE).execute(conn)

    # -- (de)serialisation ---------------------------------------------------

    @staticmethod
    def _encode(payload: Dict[str, Any]) -> Dict[str, Any]:
        """Make a ticket payload JSON-safe (datetimes → ISO, enums → value)."""
        out: Dict[str, Any] = {}
        for key, value in payload.items():
            if isinstance(value, (str, int, float, bool)) or value is None:
                out[key] = value
            elif isinstance(value, datetime):
                out[key] = value.isoformat()
            elif hasattr(value, "model_dump"):
                # Pydantic model (e.g. AssetUploadDefinition).
                out[key] = {"__model__": value.model_dump(mode="json")}
            elif hasattr(value, "value"):
                # Enum (e.g. UploadStatus) → its string value.
                out[key] = value.value
            else:
                out[key] = value
        return out

    @staticmethod
    def _decode(data: Any) -> Dict[str, Any]:
        """Reverse :meth:`_encode` enough for the module's read paths.

        ``status`` is rehydrated to :class:`UploadStatus`; ``expires_at`` to a
        timezone-aware ``datetime``; ``asset_def`` to an
        :class:`AssetUploadDefinition`.
        """
        from dynastore.models.protocols import UploadStatus
        from dynastore.modules.catalog.asset_service import AssetUploadDefinition

        if isinstance(data, str):
            data = json.loads(data)
        payload: Dict[str, Any] = dict(data)

        if "status" in payload and payload["status"] is not None:
            payload["status"] = UploadStatus(payload["status"])
        if "expires_at" in payload and isinstance(payload["expires_at"], str):
            payload["expires_at"] = datetime.fromisoformat(payload["expires_at"])
        asset_def = payload.get("asset_def")
        if isinstance(asset_def, dict) and "__model__" in asset_def:
            payload["asset_def"] = AssetUploadDefinition.model_validate(
                asset_def["__model__"]
            )
        return payload


__all__ = ["LocalUploadTicketStore", "ensure_local_upload_tickets_table"]
