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

"""Access Envelope Sidecar — opt-in PG sub-table for ABAC row-level access control.

This sidecar is the PG counterpart of the Elasticsearch access-envelope pattern
introduced in PR #1441.  It stores a compact JSONB envelope per item in a
dedicated sub-table ``{schema}.{table}_access_envelope`` (one row per hub row,
FK on ``geoid``) and applies that envelope as an additional ``WHERE`` clause on
every read dispatched through a ``QueryRequest`` that carries an
``access_filter``.

Sub-table pattern
-----------------
The envelope lives in its own table — NOT as an extra column on the hub.  This
mirrors the existing ``geometries``, ``item_metadata`` and ``attributes`` sidecars
and avoids the three hub-column risks identified in the blueprint:

* R1: ``ALTER TABLE`` on the hub is a DDL migration (blocked by project invariant).
* R2: A JSONB column on the hub widens the SELECT for every query (performance).
* R3: Accidental projection of the envelope into Feature output (security).

Write path
----------
``prepare_upsert_payload`` reads ``context["_access_envelope"]`` — a dict
pre-resolved upstream by the ``ItemService`` ABAC wiring.  When absent the
sidecar returns ``None`` (skip write, no-op for this item).

Read path
---------
``apply_query_context`` calls :func:`access_filter_to_pg_clause` with the JOIN
alias ``ae`` and appends the resulting SQL fragment to ``where_conditions``.
When ``request.access_filter is None`` the sidecar appends ``"FALSE"`` — strict
fail-closed default.

KNOWN GAPS (see tracking issue: #1457)
---------------------------------------------
G1: ``_access_envelope`` is NOT yet populated in ``item_context`` by
    ``ItemService.upsert_bulk`` for PG-sidecar collections.  Until that wiring
    lands, sidecar writes are no-ops.

G2: ``QueryRequest.access_filter`` is NOT yet populated by the PG read dispatch
    path.  Until that lands, sidecar reads fail-closed (return zero rows).

G3: ``access_filter_to_pg_clause`` does not handle the ``AccessFilter.union``
    field (multi-collection differential ABAC).  Single-collection reads are
    correct; multi-collection PG reads via the sidecar under-return (safe).

G4: ``_collection_uses_access_aware_driver`` (at ``item_service.py``) does not
    detect PG sidecar presence — must be extended to also check for
    ``sidecar_type == "access_envelope"`` in any active PG driver config.
"""

import json
import logging
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple, Union

from geojson_pydantic import Feature

from dynastore.models.query_builder import QueryRequest
from dynastore.modules.db_config.query_executor import DbResource
from dynastore.modules.storage.access_filter_pg import access_filter_to_pg_clause
from dynastore.modules.storage.drivers.pg_sidecars.access_envelope_config import (
    AccessEnvelopeSidecarConfig,
)
from dynastore.modules.storage.drivers.pg_sidecars.base import (
    FeaturePipelineContext,
    FieldCapability,
    FieldDefinition,
    SidecarProtocol,
)
from dynastore.modules.storage.drivers.pg_sidecars.registry import SidecarRegistry

logger = logging.getLogger(__name__)

_SIDECAR_ID = "access_envelope"
_SIDECAR_ALIAS = "ae"


class AccessEnvelopeSidecar(SidecarProtocol):
    """PG sub-table sidecar for per-item ABAC access envelopes.

    Stores ``{schema}.{table}_access_envelope`` — one row per hub item,
    keyed by ``geoid``.  The JSONB ``access_envelope`` column carries::

        {"visibility": "...", "owner": "...", "attrs": {...}}

    The envelope is **never** surfaced in ``Feature.properties``.

    See module docstring for the four KNOWN GAPS (#1457).
    """

    def __init__(self, config: AccessEnvelopeSidecarConfig, **_kwargs: Any) -> None:
        self.config = config

    # ── Identity ─────────────────────────────────────────────────────────────

    @property
    def sidecar_id(self) -> str:
        return _SIDECAR_ID

    @property
    def sidecar_type_id(self) -> str:
        return _SIDECAR_ID

    def is_mandatory(self) -> bool:
        return False

    # ── DDL ──────────────────────────────────────────────────────────────────

    def get_ddl(
        self,
        physical_table: str,
        partition_keys: Optional[List[str]] = None,
        partition_key_types: Optional[Dict[str, str]] = None,
        has_validity: bool = False,
    ) -> str:
        """Create the ``{table}_access_envelope`` sub-table and optional GIN index.

        This is a simple two-column table (geoid PK + access_envelope JSONB).
        Partitioning is intentionally NOT propagated — the envelope sub-table
        is not part of the item validity window, so the partition key from the
        attributes sidecar does not apply here.
        """
        col_name = self.config.column_name
        table_name = f"{physical_table}_{_SIDECAR_ID}"

        create_sql = (
            f'CREATE TABLE IF NOT EXISTS {{schema}}."{table_name}" (\n'
            f'    geoid TEXT NOT NULL,\n'
            f'    {col_name} JSONB NOT NULL,\n'
            f'    PRIMARY KEY (geoid)\n'
            f');\n'
        )

        fk_sql = (
            f'ALTER TABLE {{schema}}."{table_name}" '
            f'ADD CONSTRAINT "fk_{table_name}_hub" '
            f'FOREIGN KEY (geoid) '
            f'REFERENCES {{schema}}."{physical_table}" (geoid) '
            f'ON DELETE CASCADE;\n'
        )

        ddl = create_sql + fk_sql

        if self.config.gin_index:
            ddl += (
                f'CREATE INDEX IF NOT EXISTS "idx_{table_name}_gin" '
                f'ON {{schema}}."{table_name}" USING GIN ({col_name});\n'
            )

        # Per-attr btree expression indexes (#1453). The ABAC read filter emits
        # ``(<col>->'attrs'->>'<key>') = ANY(:p)`` and CAST(...) range
        # comparisons (see ``access_filter_pg.py``); a btree index on the SAME
        # JSONB expression accelerates both — without promoting attrs to real
        # columns (the rejected #1450 approach). Opt-in via ``btree_attrs_index``
        # plus the operator-declared ``known_attrs_keys``. Additive only
        # (CREATE INDEX IF NOT EXISTS) — no ALTER / migration, replay-safe.
        if self.config.btree_attrs_index and self.config.known_attrs_keys:
            import re as _re

            for key in self.config.known_attrs_keys:
                # Keys come from config, not request input, but an operator typo
                # must never produce injectable or invalid DDL — restrict to a
                # safe identifier charset and skip (with a warning) otherwise.
                if not _re.fullmatch(r"[A-Za-z0-9_]+", key):
                    logger.warning(
                        "AccessEnvelopeSidecar.get_ddl: skipping btree index for "
                        "attrs key %r — only [A-Za-z0-9_] is allowed",
                        key,
                    )
                    continue
                ddl += (
                    f'CREATE INDEX IF NOT EXISTS "idx_{table_name}_attrs_{key}" '
                    f'ON {{schema}}."{table_name}" '
                    f"(({col_name}->'attrs'->>'{key}'));\n"
                )

        return ddl

    # ── Query building ────────────────────────────────────────────────────────

    def _default_sidecar_alias(self) -> str:
        """Short ``ae`` alias — also hardcoded into ``apply_query_context``
        and ``get_queryable_fields`` below, so it must stay in sync with
        whatever alias a caller ultimately joins this sidecar under."""
        return _SIDECAR_ALIAS

    # get_join_clause: uses the SidecarProtocol base default (plain
    # hub.geoid = sidecar.geoid join against "{hub_table}_access_envelope").

    def get_select_fields(
        self,
        request: Optional[QueryRequest] = None,
        sidecar_alias: Optional[str] = None,
        include_all: bool = False,
    ) -> List[str]:
        """The envelope is used only in WHERE — return no SELECT fields."""
        return []

    def apply_query_context(
        self,
        request: QueryRequest,
        context: Dict[str, Any],
    ) -> None:
        """Append the access-envelope WHERE clause to the query context.

        When ``request.access_filter`` is ``None`` (G2: not yet wired), appends
        ``"FALSE"`` — strict fail-closed default so no row slips through until
        the upstream wiring lands (see KNOWN GAPS G2 in module docstring).
        """
        col_ref = f"{_SIDECAR_ALIAS}.{self.config.column_name}"
        access_filter = getattr(request, "access_filter", None)

        if access_filter is None:
            context["where_conditions"].append("FALSE")
            return

        clause, params = access_filter_to_pg_clause(
            access_filter,
            envelope_col=col_ref,
        )

        if clause is not None:
            context["where_conditions"].append(clause)
            context["params"].update(params)

    def get_queryable_fields(self) -> Dict[str, FieldDefinition]:
        """Return the envelope column as a storage-only field (expose=False)."""
        return {
            self.config.column_name: FieldDefinition(
                name=self.config.column_name,
                data_type="jsonb",
                sql_expression=f"{_SIDECAR_ALIAS}.{self.config.column_name}",
                capabilities=[FieldCapability.FILTERABLE],
                expose=False,
                description="ABAC access envelope (storage-only; never projected to Feature).",
            )
        }

    def get_feature_type_schema(self) -> Dict[str, Any]:
        """No schema contribution — envelope is never exposed."""
        return {}

    def resolve_query_path(self, attr_name: str) -> Optional[Tuple[str, str]]:
        return None

    # ── Write path ────────────────────────────────────────────────────────────

    def prepare_upsert_payload(
        self, feature: Union[Feature, Dict[str, Any]], context: Dict[str, Any]
    ) -> Optional[Dict[str, Any]]:
        """Build the sub-table row from ``context["_access_envelope"]``.

        Returns ``None`` when the key is absent (G1: upstream wiring not yet
        present); the pipeline skips the write for this sidecar.
        """
        raw = context.get("_access_envelope")
        if raw is None:
            return None

        geoid = context.get("geoid")
        if not geoid:
            logger.warning(
                "AccessEnvelopeSidecar.prepare_upsert_payload: "
                "geoid missing from context — skipping envelope write"
            )
            return None

        envelope = {
            "visibility": raw.get("_visibility"),
            "owner": raw.get("_owner"),
            "attrs": raw.get("_attrs") or {},
        }
        return {"geoid": geoid, self.config.column_name: json.dumps(envelope)}

    def finalize_upsert_payload(
        self,
        sc_payload: Dict[str, Any],
        hub_row: Dict[str, Any],
        context: Dict[str, Any],
    ) -> Dict[str, Any]:
        return sc_payload.copy()

    # ── Read path — no-op ─────────────────────────────────────────────────────

    def map_row_to_feature(
        self,
        row: Dict[str, Any],
        feature: Feature,
        context: FeaturePipelineContext,
    ) -> None:
        """No-op: the envelope must never appear in Feature output (security)."""
        pass

    def get_internal_columns(self) -> set:
        return {self.config.column_name}

    # ── Lifecycle stubs ───────────────────────────────────────────────────────

    async def setup_lifecycle_hooks(
        self, conn: DbResource, schema: str, table_name: str
    ) -> None:
        pass

    async def on_partition_create(
        self,
        conn: DbResource,
        schema: str,
        parent_table: str,
        partition_table: str,
        partition_value: Any,
    ) -> None:
        pass

    async def expire_version(
        self,
        conn: DbResource,
        physical_schema: str,
        physical_table: str,
        geoid: str,
        expire_at: datetime,
    ) -> int:
        """The access envelope sub-table has no validity column — always 0."""
        return 0

    # ── Identity helpers ──────────────────────────────────────────────────────

    def get_identity_columns(self) -> List[str]:
        return ["geoid"]


SidecarRegistry.register(_SIDECAR_ID, AccessEnvelopeSidecar)
