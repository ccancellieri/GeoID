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

"""DDL + DQL/DML for the ``region_mapping.mappings`` table (dynastore#2821).

One row per claim: ``claim_ci`` (case-folded claim text) is the PRIMARY KEY —
it carries the global uniqueness invariant, so a second mapping claiming the
same text hits the PK and PostgreSQL raises ``23505``, mapped to HTTP 409 by
the existing global exception-handler chain
(``dynastore.extensions.tools.exception_handlers.ConflictExceptionHandler``).
Re-applying the SAME mapping's SAME claim must be an idempotent update, not a
conflict — see :data:`UPDATE_OWN_CLAIM` / :data:`INSERT_CLAIM` below for how
the write path keeps both properties true without swallowing the real
constraint violation.

DDL is owned entirely by this module (schema ``region_mapping``, table
``mappings``) and provisioned once at extension-lifespan startup via
:func:`ensure_mappings_table` — mirrors
``dynastore.modules.local.local_upload_store.ensure_local_upload_tickets_table``
(``DDLBatch`` sentinel+steps, multi-pod-startup safe, ``IF NOT EXISTS`` only,
never ``ALTER``/``DROP``).
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple

from sqlalchemy import literal_column

from dynastore.modules.db_config.maintenance_tools import ensure_schema_exists
from dynastore.modules.db_config.query_executor import (
    DbResource,
    DDLBatch,
    DDLQuery,
    DQLQuery,
    ResultHandler,
    managed_transaction,
)

SCHEMA = "region_mapping"
TABLE = "mappings"
_QUALIFIED_TABLE = f"{SCHEMA}.{TABLE}"

# Every column on the table — the whitelist backing both the CQL2 filter
# pipeline (GET /region-mappings, GET /region-mappings/region.json) and the
# API's list/select projections. No other property name is ever accepted.
#
# ``title`` is JSONB (multilanguage — see ``RegisterMappingRequest.title``);
# an exact-match CQL2 filter against it only matches the literal on-disk
# JSON representation, not a resolved-language value. Left in the whitelist
# for consistency (still a legitimate column to project/order by), not
# because equality filtering on it is generally useful.
ALLOWED_COLUMNS: Tuple[str, ...] = (
    "claim_ci", "claim", "mapping_id", "role",
    "src_catalog", "src_collection", "region_prop",
    "alias", "title",
    "layer_name", "server_type", "server_min_zoom",
    "server_max_native_zoom", "server_max_zoom", "unique_id_prop", "digits",
    "created_at", "updated_at",
)

# ---------------------------------------------------------------------------
# DDL — schema/table/index, IF NOT EXISTS only.
# ---------------------------------------------------------------------------

# Every mapping-level column below (everything but the claim identity —
# claim_ci/claim/mapping_id/role) is duplicated onto every claim row sharing
# a mapping_id, exactly like the pre-existing title/alias/region_prop
# columns -- the table is one row per *claim*, not one row per *mapping*
# (see module docstring). ``apply_mapping`` writes the same values into every
# row of a mapping's claim set.
_TABLE_DDL = f"""
CREATE TABLE IF NOT EXISTS {_QUALIFIED_TABLE} (
    claim_ci                TEXT NOT NULL PRIMARY KEY,
    claim                   TEXT NOT NULL,
    mapping_id              TEXT NOT NULL,
    role                    TEXT NOT NULL,
    src_catalog             TEXT NOT NULL,
    src_collection          TEXT NOT NULL,
    region_prop             TEXT NOT NULL,
    alias                   TEXT,
    title                   JSONB,
    layer_name              TEXT NOT NULL DEFAULT 'default',
    server_type             TEXT NOT NULL DEFAULT 'MVT',
    server_subdomains       JSONB NOT NULL DEFAULT '[]'::jsonb,
    server_min_zoom         INTEGER NOT NULL DEFAULT 0,
    server_max_native_zoom  INTEGER NOT NULL DEFAULT 12,
    server_max_zoom         INTEGER NOT NULL DEFAULT 28,
    unique_id_prop          TEXT,
    digits                  INTEGER NOT NULL DEFAULT 255,
    created_at              TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at              TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
"""

_INDEX_DDL = f"""
CREATE INDEX IF NOT EXISTS idx_region_mapping_mappings_mapping_id
    ON {_QUALIFIED_TABLE} (mapping_id);
"""


async def ensure_mappings_table(engine: Any) -> None:
    """Create the ``region_mapping`` schema/table/index if absent.

    Idempotent (``IF NOT EXISTS`` throughout); safe to call on every pod's
    startup. The index is the sentinel — on a warm boot the whole batch is
    skipped in one round-trip. Called from
    ``RegionMappingService.lifespan``, never inlined elsewhere.
    """
    if engine is None:
        return
    batch = DDLBatch(
        sentinel=DDLQuery(_INDEX_DDL),
        steps=[DDLQuery(_TABLE_DDL), DDLQuery(_INDEX_DDL)],
    )
    async with managed_transaction(engine) as conn:
        await ensure_schema_exists(conn, SCHEMA)
        await batch.execute(conn)


# ---------------------------------------------------------------------------
# DML — claim writes.
# ---------------------------------------------------------------------------

# Scoped to (claim_ci, mapping_id): only updates a claim already owned by
# THIS mapping. Returns None when no such row exists — either the claim_ci
# is brand new, or it is owned by a *different* mapping, in which case the
# caller falls through to INSERT_CLAIM, whose PK violation is the real
# PG 23505 the global exception-handler chain maps to 409.
UPDATE_OWN_CLAIM = DQLQuery(
    f"""
    UPDATE {_QUALIFIED_TABLE} SET
        claim                  = :claim,
        role                   = :role,
        src_catalog            = :src_catalog,
        src_collection         = :src_collection,
        region_prop            = :region_prop,
        alias                  = :alias,
        title                  = CAST(:title AS jsonb),
        layer_name             = :layer_name,
        server_type            = :server_type,
        server_subdomains      = CAST(:server_subdomains AS jsonb),
        server_min_zoom        = :server_min_zoom,
        server_max_native_zoom = :server_max_native_zoom,
        server_max_zoom        = :server_max_zoom,
        unique_id_prop         = :unique_id_prop,
        digits                 = :digits,
        updated_at             = NOW()
    WHERE claim_ci = :claim_ci AND mapping_id = :mapping_id
    RETURNING *
    """,
    result_handler=ResultHandler.ONE_DICT,
)

INSERT_CLAIM = DQLQuery(
    f"""
    INSERT INTO {_QUALIFIED_TABLE}
        (claim_ci, claim, mapping_id, role, src_catalog, src_collection, region_prop, alias,
         title, layer_name, server_type, server_subdomains, server_min_zoom,
         server_max_native_zoom, server_max_zoom, unique_id_prop, digits)
    VALUES
        (:claim_ci, :claim, :mapping_id, :role, :src_catalog, :src_collection, :region_prop, :alias,
         CAST(:title AS jsonb), :layer_name, :server_type, CAST(:server_subdomains AS jsonb),
         :server_min_zoom, :server_max_native_zoom, :server_max_zoom, :unique_id_prop, :digits)
    RETURNING *
    """,
    result_handler=ResultHandler.ONE_DICT,
)

# Stale-claim cleanup on re-apply with a changed alias/column set: delete
# every row still owned by ``mapping_id`` whose ``claim_ci`` is not in the
# freshly computed set, so it does not squat the PK forever.
DELETE_STALE_CLAIMS = DQLQuery(
    f"""
    DELETE FROM {_QUALIFIED_TABLE}
    WHERE mapping_id = :mapping_id
      AND NOT (claim_ci = ANY(CAST(:keep_claim_ci AS TEXT[])))
    RETURNING claim_ci
    """,
    result_handler=ResultHandler.ALL_DICTS,
)

# Authoritative revoke: every claim currently sharing ``mapping_id``.
DELETE_CLAIMS_BY_MAPPING_ID = DQLQuery(
    f"""
    DELETE FROM {_QUALIFIED_TABLE}
    WHERE mapping_id = :mapping_id
    RETURNING claim_ci
    """,
    result_handler=ResultHandler.ALL_DICTS,
)

# Referential-integrity cleanup: every claim sourced from a collection that
# was just hard-deleted elsewhere in the platform. Fired from a best-effort
# async event listener (lifecycle.py), never from the collection-delete
# path itself -- see that module for why.
DELETE_CLAIMS_BY_SOURCE_COLLECTION = DQLQuery(
    f"""
    DELETE FROM {_QUALIFIED_TABLE}
    WHERE src_catalog = :catalog_id AND src_collection = :collection_id
    RETURNING claim_ci
    """,
    result_handler=ResultHandler.ALL_DICTS,
)

# Same, scoped to an entire catalog (a catalog hard-delete removes every
# collection in it in one shot).
DELETE_CLAIMS_BY_SOURCE_CATALOG = DQLQuery(
    f"""
    DELETE FROM {_QUALIFIED_TABLE}
    WHERE src_catalog = :catalog_id
    RETURNING claim_ci
    """,
    result_handler=ResultHandler.ALL_DICTS,
)

# ---------------------------------------------------------------------------
# DQL — targeted reads.
# ---------------------------------------------------------------------------

SELECT_CLAIM_BY_CI = DQLQuery(
    f"SELECT * FROM {_QUALIFIED_TABLE} WHERE claim_ci = :claim_ci",
    result_handler=ResultHandler.ONE_DICT,
)

SELECT_CLAIMS_BY_MAPPING_ID = DQLQuery(
    f"SELECT * FROM {_QUALIFIED_TABLE} WHERE mapping_id = :mapping_id ORDER BY claim",
    result_handler=ResultHandler.ALL_DICTS,
)

SELECT_PRIMARY_BY_MAPPING_ID = DQLQuery(
    f"""
    SELECT * FROM {_QUALIFIED_TABLE}
    WHERE mapping_id = :mapping_id AND role = 'primary'
    LIMIT 1
    """,
    result_handler=ResultHandler.ONE_DICT,
)


# ---------------------------------------------------------------------------
# CQL2 filter pipeline — property-to-column whitelist evaluator.
# ---------------------------------------------------------------------------


def build_cql_field_mapping() -> Dict[str, Any]:
    """Field mapping for ``parse_cql_filter``/``parse_cql2_json_filter``.

    Every claim column maps to itself as a plain SQL identifier via
    ``literal_column`` (not ``text``) — pygeofilter's ``to_filter`` needs a
    column-like object with comparison operators to emit a real
    bound-parameter predicate (mirrors the pattern in
    ``modules/catalog/item_query.py``). No column outside
    :data:`ALLOWED_COLUMNS` is ever exposed, so an unknown-field filter is
    rejected by ``parse_cql_filter`` itself (``valid_props`` mismatch) before
    any SQL is built.
    """
    return {name: literal_column(name) for name in ALLOWED_COLUMNS}


# ---------------------------------------------------------------------------
# Dynamic listing — WHERE + ORDER BY + LIMIT/OFFSET over the whitelisted
# columns. Built per-request (equality filters + an optional CQL2 WHERE
# fragment vary by caller), executed as a one-off DQLQuery — mirrors the
# dynamic-SQL-per-request pattern in ``modules/catalog/item_query.py``.
# ---------------------------------------------------------------------------


async def list_claims(
    db_resource: DbResource,
    *,
    mapping_id: Optional[str] = None,
    role: Optional[str] = None,
    src_catalog: Optional[str] = None,
    src_collection: Optional[str] = None,
    claim_ci: Optional[str] = None,
    cql_where: str = "",
    cql_params: Optional[Dict[str, Any]] = None,
    order_by: str = "mapping_id, claim",
    limit: int = 200,
    offset: int = 0,
) -> List[Dict[str, Any]]:
    """List claim rows filtered by any combination of equality predicates
    plus an optional pre-compiled CQL2 WHERE fragment.

    ``cql_where``/``cql_params`` are the output of
    ``dynastore.modules.tools.cql.parse_cql_filter`` against
    :func:`build_cql_field_mapping` — already a safe, bind-parameterized SQL
    fragment; embedded as raw text (never user input directly).
    """
    where_parts: List[str] = []
    params: Dict[str, Any] = dict(cql_params or {})

    if mapping_id is not None:
        where_parts.append("mapping_id = :mapping_id")
        params["mapping_id"] = mapping_id
    if role is not None:
        where_parts.append("role = :role")
        params["role"] = role
    if src_catalog is not None:
        where_parts.append("src_catalog = :src_catalog")
        params["src_catalog"] = src_catalog
    if src_collection is not None:
        where_parts.append("src_collection = :src_collection")
        params["src_collection"] = src_collection
    if claim_ci is not None:
        where_parts.append("claim_ci = :claim_ci")
        params["claim_ci"] = claim_ci
    if cql_where:
        where_parts.append(f"({cql_where})")

    where_clause = f"WHERE {' AND '.join(where_parts)}" if where_parts else ""
    sql = (
        f"SELECT * FROM {_QUALIFIED_TABLE} {where_clause} "
        f"ORDER BY {order_by} LIMIT :limit OFFSET :offset"
    )
    params["limit"] = limit
    params["offset"] = offset

    query = DQLQuery(sql, result_handler=ResultHandler.ALL_DICTS)
    return await query.execute(db_resource, **params)
