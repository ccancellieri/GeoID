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

"""pg_trgm lexical similarity search over materialized dimension members.

The OGC Dimensions Similarity conformance class
(``conf/dimension-similarity``) ranks dimension members by lexical
similarity to a free-text query. This module implements the cheap default
that every dimension inherits: a PostgreSQL ``pg_trgm`` trigram match
against the member label.

Dimension members are materialized as OGC API - Records into the internal
``_dimensions_`` catalog (see ``materialize_dimension`` in
``dimensions_extension``). Each member is a STAC Feature whose label lives
in the attributes sidecar JSONB column under ``title`` and whose member
code lives under ``id``. The trigram match therefore runs against
``attributes->>'title'``.

The SQL builder is a pure function so it can be unit-tested without a
database; the index helper and the search entry point need a live
connection (integration-tested against Postgres with ``pg_trgm`` enabled).
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)

# Default similarity threshold for the ``%`` operator. pg_trgm's session
# default is 0.3; we pass it explicitly via ``similarity(...) >= :threshold``
# so the behaviour does not depend on a server GUC the deployment may have
# changed.
DEFAULT_SIMILARITY_THRESHOLD = 0.3

# Attribute key holding the human-readable member label in the materialized
# record's attributes JSONB (set by ``_member_to_feature``).
LABEL_KEY = "title"
# Attribute key holding the member code (the STAC Feature id).
CODE_KEY = "id"


def build_similarity_query(
    schema: str,
    table: str,
    *,
    label_key: str = LABEL_KEY,
    code_key: str = CODE_KEY,
) -> Tuple[str, str]:
    """Build the pg_trgm similarity SQL for a materialized dimension table.

    Returns ``(sql, attr_table)``. The SQL ranks members of the hub table
    ``table`` (joined to its ``<table>_attributes`` sidecar on ``geoid``)
    by trigram similarity of ``attributes->>'<label_key>'`` to a bound
    ``:q`` parameter, filtering with the ``%`` operator and ordering by
    descending score.

    Bound parameters expected at execution time:
      - ``:q``         — the free-text query string
      - ``:threshold`` — minimum similarity (0..1)
      - ``:limit``     — max rows to return

    The schema and table names MUST already be validated SQL identifiers
    (see ``validate_sql_identifier``); they are interpolated as identifiers
    while the user query is always a bound parameter.
    """
    attr_table = f"{table}_attributes"
    label_expr = f"s.attributes->>'{label_key}'"
    code_expr = f"s.attributes->>'{code_key}'"
    sql = (
        f"SELECT {code_expr} AS id, "
        f"{label_expr} AS name, "
        f"similarity({label_expr}, :q) AS score "
        f'FROM "{schema}"."{table}" h '
        f'JOIN "{schema}"."{attr_table}" s ON h.geoid = s.geoid '
        f"WHERE h.deleted_at IS NULL "
        f"AND {label_expr} IS NOT NULL "
        f"AND {label_expr} % :q "
        f"AND similarity({label_expr}, :q) >= :threshold "
        f"ORDER BY score DESC, name ASC "
        f"LIMIT :limit"
    )
    return sql, attr_table


def build_index_ddl(schema: str, table: str, *, label_key: str = LABEL_KEY) -> str:
    """Build the idempotent GIN trigram index DDL for a member label column.

    Indexes ``(attributes->>'<label_key>')`` on the attributes sidecar with
    ``gin_trgm_ops`` so the ``%`` filter and ``similarity()`` ranking in
    :func:`build_similarity_query` are index-accelerated.

    ``schema`` / ``table`` MUST be validated SQL identifiers.
    """
    attr_table = f"{table}_attributes"
    index_name = f"{table}_attr_{label_key}_trgm_idx"
    expr = f"((attributes->>'{label_key}'))"
    return (
        f'CREATE INDEX IF NOT EXISTS "{index_name}" '
        f'ON "{schema}"."{attr_table}" '
        f"USING gin ({expr} gin_trgm_ops);"
    )


async def ensure_similarity_index(
    conn: Any,
    schema: str,
    table: str,
    *,
    label_key: str = LABEL_KEY,
) -> None:
    """Create the GIN trigram index on the member label column if missing.

    Idempotent (``CREATE INDEX IF NOT EXISTS``). Safe to call after each
    materialization run; the index is a no-op once it exists.
    """
    from dynastore.modules.db_config.query_executor import DDLQuery

    ddl = build_index_ddl(schema, table, label_key=label_key)
    await DDLQuery(ddl).execute(conn)
    logger.info(
        "Ensured pg_trgm similarity index on '%s.%s_attributes' (%s)",
        schema, table, label_key,
    )


async def _resolve_member_table(
    dimension_id: str,
    *,
    db_resource: Any,
) -> Optional[str]:
    """Resolve the physical PG table holding ``dimension_id``'s members.

    Mirrors ``ItemService._resolve_physical_table``: try the READ driver
    first, then fall back to the primary WRITE driver. pg_trgm requires PG,
    so only a driver exposing ``resolve_physical_table`` (the PG items
    driver) can satisfy this; non-PG drivers are skipped.
    """
    from dynastore.models.dimensions import DIMENSIONS_CATALOG_ID
    from dynastore.modules.storage.router import get_driver, get_write_drivers
    from dynastore.modules.storage.routing_config import Operation

    for _op in (Operation.READ, None):
        try:
            if _op is None:
                write_drivers = await get_write_drivers(
                    DIMENSIONS_CATALOG_ID, dimension_id,
                )
                driver = write_drivers[0].driver if write_drivers else None
            else:
                driver = await get_driver(_op, DIMENSIONS_CATALOG_ID, dimension_id)
        except Exception:
            continue
        resolver = getattr(driver, "resolve_physical_table", None)
        if resolver is not None:
            result = await resolver(
                DIMENSIONS_CATALOG_ID, dimension_id, db_resource=db_resource,
            )
            if result:
                return result
    return None


async def search_similar(
    dimension_id: str,
    q: str,
    *,
    limit: int = 20,
    threshold: float = DEFAULT_SIMILARITY_THRESHOLD,
    db_resource: Any = None,
) -> List[Dict[str, Any]]:
    """Default Similarity search shared by every dimension provider.

    Ranks the materialized members of ``dimension_id`` by ``pg_trgm``
    trigram similarity of their label to ``q``. Returns a list of
    ``{"id", "name", "score"}`` dicts ordered by descending score.

    This is the cheap default that satisfies ``conf/dimension-similarity``
    for any dimension whose members were materialized into the
    ``_dimensions_`` catalog. Providers needing semantically richer
    similarity (tree distance, embeddings) override at a higher layer.

    Resolves the dimension's physical schema/table from the catalogs
    protocol, ensures the trigram index exists, then runs the ranked query.
    """
    from dynastore.models.dimensions import DIMENSIONS_CATALOG_ID
    from dynastore.models.driver_context import DriverContext
    from dynastore.models.protocols.catalogs import CatalogsProtocol
    from dynastore.modules.db_config.query_executor import (
        GeoDQLQuery,
        ResultHandler,
        managed_transaction,
    )
    from dynastore.tools.discovery import get_protocol
    from dynastore.tools.protocol_helpers import get_engine
    from dynastore.tools.db import validate_sql_identifier

    catalogs = get_protocol(CatalogsProtocol)
    if catalogs is None:
        raise RuntimeError("CatalogsProtocol not available — cannot run similarity search.")

    engine = db_resource or get_engine()
    if engine is None:
        raise RuntimeError("DB engine not available — cannot run similarity search.")

    ctx = DriverContext(db_resource=engine)
    schema = await catalogs.resolve_physical_schema(DIMENSIONS_CATALOG_ID, ctx=ctx)
    if not schema:
        raise ValueError(
            f"No physical schema for dimensions catalog '{DIMENSIONS_CATALOG_ID}'."
        )
    table = await _resolve_member_table(dimension_id, db_resource=engine)
    if not table:
        raise ValueError(
            f"Dimension '{dimension_id}' has no materialized members "
            f"(no physical table in '{DIMENSIONS_CATALOG_ID}'). "
            "Run the dimensions_materialize task first."
        )

    schema = validate_sql_identifier(schema)
    table = validate_sql_identifier(table)

    sql, _ = build_similarity_query(schema, table)

    # The GIN trigram index is provisioned at materialization time
    # (``dimensions_extension.materialize_dimension``), not here: this
    # function backs the public GET similarity search endpoint, and running
    # `CREATE INDEX` DDL inline on that read path took a lock on the shared
    # attributes sidecar table on every search request (#2831). ``%`` and
    # ``similarity()`` do not require the index to produce correct results —
    # a dimension materialized before this provisioning existed just runs an
    # unindexed (slower) sequential scan until its next materialization run
    # backfills the index.
    async with managed_transaction(engine) as conn:
        rows = await GeoDQLQuery(
            sql, result_handler=ResultHandler.ALL_DICTS,
        ).execute(conn, q=q, threshold=threshold, limit=limit)

    return [
        {"id": r.get("id"), "name": r.get("name"), "score": r.get("score")}
        for r in (rows or [])
    ]
