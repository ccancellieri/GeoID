"""Sidecar-backed BoundsSourceProtocol implementation.

Queries the geometries sidecar table for per-feature 3D bboxes.
Uses ST_XMin/XMax/YMin/YMax/ZMin/ZMax on ST_Force3D(geom) so Z is
populated for both 2D and 3D features (2D collapses to z=0; the
height-attr fallback is applied by a separate extension when
configured).

Module contract:
    - ``build_bounds_query(...)`` — pure SQL string builder
    - ``row_to_feature_bounds(row)`` — pure row-parser
    - ``SidecarBoundsSource`` — protocol-compliant class that wires the
      two together against a live DB connection

The pure pieces are TDD'd; the class itself is exercised with fake
connection fixtures. End-to-end integration against a real tenant
database is deferred to a future test fixture setup.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any, Dict, Iterable, Optional, Sequence

from dynastore.models.protocols.bounds_source import BoundsSourceProtocol
from dynastore.modules.volumes.bounds import FeatureBounds
from dynastore.tools.cache import cached
from dynastore.tools.db import validate_sql_identifier

logger = logging.getLogger(__name__)


# Default TTL matches ``VolumesConfig.on_demand_cache_ttl_s`` (1h) — the
# user's preferred alternative to the spec's pre-compute volumes tasks.
# Jitter is ~10% of TTL to spread expiry across the fleet.
_BOUNDS_CACHE_TTL_S = 3600
_BOUNDS_CACHE_JITTER_S = 300


@dataclass(frozen=True)
class BoundsQuerySpec:
    """Inputs needed to build the sidecar bounds query for one collection."""

    schema: str
    hub_table: str
    geometries_table: str
    feature_id_column: str = "geoid"
    geom_column: str = "geom"
    height_column: Optional[str] = None  # COALESCE fallback for flat features
    limit: Optional[int] = None


def build_bounds_query(spec: BoundsQuerySpec) -> str:
    """Emit the SQL text for one collection's 3D bbox extraction.

    Columns returned per row: ``feature_id, min_x, min_y, min_z, max_x,
    max_y, max_z`` — exactly matches ``row_to_feature_bounds`` expectations.
    """
    # Identifiers are caller-controlled (validated upstream via
    # validate_sql_identifier in the class wrapper); this builder assumes
    # they're safe. Both sides of the JOIN use validated schema-qualified
    # double-quoted identifiers.
    hub = f'"{spec.schema}"."{spec.hub_table}"'
    geoms = f'"{spec.schema}"."{spec.geometries_table}"'

    zmin_expr = f'ST_ZMin(ST_Force3D(g."{spec.geom_column}"))'
    zmax_expr = f'ST_ZMax(ST_Force3D(g."{spec.geom_column}"))'
    if spec.height_column:
        # 2D feature → ST_ZMin/ST_ZMax on ST_Force3D return 0. Widen the
        # z range to [0, height_col] when the hub carries a height column.
        zmin_expr = f"LEAST({zmin_expr}, 0)"
        zmax_expr = (
            f"GREATEST({zmax_expr}, "
            f'COALESCE(h."{spec.height_column}", 0))'
        )

    sql = (
        "SELECT "
        f'h."{spec.feature_id_column}" AS feature_id, '
        f'ST_XMin(g."{spec.geom_column}") AS min_x, '
        f'ST_YMin(g."{spec.geom_column}") AS min_y, '
        f"{zmin_expr} AS min_z, "
        f'ST_XMax(g."{spec.geom_column}") AS max_x, '
        f'ST_YMax(g."{spec.geom_column}") AS max_y, '
        f"{zmax_expr} AS max_z "
        f"FROM {hub} h "
        f"JOIN {geoms} g "
        f'ON h."{spec.feature_id_column}" = g."{spec.feature_id_column}" '
        f'WHERE g."{spec.geom_column}" IS NOT NULL'
    )
    if spec.limit is not None:
        sql += f" LIMIT {int(spec.limit)}"
    return sql


def row_to_feature_bounds(row: Dict[str, Any]) -> FeatureBounds:
    """Translate a row dict from build_bounds_query into a FeatureBounds.

    Accepts either ``str`` or non-str feature_id — the FeatureBounds
    dataclass stringifies it for stable tile hashing downstream.
    """
    return FeatureBounds(
        feature_id=str(row["feature_id"]),
        min_x=float(row["min_x"]),
        min_y=float(row["min_y"]),
        min_z=float(row["min_z"]),
        max_x=float(row["max_x"]),
        max_y=float(row["max_y"]),
        max_z=float(row["max_z"]),
    )


def rows_to_bounds(rows: Iterable[Dict[str, Any]]) -> Sequence[FeatureBounds]:
    """Drain an iterable of rows into a list of FeatureBounds.

    Skips rows where any coordinate is NULL (defensive against partial
    sidecar data — the WHERE clause already filters null geoms, but
    Force3D on a degenerate geometry can still emit NULLs).
    """
    out = []
    for r in rows:
        if any(
            r.get(k) is None
            for k in (
                "feature_id",
                "min_x",
                "min_y",
                "min_z",
                "max_x",
                "max_y",
                "max_z",
            )
        ):
            continue
        out.append(row_to_feature_bounds(r))
    return out


class SidecarBoundsSource:
    """BoundsSourceProtocol backed by the geometries sidecar.

    Requires a PostGIS tenant DB. Constructed with a ``connection_factory``
    (a callable returning an ``async with``-able SQL connection with a
    ``.execute(sql)`` coroutine that yields rows as dict-like objects).
    Keeping the factory injectable lets unit tests run without a live DB.
    """

    def __init__(
        self,
        *,
        connection_factory,
        schema_resolver,
        hub_table_for_collection,
        geometries_table_for_collection,
        height_column: Optional[str] = None,
    ) -> None:
        self._connect = connection_factory
        self._resolve_schema = schema_resolver
        self._hub_table_for_collection = hub_table_for_collection
        self._geometries_table_for_collection = geometries_table_for_collection
        self._height_column = height_column

    @cached(
        maxsize=1024,
        ttl=_BOUNDS_CACHE_TTL_S,
        jitter=_BOUNDS_CACHE_JITTER_S,
        namespace="volumes_sidecar_bounds",
        ignore=["self"],
    )
    async def get_bounds(
        self,
        catalog_id: str,
        collection_id: str,
        *,
        limit: Optional[int] = None,
    ) -> Sequence[FeatureBounds]:
        # Validate every identifier that's spliced into SQL upstream —
        # defense in depth; validate_sql_identifier is idempotent + cheap.
        for ident in (catalog_id, collection_id):
            validate_sql_identifier(ident)

        schema = await self._resolve_schema(catalog_id)
        validate_sql_identifier(schema)
        hub = await self._hub_table_for_collection(catalog_id, collection_id)
        geoms = await self._geometries_table_for_collection(
            catalog_id, collection_id,
        )
        for t in (hub, geoms):
            validate_sql_identifier(t)

        spec = BoundsQuerySpec(
            schema=schema,
            hub_table=hub,
            geometries_table=geoms,
            height_column=self._height_column,
            limit=limit,
        )
        sql = build_bounds_query(spec)

        async with self._connect() as conn:
            rows = await conn.execute(sql)
            # rows may be a list, an async iterator, or an aiopg/asyncpg
            # cursor — normalize to a sync list.
            if hasattr(rows, "__aiter__"):
                rows = [r async for r in rows]
            elif hasattr(rows, "fetchall"):
                rows = await rows.fetchall()
        return rows_to_bounds(rows)


# Structural protocol sanity check — ensures the class still satisfies
# BoundsSourceProtocol at import time. isinstance on a Protocol checks
# method signatures, so a simple reference here would not catch drift;
# we check issubclass against the runtime_checkable protocol instead.
assert issubclass(SidecarBoundsSource, BoundsSourceProtocol)
