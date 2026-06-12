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
from dynastore.modules.db_config.query_executor import DQLQuery, ResultHandler
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
class CollectionPhysicalLayout:
    """Resolved physical PG layout for one collection's geometry sidecar.

    Returned by a ``layout_resolver`` so the volumes tiler can read the
    *actual* hub table, geometries sidecar table, geometry column, and
    feature-id column from the storage driver/sidecar config instead of
    hardcoding the ``<collection_id>`` / ``<collection_id>_geometries`` /
    ``geom`` / ``geoid`` conventions. Shared by both ``SidecarBoundsSource``
    and ``SidecarGeometryFetcher``.
    """

    schema: str
    hub_table: str
    geometries_table: str
    geom_column: str = "geom"
    feature_id_column: str = "geoid"


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
        schema_resolver=None,
        hub_table_for_collection=None,
        geometries_table_for_collection=None,
        height_column: Optional[str] = None,
        layout_resolver=None,
    ) -> None:
        """Wire the bounds source against a tenant DB.

        Two resolution modes:

        - ``layout_resolver`` (preferred): an async callable
          ``(catalog_id, collection_id) -> CollectionPhysicalLayout`` that
          returns the full physical layout (schema, hub table, geometries
          table, geom column, feature-id column) resolved from the storage
          driver/sidecar config. Pluggable + configurable end to end.
        - the legacy ``schema_resolver`` + ``hub_table_for_collection`` +
          ``geometries_table_for_collection`` trio, kept for callers/tests
          that wire fixed table conventions and the default ``geom`` /
          ``geoid`` columns.
        """
        self._connect = connection_factory
        self._resolve_schema = schema_resolver
        self._hub_table_for_collection = hub_table_for_collection
        self._geometries_table_for_collection = geometries_table_for_collection
        self._height_column = height_column
        self._layout_resolver = layout_resolver

    async def _resolve_layout(
        self, catalog_id: str, collection_id: str,
    ) -> CollectionPhysicalLayout:
        """Resolve the physical layout via the preferred or legacy path."""
        if self._layout_resolver is not None:
            return await self._layout_resolver(catalog_id, collection_id)
        schema = await self._resolve_schema(catalog_id)
        hub = await self._hub_table_for_collection(catalog_id, collection_id)
        geoms = await self._geometries_table_for_collection(
            catalog_id, collection_id,
        )
        return CollectionPhysicalLayout(
            schema=schema, hub_table=hub, geometries_table=geoms,
        )

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

        layout = await self._resolve_layout(catalog_id, collection_id)
        for t in (
            layout.schema,
            layout.hub_table,
            layout.geometries_table,
            layout.geom_column,
            layout.feature_id_column,
        ):
            validate_sql_identifier(t)

        spec = BoundsQuerySpec(
            schema=layout.schema,
            hub_table=layout.hub_table,
            geometries_table=layout.geometries_table,
            feature_id_column=layout.feature_id_column,
            geom_column=layout.geom_column,
            height_column=self._height_column,
            limit=limit,
        )
        sql = build_bounds_query(spec)

        async with self._connect() as conn:
            raw_rows = await DQLQuery(
                sql,
                result_handler=ResultHandler.ALL_DICTS,
            ).execute(conn)
        return rows_to_bounds(raw_rows or [])


# Structural protocol sanity check — ensures the class still satisfies
# BoundsSourceProtocol at import time. isinstance on a Protocol checks
# method signatures, so a simple reference here would not catch drift;
# we check issubclass against the runtime_checkable protocol instead.
assert issubclass(SidecarBoundsSource, BoundsSourceProtocol)
