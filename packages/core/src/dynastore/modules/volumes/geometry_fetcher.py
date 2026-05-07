"""PostGIS-backed geometry fetcher for tile-content generation.

Queries the geometries sidecar for WKB geometry + height attribute
for an explicit list of feature IDs. Mirrors the sidecar_bounds
design: pure SQL builder + row parser (TDD'd) + injectable class.

The injected ``connection_factory`` + resolver callbacks follow the
exact same contract as ``SidecarBoundsSource`` so the two can share
the same platform-wiring in ``platform_bounds_source.py``.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional, Sequence

from dynastore.models.protocols.geometry_fetcher import (
    FeatureGeometry,
    GeometryFetcherProtocol,
)
from dynastore.tools.db import validate_sql_identifier

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class GeometryQuerySpec:
    schema: str
    hub_table: str
    geometries_table: str
    feature_ids: Sequence[str]
    feature_id_column: str = "geoid"
    geom_column: str = "geom"
    height_column: Optional[str] = None


def build_geometry_query(spec: GeometryQuerySpec) -> str:
    """Return SQL that fetches WKB geometry + height for ``spec.feature_ids``.

    Columns returned per row: ``feature_id``, ``geom_wkb``, ``height``.
    Rows where the geometry is NULL are excluded.

    The feature-ID filter uses a ``$1::text[]`` parameter placeholder; callers
    must pass ``list(spec.feature_ids)`` as the first bind parameter so that
    feature IDs are never interpolated into the SQL string.
    """
    hub = f'"{spec.schema}"."{spec.hub_table}"'
    geoms = f'"{spec.schema}"."{spec.geometries_table}"'

    if not spec.feature_ids:
        return (
            "SELECT NULL::text AS feature_id, NULL::bytea AS geom_wkb, "
            "0.0::float AS height WHERE false"
        )

    if spec.height_column:
        height_expr = f'COALESCE(h."{spec.height_column}"::float, 0.0)'
    else:
        height_expr = "0.0"

    return (
        "SELECT "
        f'h."{spec.feature_id_column}" AS feature_id, '
        f'ST_AsBinary(ST_Force3D(g."{spec.geom_column}")) AS geom_wkb, '
        f"{height_expr} AS height "
        f"FROM {hub} h "
        f"JOIN {geoms} g "
        f'ON h."{spec.feature_id_column}" = g."{spec.feature_id_column}" '
        f'WHERE g."{spec.geom_column}" IS NOT NULL '
        f'  AND h."{spec.feature_id_column}" = ANY($1::text[])'
    )


def row_to_feature_geometry(row: Dict[str, Any]) -> Optional[FeatureGeometry]:
    """Parse a row from ``build_geometry_query`` into ``FeatureGeometry``.

    Returns ``None`` when the geometry bytes are absent (defensive guard).
    """
    wkb = row.get("geom_wkb")
    if wkb is None:
        return None
    if isinstance(wkb, memoryview):
        wkb = bytes(wkb)
    return FeatureGeometry(
        feature_id=str(row["feature_id"]),
        geom_wkb=wkb,
        height=float(row.get("height") or 0.0),
    )


def rows_to_geometries(
    rows: Iterable[Dict[str, Any]],
) -> List[FeatureGeometry]:
    """Drain *rows* into a list, skipping entries with null geometry."""
    out: List[FeatureGeometry] = []
    for r in rows:
        fg = row_to_feature_geometry(r)
        if fg is not None:
            out.append(fg)
    return out


class SidecarGeometryFetcher:
    """GeometryFetcherProtocol backed by the geometries sidecar table.

    Constructor arguments mirror ``SidecarBoundsSource`` so the same
    platform wiring can instantiate both.
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

    async def get_geometries(
        self,
        catalog_id: str,
        collection_id: str,
        feature_ids: Sequence[str],
    ) -> Sequence[FeatureGeometry]:
        if not feature_ids:
            return []

        for ident in (catalog_id, collection_id):
            validate_sql_identifier(ident)

        schema = await self._resolve_schema(catalog_id)
        validate_sql_identifier(schema)
        hub = await self._hub_table_for_collection(catalog_id, collection_id)
        geoms = await self._geometries_table_for_collection(catalog_id, collection_id)
        for t in (hub, geoms):
            validate_sql_identifier(t)

        spec = GeometryQuerySpec(
            schema=schema,
            hub_table=hub,
            geometries_table=geoms,
            feature_ids=list(feature_ids),
            height_column=self._height_column,
        )
        sql = build_geometry_query(spec)

        async with self._connect() as conn:
            rows = await conn.execute(sql, list(spec.feature_ids))
            if hasattr(rows, "__aiter__"):
                rows = [r async for r in rows]
            elif hasattr(rows, "fetchall"):
                rows = await rows.fetchall()

        normalized = [dict(r) if not isinstance(r, dict) else r for r in rows]
        return rows_to_geometries(normalized)


assert issubclass(SidecarGeometryFetcher, GeometryFetcherProtocol)
