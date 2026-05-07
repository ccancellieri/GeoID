"""Platform-wired factories for SidecarBoundsSource and SidecarGeometryFetcher.

Not auto-registered — a deployment opts in by calling
``register_sidecar_bounds_source()`` during startup. Keeping the
registration explicit avoids imposing PostGIS + geometries-sidecar
assumptions on every /volumes/* caller.

``register_sidecar_bounds_source()`` registers BOTH the bounds source
(for tileset.json index building) AND the geometry fetcher (for tile
content generation). Both share the same connection factory and table
resolvers.
"""

from __future__ import annotations

import logging
from contextlib import asynccontextmanager
from typing import Optional

from dynastore.extensions.volumes.config import VolumesConfig
from dynastore.models.protocols import CatalogsProtocol, DatabaseProtocol
from dynastore.modules.volumes.geometry_fetcher import SidecarGeometryFetcher
from dynastore.modules.volumes.sidecar_bounds import SidecarBoundsSource
from dynastore.tools.discovery import get_protocol, register_plugin

logger = logging.getLogger(__name__)


def register_sidecar_bounds_source(
    *, volumes_config: Optional[VolumesConfig] = None,
) -> None:
    """Register SidecarBoundsSource + SidecarGeometryFetcher.

    Resolves the platform's DatabaseProtocol + CatalogsProtocol at call
    time — both must already be registered. Table-name conventions
    follow the geometries sidecar's standard: hub ``<collection_id>``,
    sidecar ``<collection_id>_geometries`` in the tenant schema.
    """
    cfg = volumes_config or VolumesConfig()

    @asynccontextmanager
    async def _connection_factory():
        db = get_protocol(DatabaseProtocol)
        if db is None:
            raise RuntimeError(
                "DatabaseProtocol not registered; "
                "cannot open bounds-source connection",
            )
        engine = db.async_engine
        if engine is None:
            raise RuntimeError(
                "DatabaseProtocol has no async engine; "
                "SidecarBoundsSource requires an async tenant DB",
            )
        async with engine.connect() as conn:
            yield conn

    async def _resolve_schema(catalog_id: str) -> str:
        catalogs = get_protocol(CatalogsProtocol)
        if catalogs is None:
            raise RuntimeError("CatalogsProtocol not registered")
        schema = await catalogs.resolve_physical_schema(catalog_id)
        if schema is None:
            raise RuntimeError(
                f"CatalogsProtocol could not resolve schema for "
                f"catalog_id={catalog_id!r}",
            )
        return schema

    async def _hub_table(catalog_id: str, collection_id: str) -> str:
        return collection_id

    async def _geometries_table(catalog_id: str, collection_id: str) -> str:
        return f"{collection_id}_geometries"

    source = SidecarBoundsSource(
        connection_factory=_connection_factory,
        schema_resolver=_resolve_schema,
        hub_table_for_collection=_hub_table,
        geometries_table_for_collection=_geometries_table,
        height_column=cfg.default_height_attr,
    )
    register_plugin(source)
    logger.info("SidecarBoundsSource registered against BoundsSourceProtocol")

    fetcher = SidecarGeometryFetcher(
        connection_factory=_connection_factory,
        schema_resolver=_resolve_schema,
        hub_table_for_collection=_hub_table,
        geometries_table_for_collection=_geometries_table,
        height_column=cfg.default_height_attr,
    )
    register_plugin(fetcher)
    logger.info("SidecarGeometryFetcher registered against GeometryFetcherProtocol")
