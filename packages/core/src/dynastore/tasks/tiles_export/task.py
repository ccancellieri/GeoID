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

import io
import logging
import uuid
from typing import Any, Optional, Tuple, cast

import morecantile

from dynastore.models.driver_context import DriverContext
from dynastore.models.protocols import CatalogsProtocol, ConfigsProtocol
from dynastore.modules.db_config.query_executor import DDLQuery, DQLQuery, ResultHandler, managed_transaction
from dynastore.modules.processes.models import Process, StatusInfo
from dynastore.modules.processes.protocols import ProcessTaskProtocol
from dynastore.modules.tasks import tasks_module
from dynastore.modules.tasks.models import TaskPayload, TaskStatusEnum, TaskUpdate
from dynastore.modules.tiles import tiles_db, tiles_module
from dynastore.modules.tiles.tiles_config import TilesConfig
from dynastore.modules.tiles.tms_definitions import BUILTIN_TILE_MATRIX_SETS
from dynastore.modules.tiles.writers.pmtiles_writer import write_pmtiles
from dynastore.tools.discovery import get_protocol
from dynastore.tools.geospatial import SimplificationAlgorithm
from dynastore.tools.protocol_helpers import get_engine

from .definition import TILES_EXPORT_PROCESS_DEFINITION
from .models import TilesExportRequest

logger = logging.getLogger(__name__)

_DEFAULT_BBOX = (-180.0, -90.0, 180.0, 90.0)

_CREATE_TABLE_DDL = """
CREATE TABLE IF NOT EXISTS "{schema}".pmtiles_exports (
    id          text        PRIMARY KEY,
    collection_id text      NOT NULL,
    tms_id      text        NOT NULL,
    min_zoom    int         NOT NULL,
    max_zoom    int         NOT NULL,
    bbox        text,
    n_tiles     int,
    n_empty_tiles int,
    total_bytes bigint,
    data        bytea       NOT NULL,
    created_at  timestamptz NOT NULL DEFAULT now()
)
"""


async def _ensure_export_storage(conn, schema: str) -> None:
    await DDLQuery(_CREATE_TABLE_DDL.format(schema=schema)).execute(conn)


async def _save_export(
    conn,
    schema: str,
    *,
    collection_id: str,
    tms_id: str,
    min_zoom: int,
    max_zoom: int,
    bbox: Optional[Tuple[float, float, float, float]],
    n_tiles: int,
    n_empty_tiles: int,
    total_bytes: int,
    data: bytes,
) -> str:
    export_id = str(uuid.uuid4())
    bbox_str = ",".join(str(v) for v in bbox) if bbox else None
    sql = f"""
        INSERT INTO "{schema}".pmtiles_exports
            (id, collection_id, tms_id, min_zoom, max_zoom, bbox,
             n_tiles, n_empty_tiles, total_bytes, data)
        VALUES
            (:id, :collection_id, :tms_id, :min_zoom, :max_zoom, :bbox,
             :n_tiles, :n_empty_tiles, :total_bytes, :data)
    """
    await DQLQuery(sql, result_handler=ResultHandler.ROWCOUNT).execute(
        conn,
        id=export_id,
        collection_id=collection_id,
        tms_id=tms_id,
        min_zoom=min_zoom,
        max_zoom=max_zoom,
        bbox=bbox_str,
        n_tiles=n_tiles,
        n_empty_tiles=n_empty_tiles,
        total_bytes=total_bytes,
        data=data,
    )
    return export_id


class TilesExportTask(
    ProcessTaskProtocol[Process, TaskPayload[TilesExportRequest], Optional[StatusInfo]]
):
    """
    OGC Process that exports a collection's vector tiles as a PMTiles v3 archive.

    The archive is stored in a per-catalog ``pmtiles_exports`` table.  The
    job outputs include the ``export_id`` (UUID) needed to retrieve the file
    and the tile-count statistics.
    """

    @staticmethod
    def get_definition() -> Process:
        return TILES_EXPORT_PROCESS_DEFINITION

    def __init__(self, app_state: Any = None) -> None:
        self.app_state = app_state
        self.engine = get_engine()

    async def run(self, payload: TaskPayload[TilesExportRequest]) -> Optional[StatusInfo]:
        request = payload.inputs

        config_manager = get_protocol(ConfigsProtocol)
        catalogs = get_protocol(CatalogsProtocol)
        if config_manager is None or catalogs is None or self.engine is None:
            raise RuntimeError("Required protocols/engine unavailable for tiles_export task.")

        schema = await catalogs.resolve_physical_schema(
            request.catalog_id, ctx=DriverContext(db_resource=self.engine)
        )
        if schema is None:
            raise RuntimeError(
                f"Cannot resolve physical schema for catalog {request.catalog_id!r}."
            )

        async with managed_transaction(self.engine) as conn:
            await tasks_module.ensure_task_storage_exists(conn, schema)

        if payload.task_id:
            await tasks_module.update_task(
                self.engine,
                payload.task_id,
                TaskUpdate(status=TaskStatusEnum.RUNNING, progress=0),
                schema=schema,
            )

        try:
            runtime_config = await config_manager.get_config(
                TilesConfig, request.catalog_id, request.collection_id
            )
            if not isinstance(runtime_config, TilesConfig):
                runtime_config = TilesConfig()

            # Validate requested zoom range against collection config.
            min_zoom = max(request.min_zoom, runtime_config.min_zoom)
            max_zoom = min(request.max_zoom, runtime_config.max_zoom)
            if min_zoom > max_zoom:
                raise ValueError(
                    f"Requested zoom range [{request.min_zoom}, {request.max_zoom}] is outside "
                    f"the collection's allowed range [{runtime_config.min_zoom}, {runtime_config.max_zoom}]."
                )

            effective_bbox = request.bbox or _DEFAULT_BBOX

            # Resolve TMS.
            tms_def = await tiles_module.get_custom_tms(
                catalog_id=request.catalog_id, tms_id=request.tms_id
            )
            if not tms_def:
                tms_def = BUILTIN_TILE_MATRIX_SETS.get(request.tms_id)
            if not tms_def:
                try:
                    tms_def = morecantile.tms.get(request.tms_id)
                except Exception:
                    raise ValueError(f"TMS {request.tms_id!r} not found.")

            # Convert internal TileMatrixSet model to morecantile if needed.
            if not hasattr(tms_def, "tiles"):
                try:
                    tms_dict = (
                        tms_def.model_dump(exclude_none=True)
                        if hasattr(tms_def, "model_dump")
                        else tms_def
                    )
                    tms_def = morecantile.TileMatrixSet.model_validate(tms_dict)
                except Exception as exc:
                    raise ValueError(
                        f"Failed to convert TMS {request.tms_id!r} to morecantile: {exc}"
                    )
            # Both code paths above leave tms_def structurally compatible with
            # the local ``TileMatrixSet`` (same OGC ``tileMatrices`` shape) and
            # carrying morecantile's ``.tiles()`` / ``.crs`` API. Narrow for the
            # typechecker.
            tms_def = cast(morecantile.TileMatrixSet, tms_def)

            # Resolve collection metadata and SRID.
            meta = await tiles_module.get_tile_resolution_params(
                request.catalog_id, request.collection_id
            )
            if not meta:
                raise RuntimeError(
                    f"Cannot resolve tile metadata for "
                    f"{request.catalog_id}:{request.collection_id}."
                )

            target_srid = 3857
            if hasattr(tms_def, "crs"):
                try:
                    target_srid = await tiles_module.resolve_srid(
                        self.engine, str(tms_def.crs), request.catalog_id
                    )
                except Exception as exc:
                    logger.warning(
                        f"Failed to resolve SRID for CRS {tms_def.crs}: {exc}. "
                        f"Falling back to EPSG:3857."
                    )

            # Collect (z, x, y, data) tuples by iterating tiles asynchronously.
            pmtiles_buf = io.BytesIO()
            n_tiles_generated = 0
            n_tiles_empty = 0
            tile_data_pairs = []

            async with managed_transaction(self.engine) as conn:
                for z in range(min_zoom, max_zoom + 1):
                    try:
                        tile_iter = tms_def.tiles(*effective_bbox, zooms=[z])
                    except Exception as exc:
                        logger.warning(f"Cannot iterate tiles for zoom {z}: {exc}")
                        continue

                    for tile in tile_iter:
                        simplification = None
                        if runtime_config.simplification_by_zoom:
                            simplification = runtime_config.simplification_by_zoom.get(z)

                        data = await tiles_db.get_features_as_mvt_filtered(
                            conn=conn,
                            resolved_collections=[meta],
                            tms_def=tms_def,
                            target_srid=target_srid,
                            z=str(z),
                            x=tile.x,
                            y=tile.y,
                            simplification=simplification,
                            simplification_algorithm=(
                                runtime_config.simplification_algorithm
                                or SimplificationAlgorithm.TOPOLOGY_PRESERVING
                            ),
                        )

                        if data:
                            tile_data_pairs.append((z, tile.x, tile.y, data))
                            n_tiles_generated += 1
                        else:
                            n_tiles_empty += 1

                    # Progress update every zoom level.
                    if payload.task_id:
                        progress = int(
                            ((z - min_zoom + 1) / (max_zoom - min_zoom + 1)) * 80
                        )
                        await tasks_module.update_task(
                            self.engine,
                            payload.task_id,
                            TaskUpdate(
                                progress=progress,
                                outputs={
                                    "n_tiles": n_tiles_generated,
                                    "n_empty_tiles": n_tiles_empty,
                                    "current_zoom": z,
                                },
                            ),
                            schema=schema,
                        )

            # Write PMTiles archive.
            stats = write_pmtiles(
                tile_data_pairs,
                pmtiles_buf,
                metadata={
                    "name": request.collection_id,
                    "format": "pbf",
                    "minzoom": min_zoom,
                    "maxzoom": max_zoom,
                },
                min_zoom=min_zoom,
                max_zoom=max_zoom,
                bbox=effective_bbox,
            )
            pmtiles_bytes = pmtiles_buf.getvalue()

            # Persist to catalog storage.
            async with managed_transaction(self.engine) as conn:
                await _ensure_export_storage(conn, schema)
                export_id = await _save_export(
                    conn,
                    schema,
                    collection_id=request.collection_id,
                    tms_id=request.tms_id,
                    min_zoom=min_zoom,
                    max_zoom=max_zoom,
                    bbox=request.bbox,
                    n_tiles=stats["n_tiles"],
                    n_empty_tiles=stats["n_empty_tiles"],
                    total_bytes=stats["total_bytes"],
                    data=pmtiles_bytes,
                )

            outputs = {
                "export_id": export_id,
                "n_tiles": stats["n_tiles"],
                "n_empty_tiles": stats["n_empty_tiles"],
                "total_bytes": stats["total_bytes"],
                "tms_id": request.tms_id,
                "min_zoom": min_zoom,
                "max_zoom": max_zoom,
            }

            if payload.task_id:
                await tasks_module.update_task(
                    self.engine,
                    payload.task_id,
                    TaskUpdate(
                        status=TaskStatusEnum.COMPLETED, progress=100, outputs=outputs
                    ),
                    schema=schema,
                )

            return None

        except Exception as exc:
            logger.error(f"tiles_export task failed: {exc}", exc_info=True)
            if payload.task_id:
                await tasks_module.update_task(
                    self.engine,
                    payload.task_id,
                    TaskUpdate(
                        status=TaskStatusEnum.FAILED, error_message=str(exc)
                    ),
                    schema=schema,
                )
            raise
