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

import logging
import os
import tempfile
from typing import Dict, List, Optional, Any

import morecantile

from dynastore.modules.db_config.query_executor import managed_transaction, DbResource
from dynastore.modules.concurrency import run_in_thread

from dynastore.modules.processes.protocols import ProcessTaskProtocol
from dynastore.modules.tasks.models import TaskPayload, TaskUpdate, TaskStatusEnum
from dynastore.modules.tasks import tasks_module
from dynastore.tools.discovery import get_protocol
from dynastore.models.protocols import ConfigsProtocol, CatalogsProtocol
from dynastore.modules.tiles.tiles_config import (
    TilesConfig,
    TilesPreseedConfig,
)
from dynastore.modules.tiles import tiles_module
from dynastore.modules.tiles import tiles_db
from dynastore.modules.tiles.tiles_module import TileStorageProtocol, TileArchiveStorageProtocol
from dynastore.modules.tiles.tms_definitions import BUILTIN_TILE_MATRIX_SETS
from dynastore.modules.tiles.writers.pmtiles_writer import TileEntry, write_pmtiles_from_entries, zxy_to_tileid
from dynastore.tools.geospatial import SimplificationAlgorithm
from .models import TilePreseedRequest
from .definition import TILES_PRESEED_PROCESS_DEFINITION
from dynastore.tools.protocol_helpers import get_engine
from dynastore.modules.processes.models import ExecuteRequest, Process, StatusInfo
from dynastore.models.driver_context import DriverContext

logger = logging.getLogger(__name__)


class TilePreseedTask(
    ProcessTaskProtocol[Process, TaskPayload[ExecuteRequest], Optional[StatusInfo]]
):
    """OGC Process to pre-seed tiles."""

    @staticmethod
    def get_definition() -> Process:
        return TILES_PRESEED_PROCESS_DEFINITION

    def __init__(self, app_state: Any = None):
        self.app_state = app_state
        self.engine = get_engine()

    async def run(self, payload: TaskPayload[ExecuteRequest]) -> Optional[StatusInfo]:
        # OGC Process dispatch wraps the body in an ExecuteRequest:
        # `payload.inputs.inputs` is the user-supplied dict (with `catalog_id`
        # + `collection_id` injected from the URL path by the dispatcher).
        # Validating against TilePreseedRequest at TaskPayload construction
        # would fail because Pydantic sees the wrapper dict (`inputs`,
        # `response`, etc.) at the top level, not the inner inputs dict.
        # Same unwrap pattern as DwhJoinExportTask and ExportFeaturesTask.
        request = TilePreseedRequest.model_validate(payload.inputs.inputs)
        config_manager = get_protocol(ConfigsProtocol)
        catalogs = get_protocol(CatalogsProtocol)
        if config_manager is None or catalogs is None or self.engine is None:
            raise RuntimeError("Required protocols/engine unavailable for tiles preseed task.")
        engine: DbResource = self.engine

        catalog_id = request.catalog_id
        schema = await catalogs.resolve_physical_schema(
            catalog_id, ctx=DriverContext(db_resource=engine)
        )
        if schema is None:
            raise RuntimeError(f"Cannot resolve physical schema for catalog {catalog_id!r}.")

        async with managed_transaction(engine) as conn:
            await tasks_module.ensure_task_storage_exists(conn, schema)

        if payload.task_id:
            await tasks_module.update_task(
                engine, payload.task_id,
                TaskUpdate(status=TaskStatusEnum.RUNNING, progress=0), schema=schema,
            )

        results: Dict[str, Any] = {"generated": 0, "skipped": 0, "errors": 0}

        try:
            preseed_config = await config_manager.get_config(
                TilesPreseedConfig, catalog_id, request.collection_id
            )
            if not isinstance(preseed_config, TilesPreseedConfig) or not preseed_config.enabled:
                logger.info(f"Pre-seeding disabled for {catalog_id}:{request.collection_id}")
                return None

            runtime_config = await config_manager.get_config(
                TilesConfig, catalog_id, request.collection_id
            )
            if not isinstance(runtime_config, TilesConfig):
                runtime_config = TilesConfig()

            target_collections = []
            if request.collection_id:
                target_collections = [request.collection_id]
            elif preseed_config.collections_to_preseed:
                target_collections = preseed_config.collections_to_preseed
            else:
                logger.warning(f"No target collections for {catalog_id}. Skipping.")
                return None

            effective_bboxes = (
                request.update_bbox or preseed_config.bboxes or runtime_config.bbox or [(-180, -90, 180, 90)]
            )
            target_tms_ids = request.tms_ids or preseed_config.target_tms_ids
            formats = request.formats or preseed_config.formats

            storage = get_protocol(TileStorageProtocol)
            if not storage:
                raise RuntimeError("No TileStorageProtocol registered — pre-seeding unavailable.")

            total_processed = 0

            for col_id in target_collections:
                meta = await tiles_module.get_tile_resolution_params(catalog_id, col_id)
                if not meta:
                    logger.error(f"Collection {catalog_id}:{col_id} metadata resolution failed.")
                    results["errors"] += 1
                    continue

                for tms_id in target_tms_ids:
                    tms_def = await tiles_module.get_custom_tms(catalog_id=catalog_id, tms_id=tms_id)
                    if not tms_def:
                        tms_def = BUILTIN_TILE_MATRIX_SETS.get(tms_id)
                        if not tms_def:
                            if morecantile is None:
                                logger.error(f"TMS {tms_id} not found and morecantile not installed.")
                                results["errors"] += 1
                                continue
                            try:
                                tms_def = morecantile.tms.get(tms_id)
                            except Exception:
                                logger.error(f"TMS {tms_id} not found.")
                                results["errors"] += 1
                                continue

                    if tms_def and not hasattr(tms_def, "tiles") and morecantile:
                        try:
                            tms_dict = tms_def.model_dump(exclude_none=True) if hasattr(tms_def, "model_dump") else tms_def
                            tms_def = morecantile.TileMatrixSet.model_validate(tms_dict)
                        except Exception as e:
                            logger.error(f"Failed to convert TMS {tms_id} to morecantile: {e}")
                            results["errors"] += 1
                            continue

                    target_srid = 3857
                    if hasattr(tms_def, "crs"):
                        try:
                            target_srid = await tiles_module.resolve_srid(engine, str(tms_def.crs), catalog_id)
                        except Exception as e:
                            logger.warning(f"Failed SRID resolution for {tms_def.crs}: {e}. Using 3857.")

                    mc_tms: Any = tms_def

                    if request.output_format == "pmtiles":
                        await self._preseed_pmtiles(
                            engine=engine, request=request, payload=payload,
                            catalog_id=catalog_id, col_id=col_id, tms_id=tms_id,
                            mc_tms=mc_tms, target_srid=target_srid,
                            effective_bboxes=effective_bboxes, runtime_config=runtime_config,
                            preseed_config=preseed_config, schema=schema, results=results,
                        )
                        total_processed += results.get("generated", 0) + results.get("skipped", 0)
                    else:
                        async with managed_transaction(engine) as conn:
                            for z in range(runtime_config.min_zoom, runtime_config.max_zoom + 1):
                                for bbox in effective_bboxes:
                                    try:
                                        tiles = mc_tms.tiles(*bbox, zooms=[z])
                                    except Exception as e:
                                        logger.error(f"Error generating tiles for bbox {bbox}: {e}")
                                        continue
                                    for tile in tiles:
                                        total_processed += 1
                                        for fmt in formats:
                                            try:
                                                simplification = (
                                                    preseed_config.simplification_by_zoom_override.get(z)
                                                    if preseed_config.simplification_by_zoom_override else None
                                                )
                                                data = await tiles_db.get_features_as_mvt_filtered(
                                                    conn=conn, resolved_collections=[meta],
                                                    tms_def=mc_tms, target_srid=target_srid,
                                                    z=str(z), x=tile.x, y=tile.y,
                                                    simplification=simplification,
                                                    simplification_algorithm=runtime_config.simplification_algorithm
                                                    or SimplificationAlgorithm.TOPOLOGY_PRESERVING,
                                                )
                                                if data:
                                                    await storage.save_tile(
                                                        catalog_id=catalog_id, collection_id=col_id,
                                                        tms_id=tms_id, z=z, x=tile.x, y=tile.y,
                                                        data=data, format=fmt,
                                                    )
                                                    results["generated"] += 1
                                                else:
                                                    results["skipped"] += 1
                                            except Exception as e:
                                                logger.error(f"Error generating tile {z}/{tile.x}/{tile.y}: {e}")
                                                results["errors"] += 1

                                        if total_processed % 100 == 0 and payload.task_id:
                                            new_outputs = {**results, "processed_tiles": total_processed}
                                            await tasks_module.update_task(
                                                engine, payload.task_id,
                                                TaskUpdate(outputs=new_outputs), schema=schema,
                                            )

            if payload.task_id:
                results["processed_tiles"] = total_processed
                await tasks_module.update_task(
                    engine, payload.task_id,
                    TaskUpdate(status=TaskStatusEnum.COMPLETED, progress=100, outputs=results),
                    schema=schema,
                )

            results["status"] = TaskStatusEnum.COMPLETED.value
            return None

        except Exception as e:
            logger.error(f"Pre-seed task failed: {e}", exc_info=True)
            if payload.task_id:
                await tasks_module.update_task(
                    engine, payload.task_id,
                    TaskUpdate(status=TaskStatusEnum.FAILED, error_message=str(e)),
                    schema=schema,
                )
            raise e

    async def _preseed_pmtiles(
        self, *, engine: DbResource, request: "TilePreseedRequest",
        payload: "TaskPayload[ExecuteRequest]", catalog_id: str, col_id: str,
        tms_id: str, mc_tms: Any, target_srid: int, effective_bboxes: List[Any],
        runtime_config: Any, preseed_config: Any, schema: str, results: Dict[str, Any],
    ) -> None:
        """Two-phase PMTiles archive generation (disk-backed, no in-memory tile accumulation)."""
        archive_storage = get_protocol(TileArchiveStorageProtocol)
        if archive_storage is None:
            raise RuntimeError("No TileArchiveStorageProtocol registered.")

        min_lon, min_lat, max_lon, max_lat = -180.0, -90.0, 180.0, 90.0
        if effective_bboxes:
            min_lon = min(b[0] for b in effective_bboxes)
            min_lat = min(b[1] for b in effective_bboxes)
            max_lon = max(b[2] for b in effective_bboxes)
            max_lat = max(b[3] for b in effective_bboxes)

        data_tmp_path: Optional[str] = None
        archive_tmp_path: Optional[str] = None
        try:
            with tempfile.NamedTemporaryFile(suffix=".tile-data", delete=False) as dtf:
                data_tmp_path = dtf.name
            with tempfile.NamedTemporaryFile(suffix=".pmtiles", delete=False) as atf:
                archive_tmp_path = atf.name

            tile_entries: List[TileEntry] = []
            data_offset = 0
            total_tiles = 0

            with open(data_tmp_path, "wb") as data_out:
                async with managed_transaction(engine) as conn:
                    for z in range(runtime_config.min_zoom, runtime_config.max_zoom + 1):
                        for bbox in effective_bboxes:
                            try:
                                tiles_iter = mc_tms.tiles(*bbox, zooms=[z])
                            except Exception as exc:
                                logger.error("Error generating tiles for bbox %s: %s", bbox, exc)
                                continue
                            for tile in tiles_iter:
                                total_tiles += 1
                                try:
                                    simplification = (
                                        preseed_config.simplification_by_zoom_override.get(z)
                                        if preseed_config.simplification_by_zoom_override else None
                                    )
                                    meta = await tiles_module.get_tile_resolution_params(catalog_id, col_id)
                                    mvt = await tiles_db.get_features_as_mvt_filtered(
                                        conn=conn, resolved_collections=[meta],
                                        tms_def=mc_tms, target_srid=target_srid,
                                        z=str(z), x=tile.x, y=tile.y,
                                        simplification=simplification,
                                        simplification_algorithm=runtime_config.simplification_algorithm
                                        or SimplificationAlgorithm.TOPOLOGY_PRESERVING,
                                    )
                                    if mvt:
                                        tile_id = zxy_to_tileid(z, tile.x, tile.y)
                                        tile_entries.append(TileEntry(tile_id=tile_id, offset=data_offset, length=len(mvt)))
                                        data_out.write(mvt)
                                        data_offset += len(mvt)
                                        results["generated"] = results.get("generated", 0) + 1
                                    else:
                                        results["skipped"] = results.get("skipped", 0) + 1
                                except Exception as exc:
                                    logger.error("Error tile %d/%d/%d: %s", z, tile.x, tile.y, exc)
                                    results["errors"] = results.get("errors", 0) + 1

                                if total_tiles % 100 == 0 and payload.task_id:
                                    new_outputs = {**results, "processed_tiles": total_tiles}
                                    await tasks_module.update_task(
                                        engine, payload.task_id,
                                        TaskUpdate(outputs=new_outputs), schema=schema,
                                    )

            sorted_entries = sorted(tile_entries, key=lambda e: e.tile_id)
            min_zoom = runtime_config.min_zoom
            max_zoom = runtime_config.max_zoom
            metadata: Dict[str, Any] = {"name": tms_id, "type": "vector"}
            _data_path = data_tmp_path
            _arch_path = archive_tmp_path

            def _build_archive() -> None:
                with open(_data_path, "rb") as data_src, open(_arch_path, "wb") as arch_out:
                    write_pmtiles_from_entries(
                        sorted_entries, data_src, arch_out,
                        metadata=metadata, min_zoom=min_zoom, max_zoom=max_zoom,
                        bbox=(min_lon, min_lat, max_lon, max_lat),
                    )

            await run_in_thread(_build_archive)
            logger.info("PMTiles archive built for %s/%s/%s (%d tiles).", catalog_id, col_id, tms_id, len(sorted_entries))

            with open(archive_tmp_path, "rb") as arch_file:
                uri = await archive_storage.save_archive(catalog_id, col_id, tms_id, arch_file)
            logger.info("PMTiles archive saved: %s", uri)
            results["archive_uri"] = uri
            results["archive_tiles"] = len(sorted_entries)

        finally:
            for path in (data_tmp_path, archive_tmp_path):
                if path:
                    try:
                        os.unlink(path)
                    except OSError:
                        pass
