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

import logging
import os
import tempfile
from typing import Dict, List, Optional, Tuple, Any

import morecantile  # noqa: F401 — hard-import gate: see tests/dynastore/tasks/unit/test_heavy_task_hard_imports.py

from dynastore.modules.db_config.query_executor import (
    managed_transaction,
    DbResource,
    DQLQuery,
    ResultHandler,
)
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
from dynastore.modules.tiles import tiles_engine
from dynastore.modules.tiles.tiles_module import TileArchiveStorageProtocol
from dynastore.modules.tiles.writers.pmtiles_writer import TileEntry, write_pmtiles_from_entries, zxy_to_tileid
from dynastore.tools.geospatial import SimplificationAlgorithm
from .models import TilePreseedRequest
from .definition import TILES_PRESEED_PROCESS_DEFINITION
from dynastore.tools.protocol_helpers import get_engine
from dynastore.modules.processes.models import ExecuteRequest, Process, StatusInfo
from dynastore.models.driver_context import DriverContext

logger = logging.getLogger(__name__)

# Bounded default scope (#2813): the world bbox at an unbounded zoom range
# can fan out to an enormous tile count. Applied only when no bbox is
# configured anywhere (request/preseed/runtime) — an operator-supplied bbox
# is assumed intentional at whatever zoom range they configured.
PRESEED_DEFAULT_MAX_ZOOM = 8

BoundingBox = Tuple[float, float, float, float]


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

        # `schema` is the catalog's physical PG schema — used as the
        # `schema_name` tenant discriminator on `tasks.tasks`. It is NOT a
        # second physical home for the tasks table: tasks live globally in
        # `get_task_schema()`.tasks (see `tasks_module.GLOBAL_TASKS_TABLE_DDL`),
        # provisioned once at `TasksModule.lifespan`.

        if payload.task_id:
            await tasks_module.update_task(
                engine, payload.task_id,
                TaskUpdate(status=TaskStatusEnum.RUNNING, progress=0), schema=schema,
            )

        # seed / renew: render MVT/PMTiles and save tiles. The light
        # cache-invalidation (delete-only) path was split into its own
        # 'tiles_invalidate' task type routed to the in-process background
        # runner on the catalog service.
        results: Dict[str, Any] = {"generated": 0, "skipped": 0, "errors": 0}

        try:
            preseed_config = await config_manager.get_config(
                TilesPreseedConfig, catalog_id, request.collection_id
            )
            if not isinstance(preseed_config, TilesPreseedConfig) or not preseed_config.preseed_enabled:
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

            explicit_bboxes = request.update_bbox or preseed_config.bboxes or runtime_config.bbox
            target_tms_ids = request.tms_ids or preseed_config.target_tms_ids
            formats = request.formats or preseed_config.formats
            statement_timeout_ms = preseed_config.preseed_tile_timeout_seconds * 1000

            from dynastore.modules.tiles.tiles_module import TileStorageProtocol

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

                if explicit_bboxes:
                    effective_bboxes: List[BoundingBox] = explicit_bboxes
                    effective_max_zoom = runtime_config.max_zoom
                else:
                    effective_bboxes, effective_max_zoom = await _bounded_default_scope(
                        catalogs, catalog_id, col_id, preseed_config, runtime_config.max_zoom,
                    )
                logger.info(
                    "tiles_preseed: bounded scope catalog=%s collection=%s bbox=%s "
                    "zoom=[%d,%d]",
                    catalog_id, col_id, effective_bboxes, runtime_config.min_zoom, effective_max_zoom,
                )

                for tms_id in target_tms_ids:
                    ctx = await tiles_engine.build_render_context(
                        catalog_id, [col_id], tms_id, engine=engine, morecantile_compatible=True,
                    )
                    if ctx is None:
                        logger.error(
                            "tiles_preseed: render-context resolution failed for %s/%s/%s.",
                            catalog_id, col_id, tms_id,
                        )
                        results["errors"] += 1
                        continue
                    mc_tms: Any = ctx.tms_def
                    target_srid = ctx.target_srid

                    if request.output_format == "pmtiles":
                        await self._preseed_pmtiles(
                            engine=engine, request=request, payload=payload,
                            catalog_id=catalog_id, col_id=col_id, tms_id=tms_id,
                            ctx=ctx, mc_tms=mc_tms, target_srid=target_srid,
                            effective_bboxes=effective_bboxes, effective_max_zoom=effective_max_zoom,
                            runtime_config=runtime_config,
                            preseed_config=preseed_config, schema=schema, results=results,
                        )
                        total_processed += results.get("generated", 0) + results.get("skipped", 0)
                    else:
                        total_processed += await self._preseed_mvt(
                            engine=engine, payload=payload, ctx=ctx, storage=storage,
                            catalog_id=catalog_id,
                            col_id=col_id, tms_id=tms_id, mc_tms=mc_tms,
                            effective_bboxes=effective_bboxes, effective_max_zoom=effective_max_zoom,
                            runtime_config=runtime_config, preseed_config=preseed_config,
                            formats=formats, statement_timeout_ms=statement_timeout_ms,
                            schema=schema, results=results, total_processed_so_far=total_processed,
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

    async def _preseed_mvt(
        self, *, engine: DbResource, payload: "TaskPayload[ExecuteRequest]",
        ctx: Any, storage: Any, catalog_id: str, col_id: str, tms_id: str, mc_tms: Any,
        effective_bboxes: List[BoundingBox], effective_max_zoom: int,
        runtime_config: Any, preseed_config: Any, formats: List[str],
        statement_timeout_ms: int, schema: str, results: Dict[str, Any],
        total_processed_so_far: int,
    ) -> int:
        """Per-zoom-transaction MVT preseed loop (#2813).

        Replaces the single giant transaction spanning the whole preseed run:
        a per-zoom transaction frees the pool between zooms (the checkpoint-
        visibility fix — the old outer transaction held a pooled connection
        for the entire run, so `update_task`'s own connection checkout could
        starve on a small pool and progress never landed) and bounds the
        blast radius of a per-tile statement timeout to at most the
        remainder of one zoom's tiles: a canceled statement leaves the PG
        transaction aborted, so the rest of that zoom is skipped and picked
        up by the next preseed run rather than attempting mid-transaction
        recovery. Already-committed zooms are unaffected.
        """
        total_processed = total_processed_so_far
        zoom_span = max(1, effective_max_zoom - runtime_config.min_zoom + 1)

        for z in range(runtime_config.min_zoom, effective_max_zoom + 1):
            progress = min(99, int(100 * (z - runtime_config.min_zoom) / zoom_span))
            try:
                async with managed_transaction(engine) as conn:
                    await DQLQuery(
                        f"SET LOCAL statement_timeout = {statement_timeout_ms}",
                        result_handler=ResultHandler.NONE,
                    ).execute(conn)

                    for bbox in effective_bboxes:
                        try:
                            tiles = mc_tms.tiles(*bbox, zooms=[z])
                        except Exception as e:
                            logger.error(f"Error generating tiles for bbox {bbox}: {e}")
                            continue
                        for tile in tiles:
                            total_processed += 1
                            for fmt in formats:
                                simplification = (
                                    preseed_config.simplification_by_zoom_override.get(z)
                                    if preseed_config.simplification_by_zoom_override else None
                                )
                                data = await tiles_engine.render_tile(
                                    conn, ctx, str(z), tile.x, tile.y,
                                    format=fmt, use_l1_cache=False,
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

                            if total_processed % 100 == 0 and payload.task_id:
                                new_outputs = {**results, "processed_tiles": total_processed}
                                await tasks_module.update_task(
                                    engine, payload.task_id,
                                    TaskUpdate(outputs=new_outputs, progress=progress), schema=schema,
                                )
            except Exception as exc:
                # A canceled statement (or any other SQL error) leaves this
                # zoom's transaction aborted — nothing more can be committed
                # on it. Count it and move on to the next zoom; already-
                # committed zooms are unaffected, and a subsequent preseed
                # run picks up whatever this zoom didn't finish.
                logger.error(
                    "tiles_preseed: zoom %d aborted for %s/%s/%s (%s); "
                    "remaining tiles at this zoom will be retried on the next run.",
                    z, catalog_id, col_id, tms_id, exc,
                )
                results["errors"] += 1

        if payload.task_id:
            new_outputs = {**results, "processed_tiles": total_processed}
            await tasks_module.update_task(
                engine, payload.task_id, TaskUpdate(outputs=new_outputs, progress=99), schema=schema,
            )
        return total_processed

    async def _preseed_pmtiles(
        self, *, engine: DbResource, request: "TilePreseedRequest",
        payload: "TaskPayload[ExecuteRequest]", catalog_id: str, col_id: str,
        tms_id: str, ctx: Any, mc_tms: Any, target_srid: int,
        effective_bboxes: List[BoundingBox], effective_max_zoom: int,
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
                    for z in range(runtime_config.min_zoom, effective_max_zoom + 1):
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
                                    mvt = await tiles_engine.render_tile(
                                        conn, ctx, str(z), tile.x, tile.y,
                                        format="mvt", use_l1_cache=False,
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
            max_zoom = effective_max_zoom
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


async def _bounded_default_scope(
    catalogs: CatalogsProtocol,
    catalog_id: str,
    col_id: str,
    preseed_config: TilesPreseedConfig,
    runtime_max_zoom: int,
) -> Tuple[List[BoundingBox], int]:
    """Bounded default scope (#2813).

    When no bbox is configured anywhere (request/preseed/runtime), the world
    bbox at an unbounded zoom range can fan out to an enormous tile count.
    Derives the default bbox from the collection's STAC spatial extent
    instead of the world, and caps the zoom range to
    ``preseed_config.preseed_max_zoom`` (default ``PRESEED_DEFAULT_MAX_ZOOM``).
    Falls back to the world bbox if the collection's extent can't be resolved
    (never crashes the preseed on a metadata miss).
    """
    bbox: List[BoundingBox] = [(-180.0, -90.0, 180.0, 90.0)]
    try:
        collection = await catalogs.get_collection(catalog_id, col_id)
        spatial = getattr(getattr(collection, "extent", None), "spatial", None)
        raw_bbox = getattr(spatial, "bbox", None) if spatial else None
        if raw_bbox:
            first = raw_bbox[0]
            if first and len(first) >= 4 and any(v != 0.0 for v in first):
                bbox = [(float(first[0]), float(first[1]), float(first[2]), float(first[3]))]
    except Exception as exc:
        logger.warning(
            "tiles_preseed: could not resolve STAC spatial extent for %s/%s (%s); "
            "using world bbox as the default scope.",
            catalog_id, col_id, exc,
        )
    effective_max_zoom = min(runtime_max_zoom, preseed_config.preseed_max_zoom or PRESEED_DEFAULT_MAX_ZOOM)
    return bbox, effective_max_zoom
