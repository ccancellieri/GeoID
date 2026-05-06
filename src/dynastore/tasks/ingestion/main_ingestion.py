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
#
#    Author: Carlo Cancellieri (ccancellieri@gmail.com)
#    Company: FAO, Viale delle Terme di Caracalla, 00100 Rome, Italy
#    Contact: copyright@fao.org - http://fao.org/contact-us/terms/en/

import logging
import re
import os
import asyncio
import itertools
from typing import Optional

from dynastore.modules.catalog.asset_service import Asset, AssetCreate, VirtualAssetCreate
from dynastore.modules.catalog.models import CoreAssetReferenceType

from dynastore.modules.catalog.tools import recalculate_and_update_extents
from dynastore.modules.db_config.query_executor import DbEngine
from dynastore.models.driver_context import DriverContext

# Import Ingestion Configuration
from dynastore.tasks.ingestion.ingestion_config import IngestionPluginConfig
from dynastore.tasks.ingestion.ingestion_models import TaskIngestionRequest
from .reporters import _ingestion_reporter_registry
from dynastore.tasks.tools import initialize_reporters
from .operations import initialize_operations, run_pre_operations, run_post_operations

logger = logging.getLogger(__name__)


# Top-level keys yielded by readers (e.g. GdalOsgeoReader) that are GeoJSON
# envelope markers or reader-internal geometry slots — never publisher DBF
# columns. Excluded from the merge into feature["properties"] so they don't
# pollute the attributes JSONB sidecar. Source columns with the same names
# (if any) reach properties through the inner properties dict, not the
# top-level merge, so this is safe.
_STRUCTURAL_RAW_KEYS = frozenset({
    "geometry", "properties", "id",
    "type",                          # GeoJSON Feature envelope marker
    "geometry_wkb", "geometry_wkt",  # reader-internal geometry slots
})


async def _post_ingest_analyze(engine: "DbEngine", schema: str) -> None:
    """Run ``ANALYZE "<schema>"`` after a successful ingest.

    PR-D: Cheap planner-stat refresh on the catalog's tenant schema once
    bulk ingest completes. PostgreSQL autovacuum will pick up the change
    eventually (autovacuum_analyze_threshold = 50 + 10% of table size by
    default), but explicit ANALYZE narrows the window where queries run
    against stale stats — important for collections whose first usage is
    immediately after the initial ingest.

    Best-effort: failures are logged and swallowed so a stats hiccup
    never demotes a successful ingest to FAILED. PG-only (no equivalent
    on Iceberg/DuckDB; their stats are computed on-write). Handles both
    sync and async engines — ingestion's caller may pass either.
    """
    try:
        from sqlalchemy import text
        from sqlalchemy.ext.asyncio import AsyncEngine
        # Schema name comes from resolve_physical_schema(catalog_id) which
        # returns a validated identifier; safe to interpolate. ANALYZE
        # accepts neither parameters nor schema-search-path-qualified
        # bind params, so a quoted-identifier literal is the only option.
        stmt = text(f'ANALYZE "{schema}"')
        if isinstance(engine, AsyncEngine):
            async with engine.begin() as conn:
                await conn.execute(stmt)
        else:
            with engine.begin() as conn:
                conn.execute(stmt)
        logger.info("Post-ingest ANALYZE completed for schema %r.", schema)
    except Exception as exc:
        logger.warning(
            "Post-ingest ANALYZE failed for schema %r: %s — autovacuum "
            "will pick up the stats refresh on its next cycle.",
            schema, exc,
        )


def _resolve_source_content_type(asset: Asset) -> Optional[str]:
    """Best-effort MIME-type lookup used by reader resolution.

    1. ``asset.metadata['content_type']`` — populated by
       :func:`BucketService._prepare_blob_metadata` for every new GCS
       upload, so the happy path is one in-memory dict read.
    2. For legacy assets whose metadata pre-dates the injection (or
       non-GCS uploads), do a single ``storage.objects.get`` to read
       the blob's native ``contentType`` header.  Only attempted for
       ``gs://`` URIs and behind a try/except so a bad path / missing
       creds never blocks the task.
    """
    md = asset.metadata or {}
    ct = md.get("content_type") or md.get("contentType")
    if ct:
        return ct
    source = asset.uri or asset.href
    if not source or not source.startswith("gs://"):
        return None
    try:
        from google.cloud import storage

        bucket_name, _, object_name = source[len("gs://"):].partition("/")
        if not bucket_name or not object_name:
            return None
        client = storage.Client()
        blob = client.bucket(bucket_name).get_blob(object_name)
        if blob is None:
            return None
        return blob.content_type
    except Exception as exc:  # noqa: BLE001
        logger.warning(
            "ingestion: GCS HEAD for %r failed (%s); reader resolution will "
            "rely on URI suffix only.", asset.uri, exc,
        )
        return None


async def run_ingestion_task(
    engine: DbEngine,
    task_id: str,
    catalog_id: str,
    collection_id: str,
    task_request: TaskIngestionRequest,
    caller_id: Optional[str] = None,
):
    from dynastore.tools.discovery import get_protocol
    from dynastore.models.protocols import CatalogsProtocol

    catalog_module = get_protocol(CatalogsProtocol)
    if not catalog_module:
        raise RuntimeError("CatalogsProtocol implementation not found.")

    if task_request.asset is None:
        raise ValueError("task_request.asset is required for ingestion.")
    req_asset = task_request.asset

    pre_ops = []
    post_ops = []

    logger.info(
        f"Starting Ingestion Task '{task_id}' for collection '{catalog_id}:{collection_id}'. Source SRID: {task_request.source_srid}"
    )

    # Resolve physical schema for task storage
    phys_schema = await catalog_module.resolve_physical_schema(
        catalog_id, ctx=DriverContext(db_resource=engine) if engine else None
    )
    if phys_schema is None:
        raise RuntimeError(f"Cannot resolve physical schema for catalog {catalog_id!r}.")

    reporters = initialize_reporters(
        engine,
        task_id,
        task_request,
        task_request.reporting,
        registry=_ingestion_reporter_registry,
        schema=phys_schema,
        catalog_id=catalog_id,
        collection_id=collection_id,
    )

    await asyncio.gather(
        *(
            reporter.task_started(
                task_id,
                collection_id,
                catalog_id,
                req_asset.asset_id or req_asset.uri or "",
            )
            for reporter in reporters
        )
    )

    # --- Ensure Logical Collection Exists ---
    await catalog_module.ensure_collection_exists(
        catalog_id, collection_id, lang=task_request.lang, ctx=DriverContext(db_resource=engine)
    )

    logger.info(f"Task '{task_id}': Beginning main ingestion process.")
    try:
        # --- Fetch Physical Configuration (Immutable Storage) ---
        catalog_config = await catalog_module.get_collection_config(
            catalog_id, collection_id
        )

        # --- Fetch Ingestion Configuration (Mutable Logic) ---
        ingestion_config = await catalog_module.configs.get_config(
            IngestionPluginConfig, catalog_id, collection_id
        )

        # Initialize Operations
        pre_ops = initialize_operations(
            engine,
            task_id,
            task_request,
            task_request.pre_operations,
            catalog_config=catalog_config,
            ingestion_config=ingestion_config,
        )
        post_ops = initialize_operations(
            engine,
            task_id,
            task_request,
            task_request.post_operations,
            catalog_config=catalog_config,
            ingestion_config=ingestion_config,
        )

        # --- Ensure Storage Exists (all write drivers) ---
        from dynastore.modules.storage.router import get_write_drivers
        write_drivers = await get_write_drivers(catalog_id, collection_id)
        for resolved in write_drivers:
            await resolved.driver.ensure_storage(
                catalog_id, collection_id,
                col_config=catalog_config, db_resource=engine,
            )

        asset_manager = catalog_module.assets

        # --- Resolve or Create the Asset ---
        asset: Optional[Asset] = None
        if req_asset.asset_id:
            asset = await asset_manager.get_asset(
                catalog_id, req_asset.asset_id, collection_id
            )
            if not asset and not req_asset.uri:
                raise ValueError(
                    f"Asset with asset_id '{req_asset.asset_id}' not found and no URI provided."
                )

        if not asset and req_asset.uri:
            logger.info(f"Creating asset from URI: {req_asset.uri}")
            asset_id_for_creation = req_asset.asset_id or re.sub(
                r"[^a-zA-Z0-9_\-]", "_", os.path.basename(req_asset.uri)
            )

            # External-source ingestion: register as a virtual asset since we
            # don't manage the source bytes (the file lives in the caller's
            # storage). Stage 4 will replace this with the policy-driven
            # variant; today we keep the `Asset.uri` field populated by
            # storing the raw URI as the virtual `href`.
            asset_payload = VirtualAssetCreate(
                asset_id=asset_id_for_creation,
                href=req_asset.uri,
                metadata=req_asset.metadata or {},
            )
            asset = await asset_manager.create_asset(
                catalog_id, asset_payload, collection_id, ctx=DriverContext(db_resource=engine)
            )

        if not asset:
            raise ValueError("Could not find or create an asset.")

        # --- Run Pre-Operations ---
        if pre_ops:
            catalog = await catalog_module.get_catalog(catalog_id, ctx=DriverContext(db_resource=engine))
            collection = await catalog_module.get_collection(
                catalog_id, collection_id, ctx=DriverContext(db_resource=engine)
            )
            asset = await run_pre_operations(pre_ops, catalog, collection, asset)
            if asset is None:
                raise RuntimeError("Pre-operations returned no asset.")

        source_file_path = asset.uri or asset.href
        if source_file_path is None:
            raise RuntimeError(
                f"Asset {asset.asset_id} has no source URI: kind={asset.kind} status={asset.status}"
            )
        asset_id = asset.asset_id
        # MIME hint used by reader resolution when the URI itself carries
        # no recognisable suffix (legacy bare-filename uploads).  Source
        # of truth: the asset row's metadata; falls back to a single GCS
        # object HEAD for legacy rows whose metadata pre-dates the
        # ``content_type`` injection in ``_prepare_blob_metadata``.
        source_content_type = _resolve_source_content_type(asset)

        # --- Process and Ingest Features ---
        total_features = None
        try:
            from dynastore.tasks.ingestion.readers import resolve_reader

            _count_reader = resolve_reader(
                source_file_path, content_type=source_content_type,
            )()
            total_features = _count_reader.feature_count(
                source_file_path, content_type=source_content_type,
            )
            if total_features is not None:
                logger.info(f"Source file contains {total_features} features.")
                await asyncio.gather(
                    *(reporter.update_progress(0, total_features) for reporter in reporters)
                )
        except Exception as e:
            logger.warning(f"Could not determine total feature count: {e}")

        batch_size = task_request.database_batch_size or 500
        current_batch = []
        rows_ingested = 0

        upsert_context = {"asset_id": asset_id}

        def prepare_record_for_upsert(raw: dict, request: TaskIngestionRequest) -> dict:
            mapping = request.column_mapping
            feature = {"properties": {}}

            def _get_raw_val(key):
                if not key:
                    return None
                return (
                    raw.get(key) if key in raw else raw.get("properties", {}).get(key)
                )

            # 1. Identity
            ext_id_field = mapping.external_id or "id"
            ext_id = _get_raw_val(ext_id_field)
            if ext_id:
                feature["id"] = ext_id

            # 2. Geometry
            geometry = None
            if mapping.csv_lat_column and mapping.csv_lon_column:
                lat = _get_raw_val(mapping.csv_lat_column)
                lon = _get_raw_val(mapping.csv_lon_column)
                if lat is not None and lon is not None:
                    coords = [float(lon), float(lat)]
                    elev = _get_raw_val(mapping.csv_elevation_column)
                    if elev is not None:
                        coords.append(float(elev))
                    else:
                        if mapping.csv_elevation_column:
                            logger.warning(
                                f"Elevation column '{mapping.csv_elevation_column}' specified but not found/empty in record: {raw.keys()} props: {raw.get('properties', {}).keys()}"
                            )
                    geometry = {"type": "Point", "coordinates": coords}
            elif mapping.csv_wkt_column:
                wkt = _get_raw_val(mapping.csv_wkt_column)
                if wkt:
                    from shapely.wkt import loads
                    from shapely.geometry import mapping as shapely_mapping

                    try:
                        geometry = shapely_mapping(loads(wkt))
                    except Exception:
                        pass
            elif mapping.geometry_wkb:
                wkb = _get_raw_val(mapping.geometry_wkb)
                if wkb:
                    if isinstance(wkb, dict):
                        # GDAL/OGR already decoded the geometry to GeoJSON dict.
                        geometry = wkb
                    else:
                        from shapely.wkb import loads
                        from shapely.geometry import mapping as shapely_mapping

                        try:
                            geometry = shapely_mapping(loads(wkb))
                        except Exception:
                            pass
                # If geometry_wkb column produced nothing, fall through to the
                # standard ``geometry`` key (e.g. GDAL stores it there).
                if geometry is None:
                    geometry = raw.get("geometry")
            else:
                # Fallback: check if 'geometry' is already present (e.g. GeoJSON/Fiona)
                geometry = raw.get("geometry")

            if geometry:
                feature["geometry"] = geometry

            # 3. Attributes
            raw_props = raw.get("properties", {})
            if (
                mapping.attributes_source_type == "explicit_list"
                and mapping.attribute_mapping
            ):
                for item in mapping.attribute_mapping:
                    if item.constant is not None:
                        val = item.constant
                    else:
                        val = _get_raw_val(item.source)
                    
                    if val is not None:
                        feature["properties"][item.map_to] = val
            else:
                reserved = {
                    mapping.external_id,
                    mapping.csv_lat_column,
                    mapping.csv_lon_column,
                    mapping.csv_elevation_column,
                    mapping.csv_wkt_column,
                    mapping.geometry_wkb,
                }
                # Take from properties first
                for k, v in raw_props.items():
                    if k not in reserved:
                        feature["properties"][k] = v
                # Also take from top-level if not already taken and not reserved.
                # _STRUCTURAL_RAW_KEYS covers the GeoJSON envelope marker and the
                # reader's internal geometry slots — they're never DBF columns
                # and were leaking into the attributes JSONB when the user's
                # ColumnMappingConfig left mapping.geometry_wkb unset.
                for k, v in raw.items():
                    if (
                        k not in _STRUCTURAL_RAW_KEYS
                        and k not in reserved
                        and k not in feature["properties"]
                    ):
                        feature["properties"][k] = v

            # 4. Temporal
            valid_from = _get_raw_val(request.time_validity_start_column)
            if valid_from:
                feature["valid_from"] = valid_from
            valid_to = _get_raw_val(request.time_validity_end_column)
            if valid_to:
                feature["valid_to"] = valid_to

            return feature

        # Pluggable source reader.  ``ReaderRegistry.resolve`` picks the
        # highest-priority reader whose ``can_read(uri)`` matches —
        # GdalOsgeoReader (system libgdal, supports Parquet/FGB/SHP/CSV/…)
        # then FionaReader as a tail fallback.  Solves the
        # ``CPLE_OpenFailedError: not recognized as being in a supported
        # file format`` blocker when fiona's bundled libgdal lacks the
        # Arrow/Parquet driver.
        from dynastore.tasks.ingestion.readers import resolve_reader

        reader_cls = resolve_reader(
            source_file_path, content_type=source_content_type,
        )
        reader_inst = reader_cls()
        logger.info(
            "ingestion: source %r (content_type=%r) → reader '%s'",
            source_file_path, source_content_type,
            reader_cls.reader_id or reader_cls.__name__,
        )

        with reader_inst.open(
            source_file_path,
            encoding=task_request.encoding,
            content_type=source_content_type,
        ) as reader:
            sliced_reader = itertools.islice(
                reader,
                task_request.offset,
                task_request.limit + task_request.offset
                if task_request.limit
                else None,
            )

            for idx, raw_record in enumerate(sliced_reader, start=task_request.offset):
                feature = prepare_record_for_upsert(dict(raw_record), task_request)
                current_batch.append(feature)

                if len(current_batch) >= batch_size:
                    await catalog_module.upsert(
                        catalog_id,
                        collection_id,
                        current_batch,
                        ctx=DriverContext(db_resource=engine),
                        processing_context=upsert_context,
                    )
                    rows_ingested += len(current_batch)
                    await asyncio.gather(
                        *(
                            reporter.update_progress(rows_ingested, total_features)
                            for reporter in reporters
                        )
                    )
                    current_batch = []

            if current_batch:
                await catalog_module.upsert(
                    catalog_id,
                    collection_id,
                    current_batch,
                    ctx=DriverContext(db_resource=engine),
                    processing_context=upsert_context,
                )
                rows_ingested += len(current_batch)
                await asyncio.gather(
                    *(
                        reporter.update_progress(rows_ingested, total_features)
                        for reporter in reporters
                    )
                )

        await recalculate_and_update_extents(engine, catalog_id, collection_id)

        # Register an informational reference: this asset feeds collection_id.
        # cascade_delete=True because the DB trigger (trg_asset_cleanup) already
        # cascades row-level cleanup when the asset is deleted — this reference is
        # purely for discoverability and audit, not for blocking deletion.
        try:
            await asset_manager.add_asset_reference(
                asset_id=asset.asset_id,
                catalog_id=catalog_id,
                ref_type=CoreAssetReferenceType.COLLECTION,
                ref_id=collection_id,
                cascade_delete=True,
                ctx=DriverContext(db_resource=engine),
            )
        except Exception as ref_err:
            logger.warning(
                f"Task '{task_id}': Could not register asset reference "
                f"({asset.asset_id} → {collection_id}): {ref_err}"
            )

        await asyncio.gather(
            *(reporter.task_finished("COMPLETED") for reporter in reporters)
        )

        # PR-D: refresh planner stats on the tenant schema so subsequent
        # queries against the just-loaded data don't run with stale
        # estimates. Best-effort; see ``_post_ingest_analyze`` docstring.
        if engine is not None:
            await _post_ingest_analyze(engine, phys_schema)

        # --- Run Post-Operations ---
        if post_ops:
            catalog = await catalog_module.get_catalog(catalog_id, ctx=DriverContext(db_resource=engine))
            collection = await catalog_module.get_collection(
                catalog_id, collection_id, ctx=DriverContext(db_resource=engine)
            )
            await run_post_operations(post_ops, catalog, collection, asset, "COMPLETED")

    except Exception as e:
        logger.critical(f"Ingestion task {task_id} failed: {e}", exc_info=True)
        await asyncio.gather(
            *(
                reporter.task_finished("FAILED", error_message=str(e))
                for reporter in reporters
            )
        )
        if post_ops:
            try:
                catalog = await catalog_module.get_catalog(
                    catalog_id, ctx=DriverContext(db_resource=engine)
                )
                collection = await catalog_module.get_collection(
                    catalog_id, collection_id, ctx=DriverContext(db_resource=engine)
                )
                await run_post_operations(
                    post_ops, catalog, collection, asset, "FAILED", error_message=str(e)
                )
            except Exception:
                pass

        # Re-raise the exception to ensure the caller (and tests) know the task failed.
        # The database status has already been updated to FAILED above.
        raise e
