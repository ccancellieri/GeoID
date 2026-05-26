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
from datetime import datetime
from typing import Optional

from dateutil import parser as _dateutil_parser

from dynastore.modules.catalog.asset_service import Asset, VirtualAssetCreate
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


# Canonical items-schema data types that denote a temporal value. A property
# declared as one of these is coerced from common string representations to a
# canonical ISO-8601 string during ingestion — see ``apply_temporal_coercion``.
_TEMPORAL_DATA_TYPES = frozenset({"date", "time", "timestamp"})


def _coerce_temporal_value(value, data_type: str, parse_format: Optional[str] = None):
    """Best-effort coercion of a single value to canonical ISO-8601.

    The typed write path already accepts ISO-8601 strings for ``date`` /
    ``time`` / ``timestamp`` columns, but not arbitrary formats (e.g.
    ``31/12/2024`` or ``Jan 31 2024``). This normalises a parseable string to
    the ISO form the write path accepts.

    ``parse_format`` is the field's optional ``strptime`` hint (#1350). When
    set it is tried first, so ambiguous numeric formats are read exactly as the
    operator declared (``%d/%m/%Y`` reads ``01/02/2024`` as 1 Feb, not the
    month-first 2 Jan that ``dateutil`` auto-detection would pick). A value that
    does not match the explicit pattern is returned unchanged — it then falls
    through to the typed write, which rejects just that row via the 207
    IngestionReport rather than silently month-first-guessing against the
    operator's stated intent. With no hint, parsing falls back to
    ``dateutil`` auto-detection (the #1333 behaviour).

    Only strings are touched; a value that is already a non-string (e.g. a
    reader-decoded ``datetime``) or that cannot be parsed is returned
    unchanged. An unparseable value then falls through to the typed write,
    which rejects just that row via the 207 IngestionReport rather than
    failing the whole batch.
    """
    if not isinstance(value, str):
        return value
    text = value.strip()
    if not text:
        return value
    if parse_format:
        try:
            parsed = datetime.strptime(text, parse_format)
        except (ValueError, TypeError):
            # Explicit pattern set but this row doesn't match it: respect the
            # operator's intent and leave the row for the typed write to reject
            # (207), rather than auto-detecting a different interpretation.
            return value
    else:
        try:
            parsed = _dateutil_parser.parse(text)
        except (ValueError, OverflowError, TypeError):
            return value
    if data_type == "date":
        return parsed.date().isoformat()
    if data_type == "time":
        return parsed.time().isoformat()
    return parsed.isoformat()


def apply_temporal_coercion(properties: dict, temporal_fields: dict) -> dict:
    """Coerce every schema-declared temporal property in ``properties`` in place.

    ``temporal_fields`` maps a field name to a ``(data_type, parse_format)``
    pair — its canonical temporal ``data_type`` and the optional ``strptime``
    input hint (#1350; ``None`` when the field declares no hint). Properties not
    named there are left untouched, so this is a no-op for collections whose
    items-schema declares no temporal field. Returns ``properties`` for
    call-site convenience.
    """
    if not temporal_fields:
        return properties
    for name, (data_type, parse_format) in temporal_fields.items():
        if name in properties:
            properties[name] = _coerce_temporal_value(
                properties[name], data_type, parse_format
            )
    return properties


# Identity + lifecycle fields lifted from ``generated`` into ``record["system"]``.
# Each is omitted from the envelope when its value is None — absent values
# simply don't appear, so a reporter never sees a half-populated system bag.
_SYSTEM_KEYS = (
    "geoid",
    "external_id",
    "asset_id",
    "geometry_hash",
    "attributes_hash",
    "validity",
    "transaction_time",
    "deleted_at",
)


def _build_report_envelope(record, generated: dict | None) -> dict:
    """Reshape one ingestion outcome into the report envelope.

    Returns a dict with sibling keys ``properties`` (user attributes only —
    never mixed with platform-derived values), ``stats`` (flat dict of computed
    statistics), and ``system`` (identity + lifecycle fields, each present only
    when the ingestion actually produced a value). GeoJSON envelope keys
    (``type``/``id``/``geometry``) stay at the record top level.

    ``generated`` is the per-item entry from ``ctx.extensions["_generated_stats"]``
    or ``None`` on fallback / rejection paths where the upsert never produced one.
    A ``model_dump``-able ``record`` is normalised first so every reporter sees
    one shape.
    """
    if hasattr(record, "model_dump"):
        rec = record.model_dump(mode="json", exclude_none=True)
    elif isinstance(record, dict):
        rec = dict(record)
    else:
        return record

    props = rec.get("properties")
    rec["properties"] = dict(props) if isinstance(props, dict) else {}

    gen = generated or {}
    raw_stats = gen.get("stats") or {}
    rec["stats"] = dict(raw_stats) if isinstance(raw_stats, dict) else {}

    system: dict = {}
    for key in _SYSTEM_KEYS:
        value = gen.get(key)
        if value is not None:
            system[key] = value
    rec["system"] = system

    return rec


def _enrich_report_record(record, generated: dict) -> dict:
    """Build a SUCCESS-path report envelope. See ``_build_report_envelope``."""
    return _build_report_envelope(record, generated)


async def _broadcast_batch_outcome(
    reporters, batch: list, upsert_result, generated=None, rejections=None
):
    """Fan out per-row outcomes to every reporter after a batch upsert.

    Accepted rows are reported as SUCCESS and per-row ``rejections`` (the
    upsert's ``ctx.extensions["_rejections"]`` out-list) as FAILED, so a single
    bad row lands in the detailed report as a row failure instead of aborting
    the whole job. Contract for the payload shape lives on
    ``ReportingInterface.process_batch_outcome`` — each item is
    ``{"status", "message", "record"}``.

    With no rejections the upsert is transactional, so every input row
    persisted: the read-back ``upsert_result`` is the canonical record source
    (server-assigned fields), and if its shape doesn't line up 1:1 with the
    input batch we fall back to the input features. When there ARE rejections
    the batch is partial — the read-back holds only the accepted rows, so it is
    always the success source and the rejected rows are reported separately.

    ``generated`` is the upsert's ``ctx.extensions["_generated_stats"]`` — per
    accepted item the geoid, external_id, asset_id and full computed-statistics
    set, aligned with the read-back. It is applied only when its length matches
    the success records, else dropped rather than mis-zipped.
    """
    rejections = list(rejections or [])
    accepted = (
        upsert_result
        if isinstance(upsert_result, list)
        else ([upsert_result] if upsert_result else [])
    )
    # Legacy defensive fallback only applies when nothing was rejected: if the
    # read-back didn't align 1:1 with the input batch, report the input
    # features (server fields/stats unavailable) rather than under-reporting.
    if not rejections and len(accepted) != len(batch):
        records = batch
        generated = None
    else:
        records = accepted

    gen = (
        generated
        if isinstance(generated, list) and len(generated) == len(records)
        else None
    )
    outcomes = []
    for i, rec in enumerate(records):
        rec = _enrich_report_record(rec, gen[i] if gen is not None else {})
        outcomes.append({"status": "SUCCESS", "message": None, "record": rec})
    for rej in rejections:
        # Identity that the upsert managed to derive before the row was rejected.
        # Only keys the rejection carried surface in ``system`` (e.g. external_id
        # is known pre-write, geoid only when a sidecar minted one).
        rej_generated = {
            k: rej[k]
            for k in _SYSTEM_KEYS
            if rej.get(k) is not None
        }
        rec = _build_report_envelope(rej.get("record") or {}, rej_generated)
        outcomes.append({
            "status": "FAILED",
            "message": rej.get("message") or rej.get("reason") or "rejected",
            "record": rec,
        })
    if outcomes:
        await asyncio.gather(
            *(reporter.process_batch_outcome(outcomes) for reporter in reporters)
        )


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

        # --- Resolve temporal fields for string -> ISO-8601 coercion (#1333) ---
        # A property whose items-schema field is declared date/time/timestamp is
        # normalised from common date string formats to canonical ISO-8601 at
        # ingestion time, since the typed write path accepts ISO-8601 but not
        # arbitrary formats. A resolution failure (or a schema with no temporal
        # field) simply degrades to "no coercion".
        temporal_fields: dict = {}
        try:
            from dynastore.modules.storage.driver_config import ItemsSchema

            items_schema = await catalog_module.configs.get_config(
                ItemsSchema, catalog_id, collection_id
            )
            if items_schema and items_schema.fields:
                temporal_fields = {
                    name: (
                        fdef.data_type,
                        getattr(fdef, "parse_format", None),
                    )
                    for name, fdef in items_schema.fields.items()
                    if getattr(fdef, "data_type", None) in _TEMPORAL_DATA_TYPES
                }
        except Exception as exc:
            logger.debug(
                "Task '%s': items-schema temporal resolution skipped (%s)",
                task_id, exc,
            )
        if temporal_fields:
            logger.info(
                "Task '%s': temporal coercion active for fields: %s",
                task_id, sorted(temporal_fields),
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
                db_resource=engine,
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

        from dynastore.modules.catalog.asset_service import AssetStatus

        if asset.status == AssetStatus.PENDING:
            raise RuntimeError(
                f"Asset {asset.asset_id} is PENDING (kind={asset.kind}): "
                f"OBJECT_FINALIZE has not activated this asset yet. Either "
                f"upload completed but the GCS push event was lost / dropped "
                f"(check handle_asset_events logs for 'no physical schema' "
                f"or 'orphan finalize'), or upload is still in flight. "
                f"Re-submit ingestion only after status=='active'."
            )

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
                # Fallback: check if 'geometry' is already present (e.g. GeoJSON)
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
                # Geometry / CSV source columns are consumed into the feature
                # geometry and must not leak into the attribute set. The
                # external_id *source* field is deliberately NOT reserved: unlike
                # the geometry sources it is a genuine attribute — it backs its
                # own materialised column and the write policy's
                # ``derive.external_id`` reads it from ``properties`` at write
                # time — so stripping it here would null both that column and the
                # derived external_id. This is the ``"all"`` counterpart of
                # listing the external_id field explicitly in ``attribute_mapping``
                # (which already keeps it), so the two source modes now agree.
                reserved = {
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

            # 3b. Coerce schema-declared temporal properties (date/time/timestamp)
            # from common string formats to canonical ISO-8601 (#1333). No-op
            # when the items-schema declares no temporal field.
            apply_temporal_coercion(feature["properties"], temporal_fields)

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
        # then PyogrioReader as a tail fallback.  Solves the
        # ``CPLE_OpenFailedError: not recognized as being in a supported
        # file format`` blocker when a PyPI-wheel-bundled libgdal lacks the
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
                    upsert_ctx = DriverContext(db_resource=engine)
                    upsert_result = await catalog_module.upsert(
                        catalog_id,
                        collection_id,
                        current_batch,
                        ctx=upsert_ctx,
                        processing_context=upsert_context,
                    )
                    rows_ingested += len(current_batch)
                    await _broadcast_batch_outcome(
                        reporters, current_batch, upsert_result,
                        upsert_ctx.extensions.get("_generated_stats"),
                        rejections=upsert_ctx.extensions.get("_rejections"),
                    )
                    await asyncio.gather(
                        *(
                            reporter.update_progress(rows_ingested, total_features)
                            for reporter in reporters
                        )
                    )
                    current_batch = []

            if current_batch:
                upsert_ctx = DriverContext(db_resource=engine)
                upsert_result = await catalog_module.upsert(
                    catalog_id,
                    collection_id,
                    current_batch,
                    ctx=upsert_ctx,
                    processing_context=upsert_context,
                )
                rows_ingested += len(current_batch)
                await _broadcast_batch_outcome(
                    reporters, current_batch, upsert_result,
                    upsert_ctx.extensions.get("_generated_stats"),
                    rejections=upsert_ctx.extensions.get("_rejections"),
                )
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
