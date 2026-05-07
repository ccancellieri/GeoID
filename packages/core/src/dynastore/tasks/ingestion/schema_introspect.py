#    Copyright 2025 FAO
#
#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at
#
#        http://www.apache.org/licenses/LICENSE-2.0
"""OGR schema introspection — derive a ItemsSchema from a vector asset.

Reads a vector source (Shapefile, GeoPackage, GeoJSON, Parquet, …) via
the system OGR bindings and walks ``layer.GetLayerDefn()`` to extract
field names + native OGR types. Maps each OGR type to the
``ItemsSchema``-compatible string used by ``FieldDefinition.data_type``
(``text``, ``integer``, ``float``, ``boolean``, ``date``, ``timestamp``,
``jsonb``, ``geometry``).

The output dict is suitable to PATCH directly into ``ItemsSchema.fields``
(class_key ``"items_schema"``)::

    derived = extract_ogr_schema("/vsigs/bucket/roads.zip")
    await configs_svc.patch_config(
        catalog_id, collection_id,
        {"items_schema": {"fields": derived,
                               "strict_unknown_fields": True,
                               "materialize_fields_as_columns": True}},
    )

Hard-imports ``osgeo`` at module load time. When SCOPE excludes
``module_gdal`` the import fails — match the existing
``GdalOsgeoReader`` gating (registry skips this tool on services
without libgdal). Same rationale as in
:mod:`dynastore.tasks.ingestion.readers.osgeo_reader`.
"""

from __future__ import annotations

import logging
from typing import Any, Dict, Optional

# Hard import — gates this module on services with module_gdal in scope.
from osgeo import ogr, gdal  # noqa: F401

from dynastore.models.protocols.field_definition import FieldDefinition

logger = logging.getLogger(__name__)

# Initialize once. Idempotent.
ogr.UseExceptions()
gdal.UseExceptions()


# ---------------------------------------------------------------------------
# OGR field type → ItemsSchema data_type mapping
# ---------------------------------------------------------------------------
#
# OGR field types are integer constants (ogr.OFT*); we map them to the
# string vocabulary used by ``ItemsSchema.fields[*].data_type``.
# The PG driver's ``_DATA_TYPE_TO_PG_NAME`` table at
# ``modules/storage/field_constraints.py:35`` knows how to lift these
# into native PG columns once ``materialize_fields_as_columns=True``.

_OGR_TYPE_NAME_TO_DATA_TYPE: Dict[str, str] = {
    "Integer": "integer",
    "Integer64": "bigint",
    "Real": "float",
    "String": "text",
    "Date": "date",
    "Time": "text",        # OGR time has no PG-native equiv we care about
    "DateTime": "timestamp",
    "Binary": "text",      # base64-stringified at write time
    "IntegerList": "jsonb",
    "Integer64List": "jsonb",
    "RealList": "jsonb",
    "StringList": "jsonb",
}


def _ogr_field_type_name(field_defn: Any) -> str:
    """Return the human-readable OGR type name (``Integer`` / ``String`` / …).

    OGR exposes both ``GetType()`` (an int constant) and
    ``GetFieldTypeName(...)`` (the human label). We use the label so the
    mapping table stays readable + decoupled from the OGR enum's value
    drift across libgdal versions.
    """
    return ogr.GetFieldTypeName(field_defn.GetType())


def _map_ogr_type(field_defn: Any) -> str:
    """Map an ``ogr.FieldDefn`` to the ``ItemsSchema.data_type`` vocabulary.

    Falls back to ``"text"`` for unrecognised types — safe choice because
    every backend can store text. Logs a debug line so the operator can
    spot type-mapping gaps.
    """
    name = _ogr_field_type_name(field_defn)
    mapped = _OGR_TYPE_NAME_TO_DATA_TYPE.get(name)
    if mapped is None:
        logger.debug(
            "extract_ogr_schema: OGR field type %r has no mapping — "
            "defaulting to 'text'.", name,
        )
        return "text"
    return mapped


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def extract_ogr_schema(
    uri: str, *, layer_name: Optional[str] = None,
) -> Dict[str, FieldDefinition]:
    """Extract field definitions from a vector source.

    ``uri`` accepts every URI scheme libgdal supports — local paths,
    ``/vsigs/bucket/key`` for GCS, ``/vsizip/path/to.zip`` for zipped
    shapefiles, ``/vsicurl/...`` for remote HTTP, etc.

    ``layer_name`` selects a specific layer for multi-layer sources
    (GeoPackage, FileGDB). When None, the FIRST layer wins.

    Always adds a ``geometry`` field at the start (``data_type="geometry"``)
    so the resulting schema is immediately compatible with the geometry
    sidecar — even when the OGR source has no per-feature geometry
    column declared (it always has one or zero).

    Raises ``RuntimeError`` when the source cannot be opened or has no
    layers; callers are expected to surface this as HTTP 422 / 400.
    """
    ds = gdal.OpenEx(uri, gdal.OF_VECTOR | gdal.OF_READONLY)
    if ds is None:
        raise RuntimeError(f"OGR could not open source: {uri!r}")

    try:
        if layer_name is not None:
            layer = ds.GetLayerByName(layer_name)
            if layer is None:
                raise RuntimeError(
                    f"Layer {layer_name!r} not found in source {uri!r}; "
                    f"available: {[ds.GetLayer(i).GetName() for i in range(ds.GetLayerCount())]}"
                )
        else:
            if ds.GetLayerCount() == 0:
                raise RuntimeError(
                    f"OGR source {uri!r} has no layers."
                )
            layer = ds.GetLayer(0)

        layer_defn = layer.GetLayerDefn()
        out: Dict[str, FieldDefinition] = {}

        # Geometry first — always present (or always absent) on an OGR
        # layer; we declare it unconditionally so downstream sidecars
        # don't need a separate "is there geometry?" probe.
        out["geometry"] = FieldDefinition(
            name="geometry",
            data_type="geometry",
            description="Feature geometry (derived from OGR layer geometry column).",
        )

        for i in range(layer_defn.GetFieldCount()):
            fd = layer_defn.GetFieldDefn(i)
            fname = fd.GetName()
            out[fname] = FieldDefinition(
                name=fname,
                data_type=_map_ogr_type(fd),
                description=(
                    f"Auto-derived from OGR field {fname!r} "
                    f"(OGR type: {_ogr_field_type_name(fd)})."
                ),
            )

        logger.info(
            "extract_ogr_schema: %d fields derived from %r (layer %r).",
            len(out), uri, layer.GetName(),
        )
        return out
    finally:
        # OGR datasets need explicit close (Python GC isn't reliable
        # for libgdal handles, especially on /vsi* sources).
        ds = None  # noqa: F841
