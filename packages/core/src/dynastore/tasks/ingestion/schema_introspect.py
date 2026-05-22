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
field names + native OGR types/subtypes. Each OGR type is mapped to the
canonical ``FieldDefinition.data_type`` vocabulary via the single source of
truth in :mod:`dynastore.models.field_types` (``string``, ``integer``,
``bigint``, ``double``, ``boolean``, ``date``, ``time``, ``timestamp``,
``binary``, ``jsonb``, ``uuid``, ``geometry``), with the OGR subtype
(``boolean``/``int16``/``float32``/``json``/``uuid``) preserved on
``FieldDefinition.subtype``.

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
from typing import Any, Dict, Optional, Tuple

# Hard import — gates this module on services with module_gdal in scope.
from osgeo import ogr, gdal  # noqa: F401

from dynastore.models.field_types import ogr_to_canonical
from dynastore.models.protocols.field_definition import FieldDefinition
from dynastore.tasks.ingestion.schema_from_gdalinfo import (
    field_definition_from_ogr_names,
    geometry_field_definition,
)

logger = logging.getLogger(__name__)

# Initialize once. Idempotent.
ogr.UseExceptions()
gdal.UseExceptions()


# ---------------------------------------------------------------------------
# OGR field type → canonical data_type mapping
# ---------------------------------------------------------------------------
#
# The OGR → canonical mapping is the SSOT in ``dynastore.models.field_types``
# (GDAL is the canonical *source* of geospatial field types). Here we only read
# the OGR type/subtype names off each field and delegate the mapping, so the
# vocabulary stays consistent with every storage driver.


def _ogr_field_type_name(field_defn: Any) -> str:
    """Return the human-readable OGR type name (``Integer`` / ``String`` / …)."""
    return ogr.GetFieldTypeName(field_defn.GetType())


def _ogr_field_subtype_name(field_defn: Any) -> Optional[str]:
    """Return the OGR subtype label (``Boolean``/``Int16``/…) or ``None``."""
    sub = field_defn.GetSubType()
    if sub == ogr.OFSTNone:
        return None
    return ogr.GetFieldSubTypeName(sub)


def _map_ogr_type(field_defn: Any) -> Tuple[str, Optional[str]]:
    """Map an ``ogr.FieldDefn`` to canonical ``(data_type, subtype)``."""
    return ogr_to_canonical(
        _ogr_field_type_name(field_defn), _ogr_field_subtype_name(field_defn)
    )


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

        # Geometry first — always present (or always absent) on an OGR
        # layer; we declare it unconditionally so downstream sidecars
        # don't need a separate "is there geometry?" probe. Built through the
        # same shared helpers as the blob-sourced path so the two agree.
        out: Dict[str, FieldDefinition] = {
            "geometry": geometry_field_definition(
                ogr.GeometryTypeToName(layer.GetGeomType())
            ),
        }

        for i in range(layer_defn.GetFieldCount()):
            fd = layer_defn.GetFieldDefn(i)
            fname = fd.GetName()
            out[fname] = field_definition_from_ogr_names(
                fname, _ogr_field_type_name(fd), _ogr_field_subtype_name(fd),
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
