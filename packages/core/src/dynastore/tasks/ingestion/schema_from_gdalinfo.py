#    Copyright 2025 FAO
#
#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at
#
#        http://www.apache.org/licenses/LICENSE-2.0
"""Derive an ``items_schema`` field map from a stored ``gdalinfo`` blob.

This is the GDAL-free counterpart of :mod:`schema_introspect`: it turns the
JSON that ``modules.gdal.service.get_vector_info`` already wrote onto
``asset.metadata["gdalinfo"]`` into ``{field_name: FieldDefinition}`` — without
re-opening the source through OGR. That lets any service (including ones without
libgdal in scope) propose a collection schema straight from asset metadata.

Both this module and :mod:`schema_introspect` build their ``FieldDefinition``s
through the same two helpers here (:func:`field_definition_from_ogr_names` and
:func:`geometry_field_definition`), so the file-sourced and blob-sourced paths
produce identical results for the same OGR (type, subtype) pair — the canonical
mapping lives in exactly one place (:func:`dynastore.models.field_types.ogr_to_canonical`).
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple

from dynastore.models.field_types import ogr_to_canonical
from dynastore.models.protocols.field_definition import FieldDefinition

# The conventional key for the geometry column in a derived schema. Declared
# unconditionally (an OGR layer has one geometry column or none); downstream
# sidecars then never need a separate "is there geometry?" probe.
GEOMETRY_FIELD_NAME = "geometry"


def geometry_field_definition(
    geometry_type: Optional[str] = None,
) -> FieldDefinition:
    """Build the standard geometry ``FieldDefinition``.

    ``geometry_type`` is the OGR geometry-type label (e.g. ``"Point"``,
    ``"3D Multi Polygon"``) when known; it is recorded in the description only.
    The ``data_type`` is the bare canonical ``"geometry"`` — the concrete
    geometry type and SRID are owned by the geometry sidecar / driver, not the
    items schema.
    """
    suffix = f" ({geometry_type})" if geometry_type else ""
    return FieldDefinition(
        name=GEOMETRY_FIELD_NAME,
        data_type="geometry",
        description=f"Feature geometry (derived from OGR layer geometry column){suffix}.",
    )


def field_definition_from_ogr_names(
    name: str,
    type_name: str,
    subtype_name: Optional[str] = None,
) -> FieldDefinition:
    """Build a ``FieldDefinition`` from raw OGR (type, subtype) *names*.

    Shared by the file-sourced introspector and the blob-sourced derivation so
    both map identically. ``type_name`` / ``subtype_name`` are the human OGR
    labels (``"Integer64"``, ``"Boolean"``); they go through
    :func:`ogr_to_canonical`, which yields the canonical ``data_type`` + optional
    ``subtype`` (Boolean/JSON/UUID promote the base type).
    """
    data_type, subtype = ogr_to_canonical(type_name, subtype_name)
    sub_note = f", subtype: {subtype_name}" if subtype_name else ""
    return FieldDefinition(
        name=name,
        data_type=data_type,
        subtype=subtype,
        description=f"Auto-derived from OGR field {name!r} (OGR type: {type_name}{sub_note}).",
    )


def _select_layer(
    layers: List[Dict[str, Any]], layer_name: Optional[str],
) -> Dict[str, Any]:
    if layer_name is None:
        return layers[0]
    for layer in layers:
        if layer.get("name") == layer_name:
            return layer
    available = [layer.get("name") for layer in layers]
    raise RuntimeError(
        f"Layer {layer_name!r} not found in gdalinfo blob; available: {available}."
    )


def derive_schema_from_gdalinfo(
    gdalinfo: Optional[Dict[str, Any]],
    *,
    layer_name: Optional[str] = None,
) -> Dict[str, FieldDefinition]:
    """Derive ``{field_name: FieldDefinition}`` from a stored ``gdalinfo`` blob.

    ``gdalinfo`` is the dict produced by ``get_vector_info`` and stored at
    ``asset.metadata["gdalinfo"]`` (shape: ``{"layers": [{"name", "geometryType",
    "fields": [{"name", "type", "subtype?"}]}]}``). ``layer_name`` selects a
    layer for multi-layer sources; the first layer wins when omitted.

    A ``geometry`` field is always added first (mirrors
    :func:`schema_introspect.extract_ogr_schema`). Raises ``RuntimeError`` when
    the blob carries no layers — callers should surface that as HTTP 422 / 400.
    """
    layers = (gdalinfo or {}).get("layers") or []
    if not layers:
        raise RuntimeError(
            "gdalinfo blob has no layers; cannot derive an items schema."
        )

    layer = _select_layer(layers, layer_name)
    out: Dict[str, FieldDefinition] = {
        GEOMETRY_FIELD_NAME: geometry_field_definition(layer.get("geometryType")),
    }

    for field in layer.get("fields") or []:
        fname = field.get("name")
        if not fname:
            continue
        out[fname] = field_definition_from_ogr_names(
            fname, field.get("type") or "String", field.get("subtype"),
        )

    return out


def merge_derived_fields(
    current: Dict[str, FieldDefinition],
    derived: Dict[str, FieldDefinition],
) -> Tuple[Dict[str, FieldDefinition], Dict[str, List[str]]]:
    """Merge ``derived`` fields into the ``current`` schema, preserving tuning.

    The derivation only knows about *type* — so for a field that already exists
    in ``current`` it contributes ``data_type``/``subtype`` only; every other
    attribute an admin set (``materialize``, ``capabilities``, ``required``,
    ``unique``, ``title``, ``description`` …) is kept. Fields new in ``derived``
    are added as-is. Fields present in ``current`` but absent from ``derived``
    are kept untouched — removal is destructive and never automatic, so
    re-deriving from a different asset can't silently drop columns.

    Returns ``(merged, summary)`` where ``summary`` buckets field names into
    ``added`` / ``updated`` (type or subtype changed) / ``unchanged`` /
    ``preserved`` (kept; not present in the derivation).
    """
    merged: Dict[str, FieldDefinition] = dict(current)
    added: List[str] = []
    updated: List[str] = []
    unchanged: List[str] = []

    for name, dfd in derived.items():
        cur = current.get(name)
        if cur is None:
            merged[name] = dfd
            added.append(name)
            continue
        merged[name] = cur.model_copy(
            update={"data_type": dfd.data_type, "subtype": dfd.subtype}
        )
        if (cur.data_type, cur.subtype) == (dfd.data_type, dfd.subtype):
            unchanged.append(name)
        else:
            updated.append(name)

    summary = {
        "added": sorted(added),
        "updated": sorted(updated),
        "unchanged": sorted(unchanged),
        "preserved": sorted(n for n in current if n not in derived),
    }
    return merged, summary
