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

"""CityJSONSeq parser, dequantizer, and item-mapping functions.

Pure functions — no database access. All geometry operations use shapely
(2D footprint union) + pyproj (CRS reprojection to WGS84).
"""

from __future__ import annotations

import json
import logging
import pathlib
import re
from typing import Any, Optional

import pyproj
import shapely.geometry
import shapely.ops
from pydantic import BaseModel

from dynastore.tools.geometry_repair import coerce_to_polygonal
from dynastore.models.protocols.field_definition import (
    FieldCapability,
    FieldDefinition,
    FeatureTypeDefinition,
)

logger = logging.getLogger(__name__)

# URI form: .../EPSG/<version>/<code>  (case-insensitive)
_EPSG_URI_RE = re.compile(r"/EPSG/[^/]+/(\d+)\s*$", re.IGNORECASE)
# URN form: EPSG::<code>              (case-insensitive)
_EPSG_URN_RE = re.compile(r"EPSG::(\d+)\s*$", re.IGNORECASE)


# ---------------------------------------------------------------------------
# Data models
# ---------------------------------------------------------------------------


class CityJsonHeader(BaseModel):
    """Parsed header from the first line of a CityJSONSeq stream (or a CityJSON file)."""

    version: str
    transform_scale: list[float]
    transform_translate: list[float]
    reference_system: Optional[str] = None
    epsg: Optional[int] = None
    metadata: dict[str, Any] = {}


# CityObject types that are surface/coverage layers, not volumetric objects.
# Our ingest model unions a CityObject's surface rings into one 2D footprint
# and extrudes it by the feature's z-extent — correct for solids (Building),
# but for a terrain TIN or a flat coverage it collapses the whole layer into a
# single tile-spanning slab that swallows the scene. Real city tiles (3DBAG /
# 3D BK Delft) ship a TINRelief next to the buildings, so it must be dropped.
_NON_VOLUMETRIC_TYPES: frozenset[str] = frozenset(
    {
        "TINRelief",
        "WaterBody",
        "LandUse",
        "PlantCover",
        "TransportationSquare",
        "Road",
        "Railway",
        "Waterway",
    }
)


# ---------------------------------------------------------------------------
# Feature-type declaration (Task 1.2)
# ---------------------------------------------------------------------------

CITYOBJECT_FEATURE_TYPE: FeatureTypeDefinition = FeatureTypeDefinition(
    fields={
        "geometry": FieldDefinition(
            name="geometry",
            data_type="geometry(MultiPolygon,4326)",
            capabilities=[FieldCapability.SPATIAL],
        ),
        "citygml_type": FieldDefinition(
            name="citygml_type",
            data_type="string",
            capabilities=[FieldCapability.FILTERABLE, FieldCapability.SORTABLE],
        ),
        "lod": FieldDefinition(
            name="lod",
            data_type="jsonb",
            capabilities=[],
        ),
        "height": FieldDefinition(
            name="height",
            data_type="double",
            capabilities=[FieldCapability.FILTERABLE],
        ),
        "zmin": FieldDefinition(
            name="zmin",
            data_type="double",
            capabilities=[FieldCapability.FILTERABLE],
        ),
        "zmax": FieldDefinition(
            name="zmax",
            data_type="double",
            capabilities=[FieldCapability.FILTERABLE],
        ),
        "name": FieldDefinition(
            name="name",
            data_type="string",
            capabilities=[FieldCapability.FILTERABLE],
        ),
        "cityjson": FieldDefinition(
            name="cityjson",
            data_type="jsonb",
            capabilities=[],
            container="extras",
        ),
    }
)


# ---------------------------------------------------------------------------
# Parsing
# ---------------------------------------------------------------------------


def parse_epsg(reference_system: Optional[str]) -> Optional[int]:
    """Extract the EPSG code from an OGC CRS URI or URN.

    Only recognises EPSG-authority references:
    - URI form:  .../EPSG/<version>/<code>  e.g. https://www.opengis.net/def/crs/EPSG/0/7415
    - URN form:  EPSG::<code>               e.g. urn:ogc:def:crs:EPSG::28992

    Returns None for non-EPSG authorities (e.g. OGC/CRS84) and for None/empty input.
    """
    if not reference_system:
        return None
    m = _EPSG_URI_RE.search(reference_system)
    if m:
        return int(m.group(1))
    m = _EPSG_URN_RE.search(reference_system)
    return int(m.group(1)) if m else None


# Backwards-compat alias


def _header_from_dict(d: dict[str, Any]) -> CityJsonHeader:
    transform = d.get("transform", {})
    meta = d.get("metadata", {})
    ref_sys = meta.get("referenceSystem")
    return CityJsonHeader(
        version=d.get("version", ""),
        transform_scale=transform.get("scale", [1.0, 1.0, 1.0]),
        transform_translate=transform.get("translate", [0.0, 0.0, 0.0]),
        reference_system=ref_sys,
        epsg=parse_epsg(ref_sys),
        metadata=meta,
    )


def _split_cityjson_to_features(doc: dict[str, Any]) -> list[dict[str, Any]]:
    """Split a plain CityJSON document into CityJSONFeature-shaped dicts.

    Top-level CityObjects (those without "parents") become individual feature
    roots. Each root takes its children-closure with it. Global vertex indices
    are remapped to a compact per-feature array.

    Note: cjio 0.10.x exposes feature splitting only through its CLI
    (``cjio ... export cityjsonseq``). Its programmatic ``generate_features``
    method follows ``"children"`` keys on parent objects to collect child
    closures, so CityJSON documents that carry only ``"parents"`` on child
    objects (without a matching ``"children"`` list on the parent) would
    silently lose those children. This local implementation discovers children
    from the ``"parents"`` key on each object, which is always present in
    well-formed CityJSON, making it safe for all valid inputs.
    """
    city_objects: dict[str, Any] = doc.get("CityObjects", {})
    global_vertices: list[list[int]] = doc.get("vertices", [])

    # Build parent→children map
    children_map: dict[str, list[str]] = {}
    for obj_id, obj in city_objects.items():
        for parent_id in obj.get("parents", []):
            children_map.setdefault(parent_id, []).append(obj_id)

    # Identify roots (no parents declared), skipping surface/coverage layers
    # (terrain TINRelief, water, land use, …) that would extrude into one
    # scene-swallowing slab. Only volumetric objects become features.
    roots = [
        k
        for k, v in city_objects.items()
        if not v.get("parents") and v.get("type") not in _NON_VOLUMETRIC_TYPES
    ]

    features = []
    for root_id in roots:
        # Collect root + all descendants
        closure: dict[str, Any] = {}
        stack = [root_id]
        while stack:
            current = stack.pop()
            if current in city_objects:
                closure[current] = city_objects[current]
                stack.extend(children_map.get(current, []))

        # Collect all global vertex indices referenced in closure geometries
        global_indices_used: list[int] = []
        for obj in closure.values():
            for geom in obj.get("geometry", []):
                _collect_indices(geom.get("boundaries", []), global_indices_used)

        # Build compact vertex array + index remap
        unique_global = sorted(set(global_indices_used))
        remap = {g: local_idx for local_idx, g in enumerate(unique_global)}
        local_vertices = [global_vertices[i] for i in unique_global]

        # Deep-copy closure and remap boundaries
        remapped_closure = _remap_closure(closure, remap)

        feature = {
            "type": "CityJSONFeature",
            "id": root_id,
            "CityObjects": remapped_closure,
            "vertices": local_vertices,
        }
        features.append(feature)

    return features


def _collect_indices(boundaries: Any, out: list[int]) -> None:
    """Recursively collect all leaf integer indices from a nested boundaries list."""
    for item in boundaries:
        if isinstance(item, list):
            _collect_indices(item, out)
        elif isinstance(item, int):
            out.append(item)


def _remap_closure(
    closure: dict[str, Any], remap: dict[int, int]
) -> dict[str, Any]:
    """Return a deep copy of the CityObject closure with remapped vertex indices."""
    result = {}
    for obj_id, obj in closure.items():
        new_obj = {k: v for k, v in obj.items() if k != "geometry"}
        new_geoms = []
        for geom in obj.get("geometry", []):
            new_geom = {k: v for k, v in geom.items() if k != "boundaries"}
            new_geom["boundaries"] = _remap_boundaries(geom.get("boundaries", []), remap)
            new_geoms.append(new_geom)
        new_obj["geometry"] = new_geoms
        result[obj_id] = new_obj
    return result


def _remap_boundaries(boundaries: Any, remap: dict[int, int]) -> Any:
    if isinstance(boundaries, list):
        return [_remap_boundaries(b, remap) for b in boundaries]
    if isinstance(boundaries, int):
        return remap[boundaries]
    return boundaries


def parse_cityjsonseq(
    path: pathlib.Path | str,
) -> tuple[CityJsonHeader, list[dict[str, Any]]]:
    """Parse a CityJSONSeq (.city.jsonl) or a plain CityJSON (.city.json) file.

    For CityJSONSeq: line 1 is the header object, remaining lines are CityJSONFeature.
    For CityJSON: the single object is split into CityJSONFeature dicts.

    Returns (header, list_of_feature_dicts).
    """
    path = pathlib.Path(path)
    text = path.read_text(encoding="utf-8")
    lines = [ln for ln in text.splitlines() if ln.strip()]
    if not lines:
        raise ValueError(f"Empty file: {path}")

    first = json.loads(lines[0])
    obj_type = first.get("type", "")

    if obj_type == "CityJSONSeq":
        header = _header_from_dict(first)
        features = [json.loads(ln) for ln in lines[1:] if ln.strip()]
        return header, features

    if obj_type == "CityJSON":
        header = _header_from_dict(first)
        features = _split_cityjson_to_features(first)
        return header, features

    raise ValueError(f"Unsupported CityJSON type '{obj_type}' in {path}")


# ---------------------------------------------------------------------------
# Geometry helpers
# ---------------------------------------------------------------------------


def dequantize(
    vertices: list[list[int]], header: CityJsonHeader
) -> list[tuple[float, float, float]]:
    """Convert quantized integer vertices to real-world float coordinates."""
    sx, sy, sz = header.transform_scale
    tx, ty, tz = header.transform_translate
    return [
        (v[0] * sx + tx, v[1] * sy + ty, v[2] * sz + tz)
        for v in vertices
    ]


def extract_surfaces(geometry: dict[str, Any]) -> list[list[int]]:
    """Return outer rings (list of vertex indices) for all surfaces in one geometry.

    CityJSON nesting per type:
    - MultiSurface/CompositeSurface: boundaries[surface][ring][vertex_idx]
    - Solid:                         boundaries[shell][surface][ring][vertex_idx]
    - MultiSolid/CompositeSolid:     boundaries[solid][shell][surface][ring][vertex_idx]

    We take only the outer ring (ring index 0) of each surface.
    """
    geom_type = geometry.get("type", "")
    boundaries = geometry.get("boundaries", [])
    surfaces: list[list[int]] = []

    if geom_type in ("MultiSurface", "CompositeSurface"):
        # boundaries[surface][ring][vertex_idx]
        for surface in boundaries:
            if surface and surface[0]:
                surfaces.append(surface[0])
    elif geom_type == "Solid":
        # boundaries[shell][surface][ring][vertex_idx]
        for shell in boundaries:
            for surface in shell:
                if surface and surface[0]:
                    surfaces.append(surface[0])
    elif geom_type in ("MultiSolid", "CompositeSolid"):
        for solid in boundaries:
            for shell in solid:
                for surface in shell:
                    if surface and surface[0]:
                        surfaces.append(surface[0])
    return surfaces


# Backwards-compat alias


def _coerce_to_multipolygon(
    geom: shapely.geometry.base.BaseGeometry,
) -> shapely.geometry.MultiPolygon:
    """Return a valid MultiPolygon from any shapely geometry.

    Delegates to :func:`dynastore.tools.geometry_repair.coerce_to_polygonal`,
    which is the single source of truth for make_valid + polygonal coercion.
    Returns an empty ``MultiPolygon`` when no polygonal area survives —
    callers must check ``is_empty`` and handle accordingly.
    """
    return coerce_to_polygonal(geom)  # type: ignore[return-value]


def _build_footprint_geojson(
    feature: dict[str, Any],
    transformer: pyproj.Transformer,
    vertices_3d: list[tuple[float, float, float]],
) -> dict[str, Any]:
    """Build a WGS84 MultiPolygon GeoJSON footprint from all surfaces.

    ``vertices_3d`` must be the already-dequantized coordinates for this
    feature — dequantization is the caller's responsibility so it is done
    exactly once per feature.
    """
    polygons: list[shapely.geometry.Polygon] = []
    for obj in feature.get("CityObjects", {}).values():
        for geom in obj.get("geometry", []):
            for ring_indices in extract_surfaces(geom):
                # ring_indices is a flat list of int vertex indices
                ring_3d = [vertices_3d[i] for i in ring_indices]
                ring_2d = [transformer.transform(p[0], p[1]) for p in ring_3d]
                if len(ring_2d) >= 3:
                    polygons.append(shapely.geometry.Polygon(ring_2d))

    if not polygons:
        raise ValueError("No valid surface geometry found in feature")

    # Repair self-intersecting rings before the union — LoD2 wall/roof
    # surfaces projected to 2D routinely produce bow-tie rings that make
    # unary_union raise or return degenerate geometry, losing the building.
    # ``make_valid`` is used instead of ``buffer(0)`` because ``buffer(0)``
    # is lossy on bow-tie / figure-8 rings (it discards the self-intersection
    # entirely, losing area), whereas ``make_valid`` preserves all polygon
    # area. The subsequent ``_coerce_to_multipolygon`` call ensures the result
    # is always a Polygon or MultiPolygon (never a GeometryCollection, which
    # ES geo_shape rejects).
    repaired = [_coerce_to_multipolygon(p) for p in polygons]
    repaired = [p for p in repaired if not p.is_empty]

    merged = shapely.ops.unary_union(repaired if repaired else polygons)
    merged = _coerce_to_multipolygon(merged)
    if merged.is_empty:
        raise ValueError("No valid surface geometry found in feature")

    return shapely.geometry.mapping(merged)


# ---------------------------------------------------------------------------
# Feature mapping
# ---------------------------------------------------------------------------


def feature_to_item_input(
    feature: dict[str, Any],
    header: CityJsonHeader,
    transformer: Optional[pyproj.Transformer] = None,
) -> dict[str, Any]:
    """Map a CityJSONFeature dict to an item-input dict for DynaStore ingestion.

    ``transformer`` is optional: when None, one is built from ``header.epsg``.
    Pass a pre-built Transformer when processing many features from the same
    header to avoid repeated PROJ-db reads.

    Raises ValueError if header.epsg is None and no transformer is provided.
    """
    if transformer is None:
        if header.epsg is None:
            raise ValueError("Cannot reproject: header has no EPSG code")
        transformer = pyproj.Transformer.from_crs(
            header.epsg, 4326, always_xy=True
        )

    feature_id = feature.get("id", "")
    city_objects = feature.get("CityObjects", {})

    # Identify the root CityObject (same id as the feature, or first if not found)
    root_obj = city_objects.get(feature_id) or (
        next(iter(city_objects.values())) if city_objects else {}
    )
    root_type = root_obj.get("type", "")
    root_attrs = root_obj.get("attributes", {})

    # Dequantize once; reuse for both z-extent extraction and footprint building.
    vertices_3d = dequantize(feature.get("vertices", []), header)

    # Collect lod values and z extents
    lod_set: set[str] = set()
    z_values: list[float] = [v[2] for v in vertices_3d]
    for obj in city_objects.values():
        for geom in obj.get("geometry", []):
            lod = geom.get("lod")
            if lod is not None:
                lod_set.add(str(lod))

    zmin = min(z_values) if z_values else 0.0
    zmax = max(z_values) if z_values else 0.0

    footprint = _build_footprint_geojson(feature, transformer, vertices_3d)

    def _lod_sort_key(v: str) -> tuple[int, float | str]:
        try:
            return (0, float(v))
        except ValueError:
            return (1, v)

    props: dict[str, Any] = {
        "citygml_type": root_type,
        "lod": sorted(lod_set, key=_lod_sort_key),
        "zmin": zmin,
        "zmax": zmax,
    }
    if "height" in root_attrs:
        props["height"] = root_attrs["height"]
    if "name" in root_attrs:
        props["name"] = root_attrs["name"]
    # Flatten remaining root attributes
    for k, v in root_attrs.items():
        if k not in props:
            props[k] = v

    # The raw CityJSONFeature rides INSIDE properties: free-form top-level
    # keys are dropped by the item write path (producer-props contract),
    # while properties persist across every driver — and /cityjsonseq
    # needs the full payload back per item.
    props["cityjson"] = feature

    return {
        "id": feature_id,
        "geometry": footprint,
        "properties": props,
    }


# ---------------------------------------------------------------------------
# Validation (optional cjvalpy)
# ---------------------------------------------------------------------------


def validate_cityjson(
    header: CityJsonHeader, features: list[dict[str, Any]]
) -> list[str]:
    """Run cjvalpy validation if available; return list of warning strings."""
    try:
        import cjvalpy  # type: ignore[import]
    except ImportError:
        return []
    warnings: list[str] = []
    try:
        for feat in features:
            result = cjvalpy.validate(feat)
            if result and hasattr(result, "errors"):
                warnings.extend(str(e) for e in result.errors)
    except Exception as exc:
        logger.debug("cjvalpy validation error (non-fatal): %s", exc)
    return warnings


# ---------------------------------------------------------------------------
# Ingestion entry point (Task 1.2)
# ---------------------------------------------------------------------------


async def ingest_cityjson_file(
    catalog_id: str,
    collection_id: str,
    path: pathlib.Path | str,
    *,
    item_service: Any,
    catalog_service: Any,
    batch_size: int = 500,
) -> dict[str, Any]:
    """Parse a CityJSONSeq or CityJSON file and bulk-create items in DynaStore.

    Steps:
    1. Parse file into header + features.
    2. Update collection extras with CityJSON provenance metadata.
    3. Map each feature to an item-input dict.
    4. Bulk-create items in batches via item_service.upsert_bulk.

    Batches are processed best-effort: a failure in one batch is recorded as a
    warning and the remaining batches are still attempted.  The returned summary
    therefore distinguishes successfully-ingested items from items in failed batches.

    Returns a summary dict with keys:
    - "items"    (int)       — count of items successfully ingested
    - "failed"   (int)       — count of items in batches that raised
    - "warnings" (list[str]) — per-feature and per-batch warning messages

    Raises ValueError immediately (before any DB work) when the file header
    carries no usable EPSG reference system, because reprojection is impossible.
    """
    path = pathlib.Path(path)
    header, features = parse_cityjsonseq(path)
    warnings = validate_cityjson(header, features)

    # Fail fast: reprojection is required; without an EPSG code nothing can proceed.
    if header.epsg is None:
        raise ValueError(
            f"Cannot ingest {path.name}: CityJSON header has no usable EPSG "
            "reference system (metadata.referenceSystem)"
        )

    # Store header provenance on the collection.  The metadata is written
    # to BOTH surfaces because the PG core driver persists only its
    # declared columns: ``extras`` reaches the search/index slice while
    # ``extra_metadata`` is the PG-persisted column the READ path returns
    # — without the latter, GET /collections/{id} cannot detect the
    # collection as a 3D container.
    cityjson_meta: dict[str, Any] = {
        "cityjson:version": header.version,
        "cityjson:transform": {
            "scale": header.transform_scale,
            "translate": header.transform_translate,
        },
    }
    if header.reference_system:
        cityjson_meta["cityjson:referenceSystem"] = header.reference_system

    await catalog_service.update_collection(
        catalog_id,
        collection_id,
        {"extras": cityjson_meta, "extra_metadata": cityjson_meta},
    )

    # Build the CRS transformer once per file; reuse across all features
    # to avoid repeated PROJ-db reads (one read per feature otherwise).
    transformer: pyproj.Transformer = pyproj.Transformer.from_crs(
        header.epsg, 4326, always_xy=True
    )

    # Map features → item dicts
    items: list[dict[str, Any]] = []
    for feat in features:
        try:
            items.append(feature_to_item_input(feat, header, transformer))
        except Exception as exc:
            fid = feat.get("id", "<unknown>")
            warnings.append(f"Skipped feature {fid!r}: {exc}")

    # Bulk-create in batches (best-effort: failures are recorded, not re-raised)
    total = 0
    failed = 0
    for i in range(0, len(items), batch_size):
        batch = items[i : i + batch_size]
        try:
            await item_service.upsert(catalog_id, collection_id, batch)
            total += len(batch)
        except Exception as exc:
            batch_num = i // batch_size
            failed += len(batch)
            warnings.append(
                f"Batch {batch_num} failed ({len(batch)} items): {exc}"
            )

    # Update the collection's spatial extent + vertical range from the
    # ingested footprints.  Collections are typically created with an
    # empty extent; without this step the GeoVolumes contentExtent stays
    # [0,0,0,0,0,0] and bbox filtering is useless.
    if total and items:
        try:
            bounds = _dataset_bounds(items)
            if bounds is not None:
                minx, miny, maxx, maxy, zmin, zmax = bounds
                meta_with_range = {
                    **cityjson_meta,
                    "geovolumes:zrange": {"zmin": zmin, "zmax": zmax},
                    # The extent column only persists where the optional
                    # STAC collection sidecar is materialized; stamp the
                    # 2D bbox into extras too so the GeoVolumes
                    # contentExtent survives on core-only deployments.
                    "geovolumes:bbox": [minx, miny, maxx, maxy],
                }
                await catalog_service.update_collection(
                    catalog_id,
                    collection_id,
                    {
                        "extras": meta_with_range,
                        "extra_metadata": meta_with_range,
                        "extent": {
                            "spatial": {"bbox": [[minx, miny, maxx, maxy]]},
                            "temporal": {"interval": [[None, None]]},
                        },
                    },
                )
        except Exception as exc:
            warnings.append(f"Failed to update collection extent: {exc}")

    return {"items": total, "failed": failed, "warnings": warnings}


def _dataset_bounds(
    items: list[dict[str, Any]],
) -> Optional[tuple[float, float, float, float, float, float]]:
    """Compute (minx, miny, maxx, maxy, zmin, zmax) across all item inputs."""
    minx = miny = float("inf")
    maxx = maxy = float("-inf")
    zmin = float("inf")
    zmax = float("-inf")
    found = False
    for item in items:
        geom = item.get("geometry")
        if not geom:
            continue
        try:
            gminx, gminy, gmaxx, gmaxy = shapely.geometry.shape(geom).bounds
        except Exception:
            continue
        found = True
        minx = min(minx, gminx)
        miny = min(miny, gminy)
        maxx = max(maxx, gmaxx)
        maxy = max(maxy, gmaxy)
        props = item.get("properties") or {}
        zmin = min(zmin, float(props.get("zmin", 0.0)))
        zmax = max(zmax, float(props.get("zmax", 0.0)))
    if not found:
        return None
    return (minx, miny, maxx, maxy, zmin, zmax)
