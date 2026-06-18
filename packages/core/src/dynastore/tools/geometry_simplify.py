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

"""
Geometry simplification tool shared by Elasticsearch drivers.

Ensures a JSON-serialized document fits under a given byte budget
(Elasticsearch's per-doc limit is 10 MB). The geometry is simplified
iteratively with an adaptive tolerance chosen from the observed
size ratio; after `max_iterations` attempts the geometry is replaced
with its bounding box as a hard floor.

The caller receives a `simplification_factor` (final/original byte
ratio) and a `simplification_mode` so the persisted document can
record how much fidelity was lost.

Geometry policy (issue #1248, revised 2026-06-18)
=================================================

Simplification is **on by default** for Elasticsearch items drivers. ES indexes
a simplified copy of any geometry that exceeds the byte budget; the PostgreSQL
primary always keeps full resolution. A collection may opt out by setting
``simplify_geometry: false`` on its ES items driver config, in which case an
oversized geometry is rejected up-front by the ``item_service.upsert`` pre-write
guard (HTTP 422) rather than truncated.
"""

from typing import Any, Tuple

import orjson
from shapely.geometry import box, shape, mapping

from dynastore.tools.json import orjson_default


DEFAULT_MAX_BYTES = 10_000_000
DEFAULT_MAX_ITERATIONS = 8

MODE_NONE = "none"
MODE_TOLERANCE = "tolerance"
MODE_BBOX = "bbox"


def _doc_size(doc: dict) -> int:
    return len(orjson.dumps(doc, default=orjson_default))


def _bbox_diagonal(geom) -> float:
    minx, miny, maxx, maxy = geom.bounds
    dx = maxx - minx
    dy = maxy - miny
    diag = (dx * dx + dy * dy) ** 0.5
    return diag if diag > 0 else 1.0


def simplify_to_fit(
    doc: dict,
    *,
    max_bytes: int = DEFAULT_MAX_BYTES,
    max_iterations: int = DEFAULT_MAX_ITERATIONS,
    geometry_key: str = "geometry",
) -> Tuple[dict, float, str]:
    """
    Return `(doc, factor, mode)` with the document guaranteed to
    serialize under `max_bytes` (when possible).

    - `factor` = final_size / original_size (1.0 = unchanged; lower =
      more simplified; 0.0 = geometry replaced by bbox).
    - `mode` ∈ {"none", "tolerance", "bbox"}.

    The input `doc` is mutated in place and also returned for chaining.
    If the document has no geometry or the geometry cannot be parsed,
    the doc is returned unchanged with `mode="none"` and `factor=1.0`.
    """
    original_size = _doc_size(doc)
    if original_size <= max_bytes:
        return doc, 1.0, MODE_NONE

    geom_raw = doc.get(geometry_key)
    if not geom_raw:
        return doc, 1.0, MODE_NONE

    try:
        geom = shape(geom_raw)
    except Exception:
        return doc, 1.0, MODE_NONE

    diag = _bbox_diagonal(geom)

    # Adaptive tolerance seed: scale by how far we exceed the budget.
    # A geometry that is 2x too large needs a larger tolerance than
    # one that is 10% over. Keep the seed conservative (1e-4 of diag
    # for mild oversize) and scale linearly with the excess ratio.
    excess = original_size / max_bytes
    tolerance = diag * 1e-4 * excess

    low = 0.0                # known-too-small tolerance (doc still too big)
    high: float | None = None  # known-too-large tolerance (doc under budget)
    best_doc = None
    best_size = original_size

    for _ in range(max_iterations):
        simplified = geom.simplify(tolerance, preserve_topology=True)
        if simplified.is_empty:
            break
        doc[geometry_key] = mapping(simplified)
        size = _doc_size(doc)
        if size <= max_bytes:
            high = tolerance
            best_doc = dict(doc)
            best_size = size
            # Try a smaller tolerance to preserve more fidelity.
            tolerance = (low + tolerance) / 2 if low > 0 else tolerance / 2
        else:
            low = tolerance
            # Predict the multiplier needed to hit the budget.
            # size scales ~linearly with vertex count; vertex count
            # shrinks ~inversely with tolerance → bump tolerance by
            # (size / max_bytes), with a floor of 2x to guarantee
            # progress.
            multiplier = max(size / max_bytes, 2.0)
            if high is not None:
                tolerance = (tolerance + high) / 2
            else:
                tolerance = tolerance * multiplier

    if best_doc is not None:
        # Restore the best under-budget attempt.
        doc.clear()
        doc.update(best_doc)
        factor = best_size / original_size if original_size else 1.0
        return doc, factor, MODE_TOLERANCE

    # Fallback: bbox polygon. Always fits (5 coordinate pairs).
    doc[geometry_key] = mapping(box(*geom.bounds))
    return doc, 0.0, MODE_BBOX


def maybe_simplify_for_es(
    doc: dict,
    *,
    simplify: bool,
    max_bytes: int = DEFAULT_MAX_BYTES,
    max_iterations: int = DEFAULT_MAX_ITERATIONS,
    geometry_key: str = "geometry",
) -> Tuple[dict, float, str]:
    """Opt-in wrapper around :func:`simplify_to_fit` for ES write paths.

    Issue #1248 (revised 2026-06-18): ES simplifies geometry by default.
    Simplification is gated by the driver's ``simplify_geometry`` config flag
    (or an equivalent routing hint) which the caller resolves and passes as
    ``simplify``; that flag now defaults to ``True`` for the ES items drivers.

    - ``simplify=True`` (default for the ES items drivers): delegate to
      :func:`simplify_to_fit` so the doc is shrunk to fit the ES per-document
      byte budget. The PostgreSQL primary keeps full resolution.
    - ``simplify=False`` (explicit opt-out): return the document untouched
      with ``(doc, 1.0, MODE_NONE)``. Exact geometry is indexed; oversized
      geometries are rejected up-front by the ``item_service.upsert``
      pre-write guard rather than truncated here.

    The input ``doc`` is returned for chaining (mutated in place only when
    simplification actually runs).
    """
    if not simplify:
        return doc, 1.0, MODE_NONE
    return simplify_to_fit(
        doc,
        max_bytes=max_bytes,
        max_iterations=max_iterations,
        geometry_key=geometry_key,
    )


def geometry_geojson_size(geometry: Any) -> int:
    """Return the GeoJSON-serialized byte size of a geometry.

    Used by the ``item_service.upsert`` pre-write guard (issue #1248) to
    decide whether an item's geometry busts the ES per-document limit.

    The measurement is the geometry alone (not the whole STAC item):
    Elasticsearch's per-document size is dominated by the geometry payload
    and the 10 MB threshold (:data:`DEFAULT_MAX_BYTES`) is an ES-specific
    constraint, so measuring the geometry's GeoJSON serialization is the
    stable, driver-shape-independent signal. ``None`` / empty geometry
    measures as 0 bytes (PG-only catalogs and point geometries never trip
    the guard).
    """
    if not geometry:
        return 0
    return len(orjson.dumps(geometry, default=orjson_default))
