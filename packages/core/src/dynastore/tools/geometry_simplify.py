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
"""

from typing import Any, Tuple

import orjson
from shapely.geometry import box, shape, mapping

from dynastore.tools.json import orjson_default


DEFAULT_MAX_BYTES = 10_000_000
DEFAULT_MAX_ITERATIONS = 3

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
