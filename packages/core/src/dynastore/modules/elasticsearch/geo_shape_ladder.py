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

"""Degradation ladder for per-doc ``geo_shape`` bulk rejections (#2769).

:func:`~dynastore.modules.elasticsearch.canonical_doc.build_canonical_index_doc`
normalizes every geometry before it is ever submitted (ring winding +
antimeridian split — see
:mod:`dynastore.tools.geometry_normalize`), so most of the rejection class
this module exists for never reaches ES. This module is the reactive
fallback for the remainder: a document ES still rejects (a pole-touching
ring, an antimeridian split that itself produced a residual
self-intersection, etc.) is retried through progressively coarser geometry
rather than being handed straight to the dead-letter/skip path. PostgreSQL —
the write primary — always keeps the untouched original; ES, the search
index, is allowed a coarser footprint in the worst case rather than no
footprint at all.

Rungs, in order:

1. ``orient_validate`` — re-run winding-fix + ``make_valid`` (belt-and-braces:
   catches the case where the doc predates the proactive normalization, or
   normalization's own best-effort output was still rejected).
2. ``antimeridian_split`` — re-run the antimeridian split on the rung-1
   output.
3. ``aggressive_simplify`` — Douglas-Peucker simplify at a deliberately
   coarse tolerance (this rung trades fidelity for searchability; it is not
   the byte-budget simplification in :mod:`dynastore.tools.geometry_simplify`,
   which targets a size limit, not a rejection).
4. ``bbox_envelope`` — bounding-box rectangle; always ES-representable
   (5 coordinate pairs), so this rung never fails to produce a candidate.

Callers resubmit each candidate as a single-document ``index`` call via
:func:`retry_doc_with_ladder`; the first rung ES accepts wins.
"""
from __future__ import annotations

import logging
from typing import Any, Dict, Iterator, Optional, Tuple

from dynastore.tools.geometry_normalize import (
    orient_and_validate,
    split_antimeridian,
)

logger = logging.getLogger(__name__)

try:
    from shapely.geometry import box, mapping, shape

    _SHAPELY_AVAILABLE = True
except ImportError:  # shapely not installed — the ladder degrades to a no-op
    _SHAPELY_AVAILABLE = False


RUNG_ORIENT_VALIDATE = "orient_validate"
RUNG_ANTIMERIDIAN_SPLIT = "antimeridian_split"
RUNG_AGGRESSIVE_SIMPLIFY = "aggressive_simplify"
RUNG_BBOX_ENVELOPE = "bbox_envelope"

# Simplify tolerance as a fraction of the geometry's bbox diagonal. Chosen
# an order of magnitude coarser than the byte-budget simplifier's mild-
# oversize seed (see DEFAULT_SIMPLIFY_TARGET_BYTES in geometry_simplify.py)
# because this rung only runs after three gentler attempts already failed —
# it exists to salvage a searchable footprint, not to preserve fidelity.
_AGGRESSIVE_SIMPLIFY_TOLERANCE_FRACTION = 0.02


def _aggressive_simplify(geometry: Optional[dict]) -> Optional[dict]:
    if not geometry or not _SHAPELY_AVAILABLE:
        return None
    try:
        geom = shape(geometry)
        minx, miny, maxx, maxy = geom.bounds
        diag = ((maxx - minx) ** 2 + (maxy - miny) ** 2) ** 0.5 or 1.0
        simplified = geom.simplify(
            diag * _AGGRESSIVE_SIMPLIFY_TOLERANCE_FRACTION, preserve_topology=True,
        )
        if simplified.is_empty:
            return None
        return mapping(simplified)
    except Exception:
        return None


def _bbox_envelope(geometry: Optional[dict]) -> Optional[dict]:
    if not geometry or not _SHAPELY_AVAILABLE:
        return None
    try:
        geom = shape(geometry)
        return mapping(box(*geom.bounds))
    except Exception:
        return None


_RUNGS: Tuple[Tuple[str, Any], ...] = (
    (RUNG_ORIENT_VALIDATE, orient_and_validate),
    (RUNG_ANTIMERIDIAN_SPLIT, split_antimeridian),
    (RUNG_AGGRESSIVE_SIMPLIFY, _aggressive_simplify),
    (RUNG_BBOX_ENVELOPE, _bbox_envelope),
)


def degrade_rungs(geometry: dict) -> Iterator[Tuple[str, dict]]:
    """Yield ``(rung_name, candidate_geometry)`` in progressively coarser order.

    Each rung is computed from the PREVIOUS rung's output (or the original
    geometry for rung 1) so degradation is cumulative — e.g. the aggressive
    simplify rung runs on the already antimeridian-split geometry, not the
    raw original. A rung that cannot produce a candidate (e.g. shapely
    unavailable, or the antimeridian split is a no-op because the geometry
    does not actually cross the seam) is skipped, not yielded.
    """
    current = geometry
    for name, fn in _RUNGS:
        try:
            candidate = fn(current)
        except Exception:
            candidate = None
        if candidate is not None:
            current = candidate
            yield name, candidate


async def retry_doc_with_ladder(
    es: Any,
    *,
    index_name: str,
    doc_id: str,
    doc: Dict[str, Any],
    reason: str,
    routing: Optional[str] = None,
    geometry_key: str = "geometry",
) -> Tuple[bool, Optional[str]]:
    """Retry one ES-rejected document through the geometry degradation ladder.

    Resubmits *doc* as a single-document ``index`` call once per rung, each
    time replacing ``doc[geometry_key]`` with the rung's coarser candidate.
    The first rung ES accepts (no exception from ``es.index``) wins.

    Returns ``(recovered, rung_name)``. ``recovered=False`` (with
    ``rung_name=None``) means the document has no geometry to degrade (the
    rejection was not geometry-related) or every rung was still rejected —
    either way the original rejection stands and the caller keeps its
    existing skip/dead-letter handling unchanged.

    Never raises: a rung that itself errors (bad geometry, transport
    failure) is treated as "still rejected" and the ladder moves to the
    next rung.
    """
    geometry = doc.get(geometry_key)
    if not geometry:
        return False, None

    params = {"routing": routing} if routing else {}
    for rung_name, candidate in degrade_rungs(geometry):
        candidate_doc = dict(doc)
        candidate_doc[geometry_key] = candidate
        try:
            await es.index(index=index_name, id=doc_id, body=candidate_doc, params=params)
        except Exception as exc:
            logger.debug(
                "geo_shape_ladder: rung=%s still rejected for id=%s index=%s: %s",
                rung_name, doc_id, index_name, exc,
            )
            continue
        return True, rung_name

    return False, None
