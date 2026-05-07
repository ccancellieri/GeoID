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
Style encoding media-type constants + content-negotiation helpers.

The Styles storage model (``StyleSheet`` in ``modules/styles/models.py``)
already carries multiple encodings per style ID via a discriminated
``Union[SLDContent, MapboxContent]``. This module adds the wire-level
media-type constants and the ``Accept`` / ``?f=`` resolvers that the
OGC API - Styles HTTP layer needs.

Spec references:
  - OGC API - Styles Part 1 — stylesheet links
  - OGC_STYLES.md — "Multi-Format Style Endpoints"
"""

from __future__ import annotations

from typing import Iterable, List, Optional, Tuple


# --- IANA / vendor media types (exact strings — drift is a bug). -----------

MEDIA_TYPE_SLD_11 = "application/vnd.ogc.sld+xml;version=1.1"
MEDIA_TYPE_SLD_10 = "application/vnd.ogc.sld+xml"
MEDIA_TYPE_MAPBOX_GL = "application/json"
MEDIA_TYPE_QML = "application/xml"
MEDIA_TYPE_FLAT_STYLE = "application/vnd.ogc.flat-style+json"
MEDIA_TYPE_HTML = "text/html"


# --- Mapping between StyleFormatEnum and media types. ----------------------
#
# ``StyleFormatEnum`` in ``modules/styles/models.py`` today has
# ``MAPBOX_GL`` and ``SLD_1_1``. Additional formats (SLD 1.0, QML, Flat
# Style) can be added there + a new entry below when their content models
# land. This module does not define enum values it cannot persist — that
# would surface at the HTTP layer as encodings that register but never
# serve, which is the "stubs lie" anti-pattern.

STYLE_FORMAT_TO_MEDIA_TYPE = {
    "MapboxGL": MEDIA_TYPE_MAPBOX_GL,
    "SLD_1.1": MEDIA_TYPE_SLD_11,
}


# --- ``?f=`` query-param shortcuts. ---------------------------------------
#
# Keys match the URL-friendly shorthand; values are the full media type.
# The HTTP handler must prefer ``?f=`` over ``Accept`` when both are set
# (``?f=`` is an explicit, unambiguous caller choice).

_F_PARAM_MAP = {
    "mapbox": MEDIA_TYPE_MAPBOX_GL,
    "sld11": MEDIA_TYPE_SLD_11,
    "sld10": MEDIA_TYPE_SLD_10,
    "qml": MEDIA_TYPE_QML,
    "flat": MEDIA_TYPE_FLAT_STYLE,
    "html": MEDIA_TYPE_HTML,
    "json": MEDIA_TYPE_MAPBOX_GL,  # alias — Mapbox GL is JSON
}


def f_param_to_media_type(f: Optional[str]) -> Optional[str]:
    """Resolve a ``?f=`` shorthand to its full media type.

    Returns ``None`` when ``f`` is not a known shorthand (caller should
    fall back to Accept-header negotiation).
    """
    if f is None:
        return None
    return _F_PARAM_MAP.get(f.lower())


# --- ``Accept``-header resolver. ------------------------------------------


def _parse_accept(accept: str) -> List[Tuple[float, str]]:
    """Parse an HTTP ``Accept`` header into ``[(q_value, media_type), ...]``.

    Media type keeps any non-``q`` parameters (e.g., ``version=1.1``)
    attached — they are part of the type identity for our purposes.
    """
    parts: List[Tuple[float, str]] = []
    for raw in accept.split(","):
        tokens = [t.strip() for t in raw.split(";") if t.strip()]
        if not tokens:
            continue
        media_type = tokens[0]
        q = 1.0
        extras = []
        for tok in tokens[1:]:
            if tok.startswith("q="):
                try:
                    q = float(tok[2:])
                except ValueError:
                    q = 0.0
            else:
                extras.append(tok)
        full = media_type if not extras else media_type + ";" + ";".join(extras)
        parts.append((q, full))
    # Sort by descending quality; preserve first-occurrence order within ties.
    parts.sort(key=lambda p: -p[0])
    return parts


def normalize_accept_to_media_type(
    accept: str,
    available_media_types: Iterable[str],
) -> Optional[str]:
    """Pick the best available media type for an ``Accept`` header.

    Returns ``None`` when no available encoding satisfies the header
    (caller should respond 406 Not Acceptable).

    Empty ``Accept`` or ``*/*`` picks the first entry of
    ``available_media_types``.
    """
    available = list(available_media_types)
    if not available:
        return None
    if not accept or accept.strip() == "*/*":
        return available[0]

    parsed = _parse_accept(accept)
    available_set = set(available)
    # 1. Exact match against any available media type (preserves version params).
    for _q, mt in parsed:
        if mt in available_set:
            return mt
    # 2. Base-type match (strip params) — lets ``application/vnd.ogc.sld+xml``
    #    match an available ``application/vnd.ogc.sld+xml;version=1.1``.
    for _q, mt in parsed:
        base = mt.split(";")[0]
        for candidate in available:
            if candidate.split(";")[0] == base:
                return candidate
    return None


__all__ = [
    "MEDIA_TYPE_SLD_11",
    "MEDIA_TYPE_SLD_10",
    "MEDIA_TYPE_MAPBOX_GL",
    "MEDIA_TYPE_QML",
    "MEDIA_TYPE_FLAT_STYLE",
    "MEDIA_TYPE_HTML",
    "STYLE_FORMAT_TO_MEDIA_TYPE",
    "f_param_to_media_type",
    "normalize_accept_to_media_type",
]
