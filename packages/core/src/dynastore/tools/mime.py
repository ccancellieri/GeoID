#    Copyright 2025 FAO
#
#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at
#
#        http://www.apache.org/licenses/LICENSE-2.0
#
#    Author: Carlo Cancellieri (ccancellieri@gmail.com)
#    Company: FAO

"""MIME-type ↔ filename-extension helpers.

Stdlib :mod:`mimetypes` covers most of what we need (``application/zip``
→ ``.zip``, ``text/csv`` → ``.csv``, ``application/json`` → ``.json``,
``application/vnd.google-earth.kml+xml`` → ``.kml``, ``image/tiff`` →
``.tiff``, ``application/gml+xml`` → ``.gml``, …).

It does NOT ship the GIS-specific MIMEs we routinely see on uploaded
assets — ``application/geo+json``, ``application/geopackage+sqlite3``,
``application/vnd.flatgeobuf``, ``application/vnd.apache.parquet``,
``application/x-esri-shape``.  Register them once at import time so
every caller can keep using ``mimetypes.guess_extension(...)`` directly.

``application/octet-stream`` is intentionally treated as "unknown" —
stdlib maps it to ``.bin`` which is meaningless for a vector reader.
"""

from __future__ import annotations

import mimetypes
from typing import Optional

# Initialise the system MIME database before adding our overrides so
# stdlib types are not shadowed.
mimetypes.init()

_GEO_MIMES: tuple[tuple[str, str], ...] = (
    ("application/geo+json", ".geojson"),
    ("application/geopackage+sqlite3", ".gpkg"),
    ("application/x-sqlite3", ".gpkg"),
    ("application/vnd.flatgeobuf", ".fgb"),
    ("application/vnd.apache.parquet", ".parquet"),
    ("application/x-parquet", ".parquet"),
    ("application/x-esri-shape", ".shp"),
)

for _mime, _ext in _GEO_MIMES:
    mimetypes.add_type(_mime, _ext)


_AMBIGUOUS_MIMES: frozenset[str] = frozenset({
    "application/octet-stream",
    "binary/octet-stream",
})


def ext_from_content_type(content_type: Optional[str]) -> Optional[str]:
    """Return the filename extension (with leading dot) for *content_type*.

    Returns ``None`` when *content_type* is missing, empty, or too
    generic to map safely (e.g. ``application/octet-stream``).  Any
    parameters after ``;`` (charset, boundary, …) are stripped.
    """
    if not content_type:
        return None
    primary = content_type.split(";", 1)[0].strip().lower()
    if not primary or primary in _AMBIGUOUS_MIMES:
        return None
    return mimetypes.guess_extension(primary)
