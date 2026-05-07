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

"""Legacy fiona-backed reader, registered as a fallback.

Pre-existing call sites used :func:`fiona.open` directly.  This reader
preserves backward-compatibility with any niche driver only fiona's
bundled GDAL ships (rare — but cheap to keep registered as a tail
candidate).  ``priority=100`` puts it strictly behind
:class:`GdalOsgeoReader` (priority=10).
"""

from __future__ import annotations

import contextlib
import logging
from typing import Any, ClassVar, Iterable, Iterator, Tuple

# Hard-import gates registration.
import fiona  # noqa: F401

from .base import SourceReaderProtocol, register_reader

logger = logging.getLogger(__name__)


class FionaReader(SourceReaderProtocol):
    """Fallback reader that delegates straight to fiona."""

    reader_id: ClassVar[str] = "fiona"
    priority: ClassVar[int] = 100
    extensions: ClassVar[Tuple[str, ...]] = (
        # Legacy match-anything-with-an-extension is unsafe; keep the
        # set small and overlap with osgeo intentionally so an explicit
        # ``hint=fiona`` (future) can pin it.
        ".geojson", ".json", ".gpkg", ".shp", ".csv",
    )

    @contextlib.contextmanager
    def open(
        self,
        uri: str,
        *,
        encoding: str = "utf-8",
        content_type: str | None = None,  # noqa: ARG002 — forwarded by registry, unused here
        **opts: Any,
    ) -> Iterator[Iterable[dict]]:
        with fiona.open(uri, "r", encoding=encoding) as reader:
            logger.info("FionaReader: opened %r via fiona", uri)
            yield reader

    def feature_count(
        self, uri: str, *, content_type: str | None = None,  # noqa: ARG002
    ) -> int | None:
        try:
            with fiona.open(uri, "r") as src:
                return len(src)
        except Exception:  # noqa: BLE001
            return None


register_reader(FionaReader)
