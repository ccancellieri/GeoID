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

"""SourceReaderProtocol — the capability contract every reader implements."""

from __future__ import annotations

import logging
from pathlib import PurePosixPath
from typing import Any, ClassVar, ContextManager, Iterable, Optional, Tuple, Type

from dynastore.tools.mime import ext_from_content_type

logger = logging.getLogger(__name__)


def _uri_has_recognisable_suffix(uri: str) -> bool:
    """True iff the URI ends in something that LOOKS like a file extension.

    Used to decide whether a reader should fall back to a MIME hint.
    Falling back when the URI already has a suffix would let a
    higher-priority reader steal a match that semantically belongs to
    another reader (e.g. `.geojson` URI + accidental `application/zip`
    content_type pulling in a zip-only reader).
    """
    last = uri.rsplit("/", 1)[-1].rsplit("?", 1)[0].rsplit("#", 1)[0]
    return bool(PurePosixPath(last).suffix)


class SourceReaderProtocol:
    """Read features from a vector source identified by URI.

    Subclasses register via :func:`register_reader` (or the
    :class:`ReaderRegistry` directly).  ``can_read(uri)`` performs a
    cheap extension/scheme check; the registry picks the highest-priority
    reader whose check passes.

    The reader is opened as a context manager so cleanup of any
    underlying file handle / driver state is deterministic.
    """

    # Lower priority wins (matches existing SidecarRegistry convention).
    priority: ClassVar[int] = 100
    # File extensions this reader is preferred for.  Lower-cased,
    # leading-dot-included ('.parquet', '.shp', '.csv', '.zip').
    extensions: ClassVar[Tuple[str, ...]] = ()
    # Stable identifier surfaced in logs / errors.
    reader_id: ClassVar[str] = ""

    @classmethod
    def can_read(cls, uri: str, *, content_type: Optional[str] = None) -> bool:
        """Cheap match.

        Primary signal: lowercased *uri* ends with one of
        ``cls.extensions``.  When the URI carries **no suffix at all**
        (e.g. uploaded with a bare filename like ``aoi_oasis``), fall
        back to the extension derived from *content_type* — this lets
        ingestion recover legacy bare-URI assets without a re-upload.

        The MIME fallback is intentionally NOT used when the URI has
        a suffix that simply doesn't match this reader: that suffix
        belongs to another reader by design and stealing the match via
        a coincidental content_type would re-introduce the very kind
        of priority confusion the registry is meant to prevent.
        """
        if not cls.extensions:
            return False
        u = uri.lower()
        if any(u.endswith(ext) for ext in cls.extensions):
            return True
        if _uri_has_recognisable_suffix(uri):
            return False
        derived = ext_from_content_type(content_type)
        if derived and derived.lower() in cls.extensions:
            return True
        return False

    @classmethod
    def describe(cls) -> str:
        """Human-readable summary used in registry error messages."""
        return f"{cls.reader_id or cls.__name__}(extensions={cls.extensions})"

    def open(
        self,
        uri: str,
        *,
        encoding: str = "utf-8",
        content_type: Optional[str] = None,
        **opts: Any,
    ) -> ContextManager[Iterable[dict]]:
        """Open the source and return a ctx-mgr yielding dict-shaped records.

        Each record is a plain dict with at least a ``geometry`` key
        (GeoJSON-shaped or WKB-bytes — whatever the downstream
        ``prepare_record_for_upsert`` expects).  Implementations should
        also expose a ``len()``-able if cheap, for progress reporting.

        *content_type* is forwarded so subclasses can wrap zipped sources
        (``/vsizip/``) when the URI lacks a recognisable suffix.
        """
        raise NotImplementedError

    def feature_count(
        self, uri: str, *, content_type: Optional[str] = None,
    ) -> int | None:
        """Best-effort feature count for progress reporting; ``None`` if
        unknown without a full scan.  Default skips the count.

        *content_type* is forwarded so subclasses can wrap zipped sources
        (``/vsizip/``) when the URI lacks a recognisable suffix.
        """
        return None


# ---------------------------------------------------------------------------
# Registry façade — re-exports the singleton's API at module level so the
# call site doesn't need to know about the registry class.
# ---------------------------------------------------------------------------


def register_reader(cls: Type[SourceReaderProtocol]) -> Type[SourceReaderProtocol]:
    """Decorator / function: register *cls* with :class:`ReaderRegistry`."""
    from .registry import ReaderRegistry
    ReaderRegistry.register(cls)
    return cls


def resolve_reader(
    uri: str, *, content_type: Optional[str] = None,
) -> Type[SourceReaderProtocol]:
    """Return the highest-priority registered reader whose ``can_read(uri)``
    matches.  Raises :class:`LookupError` if no reader matches.

    *content_type* is an optional MIME hint used to recover when the
    URI itself carries no recognisable suffix (e.g. an asset uploaded
    with a bare filename).
    """
    from .registry import ReaderRegistry
    return ReaderRegistry.resolve(uri, content_type=content_type)


def _to_vsigs(uri: str) -> str:
    """Translate ``gs://bucket/key`` → ``/vsigs/bucket/key`` for GDAL.

    Many of GDAL's drivers know ``/vsigs/``; fewer know ``gs://``
    natively.  Round-tripping here keeps every GDAL-backed reader
    consistent and lets a future S3 / Azure migration adjust ONE place.
    """
    if uri.startswith("gs://"):
        return "/vsigs/" + uri[len("gs://"):]
    return uri
