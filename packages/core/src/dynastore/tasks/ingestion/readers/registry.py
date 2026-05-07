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

"""Process-local registry of source readers, ordered by priority."""

from __future__ import annotations

import logging
from typing import List, Type

from .base import SourceReaderProtocol

logger = logging.getLogger(__name__)


class ReaderRegistry:
    """Keeps the ordered list of registered :class:`SourceReaderProtocol`s."""

    _registered: List[Type[SourceReaderProtocol]] = []

    @classmethod
    def register(cls, reader_cls: Type[SourceReaderProtocol]) -> None:
        """Add *reader_cls* if its module imported cleanly.  Duplicate
        registration of the same class is a no-op."""
        if reader_cls in cls._registered:
            return
        cls._registered.append(reader_cls)
        cls._registered.sort(key=lambda c: c.priority)
        logger.info(
            "ReaderRegistry: registered %s (priority=%s, extensions=%s)",
            reader_cls.reader_id or reader_cls.__name__,
            reader_cls.priority, reader_cls.extensions,
        )

    @classmethod
    def list_readers(cls) -> List[Type[SourceReaderProtocol]]:
        return list(cls._registered)

    @classmethod
    def resolve(
        cls, uri: str, *, content_type: str | None = None,
    ) -> Type[SourceReaderProtocol]:
        """Highest-priority reader (= lowest priority value) whose
        ``can_read(uri, content_type=...)`` is True.  Raises
        :class:`LookupError` with a list of every candidate considered
        to keep diagnostics actionable.

        *content_type* is forwarded to each reader's ``can_read`` so
        readers can honour a MIME hint when the URI suffix is unknown.
        """
        for reader_cls in cls._registered:
            try:
                if reader_cls.can_read(uri, content_type=content_type):
                    return reader_cls
            except Exception as exc:  # noqa: BLE001 — defensive
                logger.warning(
                    "ReaderRegistry: %s.can_read(%r) raised %s; skipping",
                    reader_cls.reader_id or reader_cls.__name__, uri, exc,
                )
        considered = [c.describe() for c in cls._registered]
        raise LookupError(
            f"No registered reader matches URI {uri!r} "
            f"(content_type={content_type!r}).  "
            f"Considered: {considered or '<empty registry>'}"
        )

    @classmethod
    def clear(cls) -> None:
        """Test-isolation hook."""
        cls._registered.clear()
