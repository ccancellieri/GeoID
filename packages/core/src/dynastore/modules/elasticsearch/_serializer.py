#    Copyright 2025 FAO
#
#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at
#
#        http://www.apache.org/licenses/LICENSE-2.0
"""
OpenSearch JSON serializer backed by :class:`dynastore.tools.json.CustomJSONEncoder`.

The stock `opensearchpy.serializer.JSONSerializer` only knows about
datetime / UUID / Decimal / numpy / pandas. It crashed in production on
``providers[*].url: HttpUrl(...)`` when indexing a STAC Collection
(2026-04-22: ``TypeError: Unable to serialize HttpUrl(...)``).

Instead of duplicating type-handling logic, this serializer delegates
`dumps()` to :class:`~dynastore.tools.json.CustomJSONEncoder` — the same
SSOT used for task-input serialization (PR #35) and FastAPI response
rendering. Any type we add support for in one place is picked up
everywhere.
"""
from __future__ import annotations

import json
from typing import Any

from opensearchpy.exceptions import SerializationError
from opensearchpy.serializer import JSONSerializer

from dynastore.tools.json import CustomJSONEncoder

# Strings are passed through untouched by the stock implementation.
# Mirror that here so we remain a drop-in replacement.
_STRING_TYPES = (str, bytes)


class CustomOpenSearchSerializer(JSONSerializer):
    """`JSONSerializer` that dumps through `CustomJSONEncoder`.

    Handles pydantic `BaseModel`, pydantic v2 URL types, `__geo_interface__`
    objects, `set`/`frozenset`, `bytes`, plus everything the stock encoder
    already handles (datetime, UUID, Decimal, …) — via the shared encoder.
    """

    def dumps(self, data: Any) -> Any:
        if isinstance(data, str):
            return data
        try:
            return json.dumps(
                data,
                cls=CustomJSONEncoder,
                ensure_ascii=False,
                separators=(",", ":"),
            )
        except (ValueError, TypeError) as exc:
            raise SerializationError(data, exc)
