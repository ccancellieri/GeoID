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

"""Base class for any model that needs to round-trip through a :class:`TypedStore`."""

from __future__ import annotations

import re
from typing import Any, ClassVar, Optional

from pydantic import BaseModel

from dynastore.tools.typed_store.registry import TypedModelRegistry, compute_schema_id


_PASCAL_BOUNDARY_1 = re.compile(r"(.)([A-Z][a-z]+)")
_PASCAL_BOUNDARY_2 = re.compile(r"([a-z0-9])([A-Z])")


def _to_snake(name: str) -> str:
    """PascalCase → snake_case. Handles consecutive caps.

    ``GeometryStorage`` → ``geometry_storage``
    ``DGGSConfig``     → ``dggs_config``         (consecutive caps coalesce)
    ``WFSPluginConfig`` → ``wfs_plugin_config``
    ``ItemsElasticsearchPrivateDriver`` → ``items_elasticsearch_private_driver``
    ``EDRConfig``      → ``edr_config``
    Leading underscores (e.g. ``_DemoDriverA``) survive: ``_demo_driver_a``.
    """
    leading = ""
    body = name
    while body.startswith("_"):
        leading += "_"
        body = body[1:]
    s1 = _PASCAL_BOUNDARY_1.sub(r"\1_\2", body)
    return leading + _PASCAL_BOUNDARY_2.sub(r"\1_\2", s1).lower()


class PersistentModel(BaseModel):
    """Pydantic base for classes persisted via a :class:`TypedStore`.

    Two identities are attached to every subclass:

    * ``class_key()`` — wire identity, **always snake_case**, auto-derived
      from ``cls.__name__``.  Renaming the Python class renames the wire
      key by definition; per-class overrides are not allowed.
    * ``schema_id()`` — sha256 of the canonical JSON schema; content-addressed
      so any field change produces a new id. Computed lazily and cached.

    Auto-registers in :class:`TypedModelRegistry` at class-creation time.
    """

    # Lazily populated by ``schema_id``. Per-class, not per-instance.
    _cached_schema_id: ClassVar[Optional[str]] = None

    def __init_subclass__(cls, **kwargs: Any) -> None:
        super().__init_subclass__(**kwargs)
        # Reset any cached schema id inherited from a parent — different class,
        # different schema.
        cls._cached_schema_id = None
        TypedModelRegistry.register(cls)

    @classmethod
    def class_key(cls) -> str:
        return _to_snake(cls.__name__)

    @classmethod
    def schema_id(cls) -> str:
        cached = cls.__dict__.get("_cached_schema_id")
        if cached is not None:
            return cached
        sid = compute_schema_id(cls)
        cls._cached_schema_id = sid
        return sid
