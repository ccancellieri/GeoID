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

"""Scope types identifying a row's storage location.

A scope carries the minimal coordinates a ``TypedStore`` backend needs to
address a row — nothing about its schema or payload. Backends translate
scopes to physical locations (PG schema + table, ES index, in-memory dict, …).
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Union


@dataclass(frozen=True)
class PlatformScope:
    """Global singleton scope (one row per class_key, platform-wide)."""


@dataclass(frozen=True)
class CatalogScope:
    """Per-tenant scope; rows are addressed by their owning catalog."""

    catalog_id: str


@dataclass(frozen=True)
class CollectionScope:
    """Scope of a single collection inside a catalog."""

    catalog_id: str
    collection_id: str


Scope = Union[PlatformScope, CatalogScope, CollectionScope]
"""Union of all built-in scope types. Backends dispatch on the concrete type."""
