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

"""Generic typed persistence primitive.

Domain-agnostic building block used to persist *instances* of any
``PersistentModel`` subclass. Every registered class is identified by a stable
``class_key`` (``__qualname__`` by default, or a pinned ``_class_key`` ClassVar)
and a content-addressed ``schema_id`` (sha256 of the Pydantic JSON schema).

Layering:

* **Layer 1 — this package.** Generic registry + model + protocol + scopes.
* Layer 2 (``modules/db_config``) — ``PluginConfig`` extends ``PersistentModel``
  and adds waterfall resolution.
* Layer 3 (``modules/catalog``) — ``CatalogStore`` / ``CollectionStore`` /
  ``AssetStore`` are domain specialisations.
"""

from dynastore.tools.typed_store.base import PersistentModel
from dynastore.tools.typed_store.memory import InMemoryTypedStore
from dynastore.tools.typed_store.migrations import migrate, migrates
from dynastore.tools.typed_store.registry import TypedModelRegistry, compute_schema_id
from dynastore.tools.typed_store.scope import (
    CatalogScope,
    CollectionScope,
    PlatformScope,
    Scope,
)
from dynastore.tools.typed_store.store import TypedStore

__all__ = [
    "CatalogScope",
    "CollectionScope",
    "InMemoryTypedStore",
    "PersistentModel",
    "PlatformScope",
    "Scope",
    "TypedModelRegistry",
    "TypedStore",
    "compute_schema_id",
    "migrate",
    "migrates",
]
