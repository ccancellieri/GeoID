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

"""In-memory :class:`TypedStore` implementation.

Backend-free — depends only on Layer 1 primitives.  Use cases:

* DB-less dynastore deployments (ephemeral, test, embedded).
* Unit tests that need a ``TypedStore`` without a live PG server.
* Fallback tier during bootstrap before any backend is wired up.

Rows are held in a simple ``dict[(scope, class_key)] -> (schema_id, data)``.
Migrations are applied on read just like the PG backend, keeping semantics
identical across implementations.
"""

from __future__ import annotations

import time
from typing import Any, Dict, List, Optional, Tuple, Type

from dynastore.tools.typed_store.base import PersistentModel
from dynastore.tools.typed_store.migrations import migrate
from dynastore.tools.typed_store.registry import TypedModelRegistry
from dynastore.tools.typed_store.scope import Scope


class InMemoryTypedStore:
    """Process-local :class:`TypedStore` implementation.

    Satisfies the ``TypedStore`` protocol structurally.  Not thread-safe;
    callers that need cross-task safety should wrap it in their own lock.
    """

    def __init__(self) -> None:
        # (scope, class_key) -> (schema_id, payload, updated_at)
        self._rows: Dict[Tuple[Scope, str], Tuple[str, dict, float]] = {}

    async def get(
        self, scope: Scope, base: Type[PersistentModel]
    ) -> Optional[PersistentModel]:
        exact_key = base.class_key()
        keys = {m.class_key() for m in TypedModelRegistry.subclasses_of(base)}
        keys.add(exact_key)

        candidates = [
            (k, v) for (s, k), v in self._rows.items() if s == scope and k in keys
        ]
        if not candidates:
            return None
        # Exact match wins; otherwise newest updated_at
        candidates.sort(
            key=lambda item: (item[0] == exact_key, item[1][2]), reverse=True
        )
        class_key, (schema_id, payload, _ts) = candidates[0]
        cls = TypedModelRegistry.get(class_key)
        if cls is None:
            return None
        data = dict(payload)
        if schema_id != cls.schema_id():
            data = migrate(data, source=schema_id, target=cls.schema_id())
        return cls.model_validate(data)

    async def set(self, scope: Scope, instance: PersistentModel) -> None:
        cls = type(instance)
        self._rows[(scope, cls.class_key())] = (
            cls.schema_id(),
            instance.model_dump(mode="json"),
            time.time(),
        )

    async def delete(self, scope: Scope, cls: Type[PersistentModel]) -> None:
        self._rows.pop((scope, cls.class_key()), None)

    async def list(
        self,
        scope: Scope,
        base: Type[PersistentModel],
        limit: int = 100,
        offset: int = 0,
    ) -> List[PersistentModel]:
        keys = {m.class_key() for m in TypedModelRegistry.subclasses_of(base)}
        matches = sorted(
            [
                (k, v)
                for (s, k), v in self._rows.items()
                if s == scope and k in keys
            ],
            key=lambda item: item[1][2],
            reverse=True,
        )
        out: List[PersistentModel] = []
        for class_key, (schema_id, payload, _ts) in matches[offset : offset + limit]:
            cls = TypedModelRegistry.get(class_key)
            if cls is None:
                continue
            data = dict(payload)
            if schema_id != cls.schema_id():
                data = migrate(data, source=schema_id, target=cls.schema_id())
            out.append(cls.model_validate(data))
        return out

    def clear(self) -> None:
        """Test hook."""
        self._rows.clear()
