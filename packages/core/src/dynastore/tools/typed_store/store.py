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

"""Backend-agnostic contract for :class:`PersistentModel` persistence."""

from __future__ import annotations

from typing import List, Optional, Protocol, Type, TypeVar, runtime_checkable

from dynastore.tools.typed_store.base import PersistentModel
from dynastore.tools.typed_store.scope import Scope

_T_Model = TypeVar("_T_Model", bound=PersistentModel)


@runtime_checkable
class TypedStore(Protocol[_T_Model]):
    """CRUD over :class:`PersistentModel` subclasses, keyed by ``class_key``.

    Backends are free to implement read resolution with whatever index or
    query strategy they prefer. Semantics every backend MUST honour:

    * :meth:`get` — returns the row whose ``class_key`` matches ``base`` or
      any subclass of ``base``. Exact match wins; otherwise the most
      recently updated subclass row is returned. ``None`` when nothing
      compatible exists at the scope.
    * :meth:`set` — upsert keyed by ``(scope, type(instance).class_key())``.
    * :meth:`delete` — remove the row for a specific class at the scope
      (no cascade to subclasses).
    * :meth:`list` — return every row whose class is a subclass of ``base``
      at the scope, newest first.
    """

    async def get(
        self,
        scope: Scope,
        base: Type[_T_Model],
    ) -> Optional[_T_Model]:
        ...

    async def set(
        self,
        scope: Scope,
        instance: _T_Model,
    ) -> None:
        ...

    async def delete(
        self,
        scope: Scope,
        cls: Type[_T_Model],
    ) -> None:
        ...

    async def list(
        self,
        scope: Scope,
        base: Type[_T_Model],
        limit: int = 100,
        offset: int = 0,
    ) -> List[_T_Model]:
        ...
