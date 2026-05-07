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

"""In-process registry of :class:`PersistentModel` *classes*.

The registry stores **types**, not instances. ``TypedStore`` backends consult
it at read time to rehydrate a row into its original concrete subclass.
"""

from __future__ import annotations

import hashlib
import json
import logging
from typing import TYPE_CHECKING, Dict, List, Optional, Type, TypeVar

if TYPE_CHECKING:
    from dynastore.tools.typed_store.base import PersistentModel

logger = logging.getLogger(__name__)

_T = TypeVar("_T", bound="PersistentModel")


def compute_schema_id(model_cls: Type["PersistentModel"]) -> str:
    """Content-addressed hash of a Pydantic model's JSON schema.

    The hash is a ``sha256:<hex>`` string derived from the canonical JSON
    encoding of ``cls.model_json_schema()``. Deterministic and independent of
    Python attribute ordering.
    """
    schema = model_cls.model_json_schema()
    canonical = json.dumps(schema, sort_keys=True, separators=(",", ":"))
    digest = hashlib.sha256(canonical.encode("utf-8")).hexdigest()
    return f"sha256:{digest}"


class TypedModelRegistry:
    """Process-wide map from ``class_key`` to ``PersistentModel`` subclass.

    Every :class:`PersistentModel` subclass auto-registers via
    ``__init_subclass__``. Duplicate keys with distinct classes raise at import
    time — the failure is loud and deterministic.
    """

    _by_key: Dict[str, Type["PersistentModel"]] = {}

    @classmethod
    def register(cls, model: Type["PersistentModel"]) -> None:
        key = model.class_key()
        existing = cls._by_key.get(key)
        if existing is not None and existing is not model:
            # Tolerate re-import of the same class via alternate module paths
            # (entry-point scanning can import a module twice); only reject when
            # two genuinely distinct classes collide on the same key.
            if (
                existing.__module__ == model.__module__
                and existing.__qualname__ == model.__qualname__
            ):
                cls._by_key[key] = model
                return
            raise ValueError(
                f"Duplicate class_key {key!r}: "
                f"{existing.__module__}.{existing.__qualname__} vs "
                f"{model.__module__}.{model.__qualname__}"
            )
        cls._by_key[key] = model
        logger.debug("TypedModelRegistry: registered %s -> %s", key, model.__qualname__)

    @classmethod
    def get(cls, key: str) -> Optional[Type["PersistentModel"]]:
        return cls._by_key.get(key)

    @classmethod
    def subclasses_of(cls, base: Type[_T]) -> List[Type[_T]]:
        """Return every registered class that is a subclass of ``base`` (inclusive)."""
        return [m for m in cls._by_key.values() if issubclass(m, base)]

    @classmethod
    def all(cls) -> Dict[str, Type["PersistentModel"]]:
        return dict(cls._by_key)

    @classmethod
    def clear(cls) -> None:
        """Test hook only."""
        cls._by_key.clear()
