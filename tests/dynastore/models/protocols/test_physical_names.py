#    Copyright 2026 FAO
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

"""Unit tests for the PhysicalNameResolver protocol and ResourceKind enum."""
from __future__ import annotations

from typing import ClassVar, FrozenSet, Optional

import pytest


def test_resource_kind_enum_values():
    from dynastore.models.protocols.physical_names import ResourceKind

    assert ResourceKind.SCHEMA.value == "schema"
    assert ResourceKind.ITEMS.value == "items"
    assert ResourceKind.PRIVATE_ITEMS.value == "private_items"
    assert ResourceKind.ASSETS.value == "assets"
    assert ResourceKind.BUCKET.value == "bucket"
    assert ResourceKind.TOPIC.value == "topic"
    assert ResourceKind.SUBSCRIPTION.value == "subscription"
    assert ResourceKind.OBJECT_PREFIX.value == "object_prefix"


def test_resource_kind_all_members():
    from dynastore.models.protocols.physical_names import ResourceKind

    expected = {
        "SCHEMA", "ITEMS", "PRIVATE_ITEMS", "ASSETS",
        "BUCKET", "TOPIC", "SUBSCRIPTION", "OBJECT_PREFIX",
    }
    assert {m.name for m in ResourceKind} == expected


def test_resource_kind_is_str():
    from dynastore.models.protocols.physical_names import ResourceKind

    # ResourceKind inherits from str — can be used directly as a string
    assert isinstance(ResourceKind.SCHEMA, str)
    assert ResourceKind.SCHEMA == "schema"


def test_physical_name_resolver_structural_conformance():
    """A dummy impl satisfying the protocol is recognised as an instance."""
    from dynastore.models.protocols.physical_names import (
        PhysicalNameResolver,
        ResourceKind,
    )

    class _DummyResolver:
        backend: ClassVar[str] = "dummy"
        supported_kinds: ClassVar[FrozenSet[ResourceKind]] = frozenset(
            {ResourceKind.SCHEMA, ResourceKind.ITEMS}
        )

        def physical_name(
            self,
            kind: ResourceKind,
            *,
            catalog_physical_id: str,
            collection_physical_id: Optional[str] = None,
            prefix: Optional[str] = None,
        ) -> str:
            parts = [prefix or "", catalog_physical_id]
            if collection_physical_id:
                parts.append(collection_physical_id)
            parts.append(kind.value)
            return "_".join(p for p in parts if p)

    impl = _DummyResolver()
    assert isinstance(impl, PhysicalNameResolver)

    result = impl.physical_name(
        ResourceKind.SCHEMA,
        catalog_physical_id="s_abc12345",
    )
    assert "s_abc12345" in result
    assert "schema" in result


def test_physical_name_resolver_missing_method_not_conformant():
    """An object without physical_name() is NOT an instance."""
    from dynastore.models.protocols.physical_names import PhysicalNameResolver

    class _NoDuck:
        backend: str = "x"
        supported_kinds: FrozenSet = frozenset()

    assert not isinstance(_NoDuck(), PhysicalNameResolver)


def test_physical_name_resolver_is_runtime_checkable():
    """Protocol must be decorated with @runtime_checkable."""
    from dynastore.models.protocols.physical_names import PhysicalNameResolver
    from typing import Protocol, runtime_checkable

    # isinstance() must not raise TypeError (would if not runtime_checkable)
    try:
        isinstance(object(), PhysicalNameResolver)
    except TypeError as exc:
        pytest.fail(f"PhysicalNameResolver is not runtime_checkable: {exc}")
