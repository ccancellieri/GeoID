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

"""Unit tests for EsPhysicalNames and EsPrivatePhysicalNames.

Verifies:
* Protocol structural conformance (PhysicalNameResolver).
* Index names are built from the supplied ``catalog_physical_id``, NOT
  from a logical catalog id — the core P1 invariant.
* Unsupported ResourceKind raises ValueError.
"""

import pytest

from dynastore.models.protocols.physical_names import PhysicalNameResolver, ResourceKind
from dynastore.modules.elasticsearch.physical_names import (
    EsPhysicalNames,
    EsPrivatePhysicalNames,
)

PREFIX = "ds"
PHYSICAL_ID = "s_2ka8fbc3"
LOGICAL_ID = "my-catalog"  # must NEVER appear in index names produced from PHYSICAL_ID


class TestEsPhysicalNamesProtocolConformance:
    def test_conforms_to_physical_name_resolver(self):
        assert isinstance(EsPhysicalNames(), PhysicalNameResolver)

    def test_backend_is_es(self):
        assert EsPhysicalNames.backend == "es"

    def test_supported_kinds_contains_items_and_assets(self):
        assert ResourceKind.ITEMS in EsPhysicalNames.supported_kinds
        assert ResourceKind.ASSETS in EsPhysicalNames.supported_kinds

    def test_supported_kinds_does_not_contain_private_items(self):
        assert ResourceKind.PRIVATE_ITEMS not in EsPhysicalNames.supported_kinds


class TestEsPhysicalNamesItemsIndex:
    def test_items_index_contains_physical_id(self):
        resolver = EsPhysicalNames()
        name = resolver.physical_name(
            ResourceKind.ITEMS,
            catalog_physical_id=PHYSICAL_ID,
            prefix=PREFIX,
        )
        assert PHYSICAL_ID in name
        assert LOGICAL_ID not in name

    def test_items_index_has_expected_shape(self):
        resolver = EsPhysicalNames()
        name = resolver.physical_name(
            ResourceKind.ITEMS,
            catalog_physical_id=PHYSICAL_ID,
            prefix=PREFIX,
        )
        assert name == f"{PREFIX}-{PHYSICAL_ID}-items"

    def test_items_index_ends_with_items(self):
        resolver = EsPhysicalNames()
        name = resolver.physical_name(
            ResourceKind.ITEMS,
            catalog_physical_id=PHYSICAL_ID,
            prefix=PREFIX,
        )
        assert name.endswith("-items")
        assert "private" not in name


class TestEsPhysicalNamesAssetsIndex:
    def test_assets_index_contains_physical_id(self):
        resolver = EsPhysicalNames()
        name = resolver.physical_name(
            ResourceKind.ASSETS,
            catalog_physical_id=PHYSICAL_ID,
            prefix=PREFIX,
        )
        assert PHYSICAL_ID in name
        assert LOGICAL_ID not in name

    def test_assets_index_has_expected_shape(self):
        resolver = EsPhysicalNames()
        name = resolver.physical_name(
            ResourceKind.ASSETS,
            catalog_physical_id=PHYSICAL_ID,
            prefix=PREFIX,
        )
        assert name == f"{PREFIX}-{PHYSICAL_ID}-assets"

    def test_assets_index_ends_with_assets(self):
        resolver = EsPhysicalNames()
        name = resolver.physical_name(
            ResourceKind.ASSETS,
            catalog_physical_id=PHYSICAL_ID,
            prefix=PREFIX,
        )
        assert name.endswith("-assets")


class TestEsPhysicalNamesErrors:
    def test_unsupported_kind_raises_value_error(self):
        resolver = EsPhysicalNames()
        with pytest.raises(ValueError, match="PRIVATE_ITEMS"):
            resolver.physical_name(
                ResourceKind.PRIVATE_ITEMS,
                catalog_physical_id=PHYSICAL_ID,
                prefix=PREFIX,
            )

    def test_empty_physical_id_raises_value_error(self):
        resolver = EsPhysicalNames()
        with pytest.raises(ValueError):
            resolver.physical_name(
                ResourceKind.ITEMS,
                catalog_physical_id="",
                prefix=PREFIX,
            )


class TestEsPrivatePhysicalNamesProtocolConformance:
    def test_conforms_to_physical_name_resolver(self):
        assert isinstance(EsPrivatePhysicalNames(), PhysicalNameResolver)

    def test_backend_is_es_private(self):
        assert EsPrivatePhysicalNames.backend == "es_private"

    def test_supported_kinds_contains_private_items_only(self):
        assert ResourceKind.PRIVATE_ITEMS in EsPrivatePhysicalNames.supported_kinds
        assert ResourceKind.ITEMS not in EsPrivatePhysicalNames.supported_kinds
        assert ResourceKind.ASSETS not in EsPrivatePhysicalNames.supported_kinds


class TestEsPrivatePhysicalNamesPrivateItemsIndex:
    def test_private_items_index_contains_physical_id(self):
        resolver = EsPrivatePhysicalNames()
        name = resolver.physical_name(
            ResourceKind.PRIVATE_ITEMS,
            catalog_physical_id=PHYSICAL_ID,
            prefix=PREFIX,
        )
        assert PHYSICAL_ID in name
        assert LOGICAL_ID not in name

    def test_private_items_index_has_expected_shape(self):
        resolver = EsPrivatePhysicalNames()
        name = resolver.physical_name(
            ResourceKind.PRIVATE_ITEMS,
            catalog_physical_id=PHYSICAL_ID,
            prefix=PREFIX,
        )
        assert name == f"{PREFIX}-{PHYSICAL_ID}-private-items"

    def test_private_items_index_ends_with_private_items(self):
        resolver = EsPrivatePhysicalNames()
        name = resolver.physical_name(
            ResourceKind.PRIVATE_ITEMS,
            catalog_physical_id=PHYSICAL_ID,
            prefix=PREFIX,
        )
        assert name.endswith("-private-items")


class TestEsPrivatePhysicalNamesErrors:
    def test_unsupported_kind_raises_value_error(self):
        resolver = EsPrivatePhysicalNames()
        with pytest.raises(ValueError, match="ITEMS"):
            resolver.physical_name(
                ResourceKind.ITEMS,
                catalog_physical_id=PHYSICAL_ID,
                prefix=PREFIX,
            )

    def test_empty_physical_id_raises_value_error(self):
        resolver = EsPrivatePhysicalNames()
        with pytest.raises(ValueError):
            resolver.physical_name(
                ResourceKind.PRIVATE_ITEMS,
                catalog_physical_id="",
                prefix=PREFIX,
            )


class TestPhysicalIdIsolation:
    """The core invariant: index names reflect physical_id, never logical_id."""

    def test_items_index_for_physical_id_differs_from_logical_id(self):
        resolver = EsPhysicalNames()
        physical_name = resolver.physical_name(
            ResourceKind.ITEMS,
            catalog_physical_id=PHYSICAL_ID,
            prefix=PREFIX,
        )
        logical_name = resolver.physical_name(
            ResourceKind.ITEMS,
            catalog_physical_id=LOGICAL_ID,
            prefix=PREFIX,
        )
        assert physical_name != logical_name
        assert PHYSICAL_ID in physical_name
        assert LOGICAL_ID in logical_name

    def test_private_index_for_physical_id_differs_from_logical_id(self):
        resolver = EsPrivatePhysicalNames()
        physical_name = resolver.physical_name(
            ResourceKind.PRIVATE_ITEMS,
            catalog_physical_id=PHYSICAL_ID,
            prefix=PREFIX,
        )
        logical_name = resolver.physical_name(
            ResourceKind.PRIVATE_ITEMS,
            catalog_physical_id=LOGICAL_ID,
            prefix=PREFIX,
        )
        assert physical_name != logical_name
        assert PHYSICAL_ID in physical_name
        assert LOGICAL_ID in logical_name
