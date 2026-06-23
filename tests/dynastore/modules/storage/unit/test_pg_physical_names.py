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

"""Unit tests for PgPhysicalNames.

Verifies:
* Protocol structural conformance.
* SCHEMA kind returns catalog_physical_id verbatim (== physical_schema).
* ITEMS kind returns collection_physical_id verbatim (== physical_table).
* Byte-identity assertions: resolver output equals what the PG driver stores.
* Unsupported kinds and missing required args raise appropriate errors.
"""

from __future__ import annotations

import pytest

from dynastore.models.protocols.physical_names import PhysicalNameResolver, ResourceKind
from dynastore.modules.storage.drivers.postgresql_physical_names import PgPhysicalNames

CATALOG_PHYSICAL_ID = "s_2ka8fbc3"
COLLECTION_PHYSICAL_ID = "t_9xz12345"


class TestPgPhysicalNamesProtocolConformance:
    def test_conforms_to_physical_name_resolver(self):
        assert isinstance(PgPhysicalNames(), PhysicalNameResolver)

    def test_backend_is_pg(self):
        assert PgPhysicalNames.backend == "pg"

    def test_supported_kinds_contains_schema_and_items(self):
        kinds = PgPhysicalNames.supported_kinds
        assert ResourceKind.SCHEMA in kinds
        assert ResourceKind.ITEMS in kinds

    def test_supported_kinds_does_not_contain_other_kinds(self):
        kinds = PgPhysicalNames.supported_kinds
        assert ResourceKind.ASSETS not in kinds
        assert ResourceKind.PRIVATE_ITEMS not in kinds
        assert ResourceKind.BUCKET not in kinds
        assert ResourceKind.TOPIC not in kinds
        assert ResourceKind.SUBSCRIPTION not in kinds
        assert ResourceKind.OBJECT_PREFIX not in kinds


class TestPgPhysicalNamesSchema:
    """SCHEMA → catalog_physical_id verbatim (== historical physical_schema)."""

    def test_schema_returns_catalog_physical_id_verbatim(self):
        resolver = PgPhysicalNames()
        result = resolver.physical_name(
            ResourceKind.SCHEMA,
            catalog_physical_id=CATALOG_PHYSICAL_ID,
        )
        assert result == CATALOG_PHYSICAL_ID

    def test_schema_byte_identical_to_physical_schema(self):
        """physical_schema stored in catalog.catalogs IS the catalog physical_id."""
        resolver = PgPhysicalNames()
        physical_schema = CATALOG_PHYSICAL_ID  # the value the PG driver stores
        result = resolver.physical_name(
            ResourceKind.SCHEMA,
            catalog_physical_id=physical_schema,
        )
        assert result == physical_schema, (
            f"SCHEMA name {result!r} must be byte-identical to physical_schema {physical_schema!r}"
        )

    def test_schema_ignores_collection_physical_id(self):
        resolver = PgPhysicalNames()
        result = resolver.physical_name(
            ResourceKind.SCHEMA,
            catalog_physical_id=CATALOG_PHYSICAL_ID,
            collection_physical_id=COLLECTION_PHYSICAL_ID,
        )
        # Collection id must not appear in a catalog-level name
        assert result == CATALOG_PHYSICAL_ID
        assert COLLECTION_PHYSICAL_ID not in result

    def test_schema_ignores_prefix(self):
        resolver = PgPhysicalNames()
        result = resolver.physical_name(
            ResourceKind.SCHEMA,
            catalog_physical_id=CATALOG_PHYSICAL_ID,
            prefix="ds",
        )
        assert result == CATALOG_PHYSICAL_ID

    def test_schema_empty_physical_id_raises(self):
        resolver = PgPhysicalNames()
        with pytest.raises(ValueError, match="catalog_physical_id must not be empty"):
            resolver.physical_name(
                ResourceKind.SCHEMA,
                catalog_physical_id="",
            )


class TestPgPhysicalNamesItems:
    """ITEMS → collection_physical_id verbatim (== historical physical_table)."""

    def test_items_returns_collection_physical_id_verbatim(self):
        resolver = PgPhysicalNames()
        result = resolver.physical_name(
            ResourceKind.ITEMS,
            catalog_physical_id=CATALOG_PHYSICAL_ID,
            collection_physical_id=COLLECTION_PHYSICAL_ID,
        )
        assert result == COLLECTION_PHYSICAL_ID

    def test_items_byte_identical_to_physical_table(self):
        """physical_table stored in {schema}.collections IS the collection physical_id."""
        resolver = PgPhysicalNames()
        physical_table = COLLECTION_PHYSICAL_ID  # the value the PG driver stores
        result = resolver.physical_name(
            ResourceKind.ITEMS,
            catalog_physical_id=CATALOG_PHYSICAL_ID,
            collection_physical_id=physical_table,
        )
        assert result == physical_table, (
            f"ITEMS name {result!r} must be byte-identical to physical_table {physical_table!r}"
        )

    def test_items_ignores_prefix(self):
        resolver = PgPhysicalNames()
        result = resolver.physical_name(
            ResourceKind.ITEMS,
            catalog_physical_id=CATALOG_PHYSICAL_ID,
            collection_physical_id=COLLECTION_PHYSICAL_ID,
            prefix="ds",
        )
        assert result == COLLECTION_PHYSICAL_ID

    def test_items_requires_collection_physical_id(self):
        resolver = PgPhysicalNames()
        with pytest.raises(ValueError, match="collection_physical_id"):
            resolver.physical_name(
                ResourceKind.ITEMS,
                catalog_physical_id=CATALOG_PHYSICAL_ID,
            )

    def test_items_empty_catalog_physical_id_raises(self):
        resolver = PgPhysicalNames()
        with pytest.raises(ValueError, match="catalog_physical_id must not be empty"):
            resolver.physical_name(
                ResourceKind.ITEMS,
                catalog_physical_id="",
                collection_physical_id=COLLECTION_PHYSICAL_ID,
            )


class TestPgPhysicalNamesErrors:
    def test_unsupported_kind_raises_value_error(self):
        resolver = PgPhysicalNames()
        with pytest.raises(ValueError, match="does not support"):
            resolver.physical_name(
                ResourceKind.ASSETS,
                catalog_physical_id=CATALOG_PHYSICAL_ID,
                collection_physical_id=COLLECTION_PHYSICAL_ID,
            )

    def test_unsupported_kind_bucket_raises(self):
        resolver = PgPhysicalNames()
        with pytest.raises(ValueError, match="BUCKET"):
            resolver.physical_name(
                ResourceKind.BUCKET,
                catalog_physical_id=CATALOG_PHYSICAL_ID,
            )

    def test_unsupported_kind_topic_raises(self):
        resolver = PgPhysicalNames()
        with pytest.raises(ValueError, match="TOPIC"):
            resolver.physical_name(
                ResourceKind.TOPIC,
                catalog_physical_id=CATALOG_PHYSICAL_ID,
            )


class TestPgPhysicalNamesIdentityInvariant:
    """The core invariant: output is byte-identical to the historical PG values."""

    def test_schema_name_does_not_add_prefix_or_suffix(self):
        resolver = PgPhysicalNames()
        for physical_id in ("s_2ka8fbc3", "s_abc12345", "my_catalog"):
            result = resolver.physical_name(
                ResourceKind.SCHEMA,
                catalog_physical_id=physical_id,
            )
            assert result == physical_id, (
                f"SCHEMA for {physical_id!r} must be verbatim, got {result!r}"
            )

    def test_items_name_does_not_add_prefix_or_suffix(self):
        resolver = PgPhysicalNames()
        for collection_id in ("t_9xz12345", "t_abc00001", "my_table"):
            result = resolver.physical_name(
                ResourceKind.ITEMS,
                catalog_physical_id=CATALOG_PHYSICAL_ID,
                collection_physical_id=collection_id,
            )
            assert result == collection_id, (
                f"ITEMS for {collection_id!r} must be verbatim, got {result!r}"
            )
