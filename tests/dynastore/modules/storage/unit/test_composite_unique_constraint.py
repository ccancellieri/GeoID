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

"""Unit tests for composite (2+ column) ``UniqueConstraint`` support.

Covers:
- ``bridge_schema_to_attribute_sidecar`` bridging a composite constraint
  onto the attributes sidecar config (as ``composite_unique_constraints``).
- ``FeatureAttributeSidecar.get_ddl`` emitting a composite ``CREATE UNIQUE
  INDEX`` for bridged constraints.
- ``check_unique`` service-layer in-batch rejection of duplicate composite
  tuples.
"""

from __future__ import annotations

import pytest

from dynastore.models.protocols.field_definition import FieldDefinition
from dynastore.modules.storage.driver_config import ItemsSchema
from dynastore.modules.storage.drivers.pg_sidecars.attributes import (
    FeatureAttributeSidecar,
)
from dynastore.modules.storage.drivers.pg_sidecars.attributes_config import (
    AttributeStorageMode,
    FeatureAttributeSidecarConfig,
)
from dynastore.modules.storage.errors import UniqueConstraintViolationError
from dynastore.modules.storage.field_constraints import (
    bridge_schema_to_attribute_sidecar,
    check_unique,
)
from dynastore.modules.storage.schema_types import UniqueConstraint


def _schema() -> ItemsSchema:
    return ItemsSchema(
        fields={
            "name": FieldDefinition(name="name", data_type="string"),
            "code": FieldDefinition(name="code", data_type="string"),
        },
        constraints=[UniqueConstraint(field_names=["name", "code"])],
    )


# ---------------------------------------------------------------------------
# bridge_schema_to_attribute_sidecar
# ---------------------------------------------------------------------------


class TestBridgeCompositeUnique:
    def test_composite_constraint_bridged_onto_sidecar(self, caplog):
        with caplog.at_level("WARNING", logger="dynastore.modules.storage.field_constraints"):
            bridged = bridge_schema_to_attribute_sidecar(
                _schema(), FeatureAttributeSidecarConfig()
            )
        assert getattr(bridged, "composite_unique_constraints", None) == [
            ["name", "code"]
        ]
        # Every field materialized as a column -> no drop -> no warning.
        assert not any(
            "cannot be enforced" in r.message for r in caplog.records
        )

    def test_single_field_unique_constraint_not_bridged_as_composite(self):
        schema = ItemsSchema(
            fields={"name": FieldDefinition(name="name", data_type="string")},
        )
        bridged = bridge_schema_to_attribute_sidecar(
            schema, FeatureAttributeSidecarConfig()
        )
        assert not getattr(bridged, "composite_unique_constraints", None)

    def test_composite_constraint_skipped_when_columns_not_materialized(self, caplog):
        """Explicit JSONB mode: fields stay in the blob (no columns), so a
        composite index over them cannot be emitted — bridge skips it and
        must warn loudly that composite uniqueness is NOT enforced (#2650)."""
        schema = _schema()
        with caplog.at_level("WARNING", logger="dynastore.modules.storage.field_constraints"):
            bridged = bridge_schema_to_attribute_sidecar(
                schema,
                FeatureAttributeSidecarConfig(storage_mode=AttributeStorageMode.JSONB),
            )
        assert not getattr(bridged, "composite_unique_constraints", None)
        warnings = [r for r in caplog.records if r.levelname == "WARNING"]
        assert len(warnings) == 1
        message = warnings[0].message
        assert "name" in message and "code" in message
        assert "cannot be enforced" in message
        assert "NOT enforced" in message


# ---------------------------------------------------------------------------
# DDL emission
# ---------------------------------------------------------------------------


class TestCompositeUniqueDdl:
    def test_ddl_contains_composite_unique_index(self):
        config = FeatureAttributeSidecarConfig(
            composite_unique_constraints=[["name", "code"]],
        )
        ddl = FeatureAttributeSidecar(config).get_ddl("t_abc123")
        assert 'CREATE UNIQUE INDEX IF NOT EXISTS "idx_t_abc123_attributes_name_code_uniq"' in ddl
        assert '("name", "code")' in ddl

    def test_ddl_absent_when_no_composite_constraint(self):
        config = FeatureAttributeSidecarConfig()
        ddl = FeatureAttributeSidecar(config).get_ddl("t_abc123")
        assert "_uniq" not in ddl

    def test_end_to_end_bridge_then_ddl(self):
        bridged = bridge_schema_to_attribute_sidecar(
            _schema(), FeatureAttributeSidecarConfig()
        )
        ddl = FeatureAttributeSidecar(bridged).get_ddl("t_abc123")
        assert 'CREATE UNIQUE INDEX IF NOT EXISTS "idx_t_abc123_attributes_name_code_uniq"' in ddl


# ---------------------------------------------------------------------------
# check_unique — service-layer composite enforcement
# ---------------------------------------------------------------------------


class TestCheckUniqueComposite:
    @pytest.mark.asyncio
    async def test_duplicate_composite_tuple_in_batch_raises(self):
        features = [
            {"properties": {"name": "A", "code": "1"}},
            {"properties": {"name": "A", "code": "1"}},
        ]

        async def _exists(field_name, value):
            return False

        with pytest.raises(UniqueConstraintViolationError):
            await check_unique(
                {}, features, exists=_exists,
                constraints=[UniqueConstraint(field_names=["name", "code"])],
            )

    @pytest.mark.asyncio
    async def test_distinct_composite_tuples_pass(self):
        features = [
            {"properties": {"name": "A", "code": "1"}},
            {"properties": {"name": "A", "code": "2"}},
        ]

        async def _exists(field_name, value):
            return False

        await check_unique(
            {}, features, exists=_exists,
            constraints=[UniqueConstraint(field_names=["name", "code"])],
        )  # no raise

    @pytest.mark.asyncio
    async def test_null_component_never_collides(self):
        features = [
            {"properties": {"name": "A"}},
            {"properties": {"name": "A"}},
        ]

        async def _exists(field_name, value):
            return False

        await check_unique(
            {}, features, exists=_exists,
            constraints=[UniqueConstraint(field_names=["name", "code"])],
        )  # code missing on both -> no collision, no raise

    @pytest.mark.asyncio
    async def test_no_constraints_is_a_noop(self):
        async def _exists(field_name, value):
            return False

        await check_unique({}, [], exists=_exists, constraints=None)  # no raise
