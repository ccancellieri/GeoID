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

"""Identifier quoting for columnar attribute columns.

Regression for the MVT tile 500 ``column sc_attributes.code does not exist``:
columnar attribute columns are created with quoted, case-preserving DDL
(``"CODE" TEXT``), so every query reference must also be quoted. The feature
read path quotes via ``get_select_fields`` (and works), but the explicit-field
path used by MVT tile generation resolves ``FieldDefinition.sql_expression``
from ``get_queryable_fields`` / ``get_field_definitions``. Those built the
expression unquoted (``sc_attributes.CODE``), which PostgreSQL folds to
``sc_attributes.code`` and fails for any column whose name is not all-lowercase.
"""

from dynastore.modules.storage.drivers.pg_sidecars.attributes import (
    FeatureAttributeSidecar,
)
from dynastore.modules.storage.drivers.pg_sidecars.attributes_config import (
    AttributeSchemaEntry,
    AttributeStorageMode,
    FeatureAttributeSidecarConfig,
    PostgresType,
)


def _columnar_sidecar(*names: str) -> FeatureAttributeSidecar:
    cfg = FeatureAttributeSidecarConfig(
        storage_mode=AttributeStorageMode.COLUMNAR,
        attribute_schema=[
            AttributeSchemaEntry(name=n, type=PostgresType.TEXT) for n in names
        ],
    )
    return FeatureAttributeSidecar(cfg)


def test_queryable_fields_quote_uppercase_attribute_column() -> None:
    fields = _columnar_sidecar("CODE").get_queryable_fields()
    assert fields["CODE"].sql_expression == 'sc_attributes."CODE"'


def test_field_definitions_quote_uppercase_attribute_column() -> None:
    fields = _columnar_sidecar("CODE").get_field_definitions(sidecar_alias="sc_attributes")
    assert fields["CODE"].sql_expression == 'sc_attributes."CODE"'


def test_queryable_fields_quote_mixed_case_column() -> None:
    fields = _columnar_sidecar("Adm0_Name").get_queryable_fields()
    assert fields["Adm0_Name"].sql_expression == 'sc_attributes."Adm0_Name"'
