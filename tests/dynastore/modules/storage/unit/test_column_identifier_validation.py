"""Column-name validation + case-preserving DDL for the attributes sidecar.

Regression coverage for geoid #719: a collection configured with mixed-case
field names (``Shape_Leng``, ``Area``, ``validOn``) created the physical PG
columns *unquoted* — Postgres folded them to lowercase — while the upsert path
quoted them, so ingestion failed with ``UndefinedColumn: column "Area" ...``.

Fix: reject non-identifier column names at config-parse time, and quote the
column names in the CREATE TABLE / CREATE INDEX / filter-path SQL so their case
is preserved consistently with the already-quoted DML and SELECT paths.
"""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from dynastore.tools.db import InvalidIdentifierError, validate_column_identifier
from dynastore.modules.storage.drivers.pg_sidecars.attributes import (
    FeatureAttributeSidecar,
)
from dynastore.modules.storage.drivers.pg_sidecars.attributes_config import (
    AttributeIndexType,
    AttributeSchemaEntry,
    FeatureAttributeSidecarConfig,
    PostgresType,
)


# ---------------------------------------------------------------------------
# validate_column_identifier
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("name", ["Shape_Leng", "Area", "validOn", "F_", "_x", "adm2_fr"])
def test_valid_names_pass_unchanged(name: str) -> None:
    assert validate_column_identifier(name) == name


@pytest.mark.parametrize(
    "name",
    ["a b", "a-b", "a.b", "2col", "", "col;drop", 'we"ird', "x" * 64],
)
def test_invalid_names_rejected(name: str) -> None:
    with pytest.raises(InvalidIdentifierError):
        validate_column_identifier(name)


def test_reserved_word_rejected() -> None:
    with pytest.raises(InvalidIdentifierError):
        validate_column_identifier("select")


# ---------------------------------------------------------------------------
# AttributeSchemaEntry.name validator
# ---------------------------------------------------------------------------

def test_schema_entry_preserves_case() -> None:
    entry = AttributeSchemaEntry(name="Area", type=PostgresType.INTEGER)
    assert entry.name == "Area"


def test_schema_entry_rejects_strange_characters() -> None:
    with pytest.raises(ValidationError):
        AttributeSchemaEntry(name="bad name", type=PostgresType.TEXT)


# ---------------------------------------------------------------------------
# DDL quoting — columns are created case-preserved
# ---------------------------------------------------------------------------

def _columnar_sidecar() -> FeatureAttributeSidecar:
    config = FeatureAttributeSidecarConfig(
        attribute_schema=[
            AttributeSchemaEntry(
                name="Area", type=PostgresType.INTEGER, index=AttributeIndexType.BTREE
            ),
            AttributeSchemaEntry(name="Shape_Leng", type=PostgresType.FLOAT),
        ],
    )
    return FeatureAttributeSidecar(config)


def test_ddl_quotes_mixed_case_columns() -> None:
    ddl = _columnar_sidecar().get_ddl("t_test")
    # CREATE TABLE column definitions are quoted (case preserved).
    assert '"Area" ' in ddl
    assert '"Shape_Leng" ' in ddl
    # Unquoted bare tokens (which Postgres would fold to lowercase) are absent.
    assert "Area INTEGER" not in ddl
    # CREATE INDEX references the quoted column.
    assert '("Area")' in ddl


def test_resolve_query_path_quotes_column() -> None:
    expr, alias = _columnar_sidecar().resolve_query_path("Area")
    assert expr == f'{alias}."Area"'


# ---------------------------------------------------------------------------
# JSONB blob column resolution — selecting the whole ``attributes`` blob must
# return the column itself, not a non-existent ``->>'attributes'`` sub-key.
# ---------------------------------------------------------------------------

def _jsonb_sidecar() -> FeatureAttributeSidecar:
    # No attribute_schema -> AUTOMATIC resolves to JSONB.
    return FeatureAttributeSidecar(FeatureAttributeSidecarConfig())


def test_get_dynamic_field_definition_resolves_jsonb_blob_column() -> None:
    sidecar = _jsonb_sidecar()
    col = sidecar.config.jsonb_column_name
    alias = f"sc_{sidecar.sidecar_id}"

    field_def = sidecar.get_dynamic_field_definition(col)

    assert field_def is not None
    # The whole JSONB blob — NOT ``attributes->>'attributes'``.
    assert field_def.sql_expression == f"{alias}.{col}"
    assert field_def.data_type == "jsonb"
