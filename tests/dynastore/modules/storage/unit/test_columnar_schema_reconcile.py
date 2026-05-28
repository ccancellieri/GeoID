"""Tests for COLUMNAR attribute_schema reconciliation (#1489).

The write-side bridge (:func:`bridge_schema_to_attribute_sidecar`) force-promotes
every non-geometry items_schema field to an ``AttributeSchemaEntry`` when a
sidecar resolves COLUMNAR-only. ``ensure_storage`` then materialises those as
physical columns via ``CREATE TABLE IF NOT EXISTS`` — a no-op on an
already-materialised collection. So a newly-promoted column never reaches an
existing table, yet the bridged config would still advertise it; the sidecar
then emits ``SELECT "<col>"`` against a column that does not exist →
``UndefinedColumnError`` at read time (the #1491 crash class).

The fix reconciles the persisted config down to physically-present columns
(read-only introspection, never ``ALTER TABLE`` — honouring the no-in-place-DDL
rule). A dropped field degrades to the read-side silent-skip + WARN path; a
fresh (re)provision is the only way to actually add the column.

These tests pin:
* the pure decision helper :func:`reconcile_attribute_schema_to_columns`,
* the bridge→reconcile round-trip (the anti-recurrence proof),
* the async wiring in ``ItemsPostgresqlDriver._reconcile_columnar_attribute_schema``
  (drop+persist, JSONB no-op, fail-open on introspection error).
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from dynastore.models.protocols.field_definition import FieldAccess
from dynastore.modules.storage.drivers.pg_sidecars.attributes_config import (
    AttributeSchemaEntry,
    AttributeStorageMode,
    FeatureAttributeSidecarConfig,
    PostgresType,
)
from dynastore.modules.storage.drivers.pg_sidecars.registry import SidecarRegistry
from dynastore.modules.storage.drivers.postgresql import ItemsPostgresqlDriver
from dynastore.modules.storage.field_constraints import (
    bridge_schema_to_attribute_sidecar,
    reconcile_attribute_schema_to_columns,
)


def _entry(name: str, pg_type: PostgresType = PostgresType.TEXT) -> AttributeSchemaEntry:
    return AttributeSchemaEntry(name=name, type=pg_type)


def _field(data_type: str = "string", required: bool = False) -> MagicMock:
    fd = MagicMock()
    fd.data_type = data_type
    fd.required = required
    fd.unique = False
    fd.default = None
    fd.description = None
    fd.access = FieldAccess.AUTO
    return fd


# ---------------------------------------------------------------------------
# Pure helper — reconcile_attribute_schema_to_columns
# ---------------------------------------------------------------------------


def test_reconcile_keeps_present_drops_absent_preserving_order() -> None:
    entries = [_entry("code"), _entry("START_DATE"), _entry("END_DATE")]
    kept, dropped = reconcile_attribute_schema_to_columns(
        entries, {"code", "geoid"}
    )
    assert [e.name for e in kept] == ["code"]
    assert dropped == ["START_DATE", "END_DATE"]


def test_reconcile_all_present_drops_nothing() -> None:
    entries = [_entry("a"), _entry("b")]
    kept, dropped = reconcile_attribute_schema_to_columns(entries, {"a", "b", "geoid"})
    assert [e.name for e in kept] == ["a", "b"]
    assert dropped == []


def test_reconcile_empty_entries() -> None:
    kept, dropped = reconcile_attribute_schema_to_columns([], {"a"})
    assert kept == []
    assert dropped == []


def test_reconcile_is_case_sensitive() -> None:
    # Sidecar columns are created quoted → PG preserves case. A lowercase
    # physical column must NOT satisfy an upper-case advertised entry.
    entries = [_entry("START_DATE")]
    kept, dropped = reconcile_attribute_schema_to_columns(entries, {"start_date"})
    assert kept == []
    assert dropped == ["START_DATE"]


# ---------------------------------------------------------------------------
# Bridge → reconcile round-trip (the #1489 anti-recurrence proof)
# ---------------------------------------------------------------------------


def test_bridge_promotes_then_reconcile_strips_unmaterialised_column() -> None:
    """A field the bridge promotes under COLUMNAR, but which an existing table
    never physically gained, must be reconciled OUT of the persisted config —
    otherwise the sidecar advertises a SELECT of a non-existent column (500)."""
    schema = MagicMock()
    schema.fields = {
        "code": _field(required=True),       # forces COLUMNAR resolution
        "START_DATE": _field(data_type="datetime"),
        "END_DATE": _field(data_type="datetime"),
    }
    schema.default_access = FieldAccess.AUTO

    sidecar = FeatureAttributeSidecarConfig()  # AUTOMATIC, empty
    bridged = bridge_schema_to_attribute_sidecar(schema, sidecar)
    promoted = {e.name for e in bridged.attribute_schema}
    assert {"code", "START_DATE", "END_DATE"} <= promoted

    # The existing physical table only ever had ``code``.
    physical_cols = {"geoid", "code"}
    kept, dropped = reconcile_attribute_schema_to_columns(
        bridged.attribute_schema, physical_cols
    )
    assert [e.name for e in kept] == ["code"]
    assert sorted(dropped) == ["END_DATE", "START_DATE"]


# ---------------------------------------------------------------------------
# Async wiring — ItemsPostgresqlDriver._reconcile_columnar_attribute_schema
# ---------------------------------------------------------------------------


def _columnar_sidecar(*names: str) -> FeatureAttributeSidecarConfig:
    return FeatureAttributeSidecarConfig(
        storage_mode=AttributeStorageMode.COLUMNAR,
        attribute_schema=[_entry(n) for n in names],
    )


def _patch_introspection(monkeypatch, *, columns, exists=True, raises=None):
    """Stub the read-only introspection used by the reconcile method."""
    import dynastore.modules.db_config.shared_queries as shared_queries
    import dynastore.modules.db_config.query_executor as qe

    class _TableExists:
        async def execute(self, conn, *, schema, table):  # noqa: ANN001
            if raises is not None:
                raise raises
            return exists

    class _FakeDQL:
        def __init__(self, sql, result_handler=None):  # noqa: ANN001
            pass

        async def execute(self, conn, *, schema, table):  # noqa: ANN001
            return [{"column_name": c} for c in columns]

    monkeypatch.setattr(shared_queries, "table_exists_query", _TableExists())
    monkeypatch.setattr(qe, "DQLQuery", _FakeDQL)


async def test_method_drops_absent_columns_and_persists_subset(monkeypatch):
    _patch_introspection(monkeypatch, columns={"geoid", "code"})
    cfg = _columnar_sidecar("code", "START_DATE", "END_DATE")
    impl = SidecarRegistry.get_sidecar(cfg)

    out = await ItemsPostgresqlDriver._reconcile_columnar_attribute_schema(
        object(),  # method does not use ``self``
        object(),  # conn
        schema="public",
        physical_table="items_x",
        sidecar_config=cfg,
        sidecar_impl=impl,
        catalog_id="datamgr02",
        collection_id="region",
    )
    assert [e.name for e in out.attribute_schema] == ["code"]


async def test_method_noop_when_all_columns_present(monkeypatch):
    _patch_introspection(monkeypatch, columns={"geoid", "code", "START_DATE"})
    cfg = _columnar_sidecar("code", "START_DATE")
    impl = SidecarRegistry.get_sidecar(cfg)

    out = await ItemsPostgresqlDriver._reconcile_columnar_attribute_schema(
        object(), object(), schema="public", physical_table="items_x",
        sidecar_config=cfg, sidecar_impl=impl,
        catalog_id="c", collection_id="col",
    )
    # Unchanged config object returned (no spurious copy when nothing dropped).
    assert out is cfg


async def test_method_noop_on_jsonb_sidecar(monkeypatch):
    # JSONB blob catches every field — there is nothing to verify, and the
    # method must not even introspect. Make introspection explode to prove it
    # is never reached.
    _patch_introspection(monkeypatch, columns=set(), raises=AssertionError("introspected"))
    cfg = FeatureAttributeSidecarConfig(storage_mode=AttributeStorageMode.JSONB)
    impl = SidecarRegistry.get_sidecar(cfg)

    out = await ItemsPostgresqlDriver._reconcile_columnar_attribute_schema(
        object(), object(), schema="public", physical_table="items_x",
        sidecar_config=cfg, sidecar_impl=impl,
        catalog_id="c", collection_id="col",
    )
    assert out is cfg


async def test_method_fails_open_on_introspection_error(monkeypatch):
    # If introspection itself errors we cannot prove a column is absent — keep
    # the config intact rather than over-dropping a healthy collection.
    _patch_introspection(monkeypatch, columns=set(), raises=RuntimeError("boom"))
    cfg = _columnar_sidecar("code", "START_DATE")
    impl = SidecarRegistry.get_sidecar(cfg)

    out = await ItemsPostgresqlDriver._reconcile_columnar_attribute_schema(
        object(), object(), schema="public", physical_table="items_x",
        sidecar_config=cfg, sidecar_impl=impl,
        catalog_id="c", collection_id="col",
    )
    assert out is cfg
