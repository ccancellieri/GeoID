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

"""Tests for statistics-overlay reconciliation (#3155).

``ensure_storage`` stamps the policy's storage-bearing compute entries — all
geometry/place statistics by default since #3194 — onto the geometries
sidecar's ``compute_fields_overlay``, but the sidecar DDL is ``CREATE TABLE IF
NOT EXISTS``: a collection materialised before the default flip never gains
the stat columns. Without reconciliation every ingestion into such a
collection emits the missing columns into its INSERTs and fails with
``UndefinedColumnError`` (42703), turning the out-of-band column backfill into
a hard deploy prerequisite. The guard prunes the persisted overlay down to
physically-present columns so the backfill is optional: rows ingest with the
missing statistics unset, and — because the overlay is re-stamped from the
live policy on every ``ensure_storage`` — statistics resume automatically
once the columns exist.

These tests pin:
* the pure decision helper :func:`reconcile_storage_overlay_to_columns`
  (geometry vs place table routing, COLUMNAR vs JSONB column requirements,
  identity-only entries untouched),
* the default-spec round-trip (the anti-recurrence proof: a legacy table
  drops every default stat; a fully-materialised one keeps them all),
* the async wiring in
  ``ItemsPostgresqlDriver._reconcile_geometry_stats_overlay`` (drop+persist,
  no-op when complete, non-geometries sidecars untouched, fail-open on
  introspection error).
"""

from __future__ import annotations

from dynastore.modules.storage.computed_fields import (
    ComputedField,
    ComputedKind,
    StatisticStorageMode,
    default_derive_spec,
    reconcile_storage_overlay_to_columns,
)
from dynastore.modules.storage.drivers.pg_sidecars.attributes_config import (
    AttributeStorageMode,
    FeatureAttributeSidecarConfig,
)
from dynastore.modules.storage.drivers.pg_sidecars.geometries_config import (
    GeometriesSidecarConfig,
)
from dynastore.modules.storage.drivers.pg_sidecars.registry import SidecarRegistry
from dynastore.modules.storage.drivers.postgresql import ItemsPostgresqlDriver

# The columns every geometries sidecar table has had since before the stats
# default — the shape of a collection materialised pre-#3194.
_LEGACY_GEOM_COLUMNS = {"geoid", "geom", "geom_type", "geometry_hash"}


def _stat(
    kind: ComputedKind,
    mode: StatisticStorageMode = StatisticStorageMode.COLUMNAR,
) -> ComputedField:
    return ComputedField(kind=kind, storage_mode=mode)


# ---------------------------------------------------------------------------
# Pure helper — reconcile_storage_overlay_to_columns
# ---------------------------------------------------------------------------


def test_reconcile_keeps_present_drops_absent_preserving_order() -> None:
    fields = [
        _stat(ComputedKind.AREA),
        _stat(ComputedKind.LENGTH),
        _stat(ComputedKind.VERTEX_COUNT),
    ]
    kept, dropped = reconcile_storage_overlay_to_columns(
        fields, _LEGACY_GEOM_COLUMNS | {"area"}, None
    )
    assert [f.resolved_name for f in kept] == ["area"]
    assert dropped == ["length", "vertex_count"]


def test_reconcile_all_present_drops_nothing() -> None:
    fields = [_stat(ComputedKind.AREA), _stat(ComputedKind.LENGTH)]
    kept, dropped = reconcile_storage_overlay_to_columns(
        fields, _LEGACY_GEOM_COLUMNS | {"area", "length"}, None
    )
    assert [f.resolved_name for f in kept] == ["area", "length"]
    assert dropped == []


def test_reconcile_jsonb_stats_require_geom_stats_column() -> None:
    fields = [_stat(ComputedKind.AREA, StatisticStorageMode.JSONB)]
    kept, dropped = reconcile_storage_overlay_to_columns(
        fields, _LEGACY_GEOM_COLUMNS, None
    )
    assert kept == [] and dropped == ["area"]
    kept, dropped = reconcile_storage_overlay_to_columns(
        fields, _LEGACY_GEOM_COLUMNS | {"geom_stats"}, None
    )
    assert [f.resolved_name for f in kept] == ["area"] and dropped == []


def test_reconcile_place_fields_route_to_place_table() -> None:
    fields = [
        _stat(ComputedKind.AREA),
        _stat(ComputedKind.SURFACE_AREA),
        _stat(ComputedKind.Z_RANGE, StatisticStorageMode.JSONB),
    ]
    geom_cols = _LEGACY_GEOM_COLUMNS | {"area"}

    # Place table absent → every place field drops, geometry stat unaffected.
    kept, dropped = reconcile_storage_overlay_to_columns(fields, geom_cols, None)
    assert [f.resolved_name for f in kept] == ["area"]
    assert dropped == ["surface_area", "z_range"]

    # Place table with the prefixed COLUMNAR column and the JSONB blob.
    kept, dropped = reconcile_storage_overlay_to_columns(
        fields, geom_cols, {"geoid", "place", "place_surface_area", "place_stats"}
    )
    assert [f.resolved_name for f in kept] == ["area", "surface_area", "z_range"]
    assert dropped == []


def test_reconcile_identity_only_entries_are_never_dropped() -> None:
    # storage_mode=None entries (e.g. identity-rule inputs) have no physical
    # column to verify — they must pass through untouched even on a table
    # with no stat columns at all.
    fields = [
        ComputedField(kind=ComputedKind.GEOHASH, resolution=7),
        _stat(ComputedKind.AREA),
    ]
    kept, dropped = reconcile_storage_overlay_to_columns(
        fields, _LEGACY_GEOM_COLUMNS, None
    )
    assert [f.resolved_name for f in kept] == ["geohash_7"]
    assert dropped == ["area"]


def test_reconcile_empty_overlay() -> None:
    kept, dropped = reconcile_storage_overlay_to_columns([], {"geoid"}, None)
    assert kept == [] and dropped == []


# ---------------------------------------------------------------------------
# Default-spec round-trip (the anti-recurrence proof)
# ---------------------------------------------------------------------------


def test_default_spec_on_legacy_table_drops_every_stat() -> None:
    """A collection materialised before #3194 has none of the default stat
    columns: the entire default overlay must reconcile away rather than
    surface in INSERT/SELECT column lists."""
    fields = [
        f for f in default_derive_spec().to_computed_fields()
        if f.storage_mode is not None
    ]
    assert fields  # the default is non-empty by design
    kept, dropped = reconcile_storage_overlay_to_columns(
        fields, _LEGACY_GEOM_COLUMNS, None
    )
    assert kept == []
    assert sorted(dropped) == sorted(f.resolved_name for f in fields)


def test_default_spec_on_materialised_tables_keeps_every_stat() -> None:
    """After the #3155 backfill (or a fresh provision) every column exists —
    nothing may be dropped, so statistics flow end-to-end."""
    from dynastore.modules.storage.computed_fields import _PLACE_TABLE_KINDS

    fields = [
        f for f in default_derive_spec().to_computed_fields()
        if f.storage_mode is not None
    ]
    geom_cols = set(_LEGACY_GEOM_COLUMNS)
    place_cols = {"geoid", "place", "coordRefSys"}
    for f in fields:
        if f.kind in _PLACE_TABLE_KINDS:
            place_cols.add(f"place_{f.resolved_name}")
        else:
            geom_cols.add(f.resolved_name)
    kept, dropped = reconcile_storage_overlay_to_columns(fields, geom_cols, place_cols)
    assert dropped == []
    assert [f.resolved_name for f in kept] == [f.resolved_name for f in fields]


# ---------------------------------------------------------------------------
# Async wiring — ItemsPostgresqlDriver._reconcile_geometry_stats_overlay
# ---------------------------------------------------------------------------


def _geom_sidecar(*fields: ComputedField) -> GeometriesSidecarConfig:
    return GeometriesSidecarConfig(compute_fields_overlay=list(fields))


def _patch_introspection(monkeypatch, *, tables, raises=None):
    """Stub the read-only introspection: ``tables`` maps table name → column
    set; a table missing from the map does not exist."""
    import dynastore.modules.db_config.shared_queries as shared_queries
    import dynastore.modules.db_config.query_executor as qe

    class _TableExists:
        async def execute(self, conn, *, schema, table):  # noqa: ANN001
            if raises is not None:
                raise raises
            return table in tables

    class _FakeDQL:
        def __init__(self, sql, result_handler=None):  # noqa: ANN001
            pass

        async def execute(self, conn, *, schema, table):  # noqa: ANN001
            return [{"column_name": c} for c in tables[table]]

    monkeypatch.setattr(shared_queries, "table_exists_query", _TableExists())
    monkeypatch.setattr(qe, "DQLQuery", _FakeDQL)


async def _run(cfg, impl):
    return await ItemsPostgresqlDriver._reconcile_geometry_stats_overlay(
        object(),  # method does not use ``self``
        object(),  # conn
        schema="public",
        physical_table="items_x",
        sidecar_config=cfg,
        sidecar_impl=impl,
        catalog_id="datamgr02",
        collection_id="region",
    )


async def test_method_drops_absent_stats_and_persists_subset(monkeypatch):
    _patch_introspection(
        monkeypatch,
        tables={"items_x_geometries": _LEGACY_GEOM_COLUMNS | {"area"}},
    )
    cfg = _geom_sidecar(_stat(ComputedKind.AREA), _stat(ComputedKind.LENGTH))
    impl = SidecarRegistry.get_sidecar(cfg)

    out = await _run(cfg, impl)
    assert [f.resolved_name for f in out.compute_fields_overlay] == ["area"]


async def test_method_noop_when_all_columns_present(monkeypatch):
    _patch_introspection(
        monkeypatch,
        tables={"items_x_geometries": _LEGACY_GEOM_COLUMNS | {"area", "length"}},
    )
    cfg = _geom_sidecar(_stat(ComputedKind.AREA), _stat(ComputedKind.LENGTH))
    impl = SidecarRegistry.get_sidecar(cfg)

    out = await _run(cfg, impl)
    # Unchanged config object returned (no spurious copy when nothing dropped).
    assert out is cfg


async def test_method_only_introspects_place_table_when_needed(monkeypatch):
    # Geometry-only overlay + place table absent from the stub: reaching the
    # place introspection would KeyError inside _FakeDQL — prove it doesn't.
    _patch_introspection(
        monkeypatch,
        tables={"items_x_geometries": _LEGACY_GEOM_COLUMNS | {"area"}},
    )
    cfg = _geom_sidecar(_stat(ComputedKind.AREA))
    impl = SidecarRegistry.get_sidecar(cfg)

    out = await _run(cfg, impl)
    assert out is cfg


async def test_method_drops_place_stats_when_place_table_absent(monkeypatch):
    _patch_introspection(
        monkeypatch,
        tables={"items_x_geometries": _LEGACY_GEOM_COLUMNS | {"area"}},
    )
    cfg = _geom_sidecar(_stat(ComputedKind.AREA), _stat(ComputedKind.SURFACE_AREA))
    impl = SidecarRegistry.get_sidecar(cfg)

    out = await _run(cfg, impl)
    assert [f.resolved_name for f in out.compute_fields_overlay] == ["area"]


async def test_method_noop_on_non_geometries_sidecar(monkeypatch):
    # Attributes sidecars have their own reconciler — this one must not even
    # introspect. Make introspection explode to prove it is never reached.
    _patch_introspection(monkeypatch, tables={}, raises=AssertionError("introspected"))
    cfg = FeatureAttributeSidecarConfig(storage_mode=AttributeStorageMode.JSONB)
    impl = SidecarRegistry.get_sidecar(cfg)

    out = await _run(cfg, impl)
    assert out is cfg


async def test_method_noop_when_overlay_has_no_storage_fields(monkeypatch):
    _patch_introspection(monkeypatch, tables={}, raises=AssertionError("introspected"))
    cfg = _geom_sidecar(ComputedField(kind=ComputedKind.GEOHASH, resolution=7))
    impl = SidecarRegistry.get_sidecar(cfg)

    out = await _run(cfg, impl)
    assert out is cfg


async def test_method_fails_open_on_introspection_error(monkeypatch):
    # If introspection itself errors we cannot prove a column is absent — keep
    # the config intact rather than over-dropping a healthy collection.
    _patch_introspection(monkeypatch, tables={}, raises=RuntimeError("boom"))
    cfg = _geom_sidecar(_stat(ComputedKind.AREA))
    impl = SidecarRegistry.get_sidecar(cfg)

    out = await _run(cfg, impl)
    assert out is cfg
