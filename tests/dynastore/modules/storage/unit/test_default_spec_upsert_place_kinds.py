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

"""Regression #3222: place-only kinds must never reach the main-table upsert.

Since #3194 the default derive spec carries the 7 JSON-FG place statistics,
and they ride the geometries sidecar's ``compute_fields_overlay`` (there is
no separate PLACE sidecar target — the geometries sidecar owns the
``{table}_place`` table). ``compute_derived_fields`` only implements the 2D
geometry kinds and raises ``UnsupportedComputedKind`` for the place family,
whose write path is ``prepare_place_upsert_payload`` →
``compute_place_derived_fields``. Every other overlay consumer filters by
``_PLACE_TABLE_KINDS`` (DDL place-column loop, place payload builder,
``resolve_computed_value``); ``prepare_upsert_payload`` must too, or any
feature with a usable geometry fails to ingest under the default policy.
"""

import json

from dynastore.modules.storage.computed_fields import (
    ComputedField,
    ComputedKind,
    SidecarTarget,
    StatisticStorageMode,
    target_sidecar,
)
from dynastore.modules.storage.driver_config import ItemsWritePolicy
from dynastore.modules.storage.drivers.pg_sidecars.geometries import (
    _PLACE_TABLE_KINDS,
    GeometriesSidecar,
    GeometriesSidecarConfig,
)


_POLYGON = {
    "type": "Polygon",
    "coordinates": [[[0.0, 0.0], [0.0, 2.0], [2.0, 2.0], [2.0, 0.0], [0.0, 0.0]]],
}

_PRISM_3D = {
    "type": "Prism",
    "base": _POLYGON,
    "lower": 0.0,
    "upper": 10.0,
}


def _geom_sidecar(fields: "list[ComputedField]") -> GeometriesSidecar:
    return GeometriesSidecar(
        GeometriesSidecarConfig(compute_fields_overlay=fields)
    )


def _default_policy_overlay() -> "list[ComputedField]":
    """The exact overlay ``ensure_storage`` stamps from the default policy."""
    return [
        cf
        for cf in ItemsWritePolicy().compute
        if cf.storage_mode is not None
        and target_sidecar(cf.kind) == SidecarTarget.GEOMETRY
    ]


def test_default_spec_overlay_upserts_real_geometry_without_raising() -> None:
    """The full default overlay (place kinds included) must not crash."""
    overlay = _default_policy_overlay()
    place_kinds = [f.kind for f in overlay if f.kind in _PLACE_TABLE_KINDS]
    assert place_kinds, "default policy is expected to carry place stats"

    sidecar = _geom_sidecar(overlay)
    payload = sidecar.prepare_upsert_payload({"geometry": _POLYGON}, {"geoid": "g1"})

    assert payload is not None
    # 2D statistics are materialised on the main table…
    assert isinstance(payload["area"], float)
    assert payload["vertex_count"] == 5
    # …place statistics are not: they belong to the {table}_place path.
    for f in overlay:
        if f.kind in _PLACE_TABLE_KINDS:
            assert f.resolved_name not in payload


def test_jsonb_place_kind_stays_out_of_geom_stats_blob() -> None:
    """A JSONB place stat must not be folded into ``geom_stats`` either."""
    sidecar = _geom_sidecar(
        [
            ComputedField(
                kind=ComputedKind.CENTROID, storage_mode=StatisticStorageMode.JSONB
            ),
            ComputedField(
                kind=ComputedKind.Z_RANGE, storage_mode=StatisticStorageMode.JSONB
            ),
        ]
    )
    payload = sidecar.prepare_upsert_payload({"geometry": _POLYGON}, {"geoid": "g1"})

    assert payload is not None
    decoded = json.loads(payload["geom_stats"])
    assert "centroid" in decoded
    assert "z_range" not in decoded


def test_place_payload_builder_still_materialises_place_kinds() -> None:
    """The main-table filter must not starve the place-table path."""
    sidecar = _geom_sidecar(
        [
            ComputedField(
                kind=ComputedKind.Z_RANGE, storage_mode=StatisticStorageMode.COLUMNAR
            )
        ]
    )
    payload = sidecar.prepare_place_upsert_payload(
        {"place": _PRISM_3D}, {"geoid": "g1"}
    )
    assert payload is not None
    assert abs(payload["place_z_range"] - 10.0) < 1e-9
