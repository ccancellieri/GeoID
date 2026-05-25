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

"""Regression: JSONB stat blobs must be bound as JSON strings, not raw dicts.

``_upsert_sidecar_table_raw`` passes payload values straight to the driver with
no ``::jsonb`` cast and no jsonb codec (none is registered — see the absence of
``set_type_codec`` in the engine setup). A raw ``dict`` is therefore rejected at
bind time: psycopg2 sync workers raise ``can't adapt type 'dict'`` and asyncpg's
built-in jsonb codec expects ``str`` too. So the geometry sidecar must serialize
``geom_stats`` / ``place_stats`` exactly like the attributes sidecar already
serializes ``attribute_stats``. These tests pin that the prepared payload carries
a JSON *string* that round-trips to the expected blob.
"""

import json

from dynastore.modules.storage.computed_fields import (
    ComputedField,
    ComputedKind,
    StatisticStorageMode,
)
from dynastore.modules.storage.drivers.pg_sidecars.geometries import (
    GeometriesSidecar,
    GeometriesSidecarConfig,
)


_POLYGON = {
    "type": "Polygon",
    "coordinates": [[[0.0, 0.0], [0.0, 2.0], [2.0, 2.0], [2.0, 0.0], [0.0, 0.0]]],
}

_LINESTRING_3D = {
    "type": "LineString",
    "coordinates": [[0.0, 0.0, 10.0], [1.0, 0.0, 30.0], [2.0, 0.0, 20.0]],
}


def _geom_sidecar(*fields: ComputedField) -> GeometriesSidecar:
    return GeometriesSidecar(
        GeometriesSidecarConfig(compute_fields_overlay=list(fields))
    )


def test_geom_stats_jsonb_is_serialized_string() -> None:
    """A JSONB geometry stat lands in ``geom_stats`` as a JSON string."""
    sidecar = _geom_sidecar(
        ComputedField(kind=ComputedKind.CENTROID, storage_mode=StatisticStorageMode.JSONB)
    )
    payload = sidecar.prepare_upsert_payload(
        {"geometry": _POLYGON}, {"geoid": "g1"}
    )
    assert payload is not None
    blob = payload["geom_stats"]
    assert isinstance(blob, str), "geom_stats must be a JSON string, not a raw dict"
    decoded = json.loads(blob)
    assert "centroid" in decoded
    # jsonb centroid is a [lon, lat] coordinate array (~the polygon centre)
    assert decoded["centroid"] == [1.0, 1.0]


def test_place_stats_jsonb_is_serialized_string() -> None:
    """A JSONB 3D place stat lands in ``place_stats`` as a JSON string."""
    sidecar = _geom_sidecar(
        ComputedField(kind=ComputedKind.Z_RANGE, storage_mode=StatisticStorageMode.JSONB)
    )
    payload = sidecar.prepare_place_upsert_payload(
        {"place": _LINESTRING_3D}, {"geoid": "g1"}
    )
    assert payload is not None
    blob = payload["place_stats"]
    assert isinstance(blob, str), "place_stats must be a JSON string, not a raw dict"
    decoded = json.loads(blob)
    assert "z_range" in decoded
    assert abs(decoded["z_range"] - 20.0) < 1e-9  # max z 30 - min z 10


def test_columnar_geom_stat_stays_flat_not_blobbed() -> None:
    """A COLUMNAR stat is a flat typed value, never folded into geom_stats."""
    sidecar = _geom_sidecar(
        ComputedField(kind=ComputedKind.AREA, storage_mode=StatisticStorageMode.COLUMNAR)
    )
    payload = sidecar.prepare_upsert_payload(
        {"geometry": _POLYGON}, {"geoid": "g1"}
    )
    assert payload is not None
    assert "geom_stats" not in payload
    assert isinstance(payload["area"], float)
