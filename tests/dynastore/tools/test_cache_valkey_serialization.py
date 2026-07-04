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

"""msgpack round-trip for Valkey cache payloads.

Every payload ``_serialize`` accepts must come back through
``_deserialize``: msgpack's default ``strict_map_key=True`` rejected the
int-keyed dicts that pack fine on write (e.g. ``get_tile_resolution_params``'
``simplification_by_zoom: Dict[int, float]``), so every distributed-cache
read of such an entry failed — a permanent miss that also fed the circuit
breaker's failure counter.
"""
from __future__ import annotations

from datetime import datetime, timezone

from dynastore.tools.cache_valkey import _deserialize, _serialize


def test_int_keyed_dict_round_trips() -> None:
    payload = {
        "physical_table": "t_abc",
        "source_srid": 4326,
        "simplification_by_zoom": {0: 0.1, 5: 0.01, 12: 0.001},
        "min_feature_pixel_area_by_zoom": {0: 4.0, 8: 1.0},
    }
    assert _deserialize(_serialize(payload)) == payload


def test_common_payload_shapes_round_trip() -> None:
    payloads = [
        {"a": 1, "b": [1, 2, 3], "c": None},
        ["x", 1.5, True],
        "plain-string",
        {1: "one", 2: "two"},
        {datetime(2026, 7, 4, tzinfo=timezone.utc).isoformat(): "ts-str-key"},
    ]
    for payload in payloads:
        assert _deserialize(_serialize(payload)) == payload
