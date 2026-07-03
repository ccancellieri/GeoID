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

"""Unit tests for ``definitions.json.j2`` rendering (dynastore#2821).

Loads the template as a real package asset via ``importlib.resources`` (not
a hand-inlined string) -- this is exactly what breaks silently if the
``[tool.setuptools.package-data]`` entry in ``pyproject.toml`` is ever
dropped (GeoID#2736).
"""
from __future__ import annotations


def _entry(**overrides):
    """A minimal, complete ``MappingEntry`` dict -- default values match the
    hardcoded constants the template used before dynastore#2882/#443 model
    cleanup made these fields per-mapping configurable."""
    base = {
        "key": "FAO_COUNTRIES",
        "server": "https://example.org/maps/tiles/catalogs/fao/tiles/{z}/{x}/{y}.mvt?collections=countries",
        "region_prop": "adm0_code",
        "aliases": ["adm0_code", "country", "fao_country"],
        "title": "Countries",
        "region_ids_file": "https://example.org/region-mappings/fao_countries/regionIds",
        "bbox": [10.0, 20.0, 30.0, 40.0],
        "layer_name": "default",
        "server_type": "MVT",
        "server_subdomains": [],
        "server_min_zoom": 0,
        "server_max_native_zoom": 12,
        "server_max_zoom": 28,
        "unique_id_prop": "adm0_code",
        "digits": 255,
    }
    base.update(overrides)
    return base


def test_render_definitions_exact_shape() -> None:
    from dynastore.extensions.region_mapping.templates import render_definitions

    result = render_definitions([_entry()])

    assert result == {
        "regionWmsMap": {
            "FAO_COUNTRIES": {
                "layerName": "default",
                "server": "https://example.org/maps/tiles/catalogs/fao/tiles/{z}/{x}/{y}.mvt?collections=countries",
                "serverType": "MVT",
                "serverSubdomains": [],
                "serverMinZoom": 0,
                "serverMaxNativeZoom": 12,
                "serverMaxZoom": 28,
                "regionProp": "adm0_code",
                "uniqueIdProp": "adm0_code",
                "aliases": ["adm0_code", "country", "fao_country"],
                "digits": 255,
                "description": "Countries",
                "regionIdsFile": "https://example.org/region-mappings/fao_countries/regionIds",
                "bbox": [10.0, 20.0, 30.0, 40.0],
            },
        },
    }


def test_render_definitions_honours_per_mapping_overrides() -> None:
    """Non-default layerName/serverType/zoom/digits/uniqueIdProp all render
    through -- proves these are genuinely per-mapping now, not hardcoded."""
    from dynastore.extensions.region_mapping.templates import render_definitions

    result = render_definitions([_entry(
        layer_name="gaul_layer", server_type="WMS", server_subdomains=["a", "b"],
        server_min_zoom=2, server_max_native_zoom=8, server_max_zoom=20,
        unique_id_prop="internal_id", digits=4,
    )])

    entry = result["regionWmsMap"]["FAO_COUNTRIES"]
    assert entry["layerName"] == "gaul_layer"
    assert entry["serverType"] == "WMS"
    assert entry["serverSubdomains"] == ["a", "b"]
    assert entry["serverMinZoom"] == 2
    assert entry["serverMaxNativeZoom"] == 8
    assert entry["serverMaxZoom"] == 20
    assert entry["uniqueIdProp"] == "internal_id"
    assert entry["digits"] == 4


def test_render_definitions_multiple_mappings_all_present() -> None:
    from dynastore.extensions.region_mapping.templates import render_definitions

    entries = [_entry(key=f"KEY_{i}", server=f"server{i}", title=f"Title {i}") for i in range(3)]

    result = render_definitions(entries)

    assert set(result["regionWmsMap"].keys()) == {"KEY_0", "KEY_1", "KEY_2"}


def test_render_definitions_empty_mappings_yields_empty_map() -> None:
    from dynastore.extensions.region_mapping.templates import render_definitions

    assert render_definitions([]) == {"regionWmsMap": {}}


def test_render_definitions_escapes_special_characters_in_values() -> None:
    """A title/alias containing quotes or backslashes must not break the
    rendered JSON -- proves the template uses ``tojson``, not raw string
    interpolation."""
    from dynastore.extensions.region_mapping.templates import render_definitions

    result = render_definitions([_entry(
        key="K", server="s",
        aliases=['weird "alias" \\ value'],
        title='Title with "quotes" and \\backslashes\\',
        region_ids_file="f",
    )])

    entry = result["regionWmsMap"]["K"]
    assert entry["description"] == 'Title with "quotes" and \\backslashes\\'
    assert entry["aliases"] == ['weird "alias" \\ value']
