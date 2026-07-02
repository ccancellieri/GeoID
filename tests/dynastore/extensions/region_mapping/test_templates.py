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


def test_render_definitions_exact_shape() -> None:
    from dynastore.extensions.region_mapping.templates import render_definitions

    result = render_definitions([
        {
            "key": "FAO_COUNTRIES",
            "server": "https://example.org/maps/tiles/catalogs/fao/tiles/{z}/{x}/{y}.mvt?collections=countries",
            "region_prop": "adm0_code",
            "aliases": ["adm0_code", "country", "fao_country"],
            "title": "Countries",
            "region_ids_file": "https://example.org/region-mappings/fao_countries/regionIds",
            "bbox": [10.0, 20.0, 30.0, 40.0],
        },
    ])

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


def test_render_definitions_multiple_mappings_all_present() -> None:
    from dynastore.extensions.region_mapping.templates import render_definitions

    entries = [
        {
            "key": f"KEY_{i}", "server": f"server{i}", "region_prop": "id",
            "aliases": [f"a{i}"], "title": f"Title {i}",
            "region_ids_file": f"file{i}", "bbox": [0.0, 0.0, 1.0, 1.0],
        }
        for i in range(3)
    ]

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

    result = render_definitions([
        {
            "key": "K", "server": "s", "region_prop": "id",
            "aliases": ['weird "alias" \\ value'],
            "title": 'Title with "quotes" and \\backslashes\\',
            "region_ids_file": "f", "bbox": [0.0, 0.0, 1.0, 1.0],
        },
    ])

    entry = result["regionWmsMap"]["K"]
    assert entry["description"] == 'Title with "quotes" and \\backslashes\\'
    assert entry["aliases"] == ['weird "alias" \\ value']
