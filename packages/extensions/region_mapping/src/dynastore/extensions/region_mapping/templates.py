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

"""Jinja2 rendering of ``GET /region-mappings/region.json`` (dynastore#2821).

The ``regionWmsMap`` shape is emitted from a template shipped as a package
static asset (``definitions.json.j2``) rather than built up as a hardcoded
dict, loaded via ``importlib.resources`` so it works from an installed
wheel. The ``[tool.setuptools.package-data]`` line in this extension's
``pyproject.toml`` is load-bearing here -- see GeoID#2736: a missing
package-data entry silently drops non-``.py`` assets from the built wheel,
so the template renders fine from an editable/source checkout but 404s (or
here, raises ``FileNotFoundError``) once packaged.
"""
from __future__ import annotations

import importlib.resources
import json
from typing import Any, Dict, List, TypedDict

from jinja2 import Environment

_PACKAGE = "dynastore.extensions.region_mapping"
_TEMPLATE_NAME = "definitions.json.j2"


class MappingEntry(TypedDict):
    key: str
    server: str
    region_prop: str
    aliases: List[str]
    title: str
    region_ids_file: str
    bbox: List[float]


def _load_template():
    source = (
        importlib.resources.files(_PACKAGE).joinpath(_TEMPLATE_NAME).read_text(encoding="utf-8")
    )
    # Jinja2's builtin ``tojson`` filter (available on any Environment since
    # 2.9) does the safe escaping; autoescape stays off -- this template
    # renders JSON, not HTML.
    env = Environment(autoescape=False)
    return env.from_string(source)


_TEMPLATE = _load_template()


def render_definitions(mappings: List[MappingEntry]) -> Dict[str, Any]:
    """Render ``definitions.json.j2`` against ``mappings`` and return the
    parsed ``{"regionWmsMap": {...}}`` dict."""
    rendered = _TEMPLATE.render(mappings=mappings)
    return json.loads(rendered)
