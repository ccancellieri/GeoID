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

"""Unit tests for STAC datacube v2.3.0 conformance (#2985).

A paginated ``cube:dimensions`` entry (``size``/``href``/``generator`` set,
too many members to enumerate inline) could previously omit both ``extent``
and ``values`` while the collection still declared the datacube v2.3.0
conformance URI — a genuine violation of the v2.3.0 schema's
``additional_dimension`` definition, which requires ``type`` plus either
``extent`` or ``values`` on every entry.

``_ADDITIONAL_DIMENSION_SCHEMA`` below is copied verbatim from the
``additional_dimension`` definition in the real, ratified schema
(https://stac-extensions.github.io/datacube/v2.3.0/schema.json), so these
tests validate against the actual spec requirement rather than a paraphrase
of it.
"""

from __future__ import annotations

from typing import Any, Dict

import jsonschema
import pytest

from dynastore.modules.stac.stac_config import (
    DatacubeDimension,
    DatacubeDimensionType,
    OGC_DIMENSIONS_PAGINATION_URI,
)

# Copied from the "additional_dimension" definition of
# https://stac-extensions.github.io/datacube/v2.3.0/schema.json (fetched and
# verified 2026-07-04). Requires `type` + (`extent` OR `values`); `extent`
# uses the schema's `extent_open` shape, which explicitly allows null bounds.
_ADDITIONAL_DIMENSION_SCHEMA: Dict[str, Any] = {
    "title": "Additional Dimension Object",
    "type": "object",
    "anyOf": [
        {"required": ["type", "extent"]},
        {"required": ["type", "values"]},
    ],
    "not": {"required": ["axis"]},
    "properties": {
        "type": {
            "type": "string",
            "not": {"enum": ["spatial", "geometry"]},
        },
        "extent": {
            "type": "array",
            "minItems": 2,
            "maxItems": 2,
            "items": {"type": ["number", "null"]},
        },
        "values": {
            "type": "array",
            "minItems": 1,
            "items": {"oneOf": [{"type": "number"}, {"type": "string"}]},
        },
    },
}


def _dumped(dim: DatacubeDimension) -> Dict[str, Any]:
    """Render a dimension the same way stac_generator.py does for `cube:dimensions`."""
    return dim.model_dump(exclude_none=True)


class TestExtentOrValuesGuarantee:
    def test_paginated_ordinal_dimension_gets_synthesized_extent(self):
        """A paginated (size/href) dimension with neither extent nor values
        must not end up schema-invalid: an open extent is synthesized."""
        dim = DatacubeDimension(
            type=DatacubeDimensionType.ORDINAL,
            size=1512,
            href="/dimensions/temporal-dekadal/members",
            generator={"type": "daily-period", "config": {"period_days": 10}},
        )
        assert dim.extent == [None, None]
        assert dim.values is None
        jsonschema.validate(_dumped(dim), _ADDITIONAL_DIMENSION_SCHEMA)

    def test_paginated_temporal_dimension_gets_synthesized_extent(self):
        dim = DatacubeDimension(
            type=DatacubeDimensionType.TEMPORAL,
            size=1512,
            href="/dimensions/temporal-dekadal/members",
        )
        assert dim.extent == [None, None]
        jsonschema.validate(_dumped(dim), _ADDITIONAL_DIMENSION_SCHEMA)

    def test_dimension_with_explicit_extent_is_untouched(self):
        dim = DatacubeDimension(
            type=DatacubeDimensionType.TEMPORAL,
            extent=["1984-01-01T00:00:00Z", "2026-03-10T00:00:00Z"],
            size=1512,
            href="/dimensions/temporal-dekadal/members",
        )
        assert dim.extent == ["1984-01-01T00:00:00Z", "2026-03-10T00:00:00Z"]

    def test_dimension_with_values_only_is_untouched(self):
        dim = DatacubeDimension(type=DatacubeDimensionType.NOMINAL, values=["GS1", "GS2"])
        assert dim.extent is None
        assert dim.values == ["GS1", "GS2"]
        jsonschema.validate(_dumped(dim), _ADDITIONAL_DIMENSION_SCHEMA)

    def test_spatial_dimension_missing_extent_is_not_synthesized(self):
        """Spatial dims need a genuine numeric extent (schema requires it
        unconditionally for the horizontal/vertical branches); null bounds
        would not make it schema-valid, so we leave it as an authoring gap
        rather than fabricate a false extent."""
        dim = DatacubeDimension(type=DatacubeDimensionType.SPATIAL, axis="x")
        assert dim.extent is None
        assert dim.values is None

    def test_invalid_entry_without_synthesis_fails_the_real_schema(self):
        """Sanity check: an entry with neither extent nor values genuinely
        fails the v2.3.0 schema — proving the synthesis above is load-bearing."""
        broken = {"type": "ordinal", "size": 10, "href": "/x"}
        with pytest.raises(jsonschema.ValidationError):
            jsonschema.validate(broken, _ADDITIONAL_DIMENSION_SCHEMA)


class TestPaginationExtensionUriGating:
    """DynaStore-specific pagination fields (size/href/generator) must be
    declared under their own conformance URI, separate from the ratified
    datacube v2.3.0 URI (#2985)."""

    def test_pagination_uri_added_when_dimension_is_paginated(self):
        from dynastore.extensions.stac.stac_generator import (
            _cube_dimensions_extension_uris,
            SUPPORTED_STAC_EXTENSIONS,
        )

        cube_dimensions = {
            "time": DatacubeDimension(
                type=DatacubeDimensionType.TEMPORAL,
                size=1512,
                href="/dimensions/temporal-dekadal/members",
            ),
        }
        uris = _cube_dimensions_extension_uris(cube_dimensions)
        assert SUPPORTED_STAC_EXTENSIONS[0] in uris
        assert OGC_DIMENSIONS_PAGINATION_URI in uris

    def test_pagination_uri_absent_when_no_dimension_is_paginated(self):
        from dynastore.extensions.stac.stac_generator import (
            _cube_dimensions_extension_uris,
            SUPPORTED_STAC_EXTENSIONS,
        )

        cube_dimensions = {
            "season": DatacubeDimension(
                type=DatacubeDimensionType.NOMINAL,
                values=["GS1", "GS2"],
            ),
        }
        uris = _cube_dimensions_extension_uris(cube_dimensions)
        assert SUPPORTED_STAC_EXTENSIONS[0] in uris
        assert OGC_DIMENSIONS_PAGINATION_URI not in uris
