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

"""``pushdown_read_select`` — narrow a wildcard read SELECT to the read policy.

The data-oriented PG read used to fetch every sidecar column (geometry stats,
spatial-cell indexes, …) on a wildcard ``/items`` request and rely on the row
mapper to drop them — which over-fetched/joined and leaked the unexposed
columns onto the Feature root. This helper pushes the same declarative contract
the tile path uses (``project_select_for_feature_type``) down to the SQL
projection, but only when it is safe to do so. These pin that decision in
isolation (no DB, no optimizer).
"""

from types import SimpleNamespace

from dynastore.models.query_builder import FieldSelection
from dynastore.modules.storage.computed_fields import FeatureType
from dynastore.modules.storage.read_policy import pushdown_read_select


def _schema(*names: str) -> dict:
    # Minimal duck-typed ItemsSchema.fields: readable text attributes.
    return {n: SimpleNamespace(data_type="text", expose=True) for n in names}


def _wildcard() -> list:
    return [FieldSelection(field="*")]


def _names(selects) -> list:
    return [s.field for s in selects]


# --- bypass cases (return None = leave wildcard unchanged) -------------------

def test_stac_consumer_is_not_narrowed():
    assert pushdown_read_select(
        _wildcard(), FeatureType(), _schema("CODE"),
        is_stac=True, geometry_field="geom", skip_geometry=False,
    ) is None


def test_no_feature_type_is_not_narrowed():
    assert pushdown_read_select(
        _wildcard(), None, _schema("CODE"),
        is_stac=False, geometry_field="geom", skip_geometry=False,
    ) is None


def test_expose_all_is_not_narrowed():
    # expose_all reads stats/system columns off the raw row at map time, so
    # they must stay in the SELECT.
    assert pushdown_read_select(
        _wildcard(), FeatureType(expose_all=True), _schema("CODE"),
        is_stac=False, geometry_field="geom", skip_geometry=False,
    ) is None


def test_explicit_caller_select_is_not_clobbered():
    # A caller that pinned columns (e.g. the DWH join) is authoritative.
    explicit = [FieldSelection(field="geoid"), FieldSelection(field="external_id")]
    assert pushdown_read_select(
        explicit, FeatureType(), _schema("CODE"),
        is_stac=False, geometry_field="geom", skip_geometry=False,
    ) is None


def test_schemaless_collection_is_not_narrowed():
    # No declared schema -> no notion of "all declared properties"; leave the
    # wildcard read (the row-map guard still prevents stat leaks).
    assert pushdown_read_select(
        _wildcard(), FeatureType(), None,
        is_stac=False, geometry_field="geom", skip_geometry=False,
    ) is None
    assert pushdown_read_select(
        _wildcard(), FeatureType(), {},
        is_stac=False, geometry_field="geom", skip_geometry=False,
    ) is None


# --- narrowing cases --------------------------------------------------------

def test_expose_none_narrows_to_schema_plus_geometry():
    # Default expose=None -> all readable schema fields, with geometry prepended.
    out = pushdown_read_select(
        _wildcard(), FeatureType(), _schema("CODE", "NAME"),
        is_stac=False, geometry_field="geom", skip_geometry=False,
    )
    assert out is not None
    names = _names(out)
    assert names[0] == "geom"  # geometry prepended
    assert set(names) == {"geom", "CODE", "NAME"}
    # Critically: no geometry-stat columns (area/perimeter/length) are added —
    # they are not declared schema fields, so the over-fetch is gone.
    assert "area" not in names and "perimeter" not in names


def test_skip_geometry_omits_geometry():
    # returnGeometry=false / skipGeometry=true -> geometry excluded entirely.
    out = pushdown_read_select(
        _wildcard(), FeatureType(), _schema("CODE", "NAME"),
        is_stac=False, geometry_field="geom", skip_geometry=True,
    )
    assert out is not None
    names = _names(out)
    assert "geom" not in names
    assert set(names) == {"CODE", "NAME"}


def test_expose_list_is_additive_schema_plus_computed():
    # A non-empty expose list surfaces schema fields PLUS the listed computed
    # values (additive) — so an explicitly exposed stat IS projected.
    out = pushdown_read_select(
        _wildcard(), FeatureType(expose=["area"]), _schema("CODE"),
        is_stac=False, geometry_field="geom", skip_geometry=False,
    )
    assert out is not None
    names = _names(out)
    assert "CODE" in names and "area" in names and "geom" in names


def test_empty_projection_is_not_narrowed():
    # expose=[] (surface nothing) + skip_geometry would yield an empty list,
    # which validate_select re-expands to the wildcard — defeating the pushdown.
    # Leave the request untouched in that degenerate case.
    assert pushdown_read_select(
        _wildcard(), FeatureType(expose=[]), _schema("CODE"),
        is_stac=False, geometry_field="geom", skip_geometry=True,
    ) is None


def test_expose_empty_with_geometry_is_geometry_only():
    # expose=[] but geometry kept -> a geometry-only feature (non-empty select,
    # no attribute properties).
    out = pushdown_read_select(
        _wildcard(), FeatureType(expose=[]), _schema("CODE"),
        is_stac=False, geometry_field="geom", skip_geometry=False,
    )
    assert out is not None
    assert _names(out) == ["geom"]


# --- skip_geometry + bbox_geom sidecar (#2899) -------------------------------

_BBOX_FIELDS = {"bbox_xmin", "bbox_ymin", "bbox_xmax", "bbox_ymax"}


def test_skip_geometry_with_write_bbox_projects_scalar_bbox_fields():
    # skipGeometry=true + a geometries sidecar with write_bbox=True -> geometry
    # is still omitted, but the cheap ST_XMin/YMin/XMax/YMax(bbox_geom) fields
    # are projected instead so feature.bbox can still be populated.
    out = pushdown_read_select(
        _wildcard(), FeatureType(), _schema("CODE", "NAME"),
        is_stac=False, geometry_field="geom", skip_geometry=True,
        write_bbox=True,
    )
    assert out is not None
    names = _names(out)
    assert "geom" not in names
    assert _BBOX_FIELDS.issubset(names)
    assert set(names) == _BBOX_FIELDS | {"CODE", "NAME"}


def test_skip_geometry_without_write_bbox_unchanged():
    # Regression: no bbox_geom sidecar column configured -> behaviour is
    # identical to the pre-fix path (no bbox fields substituted).
    out = pushdown_read_select(
        _wildcard(), FeatureType(), _schema("CODE", "NAME"),
        is_stac=False, geometry_field="geom", skip_geometry=True,
        write_bbox=False,
    )
    assert out is not None
    names = _names(out)
    assert "geom" not in names
    assert not _BBOX_FIELDS.intersection(names)
    assert set(names) == {"CODE", "NAME"}


def test_geometry_returned_ignores_write_bbox():
    # Regression: when geometry IS returned, write_bbox must not add the
    # scalar bbox fields alongside it — geometry already carries the bbox
    # via the Priority C derive-from-geometry fallback in the row mapper.
    out = pushdown_read_select(
        _wildcard(), FeatureType(), _schema("CODE"),
        is_stac=False, geometry_field="geom", skip_geometry=False,
        write_bbox=True,
    )
    assert out is not None
    names = _names(out)
    assert names[0] == "geom"
    assert not _BBOX_FIELDS.intersection(names)
