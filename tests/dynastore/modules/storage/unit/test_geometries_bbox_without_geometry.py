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

"""``skipGeometry=true`` must not drop ``bbox`` alongside geometry (#2899).

Pins the row-mapping half of the fix: when a read projects the scalar
``bbox_xmin``/``bbox_ymin``/``bbox_xmax``/``bbox_ymax`` columns (Priority A —
what ``pushdown_read_select`` now substitutes for the geometry column when
``skip_geometry=True`` and the sidecar has ``write_bbox`` configured),
``GeometriesSidecar.map_row_to_feature`` populates ``feature.bbox`` without
ever touching ``feature.geometry``. The SQL-projection half (which columns
``pushdown_read_select``/``get_select_fields`` emit) is covered by
``test_pushdown_read_select.py`` and
``test_pg_geometries_sidecar_omits_geom_when_skip_geometry_true``.
"""

from geojson_pydantic import Feature

from dynastore.modules.storage.drivers.pg_sidecars.base import FeaturePipelineContext
from dynastore.modules.storage.drivers.pg_sidecars.geometries import GeometriesSidecar
from dynastore.modules.storage.drivers.pg_sidecars.geometries_config import (
    GeometriesSidecarConfig,
)


def _blank_feature() -> Feature:
    return Feature(type="Feature", geometry=None, properties={})


def test_scalar_bbox_columns_populate_bbox_without_geometry() -> None:
    sidecar = GeometriesSidecar(GeometriesSidecarConfig())
    feature = _blank_feature()
    row = {
        "geoid": "g1",
        "bbox_xmin": 10.0,
        "bbox_ymin": 20.0,
        "bbox_xmax": 30.0,
        "bbox_ymax": 40.0,
    }

    sidecar.map_row_to_feature(row, feature, FeaturePipelineContext())

    assert feature.geometry is None
    assert feature.bbox == (10.0, 20.0, 30.0, 40.0)


def test_no_bbox_source_leaves_bbox_unset() -> None:
    # Regression: with neither scalar columns, bbox_geom, nor geometry in the
    # row, feature.bbox stays None rather than raising.
    sidecar = GeometriesSidecar(GeometriesSidecarConfig())
    feature = _blank_feature()

    sidecar.map_row_to_feature({"geoid": "g1"}, feature, FeaturePipelineContext())

    assert feature.geometry is None
    assert feature.bbox is None
