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

"""Regression tests for the #2899 foreign-member-leak bug class.

``item_service.map_row_to_feature`` flattens anything the attributes
sidecar publishes (the *entire* raw row, see ``attributes.py``
``map_row_to_feature``) onto the Feature root unless the column is listed
in some sidecar's ``get_internal_columns()``. Any sidecar-owned storage
column missing from that set leaks onto the OGC API Features wire as a
foreign member. Pins two instances found alongside the original
``bbox_xmin``/``bbox_ymin``/``bbox_xmax``/``bbox_ymax`` fix.
"""

from dynastore.modules.storage.drivers.pg_sidecars.attributes import (
    FeatureAttributeSidecar,
)
from dynastore.modules.storage.drivers.pg_sidecars.attributes_config import (
    AttributeStorageMode,
    FeatureAttributeSidecarConfig,
)
from dynastore.modules.storage.drivers.pg_sidecars.geometries import GeometriesSidecar
from dynastore.modules.storage.drivers.pg_sidecars.geometries_config import (
    GeometriesSidecarConfig,
)


def test_custom_external_id_field_is_internal() -> None:
    # A collection can name its identifier column anything (null-object
    # pattern on FeatureAttributeSidecarConfig.external_id_field); the
    # internal-columns set must track that name, not a hardcoded literal.
    sidecar = FeatureAttributeSidecar(
        FeatureAttributeSidecarConfig(
            storage_mode=AttributeStorageMode.JSONB,
            external_id_field="asset_id",
        )
    )

    assert "asset_id" in sidecar.get_internal_columns()
    assert "external_id" not in sidecar.get_internal_columns()


def test_default_external_id_field_is_internal() -> None:
    sidecar = FeatureAttributeSidecar(FeatureAttributeSidecarConfig())

    assert "external_id" in sidecar.get_internal_columns()


def test_external_id_field_disabled_is_not_internal() -> None:
    sidecar = FeatureAttributeSidecar(
        FeatureAttributeSidecarConfig(external_id_field=None)
    )

    assert "external_id" not in sidecar.get_internal_columns()


def test_geom_type_column_is_internal() -> None:
    # geom_type is a NOT NULL storage column resolvable via
    # resolve_query_path(); it must never surface as a Feature property.
    sidecar = GeometriesSidecar(GeometriesSidecarConfig())

    assert "geom_type" in sidecar.get_internal_columns()
