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

"""Regression coverage for external_id extraction from GDAL/shapefile features.

Issue #488: when ``items_write_policy.external_id_field`` named a non-``id``
attribute (e.g. ``ADM2_PCODE``) the extracted ``external_id`` was empty in DB,
because ``_extract_value`` only fell back to ``properties`` when the path
literally contained ``id`` or ``asset_id``. GDAL features arrive as
``{"properties": {...}, "geometry": ...}`` so the lookup at the root layer
missed every user-defined field.

The ``external_id_field`` knob was folded into
:attr:`ItemsWritePolicy.derive` (a ComputedField of kind EXTERNAL_ID whose
``name`` is the source path). The sidecar still receives the policy on
``context["_items_write_policy"]``; this module covers the still-live
``_extract_value`` resolution. The separate require-external-id guard layer
was removed in #1164 (it never fired in production — the wire schema's
``required`` list was never populated on the policy); a missing/empty
external_id that is a declared required field is now rejected by the normal
required-field check plus NOT NULL columns.
"""
from __future__ import annotations

from dynastore.modules.storage.drivers.pg_sidecars.attributes import (
    FeatureAttributeSidecar,
)
from dynastore.modules.storage.drivers.pg_sidecars.attributes_config import (
    FeatureAttributeSidecarConfig,
)


def _sidecar() -> FeatureAttributeSidecar:
    return FeatureAttributeSidecar(FeatureAttributeSidecarConfig())


class TestExtractValuePropertiesFallback:
    def test_root_level_wins(self):
        sidecar = _sidecar()
        feature = {"code": "ROOT", "properties": {"code": "PROPS"}}
        assert sidecar._extract_value(feature, "code") == "ROOT"

    def test_falls_back_to_properties_for_arbitrary_field(self):
        sidecar = _sidecar()
        feature = {
            "type": "Feature",
            "geometry": {"type": "Point", "coordinates": [0, 0]},
            "properties": {"ADM2_PCODE": "TG0309"},
        }
        assert sidecar._extract_value(feature, "ADM2_PCODE") == "TG0309"

    def test_legacy_id_path_still_works(self):
        sidecar = _sidecar()
        feature = {"properties": {"id": "X"}}
        assert sidecar._extract_value(feature, "id") == "X"

    def test_asset_id_path_still_works(self):
        sidecar = _sidecar()
        feature = {"properties": {"asset_id": "A"}}
        assert sidecar._extract_value(feature, "asset_id") == "A"

    def test_dot_path_does_not_trigger_fallback(self):
        sidecar = _sidecar()
        feature = {"properties": {"ADM2_PCODE": "TG0309"}}
        assert (
            sidecar._extract_value(feature, "properties.ADM2_PCODE") == "TG0309"
        )

    def test_missing_field_returns_none(self):
        sidecar = _sidecar()
        feature = {"properties": {"OTHER": "x"}}
        assert sidecar._extract_value(feature, "ADM2_PCODE") is None

    def test_no_properties_bag_returns_none(self):
        sidecar = _sidecar()
        assert sidecar._extract_value({"geometry": None}, "ADM2_PCODE") is None


class TestExtractExternalIdFromDocPropertiesFallback:
    """ES-side counterpart of the #488 fix.

    ``ItemsElasticsearchDriver._extract_external_id_from_doc`` resolves the
    external_id on the ES write boundary (it drives the ES ``_id`` and the
    REFUSE existence check). It must mirror the PG-side
    ``_walk_external_id_path``/``_extract_value`` resolution — including the
    bare-name → ``properties`` fallback — otherwise a configured path like
    ``"CODE"`` (whose value lives under ``properties`` on a GDAL/shapefile
    feature) resolves to ``None`` on the ES side, dropping
    ``system.external_id`` and degrading the ES ``_id`` to the geoid.
    """

    @staticmethod
    def _extract(doc, path):
        from dynastore.modules.storage.drivers.elasticsearch import (
            ItemsElasticsearchDriver,
        )

        return ItemsElasticsearchDriver._extract_external_id_from_doc(doc, path)

    def test_none_path_returns_none(self):
        assert self._extract({"properties": {"CODE": "1616"}}, None) is None

    def test_root_level_wins(self):
        doc = {"CODE": "ROOT", "properties": {"CODE": "PROPS"}}
        assert self._extract(doc, "CODE") == "ROOT"

    def test_falls_back_to_properties_for_bare_field(self):
        # The regression: bare "CODE" with the value under properties.
        doc = {
            "type": "Feature",
            "geometry": {"type": "Point", "coordinates": [0, 0]},
            "properties": {"CODE": "1616"},
        }
        assert self._extract(doc, "CODE") == "1616"

    def test_dot_path_traverses_nested_dicts(self):
        doc = {"properties": {"CODE": "1616"}}
        assert self._extract(doc, "properties.CODE") == "1616"

    def test_dot_path_does_not_trigger_properties_fallback(self):
        # A dotted path is explicit; no implicit properties fallback.
        doc = {"properties": {"CODE": "1616"}}
        assert self._extract(doc, "a.b") is None

    def test_missing_field_returns_none(self):
        assert self._extract({"properties": {"OTHER": "x"}}, "CODE") is None

    def test_no_properties_bag_returns_none(self):
        assert self._extract({"geometry": None}, "CODE") is None

    def test_numeric_value_is_stringified(self):
        assert self._extract({"properties": {"CODE": 1616}}, "CODE") == "1616"
