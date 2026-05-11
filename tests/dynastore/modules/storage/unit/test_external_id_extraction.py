"""Regression coverage for external_id extraction from GDAL/shapefile features.

Issue #488: when ``items_write_policy.external_id_field`` named a non-``id``
attribute (e.g. ``ADM2_PCODE``) the extracted ``external_id`` was empty in DB,
because ``_extract_value`` only fell back to ``properties`` when the path
literally contained ``id`` or ``asset_id``. GDAL features arrive as
``{"properties": {...}, "geometry": ...}`` so the lookup at the root layer
missed every user-defined field, and ``require_external_id`` silently passed
because ``"" is None`` is false.
"""
from __future__ import annotations

from dynastore.modules.storage.drivers.pg_sidecars.attributes import (
    FeatureAttributeSidecar,
)
from dynastore.modules.storage.drivers.pg_sidecars.attributes_config import (
    FeatureAttributeSidecarConfig,
)


def _sidecar(
    *, external_id_field: str = "id", require_external_id: bool = False
) -> FeatureAttributeSidecar:
    return FeatureAttributeSidecar(
        FeatureAttributeSidecarConfig(
            external_id_field=external_id_field,
            require_external_id=require_external_id,
        )
    )


class TestExtractValuePropertiesFallback:
    def test_root_level_wins(self):
        sidecar = _sidecar(external_id_field="code")
        feature = {"code": "ROOT", "properties": {"code": "PROPS"}}
        assert sidecar._extract_value(feature, "code") == "ROOT"

    def test_falls_back_to_properties_for_arbitrary_field(self):
        sidecar = _sidecar(external_id_field="ADM2_PCODE")
        feature = {
            "type": "Feature",
            "geometry": {"type": "Point", "coordinates": [0, 0]},
            "properties": {"ADM2_PCODE": "TG0309"},
        }
        assert sidecar._extract_value(feature, "ADM2_PCODE") == "TG0309"

    def test_legacy_id_path_still_works(self):
        sidecar = _sidecar(external_id_field="id")
        feature = {"properties": {"id": "X"}}
        assert sidecar._extract_value(feature, "id") == "X"

    def test_asset_id_path_still_works(self):
        sidecar = _sidecar(external_id_field="asset_id")
        feature = {"properties": {"asset_id": "A"}}
        assert sidecar._extract_value(feature, "asset_id") == "A"

    def test_dot_path_does_not_trigger_fallback(self):
        sidecar = _sidecar(external_id_field="properties.ADM2_PCODE")
        feature = {"properties": {"ADM2_PCODE": "TG0309"}}
        assert (
            sidecar._extract_value(feature, "properties.ADM2_PCODE") == "TG0309"
        )

    def test_missing_field_returns_none(self):
        sidecar = _sidecar(external_id_field="ADM2_PCODE")
        feature = {"properties": {"OTHER": "x"}}
        assert sidecar._extract_value(feature, "ADM2_PCODE") is None

    def test_no_properties_bag_returns_none(self):
        sidecar = _sidecar(external_id_field="ADM2_PCODE")
        assert sidecar._extract_value({"geometry": None}, "ADM2_PCODE") is None


class TestValidateInsertRejectsFalsy:
    def test_rejects_when_extraction_yields_none(self):
        sidecar = _sidecar(
            external_id_field="ADM2_PCODE", require_external_id=True
        )
        result = sidecar.validate_insert({"properties": {}}, context={})
        assert result.valid is False

    def test_rejects_when_extraction_yields_empty_string(self):
        sidecar = _sidecar(
            external_id_field="ADM2_PCODE", require_external_id=True
        )
        result = sidecar.validate_insert(
            {"properties": {"ADM2_PCODE": ""}}, context={}
        )
        assert result.valid is False

    def test_accepts_when_field_resolves_via_properties(self):
        sidecar = _sidecar(
            external_id_field="ADM2_PCODE", require_external_id=True
        )
        result = sidecar.validate_insert(
            {"properties": {"ADM2_PCODE": "TG0309"}}, context={}
        )
        assert result.valid is True
