"""Regression coverage for external_id extraction from GDAL/shapefile features.

Issue #488: when ``items_write_policy.external_id_field`` named a non-``id``
attribute (e.g. ``ADM2_PCODE``) the extracted ``external_id`` was empty in DB,
because ``_extract_value`` only fell back to ``properties`` when the path
literally contained ``id`` or ``asset_id``. GDAL features arrive as
``{"properties": {...}, "geometry": ...}`` so the lookup at the root layer
missed every user-defined field, and ``require_external_id`` silently passed
because ``"" is None`` is false.

Phase 2 of #957/#950: ``external_id_field`` / ``require_external_id`` were
folded into :attr:`ItemsWritePolicy.compute` (a ComputedField of kind
EXTERNAL_ID whose ``name`` is the source path) and the JSON Schema's
``required`` list respectively. The sidecar still receives the policy on
``context["_items_write_policy"]``; only the surface shape changed.
"""
from __future__ import annotations

from typing import Any, Dict, Optional

from dynastore.modules.storage import ComputedField, ComputedKind
from dynastore.modules.storage.driver_config import ItemsWritePolicy
from dynastore.modules.storage.drivers.pg_sidecars.attributes import (
    FeatureAttributeSidecar,
)
from dynastore.modules.storage.drivers.pg_sidecars.attributes_config import (
    FeatureAttributeSidecarConfig,
)


def _sidecar() -> FeatureAttributeSidecar:
    return FeatureAttributeSidecar(FeatureAttributeSidecarConfig())


def _ctx(
    *,
    external_id_field: Optional[str] = None,
    require_external_id: bool = False,
) -> dict:
    """Build a sidecar context with a phase-2 ItemsWritePolicy.

    ``external_id_field`` becomes ``ComputedField(kind=EXTERNAL_ID, name=path)``;
    ``require_external_id`` is expressed via the JSON Schema's ``required``
    list keyed on the leaf segment of the path.
    """
    kwargs: Dict[str, Any] = {}
    if external_id_field is not None:
        kwargs["compute"] = [
            ComputedField(kind=ComputedKind.EXTERNAL_ID, name=external_id_field)
        ]
    if require_external_id and external_id_field is not None:
        leaf = external_id_field.split(".")[-1]
        kwargs["schema"] = {
            "type": "object",
            "required": [leaf],
            "properties": {leaf: {"type": "string"}},
        }
    elif require_external_id:
        # Path missing — still flag required so the validator sees the
        # "required but no path" combination.
        kwargs["schema"] = {"type": "object", "required": ["external_id"]}
        kwargs["compute"] = [ComputedField(kind=ComputedKind.EXTERNAL_ID, name="external_id")]
    policy = ItemsWritePolicy(**kwargs)
    return {"_items_write_policy": policy}


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


class TestValidateInsertReadsPolicyFromContext:
    """The sidecar's `validate_insert` MUST consult the `ItemsWritePolicy`
    on context, not any sidecar-config field. This is the contract that
    fixes #488 — operator PUTs `items_write_policy.{external_id_field,
    require_external_id}` and those values reach validation."""

    def test_no_policy_accepts(self):
        sidecar = _sidecar()
        result = sidecar.validate_insert({"properties": {}}, context={})
        assert result.valid is True

    def test_policy_without_require_accepts_empty(self):
        sidecar = _sidecar()
        ctx = _ctx(external_id_field="ADM2_PCODE", require_external_id=False)
        result = sidecar.validate_insert({"properties": {}}, context=ctx)
        assert result.valid is True

    def test_rejects_when_extraction_yields_none(self):
        sidecar = _sidecar()
        ctx = _ctx(external_id_field="ADM2_PCODE", require_external_id=True)
        result = sidecar.validate_insert({"properties": {}}, context=ctx)
        assert result.valid is False

    def test_rejects_when_extraction_yields_empty_string(self):
        sidecar = _sidecar()
        ctx = _ctx(external_id_field="ADM2_PCODE", require_external_id=True)
        result = sidecar.validate_insert(
            {"properties": {"ADM2_PCODE": ""}}, context=ctx
        )
        assert result.valid is False

    def test_accepts_when_field_resolves_via_properties(self):
        sidecar = _sidecar()
        ctx = _ctx(external_id_field="ADM2_PCODE", require_external_id=True)
        result = sidecar.validate_insert(
            {"properties": {"ADM2_PCODE": "TG0309"}}, context=ctx
        )
        assert result.valid is True

    def test_accepts_when_field_resolves_top_level(self):
        sidecar = _sidecar()
        ctx = _ctx(external_id_field="my_key", require_external_id=True)
        result = sidecar.validate_insert(
            {"my_key": "K1", "properties": {}}, context=ctx
        )
        assert result.valid is True

    def test_require_without_field_path_rejects(self):
        sidecar = _sidecar()
        ctx = _ctx(external_id_field=None, require_external_id=True)
        result = sidecar.validate_insert(
            {"properties": {"ADM2_PCODE": "TG0309"}}, context=ctx
        )
        assert result.valid is False
