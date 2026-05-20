"""Phase 1–3 feature-config skeleton: compute presets, derived/forbidden wire
schema, and expose cross-validation."""

import asyncio

import pytest

from dynastore.models.protocols.field_definition import FieldDefinition
from dynastore.modules.storage.computed_fields import (
    ComputedField,
    ComputedKind,
    FeatureType,
)
from dynastore.modules.storage.driver_config import (
    ItemsSchema,
    ItemsWritePolicy,
    _forbid_authored_wire_schema,
)
from dynastore.modules.storage.read_policy import (
    ItemsReadPolicy,
    _validate_read_policy,
)


class TestComputePresetValidator:
    def test_preset_string_expands(self) -> None:
        wp = ItemsWritePolicy(compute="geometry_stats")
        kinds = {cf.kind for cf in wp.compute}
        assert {ComputedKind.AREA, ComputedKind.PERIMETER, ComputedKind.CENTROID} <= kinds

    def test_mixed_list_explicit_coexists(self) -> None:
        wp = ItemsWritePolicy(compute=["geometry_stats", {"kind": "h3", "resolution": 9}])
        assert any(cf.resolved_name == "h3_9" for cf in wp.compute)

    def test_none_normalises_to_empty(self) -> None:
        assert ItemsWritePolicy(compute=None).compute == []

    def test_unknown_preset_rejected(self) -> None:
        with pytest.raises(ValueError, match="unknown compute preset"):
            ItemsWritePolicy(compute="not_a_preset")


class TestForbidAuthoredWireSchema:
    def test_raises_when_authored(self) -> None:
        wp = ItemsWritePolicy(schema={"type": "object"})
        with pytest.raises(ValueError, match="derived"):
            asyncio.run(_forbid_authored_wire_schema(wp, "cat", "col", None))

    def test_passes_when_none(self) -> None:
        asyncio.run(_forbid_authored_wire_schema(ItemsWritePolicy(), "cat", "col", None))


def _fake_configs(wp: ItemsWritePolicy, schema: ItemsSchema):
    async def get_config(cls, **_kw):
        return wp if cls is ItemsWritePolicy else schema

    return type("FakeConfigs", (), {"get_config": staticmethod(get_config)})()


class TestExposeValidation:
    def test_skips_when_no_expose(self) -> None:
        asyncio.run(_validate_read_policy(ItemsReadPolicy(), "cat", "col", None))

    def test_rejects_unknown_name(self, monkeypatch) -> None:
        wp = ItemsWritePolicy(compute=[ComputedField(kind=ComputedKind.AREA)])
        schema = ItemsSchema(fields={"name": FieldDefinition(name="name", data_type="text")})
        monkeypatch.setattr(
            "dynastore.tools.discovery.get_protocol",
            lambda _p: _fake_configs(wp, schema),
        )
        rp = ItemsReadPolicy(feature_type=FeatureType(expose=["area", "nope"]))
        with pytest.raises(ValueError, match="nope"):
            asyncio.run(_validate_read_policy(rp, "cat", "col", None))

    def test_accepts_computed_and_declared(self, monkeypatch) -> None:
        wp = ItemsWritePolicy(compute=[ComputedField(kind=ComputedKind.AREA)])
        schema = ItemsSchema(fields={"name": FieldDefinition(name="name", data_type="text")})
        monkeypatch.setattr(
            "dynastore.tools.discovery.get_protocol",
            lambda _p: _fake_configs(wp, schema),
        )
        rp = ItemsReadPolicy(feature_type=FeatureType(expose=["area", "name"]))
        asyncio.run(_validate_read_policy(rp, "cat", "col", None))
