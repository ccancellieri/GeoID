"""Unit tests for ``lifecycle.dispatch_preset``.

After the #1502 auto-wrap refactor all registered presets have ``apply``
(routing presets are wrapped at registration time by ``register_preset``).
The dispatcher no longer has a separate routing-bundle branch — it always
goes through the audited lifecycle.
"""
from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch
import pytest

from dynastore.modules.storage.presets.lifecycle import (
    _scope_from_base,
    dispatch_preset,
)
from dynastore.modules.storage.presets.preset import (
    AppliedDescriptor,
    NoParams,
)
from dynastore.modules.storage.presets.protocol import PresetTier
from dynastore.modules.storage.presets.routing_adapter import RoutingPresetAdapter


class _FakeGeneralisedPreset:
    """Mirrors the structural shape of PolicyContributorPreset — no ``build``."""

    name = "generalised_test"
    description = "Generalised preset for dispatch test"
    keywords = ("iam",)
    tier = PresetTier.PLATFORM
    catalog_scopable = False
    params_model = NoParams
    is_async = False

    def __init__(self):
        self.apply_called_with: Any = None
        self.revoke_called_with: Any = None

    async def dry_run(self, params, scope, ctx):
        from dynastore.modules.storage.presets.preset import PresetPlan
        return PresetPlan(preset_name=self.name, scope_key=scope, entries=())

    async def apply(self, params, scope, ctx):
        self.apply_called_with = (params, scope)
        return AppliedDescriptor(payload={"preset_name": self.name})

    async def revoke(self, applied_descriptor, ctx):
        self.revoke_called_with = applied_descriptor
        return None


class _FakeRoutingPreset:
    """Has ``build`` but no ``apply`` — auto-wrapped by register_preset."""

    name = "routing_test"

    def __init__(self):
        self.build_called_with: Any = None

    def build(self, **base_scope):
        self.build_called_with = base_scope
        bundle = MagicMock()
        bundle.iter_apply.return_value = []
        bundle.iter_rollback.return_value = []
        return bundle


def test_scope_from_base_normalises_tiers():
    assert _scope_from_base({}) == "platform"
    assert _scope_from_base({"catalog_id": "c1"}) == "catalog:c1"
    assert (
        _scope_from_base({"catalog_id": "c1", "collection_id": "x"})
        == "catalog:c1/collection:x"
    )


@pytest.mark.asyncio
async def test_dispatch_routing_preset_uses_adapter():
    """A routing preset is auto-wrapped on registration; dispatch_preset calls
    apply() on the RoutingPresetAdapter (not the old _apply_routing_bundle path).
    """
    from dynastore.modules.storage.presets.registry import _REGISTRY, register_preset

    raw_preset = _FakeRoutingPreset()
    # Ensure clean registry slot.
    _REGISTRY.pop("routing_test", None)
    register_preset(raw_preset)

    registered = _REGISTRY["routing_test"]
    assert isinstance(registered, RoutingPresetAdapter), (
        "routing preset must be wrapped in RoutingPresetAdapter after register_preset"
    )

    fake_db = MagicMock()
    fake_db.engine = MagicMock()
    fake_row = {"state": "applied"}

    with patch(
        "dynastore.modules.get_protocol",
        return_value=fake_db,
    ), patch(
        "dynastore.modules.storage.presets.lifecycle.apply_preset",
        new=AsyncMock(return_value=fake_row),
    ) as mock_apply, patch(
        "dynastore.modules.storage.presets.lifecycle._build_context",
        return_value=MagicMock(),
    ):
        result = await dispatch_preset(registered, "apply", base_scope={})

    mock_apply.assert_awaited_once()
    assert result["preset"] == "routing_test"
    assert result["state"] == "applied"

    # Cleanup.
    _REGISTRY.pop("routing_test", None)


@pytest.mark.asyncio
async def test_dispatch_generalised_preset_routes_through_lifecycle():
    """Preset without ``build`` (e.g. PolicyContributorPreset) must not raise
    AttributeError on ``preset.build(...)`` — must go through
    apply_preset / revoke_preset instead.

    This is the regression test for #1473 Bug B (Furkan's comment).
    """
    preset = _FakeGeneralisedPreset()

    fake_db = MagicMock()
    fake_db.engine = MagicMock()
    fake_row = {"state": "applied"}

    with patch(
        "dynastore.modules.get_protocol",
        return_value=fake_db,
    ), patch(
        "dynastore.modules.storage.presets.lifecycle.apply_preset",
        new=AsyncMock(return_value=fake_row),
    ) as mock_apply, patch(
        "dynastore.modules.storage.presets.lifecycle._build_context",
        return_value=MagicMock(),
    ):
        result = await dispatch_preset(preset, "apply", base_scope={})

    mock_apply.assert_awaited_once()
    args, kwargs = mock_apply.await_args
    assert args[0] == "generalised_test"
    assert args[1] == "platform"
    assert result["preset"] == "generalised_test"
    assert result["state"] == "applied"


@pytest.mark.asyncio
async def test_dispatch_unknown_op_raises():
    preset = _FakeGeneralisedPreset()
    fake_db = MagicMock()
    fake_db.engine = MagicMock()
    with patch("dynastore.modules.get_protocol", return_value=fake_db), patch(
        "dynastore.modules.storage.presets.lifecycle._build_context",
        return_value=MagicMock(),
    ):
        with pytest.raises(ValueError, match="Unknown preset op"):
            await dispatch_preset(preset, "explode", base_scope={})  # type: ignore[arg-type]
