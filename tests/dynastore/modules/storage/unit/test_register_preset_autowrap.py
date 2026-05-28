"""Tests for the auto-wrap behaviour introduced in register_preset (#1502).

Three registration contracts:
1. Shape-A class (has ``build``, no ``apply``) → wrapped in RoutingPresetAdapter.
2. Shape-B class (has ``apply``) → stored unchanged.
3. RoutingPresetAdapter re-registered → not double-wrapped.

Plus the latent-bug regression:
4. CompositePreset whose compose list contains a Shape-A child can now call
   ``apply`` without raising AttributeError.
"""
from __future__ import annotations

import uuid
from typing import ClassVar, Tuple
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from dynastore.modules.storage.presets.preset import (
    AppliedDescriptor,
    CompositePreset,
    NoParams,
    PresetContext,
    PresetPlan,
)
from dynastore.modules.storage.presets.protocol import PresetBundle, PresetTier
from dynastore.modules.storage.presets.registry import _REGISTRY, get_preset, register_preset
from dynastore.modules.storage.presets.routing_adapter import RoutingPresetAdapter


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _uid(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex[:8]}"


def _make_bundle() -> PresetBundle:
    """Minimal PresetBundle with no entries — enough to satisfy iter_apply."""
    from dynastore.modules.storage.routing_config import (
        CatalogRoutingConfig,
        FailurePolicy,
        Operation,
        OperationDriverEntry,
    )
    return PresetBundle(
        catalog_routing=CatalogRoutingConfig(
            operations={
                Operation.READ: [
                    OperationDriverEntry(
                        driver_ref="catalog_postgresql_driver",
                        on_failure=FailurePolicy.FATAL,
                    )
                ],
            }
        ),
    )


def _make_ctx() -> PresetContext:
    return PresetContext(
        db=MagicMock(),
        iam=AsyncMock(),
        policy=AsyncMock(),
        config=MagicMock(),
        tasks=None,
        cron=None,
        libs=None,
        principal=None,
        scope="platform",
    )


# ---------------------------------------------------------------------------
# Test 1 — Shape-A preset auto-wrapped in RoutingPresetAdapter
# ---------------------------------------------------------------------------

def test_register_preset_auto_wraps_routing_preset() -> None:
    """A class with ``build`` but no ``apply`` is stored as RoutingPresetAdapter."""
    name = _uid("shape-a")

    class _ShapeA:
        tier: ClassVar[PresetTier] = PresetTier.PLATFORM
        description = "shape-a test"
        keywords: ClassVar[Tuple[str, ...]] = ("routing",)
        catalog_scopable: ClassVar[bool] = False

        def build(self, **_: str) -> PresetBundle:
            return _make_bundle()

    _ShapeA.name = name  # type: ignore[attr-defined]

    _REGISTRY.pop(name, None)
    register_preset(_ShapeA())  # type: ignore[arg-type]

    registered = get_preset(name)
    assert isinstance(registered, RoutingPresetAdapter), (
        "Shape-A preset must be auto-wrapped in RoutingPresetAdapter"
    )
    assert registered.name == name
    # The adapter must delegate build() back to the original preset.
    bundle = registered.build()
    assert bundle is not None

    _REGISTRY.pop(name, None)


# ---------------------------------------------------------------------------
# Test 2 — Shape-B preset stored unchanged
# ---------------------------------------------------------------------------

def test_register_preset_does_not_wrap_generalised_preset() -> None:
    """A class with ``apply`` is stored as-is (no wrapping)."""
    name = _uid("shape-b")

    class _ShapeB:
        tier: ClassVar[PresetTier] = PresetTier.PLATFORM
        description = "shape-b test"
        keywords: ClassVar[Tuple[str, ...]] = ()
        catalog_scopable: ClassVar[bool] = False
        params_model = NoParams
        is_async: ClassVar[bool] = False

        async def dry_run(self, params, scope, ctx) -> PresetPlan:
            return PresetPlan(preset_name=name, scope_key=scope, entries=())

        async def apply(self, params, scope, ctx) -> AppliedDescriptor:
            return AppliedDescriptor()

        async def revoke(self, desc, ctx) -> None:
            return None

    instance = _ShapeB()
    instance.name = name  # type: ignore[attr-defined]

    _REGISTRY.pop(name, None)
    register_preset(instance)

    registered = get_preset(name)
    assert registered is instance, "Shape-B preset must be stored as-is"
    assert not isinstance(registered, RoutingPresetAdapter)

    _REGISTRY.pop(name, None)


# ---------------------------------------------------------------------------
# Test 3 — RoutingPresetAdapter not double-wrapped
# ---------------------------------------------------------------------------

def test_register_preset_does_not_double_wrap_adapter() -> None:
    """A RoutingPresetAdapter that is registered is not wrapped again."""
    name = _uid("adapter-direct")

    class _InnerRP:
        tier: ClassVar[PresetTier] = PresetTier.PLATFORM
        description = "inner rp"
        keywords: ClassVar[Tuple[str, ...]] = ("routing",)
        catalog_scopable: ClassVar[bool] = False

        def build(self, **_: str) -> PresetBundle:
            return _make_bundle()

    inner = _InnerRP()
    inner.name = name  # type: ignore[attr-defined]

    adapter = RoutingPresetAdapter(inner)  # type: ignore[arg-type]

    _REGISTRY.pop(name, None)
    register_preset(adapter)

    registered = get_preset(name)
    assert registered is adapter, "Pre-built adapter must not be re-wrapped"
    assert type(registered) is RoutingPresetAdapter

    _REGISTRY.pop(name, None)


# ---------------------------------------------------------------------------
# Test 4 — Composite with Shape-A child can call apply without AttributeError
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_composite_with_shape_a_child_now_applies() -> None:
    """Regression: CompositePreset.apply() used to raise AttributeError when a
    child was a Shape-A routing preset (no ``apply`` method).  After auto-wrap
    the child is a RoutingPresetAdapter, so apply() succeeds.
    """
    child_name = _uid("shape-a-child")

    class _ShapeAChild:
        tier: ClassVar[PresetTier] = PresetTier.PLATFORM
        description = "shape-a child for composite test"
        keywords: ClassVar[Tuple[str, ...]] = ("routing",)
        catalog_scopable: ClassVar[bool] = False

        def build(self, **_: str) -> PresetBundle:
            return _make_bundle()

    _ShapeAChild.name = child_name  # type: ignore[attr-defined]

    _REGISTRY.pop(child_name, None)
    register_preset(_ShapeAChild())  # type: ignore[arg-type]

    # Verify child is now an adapter with apply().
    registered_child = get_preset(child_name)
    assert isinstance(registered_child, RoutingPresetAdapter)
    assert hasattr(registered_child, "apply")

    comp_name = _uid("comp-shape-a")

    class _Comp(CompositePreset):
        description = "composite with shape-a child"
        keywords: ClassVar[Tuple[str, ...]] = ("composite",)
        tier = PresetTier.PLATFORM
        catalog_scopable = False
        compose = (child_name,)

    _Comp.name = comp_name

    _REGISTRY.pop(comp_name, None)
    register_preset(_Comp())

    comp = get_preset(comp_name)

    fake_configs = MagicMock()
    fake_configs.set_config = AsyncMock()

    # apply() must not raise AttributeError — the latent bug.
    with patch("dynastore.modules.get_protocol", return_value=fake_configs):
        result = await comp.apply(NoParams(), "platform", _make_ctx())

    assert isinstance(result, AppliedDescriptor)

    _REGISTRY.pop(child_name, None)
    _REGISTRY.pop(comp_name, None)
