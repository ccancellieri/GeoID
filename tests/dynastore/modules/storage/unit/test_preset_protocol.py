"""Generalised Preset protocol shape + registry validation + search filters."""
from __future__ import annotations

from typing import ClassVar, Tuple, Type
from unittest.mock import AsyncMock, MagicMock

import pytest
from pydantic import BaseModel

from dynastore.modules.storage.presets.preset import (
    AppliedDescriptor,
    CompositePreset,
    NoParams,
    Preset,
    PresetContext,
    PresetPlan,
    PresetPlanEntry,
    TaskHandle,
)
from dynastore.modules.storage.presets.protocol import PresetTier
from dynastore.modules.storage.presets.registry import (
    _REGISTRY,
    find_preset,
    register_preset,
    search_presets,
)


# ---------------------------------------------------------------------------
# Fixtures and helpers
# ---------------------------------------------------------------------------

def _unique_name(base: str) -> str:
    import uuid
    return f"{base}-{uuid.uuid4().hex[:8]}"


class _MinimalPreset:
    """Minimal new-style Preset implementation for shape tests."""

    is_async: ClassVar[bool] = False
    params_model: ClassVar[Type[BaseModel]] = NoParams
    catalog_scopable: ClassVar[bool] = False

    def __init__(self, name: str = "test-preset") -> None:
        self.name = name
        self.description = "A test preset"
        self.keywords: Tuple[str, ...] = ("test",)
        self.tier = PresetTier.CATALOG

    async def dry_run(self, params, scope, ctx) -> PresetPlan:
        return PresetPlan(preset_name=self.name, scope_key=scope)

    async def apply(self, params, scope, ctx):
        return AppliedDescriptor(payload={"test": True})

    async def revoke(self, applied_descriptor, ctx):
        return None


# ---------------------------------------------------------------------------
# Protocol shape tests
# ---------------------------------------------------------------------------

def test_no_params_is_valid_pydantic_model():
    p = NoParams()
    assert p.model_dump() == {}
    assert NoParams.model_json_schema()["type"] == "object"


def test_applied_descriptor_round_trip():
    orig = AppliedDescriptor(payload={"key": "value", "num": 42})
    data = orig.to_json()
    restored = AppliedDescriptor.from_json(data)
    assert restored.payload == orig.payload


def test_applied_descriptor_from_none():
    d = AppliedDescriptor.from_json(None)
    assert d.payload == {}


def test_task_handle_stores_task_id():
    import uuid
    tid = uuid.uuid4()
    h = TaskHandle(task_id=tid)
    assert h.task_id == tid
    assert h.revoke_descriptor is None


def test_preset_plan_entries_tuple():
    e = PresetPlanEntry(kind="create_policy", target="p1", detail={"x": 1})
    plan = PresetPlan(
        preset_name="test",
        scope_key="platform",
        entries=(e,),
        warnings=("w1",),
    )
    assert len(plan.entries) == 1
    assert plan.entries[0].kind == "create_policy"
    assert plan.warnings == ("w1",)


def test_preset_context_fields():
    ctx = PresetContext(
        db=MagicMock(),
        iam=MagicMock(),
        policy=MagicMock(),
        config=MagicMock(),
        tasks=None,
        cron=None,
        libs=None,
        principal=None,
        scope="platform",
    )
    assert ctx.scope == "platform"


# ---------------------------------------------------------------------------
# Registry — composite validation at registration time
# ---------------------------------------------------------------------------

def test_composite_compose_validates_children():
    """Registering a composite with an unknown child should raise ValueError."""
    name = _unique_name("composite")

    class _BadComposite(CompositePreset):
        name = _unique_name("bad-composite")
        description = "bad"
        keywords: ClassVar[Tuple[str, ...]] = ("composite",)
        tier = PresetTier.PLATFORM
        catalog_scopable = False
        compose = ("nonexistent-child-xyz-123",)

    with pytest.raises(ValueError, match="unknown child preset"):
        register_preset(_BadComposite())


def test_composite_compose_validates_known_children():
    """Registering a composite with all known children succeeds."""
    child_name = _unique_name("composite-child")

    child = _MinimalPreset(name=child_name)
    child.tier = PresetTier.PLATFORM
    register_preset(child)

    composite_name = _unique_name("composite-parent")

    class _GoodComposite(CompositePreset):
        name = composite_name
        description = "good"
        keywords: ClassVar[Tuple[str, ...]] = ("composite",)
        tier = PresetTier.PLATFORM
        catalog_scopable = False
        compose = (child_name,)

    # Should not raise.
    _GoodComposite.name = composite_name
    register_preset(_GoodComposite())
    assert find_preset(composite_name) is not None


def test_register_duplicate_preset_raises():
    name = _unique_name("dup")
    p = _MinimalPreset(name=name)
    register_preset(p)
    with pytest.raises(ValueError, match="already registered"):
        register_preset(_MinimalPreset(name=name))


# ---------------------------------------------------------------------------
# Search / discovery
# ---------------------------------------------------------------------------

def test_search_returns_all_by_default():
    result = search_presets()
    assert "items" in result
    assert isinstance(result["items"], list)


def test_search_filters_by_tier():
    result_platform = search_presets(tier=PresetTier.PLATFORM)
    result_catalog = search_presets(tier=PresetTier.CATALOG)
    for item in result_platform["items"]:
        assert item["tier"] == PresetTier.PLATFORM.value
    for item in result_catalog["items"]:
        assert item["tier"] == PresetTier.CATALOG.value


def test_search_by_q_filters_name():
    result = search_presets(q="public_catalog")
    names = [i["name"] for i in result["items"]]
    assert "public_catalog" in names


def test_search_by_name_prefix():
    result = search_presets(name="public")
    for item in result["items"]:
        assert item["name"].startswith("public")


def test_search_by_keywords_and():
    """AND-match: all supplied keywords must be present."""
    name = _unique_name("kw-preset")
    p = _MinimalPreset(name=name)
    p.keywords = ("alpha", "beta", "gamma")
    register_preset(p)

    # Both keywords present → found.
    r1 = search_presets(keywords=["alpha", "beta"])
    assert any(i["name"] == name for i in r1["items"])

    # One keyword missing → not found.
    r2 = search_presets(keywords=["alpha", "delta"])
    assert not any(i["name"] == name for i in r2["items"])


def test_search_pagination_cursor():
    result = search_presets(limit=2)
    if result["next_cursor"] is not None:
        page2 = search_presets(limit=2, cursor=result["next_cursor"])
        # Page 2 items must be strictly after the cursor.
        for item in page2["items"]:
            assert item["name"] > result["next_cursor"]


def test_search_result_entry_shape():
    result = search_presets(name="public_catalog", limit=1)
    assert result["items"]
    entry = result["items"][0]
    assert "name" in entry
    assert "description" in entry
    assert "keywords" in entry
    assert "tier" in entry
    assert "catalog_scopable" in entry
    assert "is_async" in entry
    assert "params_schema" in entry


# ---------------------------------------------------------------------------
# CompositePreset — apply / revoke round trip
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_composite_apply_calls_children_forward():
    name_a = _unique_name("comp-a")
    name_b = _unique_name("comp-b")

    class _ChildA:
        name = name_a
        description = "child a"
        keywords: ClassVar[Tuple[str, ...]] = ()
        tier = PresetTier.PLATFORM
        catalog_scopable = False
        params_model = NoParams
        is_async = False
        compose = None

        async def dry_run(self, params, scope, ctx): return PresetPlan(preset_name=self.name, scope_key=scope)
        async def apply(self, params, scope, ctx): return AppliedDescriptor(payload={"child": "a"})
        async def revoke(self, desc, ctx): return None

    class _ChildB:
        name = name_b
        description = "child b"
        keywords: ClassVar[Tuple[str, ...]] = ()
        tier = PresetTier.PLATFORM
        catalog_scopable = False
        params_model = NoParams
        is_async = False
        compose = None

        async def dry_run(self, params, scope, ctx): return PresetPlan(preset_name=self.name, scope_key=scope)
        async def apply(self, params, scope, ctx): return AppliedDescriptor(payload={"child": "b"})
        async def revoke(self, desc, ctx): return None

    register_preset(_ChildA())
    register_preset(_ChildB())

    comp_name = _unique_name("composite-ab")

    class _Comp(CompositePreset):
        name = comp_name
        description = "ab composite"
        keywords: ClassVar[Tuple[str, ...]] = ("composite",)
        tier = PresetTier.PLATFORM
        catalog_scopable = False
        compose = (name_a, name_b)

    _Comp.name = comp_name
    register_preset(_Comp())
    comp_inst = find_preset(comp_name)

    ctx = MagicMock()
    result = await comp_inst.apply(NoParams(), "platform", ctx)
    assert isinstance(result, AppliedDescriptor)
    children = result.payload["children"]
    assert children[0]["name"] == name_a
    assert children[1]["name"] == name_b


@pytest.mark.asyncio
async def test_composite_revoke_calls_children_reverse():
    name_c = _unique_name("comp-c")
    name_d = _unique_name("comp-d")
    revoke_order: list[str] = []

    class _ChildC:
        name = name_c
        description = "child c"
        keywords: ClassVar[Tuple[str, ...]] = ()
        tier = PresetTier.PLATFORM
        catalog_scopable = False
        params_model = NoParams
        is_async = False
        compose = None

        async def dry_run(self, params, scope, ctx): return PresetPlan(preset_name=self.name, scope_key=scope)
        async def apply(self, params, scope, ctx): return AppliedDescriptor(payload={"child": "c"})
        async def revoke(self, desc, ctx):
            revoke_order.append("c")
            return None

    class _ChildD:
        name = name_d
        description = "child d"
        keywords: ClassVar[Tuple[str, ...]] = ()
        tier = PresetTier.PLATFORM
        catalog_scopable = False
        params_model = NoParams
        is_async = False
        compose = None

        async def dry_run(self, params, scope, ctx): return PresetPlan(preset_name=self.name, scope_key=scope)
        async def apply(self, params, scope, ctx): return AppliedDescriptor(payload={"child": "d"})
        async def revoke(self, desc, ctx):
            revoke_order.append("d")
            return None

    register_preset(_ChildC())
    register_preset(_ChildD())

    comp_name = _unique_name("composite-cd")

    class _Comp(CompositePreset):
        name = comp_name
        description = "cd composite"
        keywords: ClassVar[Tuple[str, ...]] = ("composite",)
        tier = PresetTier.PLATFORM
        catalog_scopable = False
        compose = (name_c, name_d)

    _Comp.name = comp_name
    register_preset(_Comp())
    comp_inst = find_preset(comp_name)
    ctx = MagicMock()

    descriptor = AppliedDescriptor(payload={
        "children": [
            {"name": name_c, "descriptor": {"child": "c"}},
            {"name": name_d, "descriptor": {"child": "d"}},
        ],
        "scope": "platform",
    })
    await comp_inst.revoke(descriptor, ctx)
    # Revoke must be in reverse order: d then c.
    assert revoke_order == ["d", "c"]
