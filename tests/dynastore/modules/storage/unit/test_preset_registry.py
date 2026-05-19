"""RoutingPreset protocol + registry (#847)."""
from __future__ import annotations

import pytest

from typing import ClassVar

from dynastore.modules.storage.presets import (
    PresetBundle,
    PresetTier,
    RoutingPreset,
    get_preset,
    list_presets,
    register_preset,
)


class _DummyPreset:
    tier: ClassVar[PresetTier] = PresetTier.CATALOG

    def __init__(self, name: str = "demo-preset") -> None:
        self.name = name
        self.description = "demo"

    def build(self, catalog_id: str) -> PresetBundle:  # noqa: ARG002
        return PresetBundle()


def test_protocol_runtime_checkable_accepts_dummy():
    """A class with ``name``, ``description``, ``tier``, ``build()``
    satisfies the structural ``RoutingPreset`` protocol."""
    assert isinstance(_DummyPreset(), RoutingPreset)


def test_register_and_get_round_trip():
    name = "round-trip-preset"
    preset = _DummyPreset(name=name)
    register_preset(preset)
    assert get_preset(name) is preset
    assert name in list_presets()


def test_register_duplicate_rejected():
    preset = _DummyPreset(name="dup-preset")
    register_preset(preset)
    with pytest.raises(ValueError, match="already registered"):
        register_preset(_DummyPreset(name="dup-preset"))


def test_get_unknown_preset_raises_key_error():
    with pytest.raises(KeyError, match="no-such-preset"):
        get_preset("no-such-preset")


def test_list_presets_returns_sorted_names():
    register_preset(_DummyPreset(name="zzz-list-preset"))
    register_preset(_DummyPreset(name="aaa-list-preset"))
    names = list_presets()
    relevant = [n for n in names if n.endswith("-list-preset")]
    assert relevant == sorted(relevant)
