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

"""DB-free unit tests for ``DimensionsColdBootContributor`` (IAM-free path).

Verifies the first-run-only contract without touching IAM: the
``common_dimensions`` preset is applied (and a ``catalog.shared_properties``
marker set) exactly once — when the marker is absent and the process has a
catalog — and skipped otherwise. All protocol / lock / preset I/O is mocked.
"""
from __future__ import annotations

from contextlib import asynccontextmanager
from unittest.mock import AsyncMock, MagicMock

import pytest

from dynastore.extensions.dimensions.cold_boot_contributor import (
    PRESET_NAME,
    DimensionsColdBootContributor,
)


def _props(value=None):
    """A PropertiesProtocol-shaped mock; get_property returns *value*."""
    p = MagicMock()
    p.get_property = AsyncMock(return_value=value)
    p.set_property = AsyncMock(return_value=1)
    return p


def _get_protocol_factory(catalogs, props):
    """Fake get_protocol resolving by protocol class name."""
    def _gp(proto):
        name = getattr(proto, "__name__", "")
        if name == "CatalogsProtocol":
            return catalogs
        if name == "PropertiesProtocol":
            return props
        return None
    return _gp


def _patch_lock(monkeypatch, conn):
    """Patch acquire_startup_lock to an async CM yielding *conn* (or None)."""
    @asynccontextmanager
    async def _lock(_engine, _key, timeout=None):
        yield conn
    monkeypatch.setattr(
        "dynastore.modules.db_config.locking_tools.acquire_startup_lock", _lock,
    )


def _patch_preset(monkeypatch, preset):
    """Patch find_preset + _build_context so apply runs without a real DB."""
    monkeypatch.setattr(
        "dynastore.modules.storage.presets.registry.find_preset",
        lambda _name: preset,
    )
    monkeypatch.setattr(
        "dynastore.modules.storage.presets.lifecycle._build_context",
        lambda *a, **k: object(),
    )


def test_contributor_metadata() -> None:
    c = DimensionsColdBootContributor()
    assert c.name == "dimensions"
    # Runs after IAM (100) / auth (40) where present.
    assert c.priority < 40


async def test_skip_when_no_engine(monkeypatch: pytest.MonkeyPatch) -> None:
    preset = MagicMock(apply=AsyncMock())
    _patch_preset(monkeypatch, preset)
    await DimensionsColdBootContributor().run(engine=None)
    preset.apply.assert_not_called()


async def test_skip_when_no_catalogs(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        "dynastore.tools.discovery.get_protocol",
        _get_protocol_factory(catalogs=None, props=_props()),
    )
    preset = MagicMock(apply=AsyncMock())
    _patch_preset(monkeypatch, preset)
    await DimensionsColdBootContributor().run(engine=object())
    preset.apply.assert_not_called()


async def test_skip_when_no_properties(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        "dynastore.tools.discovery.get_protocol",
        _get_protocol_factory(catalogs=object(), props=None),
    )
    preset = MagicMock(apply=AsyncMock())
    _patch_preset(monkeypatch, preset)
    await DimensionsColdBootContributor().run(engine=object())
    preset.apply.assert_not_called()


async def test_skip_when_lock_busy(monkeypatch: pytest.MonkeyPatch) -> None:
    props = _props(value=None)
    monkeypatch.setattr(
        "dynastore.tools.discovery.get_protocol",
        _get_protocol_factory(catalogs=object(), props=props),
    )
    _patch_lock(monkeypatch, conn=None)  # lock not acquired
    preset = MagicMock(apply=AsyncMock())
    _patch_preset(monkeypatch, preset)
    await DimensionsColdBootContributor().run(engine=object())
    preset.apply.assert_not_called()
    props.set_property.assert_not_called()


async def test_skip_when_already_initialized(monkeypatch: pytest.MonkeyPatch) -> None:
    props = _props(value="true")  # marker present
    monkeypatch.setattr(
        "dynastore.tools.discovery.get_protocol",
        _get_protocol_factory(catalogs=object(), props=props),
    )
    _patch_lock(monkeypatch, conn=object())
    preset = MagicMock(apply=AsyncMock())
    _patch_preset(monkeypatch, preset)
    await DimensionsColdBootContributor().run(engine=object())
    preset.apply.assert_not_called()
    props.set_property.assert_not_called()


async def test_applies_and_marks_when_fresh(monkeypatch: pytest.MonkeyPatch) -> None:
    props = _props(value=None)  # marker absent → first run
    monkeypatch.setattr(
        "dynastore.tools.discovery.get_protocol",
        _get_protocol_factory(catalogs=object(), props=props),
    )
    _patch_lock(monkeypatch, conn=object())
    preset = MagicMock(apply=AsyncMock())
    _patch_preset(monkeypatch, preset)

    await DimensionsColdBootContributor().run(engine=object())

    preset.apply.assert_awaited_once()
    pos, _ = preset.apply.call_args
    assert pos[1] == "platform"  # scope_key
    props.set_property.assert_awaited_once()
    args, kwargs = props.set_property.call_args
    assert args[0] == "dimensions.common_dimensions_initialized"
    assert args[1] == "true"


async def test_no_mark_when_preset_unregistered(monkeypatch: pytest.MonkeyPatch) -> None:
    props = _props(value=None)
    monkeypatch.setattr(
        "dynastore.tools.discovery.get_protocol",
        _get_protocol_factory(catalogs=object(), props=props),
    )
    _patch_lock(monkeypatch, conn=object())
    _patch_preset(monkeypatch, preset=None)  # find_preset → None

    await DimensionsColdBootContributor().run(engine=object())

    props.set_property.assert_not_called()


def test_preset_name_constant() -> None:
    assert PRESET_NAME == "common_dimensions"
