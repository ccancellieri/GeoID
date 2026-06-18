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

"""Unit tests for ES driver geometry byte-budget resolver and fail-open simplify resolver.

Covers:
  - _clamp_geometry_budget: None → DEFAULT_MAX_BYTES; over-limit → DEFAULT_MAX_BYTES;
    valid value passes through; zero/negative → DEFAULT_MAX_BYTES.
  - _resolve_simplify_geometry: configs-unavailable branch returns True (fail open);
    missing config row returns True; exception returns True; explicit False returns False.
"""
from __future__ import annotations

import asyncio
from unittest.mock import patch

from dynastore.tools.geometry_simplify import DEFAULT_MAX_BYTES


# ---------------------------------------------------------------------------
# _clamp_geometry_budget — pure helper, imported from driver
# ---------------------------------------------------------------------------


def test_budget_none_is_default():
    from dynastore.modules.storage.drivers.elasticsearch import _clamp_geometry_budget

    assert _clamp_geometry_budget(None) == DEFAULT_MAX_BYTES


def test_budget_clamped_to_ceiling():
    from dynastore.modules.storage.drivers.elasticsearch import _clamp_geometry_budget

    assert _clamp_geometry_budget(50_000_000) == DEFAULT_MAX_BYTES


def test_budget_small_value_passes_through():
    from dynastore.modules.storage.drivers.elasticsearch import _clamp_geometry_budget

    assert _clamp_geometry_budget(1_000_000) == 1_000_000


def test_budget_zero_returns_default():
    from dynastore.modules.storage.drivers.elasticsearch import _clamp_geometry_budget

    assert _clamp_geometry_budget(0) == DEFAULT_MAX_BYTES


def test_budget_negative_returns_default():
    from dynastore.modules.storage.drivers.elasticsearch import _clamp_geometry_budget

    assert _clamp_geometry_budget(-500) == DEFAULT_MAX_BYTES


def test_budget_exactly_at_ceiling():
    from dynastore.modules.storage.drivers.elasticsearch import _clamp_geometry_budget

    assert _clamp_geometry_budget(DEFAULT_MAX_BYTES) == DEFAULT_MAX_BYTES


def test_budget_one_below_ceiling():
    from dynastore.modules.storage.drivers.elasticsearch import _clamp_geometry_budget

    assert _clamp_geometry_budget(DEFAULT_MAX_BYTES - 1) == DEFAULT_MAX_BYTES - 1


def test_budget_minimum_valid_value():
    from dynastore.modules.storage.drivers.elasticsearch import _clamp_geometry_budget

    assert _clamp_geometry_budget(1) == 1


# ---------------------------------------------------------------------------
# _resolve_simplify_geometry fail-open branches
# ---------------------------------------------------------------------------


class _FakeDriverConfig:
    """Sentinel sentinel config class so _resolve_simplify_geometry takes the get_protocol path."""


class _FakeDriver:
    """Minimal stand-in for _ItemsElasticsearchBase to test the resolver.

    Sets _driver_config_class to a non-None sentinel so the method takes the
    get_protocol / ConfigsProtocol waterfall path (not the public-driver path
    that calls get_driver_config).
    """

    _driver_config_class = _FakeDriverConfig

    async def _resolve_simplify_geometry(
        self,
        catalog_id,
        collection_id=None,
        *,
        db_resource=None,
    ) -> bool:
        from dynastore.modules.storage.drivers.elasticsearch import (
            _ItemsElasticsearchBase,
        )
        return await _ItemsElasticsearchBase._resolve_simplify_geometry(
            self, catalog_id, collection_id, db_resource=db_resource,
        )


def test_resolve_returns_true_when_configs_unavailable():
    """When get_protocol returns None (configs unavailable), resolver must return True."""
    driver = _FakeDriver()
    with patch(
        "dynastore.tools.discovery.get_protocol",
        return_value=None,
    ):
        result = asyncio.get_event_loop().run_until_complete(
            driver._resolve_simplify_geometry("cat1", "col1"),
        )
    assert result is True


def test_resolve_returns_true_when_get_config_raises():
    """When get_config raises, the resolver must return True (fail open)."""
    import asyncio as _asyncio

    async def _raise(*_a, **_kw):
        raise RuntimeError("db unavailable")

    driver = _FakeDriver()
    fake_configs = type("FakeConfigs", (), {"get_config": _raise})()
    with patch(
        "dynastore.tools.discovery.get_protocol",
        return_value=fake_configs,
    ):
        result = _asyncio.get_event_loop().run_until_complete(
            driver._resolve_simplify_geometry("cat1", "col1"),
        )
    assert result is True


def test_resolve_returns_true_when_config_row_missing():
    """When config row has no simplify_geometry attr, resolver must return True."""
    import asyncio as _asyncio

    class _ConfigRow:
        pass  # no simplify_geometry attribute

    async def _get_config(*_a, **_kw):
        return _ConfigRow()

    driver = _FakeDriver()
    fake_configs = type("FakeConfigs", (), {"get_config": _get_config})()
    with patch(
        "dynastore.tools.discovery.get_protocol",
        return_value=fake_configs,
    ):
        result = _asyncio.get_event_loop().run_until_complete(
            driver._resolve_simplify_geometry("cat1", "col1"),
        )
    assert result is True


def test_resolve_returns_false_when_explicitly_disabled():
    """When simplify_geometry == False in the config, resolver must return False."""
    import asyncio as _asyncio

    class _ConfigRow:
        simplify_geometry = False

    async def _get_config(*_a, **_kw):
        return _ConfigRow()

    driver = _FakeDriver()
    fake_configs = type("FakeConfigs", (), {"get_config": _get_config})()
    with patch(
        "dynastore.tools.discovery.get_protocol",
        return_value=fake_configs,
    ):
        result = _asyncio.get_event_loop().run_until_complete(
            driver._resolve_simplify_geometry("cat1", "col1"),
        )
    assert result is False


def test_resolve_returns_true_when_explicitly_enabled():
    """When simplify_geometry == True in the config, resolver must return True."""
    import asyncio as _asyncio

    class _ConfigRow:
        simplify_geometry = True

    async def _get_config(*_a, **_kw):
        return _ConfigRow()

    driver = _FakeDriver()
    fake_configs = type("FakeConfigs", (), {"get_config": _get_config})()
    with patch(
        "dynastore.tools.discovery.get_protocol",
        return_value=fake_configs,
    ):
        result = _asyncio.get_event_loop().run_until_complete(
            driver._resolve_simplify_geometry("cat1", "col1"),
        )
    assert result is True
