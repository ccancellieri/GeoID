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

"""Validated-config memoization must skip re-validation on cache hits (#2946).

``_get_platform_config_internal`` used to re-run ``_validate_stored_config`` on
EVERY ``get_config`` call, even when the underlying raw-dict cache returned the
exact same object. For routing configs that re-fires an ``@model_validator`` that
self-registers drivers — a several-hundred-MB allocation spike. On cold-wake up
to ``max_concurrent_detached_rebuilds`` do this concurrently, driving the OOM
kills tracked in #2946.

The fix memoizes the validated model keyed on ``class_key`` + the *identity* of
the source raw dict. These tests pin the three invariants:

1. Same raw-dict object returned twice => validation runs exactly once.
2. A new raw-dict object (cache refresh) => re-validation runs.
3. A cache invalidation drops the memo => next read re-validates.
"""

from __future__ import annotations

from typing import ClassVar, Tuple
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from dynastore.models.mutability import Mutable
from dynastore.models.plugin_config import PluginConfig


class _MemoTestConfig(PluginConfig):
    _address: ClassVar[Tuple[str, ...]] = ("platform", "_test_memo_config_2946")
    x: Mutable[int] = 0


def _make_service():
    """Build a PlatformConfigService without triggering __init__/DB connections,
    but with the #2946 validated-config memo initialised (normally set in
    __init__)."""
    from dynastore.modules.db_config.platform_config_service import (
        PlatformConfigService,
    )

    svc = PlatformConfigService.__new__(PlatformConfigService)
    svc._engine = AsyncMock()
    svc.get_platform_config_internal_cached = MagicMock()
    svc._validated_config_cache = {}
    return svc


_VALIDATE_PATH = (
    "dynastore.modules.db_config.platform_config_service._validate_stored_config"
)


@pytest.mark.asyncio
async def test_same_raw_dict_validates_once():
    """Two reads returning the SAME dict object validate only once."""
    svc = _make_service()
    raw = {"x": 1}
    svc.get_platform_config_internal_cached = AsyncMock(return_value=raw)

    with patch(_VALIDATE_PATH) as m_validate:
        m_validate.return_value = _MemoTestConfig(x=1)

        first = await svc._get_platform_config_internal(_MemoTestConfig)
        second = await svc._get_platform_config_internal(_MemoTestConfig)

    assert m_validate.call_count == 1, "validation must be memoized on identity hit"
    assert first is second, "the same validated model instance must be reused"


@pytest.mark.asyncio
async def test_new_raw_dict_triggers_revalidation():
    """A cache refresh (new dict object, even if equal) re-validates once."""
    svc = _make_service()
    raw_a = {"x": 1}
    raw_b = {"x": 1}  # equal value, different identity — simulates TTL refresh
    svc.get_platform_config_internal_cached = AsyncMock(side_effect=[raw_a, raw_b])

    with patch(_VALIDATE_PATH) as m_validate:
        m_validate.side_effect = [_MemoTestConfig(x=1), _MemoTestConfig(x=1)]

        await svc._get_platform_config_internal(_MemoTestConfig)
        await svc._get_platform_config_internal(_MemoTestConfig)

    assert m_validate.call_count == 2, "a new source dict object must re-validate"


@pytest.mark.asyncio
async def test_invalidation_drops_memo():
    """_invalidate_config_cache clears the validated memo, forcing re-validation
    even when the raw-dict cache would return the same object again."""
    svc = _make_service()
    raw = {"x": 1}
    svc.get_platform_config_internal_cached = AsyncMock(return_value=raw)
    svc.get_platform_config_internal_cached.cache_invalidate = MagicMock()

    with patch(_VALIDATE_PATH) as m_validate:
        m_validate.return_value = _MemoTestConfig(x=1)

        await svc._get_platform_config_internal(_MemoTestConfig)
        svc._invalidate_config_cache(_MemoTestConfig.class_key())
        await svc._get_platform_config_internal(_MemoTestConfig)

    assert m_validate.call_count == 2, "invalidation must drop the validated memo"
    svc.get_platform_config_internal_cached.cache_invalidate.assert_called_once_with(
        _MemoTestConfig.class_key()
    )


@pytest.mark.asyncio
async def test_explicit_db_resource_never_memoizes():
    """A read via an explicit db_resource bypasses both caches for
    read-your-writes consistency and must NOT populate the memo."""
    svc = _make_service()
    db_resource = MagicMock()

    with patch(_VALIDATE_PATH) as m_validate, patch(
        "dynastore.modules.db_config.platform_config_service._platform_table_exists",
        new=AsyncMock(return_value=True),
    ), patch(
        "dynastore.modules.db_config.platform_config_service.get_platform_config_query"
    ) as m_query:
        m_query.execute = AsyncMock(return_value={"x": 7})
        m_validate.return_value = _MemoTestConfig(x=7)

        await svc._get_platform_config_internal(
            _MemoTestConfig, db_resource=db_resource
        )

    assert svc._validated_config_cache == {}, "explicit-connection reads must not memoize"
    assert m_validate.call_count == 1
