"""Regression coverage for cached, resolved catalog config reads."""

from __future__ import annotations

from typing import ClassVar, Tuple
from unittest.mock import AsyncMock, MagicMock

import pytest

from dynastore.models.mutability import Mutable
from dynastore.models.plugin_config import PluginConfig


class _CachedWaterfallConfig(PluginConfig):
    _address: ClassVar[Tuple[str, ...]] = ("platform", "_test_cached_waterfall")

    value: Mutable[int] = 0
    extra: Mutable[int] = 0


@pytest.mark.asyncio
async def test_get_config_reuses_validated_model_for_same_cached_rows(monkeypatch):
    """Repeated cached reads must not re-run expensive model validators."""
    import dynastore.modules.catalog.config_service as svc_mod
    from dynastore.modules.catalog.config_service import ConfigService

    svc = ConfigService(engine=MagicMock(), catalog_manager=MagicMock())
    base = _CachedWaterfallConfig(value=1, extra=2)
    platform = MagicMock()
    platform.get_config = AsyncMock(return_value=base)
    svc._get_platform_config_service = MagicMock(return_value=platform)
    svc.get_catalog_defaults_snapshot = AsyncMock(return_value=None)

    catalog_delta = {"value": 3}
    collection_delta = {"extra": 5}
    svc.get_catalog_config_internal_cached = AsyncMock(return_value=catalog_delta)
    svc.get_collection_config_internal_cached = AsyncMock(return_value=collection_delta)

    original_validate = svc_mod._validate_stored_config
    validate_calls = []

    def _counting_validate(cls, data):
        validate_calls.append(dict(data))
        return original_validate(cls, data)

    monkeypatch.setattr(svc_mod, "_validate_stored_config", _counting_validate)

    first = await svc.get_config(
        _CachedWaterfallConfig,
        catalog_id="cat1",
        collection_id="col1",
    )
    second = await svc.get_config(
        _CachedWaterfallConfig,
        catalog_id="cat1",
        collection_id="col1",
    )

    assert second is first
    assert first.value == 3
    assert first.extra == 5
    assert len(validate_calls) == 1

    new_catalog_delta = {"value": 8}
    svc.get_catalog_config_internal_cached.return_value = new_catalog_delta

    third = await svc.get_config(
        _CachedWaterfallConfig,
        catalog_id="cat1",
        collection_id="col1",
    )

    assert third is not first
    assert third.value == 8
    assert third.extra == 5
    assert len(validate_calls) == 2
