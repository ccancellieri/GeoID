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

"""Catalog/collection-tier config reads must tolerate legacy stored keys.

Companion to ``db_config/unit/test_config_evolution.py``'s platform-tier
coverage (#2626). ``ScalingPolicyConfig`` split its stored ``cooldown_seconds``
field into ``scale_out_cooldown_seconds`` / ``scale_in_cooldown_seconds``;
#2626 fixed the platform-tier read path but every catalog/collection-tier read
in ``dynastore.modules.catalog.config_service`` still called a bare
``cls.model_validate(...)`` on the stored row, so ``GET /configs/catalogs/
{catalog_id}`` 500s the moment a pre-rename row is read back
(``extra_forbidden``). These tests reproduce that at the ``ConfigService``
layer (no DB — the physical-schema resolution and query executors are
mocked) and confirm every affected read now degrades to a dropped-key warning
instead of raising.
"""

from __future__ import annotations

from contextlib import asynccontextmanager
from typing import ClassVar, Tuple
from unittest.mock import AsyncMock, MagicMock

import pytest
from pydantic import ValidationError

from dynastore.models.mutability import Mutable
from dynastore.models.plugin_config import PluginConfig


class _LegacyShapeConfig(PluginConfig):
    """Stand-in for a config class as it existed when a stored row was written."""

    _address: ClassVar[Tuple[str, ...]] = ("platform", "_test_legacy_shape_config")
    a: Mutable[int] = 0
    b: Mutable[int] = 0


class _EvolvedShapeConfig(PluginConfig):
    """Current shape: ``b`` was removed/renamed since the row above was written.

    Shares ``_LegacyShapeConfig``'s stored ``class_key`` semantics are
    irrelevant here — tests dispatch by the class object directly, mirroring
    how ``resolve_config_class`` would hand back today's registered class for
    a class_key discriminator stored on an older row.
    """

    _address: ClassVar[Tuple[str, ...]] = ("platform", "_test_evolved_shape_config")
    a: Mutable[int] = 0


def _legacy_row() -> dict:
    """A stored-row payload carrying the legacy ``b`` key."""
    return _LegacyShapeConfig(a=1, b=2).model_dump()


def _make_service() -> "ConfigService":
    from dynastore.modules.catalog.config_service import ConfigService

    return ConfigService(engine=MagicMock(), catalog_manager=MagicMock())


@asynccontextmanager
async def _fake_txn(_engine):
    yield MagicMock()


def _wire_common_mocks(monkeypatch, svc, phys_schema: str = "phys_test"):
    import dynastore.modules.catalog.config_service as svc_mod

    monkeypatch.setattr(svc_mod, "managed_transaction", _fake_txn)
    monkeypatch.setattr(svc_mod, "DriverContext", MagicMock)
    monkeypatch.setattr(svc_mod, "check_table_exists", AsyncMock(return_value=True))
    mock_mgr = MagicMock()
    mock_mgr.resolve_physical_schema = AsyncMock(return_value=phys_schema)
    svc._get_catalog_manager = MagicMock(return_value=mock_mgr)
    return svc_mod, mock_mgr


# ---------------------------------------------------------------------------
# The exact production bug: GET /configs/catalogs/{catalog_id} → list_configs
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_catalog_list_configs_tolerates_legacy_key_and_warns(monkeypatch, caplog):
    """list_configs(catalog_id=...) must not 500 on a stored row carrying a
    key the live class no longer declares — this is the exact path
    ``GET /configs/catalogs/{catalog_id}?resolved=true&view=effective`` hits
    (via ``ConfigApiService._get_effective_configs``)."""
    svc = _make_service()
    svc_mod, _mgr = _wire_common_mocks(monkeypatch, svc)

    row = {
        "class_key": _EvolvedShapeConfig.class_key(),
        "config_data": _legacy_row(),
    }
    mock_query = AsyncMock(return_value=([row], 1))
    monkeypatch.setattr(svc_mod._cq, "list_catalog_configs_paginated", mock_query)

    with caplog.at_level(
        "WARNING", logger="dynastore.modules.db_config.stored_config_read"
    ):
        result = await svc.list_configs(catalog_id="cat1", limit=100, offset=0)

    assert result["total"] == 1
    assert result["results"][0]["config"] == {"a": 1}
    assert any(
        "legacy key" in r.getMessage() and "'b'" in r.getMessage()
        for r in caplog.records
    ), "must warn naming the dropped legacy key"


@pytest.mark.asyncio
async def test_collection_list_configs_tolerates_legacy_key(monkeypatch):
    """list_configs(catalog_id=..., collection_id=...) — the collection-tier
    twin of the catalog-scope read above."""
    svc = _make_service()
    svc_mod, mock_mgr = _wire_common_mocks(monkeypatch, svc)
    mock_mgr.collections.resolve_collection_ids = AsyncMock(
        side_effect=Exception("no external resolver in this unit test")
    )

    row = {
        "class_key": _EvolvedShapeConfig.class_key(),
        "config_data": _legacy_row(),
    }
    mock_query = AsyncMock(return_value=([row], 1))
    monkeypatch.setattr(svc_mod._cq, "list_collection_configs_paginated", mock_query)

    result = await svc.list_configs(
        catalog_id="cat1", collection_id="col1", limit=100, offset=0
    )

    assert result["total"] == 1
    assert result["results"][0]["config"] == {"a": 1}


# ---------------------------------------------------------------------------
# list_catalog_configs (bulk, non-paginated — feeds the #1079 defaults snapshot)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_list_catalog_configs_bulk_tolerates_legacy_key(monkeypatch):
    svc = _make_service()
    svc_mod, _mgr = _wire_common_mocks(monkeypatch, svc)

    row = {"class_key": _EvolvedShapeConfig.class_key(), "config_data": _legacy_row()}
    mock_query = MagicMock()
    mock_query.return_value.execute = AsyncMock(return_value=[row])
    monkeypatch.setattr(svc_mod._cq, "list_catalog_configs", mock_query)

    configs = await svc.list_catalog_configs("cat1")

    assert configs[_EvolvedShapeConfig.class_key()].a == 1
    assert not hasattr(configs[_EvolvedShapeConfig.class_key()], "b")


# ---------------------------------------------------------------------------
# get_config_by_ref (catalog tier) — via _materialise_ref_row
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_catalog_get_config_by_ref_tolerates_legacy_key(monkeypatch):
    svc = _make_service()
    svc_mod, _mgr = _wire_common_mocks(monkeypatch, svc)

    row = {"class_key": _EvolvedShapeConfig.class_key(), "config_data": _legacy_row()}
    mock_query = MagicMock()
    mock_query.return_value.execute = AsyncMock(return_value=row)
    monkeypatch.setattr(svc_mod._cq, "select_catalog_config_by_ref", mock_query)

    cfg = await svc.get_config_by_ref("some_ref", catalog_id="cat1")

    assert cfg is not None
    assert cfg.a == 1
    assert not hasattr(cfg, "b")


# ---------------------------------------------------------------------------
# get_config() resolved waterfall merge — deltas carrying a legacy key
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_get_config_waterfall_merge_tolerates_legacy_delta(monkeypatch):
    """The catalog-tier delta merged on top of the platform base must not
    inject a key the live class no longer declares into the final
    ``model_validate`` call."""
    svc = _make_service()
    svc_mod, _mgr = _wire_common_mocks(monkeypatch, svc)

    platform_svc = MagicMock()
    platform_svc.get_config = AsyncMock(return_value=_EvolvedShapeConfig(a=0))
    svc._get_platform_config_service = MagicMock(return_value=platform_svc)

    # Catalog delta is read via the cached fetcher — patch it directly to
    # avoid standing up the full cache/DB machinery.
    svc.get_catalog_config_internal_cached = AsyncMock(
        return_value={"a": 1, "b": 2}
    )

    result = await svc.get_config(_EvolvedShapeConfig, catalog_id="cat1")

    assert result.a == 1
    assert not hasattr(result, "b")


# ---------------------------------------------------------------------------
# RMW: old-config read for immutability compare (_enforce_write_immutability)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_enforce_write_immutability_tolerates_legacy_current_data(monkeypatch):
    """The RMW read of the stored row (compared against the incoming write for
    Immutable/WriteOnce enforcement) must not crash on a legacy key.

    ``enforce_config_immutability`` itself is stubbed out here — its
    materialization probe needs a real DB connection, which is out of scope
    for this test; the point under test is that reading ``current_data``
    (carrying the legacy ``b`` key) into a ``current_config`` instance
    doesn't raise ``extra_forbidden`` before enforcement ever runs.
    """
    import dynastore.modules.catalog.config_service as svc_mod

    svc = _make_service()
    enforce_mock = AsyncMock()
    monkeypatch.setattr(svc_mod, "enforce_config_immutability", enforce_mock)

    await svc._enforce_write_immutability(
        _EvolvedShapeConfig,
        _EvolvedShapeConfig(a=5),
        _legacy_row(),
        "cat1",
        None,
        conn=MagicMock(),
    )

    enforce_mock.assert_awaited_once()
    old_config_arg = enforce_mock.await_args.args[0]
    assert isinstance(old_config_arg, _EvolvedShapeConfig)
    assert old_config_arg.a == 1
    assert not hasattr(old_config_arg, "b")


# ---------------------------------------------------------------------------
# Negative: a genuine validation error on a KNOWN field must still raise.
# ---------------------------------------------------------------------------


def test_validate_stored_config_still_raises_on_real_error_at_catalog_layer():
    """Stripping unknown keys must not mask genuine corruption: a known field
    with a wrong-typed value still raises, mirroring the #2626 platform-tier
    guarantee at the catalog/collection read path's shared helper."""
    from dynastore.modules.db_config.stored_config_read import _validate_stored_config

    with pytest.raises(ValidationError):
        _validate_stored_config(_EvolvedShapeConfig, {"a": "not-an-int"})


def test_hoisted_helper_is_a_single_shared_function_not_a_duplicate():
    """The platform tier's re-export must be the SAME object as the shared
    helper — no copy-pasted second implementation."""
    from dynastore.modules.db_config.platform_config_service import (
        _validate_stored_config as platform_reexport,
    )
    from dynastore.modules.db_config.stored_config_read import (
        _validate_stored_config as shared_impl,
    )

    assert platform_reexport is shared_impl
