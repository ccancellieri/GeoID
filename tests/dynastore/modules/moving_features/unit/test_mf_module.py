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

"""Unit tests for modules/moving_features/mf_module.py lifespan behaviour.

Verifies:
- A DDL failure propagates out of lifespan (not swallowed).
- All DDL constants are idempotent (IF NOT EXISTS guards present).
- The no-engine path yields cleanly without raising.
"""

from __future__ import annotations

from contextlib import asynccontextmanager
from unittest.mock import AsyncMock, MagicMock

import pytest

from dynastore.modules.moving_features.mf_module import (
    BBOX_INDEX_DDL,
    MOVING_FEATURES_DDL,
    MovingFeaturesModule,
    TEMPORAL_GEOMETRIES_DDL,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


@asynccontextmanager
async def _raise_on_enter(*_args, **_kwargs):
    raise RuntimeError("simulated DDL failure")
    yield  # unreachable; required by asynccontextmanager


@asynccontextmanager
async def _ok_conn(*_args, **_kwargs):
    yield AsyncMock()


# ---------------------------------------------------------------------------
# DDL idempotency (no DB required)
# ---------------------------------------------------------------------------


def test_moving_features_ddl_is_idempotent():
    assert "IF NOT EXISTS" in MOVING_FEATURES_DDL, (
        "MOVING_FEATURES_DDL must be idempotent (CREATE TABLE IF NOT EXISTS)"
    )


def test_temporal_geometries_ddl_is_idempotent():
    assert "IF NOT EXISTS" in TEMPORAL_GEOMETRIES_DDL, (
        "TEMPORAL_GEOMETRIES_DDL must be idempotent (CREATE TABLE IF NOT EXISTS)"
    )


def test_bbox_index_ddl_is_idempotent():
    assert "IF NOT EXISTS" in BBOX_INDEX_DDL, (
        "BBOX_INDEX_DDL must be idempotent (CREATE INDEX IF NOT EXISTS)"
    )


def test_bbox_geom_column_declared_before_index():
    """bbox_geom must be defined in TEMPORAL_GEOMETRIES_DDL so that
    BBOX_INDEX_DDL can reference it.  A regression here (e.g. the column
    removed from the table DDL) would cause index creation to fail with
    'column does not exist' on a fresh schema."""
    assert "bbox_geom" in TEMPORAL_GEOMETRIES_DDL, (
        "bbox_geom must be declared in TEMPORAL_GEOMETRIES_DDL "
        "before BBOX_INDEX_DDL references it"
    )
    assert "bbox_geom" in BBOX_INDEX_DDL, (
        "BBOX_INDEX_DDL must reference the bbox_geom column"
    )


# ---------------------------------------------------------------------------
# Lifespan — DDL failure must propagate (not be swallowed)
# ---------------------------------------------------------------------------


async def test_lifespan_raises_on_ddl_failure(monkeypatch):
    """If managed_transaction raises (e.g. DDL failure), the exception must
    propagate out of lifespan instead of being silently swallowed."""
    mock_db = MagicMock()
    mock_db.engine = MagicMock()

    monkeypatch.setattr(
        "dynastore.modules.moving_features.mf_module.get_protocol",
        lambda _proto: mock_db,
    )
    monkeypatch.setattr(
        "dynastore.modules.moving_features.mf_module.managed_transaction",
        _raise_on_enter,
    )

    module = MovingFeaturesModule()
    with pytest.raises(RuntimeError, match="simulated DDL failure"):
        async with module.lifespan(object()):
            pass  # must not be reached


# ---------------------------------------------------------------------------
# Lifespan — successful path yields normally
# ---------------------------------------------------------------------------


async def test_lifespan_yields_when_ddl_succeeds(monkeypatch):
    """When all DDL executes without error the lifespan must yield."""
    mock_conn = AsyncMock()
    mock_db = MagicMock()
    mock_db.engine = MagicMock()

    # managed_transaction yields the connection
    monkeypatch.setattr(
        "dynastore.modules.moving_features.mf_module.managed_transaction",
        _ok_conn,
    )

    # acquire_startup_lock yields the same connection
    @asynccontextmanager
    async def _ok_lock(*_args, **_kwargs):
        yield mock_conn

    monkeypatch.setattr(
        "dynastore.modules.moving_features.mf_module.maintenance_tools.acquire_startup_lock",
        _ok_lock,
    )
    monkeypatch.setattr(
        "dynastore.modules.moving_features.mf_module.maintenance_tools.ensure_schema_exists",
        AsyncMock(return_value=None),
    )
    monkeypatch.setattr(
        "dynastore.modules.moving_features.mf_module.get_protocol",
        lambda _proto: mock_db,
    )

    # DDLQuery instances are created inline; patch execute on the class
    monkeypatch.setattr(
        "dynastore.modules.moving_features.mf_module.DDLQuery.execute",
        AsyncMock(return_value=None),
    )

    module = MovingFeaturesModule()
    entered = False
    async with module.lifespan(object()):
        entered = True
    assert entered, "lifespan must yield after successful DDL"


# ---------------------------------------------------------------------------
# Lifespan — no engine → yield cleanly, no exception
# ---------------------------------------------------------------------------


async def test_lifespan_yields_when_no_engine(monkeypatch):
    """When the database engine is unavailable the module should yield
    without raising (it is optional, consistent with the no-engine guard)."""
    monkeypatch.setattr(
        "dynastore.modules.moving_features.mf_module.get_protocol",
        lambda _proto: None,
    )

    module = MovingFeaturesModule()
    entered = False
    async with module.lifespan(object()):
        entered = True
    assert entered
