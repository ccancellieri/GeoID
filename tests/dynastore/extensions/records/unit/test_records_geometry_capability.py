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

"""Unit tests for the RFC #2550 geometry capability gate on RECORDS collections.

``records_generator.collection_has_geometry`` resolves the effective sidecar
list (``CollectionInfo.kind`` + ``allow_geometry``) — the same source of truth
``ItemsPostgresqlDriver`` uses at write/read time — rather than re-reading
``allow_geometry`` in isolation. ``db_row_to_record`` gates whether a mapped
item's ``geometry``/``bbox`` are surfaced on the wire vs. forced to ``null``.
"""

from __future__ import annotations

from typing import Any, Optional

import pytest
from geojson_pydantic import Feature as _GeoJSONFeature

from dynastore.extensions.records import records_generator as rgen
from dynastore.modules.catalog.catalog_config import CollectionInfo, CollectionKind
from dynastore.modules.storage.driver_config import ItemsPostgresqlDriverConfig


class _FakeConfigs:
    """Stub ``ConfigsProtocol``: returns a fixed ``CollectionInfo`` and an
    empty (default-fast) ``ItemsPostgresqlDriverConfig`` for any collection.
    """

    def __init__(self, info: CollectionInfo):
        self._info = info

    async def get_config(
        self,
        config_cls: Any,
        catalog_id: Optional[str] = None,
        collection_id: Optional[str] = None,
        **_kw: Any,
    ) -> Any:
        if config_cls is CollectionInfo:
            return self._info
        if config_cls is ItemsPostgresqlDriverConfig:
            return ItemsPostgresqlDriverConfig()
        raise AssertionError(f"unexpected config class: {config_cls}")


class _RaisingConfigs:
    async def get_config(self, *a: Any, **k: Any) -> Any:
        raise RuntimeError("configs backend down")


# ---------------------------------------------------------------------------
# collection_has_geometry
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_records_default_has_no_geometry(monkeypatch):
    """(b) RECORDS default (``allow_geometry`` unset) → geometry-less, as today."""
    monkeypatch.setattr(
        rgen, "get_protocol",
        lambda _proto: _FakeConfigs(CollectionInfo(kind=CollectionKind.RECORDS)),
    )
    assert await rgen.collection_has_geometry("cat", "col") is False


@pytest.mark.asyncio
async def test_records_allow_geometry_true_has_geometry(monkeypatch):
    """(a) RECORDS + ``allow_geometry=True`` → geometry capability active."""
    monkeypatch.setattr(
        rgen, "get_protocol",
        lambda _proto: _FakeConfigs(
            CollectionInfo(kind=CollectionKind.RECORDS, allow_geometry=True)
        ),
    )
    assert await rgen.collection_has_geometry("cat", "col") is True


@pytest.mark.asyncio
async def test_vector_default_has_geometry(monkeypatch):
    """Default VECTOR keeps its geometry capability (byte-identical)."""
    monkeypatch.setattr(
        rgen, "get_protocol",
        lambda _proto: _FakeConfigs(CollectionInfo(kind=CollectionKind.VECTOR)),
    )
    assert await rgen.collection_has_geometry("cat", "col") is True


@pytest.mark.asyncio
async def test_vector_allow_geometry_false_has_no_geometry(monkeypatch):
    """(c) VECTOR + ``allow_geometry=False`` → geometry capability suppressed."""
    monkeypatch.setattr(
        rgen, "get_protocol",
        lambda _proto: _FakeConfigs(
            CollectionInfo(kind=CollectionKind.VECTOR, allow_geometry=False)
        ),
    )
    assert await rgen.collection_has_geometry("cat", "col") is False


@pytest.mark.asyncio
async def test_configs_unavailable_fails_closed(monkeypatch):
    monkeypatch.setattr(rgen, "get_protocol", lambda _proto: None)
    assert await rgen.collection_has_geometry("cat", "col") is False


@pytest.mark.asyncio
async def test_resolution_error_fails_closed(monkeypatch):
    monkeypatch.setattr(rgen, "get_protocol", lambda _proto: _RaisingConfigs())
    assert await rgen.collection_has_geometry("cat", "col") is False


@pytest.mark.asyncio
async def test_resolution_error_strict_reraises(monkeypatch):
    """Write paths pass ``strict=True``: a resolution failure must re-raise
    rather than fail closed, so a transient config-service hiccup cannot
    silently null a client-submitted geometry and persist the data loss.
    """
    monkeypatch.setattr(rgen, "get_protocol", lambda _proto: _RaisingConfigs())
    with pytest.raises(RuntimeError, match="configs backend down"):
        await rgen.collection_has_geometry("cat", "col", strict=True)


# ---------------------------------------------------------------------------
# db_row_to_record — geometry_enabled gate
# ---------------------------------------------------------------------------


def test_db_row_to_record_defaults_geometry_null():
    """Default (no ``geometry_enabled`` passed) stays byte-identical to the
    pre-#2645 behaviour: ``geometry`` always ``null``.
    """
    item = _GeoJSONFeature(
        type="Feature",
        geometry={"type": "Point", "coordinates": [1.0, 2.0]},
        properties={},
        id="rec1",
    )
    record = rgen.db_row_to_record(item, "cat", "col", "http://host", layer_config=None)
    assert record.geometry is None


def test_db_row_to_record_geometry_enabled_false_nulls_geometry():
    item = _GeoJSONFeature(
        type="Feature",
        geometry={"type": "Point", "coordinates": [1.0, 2.0]},
        properties={},
        id="rec1",
    )
    record = rgen.db_row_to_record(
        item, "cat", "col", "http://host", layer_config=None, geometry_enabled=False,
    )
    assert record.geometry is None


def test_db_row_to_record_geometry_enabled_true_preserves_geometry_and_bbox():
    item = _GeoJSONFeature(
        type="Feature",
        geometry={"type": "Point", "coordinates": [1.0, 2.0]},
        properties={},
        id="rec1",
        bbox=(1.0, 2.0, 1.0, 2.0),
    )
    record = rgen.db_row_to_record(
        item, "cat", "col", "http://host", layer_config=None, geometry_enabled=True,
    )
    assert record.geometry is not None
    assert record.geometry.type == "Point"
    assert record.model_dump(exclude_none=True)["bbox"] == (1.0, 2.0, 1.0, 2.0)


def test_db_row_to_record_geometry_enabled_true_no_bbox_omits_bbox():
    item = _GeoJSONFeature(
        type="Feature", geometry=None, properties={}, id="rec1",
    )
    record = rgen.db_row_to_record(
        item, "cat", "col", "http://host", layer_config=None, geometry_enabled=True,
    )
    assert record.geometry is None
    assert "bbox" not in record.model_dump(exclude_none=True)
