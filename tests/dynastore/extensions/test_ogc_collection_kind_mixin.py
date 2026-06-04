#    Copyright 2025 FAO
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

"""Unit tests for ``OGCServiceMixin`` collection-kind classification (#1510 M-5).

``_collection_kind`` / ``_filter_collections_by_kind`` read the Phase-1.6
``CollectionInfo`` SSOT (``collection_type`` was hoisted off the per-driver
config) and fail open to ``VECTOR`` on any read problem — the contract WFS and
Records both consolidate onto here.
"""
from __future__ import annotations

from typing import Any, Dict, Optional

import pytest
from fastapi import HTTPException

from dynastore.extensions.ogc_base import OGCServiceMixin
from dynastore.modules.catalog.catalog_config import CollectionInfo, CollectionKind

_MISSING = object()
_RAISE_SERVICE = object()


class _FakeConfigs:
    """Stub ConfigsProtocol: maps collection_id -> CollectionInfo | None | Exception.

    A collection_id absent from the map returns a default ``CollectionInfo()``
    (kind=VECTOR), mirroring the configs waterfall's default-construction.
    """

    def __init__(self, by_id: Dict[Optional[str], Any]):
        self._by_id = by_id

    async def get_config(
        self,
        config_cls: Any,
        catalog_id: Optional[str] = None,
        collection_id: Optional[str] = None,
        **_kw: Any,
    ) -> Any:
        val = self._by_id.get(collection_id, _MISSING)
        if isinstance(val, Exception):
            raise val
        if val is _MISSING:
            return CollectionInfo()
        return val


class _Svc(OGCServiceMixin):
    """Minimal mixin host with a stubbed configs service."""

    def __init__(self, configs: Any):
        self._configs = configs

    async def _get_configs_service(self) -> Any:  # type: ignore[override]
        if self._configs is _RAISE_SERVICE:
            raise HTTPException(status_code=500, detail="Configs service not available.")
        return self._configs


class _Coll:
    def __init__(self, cid: str):
        self.id = cid


@pytest.mark.asyncio
async def test_records_kind_resolved():
    svc = _Svc(_FakeConfigs({"c1": CollectionInfo(kind=CollectionKind.RECORDS)}))
    assert await svc._collection_kind("cat", "c1") == CollectionKind.RECORDS


@pytest.mark.asyncio
async def test_vector_kind_resolved():
    svc = _Svc(_FakeConfigs({"c1": CollectionInfo(kind=CollectionKind.VECTOR)}))
    assert await svc._collection_kind("cat", "c1") == CollectionKind.VECTOR


@pytest.mark.asyncio
async def test_missing_config_defaults_to_vector():
    # Unmapped id -> default CollectionInfo() -> VECTOR.
    svc = _Svc(_FakeConfigs({}))
    assert await svc._collection_kind("cat", "unknown") == CollectionKind.VECTOR


@pytest.mark.asyncio
async def test_none_config_fails_open_to_vector():
    svc = _Svc(_FakeConfigs({"c1": None}))
    assert await svc._collection_kind("cat", "c1") == CollectionKind.VECTOR


@pytest.mark.asyncio
async def test_get_config_error_fails_open_to_vector():
    svc = _Svc(_FakeConfigs({"c1": RuntimeError("config read blew up")}))
    assert await svc._collection_kind("cat", "c1") == CollectionKind.VECTOR


@pytest.mark.asyncio
async def test_configs_service_unavailable_fails_open_to_vector():
    svc = _Svc(_RAISE_SERVICE)
    assert await svc._collection_kind("cat", "c1") == CollectionKind.VECTOR


@pytest.mark.asyncio
async def test_filter_collections_by_kind_selects_matching():
    svc = _Svc(
        _FakeConfigs(
            {
                "rec1": CollectionInfo(kind=CollectionKind.RECORDS),
                "vec1": CollectionInfo(kind=CollectionKind.VECTOR),
                "rec2": CollectionInfo(kind=CollectionKind.RECORDS),
                "ras1": CollectionInfo(kind=CollectionKind.RASTER),
            }
        )
    )
    colls = [_Coll("rec1"), _Coll("vec1"), _Coll("rec2"), _Coll("ras1")]
    matched = await svc._filter_collections_by_kind("cat", colls, CollectionKind.RECORDS)
    assert [c.id for c in matched] == ["rec1", "rec2"]


@pytest.mark.asyncio
async def test_filter_collections_by_kind_accepts_dict_shape():
    svc = _Svc(_FakeConfigs({"d1": CollectionInfo(kind=CollectionKind.RECORDS)}))
    matched = await svc._filter_collections_by_kind(
        "cat", [{"id": "d1"}], CollectionKind.RECORDS
    )
    assert matched == [{"id": "d1"}]


@pytest.mark.asyncio
async def test_filter_collections_by_kind_skips_idless():
    svc = _Svc(_FakeConfigs({"rec1": CollectionInfo(kind=CollectionKind.RECORDS)}))
    # An id-less dict is skipped without error; the real collection still matches.
    matched = await svc._filter_collections_by_kind(
        "cat", [{}, _Coll("rec1")], CollectionKind.RECORDS
    )
    assert [c.id for c in matched] == ["rec1"]
