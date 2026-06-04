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

"""Regression: ``RecordsService._is_records_collection`` reads the Phase-1.6
``CollectionInfo`` SSOT, not the (now collection_type-less) driver config.

Before #1510 M-5 the check read ``getattr(driver_config, "collection_type",
"VECTOR")``; Phase 1.6 removed that attribute from the PG driver config, so the
check silently fell back to ``VECTOR`` and classified *every* collection as
not-records — Records listed nothing and 404'd every get. These tests pin the
corrected delegation to ``OGCServiceMixin._collection_kind``.
"""
from __future__ import annotations

from typing import Any, Optional

import pytest

from dynastore.extensions.records.records_service import RecordsService
from dynastore.modules.catalog.catalog_config import CollectionInfo, CollectionKind


class _FakeConfigs:
    def __init__(self, kind: CollectionKind):
        self._kind = kind

    async def get_config(
        self,
        config_cls: Any,
        catalog_id: Optional[str] = None,
        collection_id: Optional[str] = None,
        **_kw: Any,
    ) -> Any:
        return CollectionInfo(kind=self._kind)


class _Coll:
    def __init__(self, cid: str):
        self.id = cid


def _service(kind: CollectionKind, monkeypatch) -> RecordsService:
    svc = RecordsService.__new__(RecordsService)
    fake = _FakeConfigs(kind)

    async def _get_configs() -> Any:
        return fake

    monkeypatch.setattr(svc, "_get_configs_service", _get_configs, raising=False)
    return svc


@pytest.mark.asyncio
async def test_records_collection_is_detected(monkeypatch):
    svc = _service(CollectionKind.RECORDS, monkeypatch)
    assert await svc._is_records_collection("cat", _Coll("col_records")) is True


@pytest.mark.asyncio
async def test_vector_collection_is_not_records(monkeypatch):
    svc = _service(CollectionKind.VECTOR, monkeypatch)
    assert await svc._is_records_collection("cat", _Coll("col_vector")) is False


@pytest.mark.asyncio
async def test_idless_collection_is_not_records(monkeypatch):
    svc = _service(CollectionKind.RECORDS, monkeypatch)
    # An id-less dict-shaped collection short-circuits to False without a lookup.
    assert await svc._is_records_collection("cat", {}) is False
