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

"""Regression coverage for #2710: task rows must never echo the internal
physical collection id on the REST surface.

Internal machinery (storage_drain, cascade_cleanup, ...) stores the
physical ``collection_id`` in the task row at spawn time. The
tasks read/serializer path must resolve it back to the collection's public
``external_id`` before it reaches the wire, reusing the same
``CollectionsProtocol.resolve_collection_external_id`` mapping the
catalog/collection CRUD read path already relies on.
"""
from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from dynastore.models.tasks import Task, TaskStatusEnum

_INTERNAL_COLLECTION_ID = "col_tv7476t25s3hf"
_EXTERNAL_COLLECTION_ID = "stations"
_CATALOG_ID = "s_abc123"


def _task(collection_id: str | None, task_type: str = "storage_drain") -> Task:
    return Task(
        task_type=task_type,
        status=TaskStatusEnum.COMPLETED,
        catalog_id=_CATALOG_ID,
        collection_id=collection_id,
        timestamp=datetime(2026, 1, 1, tzinfo=timezone.utc),
    )


def _fake_catalogs_service(resolution: str | None = _EXTERNAL_COLLECTION_ID):
    """Minimal stand-in for CatalogsProtocol exposing the reverse resolver,
    mirroring the STAC read path's own test double."""
    resolve_collection_external_id = AsyncMock(return_value=resolution)
    collections = SimpleNamespace(
        resolve_collection_external_id=resolve_collection_external_id
    )
    return SimpleNamespace(collections=collections)


class TestProjectCollectionExternalIds:
    @pytest.mark.asyncio
    async def test_internal_collection_id_is_projected(self, monkeypatch):
        import dynastore.extensions.tasks.tasks_service as svc

        catalogs_svc = _fake_catalogs_service()
        monkeypatch.setattr(svc, "get_protocol", lambda _p: catalogs_svc)

        task = _task(_INTERNAL_COLLECTION_ID)
        await svc._project_collection_external_ids([task])

        assert task.collection_id == _EXTERNAL_COLLECTION_ID
        catalogs_svc.collections.resolve_collection_external_id.assert_awaited_once_with(
            _CATALOG_ID, _INTERNAL_COLLECTION_ID, allow_missing=True
        )

    @pytest.mark.asyncio
    async def test_missing_collection_id_skips_lookup(self, monkeypatch):
        import dynastore.extensions.tasks.tasks_service as svc

        catalogs_svc = _fake_catalogs_service()
        monkeypatch.setattr(svc, "get_protocol", lambda _p: catalogs_svc)

        task = _task(None)
        await svc._project_collection_external_ids([task])

        assert task.collection_id is None
        catalogs_svc.collections.resolve_collection_external_id.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_unresolvable_id_fails_open(self, monkeypatch):
        """No live collection carries that internal id (e.g. hard-deleted
        collection): the stored value is left as-is rather than raising."""
        import dynastore.extensions.tasks.tasks_service as svc

        catalogs_svc = _fake_catalogs_service(resolution=None)
        monkeypatch.setattr(svc, "get_protocol", lambda _p: catalogs_svc)

        task = _task(_INTERNAL_COLLECTION_ID)
        await svc._project_collection_external_ids([task])

        assert task.collection_id == _INTERNAL_COLLECTION_ID

    @pytest.mark.asyncio
    async def test_no_catalogs_protocol_fails_open(self, monkeypatch):
        import dynastore.extensions.tasks.tasks_service as svc

        monkeypatch.setattr(svc, "get_protocol", lambda _p: None)

        task = _task(_INTERNAL_COLLECTION_ID)
        await svc._project_collection_external_ids([task])

        assert task.collection_id == _INTERNAL_COLLECTION_ID

    @pytest.mark.asyncio
    async def test_resolver_exception_fails_open(self, monkeypatch):
        """A resolver hiccup must never turn a 200 into a 500."""
        import dynastore.extensions.tasks.tasks_service as svc

        catalogs_svc = _fake_catalogs_service()
        catalogs_svc.collections.resolve_collection_external_id.side_effect = RuntimeError(
            "boom"
        )
        monkeypatch.setattr(svc, "get_protocol", lambda _p: catalogs_svc)

        task = _task(_INTERNAL_COLLECTION_ID)
        await svc._project_collection_external_ids([task])

        assert task.collection_id == _INTERNAL_COLLECTION_ID

    @pytest.mark.asyncio
    async def test_shared_collection_resolved_once_per_page(self, monkeypatch):
        """Many tasks sharing the same (catalog_id, collection_id) resolve once."""
        import dynastore.extensions.tasks.tasks_service as svc

        catalogs_svc = _fake_catalogs_service()
        monkeypatch.setattr(svc, "get_protocol", lambda _p: catalogs_svc)

        tasks = [_task(_INTERNAL_COLLECTION_ID) for _ in range(5)]
        await svc._project_collection_external_ids(tasks)

        assert all(t.collection_id == _EXTERNAL_COLLECTION_ID for t in tasks)
        catalogs_svc.collections.resolve_collection_external_id.assert_awaited_once_with(
            _CATALOG_ID, _INTERNAL_COLLECTION_ID, allow_missing=True
        )


class TestToTaskPageProjectsCollectionIds:
    @pytest.mark.asyncio
    async def test_list_route_page_projects_internal_collection_id(self, monkeypatch):
        """End-to-end through the shared list-route pagination helper (routes
        3, 5, 10, 11, 12 all funnel through _to_task_page)."""
        import dynastore.extensions.tasks.tasks_service as svc

        catalogs_svc = _fake_catalogs_service()
        monkeypatch.setattr(svc, "get_protocol", lambda _p: catalogs_svc)

        rows = [_task(_INTERNAL_COLLECTION_ID)]
        page = await svc._to_task_page(rows, limit=5)

        assert page.items[0].collection_id == _EXTERNAL_COLLECTION_ID


class TestGetTaskScopedUncachedProjectsCollectionId:
    @pytest.mark.asyncio
    async def test_scoped_get_projects_internal_collection_id(self, monkeypatch):
        """End-to-end through the scoped single-task getter (routes 4, 6)."""
        import uuid
        from unittest.mock import MagicMock

        import dynastore.extensions.tasks.tasks_service as svc

        catalogs_svc = _fake_catalogs_service()
        monkeypatch.setattr(svc, "get_protocol", lambda _p: catalogs_svc)

        task_id = uuid.uuid4()
        stored = _task(_INTERNAL_COLLECTION_ID)

        async def fake_schema(catalog_id, conn):
            return _CATALOG_ID

        async def fake_uncached(conn, tid):
            return stored

        monkeypatch.setattr(svc.tasks_module, "_resolve_catalog_schema", fake_schema)
        monkeypatch.setattr(svc.tasks_module, "get_task_by_id_unscoped", fake_uncached)

        task = await svc._get_task_scoped_uncached(task_id, "cat", MagicMock())

        assert task.collection_id == _EXTERNAL_COLLECTION_ID

    @pytest.mark.asyncio
    async def test_scoped_get_matches_physical_collection_id_against_external_url_id(
        self, monkeypatch
    ):
        """#2777: a task spawned by internal machinery stores the *physical*
        collection id on its row. A legitimate collection-scoped poll using
        the *external* URL id must NOT 404 — the cross-check has to resolve
        the stored physical id to external before comparing."""
        import uuid
        from unittest.mock import MagicMock

        import dynastore.extensions.tasks.tasks_service as svc

        catalogs_svc = _fake_catalogs_service()
        monkeypatch.setattr(svc, "get_protocol", lambda _p: catalogs_svc)

        task_id = uuid.uuid4()
        stored = _task(_INTERNAL_COLLECTION_ID)  # physical id, as spawned internally

        async def fake_schema(catalog_id, conn):
            return _CATALOG_ID

        async def fake_uncached(conn, tid):
            return stored

        monkeypatch.setattr(svc.tasks_module, "_resolve_catalog_schema", fake_schema)
        monkeypatch.setattr(svc.tasks_module, "get_task_by_id_unscoped", fake_uncached)

        task = await svc._get_task_scoped_uncached(
            task_id, "cat", MagicMock(), collection_id=_EXTERNAL_COLLECTION_ID
        )

        assert task.collection_id == _EXTERNAL_COLLECTION_ID

    @pytest.mark.asyncio
    async def test_scoped_get_still_404s_on_genuinely_different_collection(
        self, monkeypatch
    ):
        """A physical-id-stored task that resolves to a DIFFERENT collection
        than the URL's external id must still 404."""
        import uuid
        from unittest.mock import MagicMock

        import dynastore.extensions.tasks.tasks_service as svc
        from fastapi import HTTPException

        catalogs_svc = _fake_catalogs_service(resolution="a-different-collection")
        monkeypatch.setattr(svc, "get_protocol", lambda _p: catalogs_svc)

        task_id = uuid.uuid4()
        stored = _task(_INTERNAL_COLLECTION_ID)

        async def fake_schema(catalog_id, conn):
            return _CATALOG_ID

        async def fake_uncached(conn, tid):
            return stored

        monkeypatch.setattr(svc.tasks_module, "_resolve_catalog_schema", fake_schema)
        monkeypatch.setattr(svc.tasks_module, "get_task_by_id_unscoped", fake_uncached)

        with pytest.raises(HTTPException) as ei:
            await svc._get_task_scoped_uncached(
                task_id, "cat", MagicMock(), collection_id=_EXTERNAL_COLLECTION_ID
            )
        assert ei.value.status_code == 404
