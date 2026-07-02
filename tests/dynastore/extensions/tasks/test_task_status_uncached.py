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

"""The catalog task-status route must read task status UNCACHED (and stay scoped).

A task's terminal status is written by the BackgroundRunner that owns
completion, which runs on whichever instance claimed the task — not necessarily
the API instance serving a status poll. The in-process ``get_task`` cache on
that API instance is not invalidated by the cross-instance flip, so a cached
read pins a finished task at its creation-time status (e.g. ``ACTIVE``) for the
whole cache TTL — leaving the status link the async hard-delete advertises via
``Location`` unable to ever observe ``COMPLETED``. The scoped route must
therefore use the uncached helper, mirroring the OGC Processes job-status route,
while still scoping the lookup to the catalog's tenant schema.

Also covers #2674: once the catalog registry row has been fully purged by its
own deprovision task, ``_resolve_catalog_schema`` can no longer resolve it at
all (raises ``ValueError``). The helper must tolerate that and still serve the
task's terminal payload instead of turning it into a 404 indistinguishable
from "task never existed".

Also covers #2685: the same helper, called with ``collection_id`` set (the
collection-scoped route), must apply the same tolerance — the durable
``task.collection_id`` cross-check still runs post-teardown, but the
listing-visibility re-check for the collection is skipped once the catalog
can no longer be resolved, mirroring the catalog-level behavior from #2674.
"""

from __future__ import annotations

import uuid
from unittest.mock import MagicMock

import pytest
from fastapi import HTTPException

import dynastore.extensions.tasks.tasks_service as svc
from dynastore.models.tasks import Task, TaskStatusEnum


def _task(status: TaskStatusEnum, catalog_id: str, collection_id: str | None = None) -> Task:
    return Task(
        task_type="collection_hard_delete",
        status=status,
        catalog_id=catalog_id,
        collection_id=collection_id,
    )


@pytest.mark.asyncio
async def test_catalog_task_status_reads_uncached_not_cached(monkeypatch):
    """The scoped lookup must call the uncached helper and never the cached one."""
    task_id = uuid.uuid4()
    fresh = _task(TaskStatusEnum.COMPLETED, "s_cat")
    calls = {"uncached": 0, "cached": 0}

    async def fake_schema(catalog_id, conn):
        return "s_cat"

    async def fake_uncached(conn, tid):
        calls["uncached"] += 1
        assert tid == task_id
        return fresh

    async def fake_cached(conn, tid, schema):  # must NOT be reached
        calls["cached"] += 1
        return _task(TaskStatusEnum.ACTIVE, schema)

    monkeypatch.setattr(svc.tasks_module, "_resolve_catalog_schema", fake_schema)
    monkeypatch.setattr(svc.tasks_module, "get_task_by_id_unscoped", fake_uncached)
    monkeypatch.setattr(svc.tasks_module, "get_task", fake_cached)

    task = await svc._get_task_scoped_uncached(task_id, "cat", MagicMock())

    assert calls["uncached"] == 1
    assert calls["cached"] == 0, "catalog task status must not use the cached get_task"
    assert task.status == TaskStatusEnum.COMPLETED


@pytest.mark.asyncio
async def test_catalog_task_status_scopes_to_catalog_schema(monkeypatch):
    """A task resolving to a DIFFERENT schema than the URL's catalog is a 404."""
    task_id = uuid.uuid4()
    other = _task(TaskStatusEnum.COMPLETED, "s_other")

    async def fake_schema(catalog_id, conn):
        return "s_cat"

    async def fake_uncached(conn, tid):
        return other  # belongs to s_other, not s_cat

    monkeypatch.setattr(svc.tasks_module, "_resolve_catalog_schema", fake_schema)
    monkeypatch.setattr(svc.tasks_module, "get_task_by_id_unscoped", fake_uncached)

    with pytest.raises(HTTPException) as ei:
        await svc._get_task_scoped_uncached(task_id, "cat", MagicMock())
    assert ei.value.status_code == 404


@pytest.mark.asyncio
async def test_catalog_task_status_hidden_catalog_is_404(monkeypatch):
    """A task that DOES resolve and match the URL's catalog schema is still
    a 404 when the caller may not see that catalog (listing-visibility deny).
    Pins the branch that only runs _assert_catalog_visible while the schema
    is resolvable — this must not regress into an unconditional skip."""
    task_id = uuid.uuid4()
    matching = _task(TaskStatusEnum.COMPLETED, "s_cat")
    visible_calls = {"n": 0}

    async def fake_schema(catalog_id, conn):
        return "s_cat"

    async def fake_uncached(conn, tid):
        return matching  # belongs to s_cat — schema check passes

    async def fake_visible(catalog_id):
        visible_calls["n"] += 1
        raise HTTPException(
            status_code=404, detail=f"Catalog '{catalog_id}' not found."
        )

    monkeypatch.setattr(svc.tasks_module, "_resolve_catalog_schema", fake_schema)
    monkeypatch.setattr(svc.tasks_module, "get_task_by_id_unscoped", fake_uncached)
    monkeypatch.setattr(svc, "_assert_catalog_visible", fake_visible)

    with pytest.raises(HTTPException) as ei:
        await svc._get_task_scoped_uncached(task_id, "cat", MagicMock())
    assert ei.value.status_code == 404
    assert visible_calls["n"] == 1


@pytest.mark.asyncio
async def test_catalog_task_status_missing_task_is_404(monkeypatch):
    task_id = uuid.uuid4()

    async def fake_schema(catalog_id, conn):
        return "s_cat"

    async def fake_uncached(conn, tid):
        return None

    monkeypatch.setattr(svc.tasks_module, "_resolve_catalog_schema", fake_schema)
    monkeypatch.setattr(svc.tasks_module, "get_task_by_id_unscoped", fake_uncached)

    with pytest.raises(HTTPException) as ei:
        await svc._get_task_scoped_uncached(task_id, "cat", MagicMock())
    assert ei.value.status_code == 404


@pytest.mark.asyncio
async def test_catalog_task_status_readable_after_catalog_hard_deleted(monkeypatch):
    """#2674: once the catalog is fully torn down, schema resolution raises
    ValueError — the terminal task status must still be served, not 404'd."""
    task_id = uuid.uuid4()
    finished = _task(TaskStatusEnum.COMPLETED, "s_cat")
    visible_calls = {"n": 0}

    async def fake_schema(catalog_id, conn):
        raise ValueError(f"Catalog '{catalog_id}' not found.")

    async def fake_uncached(conn, tid):
        assert tid == task_id
        return finished

    async def fake_visible(catalog_id):  # must NOT be reached — nothing to check
        visible_calls["n"] += 1

    monkeypatch.setattr(svc.tasks_module, "_resolve_catalog_schema", fake_schema)
    monkeypatch.setattr(svc.tasks_module, "get_task_by_id_unscoped", fake_uncached)
    monkeypatch.setattr(svc, "_assert_catalog_visible", fake_visible)

    task = await svc._get_task_scoped_uncached(task_id, "cat", MagicMock())

    assert task.status == TaskStatusEnum.COMPLETED
    assert visible_calls["n"] == 0, (
        "listing-visibility has nothing to re-derive once the catalog is gone"
    )


@pytest.mark.asyncio
async def test_catalog_task_status_unknown_task_after_catalog_deleted_still_404(
    monkeypatch,
):
    """A genuinely-unknown task_id must still 404, even once the catalog is gone."""
    task_id = uuid.uuid4()

    async def fake_schema(catalog_id, conn):
        raise ValueError(f"Catalog '{catalog_id}' not found.")

    async def fake_uncached(conn, tid):
        return None

    monkeypatch.setattr(svc.tasks_module, "_resolve_catalog_schema", fake_schema)
    monkeypatch.setattr(svc.tasks_module, "get_task_by_id_unscoped", fake_uncached)

    with pytest.raises(HTTPException) as ei:
        await svc._get_task_scoped_uncached(task_id, "cat", MagicMock())
    assert ei.value.status_code == 404


# ---------------------------------------------------------------------------
# Collection-scoped route (#2685) — same helper, collection_id set
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_collection_task_status_hidden_collection_is_404(monkeypatch):
    """A task that resolves and matches both catalog and collection is still
    a 404 when the caller may not see that collection (listing-visibility
    deny). Pins the branch that only runs _assert_collection_visible while
    the schema is resolvable — must not regress into an unconditional skip."""
    task_id = uuid.uuid4()
    matching = _task(TaskStatusEnum.COMPLETED, "s_cat", collection_id="coll")
    visible_calls = {"n": 0}

    async def fake_schema(catalog_id, conn):
        return "s_cat"

    async def fake_uncached(conn, tid):
        return matching

    async def fake_catalog_visible(catalog_id):
        return None

    async def fake_collection_visible(catalog_id, collection_id):
        visible_calls["n"] += 1
        raise HTTPException(
            status_code=404,
            detail=f"Collection '{collection_id}' not found in catalog '{catalog_id}'.",
        )

    monkeypatch.setattr(svc.tasks_module, "_resolve_catalog_schema", fake_schema)
    monkeypatch.setattr(svc.tasks_module, "get_task_by_id_unscoped", fake_uncached)
    monkeypatch.setattr(svc, "_assert_catalog_visible", fake_catalog_visible)
    monkeypatch.setattr(svc, "_assert_collection_visible", fake_collection_visible)

    with pytest.raises(HTTPException) as ei:
        await svc._get_task_scoped_uncached(task_id, "cat", MagicMock(), collection_id="coll")
    assert ei.value.status_code == 404
    assert visible_calls["n"] == 1


@pytest.mark.asyncio
async def test_collection_task_status_wrong_collection_is_404(monkeypatch):
    """A task whose durable collection_id differs from the URL's is a 404,
    independent of catalog resolvability."""
    task_id = uuid.uuid4()
    other_collection = _task(TaskStatusEnum.COMPLETED, "s_cat", collection_id="other")

    async def fake_schema(catalog_id, conn):
        return "s_cat"

    async def fake_uncached(conn, tid):
        return other_collection

    monkeypatch.setattr(svc.tasks_module, "_resolve_catalog_schema", fake_schema)
    monkeypatch.setattr(svc.tasks_module, "get_task_by_id_unscoped", fake_uncached)

    with pytest.raises(HTTPException) as ei:
        await svc._get_task_scoped_uncached(task_id, "cat", MagicMock(), collection_id="coll")
    assert ei.value.status_code == 404


@pytest.mark.asyncio
async def test_collection_task_status_readable_after_catalog_hard_deleted(monkeypatch):
    """#2685: once the catalog is fully torn down, schema resolution raises
    ValueError — the terminal task status for a matching collection must
    still be served, not 404'd, and the listing-visibility re-checks must
    not run since there's nothing left to re-derive them from."""
    task_id = uuid.uuid4()
    finished = _task(TaskStatusEnum.COMPLETED, "s_cat", collection_id="coll")
    catalog_visible_calls = {"n": 0}
    collection_visible_calls = {"n": 0}

    async def fake_schema(catalog_id, conn):
        raise ValueError(f"Catalog '{catalog_id}' not found.")

    async def fake_uncached(conn, tid):
        assert tid == task_id
        return finished

    async def fake_catalog_visible(catalog_id):
        catalog_visible_calls["n"] += 1

    async def fake_collection_visible(catalog_id, collection_id):
        collection_visible_calls["n"] += 1

    monkeypatch.setattr(svc.tasks_module, "_resolve_catalog_schema", fake_schema)
    monkeypatch.setattr(svc.tasks_module, "get_task_by_id_unscoped", fake_uncached)
    monkeypatch.setattr(svc, "_assert_catalog_visible", fake_catalog_visible)
    monkeypatch.setattr(svc, "_assert_collection_visible", fake_collection_visible)

    task = await svc._get_task_scoped_uncached(
        task_id, "cat", MagicMock(), collection_id="coll"
    )

    assert task.status == TaskStatusEnum.COMPLETED
    assert catalog_visible_calls["n"] == 0
    assert collection_visible_calls["n"] == 0, (
        "listing-visibility has nothing to re-derive once the catalog is gone"
    )


@pytest.mark.asyncio
async def test_collection_task_status_wrong_collection_still_404_after_catalog_deleted(
    monkeypatch,
):
    """The durable collection cross-check must still catch a mismatched
    collection_id even after the catalog is torn down."""
    task_id = uuid.uuid4()
    other_collection = _task(TaskStatusEnum.COMPLETED, "s_cat", collection_id="other")

    async def fake_schema(catalog_id, conn):
        raise ValueError(f"Catalog '{catalog_id}' not found.")

    async def fake_uncached(conn, tid):
        return other_collection

    monkeypatch.setattr(svc.tasks_module, "_resolve_catalog_schema", fake_schema)
    monkeypatch.setattr(svc.tasks_module, "get_task_by_id_unscoped", fake_uncached)

    with pytest.raises(HTTPException) as ei:
        await svc._get_task_scoped_uncached(task_id, "cat", MagicMock(), collection_id="coll")
    assert ei.value.status_code == 404
