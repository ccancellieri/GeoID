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

"""Unit tests for the ``stac_harvest`` resume cursor (#3034).

Covers: the per-batch/per-collection checkpoint calls out of
``_harvest_collection``, the catalog-walk skip/resume wiring in
``run_harvest``, and an end-to-end (fake paginated source, no live DB/network)
proof that a resumed harvest does not re-fetch or re-write already-completed
pages.
"""
from __future__ import annotations

from typing import Any, Dict, List
from unittest.mock import AsyncMock, patch

import pytest

from dynastore.tasks.stac_harvest import task as harvest_task
from dynastore.tasks.stac_harvest.models import StacHarvestCursor, StacHarvestRequest


def _mock_catalogs() -> AsyncMock:
    catalogs = AsyncMock()
    catalogs.get_collection = AsyncMock(return_value=None)  # not present -> create
    catalogs.create_collection = AsyncMock(return_value=None)
    catalogs.update_collection = AsyncMock(return_value=None)
    catalogs.upsert = AsyncMock(return_value=None)
    return catalogs


async def _aiter(items: List[Dict[str, Any]]):
    for it in items:
        yield it


def _request(**overrides: Any) -> StacHarvestRequest:
    params: Dict[str, Any] = {
        "catalog_url": "https://src",
        "target_catalog": "cat-7",
        "drivers": "es",
        "with_assets": False,
    }
    params.update(overrides)
    return StacHarvestRequest(**params)


def _valid_items(n: int, prefix: str = "i") -> List[Dict[str, Any]]:
    return [
        {"type": "Feature", "id": f"{prefix}{i}", "geometry": None, "properties": {}}
        for i in range(n)
    ]


# ---------------------------------------------------------------------------
# _persist_harvest_cursor
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_persist_harvest_cursor_delegates_to_tasks_module():
    calls: list = []

    async def fake_update(engine, task_id, collection_id, items_href, done):
        calls.append((engine, task_id, collection_id, items_href, done))
        return True

    fake_engine = object()
    with patch(
        "dynastore.modules.tasks.tasks_module.update_task_harvest_cursor",
        AsyncMock(side_effect=fake_update),
    ):
        await harvest_task._persist_harvest_cursor(
            fake_engine, "11111111-1111-1111-1111-111111111111", "c1",
            "https://src/items?page=2", False,
        )

    assert len(calls) == 1
    engine, task_id, collection_id, items_href, done = calls[0]
    assert engine is fake_engine
    assert str(task_id) == "11111111-1111-1111-1111-111111111111"
    assert collection_id == "c1"
    assert items_href == "https://src/items?page=2"
    assert done is False


@pytest.mark.asyncio
async def test_persist_harvest_cursor_noop_without_engine_or_task_id():
    with patch(
        "dynastore.modules.tasks.tasks_module.update_task_harvest_cursor",
        AsyncMock(),
    ) as mock_update:
        await harvest_task._persist_harvest_cursor(None, "tid", "c1", None, False)
        await harvest_task._persist_harvest_cursor(object(), None, "c1", None, False)

    mock_update.assert_not_called()


@pytest.mark.asyncio
async def test_persist_harvest_cursor_swallows_write_failures():
    """Best-effort: a DB error here must never propagate out of the harvest loop."""
    with patch(
        "dynastore.modules.tasks.tasks_module.update_task_harvest_cursor",
        AsyncMock(side_effect=RuntimeError("db down")),
    ):
        await harvest_task._persist_harvest_cursor(
            object(), "11111111-1111-1111-1111-111111111111", "c1", "href", False,
        )  # must not raise


# ---------------------------------------------------------------------------
# _harvest_collection checkpoint wiring
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_flush_persists_cursor_after_each_successful_batch():
    """Each successful batch write stamps the current page_cursor.next_url,
    and the whole-collection drain stamps a final done=True."""
    catalogs = _mock_catalogs()
    request = _request()
    stats = harvest_task.HarvestStats()
    source_coll = {"type": "Collection", "id": "c1", "description": "d"}
    page_cursor = harvest_task.PageCursor(next_url="https://src/c1/items?page=1")

    persisted: list = []

    async def fake_persist(engine, task_id, collection_id, items_href, done):
        persisted.append((collection_id, items_href, done))

    async def items_with_cursor_advance():
        yield {"type": "Feature", "id": "i1", "geometry": None, "properties": {}}
        page_cursor.next_url = "https://src/c1/items?page=2"
        yield {"type": "Feature", "id": "i2", "geometry": None, "properties": {}}

    with (
        patch.object(harvest_task, "_BATCH_SIZE", 1),
        patch.object(harvest_task, "_persist_harvest_cursor", AsyncMock(side_effect=fake_persist)),
    ):
        await harvest_task._harvest_collection(
            catalogs, request, source_coll, items_with_cursor_advance(), "c1", stats,
            source_collection_id="c1", engine=object(), task_id="tid",
            page_cursor=page_cursor,
        )

    assert stats.items_written == 2
    # batch 1 flushed while page_cursor.next_url was still page=1; batch 2
    # flushed after it advanced to page=2; the drain then stamps done=True.
    assert persisted == [
        ("c1", "https://src/c1/items?page=1", False),
        ("c1", "https://src/c1/items?page=2", False),
        ("c1", None, True),
    ]


@pytest.mark.asyncio
async def test_flush_does_not_persist_cursor_on_failed_write():
    """A failed batch write must not advance the per-batch checkpoint."""
    catalogs = _mock_catalogs()
    catalogs.upsert = AsyncMock(side_effect=RuntimeError("boom"))
    request = _request()
    stats = harvest_task.HarvestStats()
    source_coll = {"type": "Collection", "id": "c1", "description": "d"}
    page_cursor = harvest_task.PageCursor(next_url="https://src/c1/items?page=1")

    with (
        patch.object(harvest_task, "_BATCH_SIZE", 1),
        patch.object(harvest_task, "_persist_harvest_cursor", AsyncMock()) as mock_persist,
    ):
        await harvest_task._harvest_collection(
            catalogs, request, source_coll, _aiter(_valid_items(1)), "c1", stats,
            source_collection_id="c1", engine=object(), task_id="tid",
            page_cursor=page_cursor,
        )

    calls = [c.args for c in mock_persist.await_args_list]
    # The one failed batch records no per-batch checkpoint; the loop still
    # drains to completion (no stall abort tripped in a single batch), so the
    # final done=True call still fires.
    assert len(calls) == 1
    assert calls[0][1:] == ("tid", "c1", None, True)


@pytest.mark.asyncio
async def test_truncated_walk_is_not_marked_done():
    """A page-fetch error mid-walk (PageCursor.truncated) must not stamp
    done=True — a resume must retry the unfetched tail, not skip it forever."""
    catalogs = _mock_catalogs()
    request = _request()
    stats = harvest_task.HarvestStats()
    source_coll = {"type": "Collection", "id": "c1", "description": "d"}
    page_cursor = harvest_task.PageCursor(
        next_url="https://src/c1/items?page=2", truncated=True,
    )

    with (
        patch.object(harvest_task, "_BATCH_SIZE", 1),
        patch.object(harvest_task, "_persist_harvest_cursor", AsyncMock()) as mock_persist,
    ):
        await harvest_task._harvest_collection(
            catalogs, request, source_coll, _aiter(_valid_items(1)), "c1", stats,
            source_collection_id="c1", engine=object(), task_id="tid",
            page_cursor=page_cursor,
        )

    calls = [c.args for c in mock_persist.await_args_list]
    # Only the one successful batch's checkpoint fired; no trailing done=True
    # despite the loop reaching its end (the generator was truncated, not
    # exhausted naturally).
    assert len(calls) == 1
    assert calls[0][1:] == ("tid", "c1", "https://src/c1/items?page=2", False)


# ---------------------------------------------------------------------------
# run_harvest — catalog-walk skip / resume wiring
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_catalog_walk_skips_completed_collections_before_resume_point():
    """Collections walked before the resume point are skipped entirely --
    _harvest_collection must never be invoked for them."""
    coll1 = {"type": "Collection", "id": "c1", "description": "d"}
    coll2 = {"type": "Collection", "id": "c2", "description": "d"}
    coll3 = {"type": "Collection", "id": "c3", "description": "d"}
    request = _request(
        resume=StacHarvestCursor(collection_id="c2", items_href="https://src/c2?p=2", done=False),
    )
    catalogs = _mock_catalogs()
    seen_collections: list = []

    async def fake_harvest_collection(catalogs_, request_, source_coll, items_iter, target_collection, stats, **kw):
        seen_collections.append(kw.get("source_collection_id"))
        async for _ in items_iter:
            pass

    with (
        patch.object(harvest_task, "_probe_single_collection", return_value=None),
        patch.object(harvest_task, "iter_collections", return_value=_aiter([coll1, coll2, coll3])),
        patch.object(harvest_task, "_harvest_collection", side_effect=fake_harvest_collection),
        patch.object(harvest_task, "_iter_items_from", return_value=_aiter([])),
        patch.object(harvest_task, "iter_items", return_value=_aiter([])),
    ):
        await harvest_task.run_harvest(
            request, catalogs, preset_ctx=None, base_scope="catalog:cat-7",
        )

    assert seen_collections == ["c2", "c3"]


@pytest.mark.asyncio
async def test_catalog_walk_resumes_matched_collection_from_persisted_href():
    """The collection matching resume.collection_id resumes its items walk
    from resume.items_href via _iter_items_from(resume_from_href=True); later
    collections start fresh via iter_items."""
    coll1 = {"type": "Collection", "id": "c1", "description": "d"}
    coll2 = {"type": "Collection", "id": "c2", "description": "d"}
    href = "https://src/collections/c2/items?page=2"
    request = _request(
        resume=StacHarvestCursor(collection_id="c2", items_href=href, done=False),
    )
    catalogs = _mock_catalogs()

    with (
        patch.object(harvest_task, "_probe_single_collection", return_value=None),
        patch.object(harvest_task, "iter_collections", return_value=_aiter([coll1, coll2])),
        patch.object(harvest_task, "_iter_items_from", return_value=_aiter([])) as mock_resume_iter,
        patch.object(harvest_task, "iter_items", return_value=_aiter([])) as mock_fresh_iter,
    ):
        await harvest_task.run_harvest(
            request, catalogs, preset_ctx=None, base_scope="catalog:cat-7",
        )

    # c1 was skipped entirely: neither iterator was asked to walk it.
    mock_resume_iter.assert_called_once()
    args, kwargs = mock_resume_iter.call_args
    assert args[0] == href
    assert args[1] == "c2"
    assert kwargs.get("resume_from_href") is True
    # c1 (skipped) never reaches either iterator; only c2 is fetched, via the
    # resume path -- iter_items (fresh-start) is never used in this walk.
    mock_fresh_iter.assert_not_called()


@pytest.mark.asyncio
async def test_catalog_walk_skips_resume_collection_when_marked_done():
    """resume.done=True means the matched collection itself already finished
    -- the walk resumes at the NEXT collection instead."""
    coll1 = {"type": "Collection", "id": "c1", "description": "d"}
    coll2 = {"type": "Collection", "id": "c2", "description": "d"}
    coll3 = {"type": "Collection", "id": "c3", "description": "d"}
    request = _request(
        resume=StacHarvestCursor(collection_id="c2", items_href=None, done=True),
    )
    catalogs = _mock_catalogs()
    seen_collections: list = []

    async def fake_harvest_collection(catalogs_, request_, source_coll, items_iter, target_collection, stats, **kw):
        seen_collections.append(kw.get("source_collection_id"))
        async for _ in items_iter:
            pass

    with (
        patch.object(harvest_task, "_probe_single_collection", return_value=None),
        patch.object(harvest_task, "iter_collections", return_value=_aiter([coll1, coll2, coll3])),
        patch.object(harvest_task, "_harvest_collection", side_effect=fake_harvest_collection),
        patch.object(harvest_task, "iter_items", return_value=_aiter([])),
    ):
        await harvest_task.run_harvest(
            request, catalogs, preset_ctx=None, base_scope="catalog:cat-7",
        )

    assert seen_collections == ["c3"]


@pytest.mark.asyncio
async def test_catalog_walk_raises_when_resume_point_never_found():
    """If the resume collection id no longer appears in /collections (renamed
    or removed at the source), fail loudly instead of silently harvesting
    nothing."""
    coll1 = {"type": "Collection", "id": "c1", "description": "d"}
    coll2 = {"type": "Collection", "id": "c2", "description": "d"}
    request = _request(
        resume=StacHarvestCursor(collection_id="gone", items_href=None, done=False),
    )
    catalogs = _mock_catalogs()

    with (
        patch.object(harvest_task, "_probe_single_collection", return_value=None),
        patch.object(harvest_task, "iter_collections", return_value=_aiter([coll1, coll2])),
    ):
        with pytest.raises(RuntimeError, match="resume cursor"):
            await harvest_task.run_harvest(
                request, catalogs, preset_ctx=None, base_scope="catalog:cat-7",
            )


# ---------------------------------------------------------------------------
# run_harvest — single-collection resume wiring
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_single_collection_resume_uses_persisted_href():
    source_coll = {"type": "Collection", "id": "MyColl", "description": "d"}
    href = "https://src/c/MyColl/items?page=2"
    request = StacHarvestRequest(
        catalog_url="https://src/c/MyColl", target_catalog="cat-7", drivers="es",
        resume=StacHarvestCursor(collection_id="MyColl", items_href=href, done=False),
    )
    catalogs = _mock_catalogs()

    with (
        patch.object(harvest_task, "_probe_single_collection",
                     return_value=(source_coll, "https://src/c/MyColl/items")),
        patch.object(harvest_task, "_iter_items_from", return_value=_aiter([])) as mock_iter,
    ):
        stats = await harvest_task.run_harvest(
            request, catalogs, preset_ctx=None, base_scope="catalog:cat-7",
        )

    args, kwargs = mock_iter.call_args
    assert args[0] == href
    assert kwargs.get("resume_from_href") is True
    assert stats.collections_seen == 1


@pytest.mark.asyncio
async def test_single_collection_resume_done_skips_items_walk_entirely():
    source_coll = {"type": "Collection", "id": "MyColl", "description": "d"}
    request = StacHarvestRequest(
        catalog_url="https://src/c/MyColl", target_catalog="cat-7", drivers="es",
        resume=StacHarvestCursor(collection_id="MyColl", items_href=None, done=True),
    )
    catalogs = _mock_catalogs()

    with (
        patch.object(harvest_task, "_probe_single_collection",
                     return_value=(source_coll, "https://src/c/MyColl/items")),
        patch.object(harvest_task, "_iter_items_from") as mock_iter,
    ):
        stats = await harvest_task.run_harvest(
            request, catalogs, preset_ctx=None, base_scope="catalog:cat-7",
        )

    mock_iter.assert_not_called()
    catalogs.create_collection.assert_not_called()
    assert stats.collections_seen == 0
    assert stats.items_written == 0


# ---------------------------------------------------------------------------
# End-to-end (fake paginated HTTP source): a resumed harvest does not re-walk
# already-completed pages.
# ---------------------------------------------------------------------------


class _FakeStacSource:
    """A tiny fake single-collection STAC source with 3 items-pages.

    Records every URL fetched via ``_http_get_json`` so a test can assert
    exactly which pages a resumed walk re-fetches.
    """

    def __init__(self) -> None:
        self.fetched_urls: List[str] = []
        self.page1_url = "https://src/c/MyColl/items?limit=100"
        self.page2_url = "https://src/c/MyColl/items?page=2"
        self.page3_url = "https://src/c/MyColl/items?page=3"

    def get(self, url: str) -> Dict[str, Any]:
        self.fetched_urls.append(url)
        if url == self.page1_url:
            return {
                "features": _valid_items(2, prefix="p1-"),
                "links": [{"rel": "next", "href": self.page2_url}],
            }
        if url == self.page2_url:
            return {
                "features": _valid_items(2, prefix="p2-"),
                "links": [{"rel": "next", "href": self.page3_url}],
            }
        if url == self.page3_url:
            return {
                "features": _valid_items(2, prefix="p3-"),
                "links": [],
            }
        raise AssertionError(f"unexpected fetch: {url}")


@pytest.mark.asyncio
async def test_resumed_single_collection_harvest_skips_already_fetched_pages():
    """First attempt drains all 3 pages, checkpointing after each batch.
    Checkpoint granularity is per-flushed-page (the page whose items were
    JUST committed, not yet the following one -- the generator has not
    resumed past its last yield when the checkpoint is read, so a resume
    conservatively re-fetches that one page; see ``PageCursor``).

    A second, independent attempt seeded with the checkpoint captured right
    after page 2's flush must fetch ONLY page 2 (re-touched once, idempotent)
    and page 3 -- page 1, completed two pages earlier, must never be
    re-walked or re-written.
    """
    source_coll = {"type": "Collection", "id": "MyColl", "description": "d"}

    # --- Attempt 1: full run, capture every checkpoint stamped. ---
    fake1 = _FakeStacSource()
    request1 = StacHarvestRequest(
        catalog_url="https://src/c/MyColl", target_catalog="cat-7", drivers="es",
        with_assets=False,
    )
    catalogs1 = _mock_catalogs()
    checkpoints: list = []

    async def capture_update(engine, task_id, collection_id, items_href, done):
        checkpoints.append((collection_id, items_href, done))
        return True

    with (
        patch.object(harvest_task, "_probe_single_collection",
                     return_value=(source_coll, "https://src/c/MyColl/items")),
        patch.object(harvest_task, "_http_get_json", side_effect=fake1.get),
        patch.object(harvest_task, "_BATCH_SIZE", 2),
        patch(
            "dynastore.modules.tasks.tasks_module.update_task_harvest_cursor",
            AsyncMock(side_effect=capture_update),
        ),
    ):
        stats1 = await harvest_task.run_harvest(
            request1, catalogs1, preset_ctx=None, base_scope="catalog:cat-7",
            engine=object(), task_id="11111111-1111-1111-1111-111111111111",
        )

    assert stats1.items_written == 6
    assert fake1.fetched_urls == [fake1.page1_url, fake1.page2_url, fake1.page3_url]
    # Checkpoint after each batch conservatively points at the page just
    # flushed (see PageCursor); the final call marks the collection done.
    assert checkpoints == [
        ("MyColl", fake1.page1_url, False),
        ("MyColl", fake1.page2_url, False),
        ("MyColl", fake1.page3_url, False),
        ("MyColl", None, True),
    ]
    resume_href_after_page_2 = checkpoints[1][1]
    assert resume_href_after_page_2 == fake1.page2_url

    # --- Attempt 2: independent run seeded with the post-page-2 checkpoint. ---
    fake2 = _FakeStacSource()
    request2 = StacHarvestRequest(
        catalog_url="https://src/c/MyColl", target_catalog="cat-7", drivers="es",
        with_assets=False,
        resume=StacHarvestCursor(
            collection_id="MyColl", items_href=resume_href_after_page_2, done=False,
        ),
    )
    catalogs2 = _mock_catalogs()

    with (
        patch.object(harvest_task, "_probe_single_collection",
                     return_value=(source_coll, "https://src/c/MyColl/items")),
        patch.object(harvest_task, "_http_get_json", side_effect=fake2.get),
        patch.object(harvest_task, "_BATCH_SIZE", 2),
    ):
        stats2 = await harvest_task.run_harvest(
            request2, catalogs2, preset_ctx=None, base_scope="catalog:cat-7",
        )

    # Page 1 (completed 2 pages before the checkpoint) is never re-fetched;
    # only page 2 (the just-flushed page at checkpoint time) and page 3 are.
    assert fake2.fetched_urls == [fake2.page2_url, fake2.page3_url]
    assert stats2.items_written == 4
    written_ids = {
        feat.id
        for call in catalogs2.upsert.await_args_list
        for feat in call.args[2]
    }
    assert written_ids == {"p2-0", "p2-1", "p3-0", "p3-1"}
    assert "p1-0" not in written_ids and "p1-1" not in written_ids
