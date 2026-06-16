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

"""``BucketService.ensure_storage_for_catalog`` must not hold a DB connection
across GCS API calls when called with ``conn=None``.

This is the R1 fix for issue #486: a single long-lived ``managed_transaction``
wrapping bucket creation + CORS apply + placeholder uploads pinned a pooled
connection for tens of seconds on ``dynastore-catalog``, starving request
handlers on the same instance. The fix splits the work into three phases:

  P1 (short tx)  -> bucket-exists short-circuit + effective-config resolution
  P2 (NO DB)     -> GCS create_bucket + _apply_bucket_settings + placeholders
  P3 (short tx)  -> _write_bucket_name (persists bucket name onto catalog config)

These tests prove the split by tracking the interleaving of
``managed_transaction`` enter/exit events with GCS-side calls.
"""
from __future__ import annotations

from contextlib import asynccontextmanager
from typing import Any, List
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from sqlalchemy.ext.asyncio import AsyncConnection

from dynastore.modules.gcp.bucket_service import BucketService
from dynastore.modules.gcp.tools.bucket import BucketConflictError


def _fake_conn() -> Any:
    """Spec'd mock that satisfies DriverContext's isinstance check on AsyncConnection."""
    return MagicMock(spec=AsyncConnection)


def _make_event_recorder():
    """Build a recorder that timestamps every interesting event by call order."""
    events: List[str] = []

    @asynccontextmanager
    async def fake_managed_transaction(_engine):
        events.append("tx_enter")
        try:
            yield _fake_conn()
        finally:
            events.append("tx_exit")

    return events, fake_managed_transaction


def _make_bm(events: List[str]) -> BucketService:
    storage_client = MagicMock()

    config_service = MagicMock()
    # No catalog-tier config persisted; ensure_storage will fall back to defaults.
    config_service.get_config = AsyncMock(return_value=None)
    config_service.set_config = AsyncMock(return_value=None)

    bm = BucketService(
        engine=MagicMock(name="engine"),
        config_service=config_service,
        storage_client=storage_client,
        project_id="my-test-project",
        region="europe-west1",
    )

    # Wrap the GCS-side helper so we can see WHEN it ran relative to tx_enter/tx_exit.
    real_gcs = bm._gcs_create_and_configure_bucket

    async def traced_gcs(*args, **kwargs):
        events.append("gcs_start")
        result = await real_gcs(*args, **kwargs)
        events.append("gcs_end")
        return result

    bm._gcs_create_and_configure_bucket = traced_gcs  # type: ignore[method-assign]
    return bm


@pytest.mark.asyncio
async def test_phase_split_when_conn_is_none():
    """conn=None -> two short transactions; GCS work happens BETWEEN them.

    This is the load-bearing assertion for issue #486 R1: the moment a single
    `tx_enter` straddles `gcs_start`/`gcs_end`, we are back to holding a pooled
    connection across 10-60s of GCS I/O.
    """
    events, fake_tx = _make_event_recorder()
    bm = _make_bm(events)

    # The new bucket name is computed locally; _write_bucket_name returns None.
    expected_bucket = bm.generate_bucket_name("cat_1")

    with patch(
        "dynastore.modules.gcp.bucket_service.managed_transaction", fake_tx
    ), patch.object(
        BucketService, "_read_bucket_name", new=AsyncMock(return_value=None)
    ), patch.object(
        BucketService, "_write_bucket_name", new=AsyncMock(return_value=None)
    ), patch(
        "dynastore.modules.gcp.bucket_service.bucket_tool"
    ) as fake_tool, patch(
        "dynastore.modules.gcp.bucket_service.run_in_thread", new=AsyncMock()
    ):
        fake_tool.create_bucket = AsyncMock(return_value=(MagicMock(), True))
        fake_tool.CATALOG_FOLDER = "catalog"
        fake_tool.COLLECTIONS_FOLDER = "collections"

        # Suppress log_info import-side effects with a stub
        with patch(
            "dynastore.modules.catalog.log_manager.log_info",
            new=AsyncMock(),
        ):
            result = await bm.ensure_storage_for_catalog("cat_1")

    assert result == expected_bucket

    # Strict event ordering: two distinct short transactions, GCS work outside both.
    assert events == [
        "tx_enter",  # P1 starts
        "tx_exit",   # P1 ends BEFORE GCS work
        "gcs_start",
        "gcs_end",
        "tx_enter",  # P3 starts AFTER GCS work
        "tx_exit",   # P3 ends
    ], f"Phase ordering violated. Saw: {events}"


@pytest.mark.asyncio
async def test_existing_bucket_short_circuits_in_phase_one():
    """If the bucket already exists, we exit after a single short read tx.

    No GCS calls, no second tx — keeps the hot read path cheap.
    """
    events, fake_tx = _make_event_recorder()
    bm = _make_bm(events)

    with patch(
        "dynastore.modules.gcp.bucket_service.managed_transaction", fake_tx
    ), patch.object(
        BucketService, "_read_bucket_name", new=AsyncMock(return_value="my-test-project-cat-1")
    ):
        result = await bm.ensure_storage_for_catalog("cat_1")

    assert result == "my-test-project-cat-1"
    assert events == ["tx_enter", "tx_exit"]


@pytest.mark.asyncio
async def test_link_failure_cleans_up_orphan_bucket():
    """If P3 (link) fails, the just-created GCS bucket must be deleted.

    Otherwise we leak a bucket the DB doesn't know about.
    """
    events, fake_tx = _make_event_recorder()
    bm = _make_bm(events)

    with patch(
        "dynastore.modules.gcp.bucket_service.managed_transaction", fake_tx
    ), patch.object(
        BucketService, "_read_bucket_name", new=AsyncMock(return_value=None)
    ), patch.object(
        BucketService, "_write_bucket_name", new=AsyncMock(side_effect=RuntimeError("DB link failed"))
    ), patch(
        "dynastore.modules.gcp.bucket_service.bucket_tool"
    ) as fake_tool, patch(
        "dynastore.modules.gcp.bucket_service.run_in_thread", new=AsyncMock()
    ), patch(
        "dynastore.modules.catalog.log_manager.log_info", new=AsyncMock()
    ):
        fake_tool.create_bucket = AsyncMock(return_value=(MagicMock(), True))
        fake_tool.delete_bucket = AsyncMock()
        fake_tool.CATALOG_FOLDER = "catalog"
        fake_tool.COLLECTIONS_FOLDER = "collections"

        result = await bm.ensure_storage_for_catalog("cat_1")

    assert result is None
    fake_tool.delete_bucket.assert_awaited_once()
    # delete_bucket got the bucket name we just created (positional arg 0).
    args, kwargs = fake_tool.delete_bucket.await_args
    assert args[0].startswith("my-test-project-cat-1")


@pytest.mark.asyncio
async def test_auto_create_false_short_circuits_without_second_tx():
    """auto_create=False on a missing bucket: one short tx, no creation."""
    events, fake_tx = _make_event_recorder()
    bm = _make_bm(events)

    with patch(
        "dynastore.modules.gcp.bucket_service.managed_transaction", fake_tx
    ), patch.object(
        BucketService, "_read_bucket_name", new=AsyncMock(return_value=None)
    ):
        result = await bm.ensure_storage_for_catalog("cat_1", auto_create=False)

    assert result is None
    assert events == ["tx_enter", "tx_exit"]


@pytest.mark.asyncio
async def test_caller_provided_conn_keeps_single_tx_path():
    """When the caller passes ``conn``, we honor their transaction.

    The phase split only kicks in for ``conn=None`` (the common case). This
    test ensures no caller currently passing ``conn`` is broken.
    """
    events, fake_tx = _make_event_recorder()
    bm = _make_bm(events)

    caller_conn = _fake_conn()

    with patch(
        "dynastore.modules.gcp.bucket_service.managed_transaction", fake_tx
    ), patch.object(
        BucketService, "_read_bucket_name", new=AsyncMock(return_value="existing-bucket")
    ):
        result = await bm.ensure_storage_for_catalog("cat_1", conn=caller_conn)

    assert result == "existing-bucket"
    # No managed_transaction was opened -- caller's conn was used directly.
    assert events == []


# ── raise_on_failure: provisioning callers must see the real cause ──
#
# Without this flag, a GCS create / DB link failure is logged with exc_info
# and swallowed as `return None`. The caller (`setup_catalog_gcp_resources`)
# then raises a generic "Bucket name returned as None", discarding the
# original exception. `raise_on_failure=True` propagates the real cause so it
# reaches the task layer and the top of the log.


@pytest.mark.asyncio
async def test_raise_on_failure_propagates_gcs_create_error():
    """raise_on_failure=True: a GCS create failure propagates, not None."""
    events, fake_tx = _make_event_recorder()
    bm = _make_bm(events)

    with patch(
        "dynastore.modules.gcp.bucket_service.managed_transaction", fake_tx
    ), patch.object(
        BucketService, "_read_bucket_name", new=AsyncMock(return_value=None)
    ), patch(
        "dynastore.modules.gcp.bucket_service.bucket_tool"
    ) as fake_tool, patch(
        "dynastore.modules.gcp.bucket_service.run_in_thread", new=AsyncMock()
    ), patch(
        "dynastore.modules.catalog.log_manager.log_info", new=AsyncMock()
    ):
        fake_tool.create_bucket = AsyncMock(
            side_effect=RuntimeError("403 GCS bucket create denied")
        )
        fake_tool.CATALOG_FOLDER = "catalog"
        fake_tool.COLLECTIONS_FOLDER = "collections"

        with pytest.raises(RuntimeError, match="GCS bucket create denied"):
            await bm.ensure_storage_for_catalog("cat_1", raise_on_failure=True)


@pytest.mark.asyncio
async def test_raise_on_failure_propagates_link_error():
    """raise_on_failure=True: a P3 link failure propagates after orphan cleanup."""
    events, fake_tx = _make_event_recorder()
    bm = _make_bm(events)

    with patch(
        "dynastore.modules.gcp.bucket_service.managed_transaction", fake_tx
    ), patch.object(
        BucketService, "_read_bucket_name", new=AsyncMock(return_value=None)
    ), patch.object(
        BucketService, "_write_bucket_name", new=AsyncMock(side_effect=RuntimeError("DB link failed"))
    ), patch(
        "dynastore.modules.gcp.bucket_service.bucket_tool"
    ) as fake_tool, patch(
        "dynastore.modules.gcp.bucket_service.run_in_thread", new=AsyncMock()
    ), patch(
        "dynastore.modules.catalog.log_manager.log_info", new=AsyncMock()
    ):
        fake_tool.create_bucket = AsyncMock(return_value=(MagicMock(), True))
        fake_tool.delete_bucket = AsyncMock()
        fake_tool.CATALOG_FOLDER = "catalog"
        fake_tool.COLLECTIONS_FOLDER = "collections"

        with pytest.raises(RuntimeError, match="DB link failed"):
            await bm.ensure_storage_for_catalog("cat_1", raise_on_failure=True)

    # Orphan cleanup still runs before the exception propagates.
    fake_tool.delete_bucket.assert_awaited_once()


@pytest.mark.asyncio
async def test_failure_still_returns_none_by_default():
    """Default (raise_on_failure=False) keeps graceful None for non-provisioning callers."""
    events, fake_tx = _make_event_recorder()
    bm = _make_bm(events)

    with patch(
        "dynastore.modules.gcp.bucket_service.managed_transaction", fake_tx
    ), patch.object(
        BucketService, "_read_bucket_name", new=AsyncMock(return_value=None)
    ), patch(
        "dynastore.modules.gcp.bucket_service.bucket_tool"
    ) as fake_tool, patch(
        "dynastore.modules.gcp.bucket_service.run_in_thread", new=AsyncMock()
    ), patch(
        "dynastore.modules.catalog.log_manager.log_info", new=AsyncMock()
    ):
        fake_tool.create_bucket = AsyncMock(
            side_effect=RuntimeError("403 GCS bucket create denied")
        )
        fake_tool.CATALOG_FOLDER = "catalog"
        fake_tool.COLLECTIONS_FOLDER = "collections"

        result = await bm.ensure_storage_for_catalog("cat_1")

    assert result is None


# ── bucket-name conflict safety: never force-delete a bucket we did not create ──
#
# `_create_bucket_sync` returns `(bucket, created)`. When `created is False`
# (the name pre-existed — a Conflict was resolved by fetching it), a later link
# failure must NOT orphan-delete the bucket: it belongs to another project or
# catalog and force-deletion would destroy its data. Instead provisioning
# surfaces a BucketConflictError so the catalog is marked 'conflict'.


@pytest.mark.asyncio
async def test_preexisting_bucket_link_failure_does_not_delete():
    """created=False + link failure → bucket is NOT deleted, conflict is raised."""
    events, fake_tx = _make_event_recorder()
    bm = _make_bm(events)

    with patch(
        "dynastore.modules.gcp.bucket_service.managed_transaction", fake_tx
    ), patch.object(
        BucketService, "_read_bucket_name", new=AsyncMock(return_value=None)
    ), patch.object(
        BucketService,
        "_write_bucket_name",
        new=AsyncMock(side_effect=RuntimeError("duplicate key value violates unique constraint")),
    ), patch(
        "dynastore.modules.gcp.bucket_service.bucket_tool"
    ) as fake_tool, patch(
        "dynastore.modules.gcp.bucket_service.run_in_thread", new=AsyncMock()
    ), patch(
        "dynastore.modules.catalog.log_manager.log_info", new=AsyncMock()
    ):
        # created=False → the bucket pre-existed this call.
        fake_tool.create_bucket = AsyncMock(return_value=(MagicMock(), False))
        fake_tool.delete_bucket = AsyncMock()
        fake_tool.CATALOG_FOLDER = "catalog"
        fake_tool.COLLECTIONS_FOLDER = "collections"

        with pytest.raises(BucketConflictError, match="already linked to another"):
            await bm.ensure_storage_for_catalog("cat_1", raise_on_failure=True)

    # The pre-existing bucket must be left intact — no delete call at all.
    fake_tool.delete_bucket.assert_not_awaited()


@pytest.mark.asyncio
async def test_create_conflict_in_other_project_propagates():
    """A BucketConflictError from create_bucket propagates unchanged (no delete)."""
    events, fake_tx = _make_event_recorder()
    bm = _make_bm(events)

    with patch(
        "dynastore.modules.gcp.bucket_service.managed_transaction", fake_tx
    ), patch.object(
        BucketService, "_read_bucket_name", new=AsyncMock(return_value=None)
    ), patch(
        "dynastore.modules.gcp.bucket_service.bucket_tool"
    ) as fake_tool, patch(
        "dynastore.modules.gcp.bucket_service.run_in_thread", new=AsyncMock()
    ), patch(
        "dynastore.modules.catalog.log_manager.log_info", new=AsyncMock()
    ):
        fake_tool.create_bucket = AsyncMock(
            side_effect=BucketConflictError("exists in another GCP project")
        )
        fake_tool.delete_bucket = AsyncMock()
        fake_tool.CATALOG_FOLDER = "catalog"
        fake_tool.COLLECTIONS_FOLDER = "collections"

        with pytest.raises(BucketConflictError, match="another GCP project"):
            await bm.ensure_storage_for_catalog("cat_1", raise_on_failure=True)

    fake_tool.delete_bucket.assert_not_awaited()
