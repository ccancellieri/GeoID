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

"""Unit tests for the monotonic ``first_ready_at`` marker (#2676).

``catalog.catalogs.provisioning_status`` is re-enterable: a reprovision or
deferred-storage backfill (``reset_checklist_for_reprovision``) flips an
already-serving catalog back to ``'provisioning'``. ``first_ready_at`` is a
separate, monotonic column stamped the first time a catalog reaches
``'ready'`` and never cleared — every write path that can set
``provisioning_status = 'ready'`` must stamp it via
``COALESCE(first_ready_at, NOW())`` so a later 'ready' transition is a no-op
on the marker.

Every write path is covered here, DB-free, by inspecting the SQL each method
builds (mirrors the collection-side SQL-inspection tests for #2194/#2308):

- the ``catalog.catalogs`` INSERT always inserts NULL — the
  ``provisioning_status='ready'`` value passed at INSERT time is a
  placeholder ``create_catalog`` sets before the checklist is built, not
  proof the row is actually staying ready (a live-DB run against this
  codebase caught exactly this: stamping from that placeholder marked every
  catalog "ever ready" at birth, including ones about to enter their first
  provisioning pass);
- the empty-checklist ("truly born ready") case stamps explicitly via
  ``_stamp_first_ready_at_query`` right after ``_create_catalog_async``
  learns the checklist is empty;
- ``update_provisioning_status``;
- ``mark_provisioning_step`` (the checklist-completion finalizer);
- ``drain_pending_checklist_steps`` (the stuck-provisioning backstop).
"""

from __future__ import annotations

import json
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from dynastore.modules.catalog.catalog_service import (
    CatalogService,
    _create_catalog_strict_query,
    _stamp_first_ready_at_query,
)
from dynastore.modules.catalog.provisioning_registry import (
    STEP_COMPLETE,
    STEP_PENDING,
    STATUS_READY,
)


def _make_service() -> CatalogService:
    return CatalogService.__new__(CatalogService)


# ---------------------------------------------------------------------------
# INSERT — always NULL; empty-checklist path stamps separately afterward
# ---------------------------------------------------------------------------


def test_insert_query_always_inserts_null_first_ready_at():
    """The INSERT must NOT derive first_ready_at from the 'ready' placeholder
    passed at insert time — that value does not yet reflect the checklist
    outcome (see module docstring)."""
    sql = _create_catalog_strict_query.template
    assert "first_ready_at" in sql
    assert "VALUES (:id, :external_id, :provisioning_status, NULL)" in sql
    assert "CASE" not in sql


def test_stamp_first_ready_at_query_is_monotonic():
    sql = _stamp_first_ready_at_query.template
    assert "COALESCE(first_ready_at, NOW())" in sql


@pytest.mark.asyncio
async def test_create_catalog_async_stamps_on_empty_checklist():
    """When the registry returns an empty checklist (no active provisioner),
    ``_create_catalog_async`` must stamp first_ready_at explicitly — the row
    stays 'ready' from creation and never reaches the checklist finalizer."""
    from contextlib import asynccontextmanager

    svc = CatalogService.__new__(CatalogService)
    catalog_model = MagicMock()
    catalog_model.id = "my-catalog"
    catalog_model.provisioning_status = "ready"
    catalog_model.external_id = None

    fake_conn = AsyncMock()

    @asynccontextmanager
    async def _txn_ctx(_engine):
        yield fake_conn

    stamp_calls: list[dict] = []

    async def _fake_stamp_execute(conn, **kwargs):
        stamp_calls.append(kwargs)

    mock_reg = MagicMock()
    mock_reg.build_checklist = AsyncMock(return_value={})

    with (
        patch(
            "dynastore.modules.catalog.catalog_service.get_catalog_engine",
            return_value=MagicMock(),
        ),
        patch(
            "dynastore.modules.catalog.catalog_service.managed_transaction",
            side_effect=_txn_ctx,
        ),
        patch("dynastore.modules.catalog.catalog_service.emit_event", new=AsyncMock()),
        patch("dynastore.modules.catalog.catalog_service.DQLQuery") as mock_dql_cls,
        patch(
            "dynastore.modules.catalog.catalog_service._insert_catalog_row_with_pk_retry",
            new=AsyncMock(return_value="c_born_ready"),
        ),
        patch(
            "dynastore.modules.catalog.catalog_service._stamp_first_ready_at_query",
        ) as mock_stamp_q,
        patch(
            "dynastore.modules.catalog.catalog_service._invalidate_catalog_model_cache",
        ),
        patch(
            "dynastore.modules.catalog.catalog_service._invalidate_catalog_external_id_cache",
        ),
        patch(
            "dynastore.modules.catalog.provisioning_registry.provisioning_registry",
            mock_reg,
        ),
    ):
        mock_dql_cls.return_value.execute = AsyncMock(return_value=None)
        mock_stamp_q.execute = AsyncMock(side_effect=_fake_stamp_execute)

        result = await svc._create_catalog_async(catalog_model, "my-catalog", None)

    assert result.provisioning_status == "ready"
    assert len(stamp_calls) == 1
    assert stamp_calls[0].get("id") == "c_born_ready"


# ---------------------------------------------------------------------------
# update_provisioning_status
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_update_provisioning_status_sql_stamps_monotonically():
    """The UPDATE built by update_provisioning_status carries the
    COALESCE(first_ready_at, NOW()) stamp guarded by status='ready'."""
    svc = _make_service()

    captured_sql: list[str] = []
    captured_kwargs: list[dict] = []

    class _CapturingQuery:
        def __init__(self, sql, result_handler=None, **_kw):
            captured_sql.append(sql)

        async def execute(self, *_a, **kwargs):
            captured_kwargs.append(kwargs)
            return {"id": "cat_x"}

    async def _fake_provision_write(engine, fn):
        return await fn(MagicMock())

    with (
        patch(
            "dynastore.modules.catalog.catalog_service._provisioning_write_with_retry",
            new=_fake_provision_write,
        ),
        patch(
            "dynastore.modules.catalog.catalog_service.DQLQuery",
            new=_CapturingQuery,
        ),
        patch(
            "dynastore.modules.catalog.catalog_service.get_catalog_engine",
            return_value=MagicMock(),
        ),
        patch.object(svc, "get_catalog_model", new=AsyncMock(return_value=None)),
        patch(
            "dynastore.modules.catalog.catalog_service._invalidate_catalog_model_cache",
        ),
    ):
        result = await svc.update_provisioning_status("cat_x", STATUS_READY)

    assert result is True
    assert captured_sql, "expected the method to build an UPDATE"
    sql = captured_sql[0]
    assert "first_ready_at = CASE WHEN CAST(:status AS VARCHAR) = 'ready'" in sql
    assert "COALESCE(first_ready_at, NOW())" in sql
    assert captured_kwargs[0].get("status") == STATUS_READY


# ---------------------------------------------------------------------------
# mark_provisioning_step — the checklist-completion finalizer
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_mark_provisioning_step_stamps_first_ready_at_on_ready():
    """When the checklist completes, the terminal UPDATE stamps first_ready_at."""
    svc = _make_service()
    checklist = {"gcp_bucket": STEP_PENDING}

    captured_sql: list[str] = []
    captured_kwargs: list[dict] = []

    class _CapturingQuery:
        def __init__(self, sql, result_handler=None, **_kw):
            captured_sql.append(sql)

        async def execute(self, *_a, **kwargs):
            captured_kwargs.append(kwargs)

    async def _fake_provision_write(engine, fn):
        return await fn(MagicMock())

    async def _fake_get_checklist(conn, **kwargs):
        return {"provisioning_checklist": json.dumps(checklist)}

    fake_get_query = MagicMock()
    fake_get_query.execute = AsyncMock(side_effect=_fake_get_checklist)

    with (
        patch(
            "dynastore.modules.catalog.catalog_service._provisioning_write_with_retry",
            new=_fake_provision_write,
        ),
        patch(
            "dynastore.modules.catalog.catalog_service._get_provisioning_checklist_query",
            fake_get_query,
        ),
        patch(
            "dynastore.modules.catalog.catalog_service.DQLQuery",
            new=_CapturingQuery,
        ),
        patch(
            "dynastore.modules.catalog.catalog_service.get_catalog_engine",
            return_value=MagicMock(),
        ),
        patch.object(svc, "get_catalog_model", new=AsyncMock(return_value=None)),
        patch(
            "dynastore.modules.catalog.catalog_service._invalidate_catalog_model_cache",
        ),
    ):
        result = await svc.mark_provisioning_step("cat_x", "gcp_bucket", STEP_COMPLETE)

    assert result is True
    assert captured_sql, "expected the method to build a terminal UPDATE"
    sql = captured_sql[0]
    assert "first_ready_at = CASE WHEN CAST(:st AS VARCHAR) = 'ready'" in sql
    assert "COALESCE(first_ready_at, NOW())" in sql
    assert captured_kwargs[0].get("st") == STATUS_READY


@pytest.mark.asyncio
async def test_mark_provisioning_step_no_status_change_has_no_stamp_clause():
    """A step mark that leaves the checklist non-terminal (still 'pending'
    siblings) issues the checklist-only UPDATE — no status/stamp column."""
    svc = _make_service()
    checklist = {"gcp_bucket": STEP_PENDING, "gcp_eventing": STEP_PENDING}

    captured_sql: list[str] = []

    class _CapturingQuery:
        def __init__(self, sql, result_handler=None, **_kw):
            captured_sql.append(sql)

        async def execute(self, *_a, **_kw):
            return None

    async def _fake_provision_write(engine, fn):
        return await fn(MagicMock())

    async def _fake_get_checklist(conn, **kwargs):
        return {"provisioning_checklist": json.dumps(checklist)}

    fake_get_query = MagicMock()
    fake_get_query.execute = AsyncMock(side_effect=_fake_get_checklist)

    with (
        patch(
            "dynastore.modules.catalog.catalog_service._provisioning_write_with_retry",
            new=_fake_provision_write,
        ),
        patch(
            "dynastore.modules.catalog.catalog_service._get_provisioning_checklist_query",
            fake_get_query,
        ),
        patch(
            "dynastore.modules.catalog.catalog_service.DQLQuery",
            new=_CapturingQuery,
        ),
        patch(
            "dynastore.modules.catalog.catalog_service.get_catalog_engine",
            return_value=MagicMock(),
        ),
    ):
        result = await svc.mark_provisioning_step("cat_x", "gcp_bucket", STEP_COMPLETE)

    # The checklist row was found and updated (result True) even though the
    # barrier still holds (evaluate_checklist returned None, no status
    # change) -> the checklist-only UPDATE carries no first_ready_at clause.
    assert result is True
    assert captured_sql, "expected the checklist-only UPDATE to be built"
    assert "first_ready_at" not in captured_sql[0]


# ---------------------------------------------------------------------------
# drain_pending_checklist_steps — the stuck-provisioning backstop
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_drain_pending_checklist_steps_stamps_first_ready_at():
    svc = _make_service()
    checklist = {"gcp_bucket": STEP_COMPLETE, "gcp_eventing": STEP_PENDING}

    captured_sql: list[str] = []
    captured_kwargs: list[dict] = []

    class _CapturingQuery:
        def __init__(self, sql, result_handler=None, **_kw):
            captured_sql.append(sql)

        async def execute(self, *_a, **kwargs):
            captured_kwargs.append(kwargs)

    async def _fake_provision_write(engine, fn):
        return await fn(MagicMock())

    async def _fake_get_checklist(conn, **kwargs):
        return {"provisioning_checklist": json.dumps(checklist)}

    fake_get_query = MagicMock()
    fake_get_query.execute = AsyncMock(side_effect=_fake_get_checklist)

    with (
        patch(
            "dynastore.modules.catalog.catalog_service._provisioning_write_with_retry",
            new=_fake_provision_write,
        ),
        patch(
            "dynastore.modules.catalog.catalog_service._get_provisioning_checklist_query",
            fake_get_query,
        ),
        patch(
            "dynastore.modules.catalog.catalog_service.DQLQuery",
            new=_CapturingQuery,
        ),
        patch(
            "dynastore.modules.catalog.catalog_service.get_catalog_engine",
            return_value=MagicMock(),
        ),
        patch.object(svc, "get_catalog_model", new=AsyncMock(return_value=None)),
        patch(
            "dynastore.modules.catalog.catalog_service._invalidate_catalog_model_cache",
        ),
    ):
        result = await svc.drain_pending_checklist_steps("cat_x", terminal_status="degraded")

    assert result is True
    assert captured_sql, "expected the method to build a terminal UPDATE"
    sql = captured_sql[0]
    assert "first_ready_at = CASE WHEN CAST(:st AS VARCHAR) = 'ready'" in sql
    assert "COALESCE(first_ready_at, NOW())" in sql
    assert captured_kwargs[0].get("st") == STATUS_READY
