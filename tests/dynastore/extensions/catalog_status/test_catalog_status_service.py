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

"""Unit tests for the catalog_status extension.

Pure-unit, mock-based (no live DB).  Uses FastAPI TestClient and the
module-level business-logic helpers to avoid requiring a real DB.

Covers:
- Service class always_on and entry-point name.
- Status view returns CatalogStatusView with provisioning fields and task
  when present; task=None when tasks table absent (graceful degradation).
- Visibility 404 when resolve_catalog_listing_ids returns a frozenset NOT
  containing the catalog; None (IAM off) → unfiltered.
- Collection status visibility 404 likewise.
- Reprovision enqueues the unified catalog_provision task (keyed on the
  physical id) and returns 202 shape; empty checklist → noop.
- Dead-letter list and requeue call the maintenance primitives with the
  resolved tenant schema.
- Policies shape: catalog_status_admin gates mutation paths with
  catalog_admin_required; read policy allows GET on status paths.
"""
from __future__ import annotations

import uuid
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

# ---------------------------------------------------------------------------
# Module paths used for patching
# ---------------------------------------------------------------------------

_SVC_MODULE = "dynastore.extensions.catalog_status.catalog_status_service"
_RESOLVE_CATALOG = f"{_SVC_MODULE}.resolve_catalog_listing_ids"
_RESOLVE_COLLECTION = f"{_SVC_MODULE}.resolve_collection_listing_ids"
_GET_PROTOCOL = f"{_SVC_MODULE}.get_protocol"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _fake_catalog(
    catalog_id: str = "test-cat",
    provisioning_status: str = "ready",
) -> SimpleNamespace:
    return SimpleNamespace(id=catalog_id, provisioning_status=provisioning_status)


def _fake_collection(collection_id: str = "test-col") -> SimpleNamespace:
    return SimpleNamespace(id=collection_id)


def _fake_task() -> SimpleNamespace:
    from datetime import datetime
    tid = uuid.uuid4()
    return SimpleNamespace(
        jobID=tid,
        task_type="gcp_provision_catalog",
        status=SimpleNamespace(value="ACTIVE"),
        error_message=None,
        retry_count=0,
        max_retries=3,
        timestamp=datetime(2026, 1, 1),
        finished_at=datetime(2026, 1, 2),
    )


# ---------------------------------------------------------------------------
# 1. Class-level invariants
# ---------------------------------------------------------------------------


def test_always_on_is_true():
    from dynastore.extensions.catalog_status.catalog_status_service import CatalogStatusService
    assert CatalogStatusService.always_on is True


def test_router_prefix():
    from dynastore.extensions.catalog_status.catalog_status_service import CatalogStatusService
    assert CatalogStatusService.router.prefix == "/catalog"


def test_entry_point_name():
    """Entry-point name must match ``catalog_status``."""
    import importlib.metadata as im

    eps = im.entry_points(group="dynastore.extensions")
    names = {ep.name for ep in eps}
    assert "catalog_status" in names, (
        f"entry-point 'catalog_status' not found; available: {names}"
    )


# ---------------------------------------------------------------------------
# 2. _assert_catalog_visible — hidden catalog raises 404
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_assert_catalog_visible_raises_on_hidden():
    from fastapi import HTTPException
    from dynastore.extensions.catalog_status.catalog_status_service import _assert_catalog_visible

    with patch(_RESOLVE_CATALOG, AsyncMock(return_value=frozenset({"other-cat"}))):
        with pytest.raises(HTTPException) as exc_info:
            await _assert_catalog_visible("test-cat")

    assert exc_info.value.status_code == 404


@pytest.mark.asyncio
async def test_assert_catalog_visible_passes_when_iam_off():
    """None return from resolver (IAM off) → no filtering."""
    from dynastore.extensions.catalog_status.catalog_status_service import _assert_catalog_visible

    # Should not raise
    with patch(_RESOLVE_CATALOG, AsyncMock(return_value=None)):
        await _assert_catalog_visible("any-cat")


@pytest.mark.asyncio
async def test_assert_catalog_visible_passes_when_in_set():
    from dynastore.extensions.catalog_status.catalog_status_service import _assert_catalog_visible

    with patch(_RESOLVE_CATALOG, AsyncMock(return_value=frozenset({"test-cat", "other"}))):
        # Should not raise
        await _assert_catalog_visible("test-cat")


# ---------------------------------------------------------------------------
# 3. _assert_collection_visible
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_assert_collection_visible_raises_on_hidden():
    from fastapi import HTTPException
    from dynastore.extensions.catalog_status.catalog_status_service import _assert_collection_visible

    with patch(_RESOLVE_COLLECTION, AsyncMock(return_value=frozenset())):
        with pytest.raises(HTTPException) as exc_info:
            await _assert_collection_visible("cat1", "col1")

    assert exc_info.value.status_code == 404


@pytest.mark.asyncio
async def test_assert_collection_visible_passes_when_iam_off():
    from dynastore.extensions.catalog_status.catalog_status_service import _assert_collection_visible

    with patch(_RESOLVE_COLLECTION, AsyncMock(return_value=None)):
        await _assert_collection_visible("cat1", "col1")


# ---------------------------------------------------------------------------
# 4. list_catalog_dead_letter — calls DLQ primitive with tenant schema
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_list_catalog_dead_letter_calls_dlq_with_schema():
    from dynastore.extensions.catalog_status.catalog_status_service import list_catalog_dead_letter

    fake_engine = MagicMock()
    fake_rows = [{"task_id": "abc", "status": "DEAD_LETTER"}]

    with (
        patch(f"{_SVC_MODULE}._platform_engine", return_value=fake_engine),
        patch(
            f"{_SVC_MODULE}._catalog_task_schema",
            AsyncMock(return_value="tenant_schema"),
        ),
        patch(
            f"{_SVC_MODULE}._dlq_list",
            AsyncMock(return_value=fake_rows),
        ) as mock_dlq_list,
    ):
        result = await list_catalog_dead_letter("test-cat")

    mock_dlq_list.assert_awaited_once_with(fake_engine, schema_name="tenant_schema")
    assert result == fake_rows


# ---------------------------------------------------------------------------
# 5. requeue_catalog_dead_letter — calls DLQ primitive with tenant schema
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_requeue_catalog_dead_letter_calls_dlq_with_schema():
    from dynastore.extensions.catalog_status.catalog_status_service import requeue_catalog_dead_letter

    fake_engine = MagicMock()

    with (
        patch(f"{_SVC_MODULE}._platform_engine", return_value=fake_engine),
        patch(
            f"{_SVC_MODULE}._catalog_task_schema",
            AsyncMock(return_value="tenant_schema"),
        ),
        patch(
            f"{_SVC_MODULE}._dlq_requeue",
            AsyncMock(return_value=True),
        ) as mock_dlq_requeue,
    ):
        result = await requeue_catalog_dead_letter("test-cat", "task-xyz")

    mock_dlq_requeue.assert_awaited_once_with(
        fake_engine, "task-xyz", reset_retries=True, schema_name="tenant_schema"
    )
    assert result["requeued"] is True
    assert result["task_id"] == "task-xyz"


# ---------------------------------------------------------------------------
# 6. Reprovision — enqueues task and returns expected shape
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_reprovision_catalog_enqueues_task():
    """Tests the reprovision business logic by calling the route handler
    directly with appropriate mocks."""
    fake_cat = _fake_catalog()
    fake_task_id = uuid.uuid4()
    fake_created = SimpleNamespace(task_id=fake_task_id)

    catalogs_mock = MagicMock()
    catalogs_mock.get_catalog_model = AsyncMock(return_value=fake_cat)
    # #2395: reprovision resolves the physical schema id and resets the
    # to-be-rerun checklist steps before enqueuing the unified executor.
    catalogs_mock.resolve_physical_schema = AsyncMock(return_value="c_phys")
    catalogs_mock.reset_checklist_for_reprovision = AsyncMock(
        return_value={"gcp_eventing": "pending"}
    )

    db_mock = SimpleNamespace(engine=MagicMock())

    create_task_mock = AsyncMock(return_value=fake_created)

    def _proto(proto):
        from dynastore.models.protocols.catalogs import CatalogsProtocol
        from dynastore.models.protocols import DatabaseProtocol
        if proto is CatalogsProtocol:
            return catalogs_mock
        if proto is DatabaseProtocol:
            return db_mock
        return None

    # The reprovision handler uses `from dynastore.modules.tasks import tasks_module`
    # (lazy import inside the body). Patch `tasks_module` in the submodule so the
    # lazy `from ... import tasks_module` inside the handler receives our mock.
    fake_tm = MagicMock()
    fake_tm.create_task_for_catalog = create_task_mock

    from dynastore.extensions.catalog_status.catalog_status_service import CatalogStatusService
    handler = None
    for route in CatalogStatusService.router.routes:
        if "reprovision" in getattr(route, "path", ""):
            handler = route.endpoint
            break
    assert handler is not None, (
        "reprovision route not found in CatalogStatusService.router.routes; "
        f"routes: {[getattr(r, 'path', None) for r in CatalogStatusService.router.routes]}"
    )

    with (
        patch(_GET_PROTOCOL, side_effect=_proto),
        patch("dynastore.modules.tasks.tasks_module", fake_tm),
    ):
        result = await handler("test-cat")

    create_task_mock.assert_awaited_once()
    # Drives the unified executor (not the legacy gcp_provision_catalog) and
    # keys it on the physical schema id, never the external id on the wire.
    kwargs = create_task_mock.await_args.kwargs
    assert kwargs["task_data"].task_type == "catalog_provision"
    assert kwargs["task_data"].inputs["catalog_id"] == "c_phys"
    assert kwargs["task_data"].inputs["operation"] == "provision"
    assert kwargs["catalog_id"] == "c_phys"
    catalogs_mock.reset_checklist_for_reprovision.assert_awaited_once()
    assert result["status"] == "queued"
    assert result["catalog_id"] == "test-cat"  # external id echoed back
    assert result["provisioning_status"] == "provisioning"
    assert result["task_id"] == str(fake_task_id)


async def test_reprovision_noop_when_no_active_provisioners():
    """An empty checklist (on-prem / no provisioners) returns a noop without
    enqueuing a task."""
    fake_cat = _fake_catalog()

    catalogs_mock = MagicMock()
    catalogs_mock.get_catalog_model = AsyncMock(return_value=fake_cat)
    catalogs_mock.resolve_physical_schema = AsyncMock(return_value="c_phys")
    catalogs_mock.reset_checklist_for_reprovision = AsyncMock(return_value={})

    db_mock = SimpleNamespace(engine=MagicMock())
    create_task_mock = AsyncMock()

    def _proto(proto):
        from dynastore.models.protocols.catalogs import CatalogsProtocol
        from dynastore.models.protocols import DatabaseProtocol
        if proto is CatalogsProtocol:
            return catalogs_mock
        if proto is DatabaseProtocol:
            return db_mock
        return None

    fake_tm = MagicMock()
    fake_tm.create_task_for_catalog = create_task_mock

    from dynastore.extensions.catalog_status.catalog_status_service import CatalogStatusService
    handler = None
    for route in CatalogStatusService.router.routes:
        if "reprovision" in getattr(route, "path", ""):
            handler = route.endpoint
            break
    assert handler is not None

    with (
        patch(_GET_PROTOCOL, side_effect=_proto),
        patch("dynastore.modules.tasks.tasks_module", fake_tm),
    ):
        result = await handler("test-cat")

    create_task_mock.assert_not_awaited()
    assert result["status"] == "noop"
    assert result["catalog_id"] == "test-cat"


# ---------------------------------------------------------------------------
# 7. Policies shape
# ---------------------------------------------------------------------------


def test_catalog_status_policies_ids_pinned():
    from dynastore.extensions.catalog_status.policies import catalog_status_policies

    ids = [p.id for p in catalog_status_policies()]
    assert "catalog_status_read" in ids
    assert "catalog_status_admin" in ids


def test_catalog_status_read_policy_shape():
    from dynastore.extensions.catalog_status.policies import catalog_status_policies

    pols = {p.id: p for p in catalog_status_policies()}
    read_pol = pols["catalog_status_read"]
    assert read_pol.effect == "ALLOW"
    assert "GET" in read_pol.actions
    # Must cover both catalog and collection status paths
    assert any("/catalog/catalogs/" in r for r in read_pol.resources)
    # The read surface exposes operational detail (schema name, task error
    # messages), so it is membership-gated: catalog_membership_required fails
    # closed for anonymous callers. A bare ALLOW here would leak status of
    # public catalogs to the open internet.
    assert any(
        c.type == "catalog_membership_required" for c in (read_pol.conditions or [])
    ), "read policy must be gated by catalog_membership_required (deny anonymous)"


def test_catalog_status_admin_policy_has_catalog_admin_required_condition():
    from dynastore.extensions.catalog_status.policies import catalog_status_policies

    pols = {p.id: p for p in catalog_status_policies()}
    admin_pol = pols["catalog_status_admin"]
    assert admin_pol.effect == "ALLOW"
    assert any(c.type == "catalog_admin_required" for c in (admin_pol.conditions or []))
    # Must gate mutation paths (reprovision and dead-letter)
    assert any("reprovision" in r or "dead-letter" in r for r in admin_pol.resources)


def test_catalog_status_read_bound_to_universal_base_role():
    """Read policy is bound to the configured base role (``unauthenticated`` by
    default), not a literal ``"anonymous"`` string that no seed provides. The
    binding only makes the policy reachable for every member; the
    catalog_membership_required condition is the actual access control and
    denies anonymous callers.
    """
    from dynastore.extensions.catalog_status.policies import catalog_status_role_bindings
    from dynastore.models.protocols.authorization import IamRolesConfig

    cfg = IamRolesConfig()
    base_policies: set = set()
    for rb in catalog_status_role_bindings():
        if rb.name == cfg.anonymous_role_name:
            base_policies.update(rb.policies or [])
    assert "catalog_status_read" in base_policies, (
        "catalog_status_read must be bound to IamRolesConfig().anonymous_role_name "
        f"(== {cfg.anonymous_role_name!r}), the universal base role every member "
        "carries; binding to a non-existent role name would reach no one"
    )


# ---------------------------------------------------------------------------
# 8. provisioning_checklist surfaced in CatalogStatusView
# ---------------------------------------------------------------------------


def test_catalog_status_view_includes_provisioning_checklist_field():
    """CatalogStatusView must declare a provisioning_checklist field that
    defaults to an empty dict and accepts a string-to-string mapping."""
    from dynastore.extensions.catalog_status.catalog_status_models import CatalogStatusView

    # Default: empty dict when no checklist is present.
    view_no_checklist = CatalogStatusView(
        external_id="cat-1",
        provisioning_status="ready",
    )
    assert view_no_checklist.provisioning_checklist == {}

    # Populated: degraded eventing is visible to the operator.
    view_with_checklist = CatalogStatusView(
        external_id="cat-1",
        provisioning_status="ready",
        provisioning_checklist={"gcp_bucket": "complete", "gcp_eventing": "degraded"},
    )
    assert view_with_checklist.provisioning_checklist == {
        "gcp_bucket": "complete",
        "gcp_eventing": "degraded",
    }


def _get_catalog_status_handler():
    from dynastore.extensions.catalog_status.catalog_status_service import CatalogStatusService
    for route in CatalogStatusService.router.routes:
        if getattr(route, "name", "") == "get_catalog_status":
            return route.endpoint
    raise AssertionError("get_catalog_status route not found")


@pytest.mark.asyncio
async def test_get_catalog_status_populates_provisioning_checklist():
    """get_catalog_status must read provisioning_checklist via the protocol
    getter using the resolved physical id (not the external id from the URL
    path) and forward it into the CatalogStatusView response."""
    checklist = {"gcp_bucket": "complete", "gcp_eventing": "degraded"}
    external_id = "cat-checklist"
    physical_id = "c_phys_checklist"
    fake_cat = SimpleNamespace(id=physical_id, provisioning_status="ready")

    catalogs_mock = MagicMock()
    catalogs_mock.get_catalog_model = AsyncMock(return_value=fake_cat)
    # resolve_physical_schema returns a physical id distinct from the external id.
    catalogs_mock.resolve_physical_schema = AsyncMock(return_value=physical_id)
    # The service calls get_provisioning_checklist keyed on the physical id.
    catalogs_mock.get_provisioning_checklist = AsyncMock(return_value=checklist)

    def _proto(proto):
        from dynastore.models.protocols.catalogs import CatalogsProtocol
        from dynastore.models.protocols import DatabaseProtocol
        if proto is CatalogsProtocol:
            return catalogs_mock
        if proto is DatabaseProtocol:
            return None
        return None

    handler = _get_catalog_status_handler()

    with (
        patch(_RESOLVE_CATALOG, AsyncMock(return_value=None)),
        patch(_GET_PROTOCOL, side_effect=_proto),
    ):
        result = await handler(external_id)

    # Must be called with the physical id, not the external id from the URL.
    catalogs_mock.get_provisioning_checklist.assert_awaited_once_with(physical_id)
    assert result.provisioning_checklist == checklist


@pytest.mark.asyncio
async def test_get_catalog_status_checklist_empty_when_getter_returns_empty():
    """When get_provisioning_checklist returns {} (no checklist in PG),
    the response must include an empty dict — not crash."""
    fake_cat = SimpleNamespace(id="c_phys_old", provisioning_status="ready")

    catalogs_mock = MagicMock()
    catalogs_mock.get_catalog_model = AsyncMock(return_value=fake_cat)
    catalogs_mock.resolve_physical_schema = AsyncMock(return_value="c_phys_old")
    catalogs_mock.get_provisioning_checklist = AsyncMock(return_value={})

    def _proto(proto):
        from dynastore.models.protocols.catalogs import CatalogsProtocol
        from dynastore.models.protocols import DatabaseProtocol
        if proto is CatalogsProtocol:
            return catalogs_mock
        if proto is DatabaseProtocol:
            return None
        return None

    handler = _get_catalog_status_handler()

    with (
        patch(_RESOLVE_CATALOG, AsyncMock(return_value=None)),
        patch(_GET_PROTOCOL, side_effect=_proto),
    ):
        result = await handler("cat-old")

    catalogs_mock.get_provisioning_checklist.assert_awaited_once_with("c_phys_old")
    assert result.provisioning_checklist == {}


@pytest.mark.asyncio
async def test_get_catalog_status_checklist_empty_on_getter_error():
    """When get_provisioning_checklist raises (e.g. DB unavailable), the
    endpoint must not 500 — it returns an empty checklist and logs a warning."""
    fake_cat = SimpleNamespace(id="c_phys_err", provisioning_status="ready")

    catalogs_mock = MagicMock()
    catalogs_mock.get_catalog_model = AsyncMock(return_value=fake_cat)
    # physical_schema is non-None so the getter is actually invoked.
    catalogs_mock.resolve_physical_schema = AsyncMock(return_value="c_phys_err")
    catalogs_mock.get_provisioning_checklist = AsyncMock(
        side_effect=RuntimeError("DB unavailable")
    )

    def _proto(proto):
        from dynastore.models.protocols.catalogs import CatalogsProtocol
        from dynastore.models.protocols import DatabaseProtocol
        if proto is CatalogsProtocol:
            return catalogs_mock
        if proto is DatabaseProtocol:
            return None
        return None

    handler = _get_catalog_status_handler()

    with (
        patch(_RESOLVE_CATALOG, AsyncMock(return_value=None)),
        patch(_GET_PROTOCOL, side_effect=_proto),
    ):
        result = await handler("cat-err")

    # Getter was called with the physical id; error degrades gracefully to empty.
    catalogs_mock.get_provisioning_checklist.assert_awaited_once_with("c_phys_err")
    assert result.provisioning_checklist == {}


@pytest.mark.asyncio
async def test_get_catalog_status_checklist_uses_physical_id_not_external_id():
    """When external_id differs from physical_id, get_provisioning_checklist
    must be called with the physical_id (from resolve_physical_schema), not
    the external_id from the URL path.  This guards the regression where
    passing the external id to the query (WHERE id = :id on the physical pk)
    silently matched nothing and returned {}."""
    external_id = "my-catalog-external"
    physical_id = "c_abc123physical"
    checklist = {"catalog_core": "complete", "gcp_bucket": "complete"}

    fake_cat = SimpleNamespace(id=physical_id, provisioning_status="ready")
    catalogs_mock = MagicMock()
    catalogs_mock.get_catalog_model = AsyncMock(return_value=fake_cat)
    catalogs_mock.resolve_physical_schema = AsyncMock(return_value=physical_id)
    catalogs_mock.get_provisioning_checklist = AsyncMock(return_value=checklist)

    def _proto(proto):
        from dynastore.models.protocols.catalogs import CatalogsProtocol
        from dynastore.models.protocols import DatabaseProtocol
        if proto is CatalogsProtocol:
            return catalogs_mock
        if proto is DatabaseProtocol:
            return None
        return None

    handler = _get_catalog_status_handler()

    with (
        patch(_RESOLVE_CATALOG, AsyncMock(return_value=None)),
        patch(_GET_PROTOCOL, side_effect=_proto),
    ):
        result = await handler(external_id)

    # The physical id and external id must differ for this test to be meaningful.
    assert physical_id != external_id
    # Getter must be called with the physical id, not the external id.
    catalogs_mock.get_provisioning_checklist.assert_awaited_once_with(physical_id)
    assert result.provisioning_checklist == checklist


@pytest.mark.asyncio
async def test_get_catalog_status_surfaces_catalog_provision_task():
    """A task of type 'catalog_provision' (the current type used by create_catalog
    since #2329) must appear in the CatalogStatusView task field."""
    import contextlib
    from datetime import datetime

    fake_cat = SimpleNamespace(id="cat-prov", provisioning_status="provisioning")
    fake_tid = uuid.uuid4()
    fake_task = SimpleNamespace(
        jobID=fake_tid,
        task_type="catalog_provision",
        status=SimpleNamespace(value="ACTIVE"),
        error_message=None,
        retry_count=0,
        max_retries=3,
        timestamp=datetime(2026, 1, 1),
        finished_at=None,
    )

    catalogs_mock = MagicMock()
    catalogs_mock.get_catalog_model = AsyncMock(return_value=fake_cat)
    catalogs_mock.resolve_physical_schema = AsyncMock(return_value="tenant_schema")
    catalogs_mock.get_provisioning_checklist = AsyncMock(
        return_value={"catalog_core": "complete"}
    )

    db_mock = SimpleNamespace(engine=MagicMock())

    def _proto(proto):
        from dynastore.models.protocols.catalogs import CatalogsProtocol
        from dynastore.models.protocols import DatabaseProtocol
        if proto is CatalogsProtocol:
            return catalogs_mock
        if proto is DatabaseProtocol:
            return db_mock
        return None

    fake_tm = MagicMock()
    fake_tm.list_tasks = AsyncMock(return_value=[fake_task])

    @contextlib.asynccontextmanager
    async def _noop_tx(engine):
        yield MagicMock()

    handler = _get_catalog_status_handler()

    # managed_transaction is imported inside the handler body; patch it where
    # the module looks it up (the query_executor module), not on catalog_status_service.
    with (
        patch(_RESOLVE_CATALOG, AsyncMock(return_value=None)),
        patch(_GET_PROTOCOL, side_effect=_proto),
        patch("dynastore.modules.tasks.tasks_module", fake_tm),
        patch(
            "dynastore.modules.db_config.query_executor.managed_transaction",
            _noop_tx,
        ),
    ):
        result = await handler("cat-prov")

    assert result.task is not None, "task must not be None for catalog_provision tasks"
    assert str(result.task.task_id) == str(fake_tid)
    assert result.provisioning_checklist == {"catalog_core": "complete"}


def test_catalog_status_role_bindings_admin_mutation():
    from dynastore.extensions.catalog_status.policies import catalog_status_role_bindings
    from dynastore.models.protocols.authorization import IamRolesConfig

    cfg = IamRolesConfig()
    from collections import defaultdict
    policy_sets: dict = defaultdict(set)
    for rb in catalog_status_role_bindings():
        for pol in (rb.policies or []):
            policy_sets[rb.name].add(pol)

    # Both sysadmin and admin must carry catalog_status_admin
    assert "catalog_status_admin" in policy_sets.get(cfg.sysadmin_role_name, set()), (
        f"sysadmin must be bound to catalog_status_admin; bindings: {dict(policy_sets)}"
    )
    assert "catalog_status_admin" in policy_sets.get(cfg.admin_role_name, set()), (
        f"admin must be bound to catalog_status_admin; bindings: {dict(policy_sets)}"
    )
