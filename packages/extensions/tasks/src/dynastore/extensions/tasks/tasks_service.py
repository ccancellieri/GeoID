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

"""Unified Tasks API.

Router prefix: ``/task``

Individual tasks are addressed under an explicit ``tasks/{task_id}``
collection segment at every scope — ``/task/tasks/{id}`` at system scope,
``/task/catalogs/{id}/tasks/{id}`` at catalog scope, etc.  This is
intentional: the URL redesign decouples the task resource from the singular
``/task`` prefix while keeping every Python identifier, module name, and
function name in its canonical plural form.

Route table (15 routes):

  SYSTEM
    1  GET  /task                                    list system tasks          [tasks_system_read]
    2  GET  /task/tasks/{task_id}                    get any task unscoped      [tasks_system_admin]
    7  POST /task                                    spawn system scope         [tasks_system_admin]
   10  GET  /task/dead-letter                        DLQ all tenants            [tasks_system_admin]
   13  POST /task/dead-letter/{task_id}/requeue                                 [tasks_system_admin]

  CATALOG
    3  GET  /task/catalogs/{catalog_id}              list catalog tasks         [tasks_read]
    4  GET  /task/catalogs/{catalog_id}/tasks/{task_id}  get one               [tasks_read]
    8  POST /task/catalogs/{catalog_id}              spawn catalog scope        [tasks_admin]
   11  GET  /task/catalogs/{catalog_id}/dead-letter  DLQ for catalog            [tasks_admin]
   14  POST /task/catalogs/{catalog_id}/dead-letter/{task_id}/requeue           [tasks_admin]

  COLLECTION
    5  GET  /task/catalogs/{catalog_id}/collections/{collection_id}             [tasks_read]
    6  GET  /task/catalogs/{catalog_id}/collections/{collection_id}/tasks/{task_id}  [tasks_read]
    9  POST /task/catalogs/{catalog_id}/collections/{collection_id}             [tasks_admin]
   12  GET  /task/catalogs/{catalog_id}/collections/{collection_id}/dead-letter [tasks_admin]
   15  POST /task/catalogs/{catalog_id}/collections/{collection_id}/dead-letter/{task_id}/requeue  [tasks_admin]

Authorization is enforced entirely through PermissionProtocol policies
declared in ``policies.py`` and registered via the preset in
``presets/__init__.py``.  No inline role checks exist in this module.
"""
from __future__ import annotations

import logging
import uuid
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

from fastapi import APIRouter, Depends, HTTPException, Query, Request, Response, status
from sqlalchemy.ext.asyncio import AsyncConnection

from dynastore.extensions.protocols import ExtensionProtocol
from dynastore.extensions.tools.db import get_async_connection, get_async_engine
from dynastore.models.auth_models import SYSTEM_USER_ID
from dynastore.models.protocols import CatalogsProtocol
from dynastore.models.protocols.visibility import (
    resolve_catalog_listing_ids,
    resolve_collection_listing_ids,
)
from dynastore.models.tasks import (
    RequeueResult,
    SpawnTaskRequest,
    Task,
    TaskCreate,
    TaskPage,
    TaskRef,
    TaskScope,
)
from dynastore.modules.tasks import tasks_module
from dynastore.modules.tasks.execution import _EXECUTION_OVERRIDES_KEY
from dynastore.modules.tasks.maintenance import (
    list_dead_letter_tasks as _dlq_list,
    requeue_dead_letter_task as _dlq_requeue,
)
from dynastore.modules.tasks.reconciliation import reconcile_task_liveness
from dynastore.modules.tasks.tasks_module import encode_cursor
from dynastore.tools.discovery import get_protocol

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Visibility helpers (mirrors catalog_status_service for 404-on-hidden parity)
# ---------------------------------------------------------------------------


async def _assert_catalog_visible(catalog_id: str) -> None:
    """Raise 404 (no-leak shape) when the catalog is hidden to this caller."""
    visible_ids = await resolve_catalog_listing_ids()
    if visible_ids is not None and catalog_id not in visible_ids:
        raise HTTPException(
            status_code=404, detail=f"Catalog '{catalog_id}' not found."
        )


async def _assert_collection_visible(catalog_id: str, collection_id: str) -> None:
    """Raise 404 when the collection is hidden to this caller."""
    visible_ids = await resolve_collection_listing_ids(catalog_id)
    if visible_ids is not None and collection_id not in visible_ids:
        raise HTTPException(
            status_code=404,
            detail=f"Collection '{collection_id}' not found in catalog '{catalog_id}'.",
        )


# ---------------------------------------------------------------------------
# Uncached task fetch + schema check (reused by routes 4 and 6)
# ---------------------------------------------------------------------------


async def _get_task_scoped_uncached(
    task_id: uuid.UUID,
    catalog_id: str,
    conn: AsyncConnection,
    collection_id: Optional[str] = None,
) -> Task:
    """Resolve ``task_id`` FIRST, then cross-check it against ``catalog_id``
    (and, when scoped, ``collection_id``).

    Task-id-first is intentional (#2674): a hard-deleted catalog's registry
    row is fully purged (a ``catalog.catalogs`` DELETE, not just a tombstone)
    by the time its own ``catalog_provision`` deprovision task reaches a
    terminal state (COMPLETED/FAILED). Resolving the catalog's tenant schema
    BEFORE the task turned a legitimate terminal-status poll into an
    unconditional 404, indistinguishable from "task never existed" —
    genuinely unknown task ids still 404 below.

    ``task_id`` is a globally-unique UUIDv7, so an unscoped read plus an
    explicit schema check (when the catalog can still be resolved) is safe.
    Mirrors the OGC Processes job-status route.

    Uncached on purpose. A task's terminal status is written by the
    BackgroundRunner that owns completion, which runs on whichever instance
    claimed the task — not necessarily the API instance serving this poll. The
    in-process ``get_task`` cache here is never invalidated by that
    cross-instance flip, so the cached read pins the task at its creation-time
    status (e.g. ``ACTIVE``) for the whole cache TTL. The async hard-delete
    advertises this route via its ``Location``/monitor link, so a cached read
    would leave callers unable to ever observe ``COMPLETED``.

    When the catalog can no longer be resolved at all — the deprovision task
    has finished dropping the registry row — the schema cross-check and the
    listing-visibility re-checks below (both catalog- and, when scoped,
    collection-level) are all skipped: there is no surviving id mapping left
    to derive any of them from. The task's terminal payload is then served on
    the strength of the perimeter ``tasks_read`` + ``catalog_membership_required``
    IAM policy that already authorized this request to reach the handler at
    all (see the PR description for the tradeoff this accepts).
    ``IamCatalogScopedOwner.cleanup_one`` bumps the IAM binding-version
    counter as part of the same deprovision cascade so a revoked member's
    cached membership stops passing that perimeter check promptly rather
    than riding out the full cache TTL, but a residual window still exists
    between the cascade starting to remove catalog-scoped grants and that
    bump landing — bounded by the cascade cleanup task's own latency, not by
    the membership cache's TTL.

    The ``task.collection_id`` cross-check (#2685) is deliberately kept
    OUTSIDE the schema-resolvable gate: it compares the task row's own
    ``collection_id`` against the URL's literal ``collection_id``, so it
    stays correct even once the catalog is torn down. Only the
    listing-visibility re-check (hidden collection ⇒ 404) needs a resolvable
    catalog, same as the catalog one.

    #2777: internal machinery (index_propagation, storage_drain,
    cascade_cleanup, ...) stores the *physical* collection id on the task
    row at spawn time, but ``collection_id`` here is always the *external*
    URL id (#2710) — a raw comparison would spuriously 404 those tasks. The
    row is projected to its external id via ``_project_collection_external_ids``
    (the same ``CollectionsProtocol.resolve_collection_external_id`` mapping
    the catalog/collection CRUD read path relies on) before comparing, so
    both sides are in the same id space. That projection fails open (stored
    value untouched) when the mapping is unavailable, so it needs no
    resolvable-schema gate of its own.
    """
    task = await tasks_module.get_task_by_id_unscoped(conn, task_id)
    if not task:
        raise HTTPException(
            status_code=404,
            detail=f"Task with ID '{task_id}' not found in catalog '{catalog_id}'.",
        )

    await _project_collection_external_ids([task])

    if collection_id is not None and task.collection_id and task.collection_id != collection_id:
        raise HTTPException(
            status_code=404,
            detail=(
                f"Task '{task_id}' does not belong to "
                f"collection '{collection_id}'."
            ),
        )

    try:
        schema: Optional[str] = await tasks_module._resolve_catalog_schema(catalog_id, conn)
    except ValueError:
        schema = None

    if schema is not None:
        if task.catalog_id != schema:
            raise HTTPException(
                status_code=404,
                detail=f"Task with ID '{task_id}' not found in catalog '{catalog_id}'.",
            )
        await _assert_catalog_visible(catalog_id)
        if collection_id is not None:
            await _assert_collection_visible(catalog_id, collection_id)

    try:
        task = await reconcile_task_liveness(conn, task, schema=task.catalog_id or "")
    except Exception as e:  # noqa: BLE001 — best-effort; never turn a 200 into a 500
        logger.warning(
            "reconcile_task_liveness failed for task %s: %s — serving unreconciled status.",
            task_id, e,
        )
    await _project_collection_external_ids([task])
    return task


# ---------------------------------------------------------------------------
# Physical -> external collection id projection (#2710)
# ---------------------------------------------------------------------------


async def _project_collection_external_ids(tasks: List[Task]) -> List[Task]:
    """Project each task's stored ``collection_id`` to its public external_id.

    Internal machinery (index_propagation, storage_drain, cascade_cleanup, ...)
    stores the physical collection id in the ``collection_id`` column at spawn
    time. Resolving here, at the REST read boundary, rather than at spawn
    time keeps every task row correct across a collection rename. Reuses the
    same ``CollectionsProtocol.resolve_collection_external_id`` mapping the
    catalog/collection CRUD read path already relies on. Fails open (leaves
    the stored value untouched) when the mapping is unavailable or the id is
    already external, so a resolver hiccup never turns a 200 into a 500.
    """
    catalogs = get_protocol(CatalogsProtocol)
    if catalogs is None:
        return tasks

    cache: Dict[Tuple[str, str], str] = {}
    for task in tasks:
        if not task.collection_id or not task.catalog_id:
            continue
        key = (task.catalog_id, task.collection_id)
        if key not in cache:
            try:
                resolved = await catalogs.collections.resolve_collection_external_id(
                    task.catalog_id, task.collection_id, allow_missing=True
                )
            except Exception:
                resolved = None
            cache[key] = resolved or task.collection_id
        task.collection_id = cache[key]
    return tasks


# ---------------------------------------------------------------------------
# Pagination helper
# ---------------------------------------------------------------------------


async def _to_task_page(rows: List[Task], limit: int) -> TaskPage:
    """Slice rows to limit; encode cursor from the (limit+1)-th row if present."""
    next_cursor: Optional[str] = None
    if len(rows) > limit:
        next_cursor = encode_cursor(rows[limit])
        rows = rows[:limit]
    await _project_collection_external_ids(rows)
    return TaskPage(items=rows, next_cursor=next_cursor)


# ---------------------------------------------------------------------------
# Spawn helpers (kept thin: module-level so handlers stay one-liners)
# ---------------------------------------------------------------------------


async def _validate_spawn(
    body: SpawnTaskRequest,
    *,
    allow_destructive: bool = False,
) -> None:
    """Validate a spawn request against the task registry.

    Raises:
        HTTPException(404): unknown task_type.
        HTTPException(422): task_type is a process (use Processes API).
        HTTPException(403): destructive operation at a non-system scope.
    """
    from dynastore.tasks import discover_tasks, get_task_config, task_kind

    discover_tasks()
    cfg = get_task_config(body.task_type)
    if cfg is None:
        raise HTTPException(
            status_code=404,
            detail=f"Unknown task type '{body.task_type}'.",
        )
    if task_kind(cfg) == "process":
        raise HTTPException(
            status_code=422,
            detail=(
                f"Task type '{body.task_type}' is an OGC Process; "
                "submit executions via the Processes API."
            ),
        )
    if not allow_destructive:
        operation = body.inputs.get("operation", "")
        if operation in {"deprovision_hard", "deprovision"}:
            raise HTTPException(
                status_code=403,
                detail=(
                    "Destructive operations (deprovision / deprovision_hard) may "
                    "only be spawned from the system scope."
                ),
            )


IDEMPOTENT_SPAWN_DESC = (
    "Idempotent dedup hit: a non-terminal task with the same dedup_key already "
    "existed, so the existing TaskRef is returned instead of creating a duplicate."
)


async def _resolve_spawn_result(
    engine: Any,
    schema: str,
    body: SpawnTaskRequest,
    task: Optional[Task],
    response: Response,
) -> Tuple[uuid.UUID, str]:
    """Resolve the ``(task_id, status)`` pair for a spawn response.

    On a successful insert, returns the new task's id and status.  When
    ``create_task`` returned None — a non-terminal task with the same
    ``dedup_key`` already exists — the spawn is idempotent: look the existing
    task up, flip the response to 200, and return its reference so a retried
    spawn yields the same handle instead of a 409.
    """
    if task is not None:
        return task.task_id, getattr(task.status, "value", task.status)

    # create_task only returns None on a dedup pre-check hit, which can only
    # happen when a dedup_key was supplied — so it is non-None here.
    if body.dedup_key is None:
        raise HTTPException(
            status_code=409,
            detail="A non-terminal task with the same dedup_key already exists.",
        )
    existing = await tasks_module.get_active_task_by_dedup_key(
        engine, schema, body.dedup_key
    )
    if existing is None:
        # Raced to a terminal state between the insert pre-check and this lookup.
        raise HTTPException(
            status_code=409,
            detail="A non-terminal task with the same dedup_key already exists.",
        )
    response.status_code = status.HTTP_200_OK
    return existing["task_id"], getattr(
        existing["status"], "value", existing["status"]
    )


async def _do_spawn_system(
    body: SpawnTaskRequest, request: Request, response: Response
) -> TaskRef:
    """Create a task at system scope (catalog_id = 'system')."""
    await _validate_spawn(body, allow_destructive=True)

    principal = getattr(request.state, "principal", None)
    caller_id = str(principal.id) if principal else SYSTEM_USER_ID
    engine = get_async_engine(request)

    inputs: Dict[str, Any] = dict(body.inputs)
    inputs["catalog_id"] = "system"
    if body.execution_overrides is not None:
        inputs[_EXECUTION_OVERRIDES_KEY] = body.execution_overrides.model_dump(exclude_none=True)

    task_data = TaskCreate(
        task_type=body.task_type,
        caller_id=caller_id,
        inputs=inputs,
        scope=TaskScope.SYSTEM,
        dedup_key=body.dedup_key,
        max_retries=(
            body.execution_overrides.max_retries
            if body.execution_overrides and body.execution_overrides.max_retries is not None
            else body.max_retries
        ),
    )
    task = await tasks_module.create_task(engine, task_data, schema="system")
    task_id, task_status = await _resolve_spawn_result(
        engine, "system", body, task, response
    )

    task_id_str = str(task_id)
    try:
        status_url = str(request.url_for("get_task_system", task_id=task_id_str))
    except Exception:
        base = str(request.base_url).rstrip("/")
        root_path = request.scope.get("root_path", "").rstrip("/")
        status_url = f"{base}{root_path}/task/tasks/{task_id_str}"

    return TaskRef(
        task_id=task_id,
        status=task_status,
        status_url=status_url,
    )


async def _do_spawn_catalog(
    catalog_id: str, body: SpawnTaskRequest, request: Request, response: Response
) -> TaskRef:
    """Create a task at catalog scope."""
    await _validate_spawn(body, allow_destructive=False)
    await _assert_catalog_visible(catalog_id)

    schema = await tasks_module._resolve_catalog_schema(catalog_id)

    principal = getattr(request.state, "principal", None)
    caller_id = str(principal.id) if principal else SYSTEM_USER_ID
    engine = get_async_engine(request)

    inputs: Dict[str, Any] = dict(body.inputs)
    inputs["catalog_id"] = schema
    if body.execution_overrides is not None:
        inputs[_EXECUTION_OVERRIDES_KEY] = body.execution_overrides.model_dump(exclude_none=True)

    task_data = TaskCreate(
        task_type=body.task_type,
        caller_id=caller_id,
        inputs=inputs,
        scope=TaskScope.CATALOG,
        dedup_key=body.dedup_key,
        max_retries=(
            body.execution_overrides.max_retries
            if body.execution_overrides and body.execution_overrides.max_retries is not None
            else body.max_retries
        ),
    )
    task = await tasks_module.create_task(engine, task_data, schema=schema)
    task_id, task_status = await _resolve_spawn_result(
        engine, schema, body, task, response
    )

    task_id_str = str(task_id)
    try:
        status_url = str(
            request.url_for(
                "get_task_status_catalog",
                catalog_id=catalog_id,
                task_id=task_id_str,
            )
        )
    except Exception:
        base = str(request.base_url).rstrip("/")
        root_path = request.scope.get("root_path", "").rstrip("/")
        status_url = (
            f"{base}{root_path}/task/catalogs/{catalog_id}/tasks/{task_id_str}"
        )

    return TaskRef(
        task_id=task_id,
        status=task_status,
        status_url=status_url,
    )


async def _do_spawn_collection(
    catalog_id: str,
    collection_id: str,
    body: SpawnTaskRequest,
    request: Request,
    response: Response,
) -> TaskRef:
    """Create a task at collection scope."""
    await _validate_spawn(body, allow_destructive=False)
    await _assert_catalog_visible(catalog_id)
    await _assert_collection_visible(catalog_id, collection_id)

    schema = await tasks_module._resolve_catalog_schema(catalog_id)

    principal = getattr(request.state, "principal", None)
    caller_id = str(principal.id) if principal else SYSTEM_USER_ID
    engine = get_async_engine(request)

    inputs: Dict[str, Any] = dict(body.inputs)
    inputs["catalog_id"] = schema
    inputs["collection_id"] = collection_id
    if body.execution_overrides is not None:
        inputs[_EXECUTION_OVERRIDES_KEY] = body.execution_overrides.model_dump(exclude_none=True)

    task_data = TaskCreate(
        task_type=body.task_type,
        caller_id=caller_id,
        inputs=inputs,
        scope=TaskScope.CATALOG,
        collection_id=collection_id,
        dedup_key=body.dedup_key,
        max_retries=(
            body.execution_overrides.max_retries
            if body.execution_overrides and body.execution_overrides.max_retries is not None
            else body.max_retries
        ),
    )
    task = await tasks_module.create_task(engine, task_data, schema=schema)
    task_id, task_status = await _resolve_spawn_result(
        engine, schema, body, task, response
    )

    task_id_str = str(task_id)
    try:
        status_url = str(
            request.url_for(
                "get_task_status_collection",
                catalog_id=catalog_id,
                collection_id=collection_id,
                task_id=task_id_str,
            )
        )
    except Exception:
        base = str(request.base_url).rstrip("/")
        root_path = request.scope.get("root_path", "").rstrip("/")
        status_url = (
            f"{base}{root_path}/task/catalogs/{catalog_id}"
            f"/collections/{collection_id}/tasks/{task_id_str}"
        )

    return TaskRef(
        task_id=task_id,
        status=task_status,
        status_url=status_url,
    )


# ---------------------------------------------------------------------------
# Service
# ---------------------------------------------------------------------------


class TasksService(ExtensionProtocol):
    priority: int = 100

    """
    Unified Tasks API.  Router prefix: ``/task``.

    Authorization is delegated entirely to IamMiddleware via the policies in
    ``policies.py`` / ``presets/__init__.py``.  No inline role checks here.
    Visibility-gating (404 for hidden catalogs/collections) is the second layer.
    """

    router: APIRouter = APIRouter(prefix="/task", tags=["Tasks API"])

    # ------------------------------------------------------------------
    # 1. GET /task — list system tasks
    # ------------------------------------------------------------------

    @router.get(
        "",
        response_model=TaskPage,
        summary="List system-scope tasks (catalog_id IN 'system','platform').",
    )
    async def list_tasks_system_view(
        conn: AsyncConnection = Depends(get_async_connection),  # type: ignore[reportGeneralTypeIssues]
        task_type: Optional[str] = Query(None),
        kind: Optional[str] = Query(None, alias="type"),
        asset_id: Optional[str] = Query(None),
        created_before: Optional[datetime] = Query(None),
        cursor: Optional[str] = Query(None),
        limit: int = Query(20, ge=1, le=100),
        task_status: Optional[str] = Query(None, alias="status"),
    ) -> TaskPage:
        """Sysadmin listing of platform-level tasks.  Requires tasks_system_read."""
        try:
            rows = await tasks_module.list_tasks_system(
                conn,
                limit=limit + 1,
                status=task_status,
                task_type=task_type,
                kind=kind,
                asset_id=asset_id,
                created_before=created_before,
                cursor=cursor,
            )
        except ValueError as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from exc
        return await _to_task_page(rows, limit)

    # ------------------------------------------------------------------
    # 2. GET /task/tasks/{task_id} — get any task unscoped
    # ------------------------------------------------------------------

    @router.get(
        "/tasks/{task_id}",
        response_model=Task,
        name="get_task_system",
        summary="Get any task by id (unscoped, sysadmin only).",
    )
    async def get_task_system(
        task_id: uuid.UUID,  # type: ignore[reportGeneralTypeIssues]
        conn: AsyncConnection = Depends(get_async_connection),
    ) -> Task:
        """Unscoped lookup by task_id UUID — returns the task regardless of tenant.
        Requires tasks_system_admin."""
        task = await tasks_module.get_task_by_id_unscoped(conn, task_id)
        if not task:
            raise HTTPException(
                status_code=404, detail=f"Task '{task_id}' not found."
            )
        try:
            # Unscoped route: no separately-resolved tenant schema in hand, so
            # reuse the already-fetched task's own catalog_id (real rows
            # always have one — the reserved 'platform'/'system' sentinels
            # included).
            task = await reconcile_task_liveness(conn, task, schema=task.catalog_id or "")
        except Exception as e:  # noqa: BLE001 — best-effort; never turn a 200 into a 500
            logger.warning(
                "reconcile_task_liveness failed for task %s: %s — serving unreconciled status.",
                task_id, e,
            )
        await _project_collection_external_ids([task])
        return task

    # ------------------------------------------------------------------
    # 3. GET /task/catalogs/{catalog_id} — list catalog tasks
    # ------------------------------------------------------------------

    @router.get(
        "/catalogs/{catalog_id}",
        response_model=TaskPage,
        summary="List tasks for a catalog.",
    )
    async def list_tasks_catalog(
        catalog_id: str,  # type: ignore[reportGeneralTypeIssues]
        conn: AsyncConnection = Depends(get_async_connection),
        task_type: Optional[str] = Query(None),
        kind: Optional[str] = Query(None, alias="type"),
        asset_id: Optional[str] = Query(None),
        created_before: Optional[datetime] = Query(None),
        cursor: Optional[str] = Query(None),
        limit: int = Query(20, ge=1, le=100),
        task_status: Optional[str] = Query(None, alias="status"),
    ) -> TaskPage:
        """Catalog-scoped task listing.  Requires tasks_read + catalog membership."""
        await _assert_catalog_visible(catalog_id)
        try:
            rows = await tasks_module.list_tasks_for_catalog(
                conn,
                catalog_id=catalog_id,
                limit=limit + 1,
                kind=kind,
                status=task_status,
                task_type=task_type,
                asset_id=asset_id,
                created_before=created_before,
                cursor=cursor,
            )
        except ValueError as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from exc
        return await _to_task_page(rows, limit)

    # ------------------------------------------------------------------
    # 4. GET /task/catalogs/{catalog_id}/tasks/{task_id}
    #    Function name MUST stay `get_task_status_catalog` — url_for invariant.
    # ------------------------------------------------------------------

    @router.get(
        "/catalogs/{catalog_id}/tasks/{task_id}",
        response_model=Task,
        name="get_task_status_catalog",
        summary="Get a task scoped to a catalog (uncached read).",
    )
    async def get_task_status_catalog(
        catalog_id: str,  # type: ignore[reportGeneralTypeIssues]
        task_id: uuid.UUID,
        conn: AsyncConnection = Depends(get_async_connection),
    ) -> Task:
        """Uncached read — see _get_task_scoped_uncached for rationale.

        Requires tasks_read + catalog membership, enforced at the IAM
        perimeter. The listing-visibility re-check (404 for hidden catalogs)
        happens inside the helper, conditional on the catalog still being
        resolvable — see _get_task_scoped_uncached for the post-hard-delete
        tradeoff (#2674)."""
        return await _get_task_scoped_uncached(task_id, catalog_id, conn)

    # ------------------------------------------------------------------
    # 5. GET /task/catalogs/{catalog_id}/collections/{collection_id}
    # ------------------------------------------------------------------

    @router.get(
        "/catalogs/{catalog_id}/collections/{collection_id}",
        response_model=TaskPage,
        summary="List tasks for a collection.",
    )
    async def list_tasks_collection(
        catalog_id: str,  # type: ignore[reportGeneralTypeIssues]
        collection_id: str,
        conn: AsyncConnection = Depends(get_async_connection),
        task_type: Optional[str] = Query(None),
        kind: Optional[str] = Query(None, alias="type"),
        asset_id: Optional[str] = Query(None),
        created_before: Optional[datetime] = Query(None),
        cursor: Optional[str] = Query(None),
        limit: int = Query(20, ge=1, le=100),
        task_status: Optional[str] = Query(None, alias="status"),
    ) -> TaskPage:
        """Collection-scoped task listing.  Requires tasks_read + catalog membership."""
        await _assert_catalog_visible(catalog_id)
        await _assert_collection_visible(catalog_id, collection_id)
        try:
            rows = await tasks_module.list_tasks_for_catalog(
                conn,
                catalog_id=catalog_id,
                limit=limit + 1,
                kind=kind,
                status=task_status,
                task_type=task_type,
                collection_id=collection_id,
                asset_id=asset_id,
                created_before=created_before,
                cursor=cursor,
            )
        except ValueError as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from exc
        return await _to_task_page(rows, limit)

    # ------------------------------------------------------------------
    # 6. GET /task/catalogs/{catalog_id}/collections/{collection_id}/tasks/{task_id}
    # ------------------------------------------------------------------

    @router.get(
        "/catalogs/{catalog_id}/collections/{collection_id}/tasks/{task_id}",
        response_model=Task,
        name="get_task_status_collection",
        summary="Get a task scoped to a collection (uncached read).",
    )
    async def get_task_status_collection(
        catalog_id: str,  # type: ignore[reportGeneralTypeIssues]
        collection_id: str,
        task_id: uuid.UUID,
        conn: AsyncConnection = Depends(get_async_connection),
    ) -> Task:
        """Uncached read — see _get_task_scoped_uncached for rationale.

        Requires tasks_read + catalog membership, enforced at the IAM
        perimeter. The collection cross-check and the listing-visibility
        re-checks (404 for hidden catalog/collection) happen inside the
        helper, the latter conditional on the catalog still being
        resolvable — see _get_task_scoped_uncached for the post-hard-delete
        tradeoff (#2685, following #2674/#2683)."""
        return await _get_task_scoped_uncached(
            task_id, catalog_id, conn, collection_id=collection_id
        )

    # ------------------------------------------------------------------
    # 7. POST /task — spawn system scope
    # ------------------------------------------------------------------

    @router.post(
        "",
        response_model=TaskRef,
        status_code=202,
        responses={200: {"description": IDEMPOTENT_SPAWN_DESC}},
        summary="Spawn a task at system scope.",
    )
    async def spawn_task_system(
        body: SpawnTaskRequest,  # type: ignore[reportGeneralTypeIssues]
        request: Request,
        response: Response,
    ) -> TaskRef:
        """Generic spawn at system scope (catalog_id='system').
        Destructive operations are permitted only here.
        Requires tasks_system_admin."""
        return await _do_spawn_system(body, request, response)

    # ------------------------------------------------------------------
    # 8. POST /task/catalogs/{catalog_id} — spawn catalog scope
    # ------------------------------------------------------------------

    @router.post(
        "/catalogs/{catalog_id}",
        response_model=TaskRef,
        status_code=202,
        responses={200: {"description": IDEMPOTENT_SPAWN_DESC}},
        summary="Spawn a task at catalog scope.",
    )
    async def spawn_task_catalog(
        catalog_id: str,  # type: ignore[reportGeneralTypeIssues]
        body: SpawnTaskRequest,
        request: Request,
        response: Response,
    ) -> TaskRef:
        """Generic spawn scoped to a catalog.
        Destructive operations are rejected here (system scope only).
        Requires tasks_admin + catalog-admin delegation."""
        return await _do_spawn_catalog(catalog_id, body, request, response)

    # ------------------------------------------------------------------
    # 9. POST /task/catalogs/{catalog_id}/collections/{collection_id}
    # ------------------------------------------------------------------

    @router.post(
        "/catalogs/{catalog_id}/collections/{collection_id}",
        response_model=TaskRef,
        status_code=202,
        responses={200: {"description": IDEMPOTENT_SPAWN_DESC}},
        summary="Spawn a task at collection scope.",
    )
    async def spawn_task_collection(
        catalog_id: str,  # type: ignore[reportGeneralTypeIssues]
        collection_id: str,
        body: SpawnTaskRequest,
        request: Request,
        response: Response,
    ) -> TaskRef:
        """Generic spawn scoped to a collection.
        Requires tasks_admin + catalog-admin delegation."""
        return await _do_spawn_collection(
            catalog_id, collection_id, body, request, response
        )

    # ------------------------------------------------------------------
    # 10. GET /task/dead-letter — DLQ all tenants
    # ------------------------------------------------------------------

    @router.get(
        "/dead-letter",
        response_model=TaskPage,
        summary="List dead-lettered tasks across all tenants (sysadmin).",
    )
    async def list_dead_letter_system(
        request: Request,  # type: ignore[reportGeneralTypeIssues]
        task_type: Optional[str] = Query(None),
        cursor: Optional[str] = Query(None),
        limit: int = Query(100, ge=1, le=500),
    ) -> TaskPage:
        """System-wide DLQ listing (all tenants).  Requires tasks_system_admin."""
        engine = get_async_engine(request)
        try:
            rows = await _dlq_list(
                engine, task_type=task_type, limit=limit + 1, cursor=cursor
            )
        except ValueError as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from exc
        return await _to_task_page(rows, limit)

    # ------------------------------------------------------------------
    # 11. GET /task/catalogs/{catalog_id}/dead-letter
    # ------------------------------------------------------------------

    @router.get(
        "/catalogs/{catalog_id}/dead-letter",
        response_model=TaskPage,
        summary="List dead-lettered tasks for a catalog (catalog-admin).",
    )
    async def list_dead_letter_catalog(
        catalog_id: str,  # type: ignore[reportGeneralTypeIssues]
        request: Request,
        task_type: Optional[str] = Query(None),
        cursor: Optional[str] = Query(None),
        limit: int = Query(100, ge=1, le=500),
    ) -> TaskPage:
        """Catalog-scoped DLQ listing.  Requires tasks_admin."""
        engine = get_async_engine(request)
        schema = await tasks_module._resolve_catalog_schema(catalog_id)
        try:
            rows = await _dlq_list(
                engine, catalog_id=schema, task_type=task_type, limit=limit + 1, cursor=cursor
            )
        except ValueError as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from exc
        return await _to_task_page(rows, limit)

    # ------------------------------------------------------------------
    # 12. GET /task/catalogs/{catalog_id}/collections/{collection_id}/dead-letter
    # ------------------------------------------------------------------

    @router.get(
        "/catalogs/{catalog_id}/collections/{collection_id}/dead-letter",
        response_model=TaskPage,
        summary="List dead-lettered tasks for a collection (catalog-admin).",
    )
    async def list_dead_letter_collection(
        catalog_id: str,  # type: ignore[reportGeneralTypeIssues]
        collection_id: str,
        request: Request,
        task_type: Optional[str] = Query(None),
        cursor: Optional[str] = Query(None),
        limit: int = Query(100, ge=1, le=500),
    ) -> TaskPage:
        """Collection-scoped DLQ listing.  Requires tasks_admin."""
        engine = get_async_engine(request)
        schema = await tasks_module._resolve_catalog_schema(catalog_id)
        try:
            rows = await _dlq_list(
                engine,
                catalog_id=schema,
                collection_id=collection_id,
                task_type=task_type,
                limit=limit + 1,
                cursor=cursor,
            )
        except ValueError as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from exc
        return await _to_task_page(rows, limit)

    # ------------------------------------------------------------------
    # 13. POST /task/dead-letter/{task_id}/requeue
    # ------------------------------------------------------------------

    @router.post(
        "/dead-letter/{task_id}/requeue",
        response_model=RequeueResult,
        summary="Requeue a dead-lettered task at system scope.",
    )
    async def requeue_dead_letter_system(
        task_id: uuid.UUID,  # type: ignore[reportGeneralTypeIssues]
        request: Request,
        reset_retries: bool = Query(True),
    ) -> RequeueResult:
        """System-scope requeue.  Requires tasks_system_admin."""
        engine = get_async_engine(request)
        ok = await _dlq_requeue(engine, str(task_id), reset_retries=reset_retries)
        return RequeueResult(
            task_id=task_id,
            requeued=ok,
            detail=None if ok else "Task not found or not in a requeueable state.",
        )

    # ------------------------------------------------------------------
    # 14. POST /task/catalogs/{catalog_id}/dead-letter/{task_id}/requeue
    # ------------------------------------------------------------------

    @router.post(
        "/catalogs/{catalog_id}/dead-letter/{task_id}/requeue",
        response_model=RequeueResult,
        summary="Requeue a dead-lettered task scoped to a catalog.",
    )
    async def requeue_dead_letter_catalog(
        catalog_id: str,  # type: ignore[reportGeneralTypeIssues]
        task_id: uuid.UUID,
        request: Request,
        reset_retries: bool = Query(True),
    ) -> RequeueResult:
        """Catalog-scoped requeue.  Requires tasks_admin."""
        engine = get_async_engine(request)
        schema = await tasks_module._resolve_catalog_schema(catalog_id)
        ok = await _dlq_requeue(
            engine,
            str(task_id),
            reset_retries=reset_retries,
            catalog_id=schema,
        )
        return RequeueResult(
            task_id=task_id,
            requeued=ok,
            detail=None if ok else "Task not found or not in a requeueable state.",
        )

    # ------------------------------------------------------------------
    # 15. POST /task/catalogs/{catalog_id}/collections/{collection_id}/dead-letter/{task_id}/requeue
    # ------------------------------------------------------------------

    @router.post(
        "/catalogs/{catalog_id}/collections/{collection_id}/dead-letter/{task_id}/requeue",
        response_model=RequeueResult,
        summary="Requeue a dead-lettered task scoped to a collection.",
    )
    async def requeue_dead_letter_collection(
        catalog_id: str,  # type: ignore[reportGeneralTypeIssues]
        collection_id: str,
        task_id: uuid.UUID,
        request: Request,
        reset_retries: bool = Query(True),
    ) -> RequeueResult:
        """Collection-scoped requeue.  Requires tasks_admin."""
        engine = get_async_engine(request)
        schema = await tasks_module._resolve_catalog_schema(catalog_id)
        ok = await _dlq_requeue(
            engine,
            str(task_id),
            reset_retries=reset_retries,
            catalog_id=schema,
            collection_id=collection_id,
        )
        return RequeueResult(
            task_id=task_id,
            requeued=ok,
            detail=None if ok else "Task not found or not in a requeueable state.",
        )
