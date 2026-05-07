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
#
#    Author: Carlo Cancellieri (ccancellieri@gmail.com)
#    Company: FAO, Viale delle Terme di Caracalla, 00100 Rome, Italy
#    Contact: copyright@fao.org - http://fao.org/contact-us/terms/en/

import logging
import json
import uuid
from typing import List, Union, Any, Optional, cast

import jsonschema as _jsonschema_scope_gate  # noqa: F401  # SCOPE gate: extension_processes requires jsonschema
_ = _jsonschema_scope_gate  # silence pyright "unused" — load-bearing for SCOPE filtering

from pydantic import ValidationError
from fastapi import (
    APIRouter,
    BackgroundTasks,
    Depends,
    HTTPException,
    Query,
    Request,
    Response,
    status,
    Body,
)
from sqlalchemy.ext.asyncio import AsyncConnection

from dynastore.extensions.protocols import ExtensionProtocol
from dynastore.extensions.tools.db import get_async_connection, get_async_engine
from dynastore.extensions.tools.exception_handlers import http_errors
from dynastore.models.protocols import CatalogsProtocol
from dynastore.tools.discovery import get_protocol

from dynastore.modules.processes.protocols import ProcessRegistryProtocol
from dynastore.tools.discovery import get_protocols

import dynastore.modules.processes.processes_module as processes_module
from dynastore.modules.tasks import tasks_module
from dynastore.modules.tasks.models import (
    Task,
    TaskStatusEnum,
)
from dynastore.modules.tasks.execution import execution_engine
from dynastore.modules.processes import models
from dynastore.modules.processes.inventory import (
    build_process_inventory_entries,
    parse_runner_filter,
    parse_scope_filter,
)
from dynastore.models.auth_models import SYSTEM_USER_ID
from dynastore.models.driver_context import DriverContext


logger = logging.getLogger(__name__)

# --- OGC Processes Conformance URIs ---
PROCESSES_CONFORMANCE = [
    "http://www.opengis.net/spec/ogcapi-processes-1/1.0/conf/core",
    "http://www.opengis.net/spec/ogcapi-processes-1/1.0/conf/ogc-process-description",
    "http://www.opengis.net/spec/ogcapi-processes-1/1.0/conf/json",
    "http://www.opengis.net/spec/ogcapi-processes-1/1.0/conf/job-list",
    "http://www.opengis.net/spec/ogcapi-processes-1/1.0/conf/dismiss",
]

router: APIRouter = APIRouter(prefix="/processes", tags=["OGC API - Processes"])


# Which process scopes are listable / executable at each URL mount point.
# This is the single source of truth for scope→URL alignment — used by both
# `_validate_process_scope_or_raise` (enforcement) and `list_processes`
# (filtering) so a process that can't be executed at a given mount is not
# advertised there either.
_PLATFORM_ALLOWED_SCOPES = frozenset({models.ProcessScope.PLATFORM})
_CATALOG_ALLOWED_SCOPES = frozenset({models.ProcessScope.CATALOG})
_COLLECTION_ALLOWED_SCOPES = frozenset({models.ProcessScope.COLLECTION})
# ASSET-scoped processes are NOT executable at `/processes` mounts — they
# route through the asset-service parametric process surface
# (`/assets/catalogs/.../assets/{asset_id}/{process_id}`, see proposed
# STAC API Asset Transactions extension in docs/proposals/) because that
# path resolves the Asset object and dispatches via `AssetProcessProtocol`.
# Exposing them at the collection mount would advertise a URL that can't
# resolve the asset context at runtime.


def _allowed_scopes_for(
    catalog_id: Optional[str], collection_id: Optional[str]
) -> frozenset:
    if catalog_id and collection_id:
        return _COLLECTION_ALLOWED_SCOPES
    if catalog_id:
        return _CATALOG_ALLOWED_SCOPES
    return _PLATFORM_ALLOWED_SCOPES


_SCOPE_URL_HINTS = {
    models.ProcessScope.PLATFORM:
        "POST /processes/{process_id}/execution (platform/sysadmin scope)",
    models.ProcessScope.CATALOG:
        "POST /catalogs/{catalog_id}/processes/{process_id}/execution",
    models.ProcessScope.COLLECTION:
        "POST /catalogs/{catalog_id}/collections/{collection_id}"
        "/processes/{process_id}/execution",
    models.ProcessScope.ASSET:
        "POST /catalogs/{catalog_id}/collections/{collection_id}"
        "/assets/{asset_id}/processes/{process_id}/execution",
}

# Templated paths advertised via HATEOAS `rel=execute` links on
# `GET /processes/{id}` — machine-readable counterparts of _SCOPE_URL_HINTS.
# RFC 6570 URI templates; the `templated: true` extra marks them as such.
_SCOPE_URL_TEMPLATES = {
    models.ProcessScope.PLATFORM:
        "/processes/{process_id}/execution",
    models.ProcessScope.CATALOG:
        "/catalogs/{catalog_id}/processes/{process_id}/execution",
    models.ProcessScope.COLLECTION:
        "/catalogs/{catalog_id}/collections/{collection_id}"
        "/processes/{process_id}/execution",
    models.ProcessScope.ASSET:
        "/catalogs/{catalog_id}/collections/{collection_id}"
        "/assets/{asset_id}/processes/{process_id}/execution",
}

# OGC link relation for "execute this process"
# (OGC API - Processes - Part 1, §7.11, rel type registry).
_OGC_REL_EXECUTE = "http://www.opengis.net/def/rel/ogc/1.0/execute"


def _build_execution_links(
    process: models.Process,
    request: Request,
    catalog_id: Optional[str] = None,
    collection_id: Optional[str] = None,
) -> List[models.Link]:
    """
    Return HATEOAS ``rel=execute`` links for a process.

    When called from a scoped listing (``catalog_id`` / ``collection_id``
    set), emit a single concrete execution URL for that mount — this keeps
    the OGC Core §7.11 invariant ("every process listed is executable at the
    same context") true per-mount.

    When called from the canonical description endpoint (no scope), emit
    one templated URL per declared scope so clients can discover the full
    set of mount points without mining the docs.
    """
    links: List[models.Link] = []

    def _link(href: str, *, title: str, templated: bool) -> models.Link:
        return models.Link.model_validate(
            {
                "href": href,
                "rel": _OGC_REL_EXECUTE,
                "type": "application/json",
                "title": title,
                "method": "POST",
                "templated": templated,
            }
        )

    if catalog_id and collection_id:
        href = str(
            request.url_for(
                "execute_process_collection",
                catalog_id=catalog_id,
                collection_id=collection_id,
                process_id=process.id,
            )
        )
        links.append(_link(href, title="Execute at this collection", templated=False))
        return links

    if catalog_id:
        href = str(
            request.url_for(
                "execute_process_catalog",
                catalog_id=catalog_id,
                process_id=process.id,
            )
        )
        links.append(_link(href, title="Execute at this catalog", templated=False))
        return links

    # Canonical description: advertise one templated URL per declared scope.
    base = str(request.base_url).rstrip("/")
    for scope in process.scopes:
        template = _SCOPE_URL_TEMPLATES[scope].replace(
            "{process_id}", process.id
        )
        links.append(
            _link(
                f"{base}{template}",
                title=f"Execute at {scope.value} scope",
                templated=True,
            )
        )
    return links


def _validate_process_scope_or_raise(
    process: models.Process,
    catalog_id: Optional[str],
    collection_id: Optional[str],
) -> None:
    """
    Reject execution requests that route a process through a URL whose scope
    isn't in the process definition's declared ``scopes``.

    A process may declare multiple scopes (e.g. a backup that runs at catalog
    or collection level): the request is accepted if ANY declared scope is
    legal at the resolved URL mount. Fails fast with 400 *before* any task
    row is written or event emitted.
    """
    allowed = _allowed_scopes_for(catalog_id, collection_id)
    if any(s in allowed for s in process.scopes):
        return

    hints = [_SCOPE_URL_HINTS[s] for s in process.scopes]
    declared = ", ".join(s.value for s in process.scopes)
    raise HTTPException(
        status_code=status.HTTP_400_BAD_REQUEST,
        detail=(
            f"Process '{process.id}' declares scopes [{declared}] and cannot "
            f"be executed at this URL. Valid routes: {'; '.join(hints)}."
        ),
    )


def _inject_path_into_inputs(
    execution_request: models.ExecuteRequest,
    catalog_id: Optional[str],
    collection_id: Optional[str],
) -> models.ExecuteRequest:
    """
    Copy URL path identifiers into ``execution_request.inputs`` so task
    implementations can read them uniformly (without each task having to
    know how it was invoked).

    If the client included a conflicting value in the body, reject with 400 —
    the URL path is the only source of truth for target identifiers.
    """
    inputs = dict(execution_request.inputs or {})
    for key, value in (("catalog_id", catalog_id), ("collection_id", collection_id)):
        if value is None:
            continue
        existing = inputs.get(key)
        if existing is not None and existing != value:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=(
                    f"Conflicting '{key}' in inputs ({existing!r}) vs URL "
                    f"path ({value!r}). Remove '{key}' from the request body — "
                    f"the URL path is authoritative."
                ),
            )
        inputs[key] = value
    return execution_request.model_copy(update={"inputs": inputs})


async def _lookup_process_or_404(process_id: str) -> models.Process:
    process: Optional[models.Process] = None
    for registry in get_protocols(ProcessRegistryProtocol):
        process = await registry.get_process(process_id)
        if process:
            break
    if not process:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Process '{process_id}' not found.",
        )
    return process


async def _render_process_list(
    request: Request,
    catalog_id: Optional[str] = None,
    collection_id: Optional[str] = None,
    scope_param: Optional[str] = None,
    runner_param: Optional[str] = None,
    typology: bool = True,
) -> models.ProcessList:
    """Render the OGC process list with optional typology enrichment.

    Strict-OGC behaviour (``typology=False``, no ``scope_param``) matches
    the pre-enrichment payload byte-for-byte: platform-only at root,
    catalog/collection narrowed at scoped mounts.

    When ``scope_param`` is given, it overrides the URL-natural scope —
    e.g. ``/processes?scope=all`` lists every process registered in this
    deployment regardless of scope, with parametric URL templates for
    unresolved IDs. When ``typology=True`` (default), each entry carries
    ``typologies[]`` priority-descending so callers can see which runner
    will execute the process.
    """
    try:
        scope_filter = parse_scope_filter(scope_param)
    except ValueError as e:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))
    runner_filter = parse_runner_filter(runner_param)

    if scope_filter is None and scope_param is None:
        # No explicit scope filter — preserve the pre-existing URL-natural narrowing
        # so strict OGC clients that don't know about `scope` see exactly today's list.
        scope_filter = set(_allowed_scopes_for(catalog_id, collection_id))

    entries = await build_process_inventory_entries(
        request,
        catalog_id=catalog_id,
        collection_id=collection_id,
        scope_filter=scope_filter,
        runner_filter=runner_filter,
        include_typology=typology,
    )

    process_summaries: List[models.ProcessSummary] = []
    for entry in entries:
        process = await _lookup_process_or_404_silent(entry.id)
        self_link_href = (
            str(request.url_for("get_process_description", process_id=entry.id))
            if process is not None
            else ""
        )
        self_link = models.Link(
            href=self_link_href,
            rel="self",
            type="application/json",
            title="Detailed process description",  # type: ignore[arg-type]
            hreflang=None,
        )
        execute_links = (
            _build_execution_links(
                process, request, catalog_id=catalog_id, collection_id=collection_id
            )
            if process is not None and models.ProcessScope.ASSET not in entry.scopes
            else []
        )
        summary_dict = entry.model_dump(by_alias=True)
        summary_dict["links"] = [self_link, *execute_links]
        process_summaries.append(models.ProcessSummary.model_validate(summary_dict))

    links = [
        models.Link(
            href=str(request.url), rel="self", type="application/json", hreflang=None
        )
    ]
    return models.ProcessList(processes=process_summaries, links=links)


async def _lookup_process_or_404_silent(process_id: str) -> Optional[models.Process]:
    """Like ``_lookup_process_or_404`` but returns ``None`` for synthesised
    entries (asset processes) that don't live in the OGC registry."""
    for registry in get_protocols(ProcessRegistryProtocol):
        process = await registry.get_process(process_id)
        if process:
            return process
    return None


@router.get(
    "/processes",
    response_model=models.ProcessList,
    name="list_processes",
)
async def list_processes(
    request: Request,
    scope: Optional[str] = Query(
        default="all",
        description=(
            "Comma-separated scopes to include: `platform`, `catalog`, "
            "`collection`, `asset`, or `all` (default). Non-OGC filter."
        ),
    ),
    typology: bool = Query(
        default=True,
        description=(
            "Include `typologies[]` (priority-desc runner list) and "
            "`url_templates[]` on each entry. Set `false` for strict-OGC payload."
        ),
    ),
    runner: Optional[str] = Query(
        default=None,
        description=(
            "Comma-separated runner_type filter (e.g. `gcp_cloud_run,asset_process`)."
        ),
    ),
):
    """Lists processes available in this deployment.

    By default (``scope=all``, ``typology=true``) returns every registered
    process across all scopes with typology + parametric URL templates. Set
    ``typology=false`` to get a strict OGC Core payload.
    """
    return await _render_process_list(
        request,
        scope_param=scope,
        runner_param=runner,
        typology=typology,
    )


@router.get(
    "/catalogs/{catalog_id}/processes",
    response_model=models.ProcessList,
    name="list_processes_catalog",
)
async def list_processes_catalog(
    catalog_id: str,
    request: Request,
    scope: Optional[str] = Query(default=None),
    typology: bool = Query(default=True),
    runner: Optional[str] = Query(default=None),
):
    """Lists catalog-scoped processes available for this catalog."""
    return await _render_process_list(
        request,
        catalog_id=catalog_id,
        scope_param=scope,
        runner_param=runner,
        typology=typology,
    )


@router.get(
    "/catalogs/{catalog_id}/collections/{collection_id}/processes",
    response_model=models.ProcessList,
    name="list_processes_collection",
)
async def list_processes_collection(
    catalog_id: str,
    collection_id: str,
    request: Request,
    scope: Optional[str] = Query(default=None),
    typology: bool = Query(default=True),
    runner: Optional[str] = Query(default=None),
):
    """Lists collection- and asset-scoped processes available for this collection."""
    return await _render_process_list(
        request,
        catalog_id=catalog_id,
        collection_id=collection_id,
        scope_param=scope,
        runner_param=runner,
        typology=typology,
    )


@router.get(
    "/processes/{process_id}",
    response_model=models.Process,
    name="get_process_description",
)
async def get_process_description(process_id: str, request: Request):
    """
    Describes a process with HATEOAS ``rel=execute`` links.

    The description is served from the canonical (unscoped) URL but each
    declared scope contributes one templated execution URL so OGC clients
    can discover where the process may actually be invoked — this restores
    the §7.11 "every listed process is executable" invariant that scoped
    URL mounts would otherwise obscure.
    """
    process = await _lookup_process_or_404(process_id)
    self_link = models.Link(
        href=str(request.url),
        rel="self",
        type="application/json",
        title="Self",  # type: ignore[arg-type]
        hreflang=None,
    )
    execute_links = _build_execution_links(process, request)
    process_dict = process.model_dump(by_alias=True)
    process_dict["links"] = [self_link, *execute_links]
    return models.Process.model_validate(process_dict)


@router.post(
    "/processes/{process_id}/execution",
    status_code=status.HTTP_201_CREATED,
    response_model=Union[
        models.StatusInfo, Any
    ],  # The response can be a status or a direct result
    name="execute_process",
)
async def execute_process(
    process_id: str,
    request: Request,
    background_tasks: BackgroundTasks,
    execution_request: models.ExecuteRequest = Body(
        ...,
        examples=[
            {
                "inputs": {
                    "dry_run": False,
                },
                "response": "document",
            },
        ],
        description=(
            "Execution inputs. Only PLATFORM-scoped processes may be executed "
            "at this URL. Tenant-scoped processes must be posted to "
            "/catalogs/{catalog_id}[/collections/{collection_id}]/processes/"
            "{process_id}/execution."
        ),
    ),
):
    """Executes a platform-scoped process, creating a new job (task)."""
    # Validate routing BEFORE touching the DB engine so bad-URL requests
    # never acquire DB resources or emit events.
    process = await _lookup_process_or_404(process_id)
    _validate_process_scope_or_raise(process, catalog_id=None, collection_id=None)

    principal = getattr(request.state, "principal", None)
    caller_id = str(principal.id) if principal else SYSTEM_USER_ID
    engine = get_async_engine(request)

    preferred_mode = _get_preferred_mode(request)
    result = None

    try:
        result = await processes_module.execute_process(
            process_id=process_id,
            execution_request=execution_request,
            engine=engine,
            caller_id=caller_id,
            preferred_mode=preferred_mode,
            background_tasks=background_tasks,
        )
    except Exception as e:
        _handle_execution_exception(process_id, e)

    return _handle_execution_result(result, request)


@router.post(
    "/catalogs/{catalog_id}/processes/{process_id}/execution",
    status_code=status.HTTP_201_CREATED,
    response_model=Union[models.StatusInfo, Any],
    name="execute_process_catalog",
)
async def execute_process_catalog(
    catalog_id: str,
    process_id: str,
    execution_request: models.ExecuteRequest,
    request: Request,
    background_tasks: BackgroundTasks,
):
    """Executes a catalog-scoped process."""
    process = await _lookup_process_or_404(process_id)
    _validate_process_scope_or_raise(process, catalog_id=catalog_id, collection_id=None)
    execution_request = _inject_path_into_inputs(
        execution_request, catalog_id=catalog_id, collection_id=None
    )

    principal = getattr(request.state, "principal", None)
    caller_id = str(principal.id) if principal else SYSTEM_USER_ID
    engine = get_async_engine(request)
    preferred_mode = _get_preferred_mode(request)
    result = None

    try:
        result = await processes_module.execute_process(
            process_id=process_id,
            execution_request=execution_request,
            engine=engine,
            caller_id=caller_id,
            preferred_mode=preferred_mode,
            background_tasks=background_tasks,
            catalog_id=catalog_id,
        )
    except Exception as e:
        _handle_execution_exception(process_id, e)

    return _handle_execution_result(result, request)


@router.post(
    "/catalogs/{catalog_id}/collections/{collection_id}/processes/{process_id}/execution",
    status_code=status.HTTP_201_CREATED,
    response_model=Union[models.StatusInfo, Any],
    name="execute_process_collection",
)
async def execute_process_collection(
    catalog_id: str,
    collection_id: str,
    process_id: str,
    execution_request: models.ExecuteRequest,
    request: Request,
    background_tasks: BackgroundTasks,
):
    """Executes a collection- or asset-scoped process."""
    process = await _lookup_process_or_404(process_id)
    _validate_process_scope_or_raise(
        process, catalog_id=catalog_id, collection_id=collection_id
    )
    execution_request = _inject_path_into_inputs(
        execution_request, catalog_id=catalog_id, collection_id=collection_id
    )

    principal = getattr(request.state, "principal", None)
    caller_id = str(principal.id) if principal else SYSTEM_USER_ID
    engine = get_async_engine(request)
    preferred_mode = _get_preferred_mode(request)
    result = None
    try:
        result = await processes_module.execute_process(
            process_id=process_id,
            execution_request=execution_request,
            engine=engine,
            caller_id=caller_id,
            preferred_mode=preferred_mode,
            background_tasks=background_tasks,
            catalog_id=catalog_id,
            collection_id=collection_id,
        )
    except Exception as e:
        _handle_execution_exception(process_id, e)

    return _handle_execution_result(result, request)


def _task_to_status_info(task: Task, request: Request) -> models.StatusInfo:
    """Helper to convert a task to OGC StatusInfo with appropriate links."""
    links = _get_job_links(task, request)
    return models.task_to_status_info(task, links=links)


def _parse_prefer_header(prefer_header: Optional[str]):
    """Extract OGC dispatch preference from an HTTP Prefer header.

    Honors RFC 7240 tokens per OGC API - Processes Part 1 §7.1:
    ``respond-async`` and ``respond-sync``. Also accepts the legacy
    ``wait=`` token (which sometimes appears from HTTP clients that
    treat ``respond-sync`` as an ``wait=0``-like hint) as a sync
    indicator so existing callers keep working.
    """
    if not prefer_header:
        return None
    hdr = prefer_header.lower()
    if "respond-async" in hdr:
        return models.JobControlOptions.ASYNC_EXECUTE
    if "respond-sync" in hdr or "wait=" in hdr:
        return models.JobControlOptions.SYNC_EXECUTE
    return None


def _get_preferred_mode(request: Request):
    return _parse_prefer_header(request.headers.get("Prefer"))


def _handle_execution_exception(process_id: str, e: Exception):
    if isinstance(e, (ValidationError, ValueError)):
        logger.error(f"Validation error for process '{process_id}': {e}")
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail=str(e)
        )
    if isinstance(e, NotImplementedError):
        raise HTTPException(status_code=status.HTTP_501_NOT_IMPLEMENTED, detail=str(e))
    logger.error(f"Execution of process '{process_id}' failed: {e}", exc_info=True)
    raise HTTPException(
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        detail=f"Process execution failed: {e}",
    )


def _handle_execution_result(
    result: Union[Task, models.StatusInfo, Any], request: Request
):
    if isinstance(result, Task):
        # ASYNC_EXECUTE: The runner returned a new task object.
        # Respond with 201 Created and a Location header.

        # Determine specialized URL if context is available
        path_params = request.scope.get("path_params", {})
        catalog_id = path_params.get("catalog_id")
        collection_id = path_params.get("collection_id")

        if catalog_id and collection_id:
            job_status_url = request.url_for(
                "get_job_status_collection",
                catalog_id=catalog_id,
                collection_id=collection_id,
                job_id=str(result.task_id),
            )
        elif catalog_id:
            job_status_url = request.url_for(
                "get_job_status_catalog",
                catalog_id=catalog_id,
                job_id=str(result.task_id),
            )
        else:
            job_status_url = request.url_for(
                "get_job_status", job_id=str(result.task_id)
            )

        links = _get_job_links(result, request)
        status_info = models.task_to_status_info(result, links=links)

        return Response(
            content=status_info.model_dump_json(by_alias=True),
            status_code=status.HTTP_201_CREATED,
            headers={"Location": str(job_status_url)},
            media_type="application/json",
        )
    else:
        # SYNC_EXECUTE: The runner returned the final result directly.
        # Respond with 200 OK and the result as the body.
        if result is None:
            return Response(content="", status_code=status.HTTP_200_OK)
        elif isinstance(result, dict) or isinstance(result, list):
            content = json.dumps(result)
        elif hasattr(result, "model_dump_json"):
            content = result.model_dump_json(by_alias=True)
        else:
            content = str(result)

        accept_header = request.headers.get("Accept", "application/json")
        if (
            "application/json" in accept_header
            or "*/*" in accept_header
            or "application/*" in accept_header
        ):
            return Response(
                content=content,
                status_code=status.HTTP_200_OK,
                media_type="application/json",
            )
        elif "text/plain" in accept_header:
            return Response(
                content=content, status_code=status.HTTP_200_OK, media_type="text/plain"
            )
        else:
            # If the requested media type is not supported, return 406.
            raise HTTPException(
                status_code=status.HTTP_406_NOT_ACCEPTABLE,
                detail=f"Requested media type '{accept_header}' not supported for synchronous process results.",
            )


async def _resolve_catalog_schema(catalog_id: str, conn: AsyncConnection) -> str:
    """Resolve the physical PG schema for a catalog."""
    catalogs = get_protocol(CatalogsProtocol)
    if not catalogs:
        raise HTTPException(status_code=500, detail="CatalogsProtocol not available.")
    catalogs = cast(CatalogsProtocol, catalogs)
    schema = await catalogs.resolve_physical_schema(catalog_id, ctx=DriverContext(db_resource=conn))
    if not schema:
        raise HTTPException(status_code=404, detail=f"Catalog '{catalog_id}' not found.")
    return schema


async def _get_job_internal(job_id: uuid.UUID, catalog_id: str, conn: AsyncConnection):
    schema = await _resolve_catalog_schema(catalog_id, conn)
    task = await tasks_module.get_task(conn, job_id, schema=schema)
    if not task:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Job '{job_id}' not found in schema '{schema}'.",
        )
    return task


@router.get(
    "/jobs/{job_id}",
    response_model=models.StatusInfo,
    name="get_job_status",
)
async def get_job_status(
    job_id: uuid.UUID,
    request: Request,
    conn: AsyncConnection = Depends(get_async_connection),
):
    """Gets the status of a specific job (unscoped lookup).

    Searches by ``task_id`` alone — task IDs are UUIDv7, globally unique, so
    a single id matches one row regardless of tenant ``schema_name``. This
    lets clients poll catalog- and collection-scoped jobs through the same
    URL the OGC POST response advertises, instead of having to pre-construct
    the scoped path. Uses the uncached helper so cross-process status writes
    (e.g. a Cloud Run Job container's ``update_task``) are reflected on the
    next poll without waiting for an in-process cache TTL.
    """
    task = await tasks_module.get_task_by_id_unscoped(conn, job_id)
    if not task:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Job '{job_id}' not found.",
        )
    return _task_to_status_info(task, request)


@router.get(
    "/jobs/{job_id}/results",
    name="get_job_results",
)
async def get_job_results(
    job_id: uuid.UUID,
    conn: AsyncConnection = Depends(get_async_connection),
):
    """Gets the results of a completed job (unscoped lookup).

    See :func:`get_job_status` for why this is unscoped.
    """
    task = await tasks_module.get_task_by_id_unscoped(conn, job_id)
    if not task:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Job '{job_id}' not found.",
        )
    return _handle_job_results(task, job_id)


@router.get(
    "/catalogs/{catalog_id}/collections/{collection_id}/jobs/{job_id}",
    response_model=models.StatusInfo,
    name="get_job_status_collection",
)
async def get_job_status_collection(
    catalog_id: str,
    collection_id: str,
    job_id: uuid.UUID,
    request: Request,
    conn: AsyncConnection = Depends(get_async_connection),
):
    """Gets the status of a specific job (Collection context)."""
    task = await _get_job_internal(job_id, catalog_id, conn)
    # Optionally verify collection_id matches if task stores it
    if task.collection_id and task.collection_id != collection_id:
        # We found the task in the catalog, but it belongs to a different collection
        # This is a soft 404 or 403 depending on strictness.
        # Given the URL implies a collection context, matching is better.
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Job '{job_id}' does not belong to collection '{collection_id}'.",
        )
    return _task_to_status_info(task, request)


@router.get(
    "/catalogs/{catalog_id}/collections/{collection_id}/jobs/{job_id}/results",
    name="get_job_results_collection",
)
async def get_job_results_collection(
    catalog_id: str,
    collection_id: str,
    job_id: uuid.UUID,
    conn: AsyncConnection = Depends(get_async_connection),
):
    """Gets the results of a completed job (Collection context)."""
    task = await _get_job_internal(job_id, catalog_id, conn)
    # Optionally verify collection_id matches if task stores it
    if task.collection_id and task.collection_id != collection_id:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Job '{job_id}' does not belong to collection '{collection_id}'.",
        )
    return _handle_job_results(task, job_id)


@router.get(
    "/catalogs/{catalog_id}/jobs/{job_id}",
    response_model=models.StatusInfo,
    name="get_job_status_catalog",
)
async def get_job_status_catalog(
    catalog_id: str,
    job_id: uuid.UUID,
    request: Request,
    conn: AsyncConnection = Depends(get_async_connection),
):
    """Gets the status of a specific job (Catalog context)."""
    task = await _get_job_internal(job_id, catalog_id, conn)
    return _task_to_status_info(task, request)


@router.get(
    "/catalogs/{catalog_id}/jobs/{job_id}/results", name="get_job_results_catalog"
)
async def get_job_results_catalog(
    catalog_id: str,
    job_id: uuid.UUID,
    conn: AsyncConnection = Depends(get_async_connection),
):
    """Gets the results of a completed job (Catalog context)."""
    task = await _get_job_internal(job_id, catalog_id, conn)
    return _handle_job_results(task, job_id)


# --- OGC Part 1: List Jobs (GET /jobs) at 3 scopes ---

@router.get(
    "/jobs",
    response_model=List[models.StatusInfo],
    name="list_jobs",
)
async def list_jobs(
    request: Request,
    limit: int = Query(20, ge=1, le=1000),
    offset: int = Query(0, ge=0),
    conn: AsyncConnection = Depends(get_async_connection),
):
    """Lists jobs (System context)."""
    tasks = await tasks_module.list_tasks(conn, schema="public", limit=limit, offset=offset)
    return [_task_to_status_info(t, request) for t in tasks]


@router.get(
    "/catalogs/{catalog_id}/jobs",
    response_model=List[models.StatusInfo],
    name="list_jobs_catalog",
)
async def list_jobs_catalog(
    catalog_id: str,
    request: Request,
    limit: int = Query(20, ge=1, le=1000),
    offset: int = Query(0, ge=0),
    conn: AsyncConnection = Depends(get_async_connection),
):
    """Lists jobs (Catalog context)."""
    schema = await _resolve_catalog_schema(catalog_id, conn)
    tasks = await tasks_module.list_tasks(conn, schema=schema, limit=limit, offset=offset)
    return [_task_to_status_info(t, request) for t in tasks]


@router.get(
    "/catalogs/{catalog_id}/collections/{collection_id}/jobs",
    response_model=List[models.StatusInfo],
    name="list_jobs_collection",
)
async def list_jobs_collection(
    catalog_id: str,
    collection_id: str,
    request: Request,
    limit: int = Query(20, ge=1, le=1000),
    offset: int = Query(0, ge=0),
    conn: AsyncConnection = Depends(get_async_connection),
):
    """Lists jobs (Collection context). Filters by collection_id."""
    schema = await _resolve_catalog_schema(catalog_id, conn)
    all_tasks = await tasks_module.list_tasks(conn, schema=schema, limit=limit, offset=offset)
    filtered = [t for t in all_tasks if getattr(t, "collection_id", None) == collection_id]
    return [_task_to_status_info(t, request) for t in filtered]


# --- OGC Part 1: Dismiss Job (DELETE /jobs/{id}) at 3 scopes ---

@router.delete(
    "/jobs/{job_id}",
    response_model=models.StatusInfo,
    name="dismiss_job",
)
async def dismiss_job(
    job_id: uuid.UUID,
    request: Request,
    conn: AsyncConnection = Depends(get_async_connection),
):
    """Dismiss a job (System context)."""
    engine = get_async_engine(request)
    try:
        task = await execution_engine.dismiss_job(job_id, engine=engine, db_schema="public")
    except ValueError:
        raise HTTPException(status_code=404, detail=f"Job '{job_id}' not found.")
    return _task_to_status_info(task, request)


@router.delete(
    "/catalogs/{catalog_id}/jobs/{job_id}",
    response_model=models.StatusInfo,
    name="dismiss_job_catalog",
)
async def dismiss_job_catalog(
    catalog_id: str,
    job_id: uuid.UUID,
    request: Request,
    conn: AsyncConnection = Depends(get_async_connection),
):
    """Dismiss a job (Catalog context)."""
    engine = get_async_engine(request)
    schema = await _resolve_catalog_schema(catalog_id, conn)
    try:
        task = await execution_engine.dismiss_job(job_id, engine=engine, db_schema=schema)
    except ValueError:
        raise HTTPException(status_code=404, detail=f"Job '{job_id}' not found.")
    return _task_to_status_info(task, request)


@router.delete(
    "/catalogs/{catalog_id}/collections/{collection_id}/jobs/{job_id}",
    response_model=models.StatusInfo,
    name="dismiss_job_collection",
)
async def dismiss_job_collection(
    catalog_id: str,
    collection_id: str,
    job_id: uuid.UUID,
    request: Request,
    conn: AsyncConnection = Depends(get_async_connection),
):
    """Dismiss a job (Collection context)."""
    engine = get_async_engine(request)
    schema = await _resolve_catalog_schema(catalog_id, conn)
    task = await _get_job_internal(job_id, catalog_id, conn)
    if task.collection_id and task.collection_id != collection_id:
        raise HTTPException(status_code=404, detail=f"Job '{job_id}' does not belong to collection '{collection_id}'.")
    try:
        task = await execution_engine.dismiss_job(job_id, engine=engine, db_schema=schema)
    except ValueError:
        raise HTTPException(status_code=404, detail=f"Job '{job_id}' not found.")
    return _task_to_status_info(task, request)


# --- OGC Part 4: Deferred Execution (POST /jobs, PATCH, POST /results) at 3 scopes ---

class _CreateJobRequest(models.BaseModel):
    """Request body for creating a deferred job."""
    process_id: str
    inputs: Optional[dict] = None


@router.post(
    "/jobs",
    status_code=status.HTTP_201_CREATED,
    response_model=models.StatusInfo,
    name="create_job",
)
async def create_job(
    body: _CreateJobRequest,
    request: Request,
):
    """Create a deferred job (System context). Status = CREATED."""
    principal = getattr(request.state, "principal", None)
    caller_id = str(principal.id) if principal else SYSTEM_USER_ID
    engine = get_async_engine(request)
    job = await execution_engine.create_job(
        task_type=body.process_id,
        inputs=body.inputs,
        engine=engine,
        caller_id=caller_id,
        db_schema="public",
    )
    status_info = _task_to_status_info(job, request)
    job_url = str(request.url_for("get_job_status", job_id=str(job.task_id)))
    return Response(
        content=status_info.model_dump_json(by_alias=True),
        status_code=status.HTTP_201_CREATED,
        headers={"Location": job_url},
        media_type="application/json",
    )


@router.post(
    "/catalogs/{catalog_id}/jobs",
    status_code=status.HTTP_201_CREATED,
    response_model=models.StatusInfo,
    name="create_job_catalog",
)
async def create_job_catalog(
    catalog_id: str,
    body: _CreateJobRequest,
    request: Request,
    conn: AsyncConnection = Depends(get_async_connection),
):
    """Create a deferred job (Catalog context). Status = CREATED."""
    principal = getattr(request.state, "principal", None)
    caller_id = str(principal.id) if principal else SYSTEM_USER_ID
    engine = get_async_engine(request)
    schema = await _resolve_catalog_schema(catalog_id, conn)
    job = await execution_engine.create_job(
        task_type=body.process_id,
        inputs=body.inputs,
        engine=engine,
        caller_id=caller_id,
        db_schema=schema,
    )
    status_info = _task_to_status_info(job, request)
    job_url = str(request.url_for("get_job_status_catalog", catalog_id=catalog_id, job_id=str(job.task_id)))
    return Response(
        content=status_info.model_dump_json(by_alias=True),
        status_code=status.HTTP_201_CREATED,
        headers={"Location": job_url},
        media_type="application/json",
    )


@router.post(
    "/catalogs/{catalog_id}/collections/{collection_id}/jobs",
    status_code=status.HTTP_201_CREATED,
    response_model=models.StatusInfo,
    name="create_job_collection",
)
async def create_job_collection(
    catalog_id: str,
    collection_id: str,
    body: _CreateJobRequest,
    request: Request,
    conn: AsyncConnection = Depends(get_async_connection),
):
    """Create a deferred job (Collection context). Status = CREATED."""
    principal = getattr(request.state, "principal", None)
    caller_id = str(principal.id) if principal else SYSTEM_USER_ID
    engine = get_async_engine(request)
    schema = await _resolve_catalog_schema(catalog_id, conn)
    job = await execution_engine.create_job(
        task_type=body.process_id,
        inputs=body.inputs,
        engine=engine,
        caller_id=caller_id,
        db_schema=schema,
        collection_id=collection_id,
    )
    status_info = _task_to_status_info(job, request)
    job_url = str(request.url_for(
        "get_job_status_collection",
        catalog_id=catalog_id,
        collection_id=collection_id,
        job_id=str(job.task_id),
    ))
    return Response(
        content=status_info.model_dump_json(by_alias=True),
        status_code=status.HTTP_201_CREATED,
        headers={"Location": job_url},
        media_type="application/json",
    )


# --- OGC Part 4: Update Job (PATCH /jobs/{id}) at 3 scopes ---

class _UpdateJobRequest(models.BaseModel):
    """Request body for updating a deferred job's inputs."""
    inputs: dict


@router.patch(
    "/jobs/{job_id}",
    response_model=models.StatusInfo,
    name="update_job",
)
async def update_job(
    job_id: uuid.UUID,
    body: _UpdateJobRequest,
    request: Request,
):
    """Update a deferred job's inputs (System context). Only while CREATED."""
    engine = get_async_engine(request)
    try:
        job = await execution_engine.update_job(job_id, body.inputs, engine=engine, db_schema="public")
    except ValueError:
        raise HTTPException(status_code=404, detail=f"Job '{job_id}' not found.")
    return _task_to_status_info(job, request)


@router.patch(
    "/catalogs/{catalog_id}/jobs/{job_id}",
    response_model=models.StatusInfo,
    name="update_job_catalog",
)
async def update_job_catalog(
    catalog_id: str,
    job_id: uuid.UUID,
    body: _UpdateJobRequest,
    request: Request,
    conn: AsyncConnection = Depends(get_async_connection),
):
    """Update a deferred job's inputs (Catalog context). Only while CREATED."""
    engine = get_async_engine(request)
    schema = await _resolve_catalog_schema(catalog_id, conn)
    try:
        job = await execution_engine.update_job(job_id, body.inputs, engine=engine, db_schema=schema)
    except ValueError:
        raise HTTPException(status_code=404, detail=f"Job '{job_id}' not found.")
    return _task_to_status_info(job, request)


@router.patch(
    "/catalogs/{catalog_id}/collections/{collection_id}/jobs/{job_id}",
    response_model=models.StatusInfo,
    name="update_job_collection",
)
async def update_job_collection(
    catalog_id: str,
    collection_id: str,
    job_id: uuid.UUID,
    body: _UpdateJobRequest,
    request: Request,
    conn: AsyncConnection = Depends(get_async_connection),
):
    """Update a deferred job's inputs (Collection context). Only while CREATED."""
    engine = get_async_engine(request)
    schema = await _resolve_catalog_schema(catalog_id, conn)
    try:
        job = await execution_engine.update_job(job_id, body.inputs, engine=engine, db_schema=schema)
    except ValueError:
        raise HTTPException(status_code=404, detail=f"Job '{job_id}' not found.")
    return _task_to_status_info(job, request)


# --- OGC Part 4: Start Job (POST /jobs/{id}/results) at 3 scopes ---

@router.post(
    "/jobs/{job_id}/results",
    status_code=status.HTTP_202_ACCEPTED,
    response_model=Union[models.StatusInfo, Any],
    name="start_job",
)
async def start_job(
    job_id: uuid.UUID,
    request: Request,
    background_tasks: BackgroundTasks,
):
    """Trigger execution of a CREATED job (System context)."""
    engine = get_async_engine(request)
    preferred_mode = _get_preferred_mode(request)
    from dynastore.modules.tasks.models import TaskExecutionMode
    mode = TaskExecutionMode.SYNCHRONOUS if preferred_mode == models.JobControlOptions.SYNC_EXECUTE else TaskExecutionMode.ASYNCHRONOUS
    try:
        result = await execution_engine.start_job(
            job_id, engine=engine, mode=mode, db_schema="public",
            background_tasks=background_tasks,
        )
    except ValueError:
        raise HTTPException(status_code=404, detail=f"Job '{job_id}' not found.")
    return _handle_execution_result(result, request)


@router.post(
    "/catalogs/{catalog_id}/jobs/{job_id}/results",
    status_code=status.HTTP_202_ACCEPTED,
    response_model=Union[models.StatusInfo, Any],
    name="start_job_catalog",
)
async def start_job_catalog(
    catalog_id: str,
    job_id: uuid.UUID,
    request: Request,
    background_tasks: BackgroundTasks,
    conn: AsyncConnection = Depends(get_async_connection),
):
    """Trigger execution of a CREATED job (Catalog context)."""
    engine = get_async_engine(request)
    schema = await _resolve_catalog_schema(catalog_id, conn)
    preferred_mode = _get_preferred_mode(request)
    from dynastore.modules.tasks.models import TaskExecutionMode
    mode = TaskExecutionMode.SYNCHRONOUS if preferred_mode == models.JobControlOptions.SYNC_EXECUTE else TaskExecutionMode.ASYNCHRONOUS
    try:
        result = await execution_engine.start_job(
            job_id, engine=engine, mode=mode, db_schema=schema,
            background_tasks=background_tasks,
        )
    except ValueError:
        raise HTTPException(status_code=404, detail=f"Job '{job_id}' not found.")
    return _handle_execution_result(result, request)


@router.post(
    "/catalogs/{catalog_id}/collections/{collection_id}/jobs/{job_id}/results",
    status_code=status.HTTP_202_ACCEPTED,
    response_model=Union[models.StatusInfo, Any],
    name="start_job_collection",
)
async def start_job_collection(
    catalog_id: str,
    collection_id: str,
    job_id: uuid.UUID,
    request: Request,
    background_tasks: BackgroundTasks,
    conn: AsyncConnection = Depends(get_async_connection),
):
    """Trigger execution of a CREATED job (Collection context)."""
    engine = get_async_engine(request)
    schema = await _resolve_catalog_schema(catalog_id, conn)
    preferred_mode = _get_preferred_mode(request)
    from dynastore.modules.tasks.models import TaskExecutionMode
    mode = TaskExecutionMode.SYNCHRONOUS if preferred_mode == models.JobControlOptions.SYNC_EXECUTE else TaskExecutionMode.ASYNCHRONOUS
    try:
        result = await execution_engine.start_job(
            job_id, engine=engine, mode=mode, db_schema=schema,
            background_tasks=background_tasks,
        )
    except ValueError:
        raise HTTPException(status_code=404, detail=f"Job '{job_id}' not found.")
    return _handle_execution_result(result, request)


def _handle_job_results(task: Task, job_id: uuid.UUID):
    if task.status == TaskStatusEnum.FAILED:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Job '{job_id}' failed and has no results. See status for error.",
        )
    if task.status != TaskStatusEnum.COMPLETED:
        raise HTTPException(
            status_code=status.HTTP_202_ACCEPTED,
            detail=f"Job '{job_id}' is not complete. Current status: {task.status}",
        )
    return task.outputs or {}


class ProcessesService(ExtensionProtocol):
    priority: int = 100
    """
    Implements the OGC API - Processes standard.
    - Dynamically discovers registered Tasks that expose a process definition.
    - Uses the 'tasks' module to manage jobs (task executions).
    """

    conformance_uris = PROCESSES_CONFORMANCE
    router = router




def _get_job_links(task: Task, request: Request) -> List[models.Link]:
    """Helper to compute OGC HATEOAS links for a job."""
    job_id = task.task_id
    path_params = request.scope.get("path_params", {})
    catalog_id = path_params.get("catalog_id")
    collection_id = path_params.get("collection_id")

    if catalog_id and collection_id:
        status_url = str(request.url_for("get_job_status_collection", catalog_id=catalog_id, collection_id=collection_id, job_id=str(job_id)))
        results_url = str(request.url_for("get_job_results_collection", catalog_id=catalog_id, collection_id=collection_id, job_id=str(job_id)))
    elif catalog_id:
        status_url = str(request.url_for("get_job_status_catalog", catalog_id=catalog_id, job_id=str(job_id)))
        results_url = str(request.url_for("get_job_results_catalog", catalog_id=catalog_id, job_id=str(job_id)))
    else:
        status_url = str(request.url_for("get_job_status", job_id=str(job_id)))
        results_url = str(request.url_for("get_job_results", job_id=str(job_id)))

    links = [
        models.Link(href=status_url, rel="self", type="application/json", title="This document"),  # type: ignore[arg-type]
    ]
    if task.status == TaskStatusEnum.COMPLETED and task.outputs is not None:
        links.append(models.Link(href=results_url, rel="http://www.opengis.net/def/rel/ogc/1.0/results", type="application/json", title="Job results"))  # type: ignore[arg-type]
    
    return links
