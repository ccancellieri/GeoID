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
import os
from typing import Optional, List, Dict, Any
from datetime import datetime
from contextlib import asynccontextmanager
from dynastore.extensions.protocols import ExtensionProtocol
from dynastore.extensions.web.decorators import expose_web_page
from dynastore.models.auth import Condition, Policy
from dynastore.models.auth_models import Role
from dynastore.models.protocols.authorization import IamRolesConfig
from dynastore.modules.db_config.query_executor import (
    DQLQuery,
    ResultHandler,
    managed_transaction,
    DbResource,
)
from dynastore.modules.catalog.catalog_module import register_event_listener
from dynastore.models.shared_models import SYSTEM_CATALOG_ID, SYSTEM_LOGS_TABLE
from dynastore.modules.catalog.log_manager import log_event
from fastapi import APIRouter, FastAPI, HTTPException, Query, Request
from fastapi.responses import HTMLResponse, JSONResponse
from .models import LogEntryCreate, LogEntry, LogsListResponse
from dynastore.modules.catalog.event_service import CatalogEventType
from dynastore.models.protocols.catalogs import CatalogsProtocol
from dynastore.models.protocols.database import DatabaseProtocol
from dynastore.tools.discovery import get_protocol

logger = logging.getLogger(__name__)


_DASHBOARD_ID = "dynastore-logs-dashboard"


def _public_path() -> str:
    """Resolve ``KIBANA_PUBLIC_PATH`` — the path under which the proxy is mounted."""
    raw = os.environ.get("KIBANA_PUBLIC_PATH", "/dashboards").strip()
    if not raw.startswith("/"):
        raw = "/" + raw
    return raw.rstrip("/") or "/dashboards"


def _kibana_url() -> Optional[str]:
    """Return a deep-link to the logs dashboard via the same-origin proxy.

    Returns ``None`` when ES isn't connected, so existing consumers of the
    ``kibana_dashboard_url`` field keep their ``None`` semantic. The returned
    URL is a path on the geoid origin (``/dashboards/...``), not a full
    URL to the upstream — so users don't need Kibana credentials.
    """
    from dynastore.modules.elasticsearch.client import get_client

    if get_client() is None:
        return None
    return f"{_public_path()}/app/dashboards#/view/{_DASHBOARD_ID}"


async def _probe_dashboards_health() -> Dict[str, bool]:
    """Probe the four dependencies the in-page status strip reports on."""
    from dynastore.modules.elasticsearch.client import get_client

    result = {
        "es": False,
        "upstream": False,
        "provisioned": False,
        "authorized": False,
    }

    # 1. ES reachable — singleton client present AND ping succeeds.
    es = get_client()
    if es is not None:
        try:
            await es.info()
            result["es"] = True
        except Exception:
            result["es"] = False

    # 2-4. Dashboards upstream reachable + authorized + dashboard present.
    upstream = os.environ.get("KIBANA_UPSTREAM_URL", "").strip().rstrip("/")
    if not upstream:
        return result

    import httpx

    headers = {"osd-xsrf": "true", "kbn-xsrf": "true"}
    key = os.environ.get("KIBANA_UPSTREAM_API_KEY", "").strip()
    if key:
        headers["Authorization"] = f"ApiKey {key}"

    try:
        async with httpx.AsyncClient(headers=headers, timeout=5.0) as client:
            try:
                r = await client.get(f"{upstream}/api/status")
                result["upstream"] = r.status_code < 500
                # 401/403 means we reached the upstream but auth failed.
                result["authorized"] = r.status_code not in (401, 403)
            except httpx.RequestError:
                return result

            if not result["authorized"]:
                return result

            try:
                # Direct by-type/id lookup avoids text-search on non-indexed
                # fields (saved-objects' `id` is a keyword, not analyzed).
                r = await client.get(
                    f"{upstream}/api/saved_objects/dashboard/{_DASHBOARD_ID}"
                )
                if r.status_code == 200:
                    result["provisioned"] = True
                elif r.status_code in (401, 403):
                    result["authorized"] = False
                # 404 → present upstream but dashboard not imported yet;
                # leave provisioned=False and return.
            except Exception:
                pass
    except Exception as exc:
        logger.debug("dashboards_health: probe failed: %s", exc)

    return result


def _logs_dashboard_policy() -> Policy:
    """Pure declaration of the logs-dashboard access policy.

    Returned to IAM via ``LogExtension.get_policies``; never registers
    anything itself. The proxy path is escaped because the framework
    treats ``Policy.resources`` entries as regexes.
    """
    import re as _re

    public_path = _public_path()
    escaped = _re.escape(public_path)
    return Policy(
        id="logs_dashboard_sysadmin_access",
        description=(
            "Sysadmin-only access to the embedded OpenSearch Dashboards / Kibana "
            "proxy and its status endpoints."
        ),
        actions=["GET", "POST", "PUT", "PATCH", "DELETE", "HEAD", "OPTIONS"],
        resources=[
            f"{escaped}",
            f"{escaped}/.*",
            "/web/pages/logs_dashboard",
            "/logs/_dashboards_health",
            "/logs/_dashboards_config",
        ],
        effect="ALLOW",
    )


def _logs_dashboard_role_binding(sysadmin_role_name: Optional[str] = None) -> Role:
    """Pure declaration of the role binding for the logs policy.

    ``sysadmin_role_name`` defaults to the active
    ``IamRolesConfig().sysadmin_role_name``; operators wiring a custom
    landscape pass an explicit name through their bootstrap.
    """
    return Role(
        name=sysadmin_role_name or IamRolesConfig().sysadmin_role_name,
        policies=["logs_dashboard_sysadmin_access"],
    )


def _logs_system_policy(sysadmin_role_name: Optional[str] = None) -> Policy:
    """Sysadmin-only access to the global ``/logs/system`` endpoint.

    System logs contain platform-wide events, error traces, and potentially
    sensitive configuration details — restrict to sysadmin only.
    """
    return Policy(
        id="logs_system_sysadmin_access",
        description=(
            "Sysadmin-only access to platform-wide system logs (/logs/system)."
        ),
        actions=["GET", "OPTIONS"],
        resources=[r"^/logs/system$"],
        effect="ALLOW",
    )


def _logs_system_role_binding(sysadmin_role_name: Optional[str] = None) -> Role:
    return Role(
        name=sysadmin_role_name or IamRolesConfig().sysadmin_role_name,
        policies=["logs_system_sysadmin_access"],
    )


def _logs_per_catalog_policy(sysadmin_role_name: Optional[str] = None) -> Policy:
    """Per-catalog access to the canonical ``/logs/catalogs/{cat}`` surface.

    Catalog members read their own catalog's logs (regular events plus
    ``index_failure_persistent`` / ``index_failure_retry`` emitted by the
    OUTBOX drain task). The ``catalog_membership_required`` condition
    handler at ``modules/iam/conditions.py`` enforces membership and
    bypasses the check for the configured sysadmin role.

    The regex matches every per-catalog log path: the bare catalog
    endpoint, the ``/logs`` suffix variant, and the per-collection
    nested path.
    """
    role_name = sysadmin_role_name or IamRolesConfig().sysadmin_role_name
    return Policy(
        id="logs_per_catalog_access",
        description=(
            "Catalog members access to their catalog's logs through the "
            "canonical /logs/ surface. Sysadmin bypass via condition."
        ),
        actions=["GET", "OPTIONS"],
        resources=[r"^/logs/catalogs/[^/]+(/.*)?$"],
        effect="ALLOW",
        conditions=[
            Condition(
                type="catalog_membership_required",
                config={"sysadmin_role": role_name},
            ),
        ],
    )


def _logs_per_catalog_role_bindings(
    admin_role_name: Optional[str] = None,
    user_role_name: Optional[str] = None,
) -> List[Role]:
    """Bind the per-catalog logs policy to ADMIN + USER roles.

    Sysadmin access is handled inside the condition handler, not via a
    separate role binding — same shape as ``web_dashboard_per_catalog_access``.
    """
    cfg = IamRolesConfig()
    admin = admin_role_name or cfg.admin_role_name
    user = user_role_name or cfg.default_user_role_name
    return [
        Role(name=admin, policies=["logs_per_catalog_access"]),
        Role(name=user, policies=["logs_per_catalog_access"]),
    ]


from dynastore.models.protocols.logs import LogsProtocol
class LogExtension(ExtensionProtocol, LogsProtocol):
    priority: int = 100
    engine: Optional[DbResource] = None

    def __init__(self, app: Any = None):
        self.app = app
        self.router = APIRouter(prefix="/logs", tags=["Logs"])
        self._setup_routes()

    def _setup_routes(self):
        self.router.add_api_route(
            "/system",
            self.get_system_logs,
            methods=["GET"],
            response_model=LogsListResponse,
            summary="Retrieve global system-level logs",
        )
        self.router.add_api_route(
            "/catalogs/{catalog_id}",
            self.get_catalog_logs,
            methods=["GET"],
            response_model=LogsListResponse,
            summary="Retrieve logs for a specific catalog",
        )
        self.router.add_api_route(
            "/catalogs/{catalog_id}/logs",
            self.get_catalog_logs,
            methods=["GET"],
            response_model=LogsListResponse,
            summary="Retrieve logs for a specific catalog",
        )
        self.router.add_api_route(
            "/catalogs/{catalog_id}/collections/{collection_id}/logs",
            self.get_collection_logs,
            methods=["GET"],
            response_model=LogsListResponse,
            summary="Retrieve logs for a specific collection",
        )
        # Status endpoints feeding the embedded logs dashboard page.
        self.router.add_api_route(
            "/_dashboards_health",
            self._get_dashboards_health,
            methods=["GET"],
            response_class=JSONResponse,
            include_in_schema=False,
        )
        self.router.add_api_route(
            "/_dashboards_config",
            self._get_dashboards_config,
            methods=["GET"],
            response_class=JSONResponse,
            include_in_schema=False,
        )

    def configure_app(self, app: FastAPI) -> None:
        """Mount the cross-origin-dashboard reverse proxy on the app.

        The proxy uses its own prefix (``${KIBANA_PUBLIC_PATH}``) rather than
        living under ``/logs`` so iframe-relative URLs in Kibana's bundles
        resolve correctly.
        """
        from dynastore.extensions.logs.dashboards_proxy import (
            build_dashboards_proxy_router,
        )
        app.include_router(build_dashboards_proxy_router())

    def get_web_pages(self):
        # Mirror the StatsExtension pattern: skip nav registration when no
        # PermissionProtocol provider is loaded. The page is sysadmin-only and
        # the underlying proxy is gated by logs_dashboard_sysadmin_access; both
        # require an authz backend to be meaningful.
        from dynastore.models.protocols.policies import PermissionProtocol
        from dynastore.extensions.tools.web_collect import collect_web_pages
        if get_protocol(PermissionProtocol) is None:
            return []
        return collect_web_pages(self)

    @expose_web_page(
        page_id="logs_dashboard",
        title="Log Analytics",
        icon="fa-chart-line",
        description="Embedded OpenSearch Dashboards / Kibana view of system logs.",
        audience_policy_id="logs_dashboard_sysadmin_access",
        section="admin",
        priority=25,
    )
    async def provide_logs_dashboard_page(self, request: Request):
        """Serve the embedded logs dashboard fragment."""
        html_path = os.path.join(
            os.path.dirname(__file__), "static", "logs-dashboard.html"
        )
        if not os.path.exists(html_path):
            raise HTTPException(status_code=404, detail="Logs dashboard template not found.")
        with open(html_path, "r", encoding="utf-8") as f:
            return HTMLResponse(f.read())

    @expose_web_page(
        page_id="catalog_logs",
        title="Catalog Logs",
        icon="fa-list-ul",
        description=(
            "Per-catalog logs, event-bus history, and tasks. Calls the "
            "canonical /logs/, /events/, /tasks/ surfaces; panels appear "
            "only when the backing module is mounted."
        ),
        audience_policy_id="logs_per_catalog_access",
        section="admin",
        priority=20,
    )
    async def provide_catalog_logs_page(self, request: Request):
        """Serve the per-catalog logs page fragment.

        Calls the canonical module REST surfaces (``/logs/``, ``/events/``,
        ``/tasks/``) directly from the browser. Server-side, we inspect
        which modules' Protocols are mounted on the current deployment
        SCOPE and inject a JSON ``__CATALOG_LOGS_CTX__`` block so the
        page hides panels whose backing module is missing — preventing
        spurious 404s on SCOPE-restricted images.
        """
        from dynastore.models.protocols.events import EventsProtocol
        from dynastore.models.protocols.tasks import TasksProtocol

        html_path = os.path.join(
            os.path.dirname(__file__), "static", "catalog-logs.html"
        )
        if not os.path.exists(html_path):
            raise HTTPException(
                status_code=404,
                detail="Catalog logs template not found.",
            )
        with open(html_path, "r", encoding="utf-8") as f:
            html = f.read()
        ctx = {
            "modules": {
                # LogService is registered by CatalogModule which is in
                # every public-facing SCOPE; the panel is unconditionally
                # visible.
                "logs": True,
                "events": get_protocol(EventsProtocol) is not None,
                "tasks": get_protocol(TasksProtocol) is not None,
            },
        }
        html = html.replace("__CATALOG_LOGS_CTX__", json.dumps(ctx))
        return HTMLResponse(html)

    async def _get_dashboards_health(self) -> Dict[str, bool]:
        """Live probe of the four dependencies surfaced in the status strip."""
        return await _probe_dashboards_health()

    async def _get_dashboards_config(self) -> Dict[str, Any]:
        """Return resolved, **masked** configuration for the embedded dashboard."""
        return {
            "upstream_url": os.environ.get("KIBANA_UPSTREAM_URL", "").strip() or None,
            "api_key_set": bool(os.environ.get("KIBANA_UPSTREAM_API_KEY", "").strip()),
            "public_path": _public_path(),
        }

    @property
    def catalogs(self) -> CatalogsProtocol:
        return self.get_protocol(CatalogsProtocol)  # type: ignore[return-value]

    @property
    def database(self) -> DatabaseProtocol:
        return self.get_protocol(DatabaseProtocol)  # type: ignore[return-value]

    # PolicyContributor: declare authz needs; IAM forwards centrally.
    # No direct call to PermissionProtocol — keeps the plugin agnostic
    # of the enforcement implementation.
    def get_policies(self):
        return [
            _logs_system_policy(),
            _logs_dashboard_policy(),
            _logs_per_catalog_policy(),
        ]

    def get_role_bindings(self):
        return [
            _logs_system_role_binding(),
            _logs_dashboard_role_binding(),
            *_logs_per_catalog_role_bindings(),
        ]

    @asynccontextmanager
    async def lifespan(self, app: Any):
        # Policies declared via PolicyContributor (get_policies +
        # get_role_bindings); IAM picks them up centrally.

        db = self.database
        if db:
            self.engine = db.engine

        if not self.engine:
            logger.warning(
                "LogExtension: No DB engine found via DatabaseProtocol. Extension disabled."
            )
            yield
            return

        # We rely on CatalogModule for schema management.

        self._register_listeners()
        logger.info("LogExtension initialized.")
        yield

    def _register_listeners(self):

        register_event_listener(
            CatalogEventType.CATALOG_CREATION, self._on_catalog_created
        )
        register_event_listener(
            CatalogEventType.CATALOG_DELETION, self._on_catalog_deleted
        )
        register_event_listener(
            CatalogEventType.CATALOG_HARD_DELETION, self._on_catalog_hard_deleted
        )
        register_event_listener(
            CatalogEventType.CATALOG_HARD_DELETION_FAILURE, self._on_catalog_failure
        )

    async def _on_catalog_created(self, catalog_id: str, **kwargs):
        db_resource = kwargs.pop("db_resource", None)
        await self.append_log(
            LogEntryCreate(
                catalog_id=catalog_id,
                event_type=CatalogEventType.CATALOG_CREATION.value,
                message="Catalog created.",
                details=kwargs,
                is_system=True,
            ),
            db_resource=db_resource,
        )

    async def _on_catalog_deleted(self, catalog_id: str, **kwargs):
        db_resource = kwargs.pop("db_resource", None)
        await self.append_log(
            LogEntryCreate(
                catalog_id=catalog_id,
                event_type=CatalogEventType.CATALOG_DELETION.value,
                message="Catalog soft-deleted.",
                details=kwargs,
                is_system=True,
            ),
            db_resource=db_resource,
        )

    async def _on_catalog_hard_deleted(self, catalog_id: str, **kwargs):
        # This goes to System Logs as the schema might be dropping/dropped
        db_resource = kwargs.pop("db_resource", None)
        await self.append_log(
            LogEntryCreate(
                catalog_id=catalog_id,
                event_type=CatalogEventType.CATALOG_HARD_DELETION.value,
                message="Catalog hard-deleted (schema dropped).",
                details=kwargs,
                is_system=True,
            ),
            db_resource=db_resource,
        )

    async def _on_catalog_failure(self, catalog_id: str, error: Optional[str] = None, **kwargs):
        # We route this via system catalog for global logging,
        # but preserve the actual failing catalog ID for LogService to extract.
        db_resource = kwargs.pop("db_resource", None)
        await self.append_log(
            LogEntryCreate(
                catalog_id=catalog_id,
                event_type=CatalogEventType.CATALOG_HARD_DELETION_FAILURE.value,
                level="ERROR",
                message=f"Hard deletion failed: {error}",
                details=kwargs,
                is_system=True,
            ),
            db_resource=db_resource,
            immediate=True,
        )

    async def append_log(
        self,
        entry: LogEntryCreate,
        db_resource: Optional[DbResource] = None,
        immediate: bool = False,
    ):
        """Appends a log entry via the catalog module's log manager."""
        await log_event(
            catalog_id=entry.catalog_id,
            event_type=entry.event_type,
            level=entry.level,
            message=entry.message,
            collection_id=entry.collection_id,
            details=entry.details,
            db_resource=db_resource,
            immediate=immediate,
            is_system=entry.is_system,
        )

    async def search_logs(
        self,
        catalog_id: str,
        collection_id: Optional[str] = None,
        event_type: Optional[str] = None,
        level: Optional[str] = None,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        limit: int = 100,
        offset: int = 0,
    ) -> List[LogEntry]:
        """
        Retrieves logs for a specific catalog (Tenant Logs).
        To view System Logs, pass catalog_id="_system_".

        Harmonization: If searching for a specific tenant, it includes records from
        both tenant logs and system logs associated with that tenant.
        """
        async with managed_transaction(self.engine) as conn:
            # 1. Prepare shared filters
            base_clauses = ["catalog_id = :catalog_id"]
            params = {"catalog_id": catalog_id, "limit": limit, "offset": offset}

            if collection_id:
                base_clauses.append("collection_id = :collection_id")
                params["collection_id"] = collection_id
            if event_type:
                base_clauses.append("event_type = :event_type")
                params["event_type"] = event_type
            if level:
                base_clauses.append("level = :level")
                params["level"] = level
            if start_date:
                base_clauses.append("timestamp >= :start_date")
                params["start_date"] = start_date
            if end_date:
                base_clauses.append("timestamp <= :end_date")
                params["end_date"] = end_date

            where_clause = " AND ".join(base_clauses)

            # 2. Determine tables to query
            queries = []

            # System Logs
            # If asking for _system_, we return ALL system logs.
            # If asking for a tenant, we return system logs matching that tenant.
            sys_where = where_clause
            if catalog_id == SYSTEM_CATALOG_ID:
                sys_where = where_clause.replace("catalog_id = :catalog_id", "TRUE")

            queries.append(
                f"SELECT * FROM catalog.{SYSTEM_LOGS_TABLE} WHERE {sys_where}"
            )

            # Tenant Logs
            if catalog_id != SYSTEM_CATALOG_ID:
                    # 1. Resolve Physical Names
                catalogs_provider = self.catalogs
                if not catalogs_provider:
                    raise HTTPException(
                        status_code=500, detail="CatalogsProtocol not available."
                    )

                physical_schema = await catalogs_provider.resolve_physical_schema(
                    catalog_id
                )
                if not physical_schema:
                    raise HTTPException(
                        status_code=404, detail=f"Catalog '{catalog_id}' not found."
                    )

                if physical_schema:
                    queries.append(
                        f'SELECT * FROM "{physical_schema}".logs WHERE {where_clause}'
                    )

            # 3. Combine queries with UNION ALL and sort
            final_sql = " UNION ALL ".join(queries)
            final_sql = f"SELECT * FROM ({final_sql}) AS combined_logs ORDER BY timestamp DESC LIMIT :limit OFFSET :offset"

            try:
                rows = await DQLQuery(
                    final_sql, result_handler=ResultHandler.ALL_DICTS
                ).execute(conn, **params)
                return [LogEntry.model_validate(r) for r in rows]
            except Exception as e:
                logger.debug(f"Log Search failed: {e}")
                return []


    async def get_system_logs(
        self,
        event_type: Optional[str] = Query(None),
        level: Optional[str] = Query(None),
        limit: int = 100,
        offset: int = 0,
    ) -> LogsListResponse:
        """Retrieve global system-level logs."""
        logs = await self.search_logs(
            catalog_id="_system_",
            event_type=event_type,
            level=level,
            limit=limit,
            offset=offset,
        )
        return LogsListResponse(
            logs=logs,
            kibana_dashboard_url=_kibana_url(),
        )

    async def get_catalog_logs(
        self,
        catalog_id: str,
        event_type: Optional[str] = Query(None),
        level: Optional[str] = Query(None),
        limit: int = 100,
        offset: int = 0,
    ) -> LogsListResponse:
        """
        Retrieve logs for a specific catalog.
        If the catalog has been hard-deleted, this may return final lifecycle events from the system log.
        """
        logs = await self.search_logs(
            catalog_id=catalog_id,
            event_type=event_type,
            level=level,
            limit=limit,
            offset=offset,
        )
        return LogsListResponse(
            logs=logs,
            kibana_dashboard_url=_kibana_url(),
        )

    async def get_collection_logs(
        self,
        catalog_id: str,
        collection_id: str,
        event_type: Optional[str] = Query(None),
        level: Optional[str] = Query(None),
        limit: int = 100,
        offset: int = 0,
    ) -> LogsListResponse:
        """Retrieve logs for a specific collection."""
        logs = await self.search_logs(
            catalog_id=catalog_id,
            collection_id=collection_id,
            event_type=event_type,
            level=level,
            limit=limit,
            offset=offset,
        )
        return LogsListResponse(
            logs=logs,
            kibana_dashboard_url=_kibana_url(),
        )
