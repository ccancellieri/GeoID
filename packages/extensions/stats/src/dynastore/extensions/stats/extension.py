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

import logging
import os
import asyncio
from pathlib import Path
from typing import Optional, Any
from contextlib import asynccontextmanager
from datetime import datetime
from fastapi import APIRouter, Query, Request
from fastapi.responses import HTMLResponse
from dynastore.extensions.protocols import ExtensionProtocol
from dynastore.extensions.web.decorators import expose_web_page
from dynastore.extensions.tools.web_collect import collect_web_pages
from dynastore.models.auth import Policy
from dynastore.models.auth_models import Role
from dynastore.models.protocols.authorization import IamRolesConfig
from dynastore.models.protocols.database import DatabaseProtocol
from dynastore.models.protocols.catalogs import CatalogsProtocol
from dynastore.models.protocols.policies import PermissionProtocol
from dynastore.modules.db_config.query_executor import DbResource
from dynastore.tools.discovery import get_protocol

logger = logging.getLogger(__name__)

from dynastore.models.protocols.stats import StatsProtocol


def _stats_policy(sysadmin_role_name: Optional[str] = None) -> Policy:
    """Pure declaration of the stats access policy.

    Returned to IAM via ``StatsExtension.get_policies``; never registers
    anything itself. ``sysadmin_role_name`` (default
    ``IamRolesConfig().sysadmin_role_name``) is a foreign key into
    ``iam.roles`` and only carries through to the role binding emitted
    alongside.
    """
    return Policy(
        id="stats_endpoint_access",
        description="Sysadmin-only access to /stats/* observability endpoints.",
        actions=["GET", "OPTIONS"],
        resources=[r"^/stats(/.*)?$"],
        effect="ALLOW",
    )


def _stats_role_binding(sysadmin_role_name: Optional[str] = None) -> Role:
    """Pure declaration of the role binding for the stats policy."""
    return Role(
        name=sysadmin_role_name or IamRolesConfig().sysadmin_role_name,
        policies=["stats_endpoint_access"],
    )


class StatsExtension(ExtensionProtocol, StatsProtocol):
    priority: int = 100
    engine: Optional[DbResource] = None

    def __init__(self, app: Any = None):
        self.app = app
        self.router = APIRouter(prefix="/stats", tags=["Stats"])
        self._setup_routes()

    def _setup_routes(self):
        self.router.add_api_route(
            "/system",
            self.get_system_stats,
            methods=["GET"],
            summary="Retrieve global system-level stats",
        )
        self.router.add_api_route(
            "/catalogs/{catalog_id}",
            self.get_catalog_stats,
            methods=["GET"],
            summary="Retrieve stats for a specific catalog",
        )
        self.router.add_api_route(
            "/catalogs/{catalog_id}/collections/{collection_id}",
            self.get_collection_stats,
            methods=["GET"],
            summary="Retrieve stats for a specific collection",
        )

    def get_web_pages(self):
        # Skip nav registration in deployments without IAM — there are no
        # authenticated roles to gate the page on, so it would surface to
        # anonymous callers or be unreachable.
        if get_protocol(PermissionProtocol) is None:
            return []
        return collect_web_pages(self)

    @expose_web_page(
        page_id="stats",
        title={"en": "System Stats", "es": "Estadísticas", "fr": "Statistiques", "it": "Statistiche"},
        icon="fa-chart-bar",
        description={
            "en": "Per-catalog object counts, storage usage, and request stats.",
            "es": "Conteos por catálogo, uso de almacenamiento y estadísticas de solicitudes.",
            "fr": "Comptes par catalogue, utilisation du stockage et statistiques des requêtes.",
            "it": "Conteggi per catalogo, uso dello storage e statistiche delle richieste.",
        },
        audience_policy_id="stats_endpoint_access",
        section="admin",
        priority=18,
    )
    async def provide_stats_page(self, request: Request) -> HTMLResponse:
        from dynastore._version import VERSION
        file_path = os.path.join(os.path.dirname(__file__), "static", "stats.html")
        if not os.path.exists(file_path):
            return HTMLResponse(
                content='<div class="text-slate-400 text-sm py-8">Stats page template missing.</div>'
            )
        html = await asyncio.to_thread(Path(file_path).read_text, encoding="utf-8")
        prefix = (request.scope.get("root_path") or "").rstrip("/")
        html = html.replace("__STATS_API_PREFIX__", prefix)
        return HTMLResponse(content=html.replace("{{VERSION}}", VERSION))

    @property
    def catalogs(self) -> CatalogsProtocol:
        svc = self.get_protocol(CatalogsProtocol)
        if svc is None:
            raise RuntimeError("CatalogsProtocol not registered")
        return svc

    @property
    def database(self) -> DatabaseProtocol:
        svc = self.get_protocol(DatabaseProtocol)
        if svc is None:
            raise RuntimeError("DatabaseProtocol not registered")
        return svc

    @property
    def stats_service(self) -> Optional[StatsProtocol]:
        return self.get_protocol(StatsProtocol)

    async def log_access(
        self,
        request: Any,
        status_code: int,
        processing_time_ms: float,
        details=None,
        schema: str = "catalog",
    ) -> None:
        """Delegation to the underlying StatsService."""
        svc = self.stats_service
        if svc and svc is not self:
            await svc.log_access(request, status_code, processing_time_ms, details=details, schema=schema)

    def log_request_completion(
        self,
        request: Any,
        status_code: int,
        processing_time_ms: float,
        details=None,
        catalog_id=None,
    ) -> None:
        """Delegation to the underlying StatsService."""
        svc = self.stats_service
        if svc and svc is not self:
            svc.log_request_completion(
                request, status_code, processing_time_ms,
                details=details, catalog_id=catalog_id,
            )

    @asynccontextmanager
    async def lifespan(self, app: Any):
        db = self.database
        if db:
            self.engine = db.engine

        if not self.engine:
            logger.warning(
                "StatsExtension: No DB engine found via DatabaseProtocol. Extension disabled."
            )
            yield
            return

        # Policies declared via PolicyContributor (get_policies +
        # get_role_bindings); IAM picks them up centrally.
        logger.info("StatsExtension initialized.")
        yield

    async def _get_stats_summary(
        self,
        catalog_id: str,
        principal_id: Optional[str] = None,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
    ):
        catalogs = self.catalogs
        
        if not self.stats_service:
            return {"total_requests": 0, "average_latency_ms": 0}

        schema = "catalog"
        if catalog_id and catalog_id != "_system_":
            try:
                schema = await catalogs.resolve_physical_schema(catalog_id) or "catalog"
            except Exception as exc:
                logger.warning(
                    "StatsExtension: failed to resolve physical schema"
                    " for catalog_id=%s, falling back to 'catalog': %s",
                    catalog_id, exc,
                )

        summary = await self.stats_service.get_summary(
            schema=schema,
            catalog_id=catalog_id if catalog_id != "_system_" else None,
            principal_id=principal_id,
            start_date=start_date,
            end_date=end_date,
        )
        return summary.model_dump() if summary else {"total_requests": 0, "average_latency_ms": 0}

    async def get_system_stats(
        self,
        principal_id: Optional[str] = Query(None),
        start_date: Optional[datetime] = Query(None),
        end_date: Optional[datetime] = Query(None),
    ):
        """Retrieve global system-level stats."""
        return await self._get_stats_summary(
            catalog_id="_system_",
            principal_id=principal_id,
            start_date=start_date,
            end_date=end_date,
        )

    async def get_catalog_stats(
        self,
        catalog_id: str,
        principal_id: Optional[str] = Query(None),
        start_date: Optional[datetime] = Query(None),
        end_date: Optional[datetime] = Query(None),
    ):
        """Retrieve stats for a specific catalog."""
        return await self._get_stats_summary(
            catalog_id=catalog_id,
            principal_id=principal_id,
            start_date=start_date,
            end_date=end_date,
        )

    async def get_collection_stats(
        self,
        catalog_id: str,
        collection_id: str,
        principal_id: Optional[str] = Query(None),
        start_date: Optional[datetime] = Query(None),
        end_date: Optional[datetime] = Query(None),
    ):
        """Retrieve stats for a specific collection."""
        return await self._get_stats_summary(
            catalog_id=catalog_id,
            principal_id=principal_id,
            start_date=start_date,
            end_date=end_date,
        )

