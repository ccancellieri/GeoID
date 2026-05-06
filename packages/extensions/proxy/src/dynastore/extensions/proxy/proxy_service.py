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
import httpx
from fastapi import (
    APIRouter,
    HTTPException,
    Request,
    BackgroundTasks,
    Response,
    FastAPI,
    Depends,
    Query,
)
from fastapi.responses import RedirectResponse, StreamingResponse
from typing import Optional, List
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from dynastore.tools.cache import cached
from dynastore.modules.proxy.proxy_module import (
    create_short_url,
    get_long_url,
    get_analytics,
    delete_short_url,
    get_urls_by_collection,
    log_redirect,
    _get_proxy_module,
)
from dynastore.modules.concurrency import run_in_background
from dynastore.modules.proxy.models import ShortURL, AnalyticsPage
from dynastore.extensions.protocols import ExtensionProtocol
from dynastore.extensions.tools.db import get_async_engine
from dynastore.modules.db_config.query_executor import DbResource
from dynastore.models.protocols.stats import StatsProtocol
from dynastore.tools.discovery import get_protocol
from .models import ProxyRequest

from . import hooks
from . import tenant_initialization  # Registration trigger

logger = logging.getLogger(__name__)

# Headers to strip from the incoming request before proxying.
STRIP_HEADERS = [
    "host",
    "content-length",
    "transfer-encoding",
    "connection",
    "x-forwarded-for",
    "x-forwarded-host",
    "x-forwarded-port",
    "x-forwarded-proto",
    "x-real-ip",
    "x-proxy-user",
    "x-proxy-host",
    "x-proxy-port",
]

FORWARD_HEADER_PREFIX = "x-forward-to-"


@cached(maxsize=10000, namespace="proxy", ignore=["conn"])
async def _get_cached_long_url(
    conn: DbResource, catalog_id: str, short_key: str
) -> Optional[str]:
    """Helper function to cache long URL lookups."""
    return await get_long_url(conn, catalog_id=catalog_id, short_key=short_key)


async def _log_proxy_analytics(
    engine: DbResource,
    catalog_id: str,
    short_key: str,
    ip_address: str,
    user_agent: str,
    referrer: str,
):
    """Background task to log specific proxy analytics (buffered)."""
    # This calls the module's log_redirect, which now buffers the write
    try:
        await log_redirect(
            engine,
            catalog_id=catalog_id,
            short_key=short_key,
            ip_address=ip_address,
            user_agent=user_agent,
            referrer=referrer,
            timestamp=datetime.now(timezone.utc),
        )
    except Exception as e:
        logger.error(f"Failed to log proxy analytics: {e}")
class ProxyService(ExtensionProtocol):
    priority: int = 100
    router: APIRouter = APIRouter(prefix="/proxy", tags=["URL Proxy"])

    def __init__(self):
        logger.info("ProxyService: Initializing URL Proxy extension.")

    @asynccontextmanager
    async def lifespan(self, app: FastAPI):
        from dynastore.models.protocols import WebModuleProtocol
        from dynastore.tools.discovery import get_protocol

        self.web_service = get_protocol(WebModuleProtocol)
        if not self.web_service:
            logger.warning(
                "ProxyService: 'web' extension not found. HTTP caching for redirects will be disabled."
            )

        from dynastore.models.protocols import HttpxProtocol

        httpx_protocol = get_protocol(HttpxProtocol)
        if httpx_protocol:
            logger.info(
                "ProxyService: 'httpx' extension found. Enabling anonymizing proxy endpoint at /p/{short_key}."
            )
            self._register_proxy_endpoint()
        else:
            logger.warning(
                "ProxyService: 'httpx' extension not found. The anonymizing proxy endpoint /p/{short_key} will be disabled."
            )

        # Register automated cleanup hooks
        hooks.register_proxy_listeners()

        yield

    @router.post(
        "/catalogs/{catalog_id}/urls", response_model=ShortURL, status_code=201
    )
    async def proxy_url_endpoint(
        catalog_id: str,  # type: ignore[reportGeneralTypeIssues]
        request: ProxyRequest,
        engine: DbResource = Depends(get_async_engine),
    ):
        try:
            new_url = await create_short_url(
                engine,
                catalog_id=catalog_id,
                long_url=str(request.long_url),
                comment=request.comment,
            )
            return new_url
        except Exception as e:
            raise HTTPException(
                status_code=409, detail=f"Could not create short URL. Error: {e}"
            )

    @router.post(
        "/catalogs/{catalog_id}/collections/{collection_id}/urls",
        response_model=ShortURL,
        status_code=201,
    )
    async def proxy_collection_url_endpoint(
        catalog_id: str,  # type: ignore[reportGeneralTypeIssues]
        collection_id: str,
        request: ProxyRequest,
        engine: DbResource = Depends(get_async_engine),
    ):
        """Creates a short URL scoped to a specific collection."""
        try:
            new_url = await create_short_url(
                engine,
                catalog_id=catalog_id,
                long_url=str(request.long_url),
                collection_id=collection_id,
                comment=request.comment,
            )
            return new_url
        except Exception as e:
            raise HTTPException(
                status_code=409, detail=f"Could not create short URL. Error: {e}"
            )

    @router.delete(
        "/catalogs/{catalog_id}/collections/{collection_id}/urls/{short_key}",
        status_code=204,
    )
    async def delete_collection_url_endpoint(
        catalog_id: str,  # type: ignore[reportGeneralTypeIssues]
        collection_id: str,
        short_key: str,
        engine: DbResource = Depends(get_async_engine),
    ):
        # Optionally verify ownership before deletion?
        # Partition-based deletion is implicitly handled by the catalog scope
        # since the parent table is partitioned.
        deleted_key = await delete_short_url(
            engine, catalog_id=catalog_id, short_key=short_key
        )
        if not deleted_key:
            raise HTTPException(status_code=404, detail="Short URL not found")

        # Invalidate the cache for the deleted short key
        _get_cached_long_url.cache_invalidate(engine, catalog_id, deleted_key)
        return Response(status_code=204)

    # --- REDIRECT ENDPOINTS ---

    @router.get(
        "/catalogs/{catalog_id}/r/{short_key}",
        name="redirect_endpoint_no_path",
        include_in_schema=False,
    )
    @router.get(
        "/catalogs/{catalog_id}/r/{short_key}/{path:path}",
        name="redirect_endpoint",
        summary="Redirects to the long URL, optionally appending a path.",
    )
    async def redirect_endpoint(
        catalog_id: str,  # type: ignore[reportGeneralTypeIssues]
        short_key: str,
        request: Request,
        background_tasks: BackgroundTasks,
        engine: DbResource = Depends(get_async_engine),
        path: str = "",
    ):
        long_url = await _get_cached_long_url(engine, catalog_id, short_key)
        if not long_url:
            raise HTTPException(status_code=404, detail="Short URL not found")

        base_url = httpx.URL(long_url)
        if path:
            target_path = base_url.path.rstrip("/") + "/" + path.lstrip("/")
            target_url = base_url.copy_with(path=target_path)
        else:
            target_url = base_url

        header_params = [
            (k[len(FORWARD_HEADER_PREFIX) :], v)
            for k, v in request.headers.items()
            if k.lower().startswith(FORWARD_HEADER_PREFIX)
        ]

        all_params = (
            list(base_url.params.multi_items())
            + list(request.query_params.multi_items())
            + header_params
        )
        target_url = target_url.copy_with(params=all_params)

        # 1. Log System Stats (Generic)
        stats_service = get_protocol(StatsProtocol)
        if stats_service:
            stats_service.log_access(
                request=request,
                background_tasks=background_tasks,  # type: ignore[call-arg]
                status_code=307,
                processing_time_ms=0,
                details={"short_key": short_key},
            )

        # 2. Log Proxy Analytics (Specific - Buffered)
        # We pass the engine to the background task to perform the buffered write
        ip_address = request.client.host if request.client else "unknown"
        user_agent = request.headers.get("user-agent", "")
        referrer = request.headers.get("referer", "")

        run_in_background(
            _log_proxy_analytics(
                engine, catalog_id, short_key, ip_address, user_agent, referrer
            ),
            name=f"proxy_analytics_{short_key}",
        )

        response = RedirectResponse(url=str(target_url), status_code=307)

        from dynastore.models.protocols import WebModuleProtocol

        web_service = get_protocol(WebModuleProtocol)
        if web_service:
            response.headers.update(web_service.get_cache_headers())
        return response

    @router.get(
        "/catalogs/{catalog_id}/collections/{collection_id}/r/{short_key}",
        name="redirect_collection_endpoint_no_path",
        include_in_schema=False,
    )
    @router.get(
        "/catalogs/{catalog_id}/collections/{collection_id}/r/{short_key}/{path:path}",
        name="redirect_collection_endpoint",
        summary="Collection-scoped redirect.",
    )
    async def redirect_collection_endpoint(
        catalog_id: str,  # type: ignore[reportGeneralTypeIssues]
        collection_id: str,
        short_key: str,
        request: Request,
        background_tasks: BackgroundTasks,
        engine: DbResource = Depends(get_async_engine),
        path: str = "",
    ):
        """
        Redirects using the same logic as the catalog-level endpoint.
        The collection_id in the path serves as semantic context.
        """
        # We reuse the same logic
        return await ProxyService.redirect_endpoint(  # type: ignore[attr-defined]
            catalog_id=catalog_id,
            short_key=short_key,
            request=request,
            background_tasks=background_tasks,
            engine=engine,
            path=path,
        )

    # --- PROXY ENDPOINTS ---

    def _register_proxy_endpoint(self):
        existing_function_names = {r.endpoint.__name__ for r in self.router.routes}  # type: ignore[attr-defined]

        if "proxy_root_endpoint" in existing_function_names:
            logger.debug(
                "Proxy endpoints already registered, skipping re-registration."
            )
            return

        from dynastore.extensions.httpx.httpx_service import get_proxy_httpx_client

        async def _perform_proxy_logic(
            catalog_id: str,
            short_key: str,
            path: str,
            request: Request,
            background_tasks: BackgroundTasks,
            engine: DbResource,
            proxy_client: httpx.AsyncClient,
        ):
            """
            Shared core logic for proxying requests.
            """
            long_url = await _get_cached_long_url(engine, catalog_id, short_key)
            if not long_url:
                raise HTTPException(status_code=404, detail="Short URL not found")

            try:
                base_url = httpx.URL(long_url)
                target_path = base_url.path.rstrip("/") + "/" + path.lstrip("/")
                all_params = list(base_url.params.multi_items()) + list(
                    request.query_params.multi_items()
                )
                target_url = base_url.copy_with(path=target_path, params=all_params)

                forwarded_headers = {
                    k[len(FORWARD_HEADER_PREFIX) :]: v
                    for k, v in request.headers.items()
                    if k.lower().startswith(FORWARD_HEADER_PREFIX)
                }

                headers = forwarded_headers.copy()
                headers["host"] = base_url.host.encode("ascii")  # type: ignore[assignment]
                headers["x-forwarded-for"] = request.client.host  # type: ignore[union-attr]
                headers["x-forwarded-proto"] = request.url.scheme
                headers["x-real-ip"] = request.client.host  # type: ignore[union-attr]

                proxy_req = proxy_client.build_request(
                    request.method,
                    target_url,
                    headers=headers,
                    content=request.stream(),
                )

                proxy_resp = await proxy_client.send(proxy_req, stream=True)

                # 1. Log System Stats
                stats_service = get_protocol(StatsProtocol)
                if stats_service:
                    stats_service.log_access(
                        request=request,
                        background_tasks=background_tasks,  # type: ignore[call-arg]
                        status_code=proxy_resp.status_code,
                        processing_time_ms=0,
                        details={"short_key": short_key, "proxy_target": long_url},
                    )

                # 2. Log Proxy Analytics (Specific - Buffered)
                ip_address = request.client.host if request.client else "unknown"
                user_agent = request.headers.get("user-agent", "")
                referrer = request.headers.get("referer", "")

                run_in_background(
                    _log_proxy_analytics(
                        engine, catalog_id, short_key, ip_address, user_agent, referrer
                    ),
                    name=f"proxy_analytics_{short_key}",
                )

                return StreamingResponse(
                    proxy_resp.aiter_raw(),
                    status_code=proxy_resp.status_code,
                    headers=proxy_resp.headers,
                )

            except httpx.RequestError as e:
                logger.error(f"Proxy request to {long_url} failed: {e}", exc_info=True)
                raise HTTPException(
                    status_code=502, detail=f"Failed to connect to the target URL: {e}"
                )

        @self.router.api_route(
            "/catalogs/{catalog_id}/p/{short_key}",
            methods=["GET", "POST", "PUT", "PATCH", "DELETE", "HEAD", "OPTIONS"],
            include_in_schema=True,
            summary="Proxy Root (No Path)",
        )
        async def proxy_root_endpoint(
            catalog_id: str,
            short_key: str,
            request: Request,
            background_tasks: BackgroundTasks,
            engine: DbResource = Depends(get_async_engine),
            proxy_client: httpx.AsyncClient = Depends(get_proxy_httpx_client),
        ):
            return await _perform_proxy_logic(
                catalog_id,
                short_key,
                "",
                request,
                background_tasks,
                engine,
                proxy_client,
            )

        @self.router.api_route(
            "/catalogs/{catalog_id}/p/{short_key}/{path:path}",
            methods=["GET", "POST", "PUT", "PATCH", "DELETE", "HEAD", "OPTIONS"],
            include_in_schema=True,
            summary="Proxy Sub-path",
        )
        async def proxy_path_endpoint(
            catalog_id: str,
            short_key: str,
            path: str,
            request: Request,
            background_tasks: BackgroundTasks,
            engine: DbResource = Depends(get_async_engine),
            proxy_client: httpx.AsyncClient = Depends(get_proxy_httpx_client),
        ):
            return await _perform_proxy_logic(
                catalog_id,
                short_key,
                path,
                request,
                background_tasks,
                engine,
                proxy_client,
            )

    # --- MANAGEMENT ENDPOINTS ---

    @router.get(
        "/catalogs/{catalog_id}/stats/{short_key}", response_model=AnalyticsPage
    )
    async def get_stats_endpoint(
        catalog_id: str,  # type: ignore[reportGeneralTypeIssues]
        short_key: str,
        engine: DbResource = Depends(get_async_engine),
        cursor: Optional[str] = Query(
            None,
            description="The cursor for keyset pagination. This is the 'id' of the last item from the previous page.",
        ),
        aggregate: bool = Query(
            default=False,
            description="If true, includes summary aggregations in the response.",
        ),
        start_date: Optional[datetime] = Query(
            default=None,
            description="The start date for filtering analytics logs (ISO 8601 format).",
        ),
        end_date: Optional[datetime] = Query(
            default=None,
            description="The end date for filtering analytics logs (ISO 8601 format).",
        ),
        page_size: int = Query(
            default=100,
            ge=1,
            le=1000,
            description="The number of analytics records to return per page.",
        ),
    ):
        return await get_analytics(
            engine,
            catalog_id=catalog_id,
            short_key=short_key,
            cursor=cursor,
            page_size=page_size,
            aggregate=aggregate,
            start_date=start_date,
            end_date=end_date,
        )

    # @router.get("/catalogs/{catalog_id}/owners/{owner_id}/urls", response_model=List[ShortURL])
    # async def get_owner_urls_endpoint(
    #     catalog_id: str,
    #     owner_id: str,
    #     engine: DbResource = Depends(get_async_engine),
    #     limit: int = Query(100, ge=1, le=1000),
    #     offset: int = Query(0, ge=0)
    # ):
    #     """Retrieves a paginated list of short URLs created by a specific owner."""
    #     urls = await get_urls_by_owner(engine, catalog_id=catalog_id, owner_id=owner_id, limit=limit, offset=offset)
    #     return urls

    @router.delete("/catalogs/{catalog_id}/urls/{short_key}", status_code=204)
    async def delete_url_endpoint(
        catalog_id: str, short_key: str, engine: DbResource = Depends(get_async_engine)  # type: ignore[reportGeneralTypeIssues]
    ):
        deleted_key = await delete_short_url(
            engine, catalog_id=catalog_id, short_key=short_key
        )
        if not deleted_key:
            raise HTTPException(status_code=404, detail="Short URL not found")

        # --- REMOVE FROM TENANT TRACKING TABLE (Generic) ---
        try:
            from dynastore.models.protocols import CatalogsProtocol

            catalogs = get_protocol(CatalogsProtocol)
            from dynastore.modules.db_config.query_executor import DDLQuery

            phys_schema = await catalogs.resolve_physical_schema(catalog_id)  # type: ignore[union-attr]
            if phys_schema:
                # We don't have collection_id here, but we can delete by short_key
                del_query = DDLQuery(
                    "DELETE FROM {schema}.collection_proxy_urls WHERE short_key = :key"
                )
                await del_query.execute(engine, schema=phys_schema, key=short_key)
        except Exception as e:
            logger.error(
                f"ProxyService: Failed to remove tracking for catalog URL: {e}"
            )

        # Invalidate the cache for the deleted short key
        _get_cached_long_url.cache_invalidate(engine, catalog_id, deleted_key)
        return Response(status_code=204)
