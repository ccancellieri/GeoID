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

"""OGC API - Styles Part 1 extension for DynaStore.

Provides style CRUD (create / read / update / delete) scoped to
(catalog_id, collection_id) pairs, with content-negotiated stylesheet
retrieval supporting Mapbox GL JSON and SLD 1.0/1.1 encodings.

Follows Pattern B (instance router + ``_register_routes`` + ``OGCServiceMixin``)
so that the service participates in the global /conformance aggregator and
exposes a standard OGC landing page at GET /styles/.
"""

import lxml as _lxml_scope_gate  # noqa: F401  # SCOPE gate: extension_styles requires module_styles (lxml)
_ = _lxml_scope_gate  # silence pyright "unused" — load-bearing for SCOPE filtering

import json as _json
import logging
import os  # noqa: E402
from contextlib import asynccontextmanager
from typing import FrozenSet, List, Optional

from fastapi import APIRouter, Body, Depends, FastAPI, HTTPException, Query, Request, Response
from fastapi.responses import JSONResponse, RedirectResponse
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncConnection
from starlette import status

from dynastore.extensions import protocols
from dynastore.extensions.ogc_base import OGCServiceMixin
from dynastore.extensions.tools.fast_api import AppJSONResponse as _AppJSONResponse
from dynastore.extensions.tools.language_utils import get_language
from dynastore.extensions.web.decorators import expose_web_page, expose_static  # noqa: E402
from dynastore.extensions.tools.db import get_async_connection
from dynastore.extensions.tools.url import get_root_url
from dynastore.models.protocols import StylesProtocol
from dynastore.modules.catalog import catalog_module
from dynastore.modules.styles import db as styles_db
from dynastore.modules.styles.encodings import (
    STYLE_FORMAT_TO_MEDIA_TYPE,
    f_param_to_media_type,
    normalize_accept_to_media_type,
)
from dynastore.modules.styles.models import (
    MapboxContent,
    SLDContent,
    Style,
    StyleCreate,
    StyleUpdate,
)
from dynastore.tools.db import validate_sql_identifier
from dynastore.extensions.tools.query import parse_hints_param  # noqa: E402

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# OGC API - Styles Part 1 conformance URIs
# ---------------------------------------------------------------------------

OGC_API_STYLES_URIS = [
    "http://www.opengis.net/spec/ogcapi-styles-1/1.0/conf/core",
    "http://www.opengis.net/spec/ogcapi-styles-1/1.0/conf/manage-styles",
    "http://www.opengis.net/spec/ogcapi-styles-1/1.0/conf/style-info",
    "http://www.opengis.net/spec/ogcapi-styles-1/1.0/conf/mapbox-style",
    "http://www.opengis.net/spec/ogcapi-styles-1/1.0/conf/sld-10",
    "http://www.opengis.net/spec/ogcapi-styles-1/1.0/conf/sld-11",
    "http://www.opengis.net/spec/ogcapi-styles-1/1.0/conf/html",
    "http://www.opengis.net/spec/ogcapi-styles-1/1.0/conf/json",
]


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _pick_stylesheet_by_media_type(style: Style, media_type: str):
    """Return the StyleSheet matching ``media_type``, or ``None``."""
    for sheet in style.stylesheets:
        fmt = getattr(sheet.content, "format", None)
        fmt_str = fmt.value if fmt is not None and hasattr(fmt, "value") else str(fmt)
        if STYLE_FORMAT_TO_MEDIA_TYPE.get(fmt_str) == media_type:
            return sheet
    return None


def _stylesheet_to_bytes(sheet) -> bytes:
    """Serialise a StyleSheet's content to its wire encoding."""
    content = sheet.content
    if isinstance(content, SLDContent):
        return content.sld_body.encode("utf-8")
    if isinstance(content, MapboxContent):
        return _json.dumps(
            content.model_dump(by_alias=True, exclude_none=True)
        ).encode("utf-8")
    return _json.dumps(
        content.model_dump() if hasattr(content, "model_dump") else dict(content)
    ).encode("utf-8")


# ---------------------------------------------------------------------------
# Service
# ---------------------------------------------------------------------------


class StylesService(protocols.ExtensionProtocol, OGCServiceMixin, StylesProtocol):
    """OGC API - Styles Part 1 extension.

    Priority 100 — alongside STAC and Features.  Uses Pattern B (instance
    router) so ``self`` is available in handlers and ``OGCServiceMixin``
    helpers work correctly.
    """

    priority: int = 100
    router: APIRouter

    # OGCServiceMixin class attributes
    conformance_uris = OGC_API_STYLES_URIS
    prefix = "/styles"
    protocol_title = "DynaStore OGC API - Styles"
    protocol_description = "Style management and retrieval via OGC API - Styles"

    def __init__(self, app: Optional[FastAPI] = None):
        super().__init__()
        self.app = app
        self.router = APIRouter(prefix="/styles", tags=["OGC API - Styles"])
        self._register_routes()

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    @asynccontextmanager
    async def lifespan(self, app: FastAPI):
        logger.info("StylesService: started.")
        yield
        logger.info("StylesService: stopped.")

    # ------------------------------------------------------------------
    # Web page contribution (WebPageContributor / StaticAssetProvider)
    # ------------------------------------------------------------------

    def get_web_pages(self):
        from dynastore.extensions.tools.web_collect import collect_web_pages
        return collect_web_pages(self)

    def get_static_assets(self):
        from dynastore.extensions.tools.web_collect import collect_static_assets
        return collect_static_assets(self)

    def get_notebooks(self):
        try:
            from .notebooks import build_contributions
        except Exception:
            return []
        return build_contributions()

    @expose_static("styles")
    def provide_static_files(self) -> List[str]:
        """Exposes the internal static directory for the Styles browser."""
        static_dir = os.path.join(os.path.dirname(__file__), "static")
        files = []
        for root, _, filenames in os.walk(static_dir):
            for filename in filenames:
                files.append(os.path.join(root, filename))
        return files

    @expose_web_page(
        page_id="styles_browser",
        title="Styles Browser",
        icon="fa-palette",
        description="Browse styles, stylesheets, and legends.",
    )
    async def provide_styles_browser(self, request: Request):
        return await self._serve_page_template("styles_browser.html")

    async def _serve_page_template(self, filename: str):
        from dynastore._version import VERSION
        file_path = os.path.join(os.path.dirname(__file__), "static", filename)
        if not os.path.exists(file_path):
            return Response(content=f"Template {filename} not found", status_code=404)
        with open(file_path, "r", encoding="utf-8") as f:
            return Response(content=f.read().replace("{{VERSION}}", VERSION), media_type="text/html")

    # ------------------------------------------------------------------
    # Route registration (Pattern B)
    # ------------------------------------------------------------------

    def _register_routes(self) -> None:
        # Standard OGC landing page + conformance
        self.router.add_api_route("/", self.get_landing_page, methods=["GET"])
        self.router.add_api_route(
            "/conformance", self.get_conformance, methods=["GET"]
        )

        col_prefix = "/catalogs/{catalog_id}/collections/{collection_id}/styles"

        self.router.add_api_route(
            col_prefix,
            self.create_style_for_collection,
            methods=["POST"],
            response_model=Style,
            status_code=status.HTTP_201_CREATED,
            summary="Create a style for a collection",
        )
        self.router.add_api_route(
            col_prefix,
            self.list_styles,
            methods=["GET"],
            response_model=List[Style],
            summary="List styles for a collection",
        )
        self.router.add_api_route(
            col_prefix + "/{style_id}",
            self.get_style,
            methods=["GET"],
            response_model=Style,
            summary="Get a specific style",
        )
        self.router.add_api_route(
            col_prefix + "/{style_id}",
            self.update_style,
            methods=["PUT"],
            response_model=Style,
            summary="Update a style",
        )
        self.router.add_api_route(
            col_prefix + "/{style_id}",
            self.delete_style,
            methods=["DELETE"],
            status_code=status.HTTP_204_NO_CONTENT,
            summary="Delete a style",
        )
        self.router.add_api_route(
            col_prefix + "/{style_id}/stylesheet",
            self.get_stylesheet,
            methods=["GET"],
            summary="Get stylesheet body (content-negotiated)",
        )
        self.router.add_api_route(
            col_prefix + "/{style_id}/metadata",
            self.get_style_metadata,
            methods=["GET"],
            summary="Style metadata with stylesheet links per encoding",
        )
        self.router.add_api_route(
            col_prefix + "/{style_id}/legend",
            self.get_style_legend,
            methods=["GET"],
            summary="Redirect to style legend image (rel=preview)",
        )
        self.router.add_api_route(
            "/all",
            self.list_all_styles,
            methods=["GET"],
            summary="List all styles across catalogs (discovery convenience)",
        )

    # ------------------------------------------------------------------
    # Standard OGC endpoints (delegated to OGCServiceMixin)
    # ------------------------------------------------------------------

    async def get_landing_page(
        self, request: Request, language: str = Depends(get_language)
    ) -> _AppJSONResponse:
        return await self.ogc_landing_page_handler(request, language=language)

    async def get_conformance(self, request: Request):
        return await self.ogc_conformance_handler(request)

    # ------------------------------------------------------------------
    # Style CRUD
    # ------------------------------------------------------------------

    async def create_style_for_collection(
        self,
        catalog_id: str,
        collection_id: str,
        style: StyleCreate = Body(...),
        conn: AsyncConnection = Depends(get_async_connection),
    ) -> Style:
        validate_sql_identifier(catalog_id)
        validate_sql_identifier(collection_id)

        if not await catalog_module.get_collection(catalog_id, collection_id):
            raise HTTPException(status_code=404, detail="Collection not found.")

        from dynastore.modules.db_config.partition_tools import ensure_partition_exists

        try:
            await ensure_partition_exists(
                conn,
                table_name="styles",
                schema="styles",
                strategy="LIST",
                partition_value=catalog_id,
            )
        except Exception as exc:
            logger.error(
                "Failed to ensure partition for catalog '%s': %s", catalog_id, exc, exc_info=True
            )
            raise HTTPException(
                status_code=500,
                detail=f"Could not prepare database for catalog '{catalog_id}'.",
            ) from exc

        try:
            return await styles_db.create_style(conn, catalog_id, collection_id, style)
        except IntegrityError as exc:
            if exc.orig and getattr(exc.orig, "sqlstate", None) == "23505":
                raise HTTPException(
                    status_code=409,
                    detail=f"Style '{style.style_id}' already exists for this collection.",
                ) from exc
            raise

    async def list_styles(
        self,
        catalog_id: str,
        collection_id: str,
        conn: AsyncConnection = Depends(get_async_connection),
        limit: int = Query(100, ge=1, le=1000),
        offset: int = Query(0, ge=0),
        request_hints: FrozenSet = Depends(parse_hints_param),
    ) -> List[Style]:
        # Accepted for uniform cross-protocol routing-hints support; this route
        # reads style metadata rows and performs no vector-geometry read.
        validate_sql_identifier(catalog_id)
        validate_sql_identifier(collection_id)

        if not await catalog_module.get_collection(catalog_id, collection_id):
            raise HTTPException(status_code=404, detail="Collection not found.")
        return await styles_db.list_styles_for_collection(
            conn, catalog_id, collection_id, limit, offset
        )

    async def get_style(
        self,
        catalog_id: str,
        collection_id: str,
        style_id: str,
        conn: AsyncConnection = Depends(get_async_connection),
    ) -> Style:
        validate_sql_identifier(catalog_id)
        validate_sql_identifier(collection_id)

        style = await styles_db.get_style_by_id_and_collection(
            conn, catalog_id, collection_id, style_id
        )
        if not style:
            raise HTTPException(status_code=404, detail="Style not found.")
        return style

    async def update_style(
        self,
        catalog_id: str,
        collection_id: str,
        style_id: str,
        style_update: StyleUpdate = Body(...),
        conn: AsyncConnection = Depends(get_async_connection),
    ) -> Style:
        validate_sql_identifier(catalog_id)
        validate_sql_identifier(collection_id)

        updated = await styles_db.update_style(conn, catalog_id, style_id, style_update)
        if not updated:
            raise HTTPException(status_code=404, detail="Style not found.")
        return updated

    async def delete_style(
        self,
        catalog_id: str,
        collection_id: str,
        style_id: str,
        conn: AsyncConnection = Depends(get_async_connection),
    ) -> None:
        validate_sql_identifier(catalog_id)
        validate_sql_identifier(collection_id)

        style_to_delete = await styles_db.get_style_by_id_and_collection(
            conn, catalog_id, collection_id, style_id
        )
        if not style_to_delete:
            raise HTTPException(status_code=404, detail="Style not found.")

        deleted = await styles_db.delete_style(conn, catalog_id, style_to_delete.id)
        if not deleted:
            raise HTTPException(status_code=404, detail="Style found but could not be deleted.")

    # ------------------------------------------------------------------
    # Content-negotiated stylesheet
    # ------------------------------------------------------------------

    async def get_stylesheet(
        self,
        request: Request,
        catalog_id: str,
        collection_id: str,
        style_id: str,
        f: Optional[str] = Query(
            None,
            description="Format shorthand: mapbox | sld11 | sld10 | qml | flat | html | json",
        ),
        conn: AsyncConnection = Depends(get_async_connection),
    ) -> Response:
        validate_sql_identifier(catalog_id)
        validate_sql_identifier(collection_id)

        style = await styles_db.get_style_by_id_and_collection(
            conn, catalog_id, collection_id, style_id
        )
        if style is None:
            raise HTTPException(status_code=404, detail="Style not found.")

        available_media_types = []
        for sheet in style.stylesheets:
            fmt = getattr(sheet.content, "format", None)
            fmt_str = fmt.value if fmt is not None and hasattr(fmt, "value") else str(fmt)
            mt = STYLE_FORMAT_TO_MEDIA_TYPE.get(fmt_str)
            if mt is not None:
                available_media_types.append(mt)

        if not available_media_types:
            raise HTTPException(
                status_code=404,
                detail="Style has no stylesheet in a supported encoding.",
            )

        # ?f= wins over Accept header.
        target = f_param_to_media_type(f) if f else None
        if target is None:
            target = normalize_accept_to_media_type(
                request.headers.get("accept", ""),
                available_media_types,
            )
        if target is None:
            raise HTTPException(
                status_code=406,
                detail=f"No acceptable encoding. Available: {available_media_types}",
            )
        if target not in available_media_types:
            raise HTTPException(
                status_code=406,
                detail=f"Requested encoding {target!r} not stored for this style.",
            )

        sheet = _pick_stylesheet_by_media_type(style, target)
        if sheet is None:
            raise HTTPException(status_code=406, detail="Encoding not stored.")
        return Response(content=_stylesheet_to_bytes(sheet), media_type=target)

    # ------------------------------------------------------------------
    # Style metadata (style-info conformance class)
    # ------------------------------------------------------------------

    async def get_style_metadata(
        self,
        request: Request,
        catalog_id: str,
        collection_id: str,
        style_id: str,
        conn: AsyncConnection = Depends(get_async_connection),
    ) -> JSONResponse:
        validate_sql_identifier(catalog_id)
        validate_sql_identifier(collection_id)

        style = await styles_db.get_style_by_id_and_collection(
            conn, catalog_id, collection_id, style_id
        )
        if style is None:
            raise HTTPException(status_code=404, detail="Style not found.")

        root = get_root_url(request).rstrip("/")
        base_href = (
            f"{root}/styles/catalogs/{catalog_id}"
            f"/collections/{collection_id}/styles/{style_id}"
        )
        stylesheet_links = []
        for sheet in style.stylesheets:
            fmt = getattr(sheet.content, "format", None)
            fmt_str = fmt.value if fmt is not None and hasattr(fmt, "value") else str(fmt)
            mt = STYLE_FORMAT_TO_MEDIA_TYPE.get(fmt_str)
            if mt is None:
                continue
            stylesheet_links.append(
                {
                    "rel": "stylesheet",
                    "type": mt,
                    "href": f"{base_href}/stylesheet",
                    "title": f"{style.title or style.style_id} ({mt})",
                }
            )

        serialized_links = [
            lnk.model_dump(by_alias=True, exclude_none=True)
            for lnk in (style.links or [])
        ]
        serialized_links.extend(stylesheet_links)
        return JSONResponse(
            content={
                "id": style.style_id,
                "title": style.title,
                "description": style.description,
                "keywords": style.keywords,
                "scope": "style",
                "links": serialized_links,
            }
        )

    # ------------------------------------------------------------------
    # Legend redirect
    # ------------------------------------------------------------------

    async def get_style_legend(
        self,
        catalog_id: str,
        collection_id: str,
        style_id: str,
        conn: AsyncConnection = Depends(get_async_connection),
    ) -> RedirectResponse:
        validate_sql_identifier(catalog_id)
        validate_sql_identifier(collection_id)

        style = await styles_db.get_style_by_id_and_collection(
            conn, catalog_id, collection_id, style_id
        )
        if style is None:
            raise HTTPException(status_code=404, detail="Style not found.")

        for link in style.links or []:
            rel = link.get("rel") if isinstance(link, dict) else getattr(link, "rel", None)
            href = link.get("href") if isinstance(link, dict) else getattr(link, "href", None)
            if rel == "preview" and href:
                return RedirectResponse(url=href, status_code=307)
        raise HTTPException(status_code=404, detail="Style has no legend.")

    # ------------------------------------------------------------------
    # Cross-catalog discovery
    # ------------------------------------------------------------------

    async def list_all_styles(
        self,
        conn: AsyncConnection = Depends(get_async_connection),
        limit: int = Query(100, ge=1, le=1000),
        offset: int = Query(0, ge=0),
    ) -> JSONResponse:
        styles = await styles_db.list_all_styles(conn, limit=limit, offset=offset)
        return JSONResponse(
            content={
                "styles": [
                    {
                        "id": s.style_id,
                        "title": s.title,
                        "description": s.description,
                        "catalog_id": s.catalog_id,
                        "collection_id": s.collection_id,
                    }
                    for s in styles
                    if s is not None
                ]
            }
        )
