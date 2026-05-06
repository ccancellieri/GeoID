# src/dynastore/extensions/notebooks/notebooks_extension.py
from contextlib import asynccontextmanager
from fastapi import APIRouter, FastAPI, Depends, HTTPException, Request
from fastapi.responses import Response
from typing import List, Optional, Any, Dict, Tuple
from fastapi import Query

from dynastore.extensions.protocols import ExtensionProtocol
from dynastore.extensions.web import expose_static, expose_web_page
from dynastore.modules.db_config.exceptions import ResourceNotFoundError
from dynastore.modules.notebooks import notebooks_module as notebook_service
from dynastore.modules.notebooks.models import NotebookCreate, Notebook, PlatformNotebookCreate, PlatformNotebook, OwnerType

import logging
import os

logger = logging.getLogger(__name__)


def _coerce_localized(value: Any) -> Any:
    """Accept a plain string title and wrap it into a LocalizedText-shaped dict.

    The ``title`` field on ``NotebookBase`` is typed as ``LocalizedText`` (a
    language-keyed dict, e.g. ``{"en": "..."}``). Historical seed payloads and
    round-tripped notebooks may carry a plain string. Normalise here so callers
    don't need to pre-wrap.
    """
    if isinstance(value, str):
        return {"en": value}
    return value


class NotebooksExtension(ExtensionProtocol):
    priority: int = 100
    """
    Extension that exposes notebook management endpoints and a web-based
    notebook browser.

    Provides:
    - Platform-level notebook REST API (cross-tenant, module-registered + sysadmin)
    - Tenant-level notebook CRUD (per catalog, user-owned)
    - Copy from platform to tenant
    - Web page for browsing, viewing and managing notebooks
    - JupyterLite static assets for in-browser execution
    """
    router = APIRouter(prefix="/notebooks", tags=["Notebooks"])

    def get_web_pages(self):
        from dynastore.extensions.tools.web_collect import collect_web_pages
        return collect_web_pages(self)

    def get_static_assets(self):
        from dynastore.extensions.tools.web_collect import collect_static_assets
        return collect_static_assets(self)

    def __init__(self, app: Optional[FastAPI] = None):
        super().__init__()
        self.app = app
        self._static_dir = os.path.join(os.path.dirname(__file__), "static")
        # Lite assets are baked at image-build time. Override via JUPYTERLITE_DIR
        # so they can live outside the hot-reload bind mount in dev compose
        # (which would otherwise mask them under src/.../static/lite/).
        self._lite_dir = os.environ.get("JUPYTERLITE_DIR") or os.path.join(self._static_dir, "lite")
        self._register_routes()

    @asynccontextmanager
    async def lifespan(self, app: FastAPI):
        from .policies import register_notebooks_policies
        register_notebooks_policies()
        logger.info("NotebooksExtension: Policies registered.")
        yield

    def _register_routes(self):
        # Platform notebooks (global, cross-tenant)
        self.router.add_api_route(
            "/platform",
            self.list_platform_notebooks,
            methods=["GET"],
            response_model=Dict[str, Any],
            summary="List platform notebooks",
        )
        self.router.add_api_route(
            "/platform/{notebook_id}",
            self.get_platform_notebook,
            methods=["GET"],
            summary="Get a platform notebook",
        )
        self.router.add_api_route(
            "/platform/{notebook_id}",
            self.save_platform_notebook,
            methods=["PUT"],
            response_model=PlatformNotebook,
            summary="Create or update a platform notebook (sysadmin)",
        )
        self.router.add_api_route(
            "/platform/{notebook_id}",
            self.delete_platform_notebook,
            methods=["DELETE"],
            summary="Soft-delete a platform notebook (sysadmin)",
        )
        # Copy platform -> tenant
        self.router.add_api_route(
            "/{catalog_id}/copy/{platform_notebook_id}",
            self.copy_platform_notebook,
            methods=["POST"],
            response_model=Notebook,
            status_code=201,
            summary="Copy a platform notebook into a tenant catalog",
        )
        # Tenant CRUD
        self.router.add_api_route(
            "/{catalog_id}",
            self.list_notebooks,
            methods=["GET"],
            response_model=Dict[str, Any],
            summary="List notebooks in a catalog",
        )
        self.router.add_api_route(
            "/{catalog_id}/{notebook_code}",
            self.get_notebook,
            methods=["GET"],
            response_model=Notebook,
            summary="Get full notebook content",
        )
        self.router.add_api_route(
            "/{catalog_id}/{notebook_code}",
            self.save_notebook,
            methods=["PUT"],
            response_model=Notebook,
            summary="Save a notebook",
        )
        self.router.add_api_route(
            "/{catalog_id}/{notebook_code}",
            self.delete_notebook,
            methods=["DELETE"],
            summary="Soft-delete a notebook",
        )

    # ------------------------------------------------------------------
    # Web page: Notebook browser
    # ------------------------------------------------------------------

    @expose_web_page(
        page_id="notebooks",
        title="Notebooks",
        icon="fa-book-open",
        description="Interactive notebooks for learning and documenting platform workflows.",
        priority=40,
    )
    async def provide_notebooks_page(self, request: Optional[Request] = None, language: str = "en"):
        """Serve the notebooks browser HTML page."""
        from dynastore._version import VERSION
        html_path = os.path.join(self._static_dir, "notebooks.html")
        if not os.path.exists(html_path):
            return Response(content="Notebooks page template not found", status_code=404)
        with open(html_path, "r", encoding="utf-8") as f:
            return Response(content=f.read().replace("{{VERSION}}", VERSION), media_type="text/html")

    # ------------------------------------------------------------------
    # Platform notebook endpoints
    # ------------------------------------------------------------------

    async def list_platform_notebooks(
        self,
        q: Optional[str] = Query(None, description="Search title/description"),
        tags: Optional[str] = Query(None, description="Comma-separated tag filter"),
        limit: int = Query(20, ge=1, le=100),
        offset: int = Query(0, ge=0),
    ) -> Dict[str, Any]:
        """List active platform notebooks (no auth required)."""
        tag_list = [t.strip() for t in tags.split(",") if t.strip()] if tags else None
        items, total = await notebook_service.list_platform_notebooks(
            q=q, tags=tag_list, limit=limit, offset=offset,
        )
        return {"items": items, "total": total, "limit": limit, "offset": offset}

    async def get_platform_notebook(self, notebook_id: str):
        """Get a platform notebook by ID (no auth required)."""
        try:
            return await notebook_service.get_platform_notebook(notebook_id)
        except ResourceNotFoundError as e:
            raise HTTPException(status_code=404, detail=str(e))

    async def save_platform_notebook(
        self,
        notebook_id: str,
        content: Dict[str, Any],
    ):
        """Create or update a platform notebook."""
        meta = content.get("metadata", {})
        title = _coerce_localized(meta.get("title", notebook_id))
        notebook_model = PlatformNotebookCreate(
            notebook_id=notebook_id,
            title=title,
            description=meta.get("description"),
            tags=list(meta.get("tags", []) or []),
            content=content,
            metadata=meta,
            registered_by="sysadmin",
            owner_type=OwnerType.SYSADMIN,
        )
        return await notebook_service.save_platform_notebook(notebook_model)

    async def delete_platform_notebook(
        self,
        notebook_id: str,
    ):
        """Soft-delete a platform notebook."""
        await notebook_service.delete_platform_notebook(notebook_id)
        return Response(status_code=204)

    # ------------------------------------------------------------------
    # Copy platform -> tenant
    # ------------------------------------------------------------------

    async def copy_platform_notebook(
        self,
        catalog_id: str,
        platform_notebook_id: str,
        request: Request,
    ):
        """Copy a platform notebook into a tenant catalog."""
        principal = getattr(request.state, "principal", None)
        owner_id = str(principal.id) if principal and hasattr(principal, "id") else None
        return await notebook_service.copy_from_platform(
            catalog_id, platform_notebook_id, owner_id  # type: ignore[arg-type]
        )

    # ------------------------------------------------------------------
    # Tenant notebook CRUD
    # ------------------------------------------------------------------

    async def list_notebooks(
        self,
        catalog_id: str,
        q: Optional[str] = Query(None, description="Search title/description"),
        tags: Optional[str] = Query(None, description="Comma-separated tag filter"),
        limit: int = Query(20, ge=1, le=100),
        offset: int = Query(0, ge=0),
    ):
        """List all active notebooks in a catalog."""
        tag_list = [t.strip() for t in tags.split(",") if t.strip()] if tags else None
        items, total = await notebook_service.list_notebooks(
            catalog_id, q=q, tags=tag_list, limit=limit, offset=offset,
        )
        return {"items": items, "total": total, "limit": limit, "offset": offset}

    async def get_notebook(
        self,
        catalog_id: str,
        notebook_code: str,
    ):
        """Get full notebook content."""
        return await notebook_service.get_notebook(catalog_id, notebook_code)

    async def save_notebook(
        self,
        catalog_id: str,
        notebook_code: str,
        content: Dict[str, Any],
        request: Request,
    ):
        """Save a notebook."""
        meta = content.get("metadata", {})
        title = _coerce_localized(meta.get("title", notebook_code))
        notebook_model = NotebookCreate(
            notebook_id=notebook_code,
            title=title,
            tags=list(meta.get("tags", []) or []),
            content=content,
            metadata=meta,
        )
        principal = getattr(request.state, "principal", None)
        owner_id = str(principal.id) if principal and hasattr(principal, "id") else None
        return await notebook_service.save_notebook(catalog_id, notebook_model, owner_id=owner_id)

    async def delete_notebook(
        self,
        catalog_id: str,
        notebook_code: str,
    ):
        """Soft-delete a notebook from a catalog."""
        await notebook_service.delete_notebook(catalog_id, notebook_code)
        return Response(status_code=204)

    # ------------------------------------------------------------------
    # Static file providers
    # ------------------------------------------------------------------

    @expose_static("lite")
    def serve_lite_static(self) -> List[str]:
        """Serve JupyterLite static assets from ``self._lite_dir``."""
        files: List[str] = []
        if not os.path.isdir(self._lite_dir):
            return files
        for root, _, filenames in os.walk(self._lite_dir):
            for filename in filenames:
                files.append(os.path.join(root, filename))
        return files

    def get_router(self) -> APIRouter:
        return self.router
