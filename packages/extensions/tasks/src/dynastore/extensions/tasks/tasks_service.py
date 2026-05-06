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

# dynastore/extensions/tasks/tasks_service.py
import logging
import importlib.resources
import uuid
from typing import List, Optional
from fastapi import APIRouter, Depends, HTTPException, Query, status, FastAPI
from fastapi.responses import HTMLResponse
from sqlalchemy.ext.asyncio import AsyncConnection
from dynastore.extensions.tools.db import get_async_connection

# Import the generic tasks module and its models
from dynastore.modules.tasks import tasks_module
from dynastore.modules.tasks.models import Task
from dynastore.extensions.protocols import ExtensionProtocol
from uvicorn.middleware.proxy_headers import ProxyHeadersMiddleware

logger = logging.getLogger(__name__)
class TasksService(ExtensionProtocol):
    priority: int = 100 # Inherit ExtensionProtocol for consistency
    
    """
    Provides a generic API for tasks status and results monitoring.
    Managed by the tasks_module.
    """
    router: APIRouter = APIRouter(prefix="/tasks", tags=["Tasks API"])

    @router.get("/catalogs/{catalog_id}/monitor", response_class=HTMLResponse, summary="Task Monitoring Dashboard")
    async def get_task_monitor_page(catalog_id: str):  # type: ignore[reportGeneralTypeIssues]
        """
        Provides an HTML page to monitor the status of asynchronous tasks for a catalog.
        """
        try:
            with importlib.resources.open_text(__package__, "monitor.html") as f:  # type: ignore[arg-type]
                html_content = f.read()
            return HTMLResponse(content=html_content)
        except Exception as e:
            logger.error(f"Failed to load or read task monitor HTML page: {e}", exc_info=True)
            raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Failed to load task monitor page.")

    @router.get("/catalogs/{catalog_id}", response_model=List[Task], summary="List all asynchronous tasks")
    async def list_tasks_catalog(
        catalog_id: str,  # type: ignore[reportGeneralTypeIssues]
        conn: AsyncConnection = Depends(get_async_connection),
        limit: int = Query(20, ge=1, le=100),
        offset: int = Query(0, ge=0),
    ):
        """Retrieves a paginated list of all tasks for a specific catalog."""
        from dynastore.modules.tasks.tasks_module import _resolve_catalog_schema
        schema = await _resolve_catalog_schema(catalog_id, conn)
        return await tasks_module.list_tasks(conn, schema=schema, limit=limit, offset=offset)

    @router.get("/catalogs/{catalog_id}/{task_id}", response_model=Task, summary="Get the status of a specific task")
    async def get_task_status_catalog(
        catalog_id: str,  # type: ignore[reportGeneralTypeIssues]
        task_id: uuid.UUID,
        conn: AsyncConnection = Depends(get_async_connection),
    ):
        """Fetches the complete record for a single task within a catalog."""
        from dynastore.modules.tasks.tasks_module import _resolve_catalog_schema
        schema = await _resolve_catalog_schema(catalog_id, conn)
        task = await tasks_module.get_task(conn, task_id, schema=schema)
        if not task:
            raise HTTPException(status_code=404, detail=f"Task with ID '{task_id}' not found in catalog '{catalog_id}'.")
        return task

    # Global fallback for system tasks (can be moved to a separate extension if needed)
    @router.get("", response_model=List[Task], summary="List all system tasks", include_in_schema=False)
    async def list_tasks_system(
        conn: AsyncConnection = Depends(get_async_connection),  # type: ignore[reportGeneralTypeIssues]
        limit: int = Query(20, ge=1, le=100),
        offset: int = Query(0, ge=0)
    ):
        return await tasks_module.list_tasks(conn, schema="public", limit=limit, offset=offset)