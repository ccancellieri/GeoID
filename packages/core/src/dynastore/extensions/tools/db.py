from typing import AsyncGenerator


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

from sqlalchemy.ext.asyncio import AsyncEngine, AsyncConnection
from fastapi import Request

from dynastore.tools.discovery import get_protocol
from dynastore.models.protocols import DatabaseProtocol


def get_async_engine(request: Request) -> AsyncEngine:
    """
    Gets the asynchronous engine.
    Prioritizes request.app.state.engine (standard FastApi/Starlette pattern).
    Fallbacks to DatabaseProtocol discovery.
    """
    if hasattr(request.app.state, "engine") and request.app.state.engine:
        return request.app.state.engine

    # Fallback to Protocol
    db_service = get_protocol(DatabaseProtocol)
    if db_service and db_service.engine:
        return db_service.engine  # type: ignore[return-value]

    raise RuntimeError(
        "Database service not available. The async engine is not initialized."
    )


async def get_async_connection(
    request: Request,
) -> AsyncGenerator[AsyncConnection, None]:
    """
    FastAPI dependency that provides a transaction-managed asynchronous `Connection`
    to API endpoints. This is the correct pattern for endpoint dependencies.
    """
    engine = get_async_engine(request)

    # .begin() starts a transaction and handles commit/rollback automatically.
    async with engine.begin() as conn:
        yield conn
