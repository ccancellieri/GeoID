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
from contextlib import asynccontextmanager
from dynastore.extensions import ExtensionProtocol
from fastapi import Request, FastAPI
from typing import Optional


logger = logging.getLogger(__name__)

MAX_HTTP_RETRIES=3
INITIAL_BACKOFF_SECONDS=2
HTTP_TIMEOUT_SECONDS=10

from dynastore.modules.httpx.httpx_module import (
    create_httpx_client as _create_httpx_client,
    create_proxy_httpx_client as _create_proxy_httpx_client
)

from dynastore.models.protocols import HttpxProtocol
class HttpxExtension(ExtensionProtocol, HttpxProtocol):
    priority: int = 100
    """
    A foundational extension to manage the lifecycle of a shared httpx.AsyncClient.
    This shared, singleton client is attached to the app_state and is used
    across the entire application.
    """
    # Explicitly declare the public 'client' attribute and the optional 'router'.
    # This makes the class fully compliant with the ExtensionProtocol and clarifies its public API.
    client: httpx.AsyncClient = None  # type: ignore[assignment]
    proxy_client: httpx.AsyncClient = None  # type: ignore[assignment]
    router = None
    app: FastAPI = None  # type: ignore[assignment]

    @asynccontextmanager
    async def lifespan(self, app: FastAPI):
        logger.info("HttpxExtension: Creating shared httpx clients.")
        self.app = app
        
        # Standard client with connection pooling
        self.client = _create_httpx_client()
        self.app.state.httpx_client = self.client

        # Special client for proxying with connection pooling disabled
        self.proxy_client = _create_proxy_httpx_client()
        self.app.state.proxy_httpx_client = self.proxy_client

        try:
            yield
        finally:
            if self.client and not self.client.is_closed:
                await self.client.aclose()
            if self.proxy_client and not self.proxy_client.is_closed:
                await self.proxy_client.aclose()
            logger.info("HttpxExtension: Shared httpx clients closed.")

    def get_httpx_client(self) -> httpx.AsyncClient:
        if not self.client or self.client.is_closed:
            logger.warning("The shared httpx.AsyncClient was not available or closed. Creating a new instance.")
            client = _create_httpx_client()
            self.client = client
            self.app.state.httpx_client = self.client
        return self.client

    def get_proxy_httpx_client(self) -> httpx.AsyncClient:
        if not self.proxy_client or self.proxy_client.is_closed:
            logger.warning("The shared proxy httpx.AsyncClient was not available or closed. Creating a new instance.")
            client = _create_proxy_httpx_client()
            self.proxy_client = client
            self.app.state.proxy_httpx_client = self.proxy_client
        return self.proxy_client

from dynastore.tools.discovery import get_protocol

def get_client() -> Optional[httpx.AsyncClient]:
    extension = get_protocol(HttpxProtocol)
    if extension:
        return extension.get_httpx_client()

async def get_httpx_client(request: Request) -> httpx.AsyncClient:
    """
    A FastAPI dependency that provides the shared, singleton httpx.AsyncClient.

    It retrieves the client instance managed by the HttpxModule's lifespan.
    As a robustness measure, if the client is not found on the app state or is
    closed, it will be created (or re-created) to ensure the application
    remains operational.
    """
    client: httpx.AsyncClient | None = getattr(request.app.state, "httpx_client", None)

    if not client or client.is_closed:
        logger.warning("The shared httpx.AsyncClient was not available or closed. Creating a new instance.")
        client = _create_httpx_client()
        request.app.state.httpx_client = client

    return client

async def get_proxy_httpx_client(request: Request) -> httpx.AsyncClient:
    """
    A FastAPI dependency that provides the shared, non-pooling httpx.AsyncClient
    for proxying requests.

    This client is configured to not reuse connections, which is essential for a
    proxy that connects to many different external hosts, avoiding SNI issues.
    """
    client: httpx.AsyncClient | None = getattr(request.app.state, "proxy_httpx_client", None)

    if not client or client.is_closed:
        logger.warning("The shared proxy httpx.AsyncClient was not available or closed. Creating a new instance.")
        client = _create_proxy_httpx_client()
        request.app.state.proxy_httpx_client = client

    return client
