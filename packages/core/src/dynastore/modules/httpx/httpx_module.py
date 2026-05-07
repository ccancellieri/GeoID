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

# dynastore/modules/httpx/httpx_module.py

import logging
import httpx
from typing import Optional

logger = logging.getLogger(__name__)

MAX_HTTP_RETRIES = 3
INITIAL_BACKOFF_SECONDS = 2
HTTP_TIMEOUT_SECONDS = 10

def create_httpx_client() -> httpx.AsyncClient:
    """Creates a new httpx.AsyncClient with standard retry and timeout settings."""
    transport = httpx.AsyncHTTPTransport(
        retries=MAX_HTTP_RETRIES,
    )
    client = httpx.AsyncClient(
        transport=transport,
        timeout=HTTP_TIMEOUT_SECONDS
    )
    return client

def create_proxy_httpx_client() -> httpx.AsyncClient:
    """Creates a new httpx.AsyncClient for proxying with pooling disabled."""
    # Disable keep-alive connections to prevent SNI misdirection issues when proxying to various hosts.
    limits = httpx.Limits(max_keepalive_connections=0, max_connections=0)
    transport = httpx.AsyncHTTPTransport(
        retries=MAX_HTTP_RETRIES,
    )
    client = httpx.AsyncClient(
        transport=transport,
        timeout=HTTP_TIMEOUT_SECONDS,
        limits=limits
    )
    return client
