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

"""``log_reference.url`` on 5xx responses must be a resolvable API URL.

The previous shape (``web/#/logs?catalog=…&log_id=…``) pointed at a web
route that never existed, and interpolated the log id unencoded — ids
embed an ISO timestamp whose ``+00:00`` suffix decodes to a space in a
query string, so the id could never round-trip. The handler now emits the
logs API's ``?log_id=`` filter with the id percent-encoded.
"""
from __future__ import annotations

import json
from unittest.mock import AsyncMock, patch

import pytest
from starlette.requests import Request

from dynastore.extensions.tools.exception_handlers import generic_exception_handler

LOG_ID_SYSTEM = "_system_:exception:2026-07-04T11:42:56.794107+00:00"
LOG_ID_CATALOG = "cat_123:exception:2026-07-04T11:42:56.794107+00:00"


def _request(path: str, root_path: str = "/geospatial/dev/api/catalog") -> Request:
    return Request(
        {
            "type": "http",
            "method": "GET",
            "scheme": "http",
            "server": ("testserver", 80),
            "root_path": root_path,
            "path": path,
            "raw_path": path.encode(),
            "query_string": b"",
            "headers": [],
        }
    )


async def _invoke(path: str, log_id: str) -> dict:
    logs_service = AsyncMock()
    logs_service.log_event.return_value = log_id
    with patch(
        "dynastore.tools.discovery.get_protocol", return_value=logs_service
    ):
        response = await generic_exception_handler(
            _request(path), RuntimeError("boom")
        )
    assert response.status_code == 500
    return json.loads(bytes(response.body))


@pytest.mark.asyncio
async def test_system_log_reference_points_at_logs_api_encoded() -> None:
    body = await _invoke("/region-mappings/x/regionIds", LOG_ID_SYSTEM)

    ref = body["log_reference"]
    assert ref["log_id"] == LOG_ID_SYSTEM
    assert ref["url"] == (
        "/geospatial/dev/api/catalog/logs/system?log_id="
        "_system_%3Aexception%3A2026-07-04T11%3A42%3A56.794107%2B00%3A00"
    )
    # The raw "+" must never appear in the query value — it would decode
    # to a space and the id would never match.
    assert "+" not in ref["url"].split("log_id=")[1]


@pytest.mark.asyncio
async def test_catalog_log_reference_uses_per_catalog_logs_path() -> None:
    body = await _invoke("/catalogs/cat_123/collections/c1/items", LOG_ID_CATALOG)

    ref = body["log_reference"]
    assert ref["catalog_id"] == "cat_123"
    assert ref["url"].startswith(
        "/geospatial/dev/api/catalog/logs/catalogs/cat_123?log_id="
    )
    assert "+" not in ref["url"].split("log_id=")[1]
    assert "#" not in ref["url"]
