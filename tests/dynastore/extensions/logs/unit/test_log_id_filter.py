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

"""The ``?log_id=`` filter on the logs read endpoints, and the LogEntry
id-type regression: backend ids are Elasticsearch document id *strings*
("_system_:exception:<iso>"); typing ``LogEntry.id`` as int made every ES
row fail validation, which ``search_logs``' catch-all turned into a silent
empty list on all /logs read endpoints."""
from __future__ import annotations

from unittest.mock import AsyncMock, patch

import pytest

from dynastore.extensions.logs.log_extension import LogExtension
from dynastore.extensions.logs.models import LogEntry

ES_ROW = {
    "id": "_system_:exception:2026-07-04T11:42:56.794107+00:00",
    "catalog_id": "_system_",
    "collection_id": None,
    "event_type": "exception",
    "level": "ERROR",
    "is_system": True,
    "message": "Error during GET /x",
    "timestamp": "2026-07-04T11:42:56.794107+00:00",
    "details": None,
}


def test_log_entry_accepts_elasticsearch_string_id() -> None:
    entry = LogEntry.model_validate(ES_ROW)
    assert entry.id == ES_ROW["id"]
    assert entry.level == "ERROR"


@pytest.mark.asyncio
async def test_system_logs_log_id_filter_returns_single_entry() -> None:
    service = AsyncMock()
    service.get_log_by_id.return_value = dict(ES_ROW)
    ext = LogExtension()
    with patch(
        "dynastore.extensions.logs.log_extension.get_protocol",
        return_value=service,
    ):
        response = await ext.get_system_logs(log_id=ES_ROW["id"])

    service.get_log_by_id.assert_awaited_once_with(ES_ROW["id"], "_system_")
    assert [e.id for e in response.logs] == [ES_ROW["id"]]


@pytest.mark.asyncio
async def test_catalog_logs_log_id_filter_scopes_to_catalog() -> None:
    service = AsyncMock()
    # LogService.get_log_by_id returns None for entries of other catalogs.
    service.get_log_by_id.return_value = None
    ext = LogExtension()
    with patch(
        "dynastore.extensions.logs.log_extension.get_protocol",
        return_value=service,
    ):
        response = await ext.get_catalog_logs("cat_123", log_id=ES_ROW["id"])

    service.get_log_by_id.assert_awaited_once_with(ES_ROW["id"], "cat_123")
    assert response.logs == []


@pytest.mark.asyncio
async def test_log_id_filter_degrades_when_no_logs_service() -> None:
    ext = LogExtension()
    with patch(
        "dynastore.extensions.logs.log_extension.get_protocol",
        return_value=None,
    ):
        response = await ext.get_system_logs(log_id=ES_ROW["id"])

    assert response.logs == []
