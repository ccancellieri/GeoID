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

"""#2749: log persistence is Elasticsearch-only now — no PG table, no PG
partition. This module does not enable the ``elasticsearch`` module, so
these tests exercise the degradation posture (same optional-module
contract as IAM): writes fall back to the stdlib logger, reads return an
empty result, and nothing raises. A live Elasticsearch write/read round
trip belongs in ``tests/dynastore/modules/elasticsearch/integration/``.
"""

import pytest
from dynastore.modules.catalog.log_manager import LogsProtocol
from dynastore.tools.discovery import get_protocol

pytestmark = [
    pytest.mark.asyncio,
    pytest.mark.enable_modules("db_config", "db", "catalog", "stats"),
]


async def test_log_event_without_backend_degrades_to_stdlib(caplog, app_lifespan_module):
    """No LogBackendProtocol is registered in this module set — log_event
    must fall back to the stdlib logger instead of raising, and must not
    return a fabricated id."""
    logs_service = get_protocol(LogsProtocol)
    assert logs_service is not None

    import logging

    with caplog.at_level(logging.INFO, logger="dynastore.modules.catalog.log_manager"):
        result = await logs_service.log_event(
            catalog_id="_system_",
            event_type="test_event",
            level="INFO",
            message="Verification test message",
            immediate=True,
        )

    assert result is None
    assert any(
        "Verification test message" in r.message for r in caplog.records
    ), "log_event must fall back to the stdlib logger when no backend is registered"


async def test_list_logs_without_backend_returns_empty(app_lifespan_module):
    """No backend registered -> list_logs degrades to [] rather than raising."""
    logs_service = get_protocol(LogsProtocol)
    assert logs_service is not None

    results = await logs_service.list_logs(catalog_id="_system_")
    assert results == []


async def test_get_log_by_id_without_backend_returns_none(app_lifespan_module):
    """No backend registered -> get_log_by_id degrades to None rather than raising."""
    logs_service = get_protocol(LogsProtocol)
    assert logs_service is not None

    result = await logs_service.get_log_by_id("does-not-exist", catalog_id="_system_")
    assert result is None
