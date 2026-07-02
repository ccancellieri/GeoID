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

"""#2749: LOG_FLUSH_THRESHOLD / LOG_FLUSH_INTERVAL env vars are replaced by
``LogServiceConfig`` (behavior-via-env-var is disallowed project-wide).
Pins that ``LogService`` sources its aggregator's threshold, interval, and
buffer cap from the config object, and that no env var is read anymore.
"""

from __future__ import annotations

import inspect
from unittest.mock import AsyncMock, patch

import pytest

from dynastore.modules.catalog import log_manager
from dynastore.modules.catalog.log_service_config import LogServiceConfig


@pytest.mark.asyncio
async def test_build_aggregator_uses_log_service_config():
    cfg = LogServiceConfig(flush_threshold=7, flush_interval_seconds=1.5, buffer_max_size=42)

    service = log_manager.LogService()
    with patch(
        "dynastore.modules.catalog.log_service_config.load",
        new=AsyncMock(return_value=cfg),
    ):
        aggregator = await service._build_aggregator()

    assert aggregator._threshold == 7
    assert aggregator._interval == 1.5
    assert aggregator._max_size == 42


def test_log_manager_source_has_no_env_var_reads():
    """No LOG_FLUSH_THRESHOLD / LOG_FLUSH_INTERVAL env var reads remain —
    those values must come from LogServiceConfig (project rule: no env
    vars for behavior)."""
    source = inspect.getsource(log_manager)
    assert "LOG_FLUSH_THRESHOLD" not in source
    assert "LOG_FLUSH_INTERVAL" not in source
    assert "os.environ" not in source
