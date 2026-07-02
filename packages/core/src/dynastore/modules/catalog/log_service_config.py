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

"""Platform-tier PluginConfig for ``LogService`` (#2749).

Replaces the ``LOG_FLUSH_THRESHOLD`` / ``LOG_FLUSH_INTERVAL`` env vars —
behavior-via-env-var is disallowed; operator-tunable values belong in the
configs waterfall. ``buffer_max_size`` bounds the aggregator's in-memory
backlog (see ``AsyncBufferAggregator`` / ``LogService``): once exceeded, the
oldest buffered entries are dropped rather than growing memory without
bound while a slow backend catches up.

Like ``ElasticsearchClientConfig``, ``flush_threshold`` / ``flush_interval_
seconds`` are read once at ``LogService.lifespan()`` startup — the
aggregator is built once and cannot be rewired live, so a config edit only
takes effect on the next process start/lifespan. ``buffer_max_size`` IS
read hot (on every buffered ``add()``), so it is resolved through a short-
TTL cache instead.
"""
from __future__ import annotations

import logging
from typing import ClassVar, Tuple

from pydantic import Field

from dynastore.models.mutability import Mutable
from dynastore.models.plugin_config import PluginConfig

logger = logging.getLogger(__name__)


class LogServiceConfig(PluginConfig):
    """Buffering knobs for the process-wide log aggregator."""

    _address: ClassVar[Tuple[str, ...]] = ("platform", "catalog", "logs")

    flush_threshold: Mutable[int] = Field(
        default=50,
        ge=1,
        le=10_000,
        description=(
            "Number of buffered log entries that triggers an immediate "
            "flush to the backend, ahead of the interval timer."
        ),
    )

    flush_interval_seconds: Mutable[float] = Field(
        default=5.0,
        ge=0.1,
        le=300.0,
        description="Maximum time a log entry waits in the buffer before a flush is forced.",
    )

    buffer_max_size: Mutable[int] = Field(
        default=2000,
        ge=1,
        le=1_000_000,
        description=(
            "Hard cap on buffered-but-unflushed log entries. Once exceeded, "
            "the oldest entries are dropped (logs are ephemeral — losing "
            "some under sustained backend slowness is acceptable; unbounded "
            "memory growth is not)."
        ),
    )


# Auto-registers via PluginConfig.__init_subclass__.


async def load() -> LogServiceConfig:
    """Fetch the live config; fall back to defaults if unavailable.

    Falling back instead of raising preserves a sane startup path: a
    missing config layer (cold boot, unit test, platform manager not yet
    registered) yields safe defaults rather than crashing LogService init.
    """
    from dynastore.models.protocols.platform_configs import (
        PlatformConfigsProtocol,
    )
    from dynastore.tools.discovery import get_protocol

    mgr = get_protocol(PlatformConfigsProtocol)
    if mgr is None:
        return LogServiceConfig()
    try:
        cfg = await mgr.get_config(LogServiceConfig)
    except Exception as exc:
        logger.debug(
            "LogServiceConfig: get_config failed (%s); using defaults", exc,
        )
        return LogServiceConfig()
    if isinstance(cfg, LogServiceConfig):
        return cfg
    return LogServiceConfig()
