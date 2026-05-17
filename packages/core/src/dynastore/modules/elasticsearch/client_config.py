#    Copyright 2026 FAO
#
#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at
#
#        http://www.apache.org/licenses/LICENSE-2.0

"""Platform-tier PluginConfig for the AsyncOpenSearch transport.

Holds the operator-tunable transport knobs that previously lived as
`ES_REQUEST_TIMEOUT` / `ES_CONNECTIONS_PER_NODE` env vars. Connection
*targets* (host, port, credentials, ssl) remain env-driven — they are
deployment-shape secrets, not operator-tunable runtime values.

The values are read once at lifespan startup and passed to
``AsyncOpenSearch(**kwargs)``; opensearch-py 3.1.0 honours the
client-level ``timeout`` on every request, including ``bulk()``. Live
edits via ``PUT /configs/plugins/elasticsearch_client_config`` only
take effect after the next module lifespan (the transport pool is built
once and cannot be rewired without dropping in-flight connections).
"""
from __future__ import annotations

import logging
from typing import ClassVar, Tuple

from pydantic import Field

from dynastore.models.mutability import Mutable
from dynastore.modules.db_config.plugin_config import PluginConfig

logger = logging.getLogger(__name__)


class ElasticsearchClientConfig(PluginConfig):
    """Per-process transport settings for the shared AsyncOpenSearch client."""

    _address: ClassVar[Tuple[str, ...]] = ("platform", "elasticsearch", "client")

    request_timeout_seconds: Mutable[int] = Field(
        default=60,
        ge=1,
        le=600,
        description=(
            "Per-request timeout (seconds) applied at client construction. "
            "Tenant bulk ingestion (`elasticsearch_private/driver.py:index_bulk`, "
            "`bulk_reindex.py`) historically needed >30s for >5k items; "
            "60s is the conservative budget that survives review-env loads "
            "without masking genuine cluster hangs."
        ),
    )

    connections_per_node: Mutable[int] = Field(
        default=10,
        ge=1,
        le=200,
        description=(
            "AsyncOpenSearch transport pool size per node (`maxsize`). 10 "
            "matches the historical default; raise for high-fanout async "
            "workloads (e.g. parallel bulk ingest of many indexes)."
        ),
    )

    max_retries: Mutable[int] = Field(
        default=3,
        ge=0,
        le=10,
        description="Transport-level retry attempts for transient errors.",
    )


# Auto-registers via PluginConfig.__init_subclass__.


async def load() -> ElasticsearchClientConfig:
    """Fetch the live config; fall back to defaults if unavailable.

    Falling back instead of raising preserves a sane startup path: a
    missing config layer (cold boot, unit test, platform manager not yet
    registered) yields safe defaults rather than crashing client init.
    """
    from dynastore.models.protocols.platform_configs import (
        PlatformConfigsProtocol,
    )
    from dynastore.tools.discovery import get_protocol

    mgr = get_protocol(PlatformConfigsProtocol)
    if mgr is None:
        return ElasticsearchClientConfig()
    try:
        cfg = await mgr.get_config(ElasticsearchClientConfig)
    except Exception as exc:
        logger.debug(
            "ElasticsearchClientConfig: get_config failed (%s); using defaults",
            exc,
        )
        return ElasticsearchClientConfig()
    if isinstance(cfg, ElasticsearchClientConfig):
        return cfg
    return ElasticsearchClientConfig()
