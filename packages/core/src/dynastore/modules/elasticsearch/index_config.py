#    Copyright 2026 FAO
#
#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at
#
#        http://www.apache.org/licenses/LICENSE-2.0

"""Platform-tier PluginConfig for Elasticsearch index-level settings.

Holds the operator-tunable knobs that previously lived as env vars under
issue #489's first cut. Routed through the standard PluginConfig waterfall
so the values are visible / editable through `/configs/plugins/...` and
seedable via `${DYNASTORE_CONFIG_ROOT}/defaults/`.
"""
from __future__ import annotations

import logging
from typing import Any, ClassVar, Dict, Tuple

from pydantic import Field

from dynastore.models.mutability import Mutable
from dynastore.modules.db_config.plugin_config import PluginConfig

logger = logging.getLogger(__name__)


class ElasticsearchIndexConfig(PluginConfig):
    """Per-index Elasticsearch settings applied at `indices.create` time.

    Each field maps to `index.mapping.total_fields.limit` on the
    corresponding physical index. ES defaults this to 1000; once STAC
    extensions + multilingual metadata + per-catalog tenant attributes
    combine, re-ingestion hits the ceiling and returns 400 until the
    index is dropped and recreated. The ceilings here are deliberately
    generous — the limit is checked at mapping-update time only, not
    per-doc, so a larger ceiling carries no per-request cost.

    Live edits via `PUT /configs/plugins/elasticsearch_index_config` only
    affect indexes created *after* the write — ES does not expose a way
    to lower a previously-set `total_fields.limit`, and raising it on an
    existing index would need a separate `put_settings` apply-handler
    (not implemented; not required for the #489 scenario).
    """
    _address: ClassVar[Tuple[str, ...]] = ("platform", "elasticsearch", "indexes")

    items_total_fields_limit: Mutable[int] = Field(
        default=2000,
        ge=1000,
        le=100000,
        description=(
            "`index.mapping.total_fields.limit` for the public per-catalog "
            "items index (`{prefix}-{catalog}-items`). Default 2000 absorbs "
            "~8 locales × ~2 text fields × current STAC extension surface "
            "without touching memory cost. Raise for catalogs with very "
            "heavy extension fan-out."
        ),
    )

    assets_total_fields_limit: Mutable[int] = Field(
        default=1500,
        ge=1000,
        le=100000,
        description=(
            "`index.mapping.total_fields.limit` for the per-catalog assets "
            "index (`{prefix}-{catalog}-assets`). Default 1500 leaves "
            "headroom for extension-tagged metadata without matching the "
            "items ceiling."
        ),
    )

    private_items_total_fields_limit: Mutable[int] = Field(
        default=1500,
        ge=1000,
        le=100000,
        description=(
            "`index.mapping.total_fields.limit` for the per-catalog private "
            "items index (`{prefix}-{catalog}-private-items`). The private "
            "index uses `dynamic: false` at root so only the `properties` "
            "subtree grows; 1500 is tighter than the public items ceiling "
            "but still absorbs realistic tenant-attribute fan-out."
        ),
    )


# Auto-registers via PluginConfig.__init_subclass__.


async def _load() -> ElasticsearchIndexConfig:
    """Fetch the live config; fall back to defaults if unavailable.

    Falling back instead of raising preserves the previous env-var
    behaviour at the index-create call sites: a missing config layer
    (cold boot, unit test, platform manager not yet registered) yields
    safe defaults rather than crashing index provisioning.
    """
    from dynastore.models.protocols.platform_configs import (
        PlatformConfigsProtocol,
    )
    from dynastore.tools.discovery import get_protocol

    mgr = get_protocol(PlatformConfigsProtocol)
    if mgr is None:
        return ElasticsearchIndexConfig()
    try:
        cfg = await mgr.get_config(ElasticsearchIndexConfig)
    except Exception as exc:
        logger.debug(
            "ElasticsearchIndexConfig: get_config failed (%s); using defaults",
            exc,
        )
        return ElasticsearchIndexConfig()
    if isinstance(cfg, ElasticsearchIndexConfig):
        return cfg
    return ElasticsearchIndexConfig()


async def get_items_index_settings() -> Dict[str, Any]:
    """Settings dict for the public per-catalog items index."""
    cfg = await _load()
    return {"index.mapping.total_fields.limit": cfg.items_total_fields_limit}


async def get_assets_index_settings() -> Dict[str, Any]:
    """Settings dict for the per-catalog assets index."""
    cfg = await _load()
    return {"index.mapping.total_fields.limit": cfg.assets_total_fields_limit}


async def get_private_items_index_settings() -> Dict[str, Any]:
    """Settings dict for the per-catalog private items index."""
    cfg = await _load()
    return {"index.mapping.total_fields.limit": cfg.private_items_total_fields_limit}
