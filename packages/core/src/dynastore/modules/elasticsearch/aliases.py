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

"""
Platform-level Elasticsearch alias helpers.

Owns only the platform-wide regular items alias
(``{prefix}-items-public``) — the alias spanning every per-tenant regular
items index. OGC discovery search routes (STAC ``/search``,
``/collections-search``, etc.) read through this alias.

Membership is managed by the regular items driver:

- On ``ItemsElasticsearchDriver.ensure_storage(catalog_id)`` →
  ``add_index_to_public_alias(<tenant index>)``.
- On routing-config apply that removes the regular driver from a
  collection's routing → ``remove_index_from_public_alias(<tenant index>)``.

This module **never** references private indexing-specific concepts. Drivers
that maintain their own internal aliases (e.g. the private driver)
manage them in their own subpackage.
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)


def _get_client() -> Optional[Any]:
    """Return the singleton ES client, or None when not initialised yet."""
    from dynastore.modules.elasticsearch.client import get_client

    return get_client()


def _get_prefix() -> str:
    """Return the deployment-wide ES index prefix (default ``dynastore``)."""
    from dynastore.modules.elasticsearch.client import get_index_prefix

    return get_index_prefix()


async def ensure_public_alias_exists() -> None:
    """Idempotently create the empty platform alias ``{prefix}-items-public``.

    No-op if the alias already exists. Called from
    :meth:`ElasticsearchModule.lifespan` so the alias exists from the moment
    the platform starts — readers can target it without 404s, even before
    any per-tenant index has been created.

    OpenSearch / Elasticsearch require an alias to point at at least one
    concrete index. To create an empty alias we use the ``aliases`` API
    with no ``add`` actions when the alias doesn't yet exist; we instead
    rely on the FIRST :func:`add_index_to_public_alias` call to create the
    alias at the same time as adding the first member. This function is a
    no-op until that point.

    The function is named ``ensure_public_alias_exists`` for symmetry with
    the index-create lifecycle even though, on a fresh cluster, the alias
    only materialises with its first member. Subsequent calls are
    idempotent.
    """
    es = _get_client()
    if es is None:
        logger.debug("aliases.ensure_public_alias_exists: ES client not "
                     "initialised; skipping")
        return

    from dynastore.modules.elasticsearch.mappings import get_public_items_alias

    alias = get_public_items_alias(_get_prefix())
    try:
        exists = await es.indices.exists_alias(name=alias)
    except Exception as exc:
        logger.debug(
            "aliases.ensure_public_alias_exists: exists_alias check failed for "
            "%s: %s",
            alias, exc,
        )
        return

    if exists:
        logger.debug("aliases.ensure_public_alias_exists: %s already exists",
                     alias)
        return

    # Alias only materialises when it has at least one member. The first
    # add_index_to_public_alias() call will create it. This is intentional —
    # an empty alias is not a stable concept in ES.
    logger.info(
        "aliases.ensure_public_alias_exists: %s will be created on first "
        "tenant-index add (ES does not support empty aliases)", alias,
    )


async def add_index_to_public_alias(index_name: str) -> None:
    """Idempotently add ``index_name`` to the platform public items alias.

    Called by the regular items driver's ``ensure_storage`` after creating
    a per-tenant index. Safe to call multiple times — a second call is a
    no-op.
    """
    es = _get_client()
    if es is None:
        logger.warning(
            "aliases.add_index_to_public_alias: ES client not initialised; "
            "cannot add %s to public alias", index_name,
        )
        return

    from dynastore.modules.elasticsearch.mappings import get_public_items_alias

    alias = get_public_items_alias(_get_prefix())
    body: Dict[str, List[Dict[str, Any]]] = {
        "actions": [{"add": {"index": index_name, "alias": alias}}]
    }
    try:
        await es.indices.update_aliases(body=body)
        logger.info(
            "aliases.add_index_to_public_alias: added %s to %s",
            index_name, alias,
        )
    except Exception as exc:
        logger.warning(
            "aliases.add_index_to_public_alias: failed to add %s to %s: %s",
            index_name, alias, exc,
        )


async def remove_index_from_public_alias(index_name: str) -> None:
    """Idempotently remove ``index_name`` from the platform public items
    alias.

    Called when the regular items driver is removed from a collection's
    routing config (so that collection's data is no longer discoverable
    via the public alias). The underlying tenant index is NOT deleted —
    only its membership in the alias.
    """
    es = _get_client()
    if es is None:
        logger.warning(
            "aliases.remove_index_from_public_alias: ES client not "
            "initialised; cannot remove %s from public alias", index_name,
        )
        return

    from dynastore.modules.elasticsearch.mappings import get_public_items_alias

    alias = get_public_items_alias(_get_prefix())
    body: Dict[str, List[Dict[str, Any]]] = {
        "actions": [{"remove": {"index": index_name, "alias": alias}}]
    }
    try:
        await es.indices.update_aliases(body=body)
        logger.info(
            "aliases.remove_index_from_public_alias: removed %s from %s",
            index_name, alias,
        )
    except Exception as exc:
        logger.debug(
            "aliases.remove_index_from_public_alias: %s removal from %s "
            "failed (may not have been a member): %s",
            index_name, alias, exc,
        )
