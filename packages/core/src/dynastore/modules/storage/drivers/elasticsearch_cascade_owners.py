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

"""Retired: public ES items and asset cascade owners.

These owners have been superseded by
:class:`~dynastore.modules.storage.drivers.routing_driven_cascade_owner.RoutingDrivenCascadeOwner`
which enumerates configured drivers at delete time and delegates to each
driver's ``drop_storage`` implementation.

Drop-storage parity:
- ``EsItemsPublicIndexOwner`` (was: indices.delete) — now covered by
  ``ItemsElasticsearchDriver.drop_storage`` which removes the public alias
  membership and deletes the per-catalog index (catalog scope) or
  delete_by_query on collection_id (collection scope).
- ``EsAssetIndexOwner`` (was: indices.delete) — now covered by
  ``AssetElasticsearchDriver.drop_storage`` which deletes the per-catalog
  assets index.

This module is retained as an empty stub so that any third-party code that
imports it does not receive an ``ImportError``.  The ``register_owners``
function is a no-op.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from dynastore.modules.catalog.cascade_registry import CascadeCleanupRegistry

logger = logging.getLogger(__name__)


def register_owners(registry: "CascadeCleanupRegistry") -> None:
    """No-op — owners retired; superseded by RoutingDrivenCascadeOwner."""
    logger.debug(
        "elasticsearch_cascade_owners.register_owners: retired stub called — "
        "no owners registered (superseded by routing_driven_cascade_owner)."
    )
