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
Private Elasticsearch storage driver subpackage — items-tier only.

Private indexing applies to items only. Catalog and collection envelopes
are PG-only for private catalogs — no ES private index at those tiers.

Exports the items driver class plus its companion entity-transformer. The
driver and the transformer are SEPARATE classes registered separately
in routing-config so the transformer can be reused with non-ES indexers
in the future.

Disabling the private-indexing feature in a deployment = removing the
``[private]`` extras group from SCOPE; nothing in the platform layer
needs to change.
"""

from dynastore.modules.storage.drivers.elasticsearch_private.driver import (
    ItemsElasticsearchPrivateDriver,
)
from dynastore.modules.storage.drivers.elasticsearch_private.transformer import (
    PrivateEntityTransformer,
)

__all__ = [
    "ItemsElasticsearchPrivateDriver",
    "PrivateEntityTransformer",
]
