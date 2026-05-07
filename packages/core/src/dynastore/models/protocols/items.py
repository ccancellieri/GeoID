#    Copyright 2025 FAO
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

"""
Composite ItemsProtocol — backward-compatible union of the focused sub-protocols.

New code should import only the sub-protocol it actually needs:

  * ``ItemCrudProtocol``          — upsert / get_item / delete
  * ``ItemQueryProtocol``         — search_items / stream_items / get_features_query
  * ``ItemIntrospectionProtocol`` — get_collection_fields / get_collection_schema /
                                    map_row_to_feature

``ItemsProtocol`` is kept as a composite alias so that existing
``get_protocol(ItemsProtocol)`` call-sites continue to work without change.
"""

from typing import Protocol, runtime_checkable

from dynastore.models.protocols.item_crud import ItemCrudProtocol
from dynastore.models.protocols.item_query import ItemQueryProtocol
from dynastore.models.protocols.item_introspection import ItemIntrospectionProtocol

__all__ = [
    "ItemCrudProtocol",
    "ItemQueryProtocol",
    "ItemIntrospectionProtocol",
    "ItemsProtocol",
]


@runtime_checkable
class ItemsProtocol(ItemCrudProtocol, ItemQueryProtocol, ItemIntrospectionProtocol, Protocol):
    """
    Composite protocol covering all item operations.

    Prefer importing one of the focused sub-protocols when only a subset of
    operations is needed — it reduces coupling and makes dependencies explicit.
    This class exists solely for backward compatibility.
    """
