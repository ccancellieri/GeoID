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
Search protocol definitions.

Abstracts search capabilities so that the search extension does not depend
on a specific backend (Elasticsearch, Solr, Meilisearch, etc.).
"""

from __future__ import annotations

from typing import Any, Dict, Optional, Protocol, TYPE_CHECKING, runtime_checkable

if TYPE_CHECKING:
    from dynastore.extensions.search.search_models import (
        CatalogSearchBody,
        GenericCollection,
        ItemCollection,
        SearchBody,
    )


@runtime_checkable
class SearchProtocol(Protocol):
    """
    Protocol for search operations over indexed entities.

    Implementations provide full-text, spatial, and temporal search across
    catalogs, collections, and items.  The search extension discovers this
    protocol at runtime and delegates all query execution to it.

    Implementors:
        - ``SearchService`` (backed by Elasticsearch) in
          ``extensions/search/search_service.py``.
    """

    async def search_items(
        self,
        body: SearchBody,
        base_url: str = "",
    ) -> ItemCollection:
        """
        Search STAC items.

        Args:
            body: Structured search request (q, bbox, datetime, intersects,
                  ids, collections, sortby, limit, token).
            base_url: Base URL for constructing pagination links.

        Returns:
            STAC ``ItemCollection`` with ``features``, ``links``,
            ``numberMatched``, ``numberReturned``.
        """
        ...

    async def search_catalogs(
        self,
        body: CatalogSearchBody,
        base_url: str = "",
    ) -> GenericCollection:
        """
        Search catalogs by keyword.

        Args:
            body: Structured search request (q, ids, limit, token).
            base_url: Base URL for constructing pagination links.

        Returns:
            ``GenericCollection`` with ``entities``, ``links``,
            ``numberReturned``.
        """
        ...

    async def search_collections(
        self,
        body: CatalogSearchBody,
        base_url: str = "",
    ) -> GenericCollection:
        """
        Search collections by keyword.

        Args:
            body: Structured search request (q, ids, limit, token).
            base_url: Base URL for constructing pagination links.

        Returns:
            ``GenericCollection`` with ``entities``, ``links``,
            ``numberReturned``.
        """
        ...

    async def reindex_catalog(
        self,
        catalog_id: str,
        driver: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Trigger a full catalog reindex.

        Args:
            catalog_id: The catalog to reindex.
                  from the catalog's indexer configuration.

        Returns:
            Dict with ``task_id``, ``catalog_id``, ``status``.
        """
        ...

    async def reindex_collection(
        self,
        catalog_id: str,
        collection_id: str,
        driver: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Trigger a single collection reindex.

        Args:
            catalog_id: The catalog owning the collection.
            collection_id: The collection to reindex.

        Returns:
            Dict with ``task_id``, ``catalog_id``, ``collection_id``,
            ``status``.
        """
        ...
