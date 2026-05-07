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
Item query protocol — search, streaming, and SQL generation for features.

All methods use ``QueryRequest`` as the primary parameter model so that
the ``QueryOptimizer`` can selectively JOIN only the sidecars required for
each request.  The legacy named-parameter overloads have been removed.
"""

from typing import (
    Protocol,
    Optional,
    Any,
    List,
    Dict,
    Tuple,
    runtime_checkable,
)

from typing import TYPE_CHECKING

from dynastore.models.ogc import Feature
from dynastore.models.query_builder import QueryRequest, QueryResponse
from dynastore.models.protocols.configs import ConfigsProtocol

if TYPE_CHECKING:
    from dynastore.modules.storage.drivers.pg_sidecars.base import ConsumerType
    from dynastore.models.driver_context import DriverContext  # noqa: F401


@runtime_checkable
class ItemQueryProtocol(Protocol):
    """
    Protocol for item search and streaming operations.

    All callers must build a ``QueryRequest`` before calling these methods.
    The implementation delegates SQL generation to ``QueryOptimizer``, which
    selectively JOINs only the sidecars required for the given request.
    """

    async def search_items(
        self,
        catalog_id: str,
        collection_id: str,
        request: QueryRequest,
        config: Optional[ConfigsProtocol] = None,
        ctx: Optional["DriverContext"] = None,
    ) -> List[Feature]:
        """
        Search and return a list of features matching the request.

        Internally uses ``QueryOptimizer.build_optimized_query`` so only
        the sidecars required for the SELECT / WHERE clauses are joined.
        """
        ...

    async def stream_items(
        self,
        catalog_id: str,
        collection_id: str,
        request: QueryRequest,
        config: Optional[ConfigsProtocol] = None,
        ctx: Optional["DriverContext"] = None,
        consumer: "Optional[ConsumerType]" = None,
    ) -> QueryResponse:
        """
        Stream features as an async iterator with O(1) memory footprint.

        Returns a ``QueryResponse`` whose ``.items`` is an ``AsyncIterator[Feature]``
        and whose ``.total_count`` is populated when ``request.include_total_count``
        is ``True``.

        The ``consumer`` parameter controls which sidecar fields are injected
        into each Feature (e.g. STAC fields are skipped for OGC Features).
        """
        ...

    async def get_features(
        self,
        conn: Any,
        catalog_id: str,
        collection_id: str,
        col_config: Optional[Any] = None,
        item_ids: Optional[List[str]] = None,
        request: Optional[QueryRequest] = None,
        **kwargs: Any,
    ) -> List[Feature]:
        """
        Retrieve a list of features via the QueryOptimizer path.

        ``item_ids`` and any extra ``kwargs`` are merged into a ``QueryRequest``
        if one is not provided by the caller.
        """
        ...

    async def get_features_query(
        self,
        conn: Any,
        catalog_id: str,
        collection_id: str,
        col_config: Any,
        params: Dict[str, Any],
        param_suffix: str = "",
    ) -> Tuple[str, Dict[str, Any]]:
        """
        Return the raw SQL string and bind parameters for a feature query.

        Used by the Tiles/MVT extension to embed the feature SQL as a
        sub-query inside a larger ST_AsMVT() expression.  Callers are
        responsible for executing the returned SQL.
        """
        ...
