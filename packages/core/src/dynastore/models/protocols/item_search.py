"""Capability protocol — structural item search.

Backend-agnostic single-method contract for the STAC ``/search`` fast path
(and any future consumer that needs a ``filter``-free item lookup). The
protocol takes plain kwargs + returns a plain dataclass so that
``extensions/stac/search.py`` can dispatch to whichever items driver a
catalog routes ``Operation.SEARCH`` to — public Elasticsearch, the
tenant-scoped private ES index, or any future search-capable backend —
without importing search-extension models or service classes.

This is an **instance-level driver capability**: items storage drivers that
serve filtered queries implement it (see ``_ElasticsearchBase``), and the
STAC dispatch resolves the driver via ``router.get_items_search_driver`` then
``isinstance``-checks it against this protocol (#989). It also closes the
architectural debt called out in #234 / #231 (the two
``stac/search.py → extensions/search/...`` cross-extension waivers).
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Protocol, runtime_checkable


@dataclass
class ItemSearchResult:
    """Backend-agnostic shape returned by ``ItemSearchProtocol.search_items_struct``.

    ``features`` mirrors STAC ``FeatureCollection.features`` (each entry
    is a STAC Item dict). ``total`` mirrors ``numberMatched``.
    """

    features: List[Dict[str, Any]] = field(default_factory=list)
    total: int = 0


@runtime_checkable
class ItemSearchProtocol(Protocol):
    """Structural-only item search (no CQL2 filter), used by the STAC
    ``/search`` ES fast path. Implementations decide their backend; the
    consumer only sees the kwargs+result contract.
    """

    async def search_items_struct(
        self,
        *,
        catalog_id: Optional[str],
        collections: Optional[List[str]],
        ids: Optional[List[str]],
        bbox: Optional[List[float]],
        intersects: Optional[Dict[str, Any]],
        datetime: Optional[str],
        limit: int,
        offset: int = 0,
    ) -> ItemSearchResult:
        ...
