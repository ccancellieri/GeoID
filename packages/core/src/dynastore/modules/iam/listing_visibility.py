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

"""IAM implementation of ``ListingVisibilityProtocol``.

Answers "which catalog / collection ids may this caller LIST" with the same
engine that authorizes every request: a catalog (or collection) is visible
iff :meth:`PolicyService.compile_read_filter` does NOT compile to
``deny_everything`` for that scope. That single predicate covers, with zero
bespoke logic:

* platform admins — their unconditional ``.*`` ALLOW compiles to
  ``allow_all`` at platform scope (no per-catalog loop at all);
* granted principals — catalog/collection grants and policy bindings
  compile to an ALLOW for exactly their scopes;
* anonymous callers — public catalogs bind an anonymous ALLOW policy in
  their tenant schema, private catalogs compile to deny;
* deny-precedence, conditions and ABAC ``attribute_predicates`` — inherited
  from the compiler's own fail-closed rules (an uncompilable DENY blacks
  out the scope; uncompilable ALLOWs under-return).

The result is projected into the canonical listing shape
(``FieldPredicate("id", visible_ids)``) consumed by
:func:`dynastore.models.protocols.visibility.extract_id_constraint`.

Cost & caching: computing the catalog filter compiles once per catalog
(one tenant-policy resolution each); the collection filter compiles once
per collection of one catalog. Results are cached per caller
(:attr:`RequestVisibility.cache_key`) for ``_TTL_SECONDS``, mirroring the
membership cache: visibility changes (new grant, policy rebind) propagate
within a minute, which is the platform's standing staleness budget for
authorization metadata. Enumeration runs under ``visibility_bypass`` so the
provider's own ``list_catalogs`` / ``list_collections`` calls are not
themselves filtered (re-entrancy guard).
"""
from __future__ import annotations

import logging
import time
from typing import TYPE_CHECKING, Dict, List, Optional, Tuple

from dynastore.models.protocols.access_filter import (
    AccessClause,
    AccessFilter,
    FieldPredicate,
)
from dynastore.models.protocols.visibility import (
    RequestVisibility,
    visibility_bypass,
)

if TYPE_CHECKING:
    from dynastore.models.protocols.policies import PermissionProtocol

logger = logging.getLogger(__name__)

#: Filter staleness budget — matches the membership cache's TTL ballpark.
_TTL_SECONDS = 60.0
#: Per-pod cache ceiling; evicts expired-first, then oldest.
_CACHE_MAXSIZE = 512
#: Enumeration page size and hard ceiling (defensive; far above real counts).
_PAGE = 500
_MAX_ENTRIES = 10_000

_CacheKey = Tuple[str, ...]
_CacheValue = Tuple[float, AccessFilter]


def _ids_filter(visible: List[str]) -> AccessFilter:
    """Project a visible-id list into the canonical listing AccessFilter."""
    if not visible:
        return AccessFilter.deny_everything()
    return AccessFilter.from_clauses(
        [AccessClause((FieldPredicate("id", tuple(sorted(visible))),))]
    )


class IamListingVisibility:
    """Structurally satisfies ``ListingVisibilityProtocol`` — no inheritance.

    Registered via ``register_plugin`` in ``IamModule.lifespan`` so storage
    implementations resolve it through
    ``get_protocol(ListingVisibilityProtocol)`` without importing IAM.
    """

    def __init__(self, permissions: "PermissionProtocol") -> None:
        self._perms = permissions
        self._cache: Dict[_CacheKey, _CacheValue] = {}

    # ---- ListingVisibilityProtocol ----------------------------------- #

    async def catalog_listing_filter(
        self, visibility: RequestVisibility
    ) -> AccessFilter:
        key = ("catalog",) + visibility.cache_key
        cached = self._cache_get(key)
        if cached is not None:
            return cached

        try:
            flt = await self._compute_catalog_filter(visibility)
        except Exception:
            logger.warning(
                "catalog_listing_filter failed for principals=%s — denying",
                visibility.principals, exc_info=True,
            )
            return AccessFilter.deny_everything()
        self._cache_put(key, flt)
        return flt

    async def collection_listing_filter(
        self, visibility: RequestVisibility, catalog_id: str
    ) -> AccessFilter:
        key = ("collection", catalog_id) + visibility.cache_key
        cached = self._cache_get(key)
        if cached is not None:
            return cached

        try:
            flt = await self._compute_collection_filter(visibility, catalog_id)
        except Exception:
            logger.warning(
                "collection_listing_filter failed for catalog=%s principals=%s"
                " — denying", catalog_id, visibility.principals, exc_info=True,
            )
            return AccessFilter.deny_everything()
        self._cache_put(key, flt)
        return flt

    async def asset_listing_filter(
        self, visibility: RequestVisibility, catalog_id: str, collection_id: Optional[str]
    ) -> AccessFilter:
        col_key = collection_id or ""
        key = ("asset", catalog_id, col_key) + visibility.cache_key
        cached = self._cache_get(key)
        if cached is not None:
            return cached

        try:
            flt = await self._compute_asset_filter(visibility, catalog_id, collection_id)
        except Exception:
            logger.warning(
                "asset_listing_filter failed for catalog=%s collection=%s principals=%s"
                " — denying", catalog_id, collection_id, visibility.principals, exc_info=True,
            )
            return AccessFilter.deny_everything()
        self._cache_put(key, flt)
        return flt

    # ---- computation -------------------------------------------------- #

    async def _compile(
        self,
        visibility: RequestVisibility,
        catalog_id: Optional[str],
        collection_id: Optional[str] = None,
    ) -> AccessFilter:
        return await self._perms.compile_read_filter(
            list(visibility.principals),
            catalog_id=catalog_id,
            collection_id=collection_id,
            principal=visibility.principal,
            principal_id=visibility.principal_id,
        )

    async def _compute_catalog_filter(
        self, visibility: RequestVisibility
    ) -> AccessFilter:
        # Unconditional platform read (e.g. sysadmin ``.*`` ALLOW) — no
        # per-catalog loop. Only when no DENY/union could narrow it.
        platform = await self._compile(visibility, None)
        if platform.allow_all and not platform.deny and not platform.union:
            return AccessFilter.allow_everything()

        visible: List[str] = []
        for cat_id in await self._enumerate_catalog_ids():
            scope = await self._compile(visibility, cat_id)
            if not scope.deny_all:
                visible.append(cat_id)
        return _ids_filter(visible)

    async def _compute_collection_filter(
        self, visibility: RequestVisibility, catalog_id: str
    ) -> AccessFilter:
        cat_scope = await self._compile(visibility, catalog_id)
        if cat_scope.deny_all:
            return AccessFilter.deny_everything()
        # NOTE: a catalog-scope ``allow_all`` shortcut would be unsound here:
        # a DENY policy pinned to one collection's paths does not match the
        # catalog-scope probe paths, so only the per-collection compile (whose
        # probes carry the real collection id) can see it. Always loop.
        visible: List[str] = []
        for col_id in await self._enumerate_collection_ids(catalog_id):
            scope = await self._compile(visibility, catalog_id, col_id)
            if not scope.deny_all:
                visible.append(col_id)
        return _ids_filter(visible)

    async def _compute_asset_filter(
        self,
        visibility: RequestVisibility,
        catalog_id: str,
        collection_id: Optional[str],
    ) -> AccessFilter:
        # Gate on the collection (or catalog) scope first — if the caller
        # cannot see the owning collection, no assets within it are visible.
        col_scope = await self._compile(visibility, catalog_id, collection_id)
        if col_scope.deny_all:
            return AccessFilter.deny_everything()

        # Enumerate asset ids and compile the read filter per asset, using the
        # same probe-path logic that already handles collection-level policy
        # matching. An asset inherits visibility from its collection scope by
        # default; an asset-specific DENY policy (matched by the asset probe
        # paths added to _read_scope_probe_paths) can narrow it further.
        visible: List[str] = []
        for asset_id in await self._enumerate_asset_ids(catalog_id, collection_id):
            scope = await self._compile_asset(
                visibility, catalog_id, collection_id, asset_id
            )
            if not scope.deny_all:
                visible.append(asset_id)
        return _ids_filter(visible)

    async def _compile_asset(
        self,
        visibility: RequestVisibility,
        catalog_id: str,
        collection_id: Optional[str],
        asset_id: str,
    ) -> AccessFilter:
        """Compile a read filter scoped to a single asset."""
        return await self._perms.compile_read_filter(
            list(visibility.principals),
            catalog_id=catalog_id,
            collection_id=collection_id,
            asset_id=asset_id,
            principal=visibility.principal,
            principal_id=visibility.principal_id,
        )

    # ---- enumeration (under bypass — see module docstring) ------------- #

    async def _enumerate_catalog_ids(self) -> List[str]:
        from dynastore.models.protocols import CatalogsProtocol
        from dynastore.tools.discovery import get_protocol

        catalogs = get_protocol(CatalogsProtocol)
        if catalogs is None:
            raise RuntimeError(
                "CatalogsProtocol not available while computing listing "
                "visibility — cannot enumerate catalogs."
            )
        ids: List[str] = []
        with visibility_bypass():
            offset = 0
            while offset < _MAX_ENTRIES:
                page = await catalogs.list_catalogs(limit=_PAGE, offset=offset)
                ids.extend(c.id for c in page or [])
                if not page or len(page) < _PAGE:
                    break
                offset += _PAGE
        return ids

    async def _enumerate_collection_ids(self, catalog_id: str) -> List[str]:
        from dynastore.models.protocols import CatalogsProtocol
        from dynastore.tools.discovery import get_protocol

        catalogs = get_protocol(CatalogsProtocol)
        if catalogs is None:
            raise RuntimeError(
                "CatalogsProtocol not available while computing listing "
                "visibility — cannot enumerate collections."
            )
        ids: List[str] = []
        with visibility_bypass():
            offset = 0
            while offset < _MAX_ENTRIES:
                page = await catalogs.list_collections(
                    catalog_id, limit=_PAGE, offset=offset
                )
                ids.extend(c.id for c in page or [])
                if not page or len(page) < _PAGE:
                    break
                offset += _PAGE
        return ids

    async def _enumerate_asset_ids(
        self, catalog_id: str, collection_id: Optional[str]
    ) -> List[str]:
        from dynastore.models.protocols.assets import AssetsProtocol
        from dynastore.tools.discovery import get_protocol

        assets = get_protocol(AssetsProtocol)
        if assets is None:
            raise RuntimeError(
                "AssetsProtocol not available while computing asset listing "
                "visibility — cannot enumerate assets."
            )
        ids: List[str] = []
        with visibility_bypass():
            offset = 0
            while offset < _MAX_ENTRIES:
                page = await assets.list_assets(
                    catalog_id=catalog_id,
                    collection_id=collection_id,
                    limit=_PAGE,
                    offset=offset,
                )
                ids.extend(a.asset_id for a in page or [])
                if not page or len(page) < _PAGE:
                    break
                offset += _PAGE
        return ids

    # ---- cache --------------------------------------------------------- #

    def _cache_get(self, key: _CacheKey) -> Optional[AccessFilter]:
        entry = self._cache.get(key)
        if entry is None:
            return None
        expiry, flt = entry
        if expiry < time.monotonic():
            self._cache.pop(key, None)
            return None
        return flt

    def _cache_put(self, key: _CacheKey, flt: AccessFilter) -> None:
        if len(self._cache) >= _CACHE_MAXSIZE:
            now = time.monotonic()
            for k in [k for k, (exp, _) in self._cache.items() if exp < now]:
                self._cache.pop(k, None)
            while len(self._cache) >= _CACHE_MAXSIZE:
                self._cache.pop(next(iter(self._cache)), None)
        self._cache[key] = (time.monotonic() + _TTL_SECONDS, flt)
