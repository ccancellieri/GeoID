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

"""Request-scoped listing visibility — the neutral contract.

Catalog and collection *listings* (and free-text search over them) must only
return entries the caller may see. The enforcement is transparent: routes and
extensions contain no authorization code; the authorization middleware
publishes WHO is asking (a :class:`RequestVisibility` snapshot, set once per
request in a :data:`ContextVar`), and the storage implementations behind
``CatalogsProtocol.list_catalogs`` / ``list_collections`` consult it and
narrow their own queries. Item-level reads follow the sibling contract in
:mod:`dynastore.models.protocols.access_filter` (``compile_read_filter``);
this module completes the same pattern for the two listing levels.

Division of responsibility (mirrors ``AccessFilter``):

* **This module (core)** owns only neutral, declarative pieces: the
  request snapshot, the contextvar, the :class:`ListingVisibilityProtocol`
  drivers resolve through ``get_protocol``, and the conservative
  ``AccessFilter`` → id-allowlist projection. No IAM vocabulary, no query
  syntax, no web-framework imports.
* **The authorization engine** registers the
  :class:`ListingVisibilityProtocol` implementation and is the only writer
  of the contextvar (its middleware). The filters it serves are compiled
  from the same policy / grant / condition graph as ``evaluate_access``.
* **Each storage driver** translates the resulting id constraint into its
  own predicate language (``id = ANY(...)`` for PG, a ``terms`` clause for
  Elasticsearch, ...). The platform never emits query syntax.

Degradation is structural: when no authorization layer is mounted nothing
sets the contextvar, :func:`get_request_visibility` returns ``None`` and
every listing is unfiltered. Conversely, fail-closed: when the contextvar IS
set (an authorization layer is active) but no provider is registered or the
provider raises, the resolution helpers return an EMPTY allowlist — a broken
authz layer must blank listings, never widen them.

Re-entrancy: the provider itself must enumerate catalogs/collections to
compute visibility. It does so under :func:`visibility_bypass`, which blanks
the contextvar for the enclosed scope so the enumeration runs unfiltered
instead of recursing.
"""
from __future__ import annotations

import logging
from contextlib import contextmanager
from contextvars import ContextVar, Token
from dataclasses import dataclass
from typing import (
    TYPE_CHECKING,
    Generator,
    Optional,
    Protocol,
    Tuple,
    runtime_checkable,
)
from uuid import UUID

from dynastore.models.protocols.access_filter import AccessFilter, FieldPredicate

if TYPE_CHECKING:
    from dynastore.models.auth import Principal

logger = logging.getLogger(__name__)

__all__ = [
    "RequestVisibility",
    "ListingVisibilityProtocol",
    "set_request_visibility",
    "reset_request_visibility",
    "get_request_visibility",
    "visibility_bypass",
    "extract_id_constraint",
    "resolve_catalog_listing_ids",
    "resolve_collection_listing_ids",
    "resolve_asset_listing_ids",
]


@dataclass(frozen=True)
class RequestVisibility:
    """WHO is asking — the caller snapshot the middleware publishes.

    ``principals`` is the same flat list the middleware passes to
    ``evaluate_access`` (effective principal id + role names, anonymous
    modelled as the anonymous role), so visibility can never diverge from
    the request-time authorization decision. ``principal`` /
    ``principal_id`` ride along for grant-level resolution (ABAC
    ``attribute_predicates``), exactly as ``compile_read_filter`` takes
    them.
    """

    principals: Tuple[str, ...]
    principal: "Optional[Principal]" = None
    principal_id: Optional[UUID] = None

    @property
    def cache_key(self) -> Tuple[str, ...]:
        """Stable per-caller key providers may cache compiled filters by."""
        pid = str(self.principal_id) if self.principal_id else ""
        return tuple(sorted(self.principals)) + (pid,)


_visibility_var: ContextVar[Optional[RequestVisibility]] = ContextVar(
    "request_visibility", default=None
)


def set_request_visibility(visibility: RequestVisibility) -> Token[Optional[RequestVisibility]]:
    """Publish the caller snapshot. The authorization middleware is the only
    legitimate caller; everything else is read-only."""
    return _visibility_var.set(visibility)


def reset_request_visibility(token: Token[Optional[RequestVisibility]]) -> None:
    _visibility_var.reset(token)


def get_request_visibility() -> Optional[RequestVisibility]:
    return _visibility_var.get()


@contextmanager
def visibility_bypass() -> Generator[None, None, None]:
    """Blank the visibility context for the enclosed scope.

    For the provider's own catalog/collection enumeration (which must run
    unfiltered to compute the filter) and for trusted internal traversals.
    Never use it on a user-facing response path.
    """
    token = _visibility_var.set(None)
    try:
        yield
    finally:
        _visibility_var.reset(token)


@runtime_checkable
class ListingVisibilityProtocol(Protocol):
    """Compiled listing scope per caller, served by the authorization engine.

    Both methods return an :class:`AccessFilter` over the *listing rows*
    themselves — each row modelled as a document with field ``id`` — built
    from the same policy/grant/condition graph as ``evaluate_access``:

    * ``allow_everything()`` — no restriction (unconditional platform read);
    * ``deny_everything()`` — the caller sees no entries;
    * otherwise allow clauses of single ``FieldPredicate("id", values)``
      shape (the projection :func:`extract_id_constraint` accepts).

    Implementations must fail closed (return ``deny_everything()`` rather
    than raise wherever possible) and are expected to cache per
    :attr:`RequestVisibility.cache_key` with a short TTL.
    """

    async def catalog_listing_filter(
        self, visibility: RequestVisibility
    ) -> AccessFilter: ...

    async def collection_listing_filter(
        self, visibility: RequestVisibility, catalog_id: str
    ) -> AccessFilter: ...

    async def asset_listing_filter(
        self, visibility: RequestVisibility, catalog_id: str, collection_id: Optional[str]
    ) -> AccessFilter: ...


def extract_id_constraint(
    flt: AccessFilter, field: str = "id"
) -> Optional[frozenset[str]]:
    """Project a listing ``AccessFilter`` to an id allowlist, conservatively.

    Returns ``None`` for "no restriction", otherwise the (possibly empty)
    frozenset of visible ids. Only the canonical shapes documented on
    :class:`ListingVisibilityProtocol` are accepted; anything else projects
    to the EMPTY set (fail closed — an unexpected filter shape must blank
    the listing, never widen it). Under-return is the safe direction, as
    with ``AccessFilter`` compilation generally.
    """
    if flt.deny_all:
        return frozenset()
    if flt.is_unconditional:
        return None
    if flt.union or flt.deny or flt.allow_all:
        logger.warning(
            "extract_id_constraint: unsupported listing-filter shape "
            "(union=%d deny=%d allow_all=%s) — failing closed to empty",
            len(flt.union), len(flt.deny), flt.allow_all,
        )
        return frozenset()

    ids: set[str] = set()
    for clause in flt.allow:
        if (
            len(clause.predicates) == 1
            and isinstance(clause.predicates[0], FieldPredicate)
            and clause.predicates[0].field == field
        ):
            ids.update(clause.predicates[0].values)
        else:
            # A clause this projection cannot express: skip it (stricter),
            # consistent with the uncompilable-ALLOW rule of AccessFilter.
            logger.warning(
                "extract_id_constraint: dropping non-id allow clause "
                "(predicates=%d) — listing may under-return",
                len(clause.predicates),
            )
    return frozenset(ids)


async def _resolve_listing_ids(
    catalog_id: Optional[str],
) -> Optional[frozenset[str]]:
    """Shared resolution: contextvar → provider → id constraint.

    ``None`` ⟹ no filtering (no caller snapshot published, i.e. no
    authorization layer in this request path). A published snapshot with a
    missing/broken provider fails closed to the empty set.
    """
    visibility = get_request_visibility()
    if visibility is None:
        return None

    from dynastore.tools.discovery import get_protocol

    provider = get_protocol(ListingVisibilityProtocol)
    if provider is None:
        logger.warning(
            "Listing visibility requested (caller snapshot present) but no "
            "ListingVisibilityProtocol provider is registered — failing "
            "closed to an empty listing."
        )
        return frozenset()

    try:
        if catalog_id is None:
            flt = await provider.catalog_listing_filter(visibility)
        else:
            flt = await provider.collection_listing_filter(visibility, catalog_id)
    except Exception:
        logger.warning(
            "Listing-visibility provider failed (catalog_id=%s) — failing "
            "closed to an empty listing.",
            catalog_id,
            exc_info=True,
        )
        return frozenset()
    return extract_id_constraint(flt)


async def resolve_catalog_listing_ids() -> Optional[frozenset[str]]:
    """Catalog ids the current request may list, or ``None`` for no filter."""
    return await _resolve_listing_ids(None)


async def resolve_collection_listing_ids(catalog_id: str) -> Optional[frozenset[str]]:
    """Collection ids of ``catalog_id`` the current request may list, or
    ``None`` for no filter."""
    return await _resolve_listing_ids(catalog_id)


async def resolve_asset_listing_ids(
    catalog_id: str, collection_id: Optional[str]
) -> Optional[frozenset[str]]:
    """Asset ids visible to the current request, or ``None`` for no filter.

    ``None`` ⟹ no authorization layer is active — listings are unfiltered.
    A non-None frozenset (possibly empty) is the set of asset ids the caller
    may see. Fail-closed: an error or missing provider returns the empty set.
    """
    visibility = get_request_visibility()
    if visibility is None:
        return None

    from dynastore.tools.discovery import get_protocol

    provider = get_protocol(ListingVisibilityProtocol)
    if provider is None:
        logger.warning(
            "Asset listing visibility requested (caller snapshot present) but no "
            "ListingVisibilityProtocol provider is registered — failing "
            "closed to an empty listing."
        )
        return frozenset()

    try:
        flt = await provider.asset_listing_filter(visibility, catalog_id, collection_id)
    except Exception:
        logger.warning(
            "Listing-visibility provider failed for asset scope "
            "(catalog_id=%s collection_id=%s) — failing closed to an empty listing.",
            catalog_id,
            collection_id,
            exc_info=True,
        )
        return frozenset()
    return extract_id_constraint(flt)
