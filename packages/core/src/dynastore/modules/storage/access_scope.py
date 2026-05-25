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
"""Shared read-scope compilation for access-controlled storage drivers.

Every read entry point that dispatches to an access-aware driver (the STAC
``/search`` fast path, the search extension's ``/search`` family and the OGC
API ``/items`` listings) must derive the SAME row-level read scope for a given
principal. Keeping that single piece of security logic in one place — rather
than re-implementing it per entry point — is what stops the paths from drifting
(a looser path is a leak). This module is driver-agnostic: it lives in the
storage core, not under any one driver, so core dispatch code can reach it
without importing a driver package.

The functions reach the authorization engine ONLY through the neutral
``PermissionProtocol`` + ``AccessFilter`` contract via ``get_protocol`` — they
import nothing from the IAM module at runtime, so the storage layer stays
decoupled from authz internals.

Fail-closed: when no ``PermissionProtocol`` is registered (an access-aware
driver in a deployment with no authz engine) the result is
``AccessFilter.deny_everything()`` — never an unfiltered scan.
"""
from __future__ import annotations

from typing import TYPE_CHECKING, List, Optional, Tuple

if TYPE_CHECKING:
    from starlette.requests import Request

    from dynastore.models.auth import Principal
    from dynastore.models.protocols.access_filter import AccessFilter

__all__ = ["compile_read_access_filter", "principals_from_request_state"]


def principals_from_request_state(
    request: "Request",
) -> "Tuple[List[str], Optional[Principal]]":
    """Derive ``(principals, principal)`` from IAM-middleware request state.

    ``IamMiddleware`` populates ``request.state.principal`` (a ``Principal`` or
    ``None`` for anonymous), ``request.state.principal_id`` and
    ``request.state.principal_role``. This reproduces the exact flat principals
    list the middleware passes to ``evaluate_access``
    (``[principal_id] + principal_role``) so a compiled read scope matches the
    request-time authorization decision. The middleware models anonymous as the
    configured anonymous role, so an unauthenticated caller still yields a
    non-empty list (public-only scope), never an empty one.
    """
    state = getattr(request, "state", None)
    principal: "Optional[Principal]" = getattr(state, "principal", None)
    principal_id = getattr(state, "principal_id", None)
    principal_role = getattr(state, "principal_role", None)
    principals: List[str] = []
    if principal_id:
        principals.append(principal_id)
    if isinstance(principal_role, list):
        principals.extend(principal_role)
    elif principal_role:
        principals.append(principal_role)
    return principals, principal


async def compile_read_access_filter(
    *,
    catalog_id: Optional[str],
    collections: Optional[List[str]],
    principals: Optional[List[str]],
    principal: "Optional[Principal]",
) -> "AccessFilter":
    """Compile the caller's read scope to a neutral ``AccessFilter``.

    Returns ``AccessFilter.deny_everything()`` when no ``PermissionProtocol`` is
    registered (fail-closed).

    Compilation scope by collection count:

    * **exactly one** collection — compiled at collection scope so
      collection-level grants pin precisely.
    * **zero / ``None``** collections — compiled at catalog scope (a
      catalog-wide read; the envelope index is per-catalog and tenant-isolated).
    * **several** collections — compiled ONCE PER COLLECTION and combined as an
      *exclusion-union* (:meth:`AccessFilter.union_of`): a document is admitted
      iff at least one collection's *complete* per-collection filter (its ALLOW
      AND DENY, all already pinned to that ``collection_id``) admits it. This is
      what lets a principal with DIFFERENT grants per collection get exactly
      what each collection allows. Crucially the per-collection filters are NOT
      flattened into one allow/deny set: each stays intact so a collection-A
      DENY (or a non-collection-pinned visibility DENY) can never suppress a
      collection-B document. A collection the principal cannot read compiles to
      ``deny_everything`` and simply contributes nothing (exclusion-union),
      never failing the whole query.
    """
    from dynastore.models.protocols.policies import PermissionProtocol
    from dynastore.models.protocols.access_filter import AccessFilter
    from dynastore.tools.discovery import get_protocol

    perms = get_protocol(PermissionProtocol)
    if perms is None:
        return AccessFilter.deny_everything()

    norm_principals = principals or []

    # Zero/None collections → catalog scope (unchanged). Exactly one collection →
    # collection scope (unchanged). These two paths are byte-for-byte identical
    # to the prior behaviour.
    if not collections or len(collections) == 1:
        single_collection = collections[0] if collections else None
        return await perms.compile_read_filter(
            norm_principals,
            catalog_id=catalog_id,
            collection_id=single_collection,
            principal=principal,
        )

    # Several collections → compile each independently (each pinned to its own
    # ``collection_id`` by the engine's ``_scope_clause``) and exclusion-union
    # them. Per-collection isolation is preserved by NOT flattening clauses.
    sub_filters = [
        await perms.compile_read_filter(
            norm_principals,
            catalog_id=catalog_id,
            collection_id=collection,
            principal=principal,
        )
        for collection in collections
    ]
    return AccessFilter.union_of(sub_filters)
