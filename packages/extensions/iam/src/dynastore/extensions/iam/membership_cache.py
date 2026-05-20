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

"""Per-pod cached lookup of a principal's catalog memberships.

Used by `CatalogMembershipHandler` (in `modules/iam/conditions.py`) and by
any future consumer that needs to know which catalogs a principal can see.
Cache key is `(provider, subject_id)` only — the `IamQueryProtocol` singleton
is excluded so cache hits survive test fixture teardown / setup boundaries.

Per-pod L1 only (`distributed=False`). 60 s TTL is the safe upper bound on
grant-change propagation; a Valkey RTT per request would cost more than the
saved DB query for short dashboard sessions. Upgrade to `distributed=True`
when Valkey is provisioned in dev + prod.

Non-IAM extension consumers (e.g. ``extensions/admin``) MUST reach this
cache via ``get_protocol(MembershipCacheProtocol)`` rather than importing
this module directly — the architectural-invariant ratchet forbids any
cross-extension imports outside the explicit shared-infra allowlist. The
``MembershipCacheProvider`` class below adapts the module-level cached
function to that Protocol and is registered as a plugin during IAM
extension startup (see ``service.py``).
"""

from __future__ import annotations

from typing import Any, Dict, Optional

from dynastore.models.protocols.iam_query import IamQueryProtocol
from dynastore.tools.cache import cached
from dynastore.tools.discovery import get_protocol


@cached(
    maxsize=512,
    ttl=60,
    jitter=10,
    namespace="dashboard_authz_membership",
    distributed=False,
    ignore=["iam_query"],
)
async def get_membership_cached(
    iam_query: IamQueryProtocol, provider: str, subject_id: str,
) -> Dict[str, Any]:
    return await iam_query.list_catalog_memberships(
        provider=provider, subject_id=subject_id,
    )


class MembershipCacheProvider:
    """Adapter exposing :func:`get_membership_cached` as
    :class:`MembershipCacheProtocol` for cross-extension consumers.

    Resolves ``IamQueryProtocol`` lazily on each call — the IAM module
    registers its implementation during its own startup, and this provider
    instance is registered when the IAM extension boots, so by the time
    callers reach the protocol the query implementation is in place. If
    it is not (slim deployment / boot order anomaly), returns an empty
    membership mapping so the caller fails closed.
    """

    # Stable structural-typing check — registered via register_plugin so
    # get_protocol(MembershipCacheProtocol) finds this instance.

    async def get_membership(
        self, provider: str, subject_id: str
    ) -> Dict[str, Any]:
        iam_query: Optional[IamQueryProtocol] = get_protocol(IamQueryProtocol)
        if iam_query is None:
            return {"platform": False, "catalogs": [], "catalog_roles": {}, "total": 0}
        return await get_membership_cached(iam_query, provider, subject_id)


# Module-level singleton — instantiated once and registered by the IAM
# extension during startup so subsequent ``get_protocol`` lookups return
# the same object.
_PROVIDER_SINGLETON: MembershipCacheProvider = MembershipCacheProvider()


def get_provider() -> MembershipCacheProvider:
    """Return the process-wide membership-cache provider singleton."""
    return _PROVIDER_SINGLETON
