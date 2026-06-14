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

"""Per-pod cached lookup of a principal's catalog memberships.

Used by ``CatalogMembershipHandler`` (in ``modules/iam/conditions.py``) and by
any future consumer that needs to know which catalogs a principal can see.

Cache key is ``(provider, subject_id, rule_version)`` — the
``IamQueryProtocol`` singleton is excluded so cache hits survive test fixture
teardown / setup boundaries; ``rule_version`` is the current platform-level
"iam" binding-version counter from Valkey (see
``modules/iam/phantom_token.get_binding_version``).

**Cross-pod invalidation**: when any pod writes a grant or revokes a role
(``postgres_iam_storage._bump_binding_version``), it increments BOTH the
affected schema's counter AND the platform "iam" counter.  Every pod's next
call to :func:`get_membership_cached` fetches the "iam" counter (memoised
in-process for ~2 s; see ``phantom_token._VERSION_L1_TTL``), finds it has
changed, and the old cache key is no longer addressable — so the lookup
falls through to the DB.  This is the same mechanism used by
``compiled_rule_cache`` (see that module for the design rationale).

When Valkey is absent (counter returns 0 on every read) the cache degrades
to a pure TTL cache with the same 60 s window as before.

Per-pod L1 only (``distributed=False``).  The return value is a plain dict
and serialising it across Valkey is straightforward, but the membership query
is already cheap (single joined SQL scan) and the primary goal here is
reducing DB load per request, not cross-pod sharing.

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
    iam_query: IamQueryProtocol,
    provider: str,
    subject_id: str,
    rule_version: int = 0,
) -> Dict[str, Any]:
    """Return the catalog-membership map for a principal.

    ``rule_version`` is the current platform binding-version counter.  It is
    included in the cache key so that any IAM mutation (grant/revoke on any
    schema) that bumps the "iam" Valkey counter invalidates this entry on
    every pod, not just the pod that performed the write.  Pass
    ``iam_rule_version()`` (sync snapshot, zero-cost) or
    ``await get_binding_version("iam")`` from the call site.
    """
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
        from dynastore.modules.iam.compiled_rule_cache import iam_rule_version

        iam_query: Optional[IamQueryProtocol] = get_protocol(IamQueryProtocol)
        if iam_query is None:
            return {"platform": False, "catalogs": [], "catalog_roles": {}, "total": 0}
        return await get_membership_cached(
            iam_query, provider, subject_id, iam_rule_version()
        )


# Module-level singleton — instantiated once and registered by the IAM
# extension during startup so subsequent ``get_protocol`` lookups return
# the same object.
_PROVIDER_SINGLETON: MembershipCacheProvider = MembershipCacheProvider()


def get_provider() -> MembershipCacheProvider:
    """Return the process-wide membership-cache provider singleton."""
    return _PROVIDER_SINGLETON
