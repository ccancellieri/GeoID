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

"""
``MembershipCacheProtocol`` — cached catalog-membership lookup for non-IAM consumers.

Extensions that need to know a principal's catalog memberships (e.g. admin
endpoints narrowing their response shape for catalog-tier admins) MUST NOT
import ``dynastore.extensions.iam.*`` directly. They consume identity from
``request.state.principal`` and resolve membership through this Protocol via
``get_protocol(MembershipCacheProtocol)``.

The concrete implementation is registered by the IAM extension; it wraps
``IamQueryProtocol.list_catalog_memberships`` behind a per-pod L1 cache so
hot critical paths (policy evaluation, admin route guards) only hit the DB
once per ``(provider, subject_id)`` per TTL window.

Consumers should degrade gracefully when ``get_protocol`` returns ``None``
(slim deployment with the IAM extension absent) — typically by failing
closed on authz-adjacent paths.
"""

from typing import Any, Dict, Protocol, runtime_checkable


@runtime_checkable
class MembershipCacheProtocol(Protocol):
    """Cached read of a principal's catalog memberships."""

    async def get_membership(
        self, provider: str, subject_id: str
    ) -> Dict[str, Any]:
        """Return the cached membership mapping for the identity.

        Mapping shape matches :meth:`IamQueryProtocol.list_catalog_memberships`:

            ``platform``       bool — identity carries a platform-scope grant
            ``catalogs``       list[str] — catalog ids with at least one grant
            ``catalog_roles``  Dict[str, list[str]] — per-catalog role names
            ``total``          int — convenience for ``len(catalogs)``
        """
        ...
