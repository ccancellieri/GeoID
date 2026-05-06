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
``IamQueryProtocol`` — read-side IAM lookups for non-IAM consumers.

Routes, extensions, and middleware-adjacent code MUST NOT import
``dynastore.modules.iam.*`` directly. They consume identity from
``request.state.principal`` (populated by ``IamMiddleware``) and any
further IAM read operations through this Protocol.

The concrete implementation is registered by the IAM module via the
plugin discovery system; consumers resolve it with
``get_protocol(IamQueryProtocol)``.
"""

from typing import Any, Dict, Protocol, runtime_checkable


@runtime_checkable
class IamQueryProtocol(Protocol):
    """Read-only IAM queries for tenant-scope authorization."""

    async def list_catalog_memberships(
        self, provider: str, subject_id: str
    ) -> Dict[str, Any]:
        """Return the set of catalogs the identity has at least one grant in.

        Returns a mapping with keys:
            ``platform``       bool — identity carries a platform-scope grant
            ``catalogs``       list[str] — catalog ids with at least one grant
            ``catalog_roles``  Dict[str, list[str]] — per-catalog role names;
                               consumers that need to know *which* roles
                               the principal holds (not just membership) —
                               e.g. the ``catalog_admin_required``
                               condition handler — read this field. Keys
                               are exactly ``catalogs``.
            ``total``          int — convenience for ``len(catalogs)``
        """
        ...
