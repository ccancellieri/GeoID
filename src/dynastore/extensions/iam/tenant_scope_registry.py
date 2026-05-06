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

"""Declarative tenant-scope routing rules consumed by ``TenantScopeMiddleware``.

The middleware walks this registry on every request to decide whether a URL
needs per-catalog authorization. Routes themselves carry no authz wiring;
operators (or extension authors) add patterns here.

Each ``TenantScopeRule`` declares:
  - ``pattern``            — compiled regex matched against ``request.url.path``;
  - ``catalog_source``     — where to extract the catalog id from. Supported:
                              * ``"query:NAME"``       (?NAME=… query param)
                              * ``"header:NAME"``      (HTTP header)
                              * ``"regex_group:NAME"`` (named capture group on
                                ``pattern`` — required for path-based extraction
                                because ``BaseHTTPMiddleware`` runs BEFORE
                                FastAPI route matching, so ``request.path_params``
                                is empty);
  - ``collection_source``  — optional, same source kinds. When set, the
                              middleware also pins ``collection_id`` to
                              ``request.state.tenant_scope`` for downstream
                              data-layer Protocols. Gating decisions remain
                              catalog-only;
  - ``default``            — value used when ``catalog_source`` resolves empty
                              (``"_system_"`` keeps platform-scope read
                              sysadmin-only);
  - ``allow_anonymous``    — when False, anonymous callers are 401'd before
                              dispatch.

Project-wide convention (per the path-based reshape): all per-catalog data
endpoints live at ``/{service}/catalogs/{catalog_id}[/collections/{collection_id}]/...``.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import List, Optional


@dataclass(frozen=True)
class TenantScopeRule:
    id: str
    pattern: re.Pattern
    catalog_source: str
    default: str
    allow_anonymous: bool
    collection_source: Optional[str] = None


# Bare platform-tier dashboard endpoints — sysadmin only. The URL never
# carries a catalog id (system tier lives outside the catalog hierarchy);
# we surface ``_system_`` as the resolved scope so the middleware's
# platform-tier check (caller must hold ``SYSADMIN`` role or
# ``membership.platform``) fires.
_DASHBOARD_PLATFORM_PATTERN = re.compile(
    r"^/web/dashboard/(?:stats|logs|events|tasks|ogc-compliance)/?$"
)

# /web/dashboard/catalogs/{catalog_id}/collections/{collection_id}[/...]
# More specific — must be checked BEFORE the per-catalog rule below, because
# the per-catalog rule also matches collection-scoped paths.
_DASHBOARD_PER_COLLECTION_PATTERN = re.compile(
    r"^/web/dashboard/catalogs/(?P<catalog_id>[^/]+)/collections/(?P<collection_id>[^/]+)(?:/.*)?$"
)

# /web/dashboard/catalogs/{catalog_id}[/...]  — covers shell + data endpoints
# (stats, logs, events, tasks, ogc-compliance, processes/ shell, etc.) AND the
# catalog-scoped collections list (which has no collection_id segment).
_DASHBOARD_PER_CATALOG_PATTERN = re.compile(
    r"^/web/dashboard/catalogs/(?P<catalog_id>[^/]+)(?:/.*)?$"
)

# /admin/catalogs/{catalog_id}[/...]  — catalog-scoped admin endpoints
# (principal-role binding GET/POST/DELETE, catalog users list). Extracting
# catalog_id makes ``catalog_admin_required`` evaluable for the
# ``admin_catalog_access`` policy.
_ADMIN_PER_CATALOG_PATTERN = re.compile(
    r"^/admin/catalogs/(?P<catalog_id>[^/]+)(?:/.*)?$"
)


TENANT_SCOPED_ROUTES: List[TenantScopeRule] = [
    TenantScopeRule(
        id="dashboard_platform",
        pattern=_DASHBOARD_PLATFORM_PATTERN,
        # Pin the resolved scope to the platform sentinel so the middleware
        # routes through its sysadmin-only branch. No URL extraction needed.
        catalog_source="default:_system_",
        default="_system_",
        allow_anonymous=False,
    ),
    TenantScopeRule(
        id="dashboard_per_collection",
        pattern=_DASHBOARD_PER_COLLECTION_PATTERN,
        catalog_source="regex_group:catalog_id",
        collection_source="regex_group:collection_id",
        default="_system_",
        allow_anonymous=False,
    ),
    TenantScopeRule(
        id="dashboard_per_catalog",
        pattern=_DASHBOARD_PER_CATALOG_PATTERN,
        catalog_source="regex_group:catalog_id",
        default="_system_",
        allow_anonymous=False,
    ),
    TenantScopeRule(
        id="admin_per_catalog",
        pattern=_ADMIN_PER_CATALOG_PATTERN,
        catalog_source="regex_group:catalog_id",
        default="_system_",
        allow_anonymous=False,
    ),
]


def match_tenant_scope_rule(path: str) -> Optional[TenantScopeRule]:
    """Return the first rule whose pattern matches ``path``, or ``None``.

    Order matters: more specific rules (e.g. per-collection) appear before
    more general rules (per-catalog) so they shadow correctly.
    """
    for rule in TENANT_SCOPED_ROUTES:
        if rule.pattern.match(path):
            return rule
    return None
