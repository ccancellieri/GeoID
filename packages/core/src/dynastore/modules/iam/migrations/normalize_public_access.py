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

"""Normalize the legacy ``/web/.*`` anonymous catch-all policy resource.

Early revisions of the ``web_public_access`` policy granted anonymous GET
access to ``/web/.*`` outright. That catch-all is broader than intended:
it also matches gated dashboard data endpoints such as
``/web/dashboard/catalogs/{id}/stats``. The policy has since been tightened
to an explicit enumeration of anonymous-safe ``/web/...`` paths (see
``dynastore.extensions.web.web._web_policies``).

This module is a pure resource-list transform, not a runtime DDL migration:
``_normalize_resources`` takes a policy's ``resources`` list and — if the
stale catch-all is present — returns a new list with it replaced by the
enumerated safe paths, leaving every other entry untouched. It is idempotent:
once the catch-all is gone, calling it again is a no-op (returns ``None``).

Any real DB-backed rollout of this transform (reading/writing
``iam.policies`` rows) is left to the caller — this module only encodes the
transform itself so it can be applied wherever a policy's resources are
loaded (backfill script, admin tool, etc.).

PR-5 of umbrella #2613.
"""
from __future__ import annotations

# The legacy anonymous catch-all this migration removes.
_STALE_PATTERN = "/web/.*"

# The enumerated anonymous-safe ``/web/...`` paths that replace the catch-all.
# Snapshot of the literal allowlist declared in
# ``dynastore.extensions.web.web._web_policies`` at the time this migration
# was written — kept as a fixed point-in-time list, like any other migration.
_SAFE_WEB_PATHS: tuple[str, ...] = (
    "/web/stac/.*",
    "/web/records/.*",
    "/web/features/.*",
    "/web/assets/.*",
    "/web/edr/.*",
    "/web/movingfeatures/.*",
    "/web/tiles/.*",
    "/web/auth/.*",
    "/web/coverages/.*",
    "/web/?$",
    "/web/pages/.*",
    "/web/extension-static/.*",
    "/web/static/.*",
    "/web/website/.*",
    "/web/docs-content/.*",
    "/web/docs-manifest$",
    "/web/config/.*",
    "/web/dashboard/?$",
    "/web/lite/.*",
)


def _normalize_resources(resources: list[str]) -> list[str] | None:
    """Replace the stale ``/web/.*`` catch-all with the enumerated safe paths.

    Returns a new list with ``_STALE_PATTERN`` removed and ``_SAFE_WEB_PATHS``
    inserted in its place, preserving every other entry. Returns ``None``
    (no-op) when ``_STALE_PATTERN`` is not present in ``resources``.
    """
    if _STALE_PATTERN not in resources:
        return None

    normalized: list[str] = []
    for resource in resources:
        if resource == _STALE_PATTERN:
            normalized.extend(_SAFE_WEB_PATHS)
        else:
            normalized.append(resource)
    return normalized
