#    Copyright 2025 FAO
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

"""OGC API - Assets conformance class URIs (draft).

These URIs are PROPOSALS to the OGC SWG; the namespace path
(``/spec/ogcapi-assets-1/1.0/conf/...``) is reserved-pending until the
specification is accepted. Do not depend on these as immutable identifiers
in external clients.
"""
from __future__ import annotations

# Draft namespace
_NS = "http://www.opengis.net/spec/ogcapi-assets-1/1.0/conf"

ASSETS_CORE = f"{_NS}/core"
"""Core: GET /assets/, /assets/conformance, GET single asset, GET list.
   Mirrors OGC API - Common requirements."""

ASSETS_UPLOADS = f"{_NS}/uploads"
"""Upload session lifecycle: POST /upload (born-claimed PENDING INSERT
   gated by AssetsWritePolicy) + GET /upload/{ticket_id}/status. The
   server backs the storage transaction, the client uploads bytes
   to the signed URL the server mints."""

ASSETS_WRITE_POLICIES = f"{_NS}/write-policies"
"""Per-collection / per-catalog AssetsWritePolicy: identity matcher chain
   (asset_id, filename, url, metadata_field, content_hash) and
   conflict-action vocabulary (refuse_fail, refuse, refuse_return, update,
   new_version)."""

ASSETS_VERSIONING = f"{_NS}/versioning"
"""NEW_VERSION on conflict - driver-capability-gated. Soft-archives the
   matched row to status='deleted' and inserts a fresh row with the
   same identity."""

ASSETS_VIRTUAL_ASSETS = f"{_NS}/virtual-assets"
"""kind='virtual' assets: external href registration without bucket
   presence. Status starts ACTIVE; uri is never set."""

ASSETS_REFERENCES = f"{_NS}/references"
"""asset_references table coordinating cascade-delete safety across
   driver-owned referencing entities (collections, DuckDB tables,
   Iceberg tables, ...)."""

ASSETS_CONFORMANCE_URIS = (
    ASSETS_CORE,
    ASSETS_UPLOADS,
    ASSETS_WRITE_POLICIES,
    ASSETS_VERSIONING,
    ASSETS_VIRTUAL_ASSETS,
    ASSETS_REFERENCES,
)

__all__ = (
    "ASSETS_CORE",
    "ASSETS_UPLOADS",
    "ASSETS_WRITE_POLICIES",
    "ASSETS_VERSIONING",
    "ASSETS_VIRTUAL_ASSETS",
    "ASSETS_REFERENCES",
    "ASSETS_CONFORMANCE_URIS",
)
