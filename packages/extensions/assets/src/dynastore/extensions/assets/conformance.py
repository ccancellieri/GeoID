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

"""Asset Transactions conformance class URIs (draft STAC API extension).

The canonical framing is the **STAC API — Asset Transactions Extension**
(``docs/proposals/asset-transactions-extension.md``); the pilot ships the STAC
track first (proposal §6.3), with the conformance classes drafted to lift into
an OGC API - Assets specification later without renaming the URIs.

These URIs are PROPOSALS; the namespace path
(``asset-transactions/1.0/conf/...``) is reserved-pending until the extension is
accepted into the STAC registry. Do not depend on these as immutable
identifiers in external clients.

The class set unions the proposal's classes (``core / upload / processes /
async / search / sync``) with the implementation-specific classes the pilot
already enforces (``write-policies / versioning / virtual-assets /
references``), so a client can probe every advertised capability deterministically.
"""
from __future__ import annotations

# Draft STAC extension namespace — see module docstring + proposal §5.
_NS = "https://stac-extensions.github.io/asset-transactions/1.0/conf"

ASSETS_CORE = f"{_NS}/core"
"""Core: GET /assets/, /assets/conformance, GET single asset, GET list, and
   process discovery (GET .../assets/{id}/processes). Mirrors OGC API - Common
   requirements."""

ASSETS_UPLOAD = f"{_NS}/upload"
"""Upload session lifecycle: POST /upload (born-claimed PENDING INSERT
   gated by AssetsWritePolicy) + GET /upload/{ticket_id}/status. The
   server backs the storage transaction, the client uploads bytes
   to the signed URL the server mints."""

ASSETS_PROCESSES = f"{_NS}/processes"
"""Asset-scoped process execution: GET/POST .../assets/{id}/{process_id} with a
   ``parameters_schema``, dispatched through ``AssetProcessProtocol`` impls
   discovered via ``get_protocols``. Idempotent processes use GET; state-changing
   ones use POST. Returns the polymorphic ``AssetProcessOutput`` envelope."""

ASSETS_ASYNC = f"{_NS}/async"
"""Asynchronous process execution: ``AssetProcessOutput.type == "job"`` with a
   ``job_id`` the client polls via the host's OGC Processes jobs surface."""

ASSETS_SEARCH = f"{_NS}/search"
"""Granular asset search via ``POST .../assets-search`` with an
   ``AssetFilter`` list. Path-scoped (catalog-tier / collection /
   cross-catalog), routing-aware (resolves the SEARCH-routed driver with a
   READ fallback), and supports the scalar operator set
   (eq/ne/gt/gte/lt/lte/like/ilike/in/nin/isnull/isnotnull) uniformly on the
   PostgreSQL and Elasticsearch backends."""

ASSETS_SYNC = f"{_NS}/sync"
"""Asset CRUD emits a sync event via a single-writer outbox
   (``AssetEntitySyncSubscriber`` and peers consume it); reverse cascade via
   ``DELETE .../assets/{id}?force=true&propagate=true`` removes items linked
   through the asset. Bucket-side metadata mirroring is advisory."""

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
    ASSETS_UPLOAD,
    ASSETS_PROCESSES,
    ASSETS_ASYNC,
    ASSETS_SEARCH,
    ASSETS_SYNC,
    ASSETS_WRITE_POLICIES,
    ASSETS_VERSIONING,
    ASSETS_VIRTUAL_ASSETS,
    ASSETS_REFERENCES,
)

__all__ = (
    "ASSETS_CORE",
    "ASSETS_UPLOAD",
    "ASSETS_PROCESSES",
    "ASSETS_ASYNC",
    "ASSETS_SEARCH",
    "ASSETS_SYNC",
    "ASSETS_WRITE_POLICIES",
    "ASSETS_VERSIONING",
    "ASSETS_VIRTUAL_ASSETS",
    "ASSETS_REFERENCES",
    "ASSETS_CONFORMANCE_URIS",
)
