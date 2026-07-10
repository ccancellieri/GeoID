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

"""Shared access-envelope derivation for access-aware storage drivers (#2687).

Pure, driver-agnostic building blocks for the ``{_visibility, _owner, _attrs}``
envelope stamped onto documents written through an access-aware driver
(``applies_access_filter=True``, e.g. the ES envelope driver, or a PG driver
carrying an ``access_envelope`` sidecar). Two call sites share these:

* **write time** — ``ItemService._resolve_access_envelope`` resolves the
  envelope from the live request's ``processing_context`` and wraps each
  piece in its own closed-default degrade (a config lookup failure must never
  block a write).
* **drain time** — ``canonical_index_read.read_canonical_index_inputs``
  recomputes the envelope from stored state (the hub row's persisted
  ``access_owner`` column plus a fresh config read) so the storage-plane
  drain can re-index an access-aware collection from an id-only obligation.
  The drain's fail-closed contract is stricter — see
  ``StorageDrainTask._build_canonical_doc`` — so these functions raise on a
  genuine resolution failure instead of guessing a default; only the
  *caller* decides whether to degrade or propagate.

Reaches IAM config types (``CatalogLookupAudience``, ``AttributeStampingPolicy``)
ONLY through ``ConfigsProtocol`` via ``get_protocol`` — never imports the IAM
module's ``AuthorizationProtocol`` — so the storage layer stays decoupled from
authz internals (mirrors ``access_scope.py`` and the envelope backfill task).
"""
from __future__ import annotations

from typing import Any, Dict, Mapping

__all__ = [
    "collection_uses_access_aware_driver",
    "resolve_catalog_visibility",
    "resolve_attribute_stamping_paths",
]


async def collection_uses_access_aware_driver(
    catalog_id: str, collection_id: str,
) -> bool:
    """True when any WRITE driver for the collection opts in to row-level ABAC.

    Single derivation point shared by write-time stamping
    (``ItemService._collection_uses_access_aware_driver``) and drain-time
    envelope recompute (``canonical_index_read.read_canonical_index_inputs``).
    Two detection branches:

    1. **ES-envelope** — the driver class carries ``applies_access_filter=True``
       (the standardized attribute set by the envelope Elasticsearch driver).
    2. **PG-sidecar** — the driver exposes a ``get_driver_config`` method (PG
       drivers) and its per-collection config lists a sidecar with
       ``sidecar_type == "access_envelope"`` (#1457 G4).

    Fail-open (returns ``False``) on any routing-resolution error — a
    misconfigured collection or a transient routing glitch never blocks: no
    envelope is safer than guessing one. A caller that already knows it is
    about to write into a resolved access-aware driver (the storage drain,
    which resolves its ``BulkIndexer`` independently of this check) must
    enforce its own fail-closed guarantee rather than relying on this
    function never degrading — see ``StorageDrainTask._build_canonical_doc``.
    """
    try:
        from dynastore.modules.storage.router import get_write_drivers

        resolved = await get_write_drivers(catalog_id, collection_id)
    except Exception:
        return False

    for r in resolved:
        # Branch 1: ES-envelope driver (applies_access_filter class attr).
        if getattr(type(r.driver), "applies_access_filter", False):
            return True

        # Branch 2: PG sidecar with sidecar_type == "access_envelope" (G4).
        get_cfg = getattr(r.driver, "get_driver_config", None)
        if callable(get_cfg):
            try:
                from dynastore.modules.storage.drivers.pg_sidecars import (
                    driver_sidecars,
                )

                drv_cfg = await get_cfg(catalog_id, collection_id)  # type: ignore[misc]
                if any(
                    getattr(sc, "sidecar_type", None) == "access_envelope"
                    for sc in driver_sidecars(drv_cfg)
                ):
                    return True
            except Exception:
                pass  # fail-open for this branch; ES check already handled above

    return False


async def resolve_catalog_visibility(catalog_id: str) -> str:
    """Resolve ``"public"`` / ``"private"`` from ``CatalogLookupAudience.is_public``.

    Raises when ``ConfigsProtocol`` is unavailable or the underlying config
    read fails — this function never guesses a default. A write-time caller
    that wants the historical closed-default degrade (keep ``"private"`` on
    any error) must catch around its own call; the drain deliberately does
    NOT catch so a resolution failure surfaces as "envelope recompute
    failed" and the row retries instead of indexing without its envelope.
    """
    from dynastore.models.protocols.configs import ConfigsProtocol
    from dynastore.modules.iam.audience_configs import CatalogLookupAudience
    from dynastore.tools.discovery import get_protocol

    configs = get_protocol(ConfigsProtocol)
    if configs is None:
        raise RuntimeError("ConfigsProtocol is not registered")
    audience = await configs.get_config(CatalogLookupAudience, catalog_id=catalog_id)
    if audience is not None and getattr(audience, "is_public", False):
        return "public"
    return "private"


async def resolve_attribute_stamping_paths(
    catalog_id: str, collection_id: str,
) -> Dict[str, str]:
    """Resolve ``AttributeStampingPolicy.attribute_paths`` for a collection.

    Returns ``{}`` when the collection has no stamping policy configured (the
    normal "not enrolled in ``_attrs`` stamping" case — not an error). Raises
    when ``ConfigsProtocol`` is unavailable or the config read itself fails,
    same contract as :func:`resolve_catalog_visibility`: callers choose
    whether to degrade or propagate.
    """
    from dynastore.models.protocols.configs import ConfigsProtocol
    from dynastore.modules.iam.stamping_config import AttributeStampingPolicy
    from dynastore.tools.discovery import get_protocol

    configs = get_protocol(ConfigsProtocol)
    if configs is None:
        raise RuntimeError("ConfigsProtocol is not registered")
    policy = await configs.get_config(
        AttributeStampingPolicy, catalog_id=catalog_id, collection_id=collection_id,
    )
    if policy is None:
        return {}
    paths: Mapping[str, Any] = getattr(policy, "attribute_paths", {}) or {}
    return dict(paths)
