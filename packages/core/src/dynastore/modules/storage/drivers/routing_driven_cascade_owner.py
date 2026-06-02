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

"""Routing-driven cascade owner — delegates drop_storage to configured drivers.

Replaces the four hard-coded ES index owners (public items, asset, private
items, envelope) with a single owner that:

1. Enumerates the drivers configured for the deleted catalog/collection by
   calling ``_resolve_driver_ids_cached`` for each relevant routing-config
   class and operation combination.
2. Emits one ``CleanupRef`` per ``(registry_kind, driver_ref)`` pair found.
3. In ``cleanup_one``, looks up the driver from the appropriate
   ``DriverRegistry`` index and calls ``driver.drop_storage()``.

This design correctly handles:
- COLLECTION-scope deletes: items/asset drivers whose drop_storage issues a
  delete_by_query on the collection's docs within the shared catalog index.
- CATALOG-scope deletes: drivers whose drop_storage drops the entire
  per-catalog index.
- Collection-metadata (#1750): the CollectionStore (collection_store_index)
  driver's drop_storage removes the catalog's collection docs from the
  singleton ``{prefix}-collections`` index.
- Private items DENY revoke: ``ItemsElasticsearchPrivateDriver.drop_storage``
  already calls ``_revoke_deny_policy`` — parity is preserved.

GCS asset drivers are EXCLUDED from this owner.  GCS object-storage cleanup
is handled by the dedicated ``GcsCatalogPrefixOwner`` / ``GcsCollectionPrefixOwner``
owners (task-runner based), which have their own lifecycle and retry semantics.
Including GCS drivers here would duplicate cleanup and conflict with those
owners.  The check is based on the driver class name: drivers whose snake_case
id contains ``gcs`` or ``bigquery`` are skipped.

Register via :func:`register_owners` from the catalog module lifespan BEFORE
the CascadeCleanupRegistry is frozen.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any, ClassVar, Iterable, Optional

from dynastore.modules.catalog.resource_owner import (
    BaseResourceOwner,
    CleanupMode,
    CleanupOutcome,
    CleanupRef,
    ResourceScope,
    ScopeRef,
)

if TYPE_CHECKING:
    from dynastore.modules.catalog.cascade_registry import CascadeCleanupRegistry

logger = logging.getLogger(__name__)

# Registry-kind strings used in CleanupRef.metadata["registry"] to identify
# which DriverRegistry index should be used in cleanup_one.
_REGISTRY_ITEMS = "items"
_REGISTRY_ASSET = "asset"
_REGISTRY_COLLECTION = "collection"

# Driver id fragments whose owners are handled by dedicated GCS / BQ owners.
# Routing-driven owner must not claim these to avoid double-cleanup.
_EXCLUDED_DRIVER_ID_FRAGMENTS = ("gcs", "bigquery", "gcp")


def _is_excluded_driver(driver_ref: str) -> bool:
    """Return True if *driver_ref* belongs to an external-storage owner."""
    lower = driver_ref.lower()
    return any(frag in lower for frag in _EXCLUDED_DRIVER_ID_FRAGMENTS)


async def _enumerate_configured_drivers(
    scope_ref: ScopeRef,
    conn: Any,
) -> list[tuple[str, str]]:
    """Return ``[(registry_kind, driver_ref), ...]`` for the deleted entity.

    Calls ``_resolve_driver_ids_cached`` for each routing-config class and
    operation that is relevant to the scope.  Deduplicates the result so each
    ``(registry_kind, driver_ref)`` pair appears at most once, regardless of
    how many operations reference it.

    Safe when no explicit config exists (the resolver falls back to model
    defaults).  ``ModuleNotFoundError`` on an optional routing-config class
    (e.g. AssetRoutingConfig when the assets SCOPE is not installed) is
    treated as "no drivers" — not an error.  Genuinely broken resolutions
    (unexpected exceptions) propagate so describe_scope fails closed and the
    delete transaction rolls back.
    """
    from dynastore.modules.storage.routing_config import (
        AssetRoutingConfig,
        CollectionRoutingConfig,
        ItemsRoutingConfig,
        Operation,
    )
    from dynastore.modules.storage.router import _resolve_driver_ids_cached

    catalog_id = scope_ref.catalog_id
    collection_id = scope_ref.collection_id

    seen: set[tuple[str, str]] = set()
    result: list[tuple[str, str]] = []

    async def _collect(
        registry_kind: str,
        routing_cls: Any,
        operations: list[str],
    ) -> None:
        for op in operations:
            try:
                entries = await _resolve_driver_ids_cached(
                    routing_cls,
                    catalog_id,
                    collection_id,
                    op,
                    frozenset(),
                )
            except Exception as exc:  # noqa: BLE001
                # Only suppress "no config/no drivers" style errors by
                # checking that the exception is truly benign.  Unknown /
                # infrastructure errors are re-raised to trigger fail-closed.
                exc_str = str(exc).lower()
                benign = (
                    "no driver" in exc_str
                    or "not available" in exc_str
                    or "configsprotocol not available" in exc_str
                )
                if not benign:
                    raise
                logger.debug(
                    "_enumerate_configured_drivers: no drivers for "
                    "routing_cls=%r op=%r catalog_id=%r: %s",
                    routing_cls.__name__, op, catalog_id, exc,
                )
                continue

            for driver_ref, _on_failure, _write_mode in entries:
                if _is_excluded_driver(driver_ref):
                    logger.debug(
                        "_enumerate_configured_drivers: skipping excluded "
                        "driver %r (GCS/BQ handled by dedicated owner).",
                        driver_ref,
                    )
                    continue
                key = (registry_kind, driver_ref)
                if key not in seen:
                    seen.add(key)
                    result.append(key)

    # Items drivers (WRITE fans-out to secondary indexes; READ/SEARCH for primaries)
    await _collect(
        _REGISTRY_ITEMS,
        ItemsRoutingConfig,
        [Operation.WRITE, Operation.READ, Operation.SEARCH],
    )

    # Asset drivers
    await _collect(
        _REGISTRY_ASSET,
        AssetRoutingConfig,
        [Operation.WRITE, Operation.UPLOAD, Operation.READ],
    )

    # Collection-metadata drivers (fixes #1750: includes collection_es_driver
    # that owns the singleton {prefix}-collections index)
    await _collect(
        _REGISTRY_COLLECTION,
        CollectionRoutingConfig,
        [Operation.WRITE, Operation.READ, Operation.SEARCH],
    )

    return result


class RoutingDrivenCascadeOwner(BaseResourceOwner):
    """Cascade owner that delegates drop_storage to routing-configured drivers.

    Handles both CATALOG and COLLECTION scope.  For each scope it enumerates
    the drivers configured for that catalog/collection via the routing config
    waterfall (same path as the write/read hot path) and emits one CleanupRef
    per driver.  The cleanup worker then calls each driver's drop_storage with
    the correct (catalog_id, collection_id) arguments.

    Replaces:
    - ``EsItemsPublicIndexOwner`` (es_public.items_index)
    - ``EsAssetIndexOwner`` (es_public.asset_index)
    - ``EsItemsIndexOwner`` (es_private.items_index)
    - ``EsItemsEnvelopeIndexOwner`` (es_envelope.items_index)

    And adds (new coverage fixing #1750):
    - Collection-metadata driver drop_storage for the singleton
      ``{prefix}-collections`` index.
    """

    owner_id: ClassVar[str] = "storage.routing_driven"

    def supported_scopes(self) -> Iterable[ResourceScope]:
        return (ResourceScope.CATALOG, ResourceScope.COLLECTION)

    async def describe_scope(
        self, scope_ref: ScopeRef, conn: Any
    ) -> list[CleanupRef]:
        """Snapshot one CleanupRef per configured driver for the deleted entity.

        Called inside the delete transaction (before schema drop) so that the
        routing config rows are still readable via ``conn``.  Re-raises on
        genuinely broken state so the transaction rolls back (fail-closed).
        """
        pairs = await _enumerate_configured_drivers(scope_ref, conn)

        refs: list[CleanupRef] = []
        for registry_kind, driver_ref in pairs:
            refs.append(
                CleanupRef(
                    kind="storage_driver",
                    locator=driver_ref,
                    owner_id=self.owner_id,
                    metadata={
                        "catalog_id": scope_ref.catalog_id,
                        "collection_id": scope_ref.collection_id,
                        "registry": registry_kind,
                    },
                )
            )

        logger.debug(
            "RoutingDrivenCascadeOwner.describe_scope: scope=%r "
            "catalog_id=%r collection_id=%r -> %d ref(s): %s",
            scope_ref.scope.value,
            scope_ref.catalog_id,
            scope_ref.collection_id,
            len(refs),
            [r.locator for r in refs],
        )
        return refs

    async def cleanup_one(
        self,
        ref: CleanupRef,
        mode: CleanupMode,
        *,
        dry_run: bool = False,
    ) -> CleanupOutcome:
        """Call driver.drop_storage for the driver identified by *ref*.

        Fail-soft: unexpected exceptions return RETRY so the durable task
        framework can retry with backoff rather than losing the cleanup work.

        SOFT mode: most drivers either no-op or raise
        ``SoftDeleteNotSupportedError`` for soft drop_storage.  We call with
        ``soft=True`` and treat ``SoftDeleteNotSupportedError`` as DONE
        (consistent with the retired owners that all returned DONE on SOFT).
        Unexpected exceptions on soft path also return DONE (retain data is
        the safe default on soft).
        """
        if dry_run:
            logger.info(
                "RoutingDrivenCascadeOwner: dry-run — would call drop_storage "
                "on driver=%r registry=%r catalog_id=%r collection_id=%r mode=%r.",
                ref.locator,
                ref.metadata.get("registry"),
                ref.metadata.get("catalog_id"),
                ref.metadata.get("collection_id"),
                mode.value,
            )
            return CleanupOutcome.DONE

        driver = _resolve_driver(ref)
        if driver is None:
            logger.warning(
                "RoutingDrivenCascadeOwner: driver=%r registry=%r not found in "
                "DriverRegistry — nothing to clean up (driver gone or not installed).",
                ref.locator, ref.metadata.get("registry"),
            )
            return CleanupOutcome.DONE

        catalog_id: str = ref.metadata.get("catalog_id", "")
        collection_id: Optional[str] = ref.metadata.get("collection_id")
        is_soft = mode == CleanupMode.SOFT

        try:
            await driver.drop_storage(catalog_id, collection_id, soft=is_soft)
            logger.info(
                "RoutingDrivenCascadeOwner: drop_storage completed for "
                "driver=%r catalog_id=%r collection_id=%r mode=%r.",
                ref.locator, catalog_id, collection_id, mode.value,
            )
            return CleanupOutcome.DONE
        except Exception as exc:  # noqa: BLE001
            exc_name = type(exc).__name__
            # SoftDeleteNotSupportedError means the driver doesn't support soft
            # drop — treat as DONE (retain data is safe).
            if "SoftDeleteNotSupported" in exc_name or (
                is_soft and "not supported" in str(exc).lower()
            ):
                logger.debug(
                    "RoutingDrivenCascadeOwner: driver=%r does not support "
                    "soft drop_storage — treating as DONE.",
                    ref.locator,
                )
                return CleanupOutcome.DONE

            if is_soft:
                # On soft path, unexpected errors should not block soft-deletes.
                logger.warning(
                    "RoutingDrivenCascadeOwner: drop_storage(soft=True) "
                    "failed for driver=%r: %s — treating as DONE.",
                    ref.locator, exc,
                )
                return CleanupOutcome.DONE

            logger.error(
                "RoutingDrivenCascadeOwner: drop_storage failed for "
                "driver=%r catalog_id=%r collection_id=%r: %s — returning RETRY.",
                ref.locator, catalog_id, collection_id, exc,
                exc_info=True,
            )
            return CleanupOutcome.RETRY


def _resolve_driver(ref: CleanupRef) -> Any:
    """Look up the driver instance from the appropriate DriverRegistry index."""
    from dynastore.modules.storage.driver_registry import DriverRegistry

    registry_kind = ref.metadata.get("registry")
    driver_ref = ref.locator

    if registry_kind == _REGISTRY_ITEMS:
        return DriverRegistry.collection_index().get(driver_ref)
    if registry_kind == _REGISTRY_ASSET:
        return DriverRegistry.asset_index().get(driver_ref)
    if registry_kind == _REGISTRY_COLLECTION:
        return DriverRegistry.collection_store_index().get(driver_ref)
    logger.warning(
        "_resolve_driver: unknown registry_kind=%r for driver=%r.",
        registry_kind, driver_ref,
    )
    return None


def register_owners(registry: "CascadeCleanupRegistry") -> None:
    """Register the routing-driven cascade owner into *registry*.

    Call from the catalog module lifespan BEFORE
    :func:`~dynastore.modules.catalog.cascade_registry.finalize_cascade_registry`
    is called.
    """
    registry.register(RoutingDrivenCascadeOwner())
    logger.info(
        "RoutingDrivenCascadeOwner: registered cascade owner %r.",
        RoutingDrivenCascadeOwner.owner_id,
    )
