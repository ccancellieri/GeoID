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

"""Routing-driven cascade owner — delegates drop_storage to configured drivers.

Replaces the four hard-coded ES index owners (public items, asset, private
items, envelope) with a single owner that:

1. Enumerates the drivers configured for the deleted catalog/collection by
   calling ``_resolve_driver_ids_cached`` for each relevant routing-config
   class and operation combination.
2. Emits one ``CleanupRef`` per ``(registry_kind, driver_ref)`` pair found,
   for drivers whose ``teardown_lane`` is ``ASYNC_CASCADE``.
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

Driver teardown classification uses the driver-declared ``teardown_lane``
``ClassVar`` (``TeardownLane`` enum) instead of substring-matching on the
driver id string:
- ``INLINE_TXN`` — PG family: storage is dropped synchronously inside the
  delete transaction and must NOT be re-dropped here (table-lock race).
- ``ASYNC_CASCADE`` (default) — enqueued for async ``drop_storage``.
- ``ASYNC_DEDICATED`` — handled by a dedicated owner (reserved for a GCS
  routing driver if one is ever registered; none exists currently).
- ``NONE`` — no teardown needed (e.g. BigQuery read-only view driver).

GCS binary storage cleanup is handled by the dedicated
``GcsCatalogPrefixOwner`` / ``GcsCollectionPrefixOwner`` owners (task-runner
based).  No GCS/GCP driver class is registered in the routing DriverRegistry,
so no ASYNC_DEDICATED lane filtering is needed in practice today.

Per-asset event-driven binary teardown (``AssetBlobReaper``-style) remains
outside the cascade registry — it is a per-asset eventing concern, not a
bulk cascade.

Register via :func:`register_owners` from the catalog module lifespan BEFORE
the CascadeCleanupRegistry is frozen.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any, ClassVar, Iterable, Optional

from dynastore.models.protocols.teardown_lane import TeardownLane
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


async def _enumerate_configured_drivers(
    scope_ref: ScopeRef,
) -> list[tuple[str, str]]:
    """Return ``[(registry_kind, driver_ref), ...]`` for the deleted entity.

    Calls ``_resolve_driver_ids_cached`` for each routing-config class and
    operation that is relevant to the scope.  Deduplicates the result so each
    ``(registry_kind, driver_ref)`` pair appears at most once, regardless of
    how many operations reference it.

    Safe when no explicit config exists: ``_resolve_driver_ids_cached``
    returns an empty list for the legitimate "no config / no driver for this
    operation" case (its no-row path fires the model's default factory, and a
    genuinely empty operation yields ``[]``).  It does NOT raise there, so a
    catalog with no routing config simply enumerates no extra drivers.

    Fails CLOSED on infrastructure failure.  The resolver only RAISES when
    routing cannot be resolved at all — most importantly
    ``RuntimeError("ConfigsProtocol not available …")`` on a configs outage,
    or an underlying DB/cache error.  Those propagate unfiltered so
    ``describe_scope`` fails and the delete transaction rolls back.  Earlier
    code suppressed any exception whose message contained
    ``"configsprotocol not available"`` / ``"not available"`` as "benign",
    which inverted the safety posture: a real configs outage returned an
    under-enumerated driver set and the delete proceeded while silently
    leaking the tenant's storage (#1764).  Substring-matching exception
    messages is brittle; there is no benign exception to suppress here, so the
    suppression is removed entirely.
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
            # No exception is suppressed here (fail-closed). The resolver
            # returns ``[]`` for the legitimate no-config / no-driver case, so
            # the only way it raises is an infrastructure failure — e.g.
            # ``ConfigsProtocol not available`` on a configs outage. Letting
            # that propagate rolls back the delete instead of proceeding with
            # an under-enumerated driver set and leaking tenant storage (#1764).
            entries = await _resolve_driver_ids_cached(
                routing_cls,
                catalog_id,
                collection_id,
                op,
                frozenset(),
            )

            for driver_ref, _on_failure in entries:
                key = (registry_kind, driver_ref)
                # Process each (registry_kind, driver_ref) at most once, even
                # when the same driver is configured for several operations —
                # regardless of lane, so skipped drivers are not re-resolved or
                # re-logged per operation.
                if key in seen:
                    continue
                seen.add(key)

                # Resolve the driver to inspect its declared teardown_lane.
                # If resolution fails (driver not installed / registry gap),
                # default to ASYNC_CASCADE so cleanup_one still runs — it
                # already treats a missing driver as DONE (fail-safe: never
                # silently drop a teardown work item).
                driver = _resolve_driver_by_parts(registry_kind, driver_ref)
                lane = getattr(driver, "teardown_lane", TeardownLane.ASYNC_CASCADE)

                if lane is TeardownLane.ASYNC_CASCADE:
                    result.append(key)
                elif lane is TeardownLane.INLINE_TXN:
                    logger.debug(
                        "_enumerate_configured_drivers: skipping INLINE_TXN "
                        "driver %r (storage dropped inside the delete transaction; "
                        "async re-drop would race the table lock).",
                        driver_ref,
                    )
                elif lane is TeardownLane.ASYNC_DEDICATED:
                    logger.debug(
                        "_enumerate_configured_drivers: skipping ASYNC_DEDICATED "
                        "driver %r (handled by a dedicated cascade owner).",
                        driver_ref,
                    )
                elif lane is TeardownLane.NONE:
                    logger.debug(
                        "_enumerate_configured_drivers: skipping NONE-lane "
                        "driver %r (no teardown needed).",
                        driver_ref,
                    )
                else:
                    # Unknown future lane value — default to ASYNC_CASCADE for safety.
                    logger.warning(
                        "_enumerate_configured_drivers: unknown teardown_lane %r "
                        "for driver %r — defaulting to ASYNC_CASCADE.",
                        lane, driver_ref,
                    )
                    result.append(key)

    # Items drivers (WRITE = primary; INDEX = materialized/derived stores; READ = primaries)
    await _collect(
        _REGISTRY_ITEMS,
        ItemsRoutingConfig,
        [Operation.WRITE, Operation.READ, Operation.INDEX],
    )

    # Asset drivers
    await _collect(
        _REGISTRY_ASSET,
        AssetRoutingConfig,
        [Operation.WRITE, Operation.UPLOAD, Operation.READ, Operation.INDEX],
    )

    # Collection-metadata drivers (fixes #1750: includes collection_es_driver
    # that owns the singleton {prefix}-collections index)
    await _collect(
        _REGISTRY_COLLECTION,
        CollectionRoutingConfig,
        [Operation.WRITE, Operation.READ, Operation.INDEX],
    )

    return result


def _resolve_driver_by_parts(registry_kind: str, driver_ref: str) -> Any:
    """Look up a driver instance from ``(registry_kind, driver_ref)``.

    Shared resolution path used by both ``_enumerate_configured_drivers``
    (to read ``teardown_lane``) and ``_resolve_driver`` (called from
    ``cleanup_one``).  Returns ``None`` when the driver is absent from the
    registry (not installed, wrong scope, or registry not yet populated).
    """
    from dynastore.modules.storage.driver_registry import DriverRegistry

    if registry_kind == _REGISTRY_ITEMS:
        return DriverRegistry.collection_index().get(driver_ref)
    if registry_kind == _REGISTRY_ASSET:
        return DriverRegistry.asset_index().get(driver_ref)
    if registry_kind == _REGISTRY_COLLECTION:
        return DriverRegistry.collection_store_index().get(driver_ref)
    logger.warning(
        "_resolve_driver_by_parts: unknown registry_kind=%r for driver=%r.",
        registry_kind, driver_ref,
    )
    return None


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

        Called inside the delete transaction (before the catalog's schema
        drop). The routing config is read by ``_enumerate_configured_drivers``
        via ``ConfigsProtocol`` on its own pooled connection — NOT via the
        delete txn's ``conn`` — and reliably observes the *pre-delete* routing
        config: the platform ``configs`` rows are untouched by the delete, and
        the catalog's own schema drop is not yet committed at describe time.
        ``conn`` is retained to satisfy the ``ResourceOwner`` interface (other
        owners read tenant tables within the delete txn). Re-raises on
        infrastructure failure so the transaction rolls back (fail-closed).
        """
        del conn  # see docstring — config is read via ConfigsProtocol, not this txn
        pairs = await _enumerate_configured_drivers(scope_ref)

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
    return _resolve_driver_by_parts(
        ref.metadata.get("registry", ""),
        ref.locator,
    )


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
