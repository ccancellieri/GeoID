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

"""Task implementation for the ``stac_harvest`` OGC Process.

Harvests a remote STAC catalog into a local dynastore catalog.  Uses INTERNAL
service protocols — no HTTP self-calls.  Cross-module dependencies are via
protocols only (no direct module imports).
"""
from __future__ import annotations

import asyncio
import json
import logging
import urllib.error
import urllib.request
from dataclasses import dataclass, field
from typing import Any, AsyncIterator, Dict, Iterator, List, Optional, Tuple

from dynastore.models.ogc import Feature
from dynastore.modules.processes.models import ExecuteRequest, Process
from dynastore.modules.processes.protocols import ProcessTaskProtocol
from dynastore.modules.tasks.models import TaskPayload
from dynastore.tools.protocol_helpers import get_engine

from .definition import STAC_HARVEST_PROCESS_DEFINITION
from .models import StacHarvestRequest

logger = logging.getLogger(__name__)

_BATCH_SIZE = 1000
# Source page size for /items.  Kept broadly compatible: some public STAC
# APIs reject large pages (e.g. earth-search returns HTTP 502 for limit=500),
# so default conservatively and shrink adaptively on a fetch error.
_PAGE_LIMIT = 100
_MIN_PAGE_LIMIT = 20
# Cap how many per-batch errors are recorded into the job result.
_MAX_RECORDED_ERRORS = 5
_STRIP_LINKS = frozenset({"links"})
# Concrete write language for collection create/update.  Source STAC
# collections carry no language, and ``"*"`` is a *read-time* wildcard
# (all translations) — passing it to a write throws, which previously
# aborted the whole harvest before any item was written.
_WRITE_LANG = "en"


# ---------------------------------------------------------------------------
# Remote STAC walk helpers (stdlib only — no extra deps)
# ---------------------------------------------------------------------------


def _http_get_json(url: str, *, timeout: int = 60) -> Any:
    """Perform a single GET and return parsed JSON.  Raises on non-2xx."""
    headers = {
        "Accept": "application/json",
        "User-Agent": "dynastore-stac-harvest/1.0",
    }
    req = urllib.request.Request(url, headers=headers, method="GET")
    with urllib.request.urlopen(req, timeout=timeout) as resp:  # noqa: S310
        return json.loads(resp.read().decode())


def _next_href(page: Dict[str, Any]) -> Optional[str]:
    for link in page.get("links") or []:
        if isinstance(link, dict) and link.get("rel") == "next":
            href = link.get("href")
            if href:
                return str(href)
    return None


async def iter_collections(catalog_url: str) -> AsyncIterator[Dict[str, Any]]:
    """Walk source /collections with rel=next cursor pagination."""
    url: Optional[str] = f"{catalog_url}/collections"
    while url:
        try:
            page = await asyncio.to_thread(_http_get_json, url)
        except Exception as exc:
            logger.warning("stac_harvest: GET %s failed: %s", url, exc)
            return
        for coll in page.get("collections") or []:
            yield coll
        url = _next_href(page)


async def iter_items(catalog_url: str, collection_id: str) -> AsyncIterator[Dict[str, Any]]:
    """Walk source /collections/{id}/items with rel=next cursor pagination.

    If the very first page fetch fails (a source may reject the requested page
    size with e.g. HTTP 502), retry the first page with a halved limit down to
    ``_MIN_PAGE_LIMIT`` before giving up — otherwise an over-large default would
    silently harvest zero items.
    """
    limit = _PAGE_LIMIT
    url: Optional[str] = (
        f"{catalog_url}/collections/{collection_id}/items?limit={limit}"
    )
    first_page = True
    while url:
        try:
            page = await asyncio.to_thread(_http_get_json, url)
        except Exception as exc:
            if first_page and limit > _MIN_PAGE_LIMIT:
                limit = max(_MIN_PAGE_LIMIT, limit // 2)
                logger.warning(
                    "stac_harvest: GET items for %s failed (%s) — "
                    "retrying first page with limit=%d",
                    collection_id, exc, limit,
                )
                url = (
                    f"{catalog_url}/collections/{collection_id}/items?limit={limit}"
                )
                continue
            logger.warning(
                "stac_harvest: GET items for %s failed: %s", collection_id, exc
            )
            return
        first_page = False
        for feat in page.get("features") or []:
            yield feat
        url = _next_href(page)


# ---------------------------------------------------------------------------
# Mapping source → dynastore payloads
# ---------------------------------------------------------------------------

_FALLBACK_EXTENT: Dict[str, Any] = {
    "spatial": {"bbox": [[-180.0, -90.0, 180.0, 90.0]]},
    "temporal": {"interval": [[None, None]]},
}


def map_collection(coll: Dict[str, Any]) -> Dict[str, Any]:
    """Map a source STAC collection dict to a dynastore collection payload.

    - Drops ``links`` (server-managed navigation).
    - Drops ``assets`` at collection level (can cause 409 on STAC item writes;
      per-item assets pass through unaffected).
    - Lowercases the ``id`` (dynastore normalises ids; mismatched case between
      collection creation and item writes causes 409 collisions).
    - Ensures required ``extent``.
    """
    out = {k: v for k, v in coll.items() if k not in _STRIP_LINKS and k != "assets"}
    out.setdefault("type", "Collection")
    out["id"] = str(out.get("id", "")).lower()
    out.setdefault("description", out.get("title") or out["id"])
    out.setdefault("extent", _FALLBACK_EXTENT)
    return out


def map_item(feature: Dict[str, Any], target_collection: str) -> Dict[str, Any]:
    """Map a source STAC item; strip navigation links, fix collection reference."""
    out = {k: v for k, v in feature.items() if k not in _STRIP_LINKS}
    out["type"] = "Feature"
    out["collection"] = target_collection
    return out


def virtual_assets_for(feature: Dict[str, Any]) -> Iterator[Dict[str, Any]]:
    """Yield VirtualAssetCreate-compatible dicts for each asset on a source item."""
    item_id = feature.get("id", "item")
    for key, asset in (feature.get("assets") or {}).items():
        href = asset.get("href")
        if not href:
            continue
        media = (asset.get("type") or "").lower()
        asset_type = "RASTER" if ("tiff" in media or "image/" in media) else "ASSET"
        owned_by = "gcs" if "storage.googleapis.com" in href else "http"
        yield {
            "asset_id": f"{item_id}.{key}",
            "href": href,
            "asset_type": asset_type,
            "kind": "virtual",
            "owned_by": owned_by,
            "metadata": {
                "roles": asset.get("roles", []),
                "title": asset.get("title"),
                "media_type": asset.get("type"),
                "source_asset_key": key,
            },
        }


# ---------------------------------------------------------------------------
# Stats dataclass (survives task run)
# ---------------------------------------------------------------------------


@dataclass
class HarvestStats:
    collections_seen: int = 0
    collections_written: int = 0
    items_written: int = 0
    items_failed: int = 0
    virtual_assets_written: int = 0
    errors: List[str] = field(default_factory=list)


# ---------------------------------------------------------------------------
# Core harvest logic — uses internal protocols only
# ---------------------------------------------------------------------------


async def _ensure_collection(
    catalogs: Any,
    catalog_id: str,
    coll: Dict[str, Any],
) -> bool:
    """Upsert the collection; return True when the collection is present afterwards.

    Writes use a concrete language (``_WRITE_LANG``) — never the ``"*"`` read
    wildcard.  On a write exception we re-check existence: a post-write hook
    failure (e.g. a best-effort async indexer) must not abort item ingestion
    when the collection row itself landed.
    """
    cid = coll["id"]
    try:
        existing = await catalogs.get_collection(catalog_id, cid, lang=_WRITE_LANG)
        if existing is None:
            await catalogs.create_collection(catalog_id, coll, lang=_WRITE_LANG)
        else:
            await catalogs.update_collection(catalog_id, cid, coll, lang=_WRITE_LANG)
        return True
    except Exception as exc:
        logger.warning(
            "stac_harvest: upsert collection %s/%s raised %s(%s) — re-checking existence",
            catalog_id, cid, type(exc).__name__, exc,
        )
        # Resilience: if the collection is present despite the raise, proceed
        # to items rather than discarding the whole collection's harvest.
        try:
            if await catalogs.get_collection(catalog_id, cid, lang=_WRITE_LANG) is not None:
                logger.warning(
                    "stac_harvest: collection %s/%s exists post-write — continuing to items",
                    catalog_id, cid,
                )
                return True
        except Exception as recheck_exc:
            logger.warning(
                "stac_harvest: existence re-check for %s/%s failed: %s(%s)",
                catalog_id, cid, type(recheck_exc).__name__, recheck_exc,
            )
        return False


async def _upsert_items_batch(
    catalogs: Any,
    catalog_id: str,
    collection_id: str,
    batch: List[Dict[str, Any]],
) -> Tuple[int, Optional[str]]:
    """Bulk-upsert a batch of STAC items via the CatalogsProtocol.

    Returns ``(written, error)`` — ``error`` is a short ``Type: message`` string
    on failure (``None`` on success) so the caller can surface it in the job
    result, since BackgroundTask log output is not reliably captured at runtime.

    Items are parsed into ``Feature`` models before the write.  The ES-primary
    write path (``item_service.upsert`` → ``ItemsElasticsearchDriver``) returns
    the input entities and the service then reads ``result.id``; a raw ``dict``
    has no ``.id`` and crashes the whole batch.  Parsing here mirrors the HTTP
    ingestion path, which validates dicts into ``Feature`` before writing.
    """
    features: List[Feature] = []
    invalid = 0
    for raw in batch:
        try:
            features.append(Feature.model_validate(raw))
        except Exception as exc:  # malformed source item — drop, keep the batch
            invalid += 1
            logger.warning(
                "stac_harvest: item %s in %s/%s failed Feature validation: %s(%s)",
                raw.get("id"), catalog_id, collection_id, type(exc).__name__, exc,
            )

    if not features:
        return 0, f"all {len(batch)} items failed Feature validation"

    try:
        await catalogs.upsert(catalog_id, collection_id, features)
        # items_failed for this batch == invalid (dropped) items; surface as a
        # soft error when non-zero but the write itself succeeded.
        err = f"{invalid} item(s) failed Feature validation" if invalid else None
        return len(features), err
    except Exception as exc:
        err = f"{type(exc).__name__}: {exc}"
        logger.warning(
            "stac_harvest: bulk write %d items into %s/%s failed: %s",
            len(features), catalog_id, collection_id, err,
        )
        return 0, err


async def _register_virtual_assets(
    catalogs: Any,
    catalog_id: str,
    collection_id: str,
    batch: List[Dict[str, Any]],
) -> int:
    """Register virtual assets for a batch of items; best-effort."""
    from dynastore.modules.catalog.asset_service import VirtualAssetCreate  # late import
    from dynastore.models.shared_models import CoreAssetReferenceType

    written = 0
    for feat in batch:
        # No id → no queryable item→asset back-reference (a sentinel would make
        # featureless items collide on the same ref_id and cross-cascade).
        item_id = feat.get("id")
        for va_dict in virtual_assets_for(feat):
            try:
                va = VirtualAssetCreate(
                    asset_id=va_dict["asset_id"],
                    href=va_dict["href"],
                    metadata=va_dict.get("metadata", {}),
                )
                await catalogs.assets.create_asset(
                    catalog_id=catalog_id,
                    asset=va,
                    collection_id=collection_id,
                )
                written += 1
                if item_id:
                    try:
                        await catalogs.assets.add_asset_reference(
                            asset_id=va.asset_id,
                            catalog_id=catalog_id,
                            ref_type=CoreAssetReferenceType.ITEM,
                            ref_id=item_id,
                            cascade_delete=True,
                        )
                    except Exception as exc:
                        logger.debug(
                            "stac_harvest: item back-ref skip for %s: %s", va.asset_id, exc
                        )
            except Exception as exc:
                # 409 = already registered; treat as success silently
                if "already" in str(exc).lower() or "exists" in str(exc).lower() or "409" in str(exc):
                    written += 1
                    if item_id:
                        try:
                            await catalogs.assets.add_asset_reference(
                                asset_id=va_dict["asset_id"],
                                catalog_id=catalog_id,
                                ref_type=CoreAssetReferenceType.ITEM,
                                ref_id=item_id,
                                cascade_delete=True,
                            )
                        except Exception as ref_exc:
                            logger.debug(
                                "stac_harvest: item back-ref skip for %s: %s",
                                va_dict.get("asset_id"), ref_exc,
                            )
                else:
                    logger.debug(
                        "stac_harvest: virtual asset %s skip: %s", va_dict.get("asset_id"), exc
                    )
    return written


async def _apply_stac_presets_direct(
    ctx: Any,
    scope: str,
    catalog_id: str,
    backend: str,
    storage_params: Any,
    stac_backend: Any,
) -> None:
    """Fallback: apply presets via the direct (non-audited) preset.apply path.

    Used when the IAM audit service / engine is unavailable so the harvest
    can still pin routing/storage configs without a live ``iam.applied_presets``
    table.
    """
    from dynastore.modules.storage.presets.registry import find_preset
    from dynastore.modules.storage.presets.stac import (
        StacPresetParams,
        _items_routing_es,
    )
    from dynastore.modules.stac.stac_storage_config import StacLevel
    from dynastore.modules.storage.routing_config import ItemsRoutingConfig

    storage_preset = find_preset("stac_storage")
    await storage_preset.apply(storage_params, scope, ctx)
    logger.info(
        "stac_harvest: pinned items storage backend=%s on %s (direct path)",
        backend, catalog_id,
    )

    if backend == "es":
        configs = ctx.config
        if configs is not None:
            items_routing = _items_routing_es()
            await configs.set_config(
                ItemsRoutingConfig,
                items_routing,
                catalog_id=catalog_id,
            )
            logger.info(
                "stac_harvest: wrote ES-only ItemsRoutingConfig for catalog=%s (direct path)",
                catalog_id,
            )
        else:
            logger.warning(
                "stac_harvest: ctx.config is None — cannot write "
                "ItemsRoutingConfig for catalog=%s (ES-only preset incomplete)",
                catalog_id,
            )
    else:
        routing_params = StacPresetParams(
            stac_level=StacLevel.ITEMS,
            stac_storage=stac_backend,
        )
        routing_preset = find_preset("stac_routing")
        await routing_preset.apply(routing_params, scope, ctx)
        logger.info(
            "stac_harvest: applied stac_routing preset (backend=%s) on %s (direct path)",
            backend, catalog_id,
        )


async def _apply_stac_presets(
    ctx: Any,
    scope: str,
    catalog_id: str,
    backend: str,
) -> None:
    """Pin item storage routing for the harvested catalog.

    ``backend`` is the resolved storage backend: ``"es"``, ``"es_pg"``, or
    ``"pg"``.  Strategy:

    - Always apply the ``stac_storage`` preset (the SSOT signal) through the
      audited ``apply_preset`` lifecycle, which records the apply in
      ``iam.applied_presets`` for idempotency and stores a revoke descriptor.
    - For ``"es"``: apply the new ``items_es_public`` preset (items-tier only)
      through the lifecycle — avoids the cumulative ``stac_routing`` preset
      which would also touch the collection tier where the
      ``collection_elasticsearch_driver`` is not registered on this deployment.
    - For ``"es_pg"`` or ``"pg"``: apply the ``stac_routing`` preset through
      the lifecycle (these backends validate on all tiers).

    IAM-optional: when the engine or ``AppliedPresetsService`` is unavailable,
    the function falls back to the direct ``preset.apply(...)`` /
    ``ConfigsProtocol.set_config(...)`` path so a deployment without IAM still
    gets routing/storage configs applied.

    ``PresetConflictError`` (already applied with same params — idempotent re-run,
    or in-progress concurrent apply) is swallowed and logged at INFO; the harvest
    continues.

    All operations are best-effort: failures are logged at WARNING (with the
    exception) and never abort the harvest.
    """
    try:
        from dynastore.modules.storage.presets.stac import (
            StacPresetParams,
        )
        from dynastore.modules.stac.stac_storage_config import (
            StacLevel,
            StacStorageBackend,
        )

        backend_map: Dict[str, StacStorageBackend] = {
            "es": StacStorageBackend.ES,
            "es_pg": StacStorageBackend.ES_PG,
            "pg": StacStorageBackend.PG,
        }
        stac_backend = backend_map.get(backend, StacStorageBackend.ES)

        storage_params = StacPresetParams(
            stac_level=StacLevel.ITEMS,
            stac_storage=stac_backend,
        )

        # Resolve engine + audit service (IAM-optional).
        engine = None
        audit = None
        try:
            from dynastore.modules import get_protocol
            from dynastore.models.protocols import DatabaseProtocol
            from dynastore.modules.iam.applied_presets_service import AppliedPresetsService

            db_proto = get_protocol(DatabaseProtocol)
            engine = db_proto.engine if db_proto is not None else None
            if engine is None and ctx is not None:
                engine = getattr(ctx, "db", None)
            if engine is not None:
                audit = AppliedPresetsService(engine)
        except Exception as iam_exc:
            logger.info(
                "stac_harvest: IAM audit service unavailable, using direct preset "
                "path for catalog=%s: %s(%s)",
                catalog_id, type(iam_exc).__name__, iam_exc,
            )

        if audit is None or engine is None:
            # IAM not available — fall back to direct apply path.
            await _apply_stac_presets_direct(
                ctx, scope, catalog_id, backend, storage_params, stac_backend
            )
            return

        # Audited path — routes through apply_preset lifecycle for audit row,
        # idempotency, and stored revoke descriptor.
        from dynastore.modules.storage.presets.lifecycle import apply_preset
        from dynastore.modules.storage.presets.errors import PresetConflictError

        # 1. Apply stac_storage (SSOT flip) — always safe on all backends.
        try:
            await apply_preset("stac_storage", scope, storage_params, ctx, engine, audit)
            logger.info(
                "stac_harvest: pinned items storage backend=%s on %s",
                backend, catalog_id,
            )
        except PresetConflictError as conflict_exc:
            logger.info(
                "stac_harvest: stac_storage already applied at %s — leaving existing "
                "config: %s",
                scope, conflict_exc,
            )

        # 2. Apply routing preset for the items tier.
        if backend == "es":
            # items_es_public: items-tier only — avoids collection-tier ES routing
            # (collection_elasticsearch_driver not registered on ES-primary deployments).
            from dynastore.modules.storage.presets.preset import NoParams
            try:
                await apply_preset(
                    "items_es_public", scope, NoParams(), ctx, engine, audit
                )
                logger.info(
                    "stac_harvest: applied items_es_public preset on %s",
                    catalog_id,
                )
            except PresetConflictError as conflict_exc:
                logger.info(
                    "stac_harvest: items_es_public already applied at %s — leaving "
                    "existing config: %s",
                    scope, conflict_exc,
                )
        else:
            # es_pg / pg: the cumulative stac_routing preset validates fine.
            routing_params = StacPresetParams(
                stac_level=StacLevel.ITEMS,
                stac_storage=stac_backend,
            )
            try:
                await apply_preset(
                    "stac_routing", scope, routing_params, ctx, engine, audit
                )
                logger.info(
                    "stac_harvest: applied stac_routing preset (backend=%s) on %s",
                    backend, catalog_id,
                )
            except PresetConflictError as conflict_exc:
                logger.info(
                    "stac_harvest: stac_routing already applied at %s — leaving "
                    "existing config: %s",
                    scope, conflict_exc,
                )

    except Exception as exc:
        logger.warning(
            "stac_harvest: STAC preset apply failed (non-fatal) on %s: %s(%s)",
            catalog_id, type(exc).__name__, exc,
        )


async def run_harvest(
    request: StacHarvestRequest,
    catalogs: Any,
    preset_ctx: Any,
    scope: str,
) -> HarvestStats:
    """Walk the source STAC catalog and async-write into the local dynastore catalog.

    Parameters
    ----------
    request:
        Validated harvest inputs.
    catalogs:
        CatalogsProtocol implementation from the runtime registry.
    preset_ctx:
        PresetContext built for this scope — used to apply STAC presets.
    scope:
        Resolved scope string (e.g. ``"catalog:fao-gismgr"``).
    """
    stats = HarvestStats()
    stac_presets_applied = False

    async for coll_raw in iter_collections(request.catalog_url):
        if request.max_collections and stats.collections_seen >= request.max_collections:
            break
        stats.collections_seen += 1
        coll = map_collection(coll_raw)
        cid = coll["id"]

        if not await _ensure_collection(catalogs, request.target_catalog, coll):
            stats.errors.append(f"collection:{cid}")
            continue
        stats.collections_written += 1

        # Pin item storage routing once the first collection is created
        # (catalog provisioning is complete at that point).
        if not stac_presets_applied and preset_ctx is not None:
            await _apply_stac_presets(
                preset_ctx, scope, request.target_catalog, request.storage_backend
            )
            stac_presets_applied = True

        batch: List[Dict[str, Any]] = []
        n_items = 0
        async for feat_raw in iter_items(request.catalog_url, coll_raw.get("id", cid)):
            if request.max_items and n_items >= request.max_items:
                break
            batch.append(map_item(feat_raw, cid))
            n_items += 1
            if len(batch) >= _BATCH_SIZE:
                written, err = await _upsert_items_batch(
                    catalogs, request.target_catalog, cid, batch
                )
                stats.items_written += written
                stats.items_failed += len(batch) - written
                if err and len(stats.errors) < _MAX_RECORDED_ERRORS:
                    stats.errors.append(f"items:{cid}:{err}")
                if request.with_assets:
                    stats.virtual_assets_written += await _register_virtual_assets(
                        catalogs, request.target_catalog, cid, batch
                    )
                batch = []

        if batch:
            written, err = await _upsert_items_batch(
                catalogs, request.target_catalog, cid, batch
            )
            stats.items_written += written
            stats.items_failed += len(batch) - written
            if err and len(stats.errors) < _MAX_RECORDED_ERRORS:
                stats.errors.append(f"items:{cid}:{err}")
            if request.with_assets:
                stats.virtual_assets_written += await _register_virtual_assets(
                    catalogs, request.target_catalog, cid, batch
                )

    return stats


# ---------------------------------------------------------------------------
# OGC Process task class
# ---------------------------------------------------------------------------


class StacHarvestTask(
    ProcessTaskProtocol[Process, TaskPayload[ExecuteRequest], Optional[Dict[str, Any]]]
):
    """OGC Process task: walk a remote STAC catalog and async-write into a local one.

    Registered as ``stac_harvest`` via the ``dynastore.tasks`` entry-point.
    Dispatched asynchronously by the ``stac_harvester`` preset's TaskSeed.
    Uses INTERNAL service protocols — no HTTP self-calls.
    """

    task_type: str = "stac_harvest"

    @staticmethod
    def get_definition() -> Process:
        return STAC_HARVEST_PROCESS_DEFINITION

    def __init__(self, app_state: Any = None) -> None:
        self.app_state = app_state
        self.engine = get_engine()

    async def run(
        self, payload: TaskPayload[ExecuteRequest]
    ) -> Optional[Dict[str, Any]]:
        """Entry point called by the task runner.

        The OGC Processes dispatcher wraps user inputs in an ``ExecuteRequest``
        at ``payload.inputs``; the actual harvest params are in
        ``payload.inputs.inputs``.
        """
        from dynastore.models.protocols import CatalogsProtocol
        from dynastore.models.protocols.configs import ConfigsProtocol
        from dynastore.tools.discovery import get_protocol

        raw_inputs = payload.inputs
        if hasattr(raw_inputs, "inputs"):
            inputs_dict = raw_inputs.inputs
        elif isinstance(raw_inputs, dict):
            inputs_dict = raw_inputs.get("inputs", {})
        else:
            inputs_dict = {}

        request = StacHarvestRequest.model_validate(inputs_dict)

        catalogs = get_protocol(CatalogsProtocol)
        if catalogs is None:
            raise RuntimeError(
                "stac_harvest: CatalogsProtocol not available in this service."
            )

        scope = f"catalog:{request.target_catalog}"

        # Build a PresetContext so _apply_stac_presets can write routing/storage
        # configs via ConfigsProtocol.  The config writer is obtained from the
        # protocol registry and injected into the context.  The preset apply is
        # best-effort: a failure logs at WARNING and does not abort the harvest.
        preset_ctx = None
        try:
            from dynastore.modules.storage.presets.preset import PresetContext

            config_writer = get_protocol(ConfigsProtocol)
            preset_ctx = PresetContext(
                db=self.engine,
                iam=None,
                policy=None,
                config=config_writer,
                tasks=None,
                cron=None,
                libs=None,
                principal=None,
                scope=scope,
                catalogs=catalogs,
            )
        except Exception as exc:
            logger.warning(
                "stac_harvest: could not build PresetContext (STAC presets will be "
                "skipped): %s(%s)", type(exc).__name__, exc,
            )

        stats = await run_harvest(request, catalogs, preset_ctx, scope)

        logger.info(
            "stac_harvest: finished — backend=%s collections=%d/%d "
            "items_written=%d items_failed=%d virtual_assets=%d errors=%d",
            request.storage_backend,
            stats.collections_written,
            stats.collections_seen,
            stats.items_written,
            stats.items_failed,
            stats.virtual_assets_written,
            len(stats.errors),
        )

        if stats.errors:
            logger.warning("stac_harvest: errors: %s", stats.errors[:20])

        summary = (
            f"collections={stats.collections_written}/{stats.collections_seen} "
            f"items_written={stats.items_written} items_failed={stats.items_failed} "
            f"virtual_assets={stats.virtual_assets_written} "
            f"backend={request.storage_backend}"
        )
        return {
            "message": summary,
            "collections_written": stats.collections_written,
            "collections_seen": stats.collections_seen,
            "items_written": stats.items_written,
            "items_failed": stats.items_failed,
            "virtual_assets_written": stats.virtual_assets_written,
            "storage_backend": request.storage_backend,
            "errors": stats.errors[:20],
        }
