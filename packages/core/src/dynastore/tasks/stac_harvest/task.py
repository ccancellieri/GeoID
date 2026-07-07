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
import asyncio
import json
import logging
import time
import urllib.error
import urllib.request
from dataclasses import dataclass, field
from typing import Any, AsyncIterator, Dict, Iterator, List, Optional, Tuple

from dynastore.models.ogc import Feature
from dynastore.modules.processes.models import ExecuteRequest, Process
from dynastore.modules.processes.protocols import ProcessTaskProtocol
from dynastore.modules.storage.presets.routing import RoutingDrivers
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
_STAC_COLLECTION_SCHEMA_FIELDS = frozenset({
    "type",
    "stac_version",
    "stac_extensions",
    "id",
    "title",
    "description",
    "keywords",
    "license",
    "providers",
    "extent",
    "summaries",
    "assets",
    "item_assets",
    "links",
    "extra_metadata",
})
_STAC_COLLECTION_FALLBACK_FIELDS = frozenset({
    "assets",
    "extent",
    "item_assets",
    "providers",
    "stac_extensions",
    "summaries",
})
# Concrete write language for collection create/update.  Source STAC
# collections carry no language, and ``"*"`` is a *read-time* wildcard
# (all translations) — passing it to a write throws, which previously
# aborted the whole harvest before any item was written.
_WRITE_LANG = "en"
# geoid#2890: 10 min of unbroken zero-progress write failures aborts the harvest
_STALL_ABORT_SECONDS = 600.0


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


def _items_href(doc: Dict[str, Any]) -> Optional[str]:
    """Return the ``rel=items`` link href of a STAC document, if present."""
    for link in doc.get("links") or []:
        if isinstance(link, dict) and link.get("rel") == "items":
            href = link.get("href")
            if href:
                return str(href)
    return None


def _probe_single_collection(catalog_url: str) -> Optional[Tuple[Dict[str, Any], str]]:
    """Probe the source URL; detect a single STAC Collection.

    Returns ``(collection_doc, items_url)`` when ``catalog_url`` points directly
    at a STAC Collection (a document whose ``type`` is ``Collection`` carrying an
    ``id``), else ``None`` (the caller then walks ``/collections``).  The items
    URL prefers the collection's ``rel=items`` link and falls back to
    ``{catalog_url}/items``.
    """
    try:
        doc = _http_get_json(catalog_url)
    except Exception as exc:
        # Not fatal — a catalog root that is not itself a fetchable JSON doc
        # (or a transient error) just falls through to the /collections walk.
        logger.info(
            "stac_harvest: source probe GET %s failed (%s) — treating as catalog",
            catalog_url, exc,
        )
        return None
    if not isinstance(doc, dict):
        return None
    if str(doc.get("type")) == "Collection" and doc.get("id"):
        return doc, (_items_href(doc) or f"{catalog_url}/items")
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


@dataclass
class PageCursor:
    """Mutable holder updated by ``_iter_items_from`` as it walks source pages.

    ``next_url`` always holds the URL of the page about to be fetched (or
    just fetched, while its items are being consumed) — i.e. the STAC
    ``rel=next`` resume point a caller should persist once the items already
    consumed are durably written (#3034). Re-fetching this URL after a
    resume may re-yield a few items from the in-flight page again, which is
    safe since item upserts are idempotent.

    ``truncated`` is set when the walk gave up on a page fetch (source error,
    after exhausting the limit-shrink retry on the first page) instead of
    reaching a page with no ``rel=next`` link. A caller must not treat a
    truncated walk as "collection fully harvested" — doing so would make a
    resume skip the still-unfetched tail forever; the last page-boundary
    cursor already persisted by a prior successful batch remains the correct
    resume point.
    """

    next_url: Optional[str] = None
    truncated: bool = False


async def _iter_items_from(
    items_url: str, label: str, *, cursor: Optional[PageCursor] = None,
    resume_from_href: bool = False,
) -> AsyncIterator[Dict[str, Any]]:
    """Walk a source items URL with rel=next cursor pagination.

    ``items_url`` is the items endpoint base (it may already carry query
    params) — a ``limit`` is appended and the first page's fetch is retried
    with a halved limit down to ``_MIN_PAGE_LIMIT`` on failure (a source may
    reject the requested page size with e.g. HTTP 502); otherwise an
    over-large default would silently harvest zero items.

    ``resume_from_href=True`` treats ``items_url`` as an already-complete
    page URL instead — a persisted ``rel=next`` cursor (#3034) — fetched
    exactly as-is with no ``limit`` appended (it carries its own) and no
    limit-shrink retry (that already ran, if needed, on the original first
    page of this same walk).

    ``cursor``, when given, is updated to the current page's URL before each
    fetch so a caller can read ``cursor.next_url`` after a batch write commits
    and persist it as the resume point (see ``PageCursor``).
    """
    if resume_from_href:
        url: Optional[str] = items_url
        first_page = False
    else:
        limit = _PAGE_LIMIT
        sep = "&" if "?" in items_url else "?"
        url = f"{items_url}{sep}limit={limit}"
        first_page = True
    while url:
        if cursor is not None:
            cursor.next_url = url
        try:
            page = await asyncio.to_thread(_http_get_json, url)
        except Exception as exc:
            if first_page and limit > _MIN_PAGE_LIMIT:
                limit = max(_MIN_PAGE_LIMIT, limit // 2)
                logger.warning(
                    "stac_harvest: GET items for %s failed (%s) — "
                    "retrying first page with limit=%d",
                    label, exc, limit,
                )
                url = f"{items_url}{sep}limit={limit}"
                continue
            logger.warning(
                "stac_harvest: GET items for %s failed: %s", label, exc
            )
            if cursor is not None:
                cursor.truncated = True
            return
        first_page = False
        for feat in page.get("features") or []:
            yield feat
        url = _next_href(page)


def iter_items(
    catalog_url: str, collection_id: str, *, cursor: Optional[PageCursor] = None,
) -> AsyncIterator[Dict[str, Any]]:
    """Walk source /collections/{id}/items with rel=next cursor pagination."""
    return _iter_items_from(
        f"{catalog_url}/collections/{collection_id}/items", collection_id,
        cursor=cursor,
    )


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
    - Lowercases the ``id`` (dynastore normalises ids; mismatched case between
      collection creation and item writes causes 409 collisions).
    - Ensures required ``extent``.
    - Mirrors source STAC extras into ``extra_metadata`` so rich collection
      metadata survives generic CatalogsProtocol writes even when the
      collection_stac sidecar is not active.
    """
    out = {k: v for k, v in coll.items() if k not in _STRIP_LINKS}

    extras: Dict[str, Any] = {}
    for key, value in out.items():
        if key == "extra_metadata" or value is None:
            continue
        if key not in _STAC_COLLECTION_SCHEMA_FIELDS:
            extras[key] = value
    for key in _STAC_COLLECTION_FALLBACK_FIELDS:
        value = out.get(key)
        if value:
            extras[key] = value

    if extras:
        existing = out.get("extra_metadata")
        if isinstance(existing, dict):
            existing.update(extras)
        else:
            out["extra_metadata"] = extras

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
    collections_skipped_empty: int = 0
    items_written: int = 0
    items_failed: int = 0
    virtual_assets_written: int = 0
    errors: List[str] = field(default_factory=list)
    # Monotonic timestamp of the first failure in the current unbroken
    # zero-write streak; ``None`` while healthy.  See _STALL_ABORT_SECONDS.
    _stall_since: Optional[float] = field(default=None, repr=False)


async def _persist_harvest_cursor(
    engine: Any,
    task_id: Any,
    collection_id: Optional[str],
    items_href: Optional[str],
    done: bool,
) -> None:
    """Persist a resume cursor onto the task row after progress is durable (#3034).

    Stamps ``inputs.inputs.resume`` on the task's own DB row so a dispatcher
    retry (Cloud Run Job timeout/kill) resumes the source walk instead of
    replaying the whole catalog from the first collection — mirroring
    ingestion's ``_persist_ingestion_cursor`` (#2820).

    Best-effort: a write failure here only degrades to a retry restarting the
    affected collection from the beginning, and must never fail an otherwise
    successful batch write. No-ops when ``engine``/``task_id`` are unavailable
    (e.g. a sync in-process execution path with no durable task row to stamp).
    """
    if engine is None or not task_id:
        return
    try:
        from dynastore.modules.tasks import tasks_module
        import uuid as _uuid

        task_uuid = task_id if isinstance(task_id, _uuid.UUID) else _uuid.UUID(str(task_id))
        await tasks_module.update_task_harvest_cursor(
            engine, task_uuid, collection_id, items_href, done,
        )
    except Exception:  # noqa: BLE001 — cursor persistence is best-effort
        logger.warning(
            "stac_harvest: failed to persist resume cursor (collection=%s "
            "done=%s) for task %s — a retry will restart that collection "
            "from the beginning.",
            collection_id, done, task_id, exc_info=True,
        )


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


async def _apply_presets_direct(
    ctx: Any,
    scope: str,
    catalog_id: str,
    drivers: "RoutingDrivers",
    routing_params: Any,
    storage_params: Any,
) -> None:
    """Fallback: apply presets via the direct (non-audited) ``preset.apply`` path.

    Used when the IAM audit service / engine is unavailable so the harvest can
    still pin routing/storage configs without a live ``iam.applied_presets`` table.
    Applies ``routing`` first (so the drivers the SSOT references exist before the
    signal flips), then ``stac_storage``.
    """
    from dynastore.modules.storage.presets.registry import find_preset

    routing_preset = find_preset("routing")
    await routing_preset.apply(routing_params, scope, ctx)
    storage_preset = find_preset("stac_storage")
    await storage_preset.apply(storage_params, scope, ctx)
    logger.info(
        "stac_harvest: applied routing(drivers=%s) + stac_storage on %s (direct path)",
        drivers.value, scope,
    )


async def _apply_harvest_presets(
    ctx: Any,
    scope: str,
    catalog_id: str,
    drivers: "RoutingDrivers",
) -> Optional[str]:
    """Pin storage routing + enable STAC for the harvest, at ``scope``.

    Two orthogonal presets are applied at the resolved scope (catalog scope for a
    catalog harvest → collection + items routing; collection scope for a
    single-collection harvest → items routing only):

    - ``routing`` (parametrised by ``drivers``) decides which drivers handle
      WRITE / READ / SEARCH.  Pinning it BEFORE any collection or item is written
      is load-bearing: on an ES-primary deployment an unpinned first
      ``create_collection`` fans the metadata write to every registered
      CollectionStore driver inside the registry transaction — including a
      synchronous, fatal Elasticsearch write that, on failure, rolls back the
      registry row and leaves the catalog on the PG default.  Pinning routing
      up front routes the very first collection to the intended backend only.
    - ``stac_storage`` writes the ``StacStorageConfig`` SSOT (level=ITEMS, backend
      derived from ``drivers``) that enables STAC materialisation — the PG STAC
      sidecar when PG is in the backend, the ES STAC route when ES is.

    IAM-optional: when the engine or ``AppliedPresetsService`` is unavailable the
    function falls back to the direct ``preset.apply(...)`` path so a deployment
    without IAM still gets the configs applied.  ``PresetConflictError`` (an
    idempotent re-run with the same params, or an in-progress concurrent apply)
    is swallowed and logged at INFO.  All operations are best-effort: a failure
    never aborts the harvest, but it IS returned as a short error string (and the
    caller folds it into the job-result errors) so a routing that silently fell
    back to the platform default is visible.  Returns ``None`` on success.
    """
    try:
        from dynastore.modules.storage.presets.routing import (
            RoutingPresetParams,
            backend_from_drivers,
        )
        from dynastore.modules.storage.presets.stac import StacPresetParams
        from dynastore.modules.stac.stac_storage_config import StacLevel

        routing_params = RoutingPresetParams(drivers=drivers)
        storage_params = StacPresetParams(
            stac_level=StacLevel.ITEMS,
            stac_storage=backend_from_drivers(drivers),
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
            await _apply_presets_direct(
                ctx, scope, catalog_id, drivers, routing_params, storage_params
            )
            return None

        # Audited path — routes through apply_preset lifecycle for the audit row,
        # idempotency, and stored revoke descriptor.  Routing first, then SSOT.
        from dynastore.modules.storage.presets.lifecycle import apply_preset
        from dynastore.modules.storage.presets.errors import PresetConflictError

        try:
            await apply_preset("routing", scope, routing_params, ctx, engine, audit)
            logger.info(
                "stac_harvest: applied routing(drivers=%s) on %s",
                drivers.value, scope,
            )
        except PresetConflictError as conflict_exc:
            logger.info(
                "stac_harvest: routing already applied at %s — leaving existing "
                "config: %s",
                scope, conflict_exc,
            )

        try:
            await apply_preset("stac_storage", scope, storage_params, ctx, engine, audit)
            logger.info(
                "stac_harvest: enabled STAC (stac_storage) on %s", scope,
            )
        except PresetConflictError as conflict_exc:
            logger.info(
                "stac_harvest: stac_storage already applied at %s — leaving existing "
                "config: %s",
                scope, conflict_exc,
            )

        return None

    except Exception as exc:
        # Surface the failure to the caller (folded into the job result errors)
        # rather than silently masking it — an unpinned routing here means the
        # catalog falls back to the platform default instead of the requested
        # ``drivers``, which is otherwise invisible (the harvest still "succeeds").
        logger.warning(
            "stac_harvest: preset apply failed (non-fatal) on %s: %s(%s)",
            scope, type(exc).__name__, exc,
        )
        return f"routing_preset_apply:{type(exc).__name__}:{str(exc)[:240]}"


async def _apply_collection_read_policy(
    config_writer: Any,
    catalog_id: str,
    collection_id: str,
    external_id_as_feature_id: bool,
) -> Optional[str]:
    """Pin the harvested collection's read-time item-id wire shape (#3070).

    A harvested collection mirrors a remote STAC source whose item ``id`` is the
    authored provider id; dynastore keeps it on ingest as the row's
    ``external_id``. Setting the collection's
    ``ItemsReadPolicy.feature_type.external_id_as_feature_id`` makes both STAC and
    OGC Features surface that source id as the item id, so a link walked back to
    the upstream catalog resolves — instead of exposing the internal geoid the
    default read policy would (post-#3070). ``ItemsReadPolicy`` is collection
    -scoped only (no catalog/platform tier), so it is written per collection here
    rather than through the catalog-scoped harvest presets.

    Best-effort, mirroring ``_apply_harvest_presets``: a failure is logged at
    WARNING and returned as a soft error string (recorded by the caller) so it
    never aborts the item walk. No-ops when no config writer is available.
    """
    if config_writer is None:
        return None
    try:
        from dynastore.modules.storage.read_policy import ItemsReadPolicy
        from dynastore.modules.storage.computed_fields import FeatureType

        policy = ItemsReadPolicy(
            feature_type=FeatureType(
                external_id_as_feature_id=external_id_as_feature_id
            )
        )
        await config_writer.set_config(
            ItemsReadPolicy, policy, catalog_id, collection_id
        )
        logger.info(
            "stac_harvest: pinned items_read_policy("
            "external_id_as_feature_id=%s) on %s/%s",
            external_id_as_feature_id, catalog_id, collection_id,
        )
        return None
    except Exception as exc:  # noqa: BLE001 — read-policy pin is best-effort
        logger.warning(
            "stac_harvest: failed to pin items_read_policy on %s/%s: %s(%s)",
            catalog_id, collection_id, type(exc).__name__, exc,
        )
        return f"read_policy:{collection_id}:{type(exc).__name__}"


async def _harvest_collection(
    catalogs: Any,
    request: StacHarvestRequest,
    source_coll: Dict[str, Any],
    items_iter: AsyncIterator[Dict[str, Any]],
    target_collection: str,
    stats: HarvestStats,
    *,
    source_collection_id: Optional[str] = None,
    engine: Any = None,
    task_id: Any = None,
    page_cursor: Optional[PageCursor] = None,
    config_writer: Any = None,
) -> None:
    """Upsert one collection and stream its items into ``target_collection``.

    ``source_coll`` is the raw source collection dict; ``items_iter`` yields its
    raw items.  Increments ``stats`` in place; never raises (failures are
    recorded as soft errors) — except when a sustained zero-progress write
    streak trips the stall abort (see ``_STALL_ABORT_SECONDS``), which raises
    ``RuntimeError`` to stop a harvest that is stuck heartbeating but making
    no progress (geoid#2890).

    ``source_collection_id`` is the *source* collection id (pre-normalisation)
    used to key the persisted resume cursor (#3034) — it must match what
    ``iter_collections``/the single-collection probe hand back, since that is
    what a resumed walk compares against. Defaults to ``target_collection``
    when not given. When ``engine``/``task_id`` are given, the cursor is
    stamped after each batch write commits (so a dispatcher retry resumes
    this collection's items walk instead of restarting it) and once more
    with ``done=True`` after the whole collection drains (so a resumed
    catalog walk skips it entirely).
    """
    source_collection_id = source_collection_id or target_collection
    target_catalog = request.target_catalog
    coll = map_collection(source_coll)
    coll["id"] = target_collection
    if not await _ensure_collection(catalogs, target_catalog, coll):
        stats.errors.append(f"collection:{target_collection}")
        return
    stats.collections_written += 1

    # Pin the item-id wire shape for this collection (#3070) before its items
    # are read back, so the harvested item id round-trips the source STAC id.
    perr = await _apply_collection_read_policy(
        config_writer, target_catalog, target_collection,
        request.external_id_as_feature_id,
    )
    if perr and len(stats.errors) < _MAX_RECORDED_ERRORS:
        stats.errors.append(perr)

    async def _flush(batch: List[Dict[str, Any]]) -> None:
        written, err = await _upsert_items_batch(
            catalogs, target_catalog, target_collection, batch
        )
        stats.items_written += written
        stats.items_failed += len(batch) - written
        if err and len(stats.errors) < _MAX_RECORDED_ERRORS:
            stats.errors.append(f"items:{target_collection}:{err}")
        if written == 0:
            now = time.monotonic()
            if stats._stall_since is None:
                stats._stall_since = now
            elapsed = now - stats._stall_since
            if elapsed >= _STALL_ABORT_SECONDS:
                raise RuntimeError(
                    f"stac_harvest: aborting — no items written for "
                    f"{elapsed:.0f}s (last error: {err})"
                )
        else:
            stats._stall_since = None
            await _persist_harvest_cursor(
                engine, task_id, source_collection_id,
                page_cursor.next_url if page_cursor else None, False,
            )
        if request.with_assets:
            stats.virtual_assets_written += await _register_virtual_assets(
                catalogs, target_catalog, target_collection, batch
            )

    batch: List[Dict[str, Any]] = []
    n_items = 0
    async for feat_raw in items_iter:
        if request.max_items and n_items >= request.max_items:
            break
        batch.append(map_item(feat_raw, target_collection))
        n_items += 1
        if len(batch) >= _BATCH_SIZE:
            await _flush(batch)
            batch = []
    if batch:
        await _flush(batch)

    if page_cursor is not None and page_cursor.truncated:
        # The walk gave up on a page fetch instead of reaching a page with no
        # rel=next link — a source hiccup, not completion. Marking this
        # collection "done" would make a resume skip its unfetched tail
        # forever; leave whatever the last successful batch's flush already
        # persisted as the resume point (see PageCursor.truncated).
        logger.warning(
            "stac_harvest: %s items walk ended on a page-fetch error, not "
            "exhaustion — leaving the resume cursor at the last committed "
            "batch instead of marking the collection done.",
            source_collection_id,
        )
    else:
        # The whole collection drained without tripping the stall abort —
        # mark it done so a resumed catalog walk skips it entirely instead
        # of re-checking its (now stale) items_href.
        await _persist_harvest_cursor(engine, task_id, source_collection_id, None, True)


async def _prepend_item(
    first: Dict[str, Any],
    rest: AsyncIterator[Dict[str, Any]],
) -> AsyncIterator[Dict[str, Any]]:
    yield first
    async for item in rest:
        yield item


async def _skip_empty_collection_if_requested(
    request: StacHarvestRequest,
    items_iter: AsyncIterator[Dict[str, Any]],
    target_collection: str,
    stats: HarvestStats,
    *,
    source_collection_id: str,
    page_cursor: Optional[PageCursor] = None,
    engine: Any = None,
    task_id: Any = None,
) -> Optional[AsyncIterator[Dict[str, Any]]]:
    """Return an item iterator, or ``None`` when an empty source is skipped."""
    if not request.skip_empty_collections:
        return items_iter

    try:
        first = await anext(items_iter)
    except StopAsyncIteration:
        if page_cursor is not None and page_cursor.truncated:
            return items_iter
        stats.collections_skipped_empty += 1
        logger.info(
            "stac_harvest: skipping empty source collection %s → %s",
            source_collection_id, target_collection,
        )
        await _persist_harvest_cursor(
            engine, task_id, source_collection_id, None, True
        )
        return None

    return _prepend_item(first, items_iter)


async def run_harvest(
    request: StacHarvestRequest,
    catalogs: Any,
    preset_ctx: Any,
    base_scope: str,
    *,
    engine: Any = None,
    task_id: Any = None,
) -> HarvestStats:
    """Walk the source STAC catalog (or single collection) and write locally.

    Detects whether ``request.catalog_url`` points at a single STAC Collection
    or a full catalog, resolves the apply scope accordingly (collection scope for
    a single-collection harvest, catalog scope otherwise), pins routing + STAC
    BEFORE the first write, then ingests.

    When ``request.resume`` is set (#3034) — stamped by a previous attempt of
    this same task via ``engine``/``task_id`` — a full-catalog walk skips every
    source collection up to and including ``resume.collection_id`` (already
    completed or in progress in a prior attempt) and resumes that collection's
    items walk from ``resume.items_href`` instead of restarting the whole
    catalog from its first collection. A single-collection harvest resumes its
    one collection's items walk directly from ``resume.items_href``.

    Parameters
    ----------
    request:
        Validated harvest inputs.
    catalogs:
        CatalogsProtocol implementation from the runtime registry.
    preset_ctx:
        PresetContext used to apply the routing / stac_storage presets.
    base_scope:
        The catalog-scope string (``"catalog:{target_catalog}"``).
    engine, task_id:
        DB engine + this task's own row id, used to persist the resume cursor
        after each batch commits. Cursor persistence is skipped (best-effort,
        never fatal) when either is ``None``.
    """
    stats = HarvestStats()
    target_catalog = request.target_catalog
    resume = request.resume

    # Detect a single-collection source (blocking probe off the event loop).
    single = await asyncio.to_thread(_probe_single_collection, request.catalog_url)

    if single is not None:
        source_coll, items_url = single
        target_col = str(request.target_collection or source_coll.get("id", "")).lower()
        source_col_id = str(source_coll.get("id", target_col))
        logger.info(
            "stac_harvest: single-collection source %s → collection %s",
            request.catalog_url, target_col,
        )
        if resume is not None and resume.done:
            # A prior attempt already drained this collection fully; nothing
            # left to walk (should be rare — a COMPLETED task is not retried).
            logger.info(
                "stac_harvest: resume cursor marks %s already done — skipping "
                "items walk.", target_col,
            )
            return stats
        # Pin routing at CATALOG scope (collection + items templates), not the
        # narrower collection scope.  ``create_collection`` for the target
        # collection resolves its CollectionRoutingConfig at catalog scope; the
        # ``routing`` preset writes ITEMS-only at collection scope, so a
        # collection-scope apply would leave the collection write on the catalog
        # default — re-triggering the #2259 fan-out-rollback on an ES-only
        # deployment.  The target collection inherits both pinned templates.
        if preset_ctx is not None:
            perr = await _apply_harvest_presets(
                preset_ctx, base_scope, target_catalog, request.drivers
            )
            if perr:
                stats.errors.append(perr)
        stats.collections_seen = 1
        page_cursor = PageCursor()
        resume_href = resume.items_href if resume is not None else None
        items_iter = _iter_items_from(
            resume_href or items_url, target_col, cursor=page_cursor,
            resume_from_href=bool(resume_href),
        )
        if resume_href:
            logger.info(
                "stac_harvest: resuming %s items walk from persisted cursor.",
                target_col,
            )
        items_iter = await _skip_empty_collection_if_requested(
            request, items_iter, target_col, stats,
            source_collection_id=source_col_id, page_cursor=page_cursor,
            engine=engine, task_id=task_id,
        )
        if items_iter is None:
            return stats
        await _harvest_collection(
            catalogs, request, source_coll, items_iter, target_col, stats,
            source_collection_id=source_col_id, engine=engine, task_id=task_id,
            page_cursor=page_cursor,
            config_writer=getattr(preset_ctx, "config", None),
        )
        return stats

    # Catalog source — pin routing/STAC at catalog scope BEFORE the loop, then
    # walk /collections.
    if preset_ctx is not None:
        perr = await _apply_harvest_presets(
            preset_ctx, base_scope, target_catalog, request.drivers
        )
        if perr:
            stats.errors.append(perr)

    # Re-walking /collections is cheap (a handful of paginated list requests)
    # even on a resume — only the per-item walk below needs to skip already
    # -completed work. ``found_resume_point`` gates the skip: True from the
    # start when there is nothing to resume.
    found_resume_point = resume is None or not resume.collection_id

    async for coll_raw in iter_collections(request.catalog_url):
        if request.max_collections and stats.collections_seen >= request.max_collections:
            break
        cid = str(map_collection(coll_raw)["id"])
        source_cid = str(coll_raw.get("id", cid))

        if not found_resume_point:
            # found_resume_point is only False when resume.collection_id is
            # truthy (see its definition above), so resume is never None here.
            assert resume is not None
            if source_cid != resume.collection_id:
                # Completed by a prior attempt (walk order precedes the
                # resume point) — skip its items walk entirely.
                continue
            found_resume_point = True
            if resume.done:
                # This collection itself finished in a prior attempt too;
                # resume from the one after it.
                continue
            resume_href = resume.items_href
        else:
            resume_href = None

        stats.collections_seen += 1
        page_cursor = PageCursor()
        items_iter = (
            _iter_items_from(
                resume_href, source_cid, cursor=page_cursor,
                resume_from_href=True,
            )
            if resume_href
            else iter_items(request.catalog_url, source_cid, cursor=page_cursor)
        )
        if resume_href:
            logger.info(
                "stac_harvest: resuming %s items walk from persisted cursor.",
                cid,
            )
        items_iter = await _skip_empty_collection_if_requested(
            request, items_iter, cid, stats,
            source_collection_id=source_cid, page_cursor=page_cursor,
            engine=engine, task_id=task_id,
        )
        if items_iter is None:
            continue
        await _harvest_collection(
            catalogs, request, coll_raw, items_iter, cid, stats,
            source_collection_id=source_cid, engine=engine, task_id=task_id,
            page_cursor=page_cursor,
            config_writer=getattr(preset_ctx, "config", None),
        )

    if not found_resume_point:
        # The persisted resume_collection_id never turned up while re-walking
        # /collections — the source likely renamed or removed it since the
        # last attempt. Every collection this attempt saw got skipped as
        # "already done", so silently returning here would report a false
        # success with 0 collections/items harvested. Fail loudly instead of
        # masking a source-side change behind an empty result.
        assert resume is not None  # implied by found_resume_point being False
        raise RuntimeError(
            f"stac_harvest: resume cursor collection_id={resume.collection_id!r} "
            "was not found while re-walking /collections — it may have been "
            "renamed or removed at the source since the previous attempt; "
            "resubmit without a resume cursor for a fresh full harvest."
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

        stats = await run_harvest(
            request, catalogs, preset_ctx, scope,
            engine=self.engine, task_id=payload.task_id,
        )

        logger.info(
            "stac_harvest: finished — drivers=%s collections=%d/%d "
            "skipped_empty=%d items_written=%d items_failed=%d "
            "virtual_assets=%d errors=%d",
            request.drivers.value,
            stats.collections_written,
            stats.collections_seen,
            stats.collections_skipped_empty,
            stats.items_written,
            stats.items_failed,
            stats.virtual_assets_written,
            len(stats.errors),
        )

        if stats.errors:
            logger.warning("stac_harvest: errors: %s", stats.errors[:20])

        summary = (
            f"collections={stats.collections_written}/{stats.collections_seen} "
            f"skipped_empty={stats.collections_skipped_empty} "
            f"items_written={stats.items_written} items_failed={stats.items_failed} "
            f"virtual_assets={stats.virtual_assets_written} "
            f"errors={len(stats.errors)} "
            f"drivers={request.drivers.value}"
        )
        if (
            stats.collections_seen > 0
            and stats.collections_written == 0
            and stats.items_written == 0
            and stats.errors
        ):
            # Every collection errored and nothing was written: reporting
            # "successful" here masks a total write failure behind soft
            # per-collection errors. Partial failure stays successful (the
            # summary carries the error count); total failure must not.
            raise RuntimeError(
                f"stac_harvest: nothing harvested — {summary}; "
                f"first errors: {stats.errors[:5]}"
            )
        return {
            "message": summary,
            "collections_written": stats.collections_written,
            "collections_seen": stats.collections_seen,
            "collections_skipped_empty": stats.collections_skipped_empty,
            "items_written": stats.items_written,
            "items_failed": stats.items_failed,
            "virtual_assets_written": stats.virtual_assets_written,
            "drivers": request.drivers.value,
            "errors": stats.errors[:20],
        }
