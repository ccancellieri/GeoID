"""
BucketAnnotationPatcher — best-effort GCS object metadata mirror.

Subscribes to ``CatalogEventType.ASSET_*`` events. When an upserted asset is
backed by GCS (``owned_by == "gcs"`` and URI ``gs://``), patches the GCS
object's ``metadata`` dict to mirror the current asset row state.

Status: advisory. Asset rows + the events outbox are the source of truth;
bucket-side annotations are forensics. Any GCS API failure is logged and
swallowed — never raised — so a transient bucket outage cannot retro-block an
already-committed asset row write.

Mirrors the registration shape of
``dynastore.modules.catalog.asset_sync.AssetEntitySyncSubscriber``.
"""

import logging
from typing import Any, Dict, Optional, Tuple
from urllib.parse import urlparse

from dynastore.modules import get_protocol
from dynastore.modules.catalog.event_service import (
    CatalogEventType,
    async_event_listener,
)
from dynastore.modules.concurrency import run_in_thread

logger = logging.getLogger(__name__)


def _parse_gs_uri(uri: str) -> Optional[Tuple[str, str]]:
    parsed = urlparse(uri)
    if parsed.scheme != "gs" or not parsed.netloc:
        return None
    blob_path = parsed.path.lstrip("/")
    if not blob_path:
        return None
    return parsed.netloc, blob_path


def _build_metadata(payload: Dict[str, Any], asset_id: str) -> Dict[str, str]:
    raw = payload.get("metadata") or {}
    out: Dict[str, str] = {
        str(k): str(v)
        for k, v in raw.items()
        if v is not None
    }
    out["asset_id"] = asset_id
    asset_type = payload.get("asset_type")
    if asset_type is not None:
        out["asset_type"] = (
            asset_type.value if hasattr(asset_type, "value") else str(asset_type)
        )
    return out


class BucketAnnotationPatcher:
    """Best-effort GCS object-metadata mirror driven by asset events."""

    @staticmethod
    async def on_asset_upsert(
        catalog_id: Optional[str] = None,
        collection_id: Optional[str] = None,
        asset_id: Optional[str] = None,
        payload: Optional[Dict[str, Any]] = None,
        **_kwargs,
    ) -> None:
        del _kwargs
        if not catalog_id or not asset_id or not isinstance(payload, dict):
            return
        if payload.get("owned_by") != "gcs":
            return
        uri = payload.get("uri")
        if not isinstance(uri, str):
            return
        parsed = _parse_gs_uri(uri)
        if parsed is None:
            return
        bucket_name, blob_path = parsed
        ctx = f"{catalog_id}/{collection_id or '<catalog-tier>'}/{asset_id}"

        try:
            from dynastore.modules.gcp.gcp_module import GCPModule

            gcp = get_protocol(GCPModule)
        except Exception as exc:
            logger.warning(
                "BucketAnnotationPatcher: GCPModule unavailable for %s: %s",
                ctx, exc,
            )
            return
        if gcp is None:
            return

        try:
            client = gcp.get_storage_client()
        except RuntimeError as exc:
            logger.warning(
                "BucketAnnotationPatcher: storage client unavailable for %s: %s",
                ctx, exc,
            )
            return

        new_metadata = _build_metadata(payload, asset_id)

        def _patch() -> None:
            blob = client.bucket(bucket_name).blob(blob_path)
            blob.reload()
            current = blob.metadata or {}
            if current == new_metadata:
                return
            blob.metadata = new_metadata
            blob.patch()

        try:
            await run_in_thread(_patch)
        except Exception as exc:
            logger.warning(
                "BucketAnnotationPatcher: patch failed for gs://%s/%s (%s): %s",
                bucket_name, blob_path, ctx, exc,
            )


def register_bucket_annotation_patcher() -> None:
    """Register ``BucketAnnotationPatcher`` on the global event bus.

    Wires ``CatalogEventType.ASSET_CREATION`` and ``ASSET_UPDATE`` to the
    upsert handler. ``ASSET_DELETION``/``ASSET_HARD_DELETION`` are not wired —
    bucket lifecycle handles object removal independently of metadata mirror.
    """
    async_event_listener(CatalogEventType.ASSET_CREATION)(
        BucketAnnotationPatcher.on_asset_upsert
    )
    async_event_listener(CatalogEventType.ASSET_UPDATE)(
        BucketAnnotationPatcher.on_asset_upsert
    )
    logger.info(
        "BucketAnnotationPatcher: registered on CatalogEventType.ASSET_CREATION/UPDATE"
    )
