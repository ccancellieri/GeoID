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

"""GCS-specific tile-cache glue: the writer config + factory, and the
signed-URL provider for the 307-redirect serve mode.

The per-tile byte-moving mechanics live in the backend-agnostic
``modules.tiles.tile_blob_storage.StorageTileWriter``, which routes every
byte through the registered ``StorageProtocol`` — no ``google`` import
required there. This module supplies the two genuinely GCS-specific pieces
that can't be generalized:

* ``GcsTileWriterConfig`` — this deployment's typed entry in
  ``TilesCachingConfig.writers``, resolving WHERE (bucket name / existence)
  before handing byte-moving off to ``StorageTileWriter``. Registered via
  ``tiles_writers.register_tile_writer_factory`` at import time so core never
  imports this module directly (name-based ``TypedModelRegistry`` lookup is
  the decoupling seam) — the field bug this module fixes: a PG-only preseed
  job image without the ``gcp`` extra never reaches this import at all.
* ``GcsTileUrlSigner`` — a short-lived V4 signed URL via IAM signBlob,
  registered as a ``tile_blob_storage.TileUrlSignerProtocol`` implementation.

Importing this module pulls in ``google`` (via ``tools.signed_urls``) at
module scope — that is fine because it is only ever imported from
``gcp_module.py``, which itself only loads on services whose SCOPE installs
the ``gcp`` extra. Nothing in the PG- or local-disk-only tile code paths
imports this module.
"""

import logging
from datetime import timedelta
from typing import ClassVar, FrozenSet, Optional

from dynastore.modules.concurrency import run_in_thread
from dynastore.models.protocols import CloudStorageClientProtocol, CloudIdentityProtocol
from dynastore.modules import get_protocol
from dynastore.modules.tiles.tile_blob_storage import (
    StorageTileWriter,
    TileUrlSignerProtocol,
    external_bucket_exists,
    get_storage_for_scheme,
)
from dynastore.modules.tiles.tiles_config import TilesCachingConfig
from dynastore.modules.tiles.tiles_module import TileStorageProtocol
from dynastore.modules.tiles.tiles_writers import TileWriterConfig, register_tile_writer_factory
from dynastore.modules.gcp.tools.signed_urls import generate_gcs_signed_url

logger = logging.getLogger(__name__)


class GcsTileWriterConfig(TileWriterConfig):
    """GCS-backed tile writer entry for ``TilesCachingConfig.writers``.

    ``bucket=None`` (default) targets the catalog's own managed bucket
    (provisioned on write via ``StorageProtocol.ensure_storage_for_catalog``);
    an explicit ``bucket`` targets an operator-supplied, geoid-unmanaged
    bucket — verified to exist on the write path.
    """

    bucket: Optional[str] = None
    prefix: Optional[str] = None


async def _gcs_writer_factory(
    config: GcsTileWriterConfig, cfg: TilesCachingConfig, catalog_id: str, ensure: bool,
) -> Optional[TileStorageProtocol]:
    storage = get_storage_for_scheme("gs")
    if storage is None:
        return None  # 'gcp' extra not installed / GCPModule not registered as StorageProtocol.

    if config.bucket:
        if ensure and not await external_bucket_exists(config.bucket):
            raise RuntimeError(
                f"Configured GCS tile cache bucket '{config.bucket}' does not "
                "exist or is not accessible to the service account."
            )
        prefix = (config.prefix or "").strip().lstrip("/") or f"{cfg.key_prefix}/{catalog_id}"
        return StorageTileWriter(storage, f"gs://{config.bucket}", prefix)

    bucket = await (
        storage.ensure_storage_for_catalog(catalog_id) if ensure else storage.get_storage_identifier(catalog_id)
    )
    if not bucket:
        return None
    return StorageTileWriter(storage, f"gs://{bucket}", cfg.key_prefix)


# No hint affinity: GCS is the default-injected first candidate already (see
# tiles_writers.resolve_effective_writers); Hint.DURABLE elevates PG instead.
register_tile_writer_factory(GcsTileWriterConfig, _gcs_writer_factory)


class GcsTileUrlSigner(TileUrlSignerProtocol):
    """Signs ``gs://`` object URIs for the tile-cache 307-redirect serve mode.

    Uses IAM signBlob (``identity_provider.get_account_email()`` +
    ``get_fresh_token()``) so no static key file is needed. Existence is
    probed first (``blob.exists()``) so a genuine cache-miss returns ``None``
    quietly, while a probe failure (e.g. missing ``storage.objects.get`` on
    the bucket) is logged at WARNING — both cases let the caller
    (``tiles_service._try_cached_tile``) fall back to the proxy path.
    """

    supported_schemes: ClassVar[FrozenSet[str]] = frozenset({"gs"})

    def _get_client_provider(self) -> CloudStorageClientProtocol:
        provider = get_protocol(CloudStorageClientProtocol)
        if not provider:
            raise RuntimeError("CloudStorageClientProtocol (GCP) is not available.")
        return provider

    def _get_identity_provider(self) -> CloudIdentityProtocol:
        provider = get_protocol(CloudIdentityProtocol)
        if not provider:
            raise RuntimeError("CloudIdentityProtocol (GCP) is not available.")
        return provider

    async def sign(self, object_uri: str) -> Optional[str]:
        if not object_uri.startswith("gs://"):
            return None
        bucket_name, blob_path = object_uri[5:].split("/", 1)

        client_provider = self._get_client_provider()
        identity_provider = self._get_identity_provider()
        storage_client = client_provider.get_storage_client()
        blob = storage_client.bucket(bucket_name).blob(blob_path)

        # Inline existence check with explicit error handling. blob.exists()
        # uses the GCS JSON metadata API (storage.objects.get). A 403 on the
        # metadata endpoint raises in google-cloud-storage v3+, so we catch it
        # here and surface it as WARNING rather than letting it propagate
        # silently. A False return (blob genuinely absent) is a normal
        # cache-miss and should not be logged above DEBUG.
        try:
            tile_exists = await run_in_thread(blob.exists)
        except Exception as exc:
            logger.warning(
                "tile_cache: existence probe raised %s for %s "
                "— redirect unavailable; proxy will be tried. "
                "Check SA has storage.objects.get (metadata) on the bucket.",
                type(exc).__name__, object_uri,
            )
            return None
        if not tile_exists:
            # Normal cache-miss; not logged above DEBUG so the operator does
            # not see noise on every uncached tile request.
            return None

        # Tile is confirmed present — sign and return the redirect URL.
        # check_exists=False: existence already verified above.
        return await generate_gcs_signed_url(
            object_uri,
            method="GET",
            expiration=timedelta(minutes=60),
            client_provider=client_provider,
            identity_provider=identity_provider,
            check_exists=False,
        )
