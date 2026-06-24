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

import logging
import asyncio
import re
from typing import Any, Dict, Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from google.cloud import storage
else:
    try:
        from google.cloud import storage
    except ImportError:
        storage = None

from dynastore.modules.db_config.query_executor import (
    managed_transaction,
    DbResource,
    _is_transient_asyncpg_error,
)
from dynastore.models.driver_context import DriverContext
from dynastore.models.protocols import ConfigsProtocol, CatalogsProtocol
from dynastore.modules.catalog.lifecycle_manager import LifecycleContext
from dynastore.tools.discovery import get_protocol
from dynastore.modules.gcp.gcp_config import (
    GcpCatalogBucketConfig,
    GcpLocation,
)
from dynastore.modules.gcp.tools import bucket as bucket_tool
from dynastore.modules.gcp.tools.bucket import BucketConflictError
from dynastore.modules.concurrency import run_in_thread

logger = logging.getLogger(__name__)


async def _phase_with_retry(engine: DbResource, fn):
    """Run ``fn(conn)`` in a short committed transaction, retrying once on a
    dead-connection transient error.

    The bucket-provisioning phases (``ensure_storage_for_catalog`` phase 1 / 3)
    open their own short transactions between slow GCS calls. On a long-lived
    deployment (e.g. dev/review on AlloyDB) the pool can hand out a wire the
    server already closed while idle, surfacing as an asyncpg ``InterfaceError``
    ("connection is closed") on first use. The write-path helper
    ``_provisioning_write_with_retry`` (#1895) already retries those for the
    catalog-status writes; the bucket phases lacked the same guard, so a stale
    connection failed the whole ``gcp_provision_catalog`` task before the bucket
    step completed — leaving the catalog wedged in 'provisioning' (#1894). The
    retry acquires a FRESH connection from the pool, never the stale one.
    """
    for attempt in range(2):
        try:
            async with managed_transaction(engine) as conn:
                return await fn(conn)
        except Exception as exc:
            orig = getattr(exc, "orig", exc)
            transient = (
                _is_transient_asyncpg_error(exc)
                or _is_transient_asyncpg_error(orig)
            )
            if attempt == 0 and transient:
                logger.warning(
                    "bucket_provision phase: transient %s (cause %s) on a pooled "
                    "connection; retrying once on a fresh connection.",
                    exc.__class__.__name__, orig.__class__.__name__,
                )
                await asyncio.sleep(0)
                continue
            raise
    raise AssertionError("_phase_with_retry: exhausted attempts")


class BucketService:
    """
    Manages GCS Buckets for Catalogs, including naming, creation, and database registration.
    """

    def __init__(
        self,
        engine: Optional[DbResource],
        config_service: Optional[ConfigsProtocol],
        storage_client: "storage.Client",
        project_id: str,
        region: str,
    ):
        self._engine = engine
        self._config_service = config_service
        self.storage_client = storage_client
        self.project_id = project_id
        self.region = region

    @property
    def engine(self) -> DbResource:
        if self._engine:
            return self._engine
        from dynastore.tools.protocol_helpers import get_engine

        engine = get_engine()
        if engine is None:
            raise RuntimeError("No database engine available.")
        return engine

    @property
    def config_service(self) -> ConfigsProtocol:
        if self._config_service:
            return self._config_service
        from dynastore.tools.discovery import get_protocol

        mgr = get_protocol(ConfigsProtocol)
        if not mgr:
            raise RuntimeError("ConfigsProtocol not available.")
        return mgr

    GCS_BUCKET_NAME_MAX_LEN: int = 63
    _HASH_LEN: int = 8
    # GCS rejects bucket names containing "google" or close misspellings.
    # See https://cloud.google.com/storage/docs/buckets#naming
    _GCS_FORBIDDEN_SUBSTRINGS: tuple = ("google", "g00gle", "g0ogle", "go0gle", "googl3")
    # Permitted bucket-name chars (lowercase letters, digits, '.', '-', '_').
    # NOTE: '_' is normalised to '-' before validation so it never reaches
    # this regex; we keep it in the class for completeness/documentation.
    _GCS_VALID_CHAR_PATTERN = re.compile(r"^[a-z0-9._-]+$")

    def generate_bucket_name(self, catalog_id: str, physical_schema: Optional[str] = None) -> str:
        """Generates the deterministic GCS bucket name for a catalog.

        Format: ``{project_id}-{identifier}`` where *identifier* is the
        physical schema (preferred) or the catalog_id, lowercased and
        with underscores translated to dashes.

        Length policy (GCS caps bucket names at 63 chars):
        - Common case: ``{project_id}-{identifier}`` fits → use as-is.
        - Identifier overflows: keep full project_id, truncate identifier
          and append an 8-char SHA1 of the original identifier so distinct
          identifiers never collide.
        - Project_id itself overflows the 1/3 budget (rare — GCP caps at
          30 chars): truncate project_id to ~1/3 of the budget keeping a
          readable head + 8-char hash, give identifier the remaining ~2/3
          with its own 8-char hash for collision safety.

        Validation (fail-fast at name generation rather than at GCS provision):
        - Empty identifier → ValueError
        - Identifier contains chars outside ``[a-z0-9._-]`` → ValueError
        - Identifier contains ``google`` or close misspellings → ValueError
          (GCS reserves these and would reject the bucket creation)
        - After truncation, trailing ``-`` or ``.`` is trimmed (GCS forbids
          ``.-`` adjacency and bucket names must end with letter/digit).

        Backwards compatibility: existing buckets are unaffected — their
        names are persisted on the catalog's ``GcpCatalogBucketConfig``
        (the ``bucket_name`` field) and read back via
        :meth:`_read_bucket_name`. Only newly provisioned catalogs use this
        scheme.
        """
        import hashlib
        if not self.project_id:
            raise RuntimeError("GCP Project ID not available.")
        raw_id = physical_schema or catalog_id
        if not raw_id:
            raise ValueError(
                "Cannot generate bucket name: both physical_schema and catalog_id are empty."
            )
        prefix = self.project_id.lower()
        identifier = raw_id.lower().replace("_", "-")
        if not self._GCS_VALID_CHAR_PATTERN.match(identifier):
            raise ValueError(
                f"Identifier {raw_id!r} contains characters invalid for GCS bucket names "
                f"(allowed: lowercase letters, digits, '.', '-', '_')."
            )
        for forbidden in self._GCS_FORBIDDEN_SUBSTRINGS:
            if forbidden in identifier:
                raise ValueError(
                    f"Identifier {raw_id!r} contains GCS-reserved substring "
                    f"{forbidden!r}; bucket names cannot contain 'google' or close misspellings."
                )
        bucket_name = f"{prefix}-{identifier}"
        if len(bucket_name) <= self.GCS_BUCKET_NAME_MAX_LEN:
            return bucket_name

        id_hash = hashlib.sha1(identifier.encode()).hexdigest()[:self._HASH_LEN]
        # Reserve room: prefix + '-' + truncated_id + '-' + id_hash
        overhead = 2 + self._HASH_LEN
        project_min = self.GCS_BUCKET_NAME_MAX_LEN // 3  # 21 chars
        identifier_cap = (self.GCS_BUCKET_NAME_MAX_LEN * 2) // 3  # 42 chars

        # Path A: full project_id fits with at least 1 char of identifier.
        max_id = self.GCS_BUCKET_NAME_MAX_LEN - len(prefix) - overhead
        if len(prefix) <= project_min * 2 and max_id >= 1:
            max_id = min(max_id, identifier_cap - self._HASH_LEN - 1)
            truncated_id = identifier[:max_id].rstrip("-.") or id_hash[:1]
            return f"{prefix}-{truncated_id}-{id_hash}"

        # Path B: project_id is itself too long. Keep ~1/3 budget readable
        # head + 8-char project hash; identifier takes the remaining ~2/3.
        proj_head_len = project_min - self._HASH_LEN - 1
        proj_hash = hashlib.sha1(prefix.encode()).hexdigest()[:self._HASH_LEN]
        truncated_prefix = f"{prefix[:proj_head_len].rstrip('-.')}-{proj_hash}"
        max_id = self.GCS_BUCKET_NAME_MAX_LEN - len(truncated_prefix) - overhead
        truncated_id = identifier[:max_id].rstrip("-.") or id_hash[:1]
        return f"{truncated_prefix}-{truncated_id}-{id_hash}"

    async def _read_bucket_name(
        self, conn: DbResource, catalog_id: str
    ) -> Optional[str]:
        """Catalog's provisioned GCS bucket name.

        Reads the ``bucket_name`` field of the catalog's
        :class:`GcpCatalogBucketConfig` — the config-backed replacement for the
        retired ``gcp.catalog_buckets`` link table. Resolved on the supplied
        connection so it observes writes made earlier in the same transaction.
        ``bucket_name`` is only ever written at catalog scope, so the config
        waterfall yields the catalog's value (or ``None`` when unprovisioned),
        matching the old per-catalog ``SELECT`` semantics.
        """
        cfg = await self.config_service.get_config(
            GcpCatalogBucketConfig,
            catalog_id=catalog_id,
            ctx=DriverContext(db_resource=conn),
        )
        return cfg.bucket_name

    async def _write_bucket_name(
        self,
        conn: DbResource,
        catalog_id: str,
        config: GcpCatalogBucketConfig,
        bucket_name: Optional[str],
    ) -> None:
        """Persist the system-assigned bucket name onto the catalog's config.

        ``config`` must be the catalog's *effective* bucket config (the object
        already persisted at catalog scope, e.g. by ``_resolve_effective_config``
        with ``persist_default=True``); we set ``bucket_name`` on it and write the
        whole object back so no operator-set field is lost. ``check_immutability``
        is ``False`` because ``bucket_name`` is a ``Computed`` field — it is
        stripped from external config writes and only the internal provisioner
        may set it.
        """
        config.bucket_name = bucket_name
        await self.config_service.set_config(
            GcpCatalogBucketConfig,
            config,
            catalog_id=catalog_id,
            check_immutability=False,
            ctx=DriverContext(db_resource=conn),
        )

    async def get_storage_identifier(self, catalog_id: str) -> Optional[str]:
        """Returns the GCS path (bucket name) for a catalog's root folder."""
        if not self.engine:
            raise RuntimeError("Database engine not available.")
        async with managed_transaction(self.engine) as conn:
            return await self._read_bucket_name(conn, catalog_id)

    async def get_catalog_storage_path(self, catalog_id: str) -> Optional[str]:
        """Returns the GCS path for a catalog's root folder (e.g., gs://bucket-name/catalog/)."""
        bucket_name = await self.ensure_storage_for_catalog(catalog_id)
        return bucket_tool.get_gcs_catalog_path(bucket_name) if bucket_name else None

    async def get_collection_storage_path(
        self, catalog_id: str, collection_id: str
    ) -> Optional[str]:
        """Returns the GCS path for a collection's folder (e.g., gs://bucket-name/collections/my-collection/)."""
        bucket_name = await self.ensure_storage_for_catalog(catalog_id)
        return (
            bucket_tool.get_gcs_collection_path(bucket_name, collection_id)
            if bucket_name
            else None
        )

    async def prepare_upload_target(
        self, catalog_id: str, collection_id: Optional[str] = None
    ):
        """
        Ensures that the target catalog and, if provided, collection exist before an
        operation like an upload. This will create them just-in-time if they don't exist.
        """
        if not self.engine:
            raise RuntimeError("Database engine not available.")

        async with managed_transaction(self.engine) as conn:
            catalogs = get_protocol(CatalogsProtocol)
            if catalogs:
                # Ensure the parent catalog exists, creating it if necessary.
                # This is the minimum requirement for any upload target.
                await catalogs.ensure_catalog_exists(catalog_id, ctx=DriverContext(db_resource=conn))

                if collection_id:
                    # If a collection is targeted, ensure it exists as a logical entity first.
                    collection_record = await catalogs.get_collection(
                        catalog_id, collection_id, ctx=DriverContext(db_resource=conn)
                    )
                    if not collection_record:
                        logger.info(
                            f"Logical collection '{catalog_id}:{collection_id}' does not exist for upload. Creating it now."
                        )
                        from dynastore.modules.catalog.models import Collection

                        default_collection_metadata = Collection.model_validate({
                            "id": collection_id,
                            "title": f"Auto-created collection for {collection_id}",
                            "extent": {
                                "spatial": {"bbox": [[0.0, 0.0, 0.0, 0.0]]},
                                "temporal": {"interval": [[None, None]]},
                            },
                        })
                        # This creates only the logical collection record, without a LayerConfig.
                        await catalogs.create_collection(
                            catalog_id, default_collection_metadata, ctx=DriverContext(db_resource=conn)
                        )
            else:
                logger.warning(
                    "CatalogsProtocol not available. Skipping catalog/collection existence check."
                )

    async def upload_file(
        self, source_path: str, target_path: str, content_type: Optional[str] = None
    ) -> str:
        """StorageProtocol: Uploads a local file to storage."""
        if not target_path.startswith("gs://"):
            raise ValueError(f"Invalid GCS path: {target_path}")

        bucket_name, blob_name = target_path[5:].split("/", 1)
        bucket = self.storage_client.bucket(bucket_name)
        blob = bucket.blob(blob_name)

        await run_in_thread(
            blob.upload_from_filename, source_path, content_type=content_type
        )
        return target_path

    async def upload_file_content(
        self, target_path: str, content: bytes, content_type: Optional[str] = None
    ) -> str:
        """StorageProtocol: Uploads content (bytes) directly to storage."""
        if not target_path.startswith("gs://"):
            raise ValueError(f"Invalid GCS path: {target_path}")

        bucket_name, blob_name = target_path[5:].split("/", 1)
        bucket = self.storage_client.bucket(bucket_name)
        blob = bucket.blob(blob_name)

        await run_in_thread(blob.upload_from_string, content, content_type=content_type)
        return target_path

    async def download_file(self, source_path: str, target_path: str) -> None:
        """StorageProtocol: Downloads a file from storage to local."""
        if not source_path.startswith("gs://"):
            raise ValueError(f"Invalid GCS path: {source_path}")

        bucket_name, blob_name = source_path[5:].split("/", 1)
        bucket = self.storage_client.bucket(bucket_name)
        blob = bucket.blob(blob_name)

        await run_in_thread(blob.download_to_filename, target_path)

    async def file_exists(self, path: str) -> bool:
        """StorageProtocol: Checks if a file exists in storage."""
        if not path.startswith("gs://"):
            return False

        bucket_name, blob_name = path[5:].split("/", 1)
        bucket = self.storage_client.bucket(bucket_name)
        blob = bucket.blob(blob_name)

        return await run_in_thread(blob.exists)

    async def wait_for_bucket_ready(
        self,
        bucket_name: str,
        max_retries: int = 8,
        initial_delay: float = 0.5,
    ) -> bool:
        """
        Polls for bucket existence with exponential backoff.
        Returns True if the bucket becomes visible, False if retries are exhausted.
        """
        bucket = self.storage_client.bucket(bucket_name)
        for attempt in range(1, max_retries + 1):
            if await run_in_thread(bucket.exists):
                return True
            if attempt < max_retries:
                delay = initial_delay * (2 ** (attempt - 1))
                logger.debug(
                    f"Attempt {attempt}/{max_retries}: Bucket '{bucket_name}' not ready. "
                    f"Retrying in {delay}s..."
                )
                await asyncio.sleep(delay)
        logger.warning(
            f"Bucket '{bucket_name}' not ready after {max_retries} attempts."
        )
        return False

    async def delete_file(self, path: str) -> None:
        """StorageProtocol: Deletes a file from storage."""
        if not path.startswith("gs://"):
            return

        bucket_name, blob_name = path[5:].split("/", 1)
        bucket = self.storage_client.bucket(bucket_name)
        blob = bucket.blob(blob_name)

        await run_in_thread(blob.delete)


    async def download_bytes_range(self, path: str, offset: int, length: int) -> bytes:
        """StorageProtocol: Efficient GCS byte-range download using blob.download_as_bytes."""
        if not path.startswith("gs://"):
            raise ValueError(f"Invalid GCS path: {path}")
        bucket_name, blob_name = path[5:].split("/", 1)
        blob = self.storage_client.bucket(bucket_name).blob(blob_name)
        return await run_in_thread(
            blob.download_as_bytes, start=offset, end=offset + length - 1
        )

    async def update_bucket_config(
        self, catalog_id: str, config: GcpCatalogBucketConfig
    ):
        """
        Updates the actual GCS bucket to match the provided configuration.
        """
        bucket_name = await self.get_storage_identifier(catalog_id)
        if bucket_name:
            await self._apply_bucket_settings(bucket_name, config)
        else:
            logger.warning(
                f"Cannot update bucket settings for catalog '{catalog_id}': No bucket found."
            )

    async def ensure_storage_for_catalog(
        self,
        catalog_id: str,
        conn: Optional[DbResource] = None,
        context: Optional[LifecycleContext] = None,
        auto_create: bool = True,
        config_override: Optional[GcpCatalogBucketConfig] = None,
        raise_on_failure: bool = False,
    ) -> Optional[str]:
        """
        Retrieves the bucket name for a catalog, creating it if it doesn't exist.
        The bucket name will be unique and tied to the catalog.

        Args:
            catalog_id: The catalog ID
            conn: Optional database connection. When None (the typical call path,
                including `GcpProvisionCatalogTask`), the DB connection is NOT held
                across GCS API calls: phase 1 (read/persist config) and phase 3
                (link bucket → catalog) each open their own short transaction;
                phase 2 (bucket create + settings + placeholders) runs with no
                pooled connection checked out. See #486 for why.
            context: Optional lifecycle context to avoid querying non-existent catalog
            auto_create: If True, create the bucket if it doesn't exist.
            config_override: Optional configuration to use for creation (overrides DB/context).
            raise_on_failure: If True, propagate the underlying exception instead of
                swallowing it and returning None. Callers that treat a missing bucket
                as a hard failure (e.g. ``GcpProvisionCatalogTask`` via
                ``setup_catalog_gcp_resources``) should set this so the real GCS / DB
                error reaches the logs and the task layer, rather than a generic
                "Bucket name returned as None". Graceful callers leave it False.
        """

        # ── Caller-managed transaction: legacy single-tx behavior. ──
        # No production caller currently passes `conn`, but the protocol
        # signature reserves it for nested-in-tx use; honor it without the
        # phase split.
        if conn is not None:
            return await self._ensure_storage_single_tx(
                conn,
                catalog_id=catalog_id,
                context=context,
                auto_create=auto_create,
                config_override=config_override,
                raise_on_failure=raise_on_failure,
            )

        # ── conn=None: three-phase to avoid holding a pooled conn across GCS I/O ──

        # Phase 1 (short tx, retried on a dead pooled conn): bucket-exists
        # short-circuit + effective config resolution.
        async def _phase1(p1_conn):
            existing = await self._read_bucket_name(p1_conn, catalog_id)
            if existing:
                return ("existing", existing)
            if not auto_create:
                logger.info(
                    f"Bucket for catalog '{catalog_id}' does not exist and "
                    "auto_create=False. Skipping creation."
                )
                return ("skip", None)
            eff = await self._resolve_effective_config(
                p1_conn,
                catalog_id=catalog_id,
                context=context,
                config_override=config_override,
                persist_default=True,
            )
            return ("config", eff)

        _p1_kind, _p1_val = await _phase_with_retry(self.engine, _phase1)
        if _p1_kind == "existing":
            return _p1_val
        if _p1_kind == "skip":
            return None
        effective_config = _p1_val

        # Phase 2 (NO DB connection held): GCS bucket create + settings + placeholders.
        if not self.project_id:
            raise RuntimeError("GCP Project ID could not be determined.")

        new_bucket_name = self.generate_bucket_name(
            catalog_id, physical_schema=context.physical_schema if context else None
        )

        try:
            bucket_created = await self._gcs_create_and_configure_bucket(
                new_bucket_name, effective_config, catalog_id
            )
        except Exception as e:
            logger.error(
                f"Failed to create or configure bucket for catalog '{catalog_id}': {e}",
                exc_info=True,
            )
            if raise_on_failure:
                raise
            return None

        # Phase 3 (short tx): link bucket → catalog. On failure, clean up the
        # bucket ONLY if we created it this call — a pre-existing bucket
        # (``bucket_created is False``) is owned by someone else and force-
        # deleting it would destroy their data.
        try:
            async def _phase3(p3_conn):
                await self._write_bucket_name(
                    p3_conn, catalog_id, effective_config, new_bucket_name
                )
                return new_bucket_name

            result = await _phase_with_retry(self.engine, _phase3)
        except Exception as e:
            logger.error(
                f"Failed to link bucket '{new_bucket_name}' to catalog '{catalog_id}': {e}",
                exc_info=True,
            )
            if bucket_created:
                await self._cleanup_orphan_bucket(new_bucket_name)
                if raise_on_failure:
                    raise
                return None
            # Pre-existing bucket (``bucket_created is False``) + link failure →
            # the deterministically-named bucket already exists in GCS and is not
            # ours. Leave it intact and surface a conflict so the catalog is
            # marked 'conflict'.
            logger.warning(
                f"Bucket '{new_bucket_name}' pre-existed before this attempt; "
                f"leaving it intact (not ours to delete)."
            )
            if raise_on_failure:
                raise BucketConflictError(
                    f"Bucket '{new_bucket_name}' already exists in GCS and is "
                    f"owned by another catalog; cannot link it to '{catalog_id}'."
                ) from e
            return None

        if result:
            logger.info(
                f"Successfully linked bucket '{result}' to catalog '{catalog_id}'."
            )
            return result

        msg = (
            f"Failed to link bucket '{new_bucket_name}' to catalog '{catalog_id}' "
            "and no result returned."
        )
        logger.error(msg)
        if bucket_created:
            await self._cleanup_orphan_bucket(new_bucket_name)
        if raise_on_failure:
            raise RuntimeError(msg)
        return None

    async def _ensure_storage_single_tx(
        self,
        conn: DbResource,
        *,
        catalog_id: str,
        context: Optional[LifecycleContext],
        auto_create: bool,
        config_override: Optional[GcpCatalogBucketConfig],
        raise_on_failure: bool = False,
    ) -> Optional[str]:
        """Legacy single-tx path for callers that pass an explicit `conn`."""
        bucket_name = await self._read_bucket_name(conn, catalog_id)
        if bucket_name:
            return bucket_name

        if not auto_create:
            logger.info(
                f"Bucket for catalog '{catalog_id}' does not exist and "
                "auto_create=False. Skipping creation."
            )
            return None

        effective_config = await self._resolve_effective_config(
            conn,
            catalog_id=catalog_id,
            context=context,
            config_override=config_override,
            persist_default=True,
        )

        if not self.project_id:
            raise RuntimeError("GCP Project ID could not be determined.")

        new_bucket_name = self.generate_bucket_name(
            catalog_id, physical_schema=context.physical_schema if context else None
        )

        try:
            bucket_created = await self._gcs_create_and_configure_bucket(
                new_bucket_name, effective_config, catalog_id
            )
        except Exception as e:
            logger.error(
                f"Failed to create or configure bucket for catalog '{catalog_id}': {e}",
                exc_info=True,
            )
            # Nothing to clean up: creation either failed before the bucket
            # existed, or it pre-existed and is not ours to delete.
            if raise_on_failure:
                raise
            return None

        # Link bucket → catalog by persisting the name onto the catalog's bucket
        # config. Clean up ONLY a bucket we created this call; a pre-existing
        # bucket is owned by someone else.
        try:
            await self._write_bucket_name(
                conn, catalog_id, effective_config, new_bucket_name
            )
            result = new_bucket_name
        except Exception as e:
            logger.error(
                f"Failed to link bucket '{new_bucket_name}' to catalog '{catalog_id}': {e}",
                exc_info=True,
            )
            if bucket_created:
                await self._cleanup_orphan_bucket(new_bucket_name)
                if raise_on_failure:
                    raise
                return None
            logger.warning(
                f"Bucket '{new_bucket_name}' pre-existed before this attempt; "
                f"leaving it intact (not ours to delete)."
            )
            if raise_on_failure:
                raise BucketConflictError(
                    f"Bucket '{new_bucket_name}' already exists in GCS and is "
                    f"owned by another catalog; cannot link it to '{catalog_id}'."
                ) from e
            return None

        if result:
            logger.info(
                f"Successfully linked bucket '{result}' to catalog '{catalog_id}'."
            )
            return result

        msg = (
            f"Failed to link bucket '{new_bucket_name}' to catalog '{catalog_id}' "
            "and no result returned."
        )
        logger.error(msg)
        if bucket_created:
            await self._cleanup_orphan_bucket(new_bucket_name)
        if raise_on_failure:
            raise RuntimeError(msg)
        return None

    async def _resolve_effective_config(
        self,
        conn: DbResource,
        *,
        catalog_id: str,
        context: Optional[LifecycleContext],
        config_override: Optional[GcpCatalogBucketConfig],
        persist_default: bool,
    ) -> GcpCatalogBucketConfig:
        """Resolve the bucket config. Priority: override → context snapshot → DB.

        When the resolution falls back to a default and ``persist_default`` is
        True, the default is written through `config_service.set_config` on the
        provided connection (idempotent setup).
        """
        effective_config: Any = config_override
        if effective_config is None:
            if context is not None:
                # Prefer the lifecycle snapshot — DO NOT query the DB here
                # because the catalog row may not be visible yet.
                if GcpCatalogBucketConfig.class_key() in context.config:
                    from dynastore.models.plugin_config import require_config_class

                    effective_config = require_config_class(
                        GcpCatalogBucketConfig.class_key()
                    ).model_validate(
                        context.config[GcpCatalogBucketConfig.class_key()]
                    )
                else:
                    effective_config = GcpCatalogBucketConfig(
                        location=GcpLocation(self.region)
                    )
            else:
                effective_config = await self.config_service.get_config(
                    GcpCatalogBucketConfig,
                    catalog_id,
                    ctx=DriverContext(db_resource=conn),
                )

        if not isinstance(effective_config, GcpCatalogBucketConfig):
            logger.debug(
                f"No GcpCatalogBucketConfig found for catalog '{catalog_id}'. "
                "Using default settings."
            )
            effective_config = GcpCatalogBucketConfig(location=GcpLocation(self.region))
            if persist_default:
                await self.config_service.set_config(
                    GcpCatalogBucketConfig,
                    effective_config,
                    catalog_id=catalog_id,
                    ctx=DriverContext(db_resource=conn),
                )
        return effective_config

    async def _gcs_create_and_configure_bucket(
        self,
        new_bucket_name: str,
        effective_config: GcpCatalogBucketConfig,
        catalog_id: str,
    ) -> bool:
        """Pure GCS-side work. Must NOT hold a pooled DB connection (see #486).

        Returns ``created``: True if this call created the bucket, False if it
        already existed. Callers use this to decide whether a later failure may
        orphan-delete the bucket — a pre-existing bucket is never ours to delete.
        """
        _bucket, created = await bucket_tool.create_bucket(
            new_bucket_name,
            effective_config,
            self.project_id,
            client=self.storage_client,
        )

        try:
            from dynastore.modules.catalog.log_manager import log_info

            await log_info(
                catalog_id,
                event_type="gcp_bucket_created" if created else "gcp_bucket_adopted",
                message=(
                    f"GCS bucket '{new_bucket_name}' created successfully."
                    if created
                    else f"GCS bucket '{new_bucket_name}' already existed; adopting it."
                ),
                details={
                    "bucket_name": new_bucket_name,
                    "location": effective_config.location,
                },
                immediate=False,
            )
        except Exception as log_e:
            logger.warning(f"Failed to log bucket creation event: {log_e}")

        await self._apply_bucket_settings(new_bucket_name, effective_config)

        def _setup_placeholders():
            bucket = self.storage_client.bucket(new_bucket_name)
            bucket.blob(f"{bucket_tool.CATALOG_FOLDER}/").upload_from_string("")
            bucket.blob(f"{bucket_tool.COLLECTIONS_FOLDER}/").upload_from_string("")

        await run_in_thread(_setup_placeholders)
        return created

    async def _cleanup_orphan_bucket(self, bucket_name: str) -> None:
        """Best-effort delete of a GCS bucket whose DB link could not be established."""
        try:
            await bucket_tool.delete_bucket(
                bucket_name, force=True, client=self.storage_client
            )
        except Exception as cleanup_e:
            logger.error(
                f"Failed to cleanup partially created bucket '{bucket_name}': {cleanup_e}"
            )

    async def _apply_bucket_settings(
        self, bucket_name: str, config: GcpCatalogBucketConfig
    ):
        """
        Maps the declarative Pydantic configuration to the GCS bucket properties.
        This handles partial updates for mutable fields.
        """
        if not self.storage_client:
            logger.warning(
                f"Storage client not available. Skipping update for bucket '{bucket_name}'."
            )
            return

        def _sync_update():
            bucket = self.storage_client.bucket(bucket_name)
            needs_patch = False

            # 1. CORS Configuration
            if config.cors is not None:
                cors_list = []
                for rule in config.cors:
                    rule_dict: Dict[str, Any] = {
                        "origin": rule.origin,
                        "method": rule.method,
                    }
                    if rule.response_header:
                        rule_dict["responseHeader"] = rule.response_header
                    if rule.max_age_seconds:
                        rule_dict["maxAgeSeconds"] = rule.max_age_seconds
                    cors_list.append(rule_dict)

                logger.info(
                    f"Applying CORS rules to bucket '{bucket_name}': {cors_list}"
                )
                bucket.cors = cors_list
                needs_patch = True

            # 2. Lifecycle Rules (Placeholder)
            # if config.lifecycle_rules:
            #     pass

            if needs_patch:
                bucket.patch()
                logger.info(
                    f"Successfully applied settings to GCS bucket '{bucket_name}'."
                )

        try:
            await run_in_thread(_sync_update)
        except Exception as e:
            logger.error(
                f"Failed to patch GCS bucket '{bucket_name}': {e}", exc_info=True
            )

    async def teardown_gcs_notification(
        self, bucket_name: str, gcs_notification_id: str
    ):
        """Deletes a GCS notification configuration from a bucket."""
        if not gcs_notification_id or not bucket_name:
            return

        def _delete_notification():
            try:
                bucket = self.storage_client.bucket(bucket_name)
                notification = bucket.notification(notification_id=gcs_notification_id)
                if notification.exists():
                    notification.delete()
                    logger.info(
                        f"Deleted GCS notification {gcs_notification_id} from bucket {bucket_name}."
                    )
                else:
                    logger.debug(
                        f"GCS notification {gcs_notification_id} not found in bucket {bucket_name}. Nothing to delete."
                    )
            except Exception as e:
                logger.warning(
                    f"Failed to delete GCS notification {gcs_notification_id}: {e}"
                )

        await run_in_thread(_delete_notification)

    async def drop_storage(
        self,
        catalog_id: str,
        conn: Optional[DbResource] = None,
        physical_schema: Optional[str] = None,
        bucket_name: Optional[str] = None,
    ) -> bool:
        """Remove the GCS bucket and its DB link for a catalog.

        Implements the ``StorageProtocol.drop_storage`` contract.  Idempotent:
        a missing bucket or already-deleted DB row is a no-op success.
        Returns True when cleanup completed successfully.

        Raises on unexpected GCS errors so callers can retry.

        Recreation-safe teardown path (#2298): a catalog hard-deleted and rapidly
        recreated under a *new* schema would, on a ``catalog_id``-keyed DB lookup,
        resolve to the *new* catalog's bucket and delete it. To target only the
        *old* bucket, callers pass an explicit target captured at delete time:

        * ``bucket_name`` — the authoritative name persisted on the old catalog's
          config (preferred; correct even for legacy catalogs whose bucket name
          does not embed the schema), or
        * ``physical_schema`` — from which the name is reconstructed
          deterministically (fallback when no persisted name was captured).

        In either explicit-target mode the DB is never consulted for the name nor
        mutated, leaving the live (recreated) catalog's bucket and config link
        untouched. ``bucket_name`` wins when both are supplied.
        """
        explicit_target = physical_schema is not None or bucket_name is not None
        if bucket_name is None and physical_schema is not None:
            try:
                bucket_name = self.generate_bucket_name(
                    catalog_id, physical_schema=physical_schema
                )
            except Exception as e:
                logger.warning(
                    "BucketService.drop_storage: cannot derive schema-targeted "
                    "bucket name for catalog %r (schema %r): %s",
                    catalog_id, physical_schema, e,
                )
                return True  # Nothing safely targetable; idempotent success.
        elif not explicit_target:
            bucket_name = await self.get_storage_identifier(catalog_id)

            # Fallback: if DB record is gone (e.g. schema already dropped), try deterministic name.
            if not bucket_name:
                try:
                    bucket_name = self.generate_bucket_name(catalog_id)
                    logger.info(
                        "BucketService.drop_storage: DB record missing for %r; "
                        "falling back to deterministic bucket name %r.",
                        catalog_id, bucket_name,
                    )
                except Exception as e:
                    logger.warning(
                        "BucketService.drop_storage: cannot determine bucket name "
                        "for catalog %r: %s",
                        catalog_id, e,
                    )
                    return True  # Nothing to clean up.

        if not bucket_name:
            return True  # No bucket provisioned; idempotent success.

        try:
            # Clear the persisted bucket-name link first (idempotent — the
            # config-backed equivalent of the old
            # ``DELETE FROM gcp.catalog_buckets``). Tier-local read: rewrite only
            # when a catalog-scoped config row actually exists and still carries a
            # name, so a catalog whose schema is already gone is never
            # resurrected and inherited defaults are never pinned at this tier.
            # Recreation-safe path (#2298) only deletes the old physical bucket
            # by name; it must never clear the config link, which now belongs to
            # the freshly recreated catalog under a different schema.
            if not explicit_target:
                persisted = await self.config_service.get_persisted_config(
                    GcpCatalogBucketConfig, catalog_id=catalog_id
                )
                if persisted and persisted.get("bucket_name"):
                    cfg = GcpCatalogBucketConfig.model_validate(persisted)
                    async with managed_transaction(self.engine) as conn:
                        await self._write_bucket_name(conn, catalog_id, cfg, None)

            # Force-delete the bucket (empties objects then deletes the bucket).
            try:
                await bucket_tool.delete_bucket(
                    bucket_name, force=True, client=self.storage_client
                )
                logger.info(
                    "BucketService.drop_storage: deleted bucket %r for catalog %r.",
                    bucket_name, catalog_id,
                )
            except Exception as e:
                from google.api_core.exceptions import NotFound
                if "404" in str(e) or isinstance(e, NotFound):
                    logger.info(
                        "BucketService.drop_storage: bucket %r already absent for catalog %r.",
                        bucket_name, catalog_id,
                    )
                else:
                    raise

            return True

        except Exception as e:
            logger.error(
                "BucketService.drop_storage: failed to remove bucket %r for catalog %r: %s",
                bucket_name, catalog_id, e,
                exc_info=True,
            )
            raise
