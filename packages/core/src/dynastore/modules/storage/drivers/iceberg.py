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

"""
Iceberg Storage Driver — Open Table Format with ACID, time-travel, and schema evolution.

Implements ``CollectionItemsStore`` using PyIceberg for full OTF
operations: snapshots, time-travel reads, schema evolution, and versioning.

Capabilities:
  ``{STREAMING, SPATIAL_FILTER, EXPORT, TIME_TRAVEL, VERSIONING, SNAPSHOTS,
    SCHEMA_EVOLUTION, SOFT_DELETE}``

DynaStore hierarchy mapping:
  - DynaStore catalog → Iceberg namespace
  - DynaStore collection → Iceberg table
  - DynaStore item/feature → Iceberg row

The driver delegates to a PyIceberg catalog configured via
``ItemsIcebergDriverConfig``.  Uses ``pyiceberg.catalog.CatalogType`` enum
and PyIceberg constants (``TYPE``, ``URI``, ``WAREHOUSE_LOCATION``) for
configuration — no string literals.

Catalog types (via ``pyiceberg.catalog.CatalogType``):
  - ``SQL`` (default): PostgreSQL-backed SqlCatalog
  - ``REST``: REST catalog (e.g., Tabular, Polaris)
  - ``GLUE``: AWS Glue Data Catalog
  - ``HIVE``: Hive Metastore
  - ``DYNAMODB``: DynamoDB-backed catalog

Warehouse resolution order:
  1. ``IcebergConfig.warehouse_uri`` (``ICEBERG_WAREHOUSE_URI`` env var)
  2. Auto-detected from platform ``StorageProtocol`` (GCS bucket, future S3)
  3. Local temp dir fallback (``file://``)

When the warehouse URI scheme is ``gs://``, the driver injects PyIceberg
GCS IO properties (``GCS_PROJECT_ID``) so PyArrowFileIO can access GCS.
S3 follows the same pattern when an AWS module is implemented.

Connection lifecycle:
  - **Lazy init**: catalog loaded on first use via ``_ensure_catalog()``.
  - **Cached**: once loaded, the same catalog instance is reused.
  - **Shutdown**: catalog reference cleared in ``lifespan()`` on app shutdown.
"""

import json
import logging
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any, AsyncIterator, ClassVar, Dict, FrozenSet, List, Optional, Union

if TYPE_CHECKING:
    from dynastore.modules.storage.storage_location import StorageLocation

from dynastore.models.ogc import Feature, FeatureCollection
from dynastore.models.driver_context import DriverContext
from dynastore.models.protocols.storage_driver import Capability
from dynastore.models.protocols.teardown_lane import TeardownLane
from dynastore.models.protocols.typed_driver import TypedDriver
from dynastore.models.query_builder import QueryRequest
from dynastore.modules.concurrency import run_in_thread
from dynastore.modules.protocols import ModuleProtocol
from dynastore.tools.async_utils import LoopLocalLock
from dynastore.modules.storage.hints import Hint
from dynastore.modules.storage.driver_config import (
    ItemsIcebergDriverConfig,
    ItemsWritePolicy,
    IcebergConfig,
)

logger = logging.getLogger(__name__)

# Iceberg type string → PyIceberg type factory (lazy-loaded)
_ICEBERG_TYPE_MAP_KEYS = {
    "string", "int32", "int64", "float32", "float64",
    "boolean", "date", "timestamp", "timestamptz", "binary",
}


def _pyiceberg_available() -> bool:
    try:
        import pyiceberg  # noqa: F401
        return True
    except ImportError:
        return False


def _resolve_iceberg_type(type_str: str):
    """Map a type string to a PyIceberg type instance."""
    from pyiceberg.types import (
        BooleanType,
        BinaryType,
        DateType,
        DoubleType,
        FloatType,
        IntegerType,
        LongType,
        StringType,
        TimestampType,
        TimestamptzType,
    )
    return {
        "string": StringType(),
        "int32": IntegerType(),
        "int64": LongType(),
        "float32": FloatType(),
        "float64": DoubleType(),
        "boolean": BooleanType(),
        "date": DateType(),
        "timestamp": TimestampType(),
        "timestamptz": TimestamptzType(),
        "binary": BinaryType(),
    }.get(type_str, StringType())


def _canonical_to_iceberg_type(data_type: str):
    """Map a canonical ``FieldDefinition.data_type`` token to a PyIceberg type.

    The canonical vocabulary (see :mod:`dynastore.models.field_types`) is the
    single token a driver gets from
    :func:`...field_projection.materialize_feature_fields`. The mapping below is
    the Iceberg-side counterpart of e.g. ``CANONICAL_TO_PG_DDL`` — tokens that
    Iceberg can express natively map to a concrete primitive; everything else
    (notably ``geometry`` / ``jsonb`` / ``uuid`` / ``time``) falls back to
    ``StringType`` so the column is creatable and the existing driver semantics
    (geometry as a WKT/WKB string blob) are preserved.

    FULLTEXT vs FILTERABLE has no Iceberg-native distinction (Iceberg has no
    analyzed text primitive); both surface as ``StringType``. The capability is
    preserved on the projected ``FieldDefinition`` for downstream consumers
    (e.g. introspection / queryables responses).
    """
    from pyiceberg.types import (
        BooleanType,
        BinaryType,
        DateType,
        DoubleType,
        IntegerType,
        LongType,
        StringType,
        TimestamptzType,
    )
    dt = (data_type or "").lower()
    # Parametrized geometry (``geometry(Point,4326)``) — keep as a string blob,
    # matching the legacy hard-coded ``geometry: StringType`` envelope.
    if dt.startswith("geometry") or dt.startswith("geography"):
        return StringType()
    return {
        "string": StringType(),
        "uuid": StringType(),     # Iceberg has no UUID primitive in the public stable surface here.
        "jsonb": StringType(),    # Stored as a JSON string blob.
        "integer": IntegerType(),
        "bigint": LongType(),
        "double": DoubleType(),
        "numeric": DoubleType(),  # Iceberg has DecimalType(precision, scale); we have no precision on the canonical token, so widen to double.
        "boolean": BooleanType(),
        "date": DateType(),
        "time": StringType(),     # Iceberg has no time-of-day primitive.
        "timestamp": TimestamptzType(),
        "binary": BinaryType(),
    }.get(dt, StringType())


# Legacy underscore-prefixed Iceberg column names for the four policy-derived
# special fields. The driver's write path (``write_entities``) and conflict
# handling (``REFUSE``/``UPDATE``/``In("_external_id", ...)``) all reference
# these prefixed names — they must keep matching the on-disk column names so
# existing collections continue to read and so newly-projected columns line up
# with what ``write_entities`` actually writes. The projection
# (``materialize_feature_fields``) emits the un-prefixed canonical names; this
# table remaps them at schema-build time only.
_PROJECTION_TO_ICEBERG_COLUMN = {
    "external_id": "_external_id",
    "asset_id": "_asset_id",
    "valid_from": "_valid_from",
    "valid_to": "_valid_to",
}


def _build_iceberg_schema_from_projection(
    projected: "Dict[str, Any]",
):
    """Build the Iceberg ``Schema`` for a collection from a projected field set.

    ``projected`` is the ``{name: FieldDefinition}`` map returned by
    :func:`...field_projection.materialize_feature_fields` — the single
    cross-driver SSOT for "what fields does this collection materialise".

    The Iceberg schema produced here has, in order:

    1. **Envelope columns** (always present, fixed field-ids 1..3 for backwards
       compatibility with existing tables created by the legacy hard-coded
       schema):
         * ``id`` (StringType, required)
         * ``geometry`` (StringType, optional) — WKT/WKB string blob; matches
           the legacy hard-coded column even when the projection also describes
           a separate per-field geometry (which it filters out as owned by the
           geometry sidecar).
         * ``properties`` (StringType, optional) — JSON blob for non-materialised
           feature properties.
    2. **Projection columns** — one column per ``FieldDefinition`` in
       ``projected``, in projection order, with the canonical ``data_type``
       mapped to Iceberg via :func:`_canonical_to_iceberg_type`. Field
       ``required`` flows through; geometry fields are skipped (the projection
       already excludes them, but the guard is defence in depth).

    Field ids are assigned sequentially starting at 4 for projection columns.
    Iceberg requires stable, unique ids; sequential assignment within a single
    ``create_table`` call is sufficient (id stability across schema evolution is
    handled by the Iceberg catalog's ``update_schema`` semantics, not by us).
    """
    from pyiceberg.schema import Schema as IcebergSchema
    from pyiceberg.types import NestedField, StringType

    fields = [
        NestedField(1, "id", StringType(), required=True),
        NestedField(2, "geometry", StringType(), required=False),
        NestedField(3, "properties", StringType(), required=False),
    ]
    next_id = 4
    for name, fd in (projected or {}).items():
        # Skip envelope-collision names (envelope wins) and geometry (owned by
        # the envelope ``geometry`` column / geometry sidecar elsewhere).
        if name in ("id", "geometry", "properties"):
            continue
        if (getattr(fd, "data_type", "") or "").lower().startswith("geometry"):
            continue
        # Apply the legacy-prefix remap for the four special policy fields so
        # the on-disk column names match what ``write_entities`` writes.
        column_name = _PROJECTION_TO_ICEBERG_COLUMN.get(name, name)
        ib_type = _canonical_to_iceberg_type(getattr(fd, "data_type", "string"))
        fields.append(
            NestedField(
                next_id,
                column_name,
                ib_type,
                required=bool(getattr(fd, "required", False)),
            )
        )
        next_id += 1
    return IcebergSchema(*fields)


class ItemsIcebergDriver(TypedDriver[ItemsIcebergDriverConfig], ModuleProtocol):
    """Iceberg storage driver — Open Table Format with full OTF capabilities.

    Uses PyIceberg for ACID transactions, snapshot management, time-travel,
    and schema evolution.  Reads and writes via PyArrow integration.

    Satisfies ``CollectionItemsStore``.
    """

    teardown_lane: ClassVar[TeardownLane] = TeardownLane.ASYNC_CASCADE

    # OTF analytical/file-export driver — explicit-pin only.  Operators
    # who want Iceberg as a SEARCH or secondary-index target set the
    # routing config entry by hand (``source: "operator"``).
    auto_register_for_routing: ClassVar[FrozenSet[str]] = frozenset()

    priority: int = 20
    preferred_chunk_size: int = 1000
    capabilities: FrozenSet[str] = frozenset({
        Capability.READ,
        Capability.WRITE,
        Capability.STREAMING,
        Capability.EXPORT,
        Capability.TIME_TRAVEL,
        Capability.VERSIONING,
        Capability.SNAPSHOTS,
        Capability.SCHEMA_EVOLUTION,
        Capability.SOFT_DELETE,
        Capability.GEOSPATIAL,
        Capability.ASSET_TRACKING,
        Capability.SOURCE_REFERENCE,
        Capability.EXTERNAL_ID_TRACKING,
        Capability.TEMPORAL_VALIDITY,
        Capability.PHYSICAL_ADDRESSING,
        Capability.SOFT_DELETE_ATOMIC,
        # REQUIRED_ENFORCEMENT / UNIQUE_ENFORCEMENT: not advertised.
        # Iceberg schema here stores feature properties as a single StringType
        # blob, so field-level required/unique cannot be enforced natively.
        # (Iceberg also has no UNIQUE primitive at all.)
        # Opt into app-level fallback via IntrospectedSchema.allow_app_level_enforcement.
    })
    preferred_for: FrozenSet[Hint] = frozenset({Hint.ANALYTICS, Hint.FEATURES, Hint.WRITE})
    supported_hints: FrozenSet[Hint] = frozenset({
        Hint.ANALYTICS, Hint.FEATURES, Hint.WRITE,
        Hint.SPATIAL_FILTER, Hint.ATTRIBUTE_FILTER, Hint.SORT, Hint.GROUP_BY,
        Hint.STATISTICS,
    })

    # Thread-safe catalog cache: loc_key → catalog instance, protected by a per-loop lock.
    _catalog_cache: ClassVar[Dict[str, Any]] = {}
    _catalog_lock: ClassVar[LoopLocalLock] = LoopLocalLock()

    def is_available(self) -> bool:
        return _pyiceberg_available()

    async def get_driver_config(
        self,
        catalog_id: str,
        collection_id: Optional[str] = None,
        *,
        db_resource: Optional[Any] = None,
    ) -> "ItemsIcebergDriverConfig":
        from dynastore.models.protocols.configs import ConfigsProtocol
        from dynastore.tools.discovery import get_protocol

        configs = get_protocol(ConfigsProtocol)
        if configs is None:
            return ItemsIcebergDriverConfig()
        config = await configs.get_config(
            ItemsIcebergDriverConfig,
            catalog_id=catalog_id,
            collection_id=collection_id,
            ctx=DriverContext(db_resource=db_resource),
        )
        if not isinstance(config, ItemsIcebergDriverConfig):
            return ItemsIcebergDriverConfig()
        return config

    @asynccontextmanager
    async def lifespan(self, app_state: object):
        logger.info("ItemsIcebergDriver: started")
        yield
        self._catalog_cache.clear()
        logger.info("ItemsIcebergDriver: stopped")

    # ------------------------------------------------------------------
    # Catalog & warehouse resolution
    # ------------------------------------------------------------------

    async def _resolve_warehouse(
        self,
        catalog_id: str,
        loc: Optional[ItemsIcebergDriverConfig] = None,
    ) -> str:
        """Resolve warehouse URI: per-collection > env var > StorageProtocol > local.

        Per-collection ``warehouse_uri`` wins so different collections can point
        at different buckets. When absent, the env-var default is used, else the
        platform ``StorageProtocol`` is consulted (usually resolves to the
        catalog's GCS bucket with an ``iceberg/`` subfolder), else a local
        temp-dir fallback.
        """
        if loc is not None:
            resolved = loc.resolve_warehouse_uri()
            if resolved:
                return resolved
        if IcebergConfig.warehouse_uri:
            return IcebergConfig.warehouse_uri

        # Auto-detect from platform StorageProtocol (GCS today, S3 in future)
        try:
            from dynastore.tools.discovery import get_protocol
            from dynastore.models.protocols.storage import StorageProtocol
            storage = get_protocol(StorageProtocol)
            if storage:
                base_path = await storage.get_catalog_storage_path(catalog_id)
                if base_path:
                    warehouse = base_path.rstrip("/") + "/iceberg/"
                    logger.info(
                        "Iceberg warehouse auto-resolved from StorageProtocol: %s",
                        warehouse,
                    )
                    return warehouse
        except Exception as e:
            logger.debug("StorageProtocol not available for warehouse: %s", e)

        # Fallback: local filesystem
        import tempfile
        return f"file://{tempfile.gettempdir()}/iceberg_warehouse"

    def _load_catalog_sync(
        self,
        loc: Optional[ItemsIcebergDriverConfig],
        warehouse: Optional[str] = None,
    ):
        """Build a PyIceberg catalog (sync, blocking — called via run_in_thread).

        Connection config is taken from the per-collection
        ``ItemsIcebergDriverConfig`` first, falling back to
        ``IcebergConfig`` env-var defaults for any unset field.
        """
        from pyiceberg.catalog import (
            load_catalog,
            CatalogType,
            TYPE as _TYPE,
            URI as _URI,
            WAREHOUSE_LOCATION as _WAREHOUSE,
        )

        # Resolve catalog type via enum (default: SQL).
        raw_type = (loc.resolve_catalog_type() if loc else IcebergConfig.catalog_type.lower())
        try:
            cat_type = CatalogType(raw_type)
        except ValueError:
            cat_type = CatalogType.SQL
            logger.warning(
                "Unknown catalog type %r, falling back to %s",
                raw_type, cat_type.value,
            )

        catalog_props: Dict[str, Any] = {_TYPE: cat_type.value}

        # URI resolution: per-collection > env > (SQL: DBConfig)
        resolved_uri = loc.resolve_catalog_uri() if loc else IcebergConfig.catalog_uri
        if resolved_uri:
            catalog_props[_URI] = resolved_uri
        elif cat_type == CatalogType.SQL:
            from dynastore.modules.db_config.db_config import DBConfig
            from dynastore.modules.db_config.tools import normalize_db_url
            sync_url = normalize_db_url(DBConfig.database_url, is_async=False)
            if sync_url.startswith("postgresql://"):
                sync_url = sync_url.replace(
                    "postgresql://", "postgresql+psycopg2://", 1
                )
            catalog_props[_URI] = sync_url

        # Extra catalog-specific properties: per-collection merged over env
        extra_props = (
            loc.resolve_catalog_properties() if loc else IcebergConfig.catalog_properties
        )
        if extra_props:
            catalog_props.update(extra_props)

        # Warehouse resolution
        if _WAREHOUSE not in catalog_props:
            if warehouse:
                catalog_props[_WAREHOUSE] = warehouse
            elif cat_type == CatalogType.SQL:
                import tempfile
                catalog_props[_WAREHOUSE] = (
                    f"file://{tempfile.gettempdir()}/iceberg_warehouse"
                )

        # Inject IO properties for cloud warehouse schemes
        resolved_wh = catalog_props.get(_WAREHOUSE, "")
        if resolved_wh.startswith("gs://"):
            import os
            from pyiceberg.io import GCS_PROJECT_ID
            catalog_props.setdefault(
                GCS_PROJECT_ID,
                os.getenv("PROJECT_ID", os.getenv("GOOGLE_CLOUD_PROJECT", "")),
            )
        # Future: elif resolved_wh.startswith("s3://"):
        #     from pyiceberg.io import S3_REGION, S3_ACCESS_KEY_ID, ...

        resolved_name = loc.resolve_catalog_name() if loc else IcebergConfig.catalog_name
        catalog = load_catalog(resolved_name, **catalog_props)
        logger.debug(
            "ItemsIcebergDriver: loaded %s catalog '%s' (warehouse=%s)",
            cat_type.value,
            resolved_name,
            resolved_wh or "none",
        )
        return catalog

    @staticmethod
    def _catalog_cache_key(
        loc: Optional[ItemsIcebergDriverConfig],
    ) -> str:
        """Stable cache key for a resolved (name, type, uri, warehouse) tuple.

        Including the per-collection values here is load-bearing: two
        collections with different warehouse_uris must get distinct catalog
        instances, otherwise writes leak across tenants.
        """
        if loc is not None:
            name = loc.resolve_catalog_name()
            ctype = loc.resolve_catalog_type()
            uri = loc.resolve_catalog_uri() or ""
            wh = loc.resolve_warehouse_uri() or ""
        else:
            name = IcebergConfig.catalog_name
            ctype = IcebergConfig.catalog_type
            uri = IcebergConfig.catalog_uri or ""
            wh = IcebergConfig.warehouse_uri or ""
        return f"{name}:{ctype}:{uri}:{wh}"

    async def _ensure_catalog(
        self, loc: ItemsIcebergDriverConfig, catalog_id: str
    ):
        """Thread-safe catalog resolution with async lock.

        Catalog connection config is per-collection first, ``IcebergConfig``
        env-var defaults second. The cache keys on the resolved tuple so
        distinct collection configs map to distinct PyIceberg catalog handles.
        Double-checked locking: fast path (no lock) for cache hit,
        lock-protected path for cache miss with ``run_in_thread`` to offload
        the blocking ``load_catalog()`` call.
        """
        loc_key = self._catalog_cache_key(loc)

        # Fast path: cache hit (no lock)
        if loc_key in self._catalog_cache:
            return self._catalog_cache[loc_key]

        async with self._catalog_lock:
            # Double-check after acquiring lock
            if loc_key in self._catalog_cache:
                return self._catalog_cache[loc_key]

            warehouse = await self._resolve_warehouse(catalog_id, loc)
            catalog = await run_in_thread(self._load_catalog_sync, loc, warehouse)
            self._catalog_cache[loc_key] = catalog
            return catalog

    def _table_identifier(
        self, loc: ItemsIcebergDriverConfig, catalog_id: str, collection_id: str
    ) -> tuple:
        """Build Iceberg table identifier: (namespace, table_name)."""
        namespace = loc.namespace or catalog_id
        table_name = loc.table_name or collection_id
        return (namespace, table_name)

    async def _get_location_async(
        self, catalog_id: str, collection_id: Optional[str] = None
    ) -> Optional[ItemsIcebergDriverConfig]:
        """Resolve ItemsIcebergDriverConfig from the config waterfall."""
        try:
            from dynastore.tools.discovery import get_protocol
            from dynastore.models.protocols.configs import ConfigsProtocol

            configs = get_protocol(ConfigsProtocol)
            if not configs:
                return None
            config = await configs.get_config(
                ItemsIcebergDriverConfig,
                catalog_id=catalog_id,
                collection_id=collection_id,
            )
            if isinstance(config, ItemsIcebergDriverConfig):
                return config
            return None
        except Exception:
            return None

    # ------------------------------------------------------------------
    # CollectionItemsStore
    # ------------------------------------------------------------------

    async def write_entities(
        self,
        catalog_id: str,
        collection_id: str,
        entities: Union[Feature, FeatureCollection, Dict[str, Any], List[Dict[str, Any]]],
        *,
        context: Optional[Dict[str, Any]] = None,
        db_resource: Optional[Any] = None,
    ) -> List[Feature]:
        """Write entities to an Iceberg table respecting ItemsWritePolicy.

        Policy semantics on Iceberg (append-only table format):

        - ``UPDATE``: delete existing rows with matching ``external_id``, then append.
          Produces a new snapshot each time, preserving time-travel history.
        - ``NEW_VERSION``: append unconditionally (natural Iceberg operation).
          No deletes — each write is a new temporal version, full time-travel history.
        - ``IGNORE``: skip rows whose ``external_id`` already exists in the table.
        - ``REFUSE``: raise ``ConflictError`` if any row's ``external_id`` is found.

        Context keys honoured (only if ``enable_validity=True`` in policy):
          - ``asset_id``: stored as ``_asset_id`` column
          - ``valid_from``: stored as ``_valid_from``; defaults to now() unless
            ``ValiditySpec.start_from is None`` (open lower bound), in which case
            ``_valid_from`` is left unset (#1172)
          - ``valid_to``: stored as ``_valid_to``
        """
        loc = await self._get_location_async(catalog_id, collection_id)
        if not loc:
            raise RuntimeError(
                "ItemsIcebergDriver: no OTF location config found"
            )

        import pyarrow as pa
        from pyiceberg.io.pyarrow import schema_to_pyarrow
        from dynastore.modules.storage.drivers._duckdb_helpers import (
            normalize_to_dicts,
            dicts_to_features,
        )
        from dynastore.modules.storage.driver_config import (
            WriteConflictPolicy,
        )

        rows = normalize_to_dicts(entities)
        if not rows:
            return []

        # Resolve write policy from config waterfall.
        policy = await self._resolve_write_policy(catalog_id, collection_id)

        # Extract ingestion context (not part of the feature payload).
        ctx = context or {}
        asset_id: Optional[str] = ctx.get("asset_id")
        valid_from = ctx.get("valid_from")
        valid_to = ctx.get("valid_to")

        now_iso = datetime.now(timezone.utc).isoformat()

        # Enrich rows with tracking columns.
        ext_id_path = policy.external_id_path()
        for row in rows:
            ext_id = self._extract_external_id(row, ext_id_path)
            if ext_id:
                row["_external_id"] = ext_id
            if asset_id is not None:
                row["_asset_id"] = asset_id
            if policy.enable_validity:
                # An open lower bound (ValiditySpec.start_from is None) leaves
                # ``_valid_from`` unset; otherwise a missing start defaults to
                # the ingestion instant. (#1172)
                start_is_open = (
                    policy.validity is not None and policy.validity.start_from is None
                )
                if valid_from is not None:
                    row["_valid_from"] = valid_from
                elif not start_is_open:
                    row["_valid_from"] = now_iso
                if valid_to is not None:
                    row["_valid_to"] = valid_to

        if not rows:
            return []

        catalog = await self._ensure_catalog(loc, catalog_id)
        table_id = self._table_identifier(loc, catalog_id, collection_id)
        table = await run_in_thread(catalog.load_table, table_id)

        on_conflict = policy.on_conflict

        if on_conflict == WriteConflictPolicy.REFUSE or policy.on_batch_conflict is not None:
            # Collect external_ids that already exist in the table.
            ext_ids = [
                row["_external_id"] for row in rows if "_external_id" in row
            ]
            if ext_ids:
                from pyiceberg.expressions import In
                from dynastore.modules.storage.driver_config import BatchConflictPolicy
                existing = await run_in_thread(
                    lambda: table.scan().filter(In("_external_id", ext_ids)).to_arrow()  # type: ignore[call-arg]
                )
                existing_ids = set(existing.column("_external_id").to_pylist()) if existing.num_rows > 0 else set()

                if (
                    policy.on_batch_conflict == BatchConflictPolicy.REFUSE
                    and existing_ids
                ):
                    from dynastore.modules.storage.errors import ConflictError
                    raise ConflictError(
                        f"Iceberg: external_id(s) {sorted(existing_ids)} already exist "
                        f"in {catalog_id}/{collection_id} (policy=refuse_batch)"
                    )

                if on_conflict == WriteConflictPolicy.REFUSE:
                    rows = [r for r in rows if r.get("_external_id") not in existing_ids]
                    if not rows:
                        return []

        elif on_conflict == WriteConflictPolicy.UPDATE:
            # Delete existing rows with matching external_ids, then append fresh rows.
            ext_ids = [
                row["_external_id"] for row in rows if "_external_id" in row
            ]
            if ext_ids:
                from pyiceberg.expressions import In
                try:
                    await run_in_thread(table.delete, delete_filter=In("_external_id", ext_ids))  # type: ignore[call-arg]
                except Exception as e:
                    logger.warning(
                        "Iceberg UPDATE: delete of existing external_ids failed — "
                        "appending anyway. Error: %s", e,
                    )

        # NEW_VERSION and UPDATE (after delete): append all rows as a new snapshot.
        arrow_schema = schema_to_pyarrow(table.schema())
        # Only include columns present in the Iceberg schema to avoid pa.null() errors.
        schema_names = set(arrow_schema.names)
        filtered_rows = [
            {k: v for k, v in row.items() if k in schema_names}
            for row in rows
        ]
        pa_table = pa.Table.from_pylist(filtered_rows, schema=arrow_schema)
        await run_in_thread(table.append, pa_table)
        del pa_table  # release memory early (Cloud Run)

        return dicts_to_features(rows)

    @staticmethod
    async def _resolve_write_policy(catalog_id: str, collection_id: str) -> "ItemsWritePolicy":
        """Resolve ItemsWritePolicy from the config waterfall."""
        from dynastore.modules.storage.driver_config import ItemsWritePolicy
        from dynastore.models.protocols.configs import ConfigsProtocol
        from dynastore.tools.discovery import get_protocol

        try:
            configs = get_protocol(ConfigsProtocol)
            if configs:
                cfg = await configs.get_config(
                    ItemsWritePolicy,
                    catalog_id=catalog_id,
                    collection_id=collection_id,
                )
                if isinstance(cfg, ItemsWritePolicy):
                    return cfg
        except Exception:
            logger.debug(
                "iceberg: write-policy resolution failed; using default", exc_info=True
            )
        return ItemsWritePolicy()

    @staticmethod
    async def _resolve_items_schema(catalog_id: str, collection_id: str):
        """Resolve ``ItemsSchema`` from the config waterfall, or ``None``.

        Mirrors :meth:`_resolve_write_policy`. The schema is consumed by the
        cross-driver :func:`...field_projection.materialize_feature_fields` to
        derive the full materialised field set for ``ensure_storage`` (#1295).
        ``None`` is a valid return — the projection treats it as "no
        author-declared fields", and only the policy-derived envelope columns
        are materialised.
        """
        from dynastore.modules.storage.driver_config import ItemsSchema
        from dynastore.models.protocols.configs import ConfigsProtocol
        from dynastore.tools.discovery import get_protocol

        try:
            configs = get_protocol(ConfigsProtocol)
            if configs is None:
                return None
            cfg = await configs.get_config(
                ItemsSchema,
                catalog_id=catalog_id,
                collection_id=collection_id,
            )
            if isinstance(cfg, ItemsSchema):
                return cfg
        except Exception:
            logger.debug(
                "iceberg: ItemsSchema resolution failed; treating as no declared schema",
                exc_info=True,
            )
        return None

    @staticmethod
    def _extract_external_id(row: dict, field_path: Optional[str]) -> Optional[str]:
        """Extract external_id using dot-notation path from a row dict."""
        if not field_path:
            return None
        val = row
        for part in field_path.split("."):
            if isinstance(val, dict):
                val = val.get(part)
            else:
                return None
        return str(val) if val is not None else None

    async def get_entity_fields(
        self,
        catalog_id: str,
        collection_id: Optional[str] = None,
        *,
        entity_level: str = "item",
        db_resource: Optional[Any] = None,
    ) -> Dict[str, Any]:
        """Return FieldDefinition dict from Iceberg table schema."""
        from dynastore.models.protocols.field_definition import (
            FieldDefinition as ProtocolFieldDefinition,
            FieldCapability,
        )

        if entity_level != "item" or not collection_id:
            return {}

        # Native Iceberg type (substring-matched, lowercased) -> canonical
        # data_type (see ``dynastore.models.field_types``). No bare "time" key —
        # it is a substring of "timestamp"/"timestamptz".
        iceberg_type_map = {
            "boolean": ("boolean", [FieldCapability.FILTERABLE]),
            "int": ("integer", [FieldCapability.FILTERABLE, FieldCapability.SORTABLE, FieldCapability.AGGREGATABLE]),
            "long": ("bigint", [FieldCapability.FILTERABLE, FieldCapability.SORTABLE, FieldCapability.AGGREGATABLE]),
            "float": ("double", [FieldCapability.FILTERABLE, FieldCapability.SORTABLE, FieldCapability.AGGREGATABLE]),
            "double": ("double", [FieldCapability.FILTERABLE, FieldCapability.SORTABLE, FieldCapability.AGGREGATABLE]),
            "decimal": ("numeric", [FieldCapability.FILTERABLE, FieldCapability.SORTABLE, FieldCapability.AGGREGATABLE]),
            "string": ("string", [FieldCapability.FILTERABLE, FieldCapability.SORTABLE]),
            "uuid": ("uuid", [FieldCapability.FILTERABLE, FieldCapability.SORTABLE]),
            "binary": ("binary", []),
            "date": ("date", [FieldCapability.FILTERABLE, FieldCapability.SORTABLE]),
            "timestamptz": ("timestamp", [FieldCapability.FILTERABLE, FieldCapability.SORTABLE]),
            "timestamp": ("timestamp", [FieldCapability.FILTERABLE, FieldCapability.SORTABLE]),
        }

        try:
            loc = await self._get_location_async(catalog_id, collection_id)
            if not loc:
                return {}
            catalog = await self._ensure_catalog(loc, catalog_id)
            table_id = self._table_identifier(loc, catalog_id, collection_id)
            table = await run_in_thread(catalog.load_table, table_id)

            result = {}
            for field in table.schema().fields:
                type_str = str(field.field_type).lower()
                field_required = bool(getattr(field, "required", False))
                for key, (data_type, caps) in iceberg_type_map.items():
                    if key in type_str:
                        result[field.name] = ProtocolFieldDefinition(
                            name=field.name,
                            data_type=data_type,
                            capabilities=caps,
                            required=field_required,
                        )
                        break
                else:
                    result[field.name] = ProtocolFieldDefinition(
                        name=field.name,
                        data_type="string",
                        capabilities=[FieldCapability.FILTERABLE],
                        required=field_required,
                    )

            # Overlay ItemsSchema-declared flags (required / unique).
            try:
                from dynastore.models.protocols.configs import ConfigsProtocol
                from dynastore.modules.storage.driver_config import ItemsSchema
                from dynastore.modules.storage.field_constraints import overlay_schema_flags
                from dynastore.tools.discovery import get_protocol

                configs = get_protocol(ConfigsProtocol)
                if configs is not None:
                    schema_cfg = await configs.get_config(
                        ItemsSchema,
                        catalog_id=catalog_id,
                        collection_id=collection_id,
                    )
                    result = overlay_schema_flags(schema_cfg, result)
            except Exception:
                logger.debug("iceberg: schema-flags overlay failed", exc_info=True)
            return result
        except Exception:
            return {}

    async def read_entities(
        self,
        catalog_id: str,
        collection_id: str,
        *,
        entity_ids: Optional[List[str]] = None,
        request: Optional[QueryRequest] = None,
        context: Optional[Dict[str, Any]] = None,
        limit: int = 100,
        offset: int = 0,
        ctx: Optional["DriverContext"] = None,
    ) -> AsyncIterator[Feature]:
        # Read-side (output) transform chains are not applied here by design
        # (geoid#1643). The restore_transform_chain is wired only on
        # Elasticsearch read paths; the routing-config validator emits a WARN
        # if output_transformers are declared against a non-ES driver.
        loc = await self._get_location_async(catalog_id, collection_id)
        if not loc:
            return

        catalog = await self._ensure_catalog(loc, catalog_id)
        table_id = self._table_identifier(loc, catalog_id, collection_id)
        table = await run_in_thread(catalog.load_table, table_id)

        scan = table.scan()

        if entity_ids:
            from pyiceberg.expressions import In
            scan = scan.filter(In("id", entity_ids))  # type: ignore[call-arg]

        if request and request.filters:
            scan = self._apply_filters(scan, request.filters)

        effective_limit = request.limit if request and request.limit else limit
        effective_offset = request.offset if request and request.offset else offset

        pa_table = await run_in_thread(scan.to_arrow)

        count = 0
        for i, row in enumerate(pa_table.to_pylist()):
            if i < effective_offset:
                continue
            if count >= effective_limit:
                break
            fid = row.pop("id", None)
            geom = row.pop("geometry", None)
            yield Feature(
                type="Feature", id=fid, geometry=geom, properties=row,
            )
            count += 1

    @staticmethod
    def _apply_filters(scan, filters):
        """Apply QueryRequest filters to an Iceberg scan.

        Supported filter operators:
          - ``eq``: equality filter via ``EqualTo``
          - ``neq``: not-equal filter via ``NotEqualTo``
          - ``gt``, ``gte``, ``lt``, ``lte``: range filters
          - ``in``: set membership via ``In``
          - ``bbox``: spatial bounding box (post-scan filter on geometry)
        """
        for f in filters:
            op = f.operator
            if op == "eq":
                from pyiceberg.expressions import EqualTo
                scan = scan.filter(EqualTo(f.field, f.value))  # type: ignore[call-arg]
            elif op == "neq":
                from pyiceberg.expressions import NotEqualTo
                scan = scan.filter(NotEqualTo(f.field, f.value))  # type: ignore[call-arg]
            elif op == "gt":
                from pyiceberg.expressions import GreaterThan
                scan = scan.filter(GreaterThan(f.field, f.value))  # type: ignore[call-arg]
            elif op == "gte":
                from pyiceberg.expressions import GreaterThanOrEqual
                scan = scan.filter(GreaterThanOrEqual(f.field, f.value))  # type: ignore[call-arg]
            elif op == "lt":
                from pyiceberg.expressions import LessThan
                scan = scan.filter(LessThan(f.field, f.value))  # type: ignore[call-arg]
            elif op == "lte":
                from pyiceberg.expressions import LessThanOrEqual
                scan = scan.filter(LessThanOrEqual(f.field, f.value))  # type: ignore[call-arg]
            elif op == "in" and isinstance(f.value, list):
                from pyiceberg.expressions import In
                scan = scan.filter(In(f.field, f.value))  # type: ignore[call-arg]
            # bbox handled at Feature level in _bbox_matches
        return scan

    @staticmethod
    def _bbox_matches(geometry, bbox: list) -> bool:
        """Check if a geometry intersects a bounding box [minx, miny, maxx, maxy].

        Supports GeoJSON-style geometry dicts and WKT strings.
        """
        if not geometry or not bbox or len(bbox) != 4:
            return False
        minx, miny, maxx, maxy = bbox

        coords = None
        if isinstance(geometry, dict):
            coords = geometry.get("coordinates")
        elif isinstance(geometry, str):
            try:
                parsed = json.loads(geometry)
                coords = parsed.get("coordinates")
            except (json.JSONDecodeError, AttributeError):
                return False

        if coords is None:
            return False

        def _flatten_coords(c):
            """Recursively flatten nested coordinate arrays to (x, y) pairs."""
            if isinstance(c, (list, tuple)) and len(c) >= 2 and isinstance(c[0], (int, float)):
                yield (c[0], c[1])
            elif isinstance(c, (list, tuple)):
                for sub in c:
                    yield from _flatten_coords(sub)

        for x, y in _flatten_coords(coords):
            if minx <= x <= maxx and miny <= y <= maxy:
                return True
        return False

    async def delete_entities(
        self,
        catalog_id: str,
        collection_id: str,
        entity_ids: List[str],
        *,
        soft: bool = False,
        db_resource: Optional[Any] = None,
    ) -> int:
        loc = await self._get_location_async(catalog_id, collection_id)
        if not loc:
            raise RuntimeError("ItemsIcebergDriver: no location config")

        catalog = await self._ensure_catalog(loc, catalog_id)
        table_id = self._table_identifier(loc, catalog_id, collection_id)
        table = await run_in_thread(catalog.load_table, table_id)

        from pyiceberg.expressions import In
        row_filter = In("id", entity_ids)  # type: ignore[call-arg]

        if soft:
            await run_in_thread(table.delete, delete_filter=row_filter)
        else:
            await run_in_thread(table.delete, delete_filter=row_filter)
            try:
                await run_in_thread(table.compact)
            except (AttributeError, NotImplementedError):
                logger.debug(
                    "iceberg: table.compact not supported by this catalog impl; skipping",
                    exc_info=True,
                )

        return len(entity_ids)

    async def ensure_storage(
        self,
        catalog_id: str,
        collection_id: Optional[str] = None,
        **kwargs,
    ) -> None:
        loc = await self._get_location_async(catalog_id, collection_id)
        if not loc:
            logger.warning(
                "ItemsIcebergDriver.ensure_storage: no location config for "
                "catalog=%s collection=%s", catalog_id, collection_id,
            )
            return

        catalog = await self._ensure_catalog(loc, catalog_id)
        namespace = loc.namespace or catalog_id

        try:
            await run_in_thread(catalog.create_namespace, namespace)
        except Exception:
            # Namespace most likely already exists; a failure for any other
            # reason will surface on the subsequent table operations.
            logger.debug(
                "iceberg: create_namespace(%s) did not succeed (likely already exists)",
                namespace,
                exc_info=True,
            )

        if collection_id:
            table_id = self._table_identifier(loc, catalog_id, collection_id)
            try:
                await run_in_thread(catalog.load_table, table_id)
            except Exception:
                # Build the table schema from the driver-agnostic field
                # projection (#1295). The projection is the SSOT for the full
                # materialised field set — author-declared schema fields plus
                # policy-derived special fields (external_id, asset_id,
                # validity bounds, hashes, spatial cells, geometry/attribute
                # statistics). Existing Iceberg tables are NOT touched here:
                # ``load_table`` succeeded for them and they keep their on-disk
                # schema. New tables are created with the full projection so
                # writes to ``_external_id`` / ``_asset_id`` / ``_valid_from``
                # / ``_valid_to`` (computed in ``write_entities``) survive
                # ``write_entities``' schema-name filter instead of being
                # silently dropped.
                from dynastore.modules.storage.field_projection import (
                    materialize_feature_fields,
                )
                schema_cfg = await self._resolve_items_schema(catalog_id, collection_id)
                policy = await self._resolve_write_policy(catalog_id, collection_id)
                projected = materialize_feature_fields(schema_cfg, policy)
                iceberg_schema = _build_iceberg_schema_from_projection(projected)
                await run_in_thread(catalog.create_table, table_id, schema=iceberg_schema)

            # Warn on unique=True fields — Iceberg has no native UNIQUE enforcement.
            try:
                from dynastore.models.protocols.configs import ConfigsProtocol
                from dynastore.modules.storage.driver_config import ItemsSchema
                from dynastore.tools.discovery import get_protocol

                configs = get_protocol(ConfigsProtocol)
                if configs is not None:
                    schema_cfg = await configs.get_config(
                        ItemsSchema,
                        catalog_id=catalog_id,
                        collection_id=collection_id,
                    )
                    if schema_cfg is not None:
                        unique_fields = [
                            name for name, fd in getattr(schema_cfg, "fields", {}).items()
                            if getattr(fd, "unique", False)
                        ]
                        if unique_fields:
                            logger.warning(
                                "ItemsIcebergDriver: unique=True on fields %s for "
                                "catalog=%s collection=%s — Iceberg has no native UNIQUE "
                                "enforcement; uniqueness is advisory only.",
                                unique_fields, catalog_id, collection_id,
                            )
            except Exception:
                logger.debug(
                    "iceberg: unique-field advisory check failed", exc_info=True
                )

    async def drop_storage(
        self,
        catalog_id: str,
        collection_id: Optional[str] = None,
        *,
        soft: bool = False,
        **kwargs: Any,
    ) -> None:
        loc = await self._get_location_async(catalog_id, collection_id)
        if not loc:
            return

        catalog = await self._ensure_catalog(loc, catalog_id)

        if soft:
            logger.info(
                "ItemsIcebergDriver.drop_storage(soft=True): "
                "catalog=%s collection=%s — table retained, tagged as deprecated",
                catalog_id, collection_id,
            )
            if collection_id:
                table_id = self._table_identifier(loc, catalog_id, collection_id)
                try:
                    table = await run_in_thread(catalog.load_table, table_id)

                    def _soft_drop():
                        with table.transaction() as tx:
                            tx.set_properties(
                                **{
                                    "dynastore.deleted": "true",
                                    "dynastore.deleted_at": datetime.now(tz=timezone.utc).isoformat(),
                                }
                            )

                    await run_in_thread(_soft_drop)
                except Exception as e:
                    logger.warning("Iceberg soft drop failed: %s", e)
            return

        if collection_id:
            table_id = self._table_identifier(loc, catalog_id, collection_id)
            try:
                await run_in_thread(catalog.drop_table, table_id)
            except Exception as e:
                logger.warning("Iceberg drop_table failed: %s", e)
        else:
            namespace = loc.namespace or catalog_id
            try:
                await run_in_thread(catalog.drop_namespace, namespace)
            except Exception as e:
                logger.warning("Iceberg drop_namespace failed: %s", e)

    async def export_entities(
        self,
        catalog_id: str,
        collection_id: str,
        *,
        format: str = "parquet",
        target_path: str = "",
        db_resource: Optional[Any] = None,
    ) -> str:
        loc = await self._get_location_async(catalog_id, collection_id)
        if not loc:
            raise ValueError("ItemsIcebergDriver: no location config for export")

        catalog = await self._ensure_catalog(loc, catalog_id)
        table_id = self._table_identifier(loc, catalog_id, collection_id)
        table = await run_in_thread(catalog.load_table, table_id)

        pa_table = await run_in_thread(lambda: table.scan().to_arrow())

        def _export_to_file():
            if format == "csv":
                import pyarrow.csv as pcsv
                # pyarrow re-exports write_csv from pyarrow._csv without an
                # __all__ entry, so pyright>=stubs flag it as a private import.
                pcsv.write_csv(pa_table, target_path)  # pyright: ignore[reportPrivateImportUsage]
            elif format == "json":
                _rows = pa_table.to_pylist()
                import builtins
                with builtins.open(target_path, "w") as f:
                    json.dump(_rows, f)
            else:
                import pyarrow.parquet as pq
                pq.write_table(pa_table, target_path)

        await run_in_thread(_export_to_file)
        del pa_table  # release memory early (Cloud Run)
        return target_path

    async def location(
        self,
        catalog_id: str,
        collection_id: str,
    ) -> "StorageLocation":
        """Return typed physical storage coordinates for this Iceberg collection."""
        from dynastore.modules.storage.storage_location import StorageLocation

        loc = await self._get_location_async(catalog_id, collection_id)
        catalog_name = loc.resolve_catalog_name() if loc else IcebergConfig.catalog_name
        namespace = (loc.namespace if loc else None) or catalog_id
        table_name = (loc.table_name if loc else None) or collection_id
        return StorageLocation(
            backend="iceberg",
            canonical_uri=f"iceberg://{catalog_name}/{namespace}/{table_name}",
            identifiers={
                "catalog": catalog_name,
                "namespace": namespace,
                "table": table_name,
            },
            display_label=f"{namespace}.{table_name}",
        )

    # ------------------------------------------------------------------
    # OTF Operations — called by CatalogService when capabilities match
    # ------------------------------------------------------------------

    async def list_snapshots(
        self,
        catalog_id: str,
        collection_id: str,
        *,
        limit: int = 100,
        ctx: Optional["DriverContext"] = None,
    ) -> list:
        """List available snapshots for a collection."""
        from dynastore.models.otf import SnapshotInfo

        loc = await self._get_location_async(catalog_id, collection_id)
        if not loc:
            return []

        catalog = await self._ensure_catalog(loc, catalog_id)
        table_id = self._table_identifier(loc, catalog_id, collection_id)
        table = await run_in_thread(catalog.load_table, table_id)

        snapshots = []
        for entry in table.history()[:limit]:
            snapshots.append(
                SnapshotInfo(
                    snapshot_id=str(entry.snapshot_id),
                    timestamp=datetime.fromtimestamp(
                        entry.timestamp_ms / 1000, tz=timezone.utc
                    ),
                    operation="unknown",
                )
            )
        return snapshots

    async def create_snapshot(
        self,
        catalog_id: str,
        collection_id: str,
        *,
        label: Optional[str] = None,
        ctx: Optional["DriverContext"] = None,
    ):
        """Create an explicit snapshot/bookmark of current state."""
        from dynastore.models.otf import SnapshotInfo

        loc = await self._get_location_async(catalog_id, collection_id)
        if not loc:
            raise RuntimeError("ItemsIcebergDriver: no location config")

        catalog = await self._ensure_catalog(loc, catalog_id)
        table_id = self._table_identifier(loc, catalog_id, collection_id)
        table = await run_in_thread(catalog.load_table, table_id)

        if label:
            await run_in_thread(lambda: table.manage_snapshots().create_branch(label))

        current = table.current_snapshot()
        return SnapshotInfo(
            snapshot_id=str(current.snapshot_id),
            parent_snapshot_id=(
                str(current.parent_snapshot_id)
                if current.parent_snapshot_id
                else None
            ),
            timestamp=datetime.fromtimestamp(
                current.timestamp_ms / 1000, tz=timezone.utc
            ),
            label=label,
            operation="snapshot",
        )

    async def rollback_to_snapshot(
        self,
        catalog_id: str,
        collection_id: str,
        snapshot_id: str,
        *,
        ctx: Optional["DriverContext"] = None,
    ) -> None:
        """Rollback collection to a previous snapshot."""
        loc = await self._get_location_async(catalog_id, collection_id)
        if not loc:
            raise RuntimeError("ItemsIcebergDriver: no location config")

        catalog = await self._ensure_catalog(loc, catalog_id)
        table_id = self._table_identifier(loc, catalog_id, collection_id)
        table = await run_in_thread(catalog.load_table, table_id)

        await run_in_thread(lambda: table.manage_snapshots().rollback_to(int(snapshot_id)))

    async def read_at_snapshot(
        self,
        catalog_id: str,
        collection_id: str,
        snapshot_id: str,
        *,
        request: Optional[QueryRequest] = None,
        limit: int = 100,
        ctx: Optional["DriverContext"] = None,
    ) -> AsyncIterator[Feature]:
        """Read entities at a specific snapshot (time-travel)."""
        loc = await self._get_location_async(catalog_id, collection_id)
        if not loc:
            return

        catalog = await self._ensure_catalog(loc, catalog_id)
        table_id = self._table_identifier(loc, catalog_id, collection_id)
        table = await run_in_thread(catalog.load_table, table_id)

        scan = table.scan(snapshot_id=int(snapshot_id))

        bbox = None
        if request and request.filters:
            scan = self._apply_filters(scan, request.filters)
            for f in request.filters:
                if f.operator == "bbox" and isinstance(f.value, list) and len(f.value) == 4:
                    bbox = f.value

        effective_limit = request.limit if request and request.limit else limit
        pa_table = await run_in_thread(scan.to_arrow)

        count = 0
        for row in pa_table.to_pylist():
            if count >= effective_limit:
                break
            geom = row.pop("geometry", None)
            if bbox and not self._bbox_matches(geom, bbox):
                continue
            fid = row.pop("id", None)
            yield Feature(
                type="Feature", id=fid, geometry=geom, properties=row,
            )
            count += 1

    async def read_at_timestamp(
        self,
        catalog_id: str,
        collection_id: str,
        as_of: datetime,
        *,
        request: Optional[QueryRequest] = None,
        limit: int = 100,
        ctx: Optional["DriverContext"] = None,
    ) -> AsyncIterator[Feature]:
        """Read entities as they existed at a point in time."""
        loc = await self._get_location_async(catalog_id, collection_id)
        if not loc:
            return

        catalog = await self._ensure_catalog(loc, catalog_id)
        table_id = self._table_identifier(loc, catalog_id, collection_id)
        table = await run_in_thread(catalog.load_table, table_id)

        # Find snapshot at or before the given timestamp
        target_ms = int(as_of.timestamp() * 1000)
        snapshot_id = None
        for entry in table.history():
            if entry.timestamp_ms <= target_ms:
                snapshot_id = entry.snapshot_id
            else:
                break

        if snapshot_id is None:
            return

        async for feature in self.read_at_snapshot(
            catalog_id, collection_id, str(snapshot_id),
            request=request, limit=limit, ctx=ctx,
        ):
            yield feature

    async def get_schema_history(
        self,
        catalog_id: str,
        collection_id: str,
        *,
        ctx: Optional["DriverContext"] = None,
    ) -> list:
        """Return schema evolution history for a collection."""
        from dynastore.models.otf import SchemaVersion, SchemaField

        loc = await self._get_location_async(catalog_id, collection_id)
        if not loc:
            return []

        catalog = await self._ensure_catalog(loc, catalog_id)
        table_id = self._table_identifier(loc, catalog_id, collection_id)
        table = await run_in_thread(catalog.load_table, table_id)

        versions = []
        for schema in table.metadata.schemas:
            fields = [
                SchemaField(
                    name=field.name,
                    type=str(field.field_type),
                    required=field.required,
                    doc=field.doc,
                )
                for field in schema.fields
            ]
            versions.append(
                SchemaVersion(
                    schema_id=str(schema.schema_id),
                    fields=fields,
                    timestamp=datetime.now(tz=timezone.utc),
                    parent_schema_id=None,
                )
            )
        return versions

    async def evolve_schema(
        self,
        catalog_id: str,
        collection_id: str,
        changes: Any,
        *,
        ctx: Optional["DriverContext"] = None,
    ):
        """Apply schema changes (add/rename/drop columns, type widening).

        Supports:
          - ``add_columns``: add new columns with typed defaults
          - ``rename_columns``: rename existing columns
          - ``drop_columns``: remove columns
          - ``type_promotions``: safe type widening (int32→int64, float32→float64)

        Does NOT rewrite data — only updates metadata.
        """
        from dynastore.models.otf import SchemaVersion, SchemaField, SchemaEvolution

        if not isinstance(changes, SchemaEvolution):
            changes = SchemaEvolution.model_validate(changes)

        loc = await self._get_location_async(catalog_id, collection_id)
        if not loc:
            raise RuntimeError("ItemsIcebergDriver: no location config")

        catalog = await self._ensure_catalog(loc, catalog_id)
        table_id = self._table_identifier(loc, catalog_id, collection_id)
        table = await run_in_thread(catalog.load_table, table_id)

        def _apply_schema_evolution():
            with table.update_schema() as update:
                for col in changes.add_columns:
                    iceberg_type = _resolve_iceberg_type(col.type)
                    update.add_column(col.name, iceberg_type, doc=col.doc)

                for old_name, new_name in changes.rename_columns.items():
                    update.rename_column(old_name, new_name)

                for col_name in changes.drop_columns:
                    update.delete_column(col_name)

                for field_name, new_type in changes.type_promotions.items():
                    iceberg_type = _resolve_iceberg_type(new_type)
                    update.update_column(field_name, field_type=iceberg_type)

        await run_in_thread(_apply_schema_evolution)

        # Return the new schema version
        current_schema = table.schema()
        fields = [
            SchemaField(
                name=f.name,
                type=str(f.field_type),
                required=f.required,
                doc=f.doc,
            )
            for f in current_schema.fields
        ]
        return SchemaVersion(
            schema_id=str(current_schema.schema_id),
            fields=fields,
            timestamp=datetime.now(tz=timezone.utc),
        )


