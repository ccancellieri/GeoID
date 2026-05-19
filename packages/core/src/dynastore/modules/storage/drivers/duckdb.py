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
#
#    Author: Carlo Cancellieri (ccancellieri@gmail.com)
#    Company: FAO, Viale delle Terme di Caracalla, 00100 Rome, Italy
#    Contact: copyright@fao.org - http://fao.org/contact-us/terms/en/

"""
DuckDB Storage Driver — file-based analytical reads via DuckDB.

Reads from configurable formats (parquet, csv, json, etc.) using DuckDB's
built-in readers.  Optionally writes to SQLite via DuckDB's ``sqlite``
extension when ``ItemsDuckdbDriverConfig.write_path`` is set.

Capabilities vary based on configuration:
  - Read-only (default):  ``{READ_ONLY, STREAMING, SPATIAL_FILTER, EXPORT}``
  - Read-write (SQLite):  ``{STREAMING, SPATIAL_FILTER, EXPORT}``

Connection lifecycle:
  - **Pool**: bounded pool of in-memory DuckDB connections, sized by
    ``DUCKDB_POOL_SIZE`` (default 4).
  - **Lazy init**: pool created on first use via ``lifespan()``, not at
    import time.
  - **Extensions loaded once**: ``spatial`` (and optionally ``sqlite``)
    extensions are installed/loaded per connection at creation time.
  - **Shutdown**: all connections closed in ``lifespan()`` on app shutdown.
  - **Async-safe**: all blocking DuckDB operations are offloaded to the
    thread pool via ``run_in_thread()`` from ``dynastore.modules.concurrency``.
"""

import json as _json
import logging
import queue
import threading
import uuid
from contextlib import asynccontextmanager, contextmanager
from typing import TYPE_CHECKING, Any, AsyncIterator, ClassVar, Dict, FrozenSet, List, Optional, Set, Union

if TYPE_CHECKING:
    from dynastore.modules.storage.storage_location import StorageLocation

from dynastore.models.ogc import Feature, FeatureCollection
from dynastore.models.protocols.storage_driver import Capability
from dynastore.models.protocols.typed_driver import TypedDriver
from dynastore.models.query_builder import QueryRequest
from dynastore.modules.concurrency import run_in_thread
from dynastore.modules.protocols import ModuleProtocol
from dynastore.modules.storage.errors import ReadOnlyDriverError, SoftDeleteNotSupportedError
from dynastore.modules.storage.driver_config import ItemsDuckdbDriverConfig, DuckDBConfig
from dynastore.modules.storage.hints import Hint

logger = logging.getLogger(__name__)

# Format → DuckDB reader function
_FORMAT_READERS: Dict[str, str] = {
    "parquet": "read_parquet",
    "csv": "read_csv_auto",
    "json": "read_json_auto",
    "ndjson": "read_json_auto",
}

# ---------------------------------------------------------------------------
# Module-level connection pool
# ---------------------------------------------------------------------------
_pool: queue.Queue = queue.Queue()
_pool_size: int = 0
_pool_lock = threading.Lock()
_pool_initialized: bool = False
_loaded_extensions: Set[str] = set()


def _duckdb_available() -> bool:
    try:
        import duckdb  # noqa: F401
        return True
    except ImportError:
        return False


def _create_connection():
    """Create a single DuckDB in-memory connection with configured settings."""
    import duckdb

    conn = duckdb.connect(":memory:")
    conn.execute(f"SET max_memory = '{DuckDBConfig.max_memory}'")
    conn.execute(f"SET threads = {DuckDBConfig.threads}")

    # Load extensions on each new connection
    for ext_name in DuckDBConfig.extensions.split(","):
        ext_name = ext_name.strip()
        if ext_name:
            _try_load_extension_on(conn, ext_name)

    return conn


def _try_load_extension_on(conn, name: str) -> bool:
    """Install and load a DuckDB extension on a specific connection."""
    try:
        conn.install_extension(name)
        conn.load_extension(name)
        _loaded_extensions.add(name)
        logger.debug("DuckDB: extension '%s' loaded on connection", name)
        return True
    except Exception:
        logger.debug("DuckDB: extension '%s' not available", name)
        return False


def _init_pool():
    """Initialize the connection pool (idempotent, thread-safe)."""
    global _pool_initialized, _pool_size

    if _pool_initialized:
        return

    with _pool_lock:
        if _pool_initialized:
            return

        size = DuckDBConfig.pool_size
        for _ in range(size):
            conn = _create_connection()
            _pool.put(conn)

        _pool_size = size
        _pool_initialized = True
        logger.info("DuckDB: connection pool initialized (size=%d)", size)


@contextmanager
def _borrow_conn(timeout: Optional[int] = None):
    """Borrow a connection from the pool; return on exit.

    Thread-safe: ``queue.Queue`` handles synchronization.
    """
    _init_pool()
    t = timeout if timeout is not None else DuckDBConfig.read_timeout
    try:
        conn = _pool.get(timeout=t)
    except queue.Empty:
        raise TimeoutError(
            f"DuckDB: no connection available within {t}s "
            f"(pool_size={_pool_size})"
        )
    try:
        yield conn
    finally:
        _pool.put(conn)


@contextmanager
def _attach_sqlite(conn, write_path: str):
    """Attach a SQLite database with a unique alias to prevent collisions.

    Uses UUID-based alias names so multiple concurrent ATTACH/DETACH
    operations on the same connection (or different pool connections)
    never collide.
    """
    alias = f"wdb_{uuid.uuid4().hex[:8]}"
    conn.execute(f"ATTACH '{write_path}' AS {alias} (TYPE SQLITE)")
    try:
        yield alias
    finally:
        try:
            conn.execute(f"DETACH {alias}")
        except Exception:
            logger.warning("DuckDB: failed to detach alias '%s'", alias)


def _close_pool():
    """Close all connections in the pool and reset state."""
    global _pool_initialized, _pool_size

    with _pool_lock:
        closed = 0
        while not _pool.empty():
            try:
                conn = _pool.get_nowait()
                conn.close()
                closed += 1
            except (queue.Empty, Exception):
                break

        _pool_initialized = False
        _pool_size = 0
        _loaded_extensions.clear()
        if closed:
            logger.info("DuckDB: connection pool closed (%d connections)", closed)


class ItemsDuckdbDriver(TypedDriver[ItemsDuckdbDriverConfig], ModuleProtocol):
    """DuckDB storage driver — file-based analytical reads.

    Reads from parquet, CSV, JSON, etc. via DuckDB's built-in readers.
    Optionally writes to SQLite when ``write_path`` is configured.

    Uses a **bounded connection pool** (in-memory, thread-safe via
    ``queue.Queue``).  All blocking DuckDB operations are offloaded to
    the thread pool via ``run_in_thread()``.

    Satisfies ``CollectionItemsStore``.
    """

    # Opt out of items-tier auto-default routing.  ``frozenset()`` =
    # explicit-pin only.  Operators CAN pin DuckDB for analytical reads
    # via ``hint=Hint.ANALYTICS`` (declared in ``supported_hints``);
    # it just shouldn't be the default SEARCH backend for every
    # collection.  ES and PG handle the canonical SEARCH path.
    auto_register_for_routing: ClassVar[FrozenSet[str]] = frozenset()

    priority: int = 30
    preferred_chunk_size: int = 1000

    capabilities: FrozenSet[str] = frozenset({
        Capability.READ,
        Capability.WRITE,
        Capability.STREAMING,
        Capability.EXPORT,
        Capability.GEOSPATIAL,
        Capability.SOURCE_REFERENCE,
        Capability.EXTERNAL_ID_TRACKING,
        Capability.TEMPORAL_VALIDITY,
        Capability.PHYSICAL_ADDRESSING,
        # REQUIRED_ENFORCEMENT / UNIQUE_ENFORCEMENT: not advertised.
        # DuckDB stores feature properties in a single JSON VARCHAR column,
        # so field-level NOT NULL / UNIQUE cannot be enforced natively.
        # Opt into app-level fallback via ItemsSchema.allow_app_level_enforcement.
    })
    preferred_for: FrozenSet[Hint] = frozenset({Hint.ANALYTICS})
    supported_hints: FrozenSet[Hint] = frozenset({
        Hint.ANALYTICS,
        Hint.SPATIAL_FILTER, Hint.ATTRIBUTE_FILTER, Hint.SORT, Hint.GROUP_BY,
    })

    def is_available(self) -> bool:
        return _duckdb_available()

    async def get_driver_config(
        self,
        catalog_id: str,
        collection_id: Optional[str] = None,
        *,
        db_resource: Optional[Any] = None,
    ) -> "ItemsDuckdbDriverConfig":
        config = await self._get_location_async(catalog_id, collection_id)
        if config is None:
            return ItemsDuckdbDriverConfig()
        return config

    @asynccontextmanager
    async def lifespan(self, app_state: object):
        _init_pool()
        logger.info("ItemsDuckdbDriver: started (pool_size=%d)", DuckDBConfig.pool_size)
        yield
        _close_pool()
        logger.info("ItemsDuckdbDriver: stopped")

    async def _get_location_async(
        self, catalog_id: str, collection_id: Optional[str] = None
    ) -> Optional[ItemsDuckdbDriverConfig]:
        """Resolve ItemsDuckdbDriverConfig from the config waterfall."""
        try:
            from dynastore.tools.discovery import get_protocol
            from dynastore.models.protocols.configs import ConfigsProtocol

            configs = get_protocol(ConfigsProtocol)
            if not configs:
                return None
            config = await configs.get_config(
                ItemsDuckdbDriverConfig,
                catalog_id=catalog_id,
                collection_id=collection_id,
            )
            return config
        except Exception:
            return None

    @staticmethod
    def _is_writable(loc: ItemsDuckdbDriverConfig) -> bool:
        return loc.write_path is not None

    @staticmethod
    def _reader_func(fmt: str) -> str:
        return _FORMAT_READERS.get(fmt, "read_parquet")

    @staticmethod
    def _extract_external_id(row: Dict[str, Any], field: str) -> Optional[str]:
        parts = field.split(".")
        val = row
        for p in parts:
            if isinstance(val, dict):
                val = val.get(p)
            else:
                return None
        return str(val) if val is not None else None

    # ------------------------------------------------------------------
    # Sync helpers (run inside thread pool)
    # ------------------------------------------------------------------

    @staticmethod
    def _row_to_feature(row_dict: Dict[str, Any], geo_col: Optional[str]) -> Feature:
        """Convert a row dict to a Feature, parsing geometry if needed."""
        feature_id = row_dict.pop("id", None)
        geometry = row_dict.pop(geo_col or "geometry", None)
        if isinstance(geometry, str):
            try:
                geometry = _json.loads(geometry)
            except Exception:
                geometry = None
        elif isinstance(geometry, (bytes, bytearray)):
            geometry = None
        return Feature(
            type="Feature",
            id=feature_id,
            geometry=geometry,
            properties=row_dict,
        )

    def _read_entities_sync(
        self,
        loc: ItemsDuckdbDriverConfig,
        entity_ids: Optional[List[str]],
        request: Optional[QueryRequest],
        limit: int,
        offset: int,
    ) -> List[Feature]:
        """Synchronous read — runs inside thread pool."""
        with _borrow_conn() as conn:
            reader = self._reader_func(loc.format)
            source = f"{reader}('{loc.path}')"

            # Detect geometry column
            geo_col: Optional[str] = None
            if "spatial" in _loaded_extensions:
                try:
                    schema = conn.execute(
                        f"DESCRIBE SELECT * FROM {source} LIMIT 0"
                    ).fetchall()
                    for col_name, col_type, *_ in schema:
                        if "GEOMETRY" in str(col_type).upper():
                            geo_col = col_name
                            break
                except Exception:
                    pass

            if geo_col:
                schema_cols = [row[0] for row in conn.execute(
                    f"DESCRIBE SELECT * FROM {source} LIMIT 0"
                ).fetchall()]
                col_exprs = [
                    f"ST_AsGeoJSON({geo_col})::VARCHAR AS {geo_col}"
                    if c == geo_col else f'"{c}"'
                    for c in schema_cols
                ]
                base_sql = f"SELECT {', '.join(col_exprs)} FROM {source}"
            else:
                base_sql = f"SELECT * FROM {source}"

            where_clauses: List[str] = []
            params: List[Any] = []
            if entity_ids:
                placeholders = ", ".join(["?"] * len(entity_ids))
                where_clauses.append(f"id IN ({placeholders})")
                params.extend(entity_ids)

            if request and request.filters:
                for f in request.filters:
                    if f.operator == "eq":
                        from dynastore.tools.db import validate_sql_identifier
                        validate_sql_identifier(f.field)
                        where_clauses.append(f"{f.field} = ?")
                        params.append(f.value)
                    elif f.operator == "bbox" and isinstance(f.value, list) and len(f.value) == 4:
                        minx, miny, maxx, maxy = f.value
                        where_clauses.append(
                            f"ST_Intersects({geo_col or 'geometry'}, "
                            f"ST_MakeEnvelope({minx}, {miny}, {maxx}, {maxy}))"
                        )

            effective_limit = limit
            effective_offset = offset
            if request:
                if request.limit is not None:
                    effective_limit = request.limit
                if request.offset is not None:
                    effective_offset = request.offset

            where_sql = f" WHERE {' AND '.join(where_clauses)}" if where_clauses else ""
            sql = f"{base_sql}{where_sql} LIMIT ? OFFSET ?"
            params.extend([effective_limit, effective_offset])

            features: List[Feature] = []
            try:
                result = conn.execute(sql, params)
                columns = [desc[0] for desc in result.description]
                chunk_size = DuckDBConfig.fetch_chunk_size
                while True:
                    chunk = result.fetchmany(chunk_size)
                    if not chunk:
                        break
                    for row in chunk:
                        row_dict = dict(zip(columns, row))
                        features.append(self._row_to_feature(row_dict, geo_col))
            except Exception as e:
                logger.error("DuckDB read_entities failed: %s", e)

            return features

    def _write_entities_sync(
        self,
        loc: ItemsDuckdbDriverConfig,
        rows: List[Dict[str, Any]],
        collection_id: str,
        catalog_id: str,
        context: Optional[Dict[str, Any]],
    ) -> List[Feature]:
        """Synchronous write — runs inside thread pool."""
        from dynastore.modules.storage.drivers._duckdb_helpers import dicts_to_features
        from dynastore.modules.storage.driver_config import (
            ItemsWritePolicy, WriteConflictPolicy,
            AssetConflictPolicy,
        )

        ctx = context or {}
        policy = ctx.get("_resolved_policy", ItemsWritePolicy())

        # Enrich rows with context metadata
        asset_id = ctx.get("asset_id")
        valid_from = ctx.get("valid_from")
        valid_to = ctx.get("valid_to")
        for row in rows:
            if policy.track_asset_id and asset_id:
                row.setdefault("asset_id", asset_id)
            if policy.enable_validity:
                if valid_from:
                    row.setdefault("valid_from", str(valid_from))
                if valid_to:
                    row.setdefault("valid_to", str(valid_to))

        if loc.write_path is None:
            raise RuntimeError("DuckDB driver: write_path is required for write operations.")

        with _borrow_conn(timeout=DuckDBConfig.write_timeout) as conn:
            write_fmt = loc.write_format or "sqlite"
            if write_fmt == "sqlite":
                from dynastore.tools.db import validate_sql_identifier
                validate_sql_identifier(collection_id)

                # Load sqlite extension on this connection if needed
                _try_load_extension_on(conn, "sqlite")

                with _attach_sqlite(conn, loc.write_path) as alias:
                    table_name = f"{alias}.{collection_id}"
                    conn.execute(
                        f"CREATE TABLE IF NOT EXISTS {table_name} "
                        f"(id VARCHAR PRIMARY KEY, geometry VARCHAR, properties VARCHAR, "
                        f"external_id VARCHAR, asset_id VARCHAR, "
                        f"valid_from VARCHAR, valid_to VARCHAR)"
                    )
                    for row in rows:
                        ext_id_path = policy.external_id_path()
                        ext_id = (
                            ctx.get("external_id_override")
                            or self._extract_external_id(row, ext_id_path)
                        )
                        if policy.external_id_required() and not ext_id:
                            raise ValueError(
                                f"DuckDB write_entities: external_id required but missing "
                                f"(field='{ext_id_path}')"
                            )

                        on_conflict = policy.on_conflict
                        if on_conflict == WriteConflictPolicy.REFUSE and ext_id:
                            existing = conn.execute(
                                f"SELECT id FROM {table_name} WHERE external_id = ?", [ext_id]
                            ).fetchone()
                            if existing:
                                continue
                        elif policy.on_asset_conflict is not None and ext_id:
                            if policy.on_asset_conflict == AssetConflictPolicy.REFUSE:
                                existing = conn.execute(
                                    f"SELECT id FROM {table_name} WHERE external_id = ?", [ext_id]
                                ).fetchone()
                                if existing:
                                    from dynastore.modules.storage.errors import ConflictError
                                    raise ConflictError(
                                        f"DuckDB: external_id '{ext_id}' already exists in "
                                        f"{catalog_id}/{collection_id} (policy=refuse_asset)"
                                    )

                        row_with_ext = dict(row)
                        if ext_id:
                            row_with_ext["external_id"] = ext_id

                        cols = ", ".join(f'"{k}"' for k in row_with_ext.keys())
                        placeholders = ", ".join(["?"] * len(row_with_ext))
                        if on_conflict == WriteConflictPolicy.NEW_VERSION:
                            conn.execute(
                                f"INSERT INTO {table_name} ({cols}) VALUES ({placeholders})",
                                list(row_with_ext.values()),
                            )
                        else:
                            conn.execute(
                                f"INSERT OR REPLACE INTO {table_name} ({cols}) VALUES ({placeholders})",
                                list(row_with_ext.values()),
                            )
            else:
                conn.execute(
                    f"COPY (SELECT * FROM rows) TO '{loc.write_path}' (FORMAT {write_fmt})"
                )

        return dicts_to_features(rows)

    def _delete_entities_sync(
        self,
        loc: ItemsDuckdbDriverConfig,
        collection_id: str,
        entity_ids: List[str],
    ) -> int:
        """Synchronous delete — runs inside thread pool."""
        from dynastore.tools.db import validate_sql_identifier
        validate_sql_identifier(collection_id)

        if loc.write_path is None:
            raise RuntimeError("DuckDB driver: write_path is required for delete operations.")

        with _borrow_conn(timeout=DuckDBConfig.write_timeout) as conn:
            _try_load_extension_on(conn, "sqlite")

            with _attach_sqlite(conn, loc.write_path) as alias:
                placeholders = ", ".join(["?"] * len(entity_ids))
                result = conn.execute(
                    f"DELETE FROM {alias}.{collection_id} WHERE id IN ({placeholders})",
                    list(entity_ids),
                )
                return result.fetchone()[0] if result.description else len(entity_ids)

    def _ensure_storage_sync(
        self,
        loc: ItemsDuckdbDriverConfig,
        catalog_id: str,
        collection_id: Optional[str],
    ) -> None:
        """Synchronous ensure_storage — runs inside thread pool."""
        import os

        # --- SQLite write backend: create dir + table ---
        if loc.write_path:
            write_dir = os.path.dirname(loc.write_path)
            if write_dir:
                os.makedirs(write_dir, exist_ok=True)

            write_fmt = loc.write_format or "sqlite"
            if write_fmt == "sqlite" and collection_id:
                from dynastore.tools.db import validate_sql_identifier
                validate_sql_identifier(collection_id)

                with _borrow_conn(timeout=DuckDBConfig.write_timeout) as conn:
                    _try_load_extension_on(conn, "sqlite")

                    with _attach_sqlite(conn, loc.write_path) as alias:
                        conn.execute(
                            f"CREATE TABLE IF NOT EXISTS {alias}.{collection_id} "
                            f"(id VARCHAR PRIMARY KEY, geometry VARCHAR, properties VARCHAR, "
                            f"external_id VARCHAR, asset_id VARCHAR, "
                            f"valid_from VARCHAR, valid_to VARCHAR)"
                        )
                        logger.info(
                            "DuckDB: initialised SQLite table '%s' in %s",
                            collection_id, loc.write_path,
                        )

        # --- Read-only source: warn if local path missing ---
        if loc.path and not loc.path.startswith(("s3://", "gs://", "http")):
            if not os.path.exists(loc.path):
                logger.info(
                    "DuckDB: source path %s does not exist yet — "
                    "it will be populated by an external ETL process or first write",
                    loc.path,
                )

    def _export_entities_sync(
        self,
        loc: ItemsDuckdbDriverConfig,
        format: str,
        target_path: str,
    ) -> str:
        """Synchronous export — runs inside thread pool."""
        with _borrow_conn() as conn:
            reader = self._reader_func(loc.format)
            sql = f"COPY (SELECT * FROM {reader}('{loc.path}')) TO '{target_path}' (FORMAT {format})"
            conn.execute(sql)
        return target_path

    def _get_entity_fields_sync(
        self,
        loc: ItemsDuckdbDriverConfig,
    ) -> Dict[str, Any]:
        """Synchronous field introspection — runs inside thread pool."""
        from dynastore.models.protocols.field_definition import (
            FieldDefinition as ProtocolFieldDefinition,
            FieldCapability,
        )

        duckdb_type_map = {
            "VARCHAR": ("string", [FieldCapability.FILTERABLE, FieldCapability.SORTABLE]),
            "INTEGER": ("integer", [FieldCapability.FILTERABLE, FieldCapability.SORTABLE, FieldCapability.AGGREGATABLE]),
            "BIGINT": ("integer", [FieldCapability.FILTERABLE, FieldCapability.SORTABLE, FieldCapability.AGGREGATABLE]),
            "SMALLINT": ("integer", [FieldCapability.FILTERABLE, FieldCapability.SORTABLE, FieldCapability.AGGREGATABLE]),
            "TINYINT": ("integer", [FieldCapability.FILTERABLE, FieldCapability.SORTABLE, FieldCapability.AGGREGATABLE]),
            "FLOAT": ("numeric", [FieldCapability.FILTERABLE, FieldCapability.SORTABLE, FieldCapability.AGGREGATABLE]),
            "DOUBLE": ("numeric", [FieldCapability.FILTERABLE, FieldCapability.SORTABLE, FieldCapability.AGGREGATABLE]),
            "BOOLEAN": ("boolean", [FieldCapability.FILTERABLE]),
            "DATE": ("datetime", [FieldCapability.FILTERABLE, FieldCapability.SORTABLE]),
            "TIMESTAMP": ("datetime", [FieldCapability.FILTERABLE, FieldCapability.SORTABLE]),
            "TIMESTAMP WITH TIME ZONE": ("datetime", [FieldCapability.FILTERABLE, FieldCapability.SORTABLE]),
            "GEOMETRY": ("geometry", [FieldCapability.SPATIAL]),
            "BLOB": ("unknown", []),
        }

        with _borrow_conn() as conn:
            reader = self._reader_func(loc.format)
            source = f"{reader}('{loc.path}')"
            schema = conn.execute(f"DESCRIBE SELECT * FROM {source} LIMIT 0").fetchall()

            result = {}
            for col_name, col_type, *_ in schema:
                type_upper = str(col_type).upper()
                matched = False
                for key, (data_type, caps) in duckdb_type_map.items():
                    if key in type_upper:
                        result[col_name] = ProtocolFieldDefinition(
                            name=col_name,
                            data_type=data_type,
                            capabilities=caps,
                        )
                        matched = True
                        break
                if not matched:
                    result[col_name] = ProtocolFieldDefinition(
                        name=col_name,
                        data_type="string",
                        capabilities=[FieldCapability.FILTERABLE],
                    )
            return result

    # ------------------------------------------------------------------
    # Async public API (offloads to thread pool)
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
        loc = await self._get_location_async(catalog_id, collection_id)
        if not loc or not self._is_writable(loc):
            raise ReadOnlyDriverError(
                "ItemsDuckdbDriver: no write_path configured — driver is read-only"
            )

        from dynastore.modules.storage.drivers._duckdb_helpers import normalize_to_dicts
        from dynastore.modules.storage.driver_config import (
            ItemsWritePolicy,
        )
        from dynastore.models.protocols.configs import ConfigsProtocol
        from dynastore.tools.discovery import get_protocol

        rows = normalize_to_dicts(entities)
        if not rows:
            return []

        # Resolve write policy (async) before entering thread pool
        ctx = dict(context or {})
        policy = ItemsWritePolicy()
        try:
            _configs = get_protocol(ConfigsProtocol)
            if _configs:
                _p = await _configs.get_config(
                    ItemsWritePolicy, catalog_id=catalog_id, collection_id=collection_id
                )
                if _p is not None:
                    policy = _p
        except Exception:
            pass
        ctx["_resolved_policy"] = policy

        return await run_in_thread(
            self._write_entities_sync, loc, rows, collection_id, catalog_id, ctx
        )

    async def read_entities(
        self,
        catalog_id: str,
        collection_id: str,
        *,
        entity_ids: Optional[List[str]] = None,
        request: Optional[QueryRequest] = None,
        context: Optional[Dict[str, Any]] = None,  # noqa: ARG002
        limit: int = 100,
        offset: int = 0,
        db_resource: Optional[Any] = None,
    ) -> AsyncIterator[Feature]:
        loc = await self._get_location_async(catalog_id, collection_id)
        if not loc or not loc.path:
            return

        features = await run_in_thread(
            self._read_entities_sync, loc, entity_ids, request, limit, offset
        )
        for f in features:
            yield f

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
        if not loc or not self._is_writable(loc):
            raise ReadOnlyDriverError(
                "ItemsDuckdbDriver: no write_path — cannot delete"
            )
        if soft:
            raise SoftDeleteNotSupportedError(
                "ItemsDuckdbDriver does not support soft delete."
            )

        write_fmt = loc.write_format or "sqlite"
        if write_fmt != "sqlite":
            raise ReadOnlyDriverError(
                "ItemsDuckdbDriver: delete only supported with SQLite write backend"
            )

        return await run_in_thread(
            self._delete_entities_sync, loc, collection_id, entity_ids
        )

    async def ensure_storage(
        self,
        catalog_id: str,
        collection_id: Optional[str] = None,
        **kwargs,
    ) -> None:
        loc = await self._get_location_async(catalog_id, collection_id)
        if not loc:
            logger.info(
                "ItemsDuckdbDriver.ensure_storage: no location config for "
                "catalog=%s collection=%s — nothing to provision",
                catalog_id, collection_id,
            )
            return

        await run_in_thread(self._ensure_storage_sync, loc, catalog_id, collection_id)

    async def drop_storage(
        self,
        catalog_id: str,
        collection_id: Optional[str] = None,
        *,
        soft: bool = False,
    ) -> None:
        if soft:
            raise SoftDeleteNotSupportedError(
                "ItemsDuckdbDriver does not support soft drop."
            )
        loc = await self._get_location_async(catalog_id, collection_id)
        if loc and loc.write_path:
            import os
            if os.path.exists(loc.write_path):
                os.remove(loc.write_path)
                logger.info("DuckDB: removed write file %s", loc.write_path)

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
        if not loc or not loc.path:
            raise ValueError("DuckDB: no source path configured for export")

        return await run_in_thread(
            self._export_entities_sync, loc, format, target_path
        )

    async def location(
        self,
        catalog_id: str,
        collection_id: str,
    ) -> "StorageLocation":
        """Return typed physical storage coordinates for this DuckDB collection."""
        from dynastore.modules.storage.storage_location import StorageLocation

        loc = await self._get_location_async(catalog_id, collection_id)
        path = (loc.path or "") if loc else ""
        fmt = (loc.format if loc else None) or "parquet"
        uri = f"duckdb:///{path}?format={fmt}" if path else f"duckdb:///?format={fmt}"
        identifiers: Dict[str, str] = {"format": fmt}
        if path:
            identifiers["path"] = path
        if loc and loc.write_path:
            identifiers["write_path"] = loc.write_path
        return StorageLocation(
            backend="duckdb",
            canonical_uri=uri,
            identifiers=identifiers,
            display_label=path or collection_id,
        )

    async def get_entity_fields(
        self,
        catalog_id: str,
        collection_id: Optional[str] = None,
        *,
        entity_level: str = "item",
        db_resource: Optional[Any] = None,
    ) -> Dict[str, Any]:
        if entity_level != "item" or not collection_id:
            return {}

        try:
            loc = await self._get_location_async(catalog_id, collection_id)
            if not loc or not loc.path:
                return {}

            result = await run_in_thread(self._get_entity_fields_sync, loc)

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
                pass
            return result
        except Exception:
            return {}

    async def count_entities(
        self,
        catalog_id: str,
        collection_id: str,
        *,
        request: Optional[Any] = None,
        db_resource: Optional[Any] = None,
    ) -> int:
        loc = await self._get_location_async(catalog_id, collection_id)
        if not loc or not loc.path:
            return 0

        def _count_sync() -> int:
            with _borrow_conn() as conn:
                reader = self._reader_func(loc.format)
                source = f"{reader}('{loc.path}')"
                result = conn.execute(f"SELECT COUNT(*) FROM {source}").fetchone()
                return int(result[0]) if result else 0

        try:
            return await run_in_thread(_count_sync)
        except Exception:
            return 0

    async def introspect_schema(
        self,
        catalog_id: str,
        collection_id: str,
        *,
        db_resource: Optional[Any] = None,
    ) -> List[Any]:
        fields = await self.get_entity_fields(catalog_id, collection_id)
        return list(fields.values())

    async def compute_extents(
        self,
        catalog_id: str,
        collection_id: str,
        *,
        db_resource: Optional[Any] = None,
    ) -> Optional[Dict[str, Any]]:
        loc = await self._get_location_async(catalog_id, collection_id)
        if not loc or not loc.path:
            return None

        def _extents_sync() -> Optional[Dict[str, Any]]:
            with _borrow_conn() as conn:
                reader = self._reader_func(loc.format)
                source = f"{reader}('{loc.path}')"

                geo_col: Optional[str] = None
                if "spatial" in _loaded_extensions:
                    try:
                        schema = conn.execute(
                            f"DESCRIBE SELECT * FROM {source} LIMIT 0"
                        ).fetchall()
                        for col_name, col_type, *_ in schema:
                            if "GEOMETRY" in str(col_type).upper():
                                geo_col = col_name
                                break
                    except Exception:
                        pass

                extents: Dict[str, Any] = {}

                if geo_col and "spatial" in _loaded_extensions:
                    try:
                        row = conn.execute(f"""
                            SELECT
                                MIN(ST_XMin({geo_col})), MIN(ST_YMin({geo_col})),
                                MAX(ST_XMax({geo_col})), MAX(ST_YMax({geo_col}))
                            FROM {source}
                        """).fetchone()
                        if row and all(v is not None for v in row):
                            extents["spatial"] = {
                                "bbox": [[float(row[0]), float(row[1]), float(row[2]), float(row[3])]]
                            }
                    except Exception:
                        pass

                try:
                    schema_rows = conn.execute(
                        f"DESCRIBE SELECT * FROM {source} LIMIT 0"
                    ).fetchall()
                    dt_col: Optional[str] = None
                    for col_name, col_type, *_ in schema_rows:
                        if "TIMESTAMP" in str(col_type).upper() or col_name.lower() in (
                            "datetime", "date"
                        ):
                            dt_col = col_name
                            break
                    if dt_col:
                        row = conn.execute(
                            f'SELECT MIN("{dt_col}"), MAX("{dt_col}") FROM {source}'
                        ).fetchone()
                        if row and row[0] is not None:
                            extents["temporal"] = {
                                "interval": [[str(row[0]), str(row[1]) if row[1] else None]]
                            }
                except Exception:
                    pass

                return extents if extents else None

        try:
            return await run_in_thread(_extents_sync)
        except Exception:
            return None

    async def aggregate(
        self,
        catalog_id: str,
        collection_id: str,
        *,
        aggregation_type: str,
        field: Optional[str] = None,
        request: Optional[Any] = None,
        db_resource: Optional[Any] = None,
    ) -> Any:
        raise NotImplementedError(
            f"ItemsDuckdbDriver: aggregate('{aggregation_type}') is not implemented"
        )

    async def restore_entities(
        self,
        catalog_id: str,
        collection_id: str,
        entity_ids: List[str],
        *,
        db_resource: Optional[Any] = None,
    ) -> int:
        raise SoftDeleteNotSupportedError(
            "ItemsDuckdbDriver does not support soft delete / restore."
        )

    async def rename_storage(
        self,
        catalog_id: str,
        old_collection_id: str,
        new_collection_id: str,
        *,
        db_resource: Optional[Any] = None,
    ) -> None:
        raise NotImplementedError(
            "ItemsDuckdbDriver does not support rename_storage"
        )


