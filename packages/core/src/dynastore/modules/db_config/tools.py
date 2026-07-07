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

import asyncio
import logging

from typing import Optional, Any, Union, Dict
from dynastore.modules.db_config.db_config import DBConfig
from dynastore.modules.db_config.query_executor import (
    DbResource,
    DQLQuery,
    ResultHandler,
)
from dynastore.modules.db_config import maintenance_tools
from sqlalchemy import Column, Integer, String, Float, Boolean, DateTime, JSON
from sqlalchemy.dialects.postgresql import UUID, TSTZRANGE
from geoalchemy2 import Geometry
from dynastore.tools.cache import cached


def normalize_db_url(url: str, is_async: bool = False) -> str:
    """
    Normalizes a database URL for use with sync (psycopg2) or async (asyncpg) drivers.

    It ensures the correct protocol prefix and converts driver-specific
    parameters (like ssl vs sslmode).
    """
    if not url:
        return url

    # Strip shell quotes that may leak from .env parsing (shlex.quote wraps in '')
    url = url.strip("'\"")

    # 1. Handle Protocol
    if is_async:
        if url.startswith("postgresql://"):
            url = url.replace("postgresql://", "postgresql+asyncpg://", 1)
    else:
        if url.startswith("postgresql+asyncpg://"):
            url = url.replace("postgresql+asyncpg://", "postgresql://", 1)

    # 2. Handle SSL Parameters
    # asyncpg uses 'ssl'
    # psycopg2 uses 'sslmode'
    if is_async:
        # Convert sslmode=... to ssl=...
        if "sslmode=" in url:
            url = url.replace("sslmode=", "ssl=")
    else:
        # Convert ssl=... to sslmode=...
        if "ssl=" in url:
            url = url.replace("ssl=", "sslmode=")

    return url


# Base Postgres extensions every database needs before geometry / columnar
# storage works. CREATE EXTENSION is independent per name; the order matters
# only for readability EXCEPT that ``pg_trgm`` is created LAST and is reused as
# the "all present" sentinel by the boot guard below — never reorder it off the
# tail without updating ``_EXT_SENTINEL``.
BASE_DB_EXTENSIONS: tuple[str, ...] = (
    "postgis",
    "postgis_topology",
    "btree_gist",
    "btree_gin",
    # pgcrypto provides ``digest()`` used by the geometries / attributes sidecar
    # GENERATED columns to maintain the SHA256 *_hash columns without
    # application-side write code.
    "pgcrypto",
    # pg_trgm powers lexical fuzzy matching (``similarity()`` / ``%`` operator),
    # used by the dimension Similarity conformance class to rank materialized
    # dimension members by trigram similarity against their member labels.
    "pg_trgm",
)

# Presence of the last-created extension implies the whole set is present.
_EXT_SENTINEL: str = BASE_DB_EXTENSIONS[-1]

# Upper bound on how long the foundational lifespan (``DatastoreModule``,
# priority 7) waits for the bootstrap-marker read before giving up and booting
# without running the one-time init. Deliberately small: the marker read is a
# single indexed ``SELECT`` that is sub-millisecond once it holds a connection,
# so the only thing this budget covers is *acquiring* a connection. During a
# cold-boot thundering herd (a rollout or scale-up storm) the shared pool can be
# momentarily saturated; rather than let the marker read block and abort the
# whole pod (which crash-loops the fleet), we cap the wait here and skip init —
# a serving pod assumes an already-initialised database.
_BOOTSTRAP_MARKER_READ_TIMEOUT_SECONDS: float = 3.0

# Upper bound on the one-time init (base-extension bootstrap + platform-config
# storage) run from the priority-7 foundational lifespan on a not-yet-marked
# database. Generous enough for genuine ``CREATE EXTENSION`` work (PostGIS et al.)
# against a free pool on a fresh DB, but bounded so a cold-boot pool storm can
# never let a DB probe hang — and then abort — the foundational lifespan. On
# expiry (or any failure) the pod boots degraded; a later boot or the dedicated
# init job completes the idempotent, advisory-locked steps once the pool clears.
_INIT_DB_BOOTSTRAP_TIMEOUT_SECONDS: float = 20.0

# PostgreSQL SQLSTATEs that mean the marker's schema/table does not exist yet —
# i.e. a genuinely un-bootstrapped database (``catalog.shared_properties`` is
# created later, by ``CatalogModule`` at priority 20, so at this priority-7 read
# a fresh DB legitimately has neither the ``catalog`` schema nor the table).
# These are the ONLY read failures that should route into the one-time init; any
# other failure (a connection/pool error, a timeout) means the DB is reachable-
# but-saturated — an already-initialised DB under load — where init must be
# skipped, never re-run.
_FRESH_DB_SQLSTATES: frozenset[str] = frozenset(
    {"42P01", "3F000"}  # undefined_table, invalid_schema_name
)


def _is_fresh_db_error(exc: BaseException) -> bool:
    """True iff ``exc`` (or a cause in its chain) is a missing-schema/table error.

    Distinguishes a genuinely fresh database (the marker's schema/table has not
    been created yet → bootstrap) from an already-initialised database we simply
    could not reach under pool pressure (→ skip). Walks the ``__cause__`` /
    ``__context__`` chain and inspects both the exception and any DBAPI
    ``orig`` for an asyncpg ``sqlstate`` or psycopg ``pgcode``.
    """
    seen: set[int] = set()
    cur: Optional[BaseException] = exc
    while cur is not None and id(cur) not in seen:
        seen.add(id(cur))
        for obj in (cur, getattr(cur, "orig", None)):
            if obj is None:
                continue
            code = getattr(obj, "sqlstate", None) or getattr(obj, "pgcode", None)
            if code in _FRESH_DB_SQLSTATES:
                return True
        cur = cur.__cause__ or cur.__context__
    return False


async def _base_extensions_present(resource: DbResource) -> bool:
    """True iff the base extensions are installed in this database.

    DB-backed truth: a direct ``pg_extension`` probe on the sentinel extension.

    Deliberately **uncached**. A ``@cached`` wrapper here is not merely
    ineffective at the priority-7 ``DatastoreModule`` lifespan (which runs before
    ``CacheModule`` at priority 9 registers the distributed backend) — it is
    actively harmful during a cold-boot thundering herd: every pod misses the
    positive and blocks in the cache slow-path (``_await_shared_rebuild``, ~30s)
    waiting for *another* pod to compute and publish the value, but none ever
    does (they are all waiting on each other), so the wait re-raises and aborts
    the foundational lifespan — and no pod runs the probe that would let it
    create the extensions. A direct probe lets each pod get its own answer, so
    exactly one wins the advisory lock in ``ensure_base_extensions`` and creates
    the extensions while the rest observe them present. The probe is a single
    indexed ``pg_extension`` ``SELECT`` — cheap enough to run on every boot.

    Reading the live catalog is also inherently repoint-safe: a freshly
    provisioned / repointed database (sentinel absent) always reports ``False``
    and re-bootstraps, and can never inherit a stale "present" answer keyed to a
    previous database.
    """
    from dynastore.modules.db_config.locking_tools import check_extension_exists

    return await check_extension_exists(resource, _EXT_SENTINEL)


async def _platform_bootstrap_present(resource: DbResource) -> bool:
    """True iff a prior boot fully initialised this platform (marker set).

    Deliberately **uncached** and **error-propagating**, unlike
    :func:`dynastore.modules.catalog.bootstrap_guard.is_initialized`, which
    swallows every read failure into ``False``. That swallowing is unsafe here:
    a serving pod whose marker IS set but which cannot reach the DB under a
    cold-boot pool storm would degrade to "not initialised" and wrongly re-run
    the one-time init (which then blocks and aborts the foundational lifespan).
    By letting the read raise, the sole caller can tell a genuinely fresh DB
    (missing schema/table → bootstrap) from an unreachable one (→ skip).

    Reads ``platform.bootstrap_initialized`` from ``catalog.shared_properties``
    via ``PropertiesProtocol`` on the passed engine. Two kinds of "cannot read"
    are treated differently, which is the whole point of not delegating to
    ``is_initialized`` (that helper collapses both into ``False``):

    * ``PropertiesProtocol`` not registered → return ``False`` (degrade to "not
      initialised"). Protocol registration is in-memory and independent of the
      database/pool, so this can NEVER be a cold-boot pool-pressure signal; it
      only happens in a degenerate/unconfigured context, where re-running the
      idempotent init is harmless and a genuinely fresh DB must still bootstrap.
    * a DB read failure (missing schema/table, or a connection/pool error) →
      **propagate**, so the caller can tell a fresh DB (bootstrap) from an
      unreachable-under-pressure one (skip).

    Not ``@cached``: this runs from the priority-7 ``DatastoreModule`` lifespan,
    before ``CacheModule`` (priority 9) registers the distributed backend, so a
    cache wrapper would resolve to a process-local backend only — no cross-fleet
    dedup, no intra-pod reuse (called once per boot) — while dragging in the
    cache slow-path that blocks ~30s and re-raises on a cold miss.
    """
    from dynastore.modules.catalog.bootstrap_guard import BOOTSTRAP_GUARD_KEY
    from dynastore.models.protocols.properties import PropertiesProtocol
    from dynastore.tools.discovery import get_protocol

    props = get_protocol(PropertiesProtocol)
    if props is None:
        logging.getLogger(__name__).debug(
            "bootstrap-marker read: PropertiesProtocol not registered — "
            "degrading to 'not initialised'."
        )
        return False
    value = await props.get_property(BOOTSTRAP_GUARD_KEY, db_resource=resource)
    return value == "true"


async def ensure_base_extensions(resource: DbResource) -> None:
    """Ensure the base Postgres extensions exist — guarded for per-boot cheapness.

    Safe to call from any service's startup on any (sync or async) engine: the
    presence probe collapses the steady-state cost to a single indexed
    ``pg_extension`` ``SELECT``. Only when the sentinel extension is genuinely
    absent are the ``CREATE EXTENSION`` statements issued — each one advisory-
    locked + idempotent via ``ensure_db_extension`` (which embeds its own
    connection-invalidation retry), so a fleet-wide boot herd converges with a
    single winner creating the extensions and the rest observing them present.
    """
    if await _base_extensions_present(resource):
        return
    for ext in BASE_DB_EXTENSIONS:
        await maintenance_tools.ensure_db_extension(resource, ext)


async def ensure_init_db(resource: DbResource):
    """Initializes the database base extensions + platform-config storage.

    The base-extension step is delegated to :func:`ensure_base_extensions`,
    whose direct presence probe makes repeated calls cheap; the platform-config
    initializer (which issues raw DDL directly) is wrapped in
    ``retry_on_invalidated_connection`` so a transient DB drop during dev startup
    (db_entrypoint_dev.sh reset) does not abort the foundational lifespan.

    Gated on the platform bootstrap marker via
    :func:`_platform_bootstrap_present`: once a prior boot has fully initialised
    this deployment (marker ``platform.bootstrap_initialized`` set in
    ``catalog.shared_properties``), the whole one-time step is skipped from a
    single direct read. This keeps a serving pod's foundational lifespan from
    re-running DDL — and, more importantly, from re-issuing the DB-blocking
    extension-presence probe — on every cold boot against an already-initialised
    DB.

    The marker read is bounded (:data:`_BOOTSTRAP_MARKER_READ_TIMEOUT_SECONDS`)
    and its failures are classified rather than uniformly treated as "fresh DB":

    * marker present → skip (already initialised);
    * marker read fails with a missing-schema/table error → genuinely fresh DB
      (``catalog.shared_properties`` is created later, by ``CatalogModule`` at
      priority 20) → run the one-time init;
    * marker absent but readable → also run init (partially-initialised DB);
    * marker read times out or fails any other way → the DB is reachable-but-
      saturated, i.e. an already-initialised DB under a cold-boot thundering
      herd → **skip**. Re-running init here would drive the extension-presence
      probe into a ~30s block that re-raises and aborts the pod, crash-looping
      the fleet during a rollout / scale-up storm.

    Init proceeds only on a genuinely fresh (or partially-initialised, absent-
    but-readable-marker) DB. Even then it is **best-effort and bounded**
    (:data:`_INIT_DB_BOOTSTRAP_TIMEOUT_SECONDS`): a rollout / scale-up storm can
    hit a freshly reset dev database with many pods at once, so if the idempotent,
    advisory-locked bootstrap cannot complete in time the pod boots degraded
    rather than aborting — a later boot or the dedicated init job finishes it.
    A serving pod against an already-marked DB skips the whole step outright.
    """
    try:
        initialized = await asyncio.wait_for(
            _platform_bootstrap_present(resource),
            timeout=_BOOTSTRAP_MARKER_READ_TIMEOUT_SECONDS,
        )
    except asyncio.TimeoutError:
        logging.getLogger(__name__).warning(
            "ensure_init_db: bootstrap-marker read did not complete within "
            "%.0fs (database reachable but connection pool likely saturated at "
            "cold boot) — skipping one-time init to keep the foundational "
            "lifespan from aborting.",
            _BOOTSTRAP_MARKER_READ_TIMEOUT_SECONDS,
        )
        return
    except Exception as exc:  # noqa: BLE001 — classified below
        if not _is_fresh_db_error(exc):
            # Reachable-but-saturated: an already-initialised DB we could not
            # read under pool pressure. Skipping is correct and safe — re-running
            # init would block the extension probe ~30s and abort the pod.
            logging.getLogger(__name__).warning(
                "ensure_init_db: bootstrap-marker read failed (%s) — treating as "
                "an already-initialised database under pool pressure and skipping "
                "one-time init to keep the foundational lifespan from aborting.",
                exc,
            )
            return
        # Missing schema/table → genuinely fresh DB → fall through to init.
        logging.getLogger(__name__).info(
            "ensure_init_db: bootstrap-marker schema/table absent — treating as a "
            "fresh database and running one-time init."
        )
        initialized = False
    if initialized:
        logging.getLogger(__name__).info(
            "ensure_init_db: platform already initialised (bootstrap marker "
            "present) — skipping extension + platform-config bootstrap."
        )
        return

    # One-time init on a genuinely fresh (or partially-initialised) database.
    # Best-effort AND bounded: the base-extension bootstrap and platform-config
    # storage init are idempotent + advisory-locked, so if a cold-boot pool storm
    # keeps them from completing in time, booting degraded is strictly better than
    # aborting and crash-looping the fleet — a later boot or the dedicated init
    # job completes them once the pool clears. This mirrors the best-effort
    # treatment DBService already applies to its async-engine extension ensure;
    # the foundational DatastoreModule lifespan must be just as resilient.
    async def _run_one_time_init() -> None:
        await ensure_base_extensions(resource)
        # --- Initialize Platform Config Storage ---
        from dynastore.modules.db_config.platform_config_service import (
            PlatformConfigService,
        )

        await maintenance_tools.retry_on_invalidated_connection(
            lambda: PlatformConfigService.initialize_storage(resource),
            label="PlatformConfigService.initialize_storage",
        )

    try:
        await asyncio.wait_for(
            _run_one_time_init(), timeout=_INIT_DB_BOOTSTRAP_TIMEOUT_SECONDS
        )
    except Exception as exc:  # noqa: BLE001 — best-effort; must never abort boot
        logging.getLogger(__name__).warning(
            "ensure_init_db: one-time bootstrap (base extensions + platform-config "
            "storage) did not complete within %.0fs (%s) — continuing startup "
            "degraded rather than aborting the foundational lifespan; the steps are "
            "idempotent and advisory-locked, so a later boot or the init job "
            "completes them.",
            _INIT_DB_BOOTSTRAP_TIMEOUT_SECONDS,
            exc,
        )


def get_config(app_state) -> DBConfig:
    """Returns the current database configuration."""
    return app_state.db_config


# --- Reflection Tools ---

_get_table_columns_query = DQLQuery(
    "SELECT column_name, data_type, udt_name FROM information_schema.columns WHERE table_schema = :schema AND table_name = :table;",
    result_handler=ResultHandler.ALL,
)


def map_pg_type_to_sqlalchemy_type(
    pg_type: Union[str, Any], udt_name: Optional[str] = None
) -> Optional[Any]:
    """Maps PostgreSQL data_type strings to SQLAlchemy types."""
    if not isinstance(pg_type, str):
        # Handle PostgresType enum or similar
        pg_type = str(getattr(pg_type, "value", pg_type))

    pg_type = pg_type.lower()
    if pg_type == "user-defined" and udt_name == "geometry":
        return Geometry
    if pg_type in ("character varying", "text", "character"):
        return String
    if pg_type in ("integer", "smallint", "bigint"):
        return Integer
    if pg_type in ("double precision", "numeric", "real"):
        return Float
    if pg_type == "boolean":
        return Boolean
    if pg_type.startswith("timestamp"):
        return DateTime
    if pg_type == "uuid":
        return UUID
    if pg_type == "date":
        return DateTime  # Or Date if you want to be more specific
    if pg_type == "jsonb":
        return JSON
    if pg_type == "tstzrange":
        return TSTZRANGE
    # Return None for types we don't want to map (e.g., geometry, jsonb)
    # jsonb will be handled by the layer_config logic
    return None


def map_pg_to_json_type(pg_type: Union[str, Any]) -> str:
    """
    Map PostgreSQL type (string or Enum) to JSON Schema type.
    Centralized utility used by OGC Features, WFS, and Sidecars.
    """
    if not isinstance(pg_type, str):
        pg_type = str(getattr(pg_type, "value", pg_type))

    pg_type = pg_type.lower()

    if any(t in pg_type for t in ["int", "serial", "bigint"]):
        return "integer"
    if any(t in pg_type for t in ["float", "numeric", "double", "real", "decimal"]):
        return "number"
    if "bool" in pg_type:
        return "boolean"
    if "json" in pg_type:
        return "object"
    # Dates, UUIDs, Text are typically 'string' in JSON Schema
    return "string"


@cached(
    maxsize=128,
    namespace="field_mapping",
    ignore=["conn"],
    ttl=300,
)
async def get_dynamic_field_mapping(
    conn: DbResource, schema: str, table: str
) -> Dict[str, Column]:
    """
    Retrieves all 'flat' columns as a dictionary of SQLAlchemy Column objects.
    """
    try:
        result = await _get_table_columns_query.execute(
            conn, schema=schema, table=table
        )
        field_mapping = {}
        for row in result:
            col_name, pg_type, udt_name = row[0], row[1], row[2]
            sa_type = map_pg_type_to_sqlalchemy_type(pg_type, udt_name)
            if sa_type:
                # Create a SQLAlchemy Column object for this field
                field_mapping[col_name] = Column(col_name, sa_type)

        return field_mapping
    except Exception as e:
        logging.getLogger(__name__).error(
            f"Failed to dynamically get field mapping for {schema}.{table}: {e}",
            exc_info=True,
        )
        return {}
