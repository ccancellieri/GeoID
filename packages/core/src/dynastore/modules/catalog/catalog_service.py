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
CatalogService: Handles all catalog-level CRUD operations.

This service implements CatalogsProtocol and provides:
- Catalog creation, retrieval, updates, deletion
- Catalog listing and search
- Physical schema resolution
- Catalog-level caching
"""

import asyncio
import logging
import json
import os
from typing import (
    Awaitable,
    Callable,
    List,
    Optional,
    Any,
    Dict,
    FrozenSet,
    TypeVar,
    Union,
    Set,
    Tuple,
    TYPE_CHECKING,
)

if TYPE_CHECKING:
    from dynastore.modules.storage.drivers.pg_sidecars.base import ConsumerType
    from dynastore.modules.db_config.query_executor import DDLBatch
    from dynastore.modules.storage.hints import Hint
from dynastore.tools.cache import cached
from dynastore.models.driver_context import DriverContext

from dynastore.modules.db_config.query_executor import (
    DDLQuery,
    DQLQuery,
    DbResource,
    ResultHandler,
    _is_transient_asyncpg_error,
    managed_nested_transaction,
    managed_transaction,
)
from dynastore.modules.catalog.models import (
    Catalog,
    CatalogUpdate,
    LocalizedText,
    Collection,
)
from dynastore.models.shared_models import Feature
from dynastore.modules.catalog.catalog_config import CollectionPluginConfig
from dynastore.models.protocols import (
    CatalogsProtocol,
    ItemsProtocol,
    CollectionsProtocol,
    AssetsProtocol,
    ConfigsProtocol,
    LocalizationProtocol,
)
from dynastore.tools.db import validate_sql_identifier
from dynastore.tools.json import CustomJSONEncoder
from dynastore.tools.discovery import get_protocol
from dynastore.models.query_builder import QueryRequest, QueryResponse
from dynastore.modules.catalog.event_service import CatalogEventType, emit_event
from dynastore.modules.db_config.maintenance_tools import ensure_schema_exists
from dynastore.modules.db_config.typed_store.ddl import tenant_configs_ddl
from dynastore.tools.async_utils import signal_bus
from dynastore.modules.catalog.lifecycle_manager import lifecycle_registry, LifecycleContext

logger = logging.getLogger(__name__)

# ==============================================================================
#  CORE DDL DEFINITIONS (Base Catalog)
# ==============================================================================

# 1. COLLECTIONS
TENANT_COLLECTIONS_DDL = """
CREATE TABLE IF NOT EXISTS {schema}.collections (
    id VARCHAR NOT NULL,
    external_id VARCHAR NOT NULL,
    catalog_id VARCHAR NOT NULL,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    deleted_at TIMESTAMPTZ DEFAULT NULL,
    lifecycle_status VARCHAR DEFAULT NULL,
    PRIMARY KEY (id)
);
CREATE UNIQUE INDEX IF NOT EXISTS collections_external_uq
    ON {schema}.collections (external_id)
    WHERE deleted_at IS NULL;

"""
"""``lifecycle_status`` is the transitional-state overlay (#2066):
``'provisioning'`` while external async init is in flight, ``'deleting'``
while a hard-delete purge is in flight, ``NULL`` otherwise.  It is resolved
*above* ``deleted_at`` by :meth:`CollectionStore.get_lifecycle`; a row with a
NULL overlay and NULL ``deleted_at`` is ``ACTIVE``."""

def _build_tenant_core_ddl_batch(schema: str) -> "DDLBatch":
    """Build the per-tenant core DDL batch.

    Warm path: ``collection_configs`` (the last table created by
    ``tenant_configs_ddl``) acts as the sentinel. If it exists, the
    collections + config tables are skipped in one round-trip.  Cold
    path runs all DDLs under a single connection with nested savepoints.

    The domain-scoped metadata tables (``collection_core`` +
    ``collection_stac``) are created by
    :func:`ensure_tenant_metadata_domain_tables` — not in this batch.

    The IAM-side tenant tables (``roles``, ``role_hierarchy``, ``grants``)
    are also added here so the unified-grants model is available before
    any per-tenant lifecycle hook (e.g. STAC, GCP) needs to issue grants
    or look up authorization. Default role rows are seeded by the
    ``IamModule`` lifecycle hook ``initialize_iam_tenant`` via
    :meth:`PolicyService.provision_default_policies` — which reads the
    catalog-tier seed list from ``IamRolesConfig.catalog_roles``.
    """
    from dynastore.modules.db_config.query_executor import DDLBatch
    from dynastore.modules.db_config.locking_tools import check_table_exists
    from dynastore.modules.iam.iam_queries import (
        CREATE_ROLES_TABLE,
        CREATE_ROLE_HIERARCHY_TABLE,
        CREATE_GRANTS_TABLE,
    )

    def _check_sentinel(conn):
        return check_table_exists(conn, "collection_configs", schema)

    tenant_configs_sql = tenant_configs_ddl(schema)
    return DDLBatch(
        sentinel=DDLQuery(tenant_configs_sql, check_query=_check_sentinel),
        steps=[
            DDLQuery(TENANT_COLLECTIONS_DDL),
            CREATE_ROLES_TABLE,
            CREATE_ROLE_HIERARCHY_TABLE,
            CREATE_GRANTS_TABLE,
            DDLQuery(tenant_configs_sql, check_query=_check_sentinel),
        ],
    )


# --- Helpers ---

BASE36 = "0123456789abcdefghijklmnopqrstuvwxyz"


def encode_base36(num: int) -> str:
    if num == 0:
        return BASE36[0]
    arr = []
    base = len(BASE36)
    while num:
        num, rem = divmod(num, base)
        arr.append(BASE36[rem])
    arr.reverse()
    return "".join(arr)


def generate_physical_name(prefix: str) -> str:
    """Generate a schema/bucket-safe physical name from a UUIDv7's random bits.

    Format: ``{prefix}_{13-char base32}``  e.g.  ``s_2ka8fbc3d4e5f``

    The 13-character suffix is drawn from the low 65 random bits of a UUIDv7
    (version + variant bits stripped).  Base32 (lowercase a-z + 2-9, RFC 4648
    alphabet minus ambiguous chars 0/1) yields ~67 bits of entropy, keeping
    birthday-collision probability below 1-in-10^6 past 10 M names in the
    same namespace.  The token is all-lowercase alphanumeric so it is safe
    as a PostgreSQL identifier, a GCS bucket-name component, and an ES index
    name component without quoting.  The full ``{prefix}_{13}`` string is at
    most 18 chars, well within PG's 63-char identifier limit.
    """
    from dynastore.tools.identifiers import generate_uuidv7

    # Base32 alphabet: digits 2-9 + a-z (avoids ambiguous 0/1/l/o).
    _ALPHABET = "23456789abcdefghijklmnopqrstuvwxyz"  # 34 chars; but we use 32
    _ALPHABET32 = _ALPHABET[:32]  # keep exactly 32 symbols for clean 5-bit grouping

    uid = generate_uuidv7().int
    # Mask to the low 65 bits (random portion of UUIDv7 v1 layout).
    rand_bits = uid & ((1 << 65) - 1)

    chars = []
    val = rand_bits
    for _ in range(13):
        chars.append(_ALPHABET32[val & 0x1F])
        val >>= 5
    chars.reverse()
    suffix = "".join(chars)
    return f"{prefix}_{suffix}"


def get_catalog_engine(db_resource: Optional[DbResource] = None) -> DbResource:
    """Get database engine for catalog operations."""
    if db_resource:
        return db_resource

    from dynastore.tools.protocol_helpers import get_engine

    return get_engine()  # type: ignore[return-value]


def _async_catalog_create_enabled() -> bool:
    """Return True when DYNASTORE_ASYNC_CATALOG_CREATE=true/1/yes.

    Default is False — catalog creation is synchronous and returns 201, identical
    to the pre-2329 behaviour.  When enabled, the core tenant-provisioning steps
    are deferred to the ``catalog_core_init`` worker task and the create endpoint
    returns 202 with a Location header pointing at the catalog resource.
    """
    return os.getenv("DYNASTORE_ASYNC_CATALOG_CREATE", "false").lower() in ("true", "1", "yes")


_T = TypeVar("_T")


async def _provisioning_write_with_retry(
    engine: DbResource,
    fn: Callable[[Any], Awaitable[_T]],
) -> _T:
    """Run ``fn(conn)`` inside a short, committed PG transaction.

    Retries exactly once on dead-connection transient errors (asyncpg
    InterfaceError / ConnectionDoesNotExistError) that surface when the
    pool recycles a wire that was closed server-side by
    ``idle_in_transaction_session_timeout``.  The retry uses a fresh
    connection acquired from the pool — never the stale one.

    This helper exists solely to centralise the retry logic for the
    provisioning write path (#1895).  It must NOT be used for non-
    idempotent writes.
    """
    for attempt in range(2):
        try:
            async with managed_transaction(engine) as conn:
                return await fn(conn)
        except Exception as exc:
            orig = getattr(exc, "orig", exc)
            is_transient = (
                _is_transient_asyncpg_error(exc)
                or _is_transient_asyncpg_error(orig)
            )
            if attempt == 0 and is_transient:
                logger.warning(
                    "provisioning_write_retry "
                    "attempt=0 exc=%s cause=%s; retrying on fresh connection",
                    exc.__class__.__name__,
                    orig.__class__.__name__,
                )
                await asyncio.sleep(0)  # yield to event loop before retry
                continue
            raise
    # Unreachable: the loop always returns or raises on both iterations.
    raise AssertionError("_provisioning_write_with_retry: exhausted attempts")


# PK constraint name for catalog.catalogs; asyncpg surfaces this in
# UniqueViolationError.constraint_name.  The name is fixed by the DDL in
# catalog_module.py (``id VARCHAR PRIMARY KEY``).
_CATALOG_PK_CONSTRAINT = "catalogs_pkey"
# Maximum retries for internal-id PK regeneration.  At 67-bit entropy the
# probability of 5 consecutive PK collisions is astronomically small.
_CATALOG_PK_MAX_RETRIES = 5


async def _insert_catalog_row_with_pk_retry(
    conn: Any,
    *,
    external_id: str,
    provisioning_status: str,
) -> str:
    """Insert the ``catalog.catalogs`` registry row, regenerating the internal id
    on a PK collision (astronomically rare after the entropy widening but still
    possible in theory).

    Returns the final ``internal_id`` that was committed.

    Only PK clashes (constraint = ``catalogs_pkey`` / pgcode 23505 on the ``id``
    column) trigger a retry.  A unique violation on ``external_id``
    (``catalogs_external_uq``) is a genuine user conflict and is re-raised
    immediately — it must NOT be retried.
    """
    from dynastore.modules.db_config.exceptions import UniqueViolationError as _UVE

    for attempt in range(_CATALOG_PK_MAX_RETRIES):
        internal_id = generate_physical_name("c")
        try:
            await _create_catalog_strict_query.execute(
                conn,
                id=internal_id,
                external_id=external_id,
                provisioning_status=provisioning_status,
            )
            return internal_id
        except Exception as exc:
            # Check whether this is a unique violation on the PK (id clash)
            # vs. the external_id unique index (real user conflict).
            orig = getattr(exc, "orig", exc)
            pgcode = getattr(orig, "pgcode", None)
            constraint = getattr(orig, "constraint_name", None) or ""
            is_unique = pgcode == "23505" or isinstance(exc, _UVE) or isinstance(orig, _UVE)
            is_pk_clash = is_unique and (
                constraint == _CATALOG_PK_CONSTRAINT
                or "catalogs_pkey" in str(exc).lower()
                or "catalogs_pkey" in str(orig).lower()
            )
            if is_unique and not is_pk_clash:
                # external_id unique-constraint violation — real conflict.
                if not isinstance(exc, _UVE):
                    raise _UVE(
                        f"Catalog '{external_id}' already exists"
                    ) from exc
                raise
            if is_pk_clash and attempt < _CATALOG_PK_MAX_RETRIES - 1:
                logger.warning(
                    "_insert_catalog_row_with_pk_retry: PK clash on attempt %d "
                    "(internal_id=%r); regenerating",
                    attempt, internal_id,
                )
                continue
            raise
    # Unreachable — loop always returns or raises.
    raise AssertionError("_insert_catalog_row_with_pk_retry: exhausted attempts")


def _build_catalog_metadata_payload(catalog_model: Catalog) -> Dict[str, Any]:
    """Flatten the Catalog model into a dict keyed by domain-metadata columns.

    Keys align with the column tuples in
    :mod:`dynastore.modules.storage.drivers.core_postgresql` (CORE)
    and :mod:`dynastore.modules.stac.drivers.postgresql` (STAC)
    so the ``catalog_router`` can fan the payload out to every
    registered driver and let each driver ``_filter_payload`` down to
    its own column slice.  Absent fields are omitted (not set to
    ``None``) so drivers skip the write entirely when their filtered
    slice is empty (default-fast invariant).

    This helper stays in the service layer because it knows the public
    Catalog model's shape.  The drivers read an opaque dict and are not
    coupled to the Catalog class.
    """
    out: Dict[str, Any] = {}

    # CORE domain fields
    if catalog_model.title is not None:
        out["title"] = catalog_model.title.model_dump(exclude_none=True) \
            if hasattr(catalog_model.title, "model_dump") else catalog_model.title
    if catalog_model.description is not None:
        out["description"] = catalog_model.description.model_dump(exclude_none=True) \
            if hasattr(catalog_model.description, "model_dump") else catalog_model.description
    if catalog_model.keywords is not None:
        out["keywords"] = catalog_model.keywords.model_dump(exclude_none=True) \
            if hasattr(catalog_model.keywords, "model_dump") else catalog_model.keywords
    if catalog_model.license is not None:
        out["license"] = catalog_model.license.model_dump(exclude_none=True) \
            if hasattr(catalog_model.license, "model_dump") else catalog_model.license
    if catalog_model.extra_metadata is not None:
        out["extra_metadata"] = catalog_model.extra_metadata.model_dump(exclude_none=True) \
            if hasattr(catalog_model.extra_metadata, "model_dump") else catalog_model.extra_metadata

    # STAC domain fields (catalog-tier subset — no extent / providers / summaries here)
    if catalog_model.stac_version:
        out["stac_version"] = catalog_model.stac_version
    if catalog_model.stac_extensions:
        out["stac_extensions"] = list(catalog_model.stac_extensions)
    conforms_to = getattr(catalog_model, "conformsTo", None)
    if conforms_to:
        out["conforms_to"] = list(conforms_to)
    if catalog_model.links:
        out["links"] = [
            link.model_dump(exclude_none=True) if hasattr(link, "model_dump") else link
            for link in catalog_model.links
        ]
    # ``assets`` on the Catalog envelope — catalog-level assets (not item assets).
    catalog_assets = getattr(catalog_model, "assets", None)
    if catalog_assets:
        out["assets"] = catalog_assets

    # Lifecycle field — required on the metadata-driver fan-out so search
    # backends (ES indexer) reflect the same state as the source-of-truth
    # ``catalog.catalogs`` row. PG CORE / STAC drivers ``_filter_payload``
    # this key out (it's not in their column tuples); ES has dynamic mapping
    # and indexes it as a keyword via the dynamic templates. Without this,
    # status transitions written via ``update_provisioning_status`` never
    # reach ES and the index goes stale (observed on review env 2026-04-30:
    # PG flipped to 'ready' but ES still showed 'provisioning').
    if catalog_model.provisioning_status is not None:
        out["provisioning_status"] = catalog_model.provisioning_status

    return out


def _extract_update_payload(
    catalog_model: Catalog,
    updated_fields: set[str],
) -> Dict[str, Any]:
    """Partial-update variant of :func:`_build_catalog_metadata_payload`.

    Unlike the full-envelope flattener, this helper emits ONLY the
    keys the caller listed in ``updated_fields`` (the set of fields the
    update request carried) and that have a non-None value on
    ``catalog_model``.  A PATCH that sets ``title`` alone yields a
    payload of exactly ``{"title": {"en": "..."}}`` — downstream
    Primary drivers' ``_filter_payload`` + PATCH-semantic UPSERT then
    touch nothing else in the split tables.

    The ``updated_fields`` set uses the public Catalog-model attribute
    names (``title``, ``description``, ``keywords``, ``license``,
    ``extra_metadata``, ``stac_version``, ``stac_extensions``,
    ``conformsTo`` / ``conforms_to``, ``links``, ``assets``).  The
    output dict uses snake_case keys matching the split-table column
    names — the drivers' ``_*_COLUMNS`` tuples consume that shape.
    """
    out: Dict[str, Any] = {}

    def _dump(value: Any) -> Any:
        if value is None:
            return None
        if hasattr(value, "model_dump"):
            return value.model_dump(exclude_none=True)
        return value

    # CORE + catalog-STAC field-name ↔ split-column mapping.  The
    # ``conformsTo`` alias is explicit here because the client-facing
    # field name is camelCase but the split column is snake_case.
    candidates = [
        ("title",            "title"),
        ("description",      "description"),
        ("keywords",         "keywords"),
        ("license",          "license"),
        ("extra_metadata",   "extra_metadata"),
        ("stac_version",     "stac_version"),
        ("stac_extensions",  "stac_extensions"),
        ("conformsTo",       "conforms_to"),
        ("conforms_to",      "conforms_to"),
        ("links",            "links"),
        ("assets",           "assets"),
    ]
    for src_field, out_key in candidates:
        if src_field not in updated_fields:
            continue
        value = getattr(catalog_model, src_field, None)
        if value is None:
            continue
        dumped = _dump(value)
        if dumped is None:
            continue
        if src_field in ("stac_extensions", "conformsTo", "conforms_to"):
            dumped = list(dumped) if hasattr(dumped, "__iter__") else dumped
        elif src_field == "links":
            dumped = [
                _dump(link) if hasattr(link, "model_dump") else link
                for link in value
            ]
        out[out_key] = dumped

    return out


# Fields owned exclusively by ``catalog.catalogs`` (technical registry
# row).  CatalogMetadata sidecar drivers (PG core/stac, ES indexer …) may
# carry stale snapshots of these — e.g. CatalogElasticsearchDriver indexes
# the full payload at create time but isn't re-indexed when
# ``update_provisioning_status`` flips the row.  ``_unpack_catalog_row``
# refuses to let any router overlay shadow these fields.
_CONTROL_PLANE_CATALOG_FIELDS: FrozenSet[str] = frozenset({
    "id",
    "provisioning_status",
    "deleted_at",
})


# --- Queries ---

# The catalog.catalogs INSERT carries only the technical registry
# columns.  Metadata lands in catalog.catalog_core / _stac via
# a router-direct upsert from ``create_catalog``; no legacy metadata
# columns remain on ``catalog.catalogs`` after the M2.5 hard cut.
#
# No ON CONFLICT clause: PK collisions are caught by _insert_catalog_row_with_pk_retry
# (which regenerates the internal id and retries up to 5 times on PK clash only).
# A unique violation on external_id is a genuine user conflict and bubbles as
# UniqueViolationError → HTTP 409.
_create_catalog_strict_query = DQLQuery(
    "INSERT INTO catalog.catalogs (id, external_id, provisioning_status) "
    "VALUES (:id, :external_id, :provisioning_status);",
    result_handler=ResultHandler.ROWCOUNT,
)

# #1175: store the materialised provisioning checklist and flip the catalog to
# 'provisioning' in one statement (called from create_catalog when at least one
# provisioner is active for the new catalog).
_set_provisioning_checklist_query = DQLQuery(
    "UPDATE catalog.catalogs "
    "SET provisioning_status = :status, "
    "provisioning_checklist = CAST(:checklist AS jsonb) "
    "WHERE id = :id;",
    result_handler=ResultHandler.NONE,
)

# #1175: read the current checklist for a step update (row-locked so concurrent
# provisioner completions serialise on the same catalog).
_get_provisioning_checklist_query = DQLQuery(
    "SELECT provisioning_checklist FROM catalog.catalogs WHERE id = :id FOR UPDATE;",
    result_handler=ResultHandler.ONE_DICT,
)

_get_catalog_query = DQLQuery(
    "SELECT * FROM catalog.catalogs WHERE id = :id AND deleted_at IS NULL;",
    result_handler=ResultHandler.ONE_DICT,
)

_list_catalogs_query = DQLQuery(
    "SELECT * FROM catalog.catalogs WHERE deleted_at IS NULL ORDER BY id LIMIT :limit OFFSET :offset;",
    result_handler=ResultHandler.ALL_DICTS,
)

# Threshold above which ``list_catalogs`` warns about sequential router
# round-trips.  ~200 RTTs (e.g. a 100-row page × 2 domain drivers) is
# the point at which p95 latency for a paged listing becomes noticeable
# under the current non-batched router read path.
_LIST_CATALOGS_ROUNDTRIP_WARN_THRESHOLD = 200

_soft_delete_catalog_query = DQLQuery(
    "UPDATE catalog.catalogs SET deleted_at = NOW() WHERE id = :id AND deleted_at IS NULL;",
    result_handler=ResultHandler.ROWCOUNT,
)

_hard_delete_catalog_query = DQLQuery(
    "DELETE FROM catalog.catalogs WHERE id = :id;",
    result_handler=ResultHandler.ROWCOUNT,
)

_drop_schema_query = DDLQuery("DROP SCHEMA IF EXISTS {schema} CASCADE;")

_delete_tenant_cron_jobs_query = DQLQuery(
    "DELETE FROM cron.job WHERE jobname LIKE :pattern;",
    result_handler=ResultHandler.ROWCOUNT,
)


def _catalog_model_is_ready(c: Any) -> bool:
    """Only cache 'ready' catalogs: transient states ('provisioning',
    'failed') would otherwise stick in L1 forever (no cross-worker
    invalidation), making init-upload return 503 long after provisioning
    completes.  Applied on read too (cache.py fast path) so pre-existing
    stale entries can't keep being served.  L1 stores the Catalog model
    directly; L2 (msgpack) returns a dict — handle both.
    """
    if c is None:
        return False
    status = (
        c.get("provisioning_status")
        if isinstance(c, dict)
        else getattr(c, "provisioning_status", None)
    )
    return status == "ready"


@cached(
    maxsize=128,
    ttl=30,
    jitter=5,
    namespace="catalog_model",
    condition=_catalog_model_is_ready,
    ignore=["service"],
)
async def _catalog_model_cache(service: "CatalogService", catalog_id: str):
    """Process-shared cache for catalog metadata models.

    Keyed on ``catalog_id`` only — ``service`` is ignored so every
    ``CatalogService`` instance shares one cache entry per catalog. A
    module-level cache (single decorator closure → single backend) is what
    makes ``cache_invalidate`` from any instance visible to reads issued
    through any other instance; an instance-bound cache gives each service its
    own backend, so a write+invalidate on one instance leaves stale entries
    readable through another.
    """
    return await service._get_catalog_model_db(catalog_id)


@cached(
    maxsize=2048,
    ttl=300,
    namespace="catalog_physical_schema",
    ignore=["service"],
    condition=lambda v: v is not None,
)
async def _physical_schema_cache(
    service: "CatalogService", catalog_id: str
) -> Optional[str]:
    """Resolve a catalog's physical PG schema as a plain string.

    Read straight from the authoritative ``catalog.catalogs`` registry — NOT
    derived from the cached ``Catalog`` model. ``physical_schema`` does not
    survive the distributed (L2) cache round-trip (the Valkey encoder serializes
    models via ``model_dump_json()``, which historically dropped the
    ``exclude=True`` field; the field is now off the model entirely), so a
    process reading the model cold from L2 would resolve ``None`` and fail. A
    plain string round-trips losslessly; ``condition`` keeps misses out of the
    cache so a just-provisioned catalog resolves immediately. The cache is a
    pure accelerator — on any miss (L1 or L2) the registry SELECT is the source
    of truth.
    """
    return await service._get_physical_schema_db(catalog_id)


def _invalidate_catalog_model_cache(catalog_id: str) -> None:
    """Drop the shared catalog-model cache entry for a catalog.

    Also drops the physical-schema string cache so a delete / tombstone-reclaim
    can't leave a stale schema pointer behind. ``service`` is part of the cache
    signature but ignored for keying, so any sentinel is fine here.
    """
    _catalog_model_cache.cache_invalidate(None, catalog_id)
    _physical_schema_cache.cache_invalidate(None, catalog_id)


@cached(
    maxsize=2048,
    ttl=300,
    namespace="catalog_external_id",
    ignore=["service"],
    condition=lambda v: v is not None,
)
async def _catalog_external_id_cache(
    service: "CatalogService", external_id: str
) -> Optional[str]:
    """Resolve a catalog's internal ``id`` from its public ``external_id``.

    Read straight from the authoritative ``catalog.catalogs`` registry.
    A plain string round-trips losslessly through the distributed cache;
    ``condition`` keeps misses out so a just-created catalog resolves
    immediately.  The cache is a pure accelerator — on any miss the
    registry SELECT is the source of truth.
    """
    return await service._get_catalog_id_by_external_id_db(external_id)


def _invalidate_catalog_external_id_cache(external_id: str) -> None:
    """Drop the external_id → internal id cache entry for a catalog.

    Called on create (in case a tombstone was reclaimed) and on future
    rename/delete operations.  ``service`` is ignored for keying so any
    sentinel works.
    """
    _catalog_external_id_cache.cache_invalidate(None, external_id)


from dynastore.modules.catalog.collection_service import CollectionService
from dynastore.modules.catalog.item_service import ItemService


class CatalogService(CatalogsProtocol):
    """Service for catalog-level operations implementing CatalogsProtocol."""

    # Protocol attributes
    priority: int = 10  # Higher priority than CatalogModule

    def __init__(
        self,
        engine: Optional[DbResource] = None,
        collection_service: Optional[CollectionService] = None,
        item_service: Optional[ItemService] = None,
        cascade_orchestrator: Optional[Any] = None,
    ):
        self.engine = engine
        self._collection_service = collection_service
        self._item_service = item_service
        # Lazy-import to avoid circular dependency at module load time.
        # CascadeOrchestrator defaults to the process-global registry.
        self._cascade_orchestrator = cascade_orchestrator
        
        # Initialize internal services if not provided, provided we have an engine
        if self.engine:
            if not self._collection_service:
                self._collection_service = CollectionService(self.engine)
            if not self._item_service:
                self._item_service = ItemService(self.engine)

        # The catalog-model read cache is a process-shared module-level cache
        # (``_catalog_model_cache``) rather than an instance-bound one, so
        # invalidations land in the same backend every instance reads from.
        # See that function's docstring for why this matters.

    def is_available(self) -> bool:
        """Returns True if the service is initialized and ready."""
        return (
            self.engine is not None
            and self._collection_service is not None
            and self._col_svc.is_available()
            and self._item_service is not None
            and self._item_svc.is_available()
        )

    @property
    def _col_svc(self) -> CollectionService:
        assert self._collection_service is not None
        return self._collection_service

    @property
    def _item_svc(self) -> ItemService:
        assert self._item_service is not None
        return self._item_service

    # === Unified Protocol Properties (Delegation) ===

    @property
    def items(self) -> ItemsProtocol:
        assert self._item_service is not None
        return self._item_service

    @property
    def collections(self) -> CollectionsProtocol:
        from typing import cast as _cast
        assert self._collection_service is not None
        # CollectionService implements most of CollectionsProtocol; aspirational methods not yet done
        return _cast(CollectionsProtocol, self._collection_service)

    @property
    def assets(self) -> Optional[AssetsProtocol]:
        from dynastore.tools.discovery import get_protocol as _gp
        return _gp(AssetsProtocol)

    @property
    def configs(self) -> Optional[ConfigsProtocol]:
        from dynastore.tools.discovery import get_protocol as _gp
        return _gp(ConfigsProtocol)

    @property
    def localization(self) -> Optional[LocalizationProtocol]:
        from dynastore.tools.discovery import get_protocol as _gp
        return _gp(LocalizationProtocol)

    # --- Schema Resolution ---

    async def _get_physical_schema_db(self, catalog_id: str) -> Optional[str]:
        """Authoritative physical-schema lookup against ``catalog.catalogs``.

        Since ``id`` IS the schema name (physical_schema column was dropped),
        this returns the catalog's ``id`` directly from the registry row.
        This is the cold-miss fallback behind ``_physical_schema_cache``.
        """
        async with managed_transaction(self.engine) as conn:
            return await DQLQuery(
                "SELECT id FROM catalog.catalogs WHERE id = :catalog_id AND deleted_at IS NULL;",
                result_handler=ResultHandler.SCALAR_ONE_OR_NONE,
            ).execute(conn, catalog_id=catalog_id)

    async def _get_catalog_id_by_external_id_db(self, external_id: str) -> Optional[str]:
        """Authoritative external_id → internal id lookup against ``catalog.catalogs``.

        The cold-miss fallback behind ``_catalog_external_id_cache``.
        """
        async with managed_transaction(self.engine) as conn:
            return await DQLQuery(
                "SELECT id FROM catalog.catalogs "
                "WHERE external_id = :external_id AND deleted_at IS NULL;",
                result_handler=ResultHandler.SCALAR_ONE_OR_NONE,
            ).execute(conn, external_id=external_id)

    async def resolve_catalog_id(
        self,
        external_id: str,
        allow_missing: bool = False,
    ) -> Optional[str]:
        """Resolve the immutable internal ``id`` for a catalog from its public ``external_id``.

        Authoritative source: the ``catalog.catalogs`` registry. Goes through
        ``_catalog_external_id_cache`` — a lossless string cache. The cache is
        a pure accelerator; on any miss the registry SELECT is the source of
        truth.

        Returns the internal id string, or ``None`` / raises ``ValueError``
        depending on ``allow_missing``.
        """
        internal_id = await _catalog_external_id_cache(self, external_id)
        if not internal_id and not allow_missing:
            raise ValueError(f"Catalog '{external_id}' not found.")
        return internal_id

    async def resolve_physical_schema(
        self,
        catalog_id: str,
        ctx: Optional["DriverContext"] = None,
        allow_missing: bool = False,
    ) -> Optional[str]:
        """Resolve the per-tenant physical PG schema for a catalog.

        Authoritative source: the ``catalog.catalogs`` registry. When a caller
        supplies a connection (``ctx.db_resource``) the lookup joins that
        transaction directly; otherwise it goes through ``_physical_schema_cache``
        — a lossless *string* cache. Resolution is never derived from the cached
        ``Catalog`` model (which cannot carry ``physical_schema`` across the
        distributed cache — see ``_physical_schema_cache``).

        Phase 2: accepts both external and internal catalog ids.  Resolution is
        **internal-id-first**: ``id`` is the immutable, unambiguous PK (and IS the
        schema name), so a direct id hit is authoritative and taken as-is.  Only
        when ``catalog_id`` is not a known internal id does the lookup fall back to
        resolving it as a public ``external_id``.  The reverse order is unsafe: an
        already-internal id passed to the external resolver can collide with a
        *different* catalog whose ``external_id`` happens to equal that id, silently
        routing to the wrong schema (observed on dev where legacy rows carry
        ``c_…``-shaped external_ids).  Internal-first makes all callers — external
        path-param id or already-resolved internal id — correct.
        """
        db_resource = ctx.db_resource if ctx else None
        if db_resource:
            async with managed_transaction(db_resource) as conn:
                # Internal-first: a direct id hit is authoritative.
                res = await DQLQuery(
                    "SELECT id FROM catalog.catalogs WHERE id = :catalog_id AND deleted_at IS NULL;",
                    result_handler=ResultHandler.SCALAR_ONE_OR_NONE,
                ).execute(conn, catalog_id=catalog_id)
                if res:
                    return res
                # Not a known internal id — interpret as a public external_id.
                _internal = await _catalog_external_id_cache(self, catalog_id)
                if _internal is not None:
                    res = await DQLQuery(
                        "SELECT id FROM catalog.catalogs WHERE id = :catalog_id AND deleted_at IS NULL;",
                        result_handler=ResultHandler.SCALAR_ONE_OR_NONE,
                    ).execute(conn, catalog_id=_internal)
                    if res:
                        return res
                if not allow_missing:
                    raise ValueError(f"Catalog '{catalog_id}' not found.")
                return None
        # No caller-supplied connection: use the string caches, internal-first.
        ps = await _physical_schema_cache(self, catalog_id)
        if ps:
            return ps
        # Not a known internal id — interpret as a public external_id.
        _internal = await _catalog_external_id_cache(self, catalog_id)
        if _internal is not None:
            ps = await _physical_schema_cache(self, _internal)
            if ps:
                return ps
        if not allow_missing:
            raise ValueError(f"Catalog '{catalog_id}' not found.")
        return None

    # --- Collection Resolution ---
    async def resolve_datasource(
        self,
        catalog_id: str,
        collection_id: str,
        *,
        operation: str = "READ",
        hints: Optional[FrozenSet["Hint"]] = None,
    ):
        """Resolve the best storage driver for a collection.

        Delegates to the storage router which resolves via
        ``ItemsRoutingConfig`` operation → ordered driver list.
        """
        from dynastore.modules.storage.router import get_driver
        return await get_driver(
            operation,
            catalog_id,
            collection_id,
            hints=hints if hints is not None else frozenset(),
        )

    async def resolve_physical_table(
        self,
        catalog_id: str,
        collection_id: str,
        db_resource: Optional[DbResource] = None,
    ) -> Optional[str]:
        return await self._col_svc.resolve_physical_table(
            catalog_id, collection_id, db_resource=db_resource
        )

    async def is_active(
        self,
        catalog_id: str,
        collection_id: str,
        db_resource: Optional[DbResource] = None,
    ) -> bool:
        return await self._col_svc.is_active(
            catalog_id, collection_id, db_resource=db_resource
        )

    async def ensure_alive(
        self,
        catalog_id: str,
        collection_id: str,
        db_resource: Optional[DbResource] = None,
    ) -> None:
        await self._col_svc.ensure_alive(
            catalog_id, collection_id, db_resource=db_resource
        )

    async def activate_collection(
        self,
        catalog_id: str,
        collection_id: str,
        ctx: Optional["DriverContext"] = None,
    ) -> None:
        await self._col_svc.activate_collection(
            catalog_id, collection_id, ctx=ctx,
        )

    async def set_physical_table(
        self,
        catalog_id: str,
        collection_id: str,
        physical_table: str,
        db_resource: Optional[DbResource] = None,
    ) -> None:
        return await self._col_svc.set_physical_table(
            catalog_id, collection_id, physical_table, db_resource=db_resource
        )

    # --- Catalog CRUD ---

    async def ensure_catalog_exists(
        self,
        catalog_id: str,
        lang: str = "en",
        ctx: Optional["DriverContext"] = None,
    ) -> None:
        """Ensures that a catalog exists, creating it if necessary (JIT creation)."""
        # Phase 2: catalog_id is the external public label.  Resolve to internal
        # before checking existence so get_catalog_model (which queries by internal
        # id) does not always report "missing" and trigger spurious re-creation.
        existing_internal = await self.resolve_catalog_id(catalog_id, allow_missing=True)
        if existing_internal is not None:
            # Already exists; nothing to do.
            return
        # If lang is not '*', we provide a simple string which create_catalog will localize
        # If lang is '*', we provide the default 'en' dictionary
        title = {"en": catalog_id} if lang == "*" else catalog_id
        await self.create_catalog(
            {"id": catalog_id, "title": title},
            lang=lang,
            ctx=ctx,
        )

    async def ensure_collection_exists(
        self,
        catalog_id: str,
        collection_id: str,
        lang: str = "en",
        ctx: Optional["DriverContext"] = None,
    ) -> None:
        """Ensures that a collection exists, creating it if necessary (JIT creation)."""
        db_resource = ctx.db_resource if ctx else None
        # Phase 2: resolve external catalog_id → internal before delegating to the
        # internal CollectionService helper.  The collection_id remains as-is
        # (external) so CollectionService.ensure_collection_exists → create_collection
        # can perform the external→internal split there.  When the catalog does not
        # yet exist, keep the original external catalog_id so create_collection's
        # own catalog-existence check fires with an actionable error.
        internal_catalog_id = await self.resolve_catalog_id(catalog_id, allow_missing=True)
        if internal_catalog_id is not None:
            catalog_id = internal_catalog_id
        await self._col_svc.ensure_collection_exists(
            db_resource, catalog_id, collection_id, lang=lang  # type: ignore[arg-type]
        )

    async def ensure_physical_table_exists(
        self,
        catalog_id: str,
        collection_id: str,
        config: Any,
        db_resource: Optional[DbResource] = None,
    ) -> None:
        return await self._item_svc.ensure_physical_table_exists(
            catalog_id, collection_id, config, db_resource=db_resource
        )

    async def ensure_partition_exists(
        self,
        catalog_id: str,
        collection_id: str,
        config: Any,
        partition_value: Any,
        ctx: Optional["DriverContext"] = None,
    ) -> None:
        return await self._item_svc.ensure_partition_exists(
            catalog_id, collection_id, config, partition_value, ctx=ctx
        )

    async def get_catalog(
        self,
        catalog_id: str,
        lang: str = "en",
        ctx: Optional["DriverContext"] = None,
        *,
        hints: FrozenSet = frozenset(),
    ) -> Catalog:
        # Phase 2: resolve external→internal id at the public boundary so every
        # downstream path (visibility check, cache lookup, DB query) operates on
        # the immutable internal key.  allow_missing=True so that callers
        # holding an already-internal id (pre-Phase-2 path params, internal
        # service calls) fall through without a false 404.  A genuinely missing
        # catalog is caught by get_catalog_model returning None below.
        _resolved = await self.resolve_catalog_id(catalog_id, allow_missing=True)
        if _resolved is not None:
            catalog_id = _resolved

        # Enforce the direct-get visibility contract (#2050): a catalog the
        # caller has no visibility grant for must be indistinguishable from a
        # missing one.  resolve_catalog_listing_ids() returns None when no
        # authorization layer is active (IAM off) — in that case we skip the
        # check and preserve prior behaviour.  An empty frozenset means the
        # caller may see nothing; a non-empty frozenset that does not contain
        # catalog_id (now the resolved internal id) means this specific catalog
        # is filtered out for this caller.  Both map to the same ValueError the
        # "genuinely missing" branch raises so the HTTP layer renders a uniform 404.
        from dynastore.models.protocols.visibility import resolve_catalog_listing_ids

        visible_ids = await resolve_catalog_listing_ids()
        if visible_ids is not None and catalog_id not in visible_ids:
            raise ValueError(f"Catalog '{catalog_id}' not found.")

        model = await self.get_catalog_model(catalog_id, ctx=ctx, hints=hints)
        if not model:
            raise ValueError(f"Catalog '{catalog_id}' not found.")
        return model

    async def _run_core_init(
        self,
        conn: Any,
        catalog_model: Catalog,
        external_id: str,
        physical_schema: str,
    ) -> None:
        """Run the core tenant-provisioning steps inside an existing transaction.

        Executes all DDL and lifecycle hooks that must complete atomically with
        the catalog.catalogs INSERT before the transaction commits.  The caller
        owns the transaction; this method must NOT open a new one.

        Steps (in order):
          1. Create the tenant schema (``IF NOT EXISTS`` — idempotent).
          2. Create core tenant tables (collections, configs, IAM) via the
             module-level DDL batch (warm-path sentinel skips in one round-trip).
          3. Create per-tenant collection-metadata tables (catalog_core et al.).
          4. Run module-specific ``init_catalog`` lifecycle hooks (SAVEPOINTs).
          5. Stamp ``external_id`` on ``catalog_model`` for downstream drivers.
          6. Persist catalog metadata via the catalog router.
          7. Materialise the provisioning checklist and flip status to
             ``'provisioning'`` when at least one provisioner is active.
          8. Run ``post_create_catalog`` lifecycle hooks (SAVEPOINTs).
          9. Snapshot catalog config defaults (best-effort, non-fatal).
         10. Emit ``CATALOG_CREATION`` and ``AFTER_CATALOG_CREATION`` events.

        Mutates ``catalog_model.external_id`` and
        ``catalog_model.provisioning_status`` in place.
        """
        # --- CRITICAL: Core tenant tables MUST be created directly in the outer
        # transaction, NOT inside a lifecycle SAVEPOINT (begin_nested).
        #
        # PostgreSQL DDL (CREATE SCHEMA, CREATE TABLE) inside a SAVEPOINT is
        # problematic: if any error occurs, only the SAVEPOINT rolls back — but
        # because DDL is not transactional in some PG contexts (especially when
        # combined with the asyncpg driver), the schema/tables may or may not be
        # created, leaving subsequent SAVEPOINT-wrapped hooks (stats, tiles, gcp…)
        # with nothing to work against.
        #
        # By creating schema + core tables here (outer tx), all lifecycle hooks
        # are guaranteed to find them ready.

        # 1. Tenant schema only. The shared ``configs`` schema and its
        # tables (the FK target for catalog_configs/collection_configs) are
        # bootstrapped once at application startup by
        # PlatformConfigService.initialize_storage. Re-asserting that shared
        # DDL on every create took schema-level locks and silently
        # serialized concurrent creates under load. Bootstrap once at boot,
        # never per request.
        await ensure_schema_exists(conn, physical_schema)

        # 2. Core Tables (collections, catalog_configs, collection_configs)
        # Single module-level batch — warm path skips everything in one
        # round-trip once collection_configs (the last table) exists.
        logger.info(
            f"Creating core tenant tables for schema: {physical_schema} (Catalog: {catalog_model.id})"
        )
        await _build_tenant_core_ddl_batch(physical_schema).execute(
            conn, schema=physical_schema
        )

        # 2b. Catalog-tier IAM seeding is performed by the IamModule's
        # lifecycle hook ``initialize_iam_tenant`` which calls
        # ``PolicyService.provision_default_policies(catalog_id, ...)``.
        # That path is config-driven (``IamRolesConfig.catalog_roles``)
        # and replaces the historical inline SQL seed (geoid#643).

        # 3. Per-tenant collection-metadata CORE table.  STAC sidecar
        # (when StacModule is loaded) attaches via lifecycle_registry
        # below.  MUST precede lifecycle hooks because downstream
        # drivers may write metadata immediately.
        from dynastore.modules.catalog.db_init.core_tables import (
            ensure_tenant_core_tables,
        )
        await ensure_tenant_core_tables(conn, physical_schema)

        # 4. Module-specific lifecycle hooks (stats, tiles, …) all run AFTER
        #    the schema and core tables exist, inside their own SAVEPOINTs.
        await lifecycle_registry.init_catalog(
            conn, physical_schema, catalog_id=catalog_model.id
        )

        # Stamp external_id on the model so metadata drivers and callers
        # can round-trip it without re-querying the registry.
        catalog_model.external_id = external_id  # type: ignore[attr-defined]

        # Catalog metadata persistence — router-direct.
        #
        # The catalog.catalogs registry row is committed (INSERT above),
        # so the FK into catalog.catalogs(id) from the domain-scoped
        # metadata tables is satisfied.  The router fans out the
        # payload across every registered CatalogStore driver
        # (PG Core / PG Stac today; ES indexers, etc. in the future).
        # Each driver filters down to its own domain's columns and
        # skips the write when the filtered payload is empty — so a
        # caller who supplied no metadata produces zero rows, and a
        # STAC-only payload writes only to the STAC driver.
        catalog_metadata = _build_catalog_metadata_payload(catalog_model)
        if catalog_metadata:
            from dynastore.modules.catalog.catalog_router import (
                upsert_catalog_metadata,
            )
            await upsert_catalog_metadata(
                catalog_model.id,
                catalog_metadata,
                db_resource=conn,
            )

        # #1175: materialise the provisioning checklist from the registered
        # provisioners now that the row exists. Built BEFORE the post-create
        # hooks so the full barrier is in place before any provisioner marks
        # its step — a step that completes early can't flip the catalog ready
        # while a slower step is still pending. An empty checklist (on-prem /
        # no active provider) leaves the catalog 'ready'; otherwise it becomes
        # 'provisioning' until every step is terminal.
        from dynastore.modules.catalog.provisioning_registry import (
            provisioning_registry,
            STATUS_PROVISIONING,
        )
        checklist = await provisioning_registry.build_checklist(
            catalog_model.id, conn
        )
        if checklist:
            await _set_provisioning_checklist_query.execute(
                conn,
                id=catalog_model.id,
                status=STATUS_PROVISIONING,
                checklist=json.dumps(checklist),
            )
            catalog_model.provisioning_status = STATUS_PROVISIONING
            _invalidate_catalog_model_cache(catalog_model.id)

        # Post-INSERT sync lifecycle phase: runs after the catalog.catalogs
        # row exists so module hooks may reference it (FK inserts, status
        # UPDATEs). A provisioner's post-create hook does its synchronous
        # work and/or enqueues an async task that later calls
        # ``mark_provisioning_step`` for its checklist key (#1131 / #1175).
        await lifecycle_registry.post_create_catalog(
            conn, physical_schema, catalog_id=catalog_model.id
        )

        # #1079 (c): freeze the catalog's inherited config defaults now that
        # the registry row + tenant config tables exist. Captures the
        # resolved platform/code defaults for stable value-configs into a
        # schema-id-tagged blob so a later default change cannot silently
        # re-resolve into this catalog's collections. Best-effort — a
        # snapshot failure must not abort catalog creation.
        try:
            _cfg = self.configs
            if _cfg is not None:
                await _cfg.snapshot_catalog_defaults(
                    catalog_model.id, ctx=DriverContext(db_resource=conn)
                )
        except Exception:
            logger.warning(
                "catalog %s: defaults-snapshot capture failed",
                catalog_model.id,
                exc_info=True,
            )

        # Lifecycle Phase 2: EVENT (Now after schema is ready AND record exists)
        await emit_event(
            CatalogEventType.CATALOG_CREATION,
            catalog_id=catalog_model.id,
            db_resource=conn,
        )

        # Lifecycle Phase 3: AFTER
        await emit_event(
            CatalogEventType.AFTER_CATALOG_CREATION,
            catalog_id=catalog_model.id,
            db_resource=conn,
        )

    async def create_catalog(
        self,
        catalog_data: Union[Dict[str, Any], Catalog],
        lang: str = "en",
        ctx: Optional["DriverContext"] = None,
    ) -> Catalog:
        """Create a new catalog."""
        db_resource = ctx.db_resource if ctx else None

        if isinstance(catalog_data, dict):
            from dynastore.models.localization import validate_language_consistency

            validate_language_consistency(catalog_data, lang)

        catalog_model = (
            Catalog.create_from_localized_input(catalog_data, lang)
            if isinstance(catalog_data, dict)
            else catalog_data
        )
        validate_sql_identifier(catalog_model.id)

        # Split public label from internal key.  The user-supplied ``id`` is
        # the renamable public label (external_id); a generated opaque key
        # becomes the immutable internal ``id`` (PK).  All downstream storage
        # (ES, GCS, IAM, asset, item tables) continues to key on ``id``
        # unchanged — this is the only place the split is made.
        # The final internal_id is assigned by _insert_catalog_row_with_pk_retry
        # (which handles PK-collision regeneration); set a placeholder now so
        # pre-INSERT lifecycle hooks that reference catalog_model.id get a valid
        # token, and update it after the INSERT returns the committed id.
        external_id = catalog_model.id
        internal_id = generate_physical_name("c")
        catalog_model.id = internal_id

        # #1175: provisioning readiness is driven by the provisioning checklist
        # built from the registered provisioners (see provisioning_registry),
        # not by a single provider. Start 'ready'; the checklist build below
        # (after the catalog.catalogs row exists) flips the catalog to
        # 'provisioning' when at least one provisioner is active for it. On-prem
        # with no active provisioner stays 'ready' immediately.
        catalog_model.provisioning_status = "ready"

        if _async_catalog_create_enabled():
            return await self._create_catalog_async(
                catalog_model, external_id, db_resource
            )
        return await self._create_catalog_sync(
            catalog_model, external_id, db_resource
        )

    async def _create_catalog_async(
        self,
        catalog_model: "Catalog",
        external_id: str,
        db_resource: Any,
    ) -> "Catalog":
        """Async create path (DYNASTORE_ASYNC_CATALOG_CREATE=true).

        Inserts the catalog.catalogs row, seeds the provisioning checklist with
        ``catalog_core: pending`` (plus any other active provisioners), then
        enqueues a ``catalog_core_init`` task that runs the full tenant
        provisioning steps.  Returns immediately with
        ``provisioning_status='provisioning'`` — the caller converts this to a
        202 response.

        The tenant schema does NOT exist when this method returns; all code
        that assumes the schema is ready must stay inside the task.
        """
        from dynastore.modules.catalog.provisioning_registry import (
            provisioning_registry,
            STATUS_PROVISIONING,
            STEP_PENDING,
        )
        from dynastore.modules.tasks.models import TaskCreate
        from dynastore.modules.tasks.tasks_module import create_task, get_task_schema

        catalog_model.provisioning_status = STATUS_PROVISIONING

        async with managed_transaction(get_catalog_engine(db_resource)) as conn:
            await emit_event(
                CatalogEventType.BEFORE_CATALOG_CREATION,
                catalog_id=catalog_model.id,
                db_resource=conn,
            )

            tombstoned_row = await DQLQuery(
                "SELECT id FROM catalog.catalogs WHERE external_id = :external_id AND deleted_at IS NOT NULL;",
                result_handler=ResultHandler.ONE_OR_NONE,
            ).execute(conn, external_id=external_id)
            if tombstoned_row is not None:
                old_internal_id = tombstoned_row[0] if tombstoned_row else None
                _reclaim_id = old_internal_id or catalog_model.id
                logger.info(
                    "[LIFECYCLE] Reclaiming soft-deleted catalog external_id='%s' "
                    "(internal_id='%s') for reuse",
                    external_id,
                    _reclaim_id,
                )
                await self._purge_catalog_storage(conn, _reclaim_id)
                _invalidate_catalog_external_id_cache(external_id)

            committed_internal_id = await _insert_catalog_row_with_pk_retry(
                conn,
                external_id=external_id,
                provisioning_status=catalog_model.provisioning_status,
            )
            catalog_model.id = committed_internal_id

            # Seed the provisioning checklist.  ``catalog_core`` is always
            # pending on this path (the task will mark it complete or failed).
            # Active provisioners (e.g. GCP bucket/eventing) that were already
            # registered are also included so their steps are barriers.
            checklist: Dict[str, str] = {"catalog_core": STEP_PENDING}
            extra = await provisioning_registry.build_checklist(
                catalog_model.id, conn
            )
            checklist.update(extra)

            await _set_provisioning_checklist_query.execute(
                conn,
                id=catalog_model.id,
                status=STATUS_PROVISIONING,
                checklist=json.dumps(checklist),
            )

            # Enqueue the durable task into the GLOBAL task schema.
            # The tenant schema does not exist yet; the task creates it.
            task_request = TaskCreate(
                task_type="catalog_core_init",
                inputs={"catalog_id": committed_internal_id, "external_id": external_id},
                caller_id="system",
                type="task",
            )
            await create_task(conn, task_request, get_task_schema())

            _invalidate_catalog_model_cache(catalog_model.id)
            _invalidate_catalog_external_id_cache(external_id)

        logger.info(
            "catalog '%s' (external='%s'): async create committed; "
            "catalog_core_init task enqueued",
            catalog_model.id, external_id,
        )
        catalog_model.external_id = external_id  # type: ignore[attr-defined]
        return catalog_model

    async def _create_catalog_sync(
        self,
        catalog_model: "Catalog",
        external_id: str,
        db_resource: Any,
    ) -> "Catalog":
        """Synchronous create path (default, DYNASTORE_ASYNC_CATALOG_CREATE=false).

        Runs all provisioning steps inline inside a single transaction and
        returns the fully-provisioned catalog model.  This is the pre-2329
        behaviour, unchanged.
        """
        physical_schema: str = ""  # set after INSERT below

        async with managed_transaction(get_catalog_engine(db_resource)) as conn:
            # Lifecycle Phase 1: BEFORE
            await emit_event(
                CatalogEventType.BEFORE_CATALOG_CREATION,
                catalog_id=catalog_model.id,
                db_resource=conn,
            )

            # Reclaim a soft-deleted (tombstoned) catalog id. A prior default
            # (soft) DELETE leaves the catalog.catalogs row with deleted_at set,
            # the physical schema intact, metadata sidecars in the router-
            # managed tables, and cron jobs still registered. Purge that residue
            # here so the id is reused as a clean, fresh catalog. A still-live
            # row (deleted_at IS NULL) is left untouched, so the INSERT below
            # raises the usual conflict.
            tombstoned_row = await DQLQuery(
                "SELECT id FROM catalog.catalogs WHERE external_id = :external_id AND deleted_at IS NOT NULL;",
                result_handler=ResultHandler.ONE_OR_NONE,
            ).execute(conn, external_id=external_id)
            if tombstoned_row is not None:
                # Reclaim: purge storage keyed on the OLD internal id of the
                # tombstoned row, then drop the row so our INSERT below succeeds.
                old_internal_id = tombstoned_row[0] if tombstoned_row else None
                _reclaim_id = old_internal_id or catalog_model.id
                logger.info(
                    "[LIFECYCLE] Reclaiming soft-deleted catalog external_id='%s' "
                    "(internal_id='%s') for reuse",
                    external_id,
                    _reclaim_id,
                )
                await self._purge_catalog_storage(conn, _reclaim_id)
                _invalidate_catalog_external_id_cache(external_id)

            # Insert the catalog.catalogs registry row first so the committed
            # internal id is known before any schema or DDL is created.
            # _insert_catalog_row_with_pk_retry regenerates the id on a PK
            # collision (astronomically rare) and retries up to 5 times.
            # A unique violation on external_id is a genuine user conflict
            # and bubbles immediately as UniqueViolationError → HTTP 409.
            # The schema name IS the committed internal id — we must not create
            # the schema with a placeholder that may differ from the committed id.
            committed_internal_id = await _insert_catalog_row_with_pk_retry(
                conn,
                external_id=external_id,
                provisioning_status=catalog_model.provisioning_status,
            )
            # Update the model to the committed internal id.
            catalog_model.id = committed_internal_id

            # The schema name IS the internal id (physical_schema column dropped).
            # Assigned here, after the INSERT, so schema creation uses the exact
            # id that was committed — never a placeholder that may have differed.
            physical_schema = committed_internal_id

            await self._run_core_init(conn, catalog_model, external_id, physical_schema)

            # Invalidate cache to ensure it's re-fetched in subsequent calls
            _invalidate_catalog_model_cache(catalog_model.id)
            _invalidate_catalog_external_id_cache(external_id)

        # Execute async external component initializers OUTSIDE transaction
        config_snapshot = {}
        try:
            from dynastore.tools.discovery import get_protocol
            from dynastore.models.protocols.configs import ConfigsProtocol

            config_mgr = get_protocol(ConfigsProtocol)
            if config_mgr:
                config_snapshot.update(
                    await config_mgr.list_catalog_configs(catalog_model.id)
                )
        except Exception as exc:
            logger.warning(
                "catalog %s: failed to load config snapshot for lifecycle init: %s",
                catalog_model.id, exc,
            )

        lifecycle_registry.init_async_catalog(
            catalog_model.id,
            LifecycleContext(
                physical_schema=physical_schema,
                config=config_snapshot
            )
        )

        # Invalidate caches BEFORE emitting signal to prevent visibility gap race conditions.
        # (The in-transaction invalidate above already covered the happy path; this second
        # call guards against readers between the transaction commit and the signal below.)
        _invalidate_catalog_model_cache(catalog_model.id)
        _invalidate_catalog_external_id_cache(external_id)

        # Emit signal to wake up background tasks (Visibility Gap fix)
        # This must happen OUTSIDE the transaction above so that background listeners
        # (like GCP provisioning) can see the committed 'catalog' row.
        await signal_bus.emit("AFTER_CATALOG_CREATION", identifier=catalog_model.id)

        # Re-fetch through ``get_catalog_model`` so the returned Catalog
        # carries metadata merged from the split tables — ``Catalog.model_validate``
        # of the raw ``catalog.catalogs`` row would yield ``title=None`` /
        # ``description=None`` etc. since those columns were dropped from the
        # registry in M2.5b and now live in ``catalog_core`` /
        # ``_stac`` (router-direct upsert above).
        merged = await self.get_catalog_model(
            catalog_model.id,
            ctx=DriverContext(db_resource=db_resource) if db_resource else None,
        )
        if merged is None:
            # Fallback: registry row missing despite the INSERT above is a
            # genuine consistency violation — fall back to the original
            # technical-row hydration so the caller still gets *some* model.
            result = await _get_catalog_query.execute(
                get_catalog_engine(db_resource), id=catalog_model.id
            )
            return Catalog.model_validate(result)
        return merged

    def _unpack_catalog_row(
        self,
        row: Any,
        *,
        router_metadata: Optional[Dict[str, Any]] = None,
    ) -> Optional[Catalog]:
        """Unpacks a database row into a Catalog model.

        The extra_metadata column stores only user-provided extra metadata
        (a localized JSONB dict), not an envelope with type/conformsTo/links.
        Those fields come from the Catalog model defaults.

        Router overlay
        --------------

        When ``router_metadata`` is supplied, its keys overwrite any
        corresponding columns on the row dict before model-validation.
        Catalog writes land in ``catalog.catalog_core`` /
        ``_stac`` via the router-direct upsert in ``create_catalog``;
        reads hydrate via the catalog-metadata router and pass the
        result here as ``router_metadata``.  Absence of router data
        (e.g. a router-less call path) yields a Catalog built from the
        technical registry row alone.
        """
        if not row:
            return None

        # Convert to dict
        data = dict(row._mapping) if hasattr(row, "_mapping") else dict(row)

        # #1175: the provisioning checklist is internal control-plane state
        # (read directly by ``mark_provisioning_step``); keep it out of the
        # Catalog model / API representation. ``provisioning_status`` remains the
        # public-facing field.
        data.pop("provisioning_checklist", None)

        # Unpack STAC dedicated columns if present
        if "conforms_to" in data and data["conforms_to"]:
            data["conformsTo"] = data["conforms_to"]

        # Ensure jsonb fields are loaded correctly if driver doesn't cast automatically
        for key in ["conformsTo", "links", "assets", "extra_metadata", "stac_extensions"]:
            dict_val = data.get(key)
            if isinstance(dict_val, str):
                try:
                    data[key] = json.loads(dict_val)
                except Exception:
                    data[key] = None

        # Router overlay.  Router-supplied keys overwrite any columns
        # left on the row dict — EXCEPT control-plane fields owned
        # exclusively by ``catalog.catalogs``.  Metadata-tier drivers
        # (e.g. CatalogElasticsearchDriver indexes a snapshot of the
        # full metadata payload) can carry stale copies of these fields
        # because freshness updates only mutate the row, not the
        # metadata sidecars.  Control-plane fields are authoritative on
        # the row; never let an overlay shadow them.
        if router_metadata:
            for key, value in router_metadata.items():
                if key in _CONTROL_PLANE_CATALOG_FIELDS:
                    continue
                data[key] = value
            if router_metadata.get("conforms_to"):
                # Special-case ``conforms_to`` which in the router envelope
                # carries its snake-case form, but Catalog consumes
                # ``conformsTo`` via Pydantic alias — fill both so either
                # spelling resolves after validation.
                data["conformsTo"] = router_metadata["conforms_to"]

        # ``physical_schema`` is resolved via the registry, never carried on the
        # public model. ``SELECT *`` includes the column and ``extra="allow"``
        # would otherwise re-attach (and serialize / leak) it — drop it here so
        # the model stays clean.
        data.pop("physical_schema", None)
        return Catalog.model_validate(data)

    def _list_catalog_store_driver_types(self) -> List[type]:
        """Return the ``CatalogStore`` classes currently registered.

        Used by :meth:`list_catalogs` to compute the expected per-row
        round-trip count for the threshold-warn log.  Kept as a
        lightweight helper (no I/O) so the hot path doesn't pay for
        anything beyond a single ``get_protocols`` lookup.

        Returns an empty list if the discovery layer throws — listing
        degradation-warn accuracy is not worth propagating an exception
        through the happy path.
        """
        try:
            from dynastore.models.protocols.entity_store import (
                CatalogStore,
            )
            from dynastore.tools.discovery import get_protocols

            return [type(d) for d in get_protocols(CatalogStore)]
        except Exception:  # noqa: BLE001 — diagnostic only
            return []

    async def _resolve_catalog_router_metadata(
        self,
        catalog_id: str,
        *,
        hints: FrozenSet = frozenset(),
        db_resource: Optional[Any] = None,
    ) -> Optional[Dict[str, Any]]:
        """Best-effort fetch of router-supplied catalog metadata.

        Degrades to ``None`` on any router error so the call site can
        fall back to the legacy SELECT's columns instead of 5xx'ing
        a catalog read.  The router itself already swallows per-driver
        exceptions (partial-envelope semantics), so this guard is
        belt-and-braces against a total-outage scenario where the
        router's driver resolution itself raises.

        Hints are threaded straight through.  An empty hint set keeps the
        existing merge-all behaviour (byte-identical default read).  A
        non-empty hint set lets a deployment whose routing config declares
        hinted READ drivers prefer one driver's view (first-non-None);
        see ``get_catalog_metadata``.
        """
        try:
            from dynastore.modules.catalog.catalog_router import (
                get_catalog_metadata,
            )

            return await get_catalog_metadata(
                catalog_id,
                hints=hints,
                db_resource=db_resource,
            )
        except Exception as exc:  # noqa: BLE001 — degrade to legacy SELECT
            logger.warning(
                "Catalog-metadata router failed for %s: %s — falling back "
                "to legacy catalog.catalogs columns",
                catalog_id, exc,
            )
            return None

    async def _get_catalog_model_db(self, catalog_id: str) -> Optional[Catalog]:
        """Get catalog model from database."""
        async with managed_transaction(self.engine) as conn:
            result = await _get_catalog_query.execute(conn, id=catalog_id)
        # Release the ``catalog.catalogs`` AccessShareLock before the router
        # fan-out.  ``_resolve_catalog_router_metadata`` reaches every
        # registered domain driver, some of which are network-bound (e.g.
        # Elasticsearch); holding this read transaction open across that I/O
        # leaves the backend ``idle in transaction`` and convoys a DDL
        # ``AccessExclusive`` waiter, which froze the platform (#1233/#1234).
        # The fan-out runs on its own connection (``db_resource=None``).
        router_metadata = await self._resolve_catalog_router_metadata(catalog_id)
        return self._unpack_catalog_row(
            result, router_metadata=router_metadata,
        )

    async def get_catalog_model(
        self,
        catalog_id: str,
        ctx: Optional["DriverContext"] = None,
        *,
        hints: FrozenSet = frozenset(),
    ) -> Optional[Catalog]:
        """Get catalog by ID, optionally hint-routed.

        Phase 3: resolves external→internal id at the model-read boundary so
        callers (stac_generator, admin endpoints) that pass HTTP path params
        (external ids) get back the correct model.  allow_missing=True so
        callers already holding internal ids fall through without a spurious
        miss; a genuinely missing catalog returns None via the downstream query.

        Cache behaviour (requirement B):
        - When ``hints`` is empty the result is served from the shared
          ``_catalog_model_cache`` (keyed by catalog_id).  The cache
          entry is populated via ``_get_catalog_model_db`` on the no-hint
          merge-all path, so the cached model is the full default envelope
          (byte-identical to the pre-hints baseline).
        - When ``hints`` is non-empty the cache is bypassed entirely so
          a geometry_simplified read cannot be served a cached
          default-shaped model and vice-versa.
        """
        _resolved = await self.resolve_catalog_id(catalog_id, allow_missing=True)
        if _resolved is not None:
            catalog_id = _resolved

        db_resource = ctx.db_resource if ctx else None
        if db_resource or hints:
            # Bypass cache for hinted reads to avoid cross-hint contamination.
            # The db_resource path also bypasses cache (pre-existing behaviour).
            if db_resource:
                async with managed_transaction(db_resource) as conn:
                    result = await _get_catalog_query.execute(conn, id=catalog_id)
                # Keep the router fan-out (network-bound driver I/O) out of the
                # catalog.catalogs read transaction so it can't be held
                # idle-in-transaction across that I/O (#1234); see
                # _get_catalog_model_db.  The fan-out reads on its own connection.
                router_metadata = await self._resolve_catalog_router_metadata(
                    catalog_id, hints=hints,
                )
            else:
                async with managed_transaction(self.engine) as conn:
                    result = await _get_catalog_query.execute(conn, id=catalog_id)
                router_metadata = await self._resolve_catalog_router_metadata(
                    catalog_id, hints=hints,
                )
            catalog = self._unpack_catalog_row(
                result, router_metadata=router_metadata,
            )
        else:
            catalog = await _catalog_model_cache(self, catalog_id)

        if catalog is None:
            return None

        return await self._run_catalog_pipeline(catalog_id, catalog)

    async def _run_catalog_pipeline(
        self, catalog_id: str, catalog: Catalog
    ) -> Optional[Catalog]:
        """Apply CatalogPipelineProtocol stages (optional, priority-ordered).

        Stages may augment, filter, or transform the catalog metadata dict.
        Stages returning ``None`` drop the catalog — the caller is
        responsible for rendering that as a 404 in the HTTP layer.

        An empty stage registry is safe: the input catalog passes through
        unchanged.
        """
        try:
            from dynastore.tools.discovery import get_protocols
            from dynastore.models.protocols.catalog_pipeline import CatalogPipelineProtocol

            stages = sorted(
                get_protocols(CatalogPipelineProtocol),
                key=lambda s: s.priority,
            )
            if not stages:
                return catalog

            data = catalog.model_dump(by_alias=True, exclude_none=True)
            for stage in stages:
                try:
                    if not stage.can_apply(catalog_id):
                        continue
                    result = await stage.apply(catalog_id, data, context={})
                except Exception as _stage_err:
                    logger.warning(
                        "CatalogPipeline stage '%s' failed for %s: %s",
                        getattr(stage, "pipeline_id", repr(stage)),
                        catalog_id,
                        _stage_err,
                    )
                    continue
                if result is None:
                    return None  # stage dropped the catalog
                data = result
            return Catalog.model_validate(data)
        except Exception:
            return catalog  # discovery failure must not break the read path

    async def update_catalog(
        self,
        catalog_id: str,
        updates: Union[Dict[str, Any], CatalogUpdate],
        lang: str = "en",
        ctx: Optional["DriverContext"] = None,
    ) -> Optional[Catalog]:
        """Update a catalog."""
        db_resource = ctx.db_resource if ctx else None
        validate_sql_identifier(catalog_id)
        # Phase 2: resolve external→internal id at the public boundary.
        # allow_missing=True so callers holding an already-internal id fall
        # through; genuinely missing catalogs are caught by get_catalog_model.
        _resolved = await self.resolve_catalog_id(catalog_id, allow_missing=True)
        if _resolved is not None:
            catalog_id = _resolved

        if isinstance(updates, dict):
            from dynastore.models.localization import validate_language_consistency

            validate_language_consistency(updates, lang)

        async with managed_transaction(get_catalog_engine(db_resource)) as conn:
            existing_model = await self.get_catalog_model(catalog_id, ctx=DriverContext(db_resource=conn))
            if not existing_model:
                raise ValueError(f"Catalog '{catalog_id}' not found.")

            # Merge updates into existing model
            merged_model = existing_model.merge_localized_updates(updates, lang)

            await emit_event(
                CatalogEventType.CATALOG_UPDATE, catalog_id=catalog_id, db_resource=conn
            )

            # Identify which fields the PATCH actually carried so the
            # router-fanned UPSERT only touches those columns.
            update_fields = set(
                updates.keys()
                if isinstance(updates, dict)
                else updates.model_dump(exclude_unset=True).keys()
            )

            # Writes go directly to the split tables via the catalog-
            # metadata router.  PATCH semantics via
            # ``_extract_update_payload`` + ``_filter_payload`` inside
            # each Primary driver ensure only the supplied columns are
            # touched — absent fields keep their existing values.
            updated_payload = _extract_update_payload(
                merged_model, update_fields,
            )
            if not updated_payload:
                # No column-level changes — still emit
                # AFTER_CATALOG_UPDATE for consumers that react to
                # mutation intent regardless of payload shape.
                await emit_event(
                    CatalogEventType.AFTER_CATALOG_UPDATE,
                    catalog_id=catalog_id,
                    db_resource=conn,
                )
                _invalidate_catalog_model_cache(catalog_id)
                return merged_model

            from dynastore.modules.catalog.catalog_router import (
                upsert_catalog_metadata,
            )
            # Router exceptions bubble — an UPDATE that fails to
            # persist is a caller-visible failure; the legacy
            # ``catalog.catalogs`` no longer backs the data so we
            # cannot fall back silently.
            await upsert_catalog_metadata(
                catalog_id, updated_payload, db_resource=conn,
            )

            await emit_event(
                CatalogEventType.AFTER_CATALOG_UPDATE,
                catalog_id=catalog_id,
                db_resource=conn,
            )

            # Re-read inside the same transaction so the returned model
            # reflects the freshly-written split-table data. The router
            # fan-out MUST share ``conn`` — when the outer caller (e.g. a
            # FastAPI request) owns the transaction it is not yet committed,
            # so a fan-out on a separate pool connection would observe only
            # the pre-write state and the handler would echo stale data.
            # ``update_collection`` solves the same hazard the same way
            # (see ``collection_service.update_collection`` re-fetch).
            in_tx_post = await _get_catalog_query.execute(conn, id=catalog_id)
            in_tx_router = await self._resolve_catalog_router_metadata(
                catalog_id, db_resource=conn,
            )
            fresh = self._unpack_catalog_row(
                in_tx_post, router_metadata=in_tx_router,
            )

        # Invalidate cache AFTER the transaction exits so concurrent readers
        # cannot repopulate it with pre-commit state.
        _invalidate_catalog_model_cache(catalog_id)

        if fresh is None:
            return merged_model
        return await self._run_catalog_pipeline(catalog_id, fresh)

    async def rename_catalog(
        self,
        internal_id: str,
        new_external_id: str,
        ctx: Optional["DriverContext"] = None,
    ) -> Tuple[str, str]:
        """Rename a catalog's public label (external_id) without touching any storage.

        The internal immutable ``id`` (PK) is unchanged. All downstream stores
        (ES, GCS, IAM, item/asset tables) are keyed on the internal id and
        require no update — this method issues exactly one SQL UPDATE row.

        Args:
            internal_id:     The immutable internal id of the catalog to rename.
            new_external_id: The desired new public label.
            ctx:             Optional driver context (db connection).

        Returns:
            ``(prev_external_id, new_external_id)`` tuple.

        Raises:
            CatalogRenameConflictError: if another live catalog already has
                ``external_id = new_external_id``.
            ValueError: if no live catalog row exists for ``internal_id``.
        """
        validate_sql_identifier(new_external_id)

        from dynastore.modules.db_config.exceptions import CatalogRenameConflictError

        db_resource = ctx.db_resource if ctx else None

        async with managed_transaction(get_catalog_engine(db_resource)) as conn:
            # Fetch current row to (a) confirm existence and (b) get the current external_id.
            current = await DQLQuery(
                "SELECT id, external_id FROM catalog.catalogs "
                "WHERE id = :id AND deleted_at IS NULL;",
                result_handler=ResultHandler.ONE_OR_NONE,
            ).execute(conn, id=internal_id)
            if current is None:
                raise ValueError(f"Catalog with internal id '{internal_id}' not found.")
            _row = dict(current._mapping) if hasattr(current, "_mapping") else dict(current)
            prev_external_id: str = _row["external_id"]

            if prev_external_id == new_external_id:
                # No-op: already has the requested label.
                return (prev_external_id, new_external_id)

            # Check no OTHER live catalog holds the new label.
            conflict = await DQLQuery(
                "SELECT id FROM catalog.catalogs "
                "WHERE external_id = :external_id AND deleted_at IS NULL AND id != :id;",
                result_handler=ResultHandler.ONE_OR_NONE,
            ).execute(conn, external_id=new_external_id, id=internal_id)
            if conflict is not None:
                raise CatalogRenameConflictError(new_external_id)

            await DQLQuery(
                "UPDATE catalog.catalogs "
                "SET external_id = :new_external_id, updated_at = NOW() "
                "WHERE id = :id AND deleted_at IS NULL;",
                result_handler=ResultHandler.ROWCOUNT,
            ).execute(conn, new_external_id=new_external_id, id=internal_id)

        # Invalidate both prev and new external_id cache entries, and the model cache.
        _invalidate_catalog_external_id_cache(prev_external_id)
        _invalidate_catalog_external_id_cache(new_external_id)
        _invalidate_catalog_model_cache(internal_id)

        logger.info(
            "[RENAME] Catalog internal_id=%r: external_id '%s' → '%s'",
            internal_id, prev_external_id, new_external_id,
        )
        return (prev_external_id, new_external_id)

    async def delete_catalog_language(
        self, catalog_id: str, lang: str, ctx: Optional["DriverContext"] = None
    ) -> bool:
        """Deletes a specific language variant from a catalog."""
        db_resource = ctx.db_resource if ctx else None
        validate_sql_identifier(catalog_id)
        # Phase 2: resolve external→internal id at the public boundary.
        # allow_missing=True so callers holding an already-internal id fall
        # through; genuinely missing catalogs are caught by get_catalog_model.
        _resolved = await self.resolve_catalog_id(catalog_id, allow_missing=True)
        if _resolved is not None:
            catalog_id = _resolved

        async with managed_transaction(get_catalog_engine(db_resource)) as conn:
            model = await self.get_catalog_model(catalog_id, ctx=DriverContext(db_resource=conn))
            if not model:
                raise ValueError(f"Catalog '{catalog_id}' not found.")

            # Check if language exists and if it's not the last one

            can_delete = False
            fields_to_update: Dict[str, str] = {}
            # Python-native parallel of ``fields_to_update`` used to
            # propagate the delete into the split tables via the router
            # post-M2.4 read flip.  See the propagation block below.
            router_fields_to_update: Dict[str, Any] = {}

            for field in [
                "title",
                "description",
                "keywords",
                "license",
                "extra_metadata",
            ]:
                val = getattr(model, field, None)
                if val:
                    langs = val.get_available_languages()
                    if lang in langs:
                        if len(langs) <= 1:
                            raise ValueError(
                                f"Cannot delete language '{lang}' from field '{field}': it is the only language available."
                            )

                        # Use merge_updates with None to simulate deletion for that language?
                        # Actually LocalizedDTO.merge_updates doesn't support deletion of a language easily via merge.
                        # We might need a 'delete_language' on LocalizedDTO or just do it here.

                        # Let's do it manually for now
                        data = val.model_dump(exclude_none=True)
                        if lang in data:
                            del data[lang]
                            fields_to_update[field] = json.dumps(
                                data, cls=CustomJSONEncoder
                            )
                            # Parallel Python-native dict for the M2.4
                            # split-table propagation below (the legacy
                            # fields_to_update holds JSON-string values
                            # ready for the UPDATE; the router path
                            # wants the un-serialised shape).
                            router_fields_to_update[field] = data
                            can_delete = True

            if not can_delete:
                return False

            # M2.5a — route the language removal through the catalog-
            # metadata router.  The legacy ``UPDATE catalog.catalogs``
            # is gone; the split-table rows are the only source post-
            # M2.4 overlay flip.  Router exceptions bubble: a failed
            # per-domain UPSERT means the caller's delete didn't
            # actually land and must know.
            if router_fields_to_update:
                from dynastore.modules.catalog.catalog_router import (
                    upsert_catalog_metadata,
                )
                await upsert_catalog_metadata(
                    catalog_id,
                    router_fields_to_update,
                    db_resource=conn,
                )

            _invalidate_catalog_model_cache(catalog_id)
            return True

    async def list_catalogs(
        self,
        limit: int = 100,
        offset: int = 0,
        lang: str = "en",
        ctx: Optional["DriverContext"] = None,
        q: Optional[str] = None,
        ids: Optional[Set[str]] = None,
    ) -> List[Catalog]:
        """List all catalogs.

        ``ids`` — restrict results to these catalog ids; applied before
        pagination so LIMIT/OFFSET reflect the filtered set.  ``None``
        means no restriction.

        Listing visibility: when the request published a caller snapshot
        (``RequestVisibility``), the listing is transparently narrowed to
        the catalogs that caller may see — intersected with any explicit
        ``ids`` restriction, and applied before pagination like ``ids``.
        Background/CLI work (no snapshot) lists unfiltered. This is the PG
        driver's translation of the neutral listing constraint (``id =
        ANY`` ahead of LIMIT/OFFSET); other drivers translate it to their
        own predicate language.
        """
        from dynastore.models.protocols.visibility import (
            resolve_catalog_listing_ids,
        )

        visible_ids = await resolve_catalog_listing_ids()
        if visible_ids is not None:
            ids = (
                set(visible_ids)
                if ids is None
                else {i for i in ids if i in visible_ids}
            )
            if not ids:
                return []

        db_resource = ctx.db_resource if ctx else None
        async with managed_transaction(get_catalog_engine(db_resource)) as conn:
            if not q:
                if ids is not None:
                    sql = (
                        "SELECT * FROM catalog.catalogs "
                        "WHERE deleted_at IS NULL AND id = ANY(:ids) "
                        "ORDER BY id LIMIT :limit OFFSET :offset;"
                    )
                    query = DQLQuery(sql, result_handler=ResultHandler.ALL_DICTS)
                    results = await query.execute(
                        conn, limit=limit, offset=offset, ids=list(ids)
                    )
                else:
                    results = await _list_catalogs_query.execute(
                        conn, limit=limit, offset=offset
                    )
            else:
                # M2.5b — the legacy ``title`` / ``description`` columns
                # are gone from ``catalog.catalogs``.  Search now joins
                # through ``catalog.catalog_core`` (the only
                # place those fields live post-M2.5) and applies the
                # same ILIKE pattern to the JSONB ``en`` field.  Left
                # join so catalogs with no metadata row still match on
                # ``id ILIKE``.
                ids_clause = " AND c.id = ANY(:ids)" if ids is not None else ""
                sql = (
                    "SELECT c.* FROM catalog.catalogs c "
                    "LEFT JOIN catalog.catalog_core m "
                    "  ON m.catalog_id = c.id "
                    f"WHERE c.deleted_at IS NULL{ids_clause} AND ("
                    "  c.id ILIKE :q "
                    "  OR m.title->>'en' ILIKE :q "
                    "  OR m.description->>'en' ILIKE :q"
                    ") ORDER BY c.id LIMIT :limit OFFSET :offset;"
                )
                query = DQLQuery(sql, result_handler=ResultHandler.ALL_DICTS)
                params: Dict[str, Any] = dict(limit=limit, offset=offset, q=f"%{q}%")
                if ids is not None:
                    params["ids"] = list(ids)
                results = await query.execute(conn, **params)
                
        # M2.4 — overlay router-supplied metadata per-row.  Each row carries
        # a catalog_id we look up through the router; the merged envelope wins
        # over the legacy catalog.catalogs columns.
        #
        # The fan-out runs OUTSIDE the read transaction above: holding the
        # ``catalog.catalogs`` AccessShareLock across N×M driver round-trips
        # (some network-bound) is the list-path twin of the single-row
        # idle-in-transaction leak fixed in #1234.  Each per-row fan-out reads
        # on its own connection (``db_resource=None``).
        #
        # The loop stays sequential for now (it was previously forced
        # sequential by sharing one connection; with per-driver connections a
        # future ``asyncio.gather`` is possible).  For a page of N catalogs ×
        # M domain drivers that is N×M round-trips; warn when the product gets
        # large enough that operators will notice the latency.
        router_drivers_count = len(self._list_catalog_store_driver_types())
        expected_roundtrips = len(results) * max(router_drivers_count, 1)
        if expected_roundtrips >= _LIST_CATALOGS_ROUNDTRIP_WARN_THRESHOLD:
            logger.warning(
                "list_catalogs will issue ~%d sequential SQL "
                "round-trips (%d rows × %d domain drivers). "
                "Consider reducing ``limit`` or adopting the "
                "batched JOIN query planned for M3+.",
                expected_roundtrips, len(results), router_drivers_count,
            )

        models: List[Catalog] = []
        for r in results:
            row_id = (
                r._mapping["id"] if hasattr(r, "_mapping") else r["id"]
            ) if r else None
            router_metadata = (
                await self._resolve_catalog_router_metadata(row_id)
                if row_id is not None
                else None
            )
            model = self._unpack_catalog_row(
                r, router_metadata=router_metadata,
            )
            if model is not None:
                models.append(model)
        return models

    async def search_catalogs(
        self,
        filters: Optional[Dict[str, Any]] = None,
        limit: int = 100,
        offset: int = 0,
        db_resource: Optional[DbResource] = None,
    ) -> List[Catalog]:
        """Search catalogs with filters."""
        # TODO: implement this feature reusing ogc filters, reemove delegate to list_catalogs
        return await self.list_catalogs(
            limit=limit, offset=offset, ctx=DriverContext(db_resource=db_resource) if db_resource else None
        )

    # --- Config Operations (delegated to ConfigsProtocol via aggregation if needed, or keeping legacy) ---
    # Actually, the protocol says CatalogsProtocol has get_catalog_config and get_collection_config

    async def get_catalog_config(
        self, catalog_id: str, ctx: Optional["DriverContext"] = None
    ):
        db_resource = ctx.db_resource if ctx else None
        from dynastore.models.protocols.configs import ConfigsProtocol

        configs = get_protocol(ConfigsProtocol)
        from dynastore.modules.catalog.catalog_config import CollectionPluginConfig

        return await configs.get_config(  # type: ignore[union-attr]
            CollectionPluginConfig, catalog_id, ctx=DriverContext(db_resource=db_resource
        ))

    async def get_collection_config(
        self,
        catalog_id: str,
        collection_id: str,
        ctx: Optional["DriverContext"] = None,
    ):
        db_resource = ctx.db_resource if ctx else None
        from dynastore.modules.storage.router import get_driver
        from dynastore.modules.storage.routing_config import Operation

        driver = await get_driver(Operation.READ, catalog_id, collection_id)
        return await driver.get_driver_config(
            catalog_id, collection_id, db_resource=db_resource
        )

    async def _purge_catalog_storage(
        self,
        conn: DbResource,
        catalog_id: str,
    ) -> Optional[str]:
        """Tear down a catalog's physical + metadata footprint within ``conn``.

        Shared by hard delete (``force=True``) and ``create_catalog``'s
        tombstone reset: resolves the physical schema from the registry row
        (skipping the ``deleted_at IS NULL`` filter so it works on tombstoned
        rows too), drops the physical schema CASCADE, removes cron jobs, and
        hard-deletes the ``catalog.catalogs`` registry row. The registry-row
        deletion cascades to ``catalog_core`` and ``catalog_stac`` via the
        ``ON DELETE CASCADE`` FK so no explicit metadata fan-out is needed.

        The caller owns any async external-resource destroy (e.g.
        ``lifecycle_registry.destroy_async_catalog``). Returns the old
        physical schema name (``None`` if the catalog had no schema recorded).

        Fail-closed: any exception from ``snapshot_and_enqueue`` propagates
        to the caller, rolling back the ``managed_transaction`` and aborting
        the schema drop.  This ensures external resources are never orphaned
        silently — the operator must fix the underlying issue and retry.
        """
        from dynastore.modules.catalog.cascade_runtime import CascadeOrchestrator
        from dynastore.modules.catalog.resource_owner import CleanupMode, ResourceScope, ScopeRef

        # Snapshot CleanupRefs BEFORE the schema drop while DB rows are still
        # readable.  The enqueue itself happens after the caller's transaction
        # commits (create_task opens its own tx — see cascade_runtime docstring).
        orchestrator: CascadeOrchestrator = (
            self._cascade_orchestrator
            if self._cascade_orchestrator is not None
            else CascadeOrchestrator()
        )
        scope_ref = ScopeRef(scope=ResourceScope.CATALOG, catalog_id=catalog_id)
        cascade_task_id = await orchestrator.snapshot_and_enqueue(
            conn, scope_ref, CleanupMode.HARD
        )

        # Resolve physical schema (== id) without deleted_at filter — works for
        # both live and tombstoned rows.
        old_physical_schema = await DQLQuery(
            "SELECT id FROM catalog.catalogs WHERE id = :catalog_id;",
            result_handler=ResultHandler.SCALAR_ONE_OR_NONE,
        ).execute(conn, catalog_id=catalog_id)

        if old_physical_schema:
            # DROP SCHEMA CASCADE takes AccessExclusiveLock and, under concurrent
            # catalog deletes, contends on shared system-catalog rows (pg_depend)
            # — a peer drop can hold a row lock past the 5s lock_timeout, raising
            # LockNotAvailableError (55P03) and bubbling a raw 500 that leaks the
            # catalog. safe_drop_relation runs the drop under a bounded
            # lock_timeout inside a savepoint and retries on 55P03/40P01, so a
            # transient lock wait self-heals instead of failing the request.
            from dynastore.modules.db_config.locking_tools import safe_drop_relation

            await safe_drop_relation(
                conn,
                schema=old_physical_schema,
                relation=old_physical_schema,  # unused for kind="schema"
                kind="schema",
                cascade=True,
                max_retries=5,
            )
            try:
                async with managed_nested_transaction(conn) as nested:
                    deleted_jobs = await _delete_tenant_cron_jobs_query.execute(
                        nested, pattern=f"%{old_physical_schema}%"
                    )
                if deleted_jobs:
                    logger.info(
                        "Removed %d cron job(s) for schema %s",
                        deleted_jobs, old_physical_schema,
                    )
            except Exception as cron_err:
                logger.warning(
                    "Could not remove cron jobs for %s (non-fatal): %s",
                    old_physical_schema, cron_err,
                )

        # Deleting the registry row cascades to catalog_core and catalog_stac
        # via their ON DELETE CASCADE FK, so no explicit metadata fan-out is
        # needed here.
        await _hard_delete_catalog_query.execute(conn, id=catalog_id)

        if cascade_task_id is not None:
            logger.info(
                "Enqueued cascade cleanup task %s for catalog %r.",
                cascade_task_id, catalog_id,
            )

        return old_physical_schema

    async def delete_catalog(
        self,
        catalog_id: str,
        force: bool = False,
        ctx: Optional["DriverContext"] = None,
    ) -> bool:
        """
        Delete a catalog.

        If force=True, triggers a hard deletion (removal of schema and data).
        Otherwise, performs a soft delete (marks as deleted without touching
        the physical schema, metadata sidecars, or catalog_configs so the id
        can later be hard-deleted or reclaimed by create_catalog).
        """
        db_resource = ctx.db_resource if ctx else None
        validate_sql_identifier(catalog_id)
        # Phase 2: resolve external→internal id at the public boundary.  A soft
        # delete of a nonexistent external_id returns False (same semantics as a
        # 0-row UPDATE); for that use allow_missing=True and check for None.
        internal_id = await self.resolve_catalog_id(catalog_id, allow_missing=True)
        if internal_id is None:
            return False
        catalog_id = internal_id

        config_snapshot: Dict[str, Any] = {}
        physical_schema: Optional[str] = None
        async with managed_transaction(get_catalog_engine(db_resource)) as conn:
            if db_resource is None:
                # Relax idle_in_transaction_session_timeout for THIS delete
                # transaction only (SET LOCAL auto-reverts on commit/rollback).
                #
                # _purge_catalog_storage calls snapshot_and_enqueue inside this
                # transaction; its RoutingDrivenCascadeOwner.describe_scope reads
                # routing config via ConfigsProtocol on a *second* pooled
                # connection (by design — it must observe live, pre-drop config),
                # leaving this transaction's connection idle. Under a cold config
                # cache or pool contention that idle gap exceeds the 30s default,
                # PostgreSQL terminates the backend, and the next statement fails
                # with "the underlying connection is closed", leaving the catalog
                # stuck mid-delete. Same mechanism and fix as the collection
                # hard-delete path in CollectionService.delete_collection.
                #
                # Direct conn.execute (NOT DDLQuery): DDLQuery wraps the statement
                # in a SAVEPOINT when the connection is already in a transaction,
                # and a SET LOCAL scoped to that savepoint would not survive its
                # release. The transaction stays bounded by lock_timeout and
                # per-statement command_timeout and is driven by the background
                # task runner, so disabling the idle reaper here is safe.
                from typing import cast

                from sqlalchemy import text
                from sqlalchemy.ext.asyncio import AsyncConnection

                await cast(AsyncConnection, conn).execute(
                    text("SET LOCAL idle_in_transaction_session_timeout = '0'")
                )
            if force:
                # Resolve the physical schema before purge (purge will delete
                # the row so we capture it here for the post-txn async hook).
                physical_schema = await DQLQuery(
                    "SELECT id FROM catalog.catalogs WHERE id = :catalog_id;",
                    result_handler=ResultHandler.SCALAR_ONE_OR_NONE,
                ).execute(conn, catalog_id=catalog_id)
                if not physical_schema:
                    # Catalog not found at all — nothing to delete.
                    return False

                # Snapshot the config BEFORE the purge removes it — the async
                # external-resource destroy scheduled after the txn needs it.
                from dynastore.models.protocols import ConfigsProtocol
                config_manager = get_protocol(ConfigsProtocol)
                if config_manager:
                    try:
                        config_snapshot = await config_manager.list_catalog_configs(
                            catalog_id, ctx=DriverContext(db_resource=conn)
                        )
                    except Exception as e:
                        logger.debug(
                            "Could not list catalog configs before deletion: %s", e
                        )

                # 2. Hard Delete (Force)
                # Lifecycle: BEFORE -> HARD_DELETE internal -> AFTER
                await emit_event(
                    CatalogEventType.BEFORE_CATALOG_HARD_DELETION,
                    catalog_id=catalog_id,
                    db_resource=conn,
                )

                logger.info(
                    "[LIFECYCLE] Hard deleting catalog '%s'", catalog_id
                )
                await self._purge_catalog_storage(conn, catalog_id)
                logger.info(
                    "[LIFECYCLE] Hard deleted catalog '%s' successfully", catalog_id
                )

            else:
                # Soft delete: tombstone the registry row only. The physical
                # schema, metadata sidecars and catalog_configs are intentionally
                # retained so the id can later be either hard-deleted or
                # reclaimed by create_catalog — both of which purge the residue
                # via _purge_catalog_storage for a clean reset. Retained
                # configs are inert while the row is tombstoned (every read
                # filters deleted_at IS NULL).
                rows = await _soft_delete_catalog_query.execute(conn, id=catalog_id)
                if rows == 0:
                    return False

                await emit_event(
                    CatalogEventType.CATALOG_DELETION,
                    catalog_id=catalog_id,
                    db_resource=conn,
                )
                await emit_event(
                    CatalogEventType.CATALOG_METADATA_CHANGED,
                    catalog_id=catalog_id,
                    db_resource=conn,
                    payload={
                        "catalog_id": catalog_id,
                        "operation": "soft_delete",
                    },
                )
                _invalidate_catalog_model_cache(catalog_id)
                return True

            # Reached only on the force=True path.

            # Emit main HARD_DELETION event (triggers async destroyers)
            await emit_event(
                CatalogEventType.CATALOG_HARD_DELETION,
                catalog_id=catalog_id,
                db_resource=conn,
                physical_schema=physical_schema,
            )

            # Fire the canonical secondary-index cleanup signal.
            await emit_event(
                CatalogEventType.CATALOG_METADATA_CHANGED,
                catalog_id=catalog_id,
                db_resource=conn,
                payload={
                    "catalog_id": catalog_id,
                    "operation": "delete",
                },
            )

            # Emit AFTER event
            await emit_event(
                CatalogEventType.AFTER_CATALOG_HARD_DELETION,
                catalog_id=catalog_id,
                db_resource=conn,
                physical_schema=physical_schema,
            )

        # Post-transaction cleanup
        _invalidate_catalog_model_cache(catalog_id)

        if physical_schema:
            try:
                from dynastore.modules.catalog.lifecycle_manager import LifecycleContext

                lifecycle_registry.destroy_async_catalog(
                    catalog_id,
                    LifecycleContext(physical_schema=physical_schema, config=config_snapshot),
                )
            except Exception as e:
                logger.warning(
                    "Failed to trigger async destroy for catalog %s: %s",
                    catalog_id, e,
                )

        return True

    async def list_collections(
        self,
        catalog_id: str,
        limit: int = 10,
        offset: int = 0,
        lang: str = "en",
        ctx: Optional["DriverContext"] = None,
        q: Optional[str] = None,
        *,
        hints: FrozenSet = frozenset(),
    ):
        return await self._col_svc.list_collections(
            catalog_id, limit=limit, offset=offset, lang=lang, ctx=ctx, q=q, hints=hints,
        )

    async def get_collection_model(
        self,
        catalog_id: str,
        collection_id: str,
        db_resource: Optional[DbResource] = None,
        *,
        hints: FrozenSet = frozenset(),
    ) -> Optional[Collection]:
        # Phase 3: resolve external→internal ids at the output boundary so
        # callers (stac_generator, features_service) that pass HTTP path params
        # (external ids) get back the correct model.  allow_missing=True so
        # callers already holding internal ids fall through without a spurious
        # miss; genuinely absent catalogs/collections return None via the
        # downstream DB query.
        _cat_internal = await self.resolve_catalog_id(catalog_id, allow_missing=True)
        if _cat_internal is not None:
            catalog_id = _cat_internal
        _col_internal = await self._col_svc.resolve_collection_id(
            catalog_id, collection_id, allow_missing=True
        )
        if _col_internal is not None:
            collection_id = _col_internal

        return await self._col_svc.get_collection_model(
            catalog_id, collection_id, db_resource=db_resource, hints=hints,
        )

    async def get_collection(
        self,
        catalog_id: str,
        collection_id: str,
        lang: str = "en",
        ctx: Optional["DriverContext"] = None,
        *,
        hints: FrozenSet = frozenset(),
    ) -> Optional[Collection]:
        # Phase 2: resolve external→internal ids at the public boundary.
        # allow_missing=True so callers holding already-internal ids fall through;
        # genuinely missing catalogs/collections are caught by get_collection_model.
        _resolved_cat = await self.resolve_catalog_id(catalog_id, allow_missing=True)
        if _resolved_cat is not None:
            catalog_id = _resolved_cat
        _resolved_col = await self._col_svc.resolve_collection_id(
            catalog_id, collection_id, allow_missing=True
        )
        if _resolved_col is not None:
            collection_id = _resolved_col

        # Enforce the direct-get visibility contract (#2050): a collection the
        # caller has no visibility grant for is indistinguishable from a
        # missing one.  resolve_collection_listing_ids() returns None when
        # IAM is not active — preserve prior behaviour in that case.  When a
        # non-None frozenset is returned and collection_id is absent from it,
        # return None so the HTTP layer renders 404 (same as a genuine miss).
        from dynastore.models.protocols.visibility import resolve_collection_listing_ids

        visible_ids = await resolve_collection_listing_ids(catalog_id)
        if visible_ids is not None and collection_id not in visible_ids:
            return None

        db_resource = ctx.db_resource if ctx else None
        return await self._col_svc.get_collection_model(
            catalog_id, collection_id, db_resource=db_resource, hints=hints,
        )

    async def get_collection_column_names(
        self,
        catalog_id: str,
        collection_id: str,
        ctx: Optional["DriverContext"] = None,
    ) -> Set[str]:
        return await self._col_svc.get_collection_column_names(
            catalog_id, collection_id, ctx=ctx
        )

    async def create_collection(
        self,
        catalog_id: str,
        collection_definition: Union[Dict[str, Any], Collection],
        lang: str = "en",
        ctx: Optional["DriverContext"] = None,
        **kwargs,
    ) -> Collection:
        return await self._col_svc.create_collection(
            catalog_id,
            collection_definition,
            lang=lang,
            ctx=ctx,
            **kwargs,
        )

    async def update_collection(
        self,
        catalog_id: str,
        collection_id: str,
        updates: Dict[str, Any],
        lang: str = "en",
        ctx: Optional["DriverContext"] = None,
    ) -> Optional[Collection]:
        return await self._col_svc.update_collection(
            catalog_id, collection_id, updates, lang=lang, ctx=ctx
        )

    async def rename_collection(
        self,
        catalog_internal_id: str,
        collection_internal_id: str,
        new_external_id: str,
        ctx: Optional["DriverContext"] = None,
    ) -> Tuple[str, str]:
        """Rename a collection's public label (external_id) within a catalog.

        Delegates to :meth:`CollectionService.rename_collection`. The internal
        ids (catalog_internal_id, collection_internal_id) must already be
        resolved; the caller is responsible for resolving external→internal
        before invoking this method.

        Returns ``(prev_external_id, new_external_id)``.
        """
        return await self._col_svc.rename_collection(
            catalog_internal_id, collection_internal_id, new_external_id, ctx=ctx
        )

    async def delete_collection(
        self,
        catalog_id: str,
        collection_id: str,
        force: bool = False,
        ctx: Optional["DriverContext"] = None,
    ) -> bool:
        return await self._col_svc.delete_collection(
            catalog_id, collection_id, force=force, ctx=ctx
        )

    async def delete_collection_language(
        self,
        catalog_id: str,
        collection_id: str,
        lang: str,
        ctx: Optional["DriverContext"] = None,
    ) -> bool:
        return await self._col_svc.delete_collection_language(
            catalog_id, collection_id, lang, ctx=ctx
        )

    async def create_physical_collection(
        self,
        conn,
        schema: str,
        catalog_id: str,
        collection_id: str,
        physical_table: Optional[str] = None,
        layer_config=None,
        **kwargs,
    ):
        from dynastore.modules.storage.router import get_driver

        try:
            driver = await get_driver("WRITE", catalog_id, collection_id)
        except ValueError:
            # No write driver registered for this collection.  Re-raise so
            # the caller's transaction rolls back and any prior
            # ``set_physical_table`` pin is not committed without the table
            # actually existing (atomicity guard, #1847).
            logger.error(
                "create_physical_collection: no WRITE driver for %s/%s — "
                "cannot create physical table %r; aborting provisioning.",
                catalog_id, collection_id, physical_table,
            )
            raise
        await driver.ensure_storage(
            catalog_id,
            collection_id,
            physical_table=physical_table,
            layer_config=layer_config,
            db_resource=conn,
        )

    # --- Item Operations (delegated) ---

    async def upsert(
        self,
        catalog_id: str,
        collection_id: str,
        items: Union[Dict[str, Any], List[Dict[str, Any]], Any],
        ctx: Optional[DriverContext] = None,
        processing_context: Optional[Dict[str, Any]] = None,
    ) -> Union[Dict[str, Any], List[Dict[str, Any]], Any]:
        """Create or update items (single or bulk) via ItemService."""
        return await self._item_svc.upsert(
            catalog_id,
            collection_id,
            items,
            ctx=ctx,
            processing_context=processing_context,
        )

    async def get_item(
        self,
        catalog_id: str,
        collection_id: str,
        item_id: Any,
        ctx: Optional[DriverContext] = None,
        lang: str = "en",
        context: Optional[Any] = None,
        access_filter: Optional[Any] = None,
    ):
        return await self._item_svc.get_item(
            catalog_id, collection_id, item_id,
            ctx=ctx, lang=lang, context=context, access_filter=access_filter,
        )

    async def delete_item(
        self,
        catalog_id: str,
        collection_id: str,
        item_id: str,
        ctx: Optional[DriverContext] = None,
        caller_id: Optional[str] = None,
    ) -> int:
        # Resolves ID internally in ItemService
        return await self._item_svc.delete_item(
            catalog_id, collection_id, item_id, ctx=ctx, caller_id=caller_id
        )

    async def delete_item_language(
        self,
        catalog_id: str,
        collection_id: str,
        item_id: str,
        lang: str,
        ctx: Optional[DriverContext] = None,
    ) -> int:
        return await self._item_svc.delete_item_language(
            catalog_id, collection_id, item_id, lang, ctx=ctx
        )

    async def resolve_external_id_by_geoid(
        self,
        catalog_id: str,
        collection_id: str,
        geoid: str,
        ctx: Optional[DriverContext] = None,
    ) -> Optional[str]:
        return await self._item_svc.resolve_external_id_by_geoid(
            catalog_id, collection_id, geoid, ctx=ctx
        )

    @property
    def count_items_by_asset_id_query(self) -> Any:
        return self._item_svc.count_items_by_asset_id_query

    def map_row_to_feature(
        self,
        row: Any,
        col_config: CollectionPluginConfig,
        lang: str = "en",
        read_policy: Optional[Any] = None,
    ) -> Feature:
        return self._item_svc.map_row_to_feature(  # type: ignore[return-value]
            row, col_config, lang=lang, read_policy=read_policy
        )

    async def get_collection_schema(
        self,
        catalog_id: str,
        collection_id: str,
        db_resource: Optional[Any] = None,
    ) -> Dict[str, Any]:
        return await self._item_svc.get_collection_schema(
            catalog_id, collection_id, db_resource=db_resource
        )

    async def search(
        self,
        catalog_id: str,
        collection_id: str,
        filter_cql: Optional[str] = None,
        properties: Optional[List[str]] = None,
        include_geometry: bool = True,
        limit: int = 10,
        offset: int = 0,
        db_resource: Optional[DbResource] = None,
    ) -> Dict[str, Any]:
        """
        High-level search helper that returns a FeatureCollection structure.
        Uses raw_where for CQL support for now.
        """
        from dynastore.models.query_builder import (
            QueryRequest,
            FieldSelection,
        )

        # 1. Build QueryRequest
        selects = []

        # Geometry
        if include_geometry:
            selects.append(FieldSelection(field="geom"))

        # Properties
        if properties:
            for p in properties:
                selects.append(FieldSelection(field=p))
        else:
            if properties is None:
                selects.append(FieldSelection(field="*"))

        # Build Request
        raw_where = filter_cql

        request = QueryRequest(
            select=selects, limit=limit, offset=offset, raw_where=raw_where
        )

        items = await self.search_items(
            catalog_id, collection_id, request
        )

        return {"type": "FeatureCollection", "features": items}

    async def get_features_query(
        self,
        conn: Any,
        catalog_id: str,
        collection_id: str,
        col_config: Any,
        params: Dict[str, Any],
        param_suffix: str = "",
        access_filter: Optional[Any] = None,
    ) -> Tuple[str, Dict[str, Any]]:
        return await self._item_svc.get_features_query(
            conn, catalog_id, collection_id, col_config, params, param_suffix,
            access_filter=access_filter,
        )

    async def search_items(
        self,
        catalog_id: str,
        collection_id: str,
        request: QueryRequest,
        config: Optional[ConfigsProtocol] = None,
        ctx: Optional[DriverContext] = None,
        consumer: "Optional[ConsumerType]" = None,
    ) -> List[Dict[str, Any]]:
        """Search and retrieve items using optimized query generation."""
        from dynastore.modules.storage.drivers.pg_sidecars.base import ConsumerType as _CT
        return await self._item_svc.search_items(  # type: ignore[return-value]
            catalog_id, collection_id, request, config=config, ctx=ctx,
            consumer=consumer or _CT.GENERIC,
        )

    async def stream_items(
        self,
        catalog_id: str,
        collection_id: str,
        request: QueryRequest,
        config: Optional[ConfigsProtocol] = None,
        ctx: Optional[DriverContext] = None,
        consumer: "Optional[ConsumerType]" = None,
        hints: "FrozenSet[Hint]" = frozenset(),
    ) -> QueryResponse:
        """Stream search results using an async iterator."""
        from dynastore.modules.storage.drivers.pg_sidecars.base import ConsumerType as _CT
        return await self._item_svc.stream_items(
            catalog_id, collection_id, request,
            config=config, ctx=ctx,
            consumer=consumer or _CT.GENERIC, hints=hints,
        )

    async def get_collection_fields(
        self,
        catalog_id: str,
        collection_id: str,
        db_resource: Optional[DbResource] = None,
    ) -> Dict[str, Any]:
        """
        Retrieves field definitions for a physical table.
        Used by WFS to map SQL types without full reflection.
        Delegates to ItemService.
        """
        return await self._item_svc.get_collection_fields(
            catalog_id,
            collection_id,
            db_resource=db_resource,
        )

    async def get_categorized_fields(
        self,
        catalog_id: str,
        collection_id: str,
        db_resource: Optional[Any] = None,
    ) -> Tuple[FrozenSet[str], FrozenSet[str], FrozenSet[str]]:
        """Return ``(system, stats, properties)`` field-name sets. Delegates to ItemService."""
        return await self._item_svc.get_categorized_fields(
            catalog_id,
            collection_id,
            db_resource=db_resource,
        )

    async def update_provisioning_status(
        self, catalog_id: str, status: str, ctx: Optional["DriverContext"] = None
    ) -> bool:
        """Updates the provisioning status (provisioning | ready | failed) for a catalog.

        After committing the source-of-truth row in ``catalog.catalogs``, fans
        the change out across every registered ``CatalogStore`` driver
        via ``catalog_router.upsert_catalog_metadata``. Without that
        propagation, search backends (ES indexer) keep the stale
        ``provisioning_status`` value and reads return inconsistent state
        relative to the row (observed on review env 2026-04-30: PG flipped to
        'ready' but ES still showed 'provisioning'). Mirrors the create-time
        fan-out at create_catalog (above).

        The metadata fan-out (which includes non-PG drivers such as ES) runs
        OUTSIDE the PG transaction so that slow non-PG I/O (e.g. ES
        refresh=wait_for) never holds a BEGIN open long enough to trigger
        idle_in_transaction_session_timeout (#1895).
        """
        db_resource = ctx.db_resource if ctx else None
        engine = get_catalog_engine(db_resource)

        # Phase 1 — short PG transaction: write the authoritative row and
        # return immediately.  The transaction is committed before any
        # non-PG fan-out driver is called.
        sql = "UPDATE catalog.catalogs SET provisioning_status = :status WHERE id = :id RETURNING id;"

        async def _do_update(conn: Any) -> Any:
            return await DQLQuery(sql, result_handler=ResultHandler.ONE_DICT).execute(
                conn, id=catalog_id, status=status
            )

        result = await _provisioning_write_with_retry(engine, _do_update)
        if not result:
            return False
        _invalidate_catalog_model_cache(catalog_id)

        # Phase 2 — OUTSIDE the transaction: fan out to metadata drivers.
        # The PG row is committed; a fresh read sees the new status, removing
        # the read-after-write race while avoiding a held connection across
        # potentially slow non-PG I/O.
        catalog_model = await self.get_catalog_model(catalog_id)
        if catalog_model is not None:
            metadata = _build_catalog_metadata_payload(catalog_model)
            if metadata:
                from dynastore.modules.catalog.catalog_router import (
                    upsert_catalog_metadata,
                )
                await upsert_catalog_metadata(catalog_id, metadata)
        return True

    async def mark_provisioning_step(
        self,
        catalog_id: str,
        key: str,
        step_status: str = "complete",
        ctx: Optional["DriverContext"] = None,
    ) -> bool:
        """Mark one provisioning-checklist step terminal and re-evaluate readiness (#1175).

        Sets ``provisioning_checklist[key] = step_status`` and, when the whole
        checklist is terminal, flips ``provisioning_status`` — ``ready`` when
        every step is ``complete``/``skipped`` (the terminal "default last"
        step), or ``failed`` when any step is ``failed``. A catalog with no
        checklist (legacy / on-prem, created already ``ready``) is a no-op
        returning ``False``.

        The row is ``SELECT … FOR UPDATE`` so concurrent provisioner completions
        on the same catalog serialise instead of racing on the JSONB blob. A
        status change fans out to the metadata drivers, mirroring
        :meth:`update_provisioning_status`.

        The metadata fan-out runs OUTSIDE the PG transaction for the same
        idle_in_transaction_session_timeout safety as
        :meth:`update_provisioning_status` (#1895).
        """
        from dynastore.modules.catalog.provisioning_registry import (
            evaluate_checklist,
        )

        db_resource = ctx.db_resource if ctx else None
        engine = get_catalog_engine(db_resource)

        # Phase 1 — short PG transaction: update checklist + status.
        # Returns the new_status (str | None) or a sentinel for early-exit.
        _NOT_FOUND = object()
        _NO_CHECKLIST = object()

        async def _do_checklist_update(conn: Any) -> Any:
            row = await _get_provisioning_checklist_query.execute(conn, id=catalog_id)
            if not row:
                return _NOT_FOUND
            raw = row.get("provisioning_checklist")
            if raw is None:
                return _NO_CHECKLIST
            checklist = json.loads(raw) if isinstance(raw, str) else dict(raw)
            checklist[key] = step_status
            new_status = evaluate_checklist(checklist)
            if new_status is not None:
                await DQLQuery(
                    "UPDATE catalog.catalogs "
                    "SET provisioning_checklist = CAST(:cl AS jsonb), "
                    "provisioning_status = :st WHERE id = :id;",
                    result_handler=ResultHandler.NONE,
                ).execute(conn, id=catalog_id, cl=json.dumps(checklist), st=new_status)
            else:
                await DQLQuery(
                    "UPDATE catalog.catalogs "
                    "SET provisioning_checklist = CAST(:cl AS jsonb) "
                    "WHERE id = :id;",
                    result_handler=ResultHandler.NONE,
                ).execute(conn, id=catalog_id, cl=json.dumps(checklist))
            return new_status

        result = await _provisioning_write_with_retry(engine, _do_checklist_update)

        if result is _NOT_FOUND:
            logger.warning(
                "mark_provisioning_step: catalog '%s' not found.", catalog_id
            )
            return False
        if result is _NO_CHECKLIST:
            logger.debug(
                "mark_provisioning_step: catalog '%s' has no checklist; "
                "step '%s' ignored.", catalog_id, key,
            )
            return False

        new_status = result
        _invalidate_catalog_model_cache(catalog_id)

        # Phase 2 — OUTSIDE the transaction: fan out when the overall status
        # changed.  The PG row is committed so a fresh read sees the new state.
        if new_status is not None:
            catalog_model = await self.get_catalog_model(catalog_id)
            if catalog_model is not None:
                metadata = _build_catalog_metadata_payload(catalog_model)
                if metadata:
                    from dynastore.modules.catalog.catalog_router import (
                        upsert_catalog_metadata,
                    )
                    await upsert_catalog_metadata(catalog_id, metadata)
        return True

    async def drain_pending_checklist_steps(
        self,
        catalog_id: str,
        terminal_status: str = "degraded",
        ctx: Optional["DriverContext"] = None,
    ) -> bool:
        """Mark every still-pending checklist step terminal and re-evaluate (#1902).

        Called by the provisioning-task runner when the task exits (any path)
        without having marked every step itself, and by the reconciler sweep
        for catalogs stuck in ``provisioning`` with no live task.

        All steps that are still ``"pending"`` are set to ``terminal_status``
        (default ``"degraded"`` so the catalog still becomes ready; pass
        ``"failed"`` for a hard-failure path). Steps already in a terminal
        state (``complete``/``skipped``/``degraded``/``failed``) are not
        touched. After updating the checklist ``evaluate_checklist`` decides
        the new catalog status exactly as ``mark_provisioning_step`` does.

        Returns ``True`` when at least one step was updated; ``False`` when
        the catalog was not found, has no checklist, or all steps were already
        terminal.
        """
        from dynastore.modules.catalog.provisioning_registry import (
            STEP_PENDING,
            evaluate_checklist,
        )

        db_resource = ctx.db_resource if ctx else None
        engine = get_catalog_engine(db_resource)

        _NOT_FOUND = object()
        _NO_CHECKLIST = object()
        _ALREADY_TERMINAL = object()

        async def _do_drain(conn: Any) -> Any:
            row = await _get_provisioning_checklist_query.execute(conn, id=catalog_id)
            if not row:
                return _NOT_FOUND
            raw = row.get("provisioning_checklist")
            if raw is None:
                return _NO_CHECKLIST
            checklist = json.loads(raw) if isinstance(raw, str) else dict(raw)
            pending_keys = [k for k, v in checklist.items() if v == STEP_PENDING]
            if not pending_keys:
                return _ALREADY_TERMINAL
            for key in pending_keys:
                checklist[key] = terminal_status
            new_status = evaluate_checklist(checklist)
            if new_status is not None:
                await DQLQuery(
                    "UPDATE catalog.catalogs "
                    "SET provisioning_checklist = CAST(:cl AS jsonb), "
                    "provisioning_status = :st WHERE id = :id;",
                    result_handler=ResultHandler.NONE,
                ).execute(conn, id=catalog_id, cl=json.dumps(checklist), st=new_status)
            else:
                await DQLQuery(
                    "UPDATE catalog.catalogs "
                    "SET provisioning_checklist = CAST(:cl AS jsonb) "
                    "WHERE id = :id;",
                    result_handler=ResultHandler.NONE,
                ).execute(conn, id=catalog_id, cl=json.dumps(checklist))
            return (pending_keys, new_status)

        result = await _provisioning_write_with_retry(engine, _do_drain)

        if result is _NOT_FOUND:
            logger.warning(
                "drain_pending_checklist_steps: catalog '%s' not found.", catalog_id
            )
            return False
        if result is _NO_CHECKLIST:
            logger.debug(
                "drain_pending_checklist_steps: catalog '%s' has no checklist.",
                catalog_id,
            )
            return False
        if result is _ALREADY_TERMINAL:
            logger.debug(
                "drain_pending_checklist_steps: catalog '%s' — all steps already "
                "terminal, nothing to drain.",
                catalog_id,
            )
            return False

        pending_keys, new_status = result
        logger.warning(
            "drain_pending_checklist_steps: catalog '%s' — %d step(s) %s "
            "still pending; marked '%s'. New catalog status: %s.",
            catalog_id, len(pending_keys), pending_keys, terminal_status,
            new_status or "unchanged (still provisioning)",
        )
        _invalidate_catalog_model_cache(catalog_id)

        # Fan out when the overall status changed (mirrors mark_provisioning_step).
        if new_status is not None:
            catalog_model = await self.get_catalog_model(catalog_id)
            if catalog_model is not None:
                metadata = _build_catalog_metadata_payload(catalog_model)
                if metadata:
                    from dynastore.modules.catalog.catalog_router import (
                        upsert_catalog_metadata,
                    )
                    await upsert_catalog_metadata(catalog_id, metadata)
        return True


# --- Standalone Utilities ---


async def ensure_catalog_exists(
    db_resource: DbResource,
    catalog_id: str,
    title: Optional[LocalizedText] = None,
    description: Optional[LocalizedText] = None,
):
    """Standalone helper to ensure a catalog exists."""
    from dynastore.tools.discovery import get_protocol
    from dynastore.models.protocols.catalogs import CatalogsProtocol

    catalogs = get_protocol(CatalogsProtocol)
    _ctx = DriverContext(db_resource=db_resource) if db_resource else None
    if catalogs:
        await catalogs.ensure_catalog_exists(catalog_id, ctx=_ctx)
    else:
        # Fallback if discovery not ready
        service = CatalogService(db_resource)  # type: ignore[abstract]
        if not await service.get_catalog_model(catalog_id, ctx=_ctx):
            await service.create_catalog(
                {"id": catalog_id, "title": title, "description": description},
                ctx=_ctx,
            )


async def ensure_collection_exists(
    db_resource: DbResource,
    catalog_id: str,
    collection_id: str,
    title: Optional[LocalizedText] = None,
    description: Optional[LocalizedText] = None,
):
    """Standalone helper to ensure a collection exists."""
    from dynastore.tools.discovery import get_protocol
    from dynastore.models.protocols.catalogs import CatalogsProtocol

    catalogs = get_protocol(CatalogsProtocol)

    # Ensure catalog first
    await ensure_catalog_exists(db_resource, catalog_id)

    _ctx = DriverContext(db_resource=db_resource) if db_resource else None
    if catalogs:
        if not await catalogs.get_collection(
            catalog_id, collection_id, ctx=_ctx
        ):
            await catalogs.create_collection(
                catalog_id,
                {"id": collection_id, "title": title, "description": description},
                ctx=_ctx,
            )
    else:
        # Fallback
        service = CatalogService(db_resource)  # type: ignore[abstract]
        if not await service.get_collection_model(
            catalog_id, collection_id, db_resource=db_resource
        ):
            await service.create_collection(
                catalog_id,
                {"id": collection_id, "title": title, "description": description},
                ctx=_ctx,
            )
