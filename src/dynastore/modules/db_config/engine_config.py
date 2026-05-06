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

"""Platform-tier engine configurations (Cycle F.1).

Engines are platform-only connection-and-pool resources, sysadmin-locked
by default.  They are referenced from driver configs via ``engine_ref``
(Cycle F.2) so multiple drivers of the same class can share a connection
pool, and so a single driver class can run against multiple physical
engines (e.g. ``pg_main`` + ``pg_secondary`` for sharding).

Cycle F.1 shipped **single-instance-per-kind**: one default engine of
each ``engine_class`` (``postgresql_engine``, ``elasticsearch_engine``,
``duckdb_engine``, ``iceberg_engine``) keyed by the snake_case class
name.  Cycle F.4c then added the ref-keyed storage layer
(``platform_configs.ref_key`` column, ``get_config_by_ref`` /
``set_config_by_ref`` API) that lets operators register multiple
instances per kind alongside the canonical class-keyed default.

Driver-tier lifecycle is forbidden (decision #15 / #18 in
``binary-leaping-lightning.md``): tenants cannot influence platform
resource policy.  Lifecycle lives ONLY on engines.
"""

from __future__ import annotations

import logging
from typing import Any, ClassVar, Dict, Literal, Optional, Tuple

from pydantic import BaseModel, Field, model_validator

from dynastore.modules.db_config.platform_config_service import PluginConfig
from dynastore.tools.secrets import Secret


def _logger() -> logging.Logger:
    return logging.getLogger(__name__)


class EngineLifecycleConfig(BaseModel):
    """Lifecycle policy attached to a platform engine.

    Engines are lazy-instantiated singletons by default.  Operators that
    need bounded resource use (e.g. DuckDB processes idling under load)
    set ``policy="ttl_lru"`` and configure ``ttl_seconds`` /
    ``max_parallel``.

    Driver-tier lifecycle is forbidden — a driver instance binds to the
    engine indicated by its ``engine_ref`` and inherits whatever pooling
    / eviction policy the engine declares.  This preserves strict
    tenant-vs-platform separation: tenant configs cannot influence
    platform resource policy.
    """

    policy: Literal["global", "ttl_lru"] = Field(
        default="global",
        description=(
            "``global``: lazy-instantiated singleton, never evicted. "
            "Right for cheap connection clients (PG pool, ES client) "
            "where keeping the instance warm has no downside.  "
            "``ttl_lru``: idle eviction after ``ttl_seconds``; cap on "
            "concurrent in-flight calls via ``max_parallel``.  Right "
            "for heavy local state (DuckDB process, Iceberg catalog "
            "cache)."
        ),
    )

    ttl_seconds: Optional[int] = Field(
        default=None,
        ge=1,
        description=(
            "Idle TTL before eviction; required when ``policy='ttl_lru'``. "
            "Ignored under ``policy='global'``."
        ),
    )

    max_parallel: Optional[int] = Field(
        default=None,
        ge=1,
        description=(
            "Soft cap on concurrent in-flight calls per instance.  "
            "Exceeding the cap emits a structured-log warning rather "
            "than queueing or rejecting (operators can promote the "
            "warning to an error via log-aggregation rules)."
        ),
    )

    immutable: bool = Field(
        default=True,
        description=(
            "When True (default), the engine cannot be reconfigured or "
            "dropped via the standard configuration API once "
            "provisioned — any change requires a sysadmin-level "
            "operation outside the runtime PATCH path.  Operators "
            "wanting a maintenance window flip ``enabled=False`` on "
            "the engine itself rather than mutating immutable fields."
        ),
    )

    @model_validator(mode="after")
    def _ttl_required_for_ttl_lru(self) -> "EngineLifecycleConfig":
        if self.policy == "ttl_lru" and self.ttl_seconds is None:
            raise ValueError(
                "EngineLifecycleConfig: policy='ttl_lru' requires ttl_seconds "
                "to be set (no implicit default — operators must pick a value "
                "matching the engine's idle-cost profile)."
            )
        return self


class EngineConfig(PluginConfig):
    """Abstract base for every platform engine.

    Concrete subclasses (one per engine kind) declare an ``engine_class``
    discriminator + the engine's connection / pool fields.  All engines
    live at ``_address = ("platform", "engines")`` — the configs API
    surfaces them keyed by their ``class_key()`` (= snake_case of the
    class name).

    **IAM**: sysadmin-only writes are enforced by the existing
    ``configs_access`` policy (``extensions/configs/policies.py``) which
    gates the entire ``/configs/.*`` surface to the SYSADMIN role.
    Engines need no additional policy registration — they inherit the
    platform-tier gate.  Per-engine RBAC carve-outs are out of scope
    for F.1 (future cycle).
    """

    is_abstract_base: ClassVar[bool] = True

    # Engine-class discriminator — populated by concrete subclasses.
    # NOT a Pydantic field; subclasses re-declare the ClassVar.
    engine_class: ClassVar[str] = ""

    _visibility: ClassVar[Optional[str]] = "platform"

    def __init_subclass__(cls, **kwargs: Any) -> None:
        super().__init_subclass__(**kwargs)
        # Concrete EngineConfig subclasses MUST override ``engine_class``
        # to a non-empty discriminator.  Without this enforcement a typo
        # (or copy-paste forget) would silently inherit the empty-string
        # base default and the engine would be invisible to driver-side
        # ``required_engine_class`` validation (F.2).
        if not cls.__dict__.get("is_abstract_base", False):
            engine_class_val = cls.__dict__.get("engine_class")
            if not engine_class_val:
                raise TypeError(
                    f"{cls.__module__}.{cls.__qualname__} is a concrete "
                    f"EngineConfig but does not declare ``engine_class`` "
                    f"(or declares it empty).  Add e.g. "
                    f"``engine_class: ClassVar[str] = \"my_engine\"`` so "
                    f"driver-side ``required_engine_class`` checks (F.2) "
                    f"can match this engine."
                )

    enabled: bool = Field(
        default=True,
        description=(
            "When False, drivers referencing this engine return 503 "
            "Service Unavailable at first dispatch with a structured-"
            "log warning.  Use this for maintenance windows without "
            "destructive deletes."
        ),
    )

    lifecycle: EngineLifecycleConfig = Field(
        default_factory=EngineLifecycleConfig,
        description=(
            "Lifecycle policy for instances of this engine.  See "
            "``EngineLifecycleConfig`` for the policy semantics."
        ),
    )


class PostgresqlEngineConfig(EngineConfig):
    """PostgreSQL connection pool — backs every PG-driver class.

    Drivers (``items_postgresql_driver``, ``catalog_postgresql_driver``,
    ``collection_postgresql_driver``, ``asset_postgresql_driver``)
    reference this engine via ``engine_ref`` (F.2).  Default lifecycle
    is ``global`` — the pool is cheap to keep warm.
    """

    engine_class: ClassVar[str] = "postgresql_engine"
    _address: ClassVar[Tuple[str, ...]] = ("platform", "engines")

    connection_url: Optional[Secret] = Field(
        default=None,
        description=(
            "Optional override for the PG connection URL.  When None, "
            "the engine inherits ``DBConfig.database_url`` (env-driven). "
            "Set when an operator wants a per-engine override (e.g. a "
            "secondary PG cluster for hot/cold tiering — UC3)."
        ),
    )

    pool_size: int = Field(
        default=10,
        ge=1,
        le=200,
        description=(
            "Maximum concurrent PG connections in the pool.  Tune to "
            "the deployment's process count × per-process workload."
        ),
    )

    pool_timeout_sec: int = Field(
        default=30,
        ge=1,
        description=(
            "Seconds to wait for a free connection before raising "
            "``asyncpg.exceptions.PoolTimeoutError``."
        ),
    )

    async def engine_init(self) -> Any:
        """Build a dedicated ``asyncpg.Pool`` for this engine.

        Connection URL precedence: explicit ``connection_url`` (Secret) →
        env-driven ``DBConfig.database_url`` fallback.  The asyncpg driver
        accepts libpq-flavoured DSNs only, so the SQLAlchemy
        ``postgresql+asyncpg://`` prefix is normalised to ``postgresql://``
        — matches the strip already done in the outbox-pool helper.
        """
        import asyncpg  # local import: keeps F.1 import light

        from dynastore.modules.db_config.db_config import DBConfig

        if self.connection_url is not None:
            dsn = self.connection_url.reveal()
        else:
            dsn = DBConfig.database_url
        dsn = dsn.replace("postgresql+asyncpg://", "postgresql://")
        return await asyncpg.create_pool(
            dsn=dsn,
            min_size=1,
            max_size=self.pool_size,
            timeout=self.pool_timeout_sec,
        )

    async def engine_release(self, instance: Any) -> None:
        """Close the asyncpg pool.  Idempotent — releasing twice is safe."""
        close = getattr(instance, "close", None)
        if close is None:
            return
        try:
            await close()
        except Exception:
            logger = _logger()
            logger.exception(
                "PostgresqlEngineConfig: pool close raised; instance dropped."
            )


class ElasticsearchEngineConfig(EngineConfig):
    """Elasticsearch / OpenSearch client — backs every ES-driver class.

    Drivers (``items_elasticsearch_driver``,
    ``items_elasticsearch_private_driver``,
    ``catalog_elasticsearch_driver``,
    ``collection_elasticsearch_driver``,
    ``collection_elasticsearch_private_driver``) reference this engine
    via ``engine_ref`` (F.2).
    """

    engine_class: ClassVar[str] = "elasticsearch_engine"
    _address: ClassVar[Tuple[str, ...]] = ("platform", "engines")

    cluster_url: Optional[Secret] = Field(
        default=None,
        description=(
            "Optional override for the ES cluster URL.  When None, the "
            "engine inherits the existing SFEOS-derived client (env-"
            "driven via ``ELASTICSEARCH_HOSTS`` etc.)."
        ),
    )

    api_key: Optional[Secret] = Field(
        default=None,
        description=(
            "Optional API key for authenticated clusters.  Stored as a "
            "Secret so the encrypted-at-rest path round-trips through "
            "the standard secrets infra."
        ),
    )

    request_timeout_sec: int = Field(
        default=30,
        ge=1,
        description=(
            "Per-request timeout sent to the ES client.  Slow analytical "
            "scans should bump this; transactional reads keep the "
            "default."
        ),
    )

    async def engine_init(self) -> Any:
        """Build a dedicated ``AsyncOpenSearch`` client for this engine.

        Honours ``cluster_url`` + ``api_key`` overrides; falls back to the
        env-driven SFEOS client (``ELASTICSEARCH_HOSTS`` etc.) when both
        are unset — the same source the existing ``module_elasticsearch``
        boot path uses, so default deployments behave identically.
        """
        if self.cluster_url is None and self.api_key is None:
            from dynastore.modules.elasticsearch.client import get_client

            client = get_client()
            if client is None:
                raise RuntimeError(
                    "ElasticsearchEngineConfig: no cluster_url override and "
                    "the env-driven client is not initialised.  Either set "
                    "cluster_url on the engine or ensure ElasticsearchModule "
                    "lifespan has started before consuming the engine."
                )
            return client

        from opensearchpy import AsyncOpenSearch  # local import

        kwargs: Dict[str, Any] = {
            "timeout": self.request_timeout_sec,
        }
        if self.cluster_url is not None:
            kwargs["hosts"] = [self.cluster_url.reveal()]
        if self.api_key is not None:
            kwargs["http_auth"] = self.api_key.reveal()
        return AsyncOpenSearch(**kwargs)

    async def engine_release(self, instance: Any) -> None:
        """Close the OpenSearch client.

        The env-driven shared client is owned by ``ElasticsearchModule``;
        only dedicated clients (constructed when an override is set)
        carry a ``close`` we should call.  Best-effort: failures logged.
        """
        if self.cluster_url is None and self.api_key is None:
            return  # shared client; not our responsibility
        close = getattr(instance, "close", None)
        if close is None:
            return
        try:
            await close()
        except Exception:
            logger = _logger()
            logger.exception(
                "ElasticsearchEngineConfig: client close raised; "
                "instance dropped."
            )


class DuckdbEngineConfig(EngineConfig):
    """DuckDB process pool — backs the items DuckDB driver.

    Heavy local state (in-process duckdb instance + memory budget) →
    default lifecycle suggests ``ttl_lru`` for idle eviction in
    long-running deployments.  Operators tune ``pool_size`` to the
    available cores; ``max_memory_gb`` caps the per-process heap.
    """

    engine_class: ClassVar[str] = "duckdb_engine"
    _address: ClassVar[Tuple[str, ...]] = ("platform", "engines")

    pool_size: int = Field(
        default=4,
        ge=1,
        le=64,
        description=(
            "Number of DuckDB processes in the pool.  Tune to available "
            "cores × workload concurrency."
        ),
    )

    max_memory_gb: int = Field(
        default=4,
        ge=1,
        description="Per-process memory budget (PRAGMA memory_limit).",
    )

    threads: int = Field(
        default=4,
        ge=1,
        le=64,
        description="Per-process thread count (PRAGMA threads).",
    )

    async def engine_init(self) -> Any:
        """Build an in-memory DuckDB connection with PRAGMA tuning applied.

        DuckDB has no native pool — ``pool_size`` is informational; the
        engine returns a single per-engine connection pre-configured with
        ``threads`` and ``memory_limit``.  Concurrent query throughput
        comes from DuckDB's internal threading, so a single connection
        per engine is the right unit (re-creating it per request would
        defeat the cache's purpose).
        """
        import duckdb  # type: ignore[import-not-found]  # optional extra: module_storage_duckdb

        conn = duckdb.connect(":memory:")
        conn.execute(f"PRAGMA threads={self.threads}")
        conn.execute(f"PRAGMA memory_limit='{self.max_memory_gb}GB'")
        return conn

    async def engine_release(self, instance: Any) -> None:
        """Close the DuckDB connection.  ``close()`` is sync on duckdb.

        We swallow + log instead of raising — eviction is best-effort.
        """
        close = getattr(instance, "close", None)
        if close is None:
            return
        try:
            close()
        except Exception:
            logger = _logger()
            logger.exception(
                "DuckdbEngineConfig: connection close raised; "
                "instance dropped."
            )


class IcebergEngineConfig(EngineConfig):
    """Iceberg catalog client — backs the items Iceberg driver.

    Each engine binds to ONE Iceberg catalog; tenants needing a
    different warehouse ask a sysadmin to provision a second engine
    (e.g. ``iceberg_warehouse_b``).  Catalog properties carry
    Secret-typed values so the encrypted-at-rest path round-trips.
    """

    engine_class: ClassVar[str] = "iceberg_engine"
    _address: ClassVar[Tuple[str, ...]] = ("platform", "engines")

    catalog_uri: Optional[Secret] = Field(
        default=None,
        description=(
            "Iceberg catalog URI (REST or SQL).  Stored as a Secret so "
            "credentials in the URI are encrypted at rest."
        ),
    )

    warehouse_uri: Optional[Secret] = Field(
        default=None,
        description=(
            "Optional warehouse location override (e.g. an S3/GCS "
            "bucket URL).  Stored as a Secret."
        ),
    )

    catalog_properties: Dict[str, Secret] = Field(
        default_factory=dict,
        description=(
            "Free-form catalog properties forwarded to PyIceberg's "
            "``load_catalog``.  Each value is encrypted at rest; use "
            "this to carry per-engine credentials (e.g. AWS access "
            "keys, REST-catalog auth tokens)."
        ),
    )

    async def engine_init(self) -> Any:
        """Load the PyIceberg catalog client for this engine.

        Builds the properties dict from ``catalog_uri`` / ``warehouse_uri``
        / ``catalog_properties`` (Secret-typed → revealed to plaintext at
        the load-call boundary), then delegates to
        ``pyiceberg.catalog.load_catalog``.  PyIceberg's catalog object
        owns its own connection pooling.
        """
        from pyiceberg.catalog import load_catalog  # type: ignore[import-not-found]  # optional extra: module_storage_iceberg

        properties: Dict[str, str] = {}
        if self.catalog_uri is not None:
            properties["uri"] = self.catalog_uri.reveal()
        if self.warehouse_uri is not None:
            properties["warehouse"] = self.warehouse_uri.reveal()
        for key, secret in self.catalog_properties.items():
            properties[key] = secret.reveal()
        # Catalog name is informational for PyIceberg; using the engine's
        # class_key keeps the name stable across boots.
        return load_catalog(self.__class__.class_key(), **properties)

    async def engine_release(self, instance: Any) -> None:
        """PyIceberg catalogs have no explicit close — drop reference."""
        # No-op intentional.  The catalog object's underlying SQL/HTTP
        # clients are GC-driven; eviction frees them once the reference
        # count drops to zero.  Documented here so a future reader does
        # not assume we're missing a teardown call.
        return None


__all__ = [
    "EngineLifecycleConfig",
    "EngineConfig",
    "PostgresqlEngineConfig",
    "ElasticsearchEngineConfig",
    "DuckdbEngineConfig",
    "IcebergEngineConfig",
]
