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

from dynastore.models.mutability import Mutable
from dynastore.modules.db_config.plugin_config import PluginConfig
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

    policy: Mutable[Literal["global", "ttl_lru"]] = Field(
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

    ttl_seconds: Mutable[Optional[int]] = Field(
        default=None,
        ge=1,
        description=(
            "Idle TTL before eviction; required when ``policy='ttl_lru'``. "
            "Ignored under ``policy='global'``."
        ),
    )

    max_parallel: Mutable[Optional[int]] = Field(
        default=None,
        ge=1,
        description=(
            "Soft cap on concurrent in-flight calls per instance.  "
            "Exceeding the cap emits a structured-log warning rather "
            "than queueing or rejecting (operators can promote the "
            "warning to an error via log-aggregation rules)."
        ),
    )

    immutable: Mutable[bool] = Field(
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
    live at ``_address = ("platform", "protocols", "storage")`` — the configs API
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

    _freeze_at: ClassVar[Optional[str]] = "platform"

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
                    f'``engine_class: ClassVar[str] = "my_engine"`` so '
                    f"driver-side ``required_engine_class`` checks (F.2) "
                    f"can match this engine."
                )

    enabled: Mutable[bool] = Field(
        default=True,
        description=(
            "When False, drivers referencing this engine return 503 "
            "Service Unavailable at first dispatch with a structured-"
            "log warning.  Use this for maintenance windows without "
            "destructive deletes."
        ),
    )

    lifecycle: Mutable[EngineLifecycleConfig] = Field(
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
    _address: ClassVar[Tuple[str, ...]] = ("platform", "protocols", "storage")

    connection_url: Mutable[Optional[Secret]] = Field(
        default=None,
        description=(
            "Optional override for the PG connection URL.  When None, "
            "the engine inherits ``DBConfig.database_url`` (env-driven). "
            "Set when an operator wants a per-engine override (e.g. a "
            "secondary PG cluster for hot/cold tiering — UC3)."
        ),
    )

    pool_size: Mutable[int] = Field(
        default=10,
        ge=1,
        le=200,
        description=(
            "Maximum concurrent PG connections in the pool.  Tune to "
            "the deployment's process count × per-process workload."
        ),
    )

    pool_timeout_sec: Mutable[int] = Field(
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
    ``collection_elasticsearch_driver``) reference this engine
    via ``engine_ref`` (F.2).
    """

    engine_class: ClassVar[str] = "elasticsearch_engine"
    _address: ClassVar[Tuple[str, ...]] = ("platform", "protocols", "storage")

    cluster_url: Mutable[Optional[Secret]] = Field(
        default=None,
        description=(
            "Optional override for the ES cluster URL.  When None, the "
            "engine inherits the platform-wide client (env-driven via "
            "``ELASTICSEARCH_HOSTS`` etc.)."
        ),
    )

    api_key: Mutable[Optional[Secret]] = Field(
        default=None,
        description=(
            "Optional API key for authenticated clusters.  Stored as a "
            "Secret so the encrypted-at-rest path round-trips through "
            "the standard secrets infra."
        ),
    )

    request_timeout_sec: Mutable[int] = Field(
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
        env-driven client (``ELASTICSEARCH_HOSTS`` etc.) when both are
        unset — the same source the existing ``module_elasticsearch``
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
                "ElasticsearchEngineConfig: client close raised; " "instance dropped."
            )


class DuckdbEngineConfig(EngineConfig):
    """DuckDB process pool — backs the items DuckDB driver.

    Heavy local state (in-process duckdb instance + memory budget) →
    default lifecycle suggests ``ttl_lru`` for idle eviction in
    long-running deployments.  Operators tune ``pool_size`` to the
    available cores; ``max_memory_gb`` caps the per-process heap.
    """

    engine_class: ClassVar[str] = "duckdb_engine"
    _address: ClassVar[Tuple[str, ...]] = ("platform", "protocols", "storage")

    pool_size: Mutable[int] = Field(
        default=4,
        ge=1,
        le=64,
        description=(
            "Number of DuckDB processes in the pool.  Tune to available "
            "cores × workload concurrency."
        ),
    )

    max_memory_gb: Mutable[int] = Field(
        default=4,
        ge=1,
        description="Per-process memory budget (PRAGMA memory_limit).",
    )

    threads: Mutable[int] = Field(
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
        import duckdb  # optional extra: module_storage_duckdb

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
                "DuckdbEngineConfig: connection close raised; " "instance dropped."
            )


class IcebergEngineConfig(EngineConfig):
    """Iceberg catalog client — backs the items Iceberg driver.

    Each engine binds to ONE Iceberg catalog; tenants needing a
    different warehouse ask a sysadmin to provision a second engine
    (e.g. ``iceberg_warehouse_b``).  Catalog properties carry
    Secret-typed values so the encrypted-at-rest path round-trips.
    """

    engine_class: ClassVar[str] = "iceberg_engine"
    _address: ClassVar[Tuple[str, ...]] = ("platform", "protocols", "storage")

    catalog_uri: Mutable[Optional[Secret]] = Field(
        default=None,
        description=(
            "Iceberg catalog URI (REST or SQL).  Stored as a Secret so "
            "credentials in the URI are encrypted at rest."
        ),
    )

    warehouse_uri: Mutable[Optional[Secret]] = Field(
        default=None,
        description=(
            "Optional warehouse location override (e.g. an S3/GCS "
            "bucket URL).  Stored as a Secret."
        ),
    )

    catalog_properties: Mutable[Dict[str, Secret]] = Field(
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
        from pyiceberg.catalog import (
            load_catalog,
        )  # optional extra: module_storage_iceberg

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


class ValkeyEngineConfig(EngineConfig):
    """Valkey (Redis-compatible) cache client engine.

    Backs the shared ``ValkeyCacheBackend`` consumed by the
    ``CacheModule``.  Operators reconfigure connection params
    (URL/host/TLS/IAM/cluster) live via the configs API; the cache
    module's apply handler tears down the old client + registers a
    new one without restart.

    Default lifecycle: ``global`` — the client is cheap to keep
    warm.  ``immutable=False`` so the engine itself is reconfigurable
    (default base engines lock at ``immutable=True``).
    """

    engine_class: ClassVar[str] = "valkey_engine"
    _address: ClassVar[Tuple[str, ...]] = ("platform", "protocols", "storage")

    # Override default to allow live reconfig.
    lifecycle: Mutable[EngineLifecycleConfig] = Field(
        default_factory=lambda: EngineLifecycleConfig(immutable=False),
        description=(
            "Lifecycle policy for this engine.  Defaults to "
            "``policy='global'`` with ``immutable=False`` so the cache "
            "module's apply handler can rebuild the client on config "
            "changes without a restart."
        ),
    )

    # ------------------------------------------------------------------
    # Connection (Mutable Secret — live reconnect on change)
    # ------------------------------------------------------------------
    connection_url: Mutable[Optional[Secret]] = Field(
        default=None,
        description=(
            "Optional Valkey connection URL (e.g. ``valkey://10.0.0.1:6379``). "
            "When None, falls back to the ``VALKEY_URL`` environment variable "
            "for boot-time bootstrap.  In ``cluster_mode=True`` deployments, "
            "``discovery_host`` + ``discovery_port`` take precedence over a URL."
        ),
    )

    discovery_host: Mutable[Optional[Secret]] = Field(
        default=None,
        description=(
            "GCP Memorystore for Valkey CLUSTER discovery endpoint host.  "
            "When set with ``cluster_mode=True``, the client connects to "
            "the discovery endpoint only (with ``dynamic_startup_nodes=False`` "
            "preventing topology refresh from CLUSTER SLOTS)."
        ),
    )

    discovery_port: Mutable[int] = Field(
        default=6379,
        ge=1,
        le=65535,
        description="Port paired with ``discovery_host`` for cluster discovery.",
    )

    # ------------------------------------------------------------------
    # Cluster (Mutable bool — Memorystore Valkey CLUSTER pattern)
    # ------------------------------------------------------------------
    cluster_mode: Mutable[bool] = Field(
        default=False,
        description=(
            "Use ``valkey.asyncio.cluster.ValkeyCluster`` instead of the "
            "standalone client.  Required for GCP Memorystore for Valkey "
            "CLUSTER instances; standalone clients misroute commands and "
            "surface NOAUTH-shaped errors against a cluster endpoint."
        ),
    )

    require_full_coverage: Mutable[bool] = Field(
        default=False,
        description=(
            "ValkeyCluster ``require_full_coverage`` flag.  False means "
            "the client tolerates partial slot coverage during topology "
            "transitions — the right default for Memorystore CLUSTER "
            "where the discovery endpoint always answers but per-node "
            "visibility may lag during failover."
        ),
    )

    dynamic_startup_nodes: Mutable[bool] = Field(
        default=False,
        description=(
            "ValkeyCluster ``dynamic_startup_nodes`` flag.  False means "
            "the client does NOT refresh topology from CLUSTER SLOTS — "
            "essential for Memorystore Valkey CLUSTER discovery-only "
            "mode where direct backend connections are blocked."
        ),
    )

    # ------------------------------------------------------------------
    # TLS (Mutable)
    # ------------------------------------------------------------------
    tls: Mutable[bool] = Field(
        default=False,
        description=(
            "Wrap the connection in TLS regardless of URL scheme.  "
            "Required for GCP Memorystore IAM mode."
        ),
    )

    tls_ca_path: Mutable[Optional[str]] = Field(
        default=None,
        description=(
            "Optional path to a server CA bundle for verification.  "
            "When None and ``tls=True``, cert/hostname checks default "
            "to disabled (acceptable on private VPC)."
        ),
    )

    tls_cert_reqs: Mutable[Literal["none", "optional", "required"]] = Field(
        default="none",
        description=(
            "TLS certificate verification mode.  ``none`` is the "
            "Memorystore VPC-internal default; set to ``required`` "
            "when ``tls_ca_path`` is provided."
        ),
    )

    tls_check_hostname: Mutable[bool] = Field(
        default=False,
        description=(
            "Whether to validate the server certificate hostname.  "
            "Defaults to False on private VPC; enable when using "
            "publicly-rooted CAs."
        ),
    )

    # ------------------------------------------------------------------
    # IAM (Mutable)
    # ------------------------------------------------------------------
    iam_auth: Mutable[bool] = Field(
        default=False,
        description=(
            "Authenticate via a Google OAuth2 access token minted from "
            "ADC (GCP Memorystore for Valkey IAM AUTH mode).  Requires "
            "``google-auth`` (provided by the ``module_gcp`` extra)."
        ),
    )

    # ------------------------------------------------------------------
    # Timeouts / keepalives (Mutable)
    # ------------------------------------------------------------------
    socket_connect_timeout_seconds: Mutable[float] = Field(
        default=3.0,
        ge=0.1,
        le=60,
        description=(
            "Timeout for individual Valkey socket connection attempts.  "
            "Applies to each node in cluster mode.  Keep low for fast "
            "failover; the discovery endpoint should answer in <1s on a "
            "healthy network."
        ),
    )

    socket_timeout_seconds: Mutable[float] = Field(
        default=5.0,
        ge=0.5,
        le=120,
        description=(
            "Read timeout for individual Valkey operations.  Cache "
            "callers wrap each op so a timeout falls through to the "
            "source; three consecutive timeouts trip the circuit "
            "breaker that drops Valkey from rotation entirely."
        ),
    )

    tcp_keepalive_idle_seconds: Mutable[int] = Field(
        default=300,
        ge=10,
        le=1200,
        description=(
            "Idle time before the first TCP keepalive probe on a Valkey "
            "connection.  Sits well under Cloud NAT's ~1200s established-"
            "connection timeout so idle Cloud Run↔Memorystore sockets are "
            "kept alive instead of being silently dropped."
        ),
    )

    tcp_keepalive_interval_seconds: Mutable[int] = Field(
        default=30,
        ge=5,
        le=300,
        description="Interval between TCP keepalive probes once a Valkey connection is idle.",
    )

    tcp_keepalive_count: Mutable[int] = Field(
        default=5,
        ge=1,
        le=20,
        description=(
            "Number of unacknowledged TCP keepalive probes before a "
            "Valkey connection is considered dead."
        ),
    )

    async def engine_init(self) -> Any:
        """Build a Valkey async client from the current config snapshot.

        Connection precedence:
          1. ``cluster_mode=True`` + ``discovery_host`` set → cluster
             discovery endpoint (Memorystore Valkey CLUSTER pattern).
          2. ``connection_url`` (Secret) → either standalone or cluster
             based on ``cluster_mode``.
          3. ``VALKEY_URL`` env var → bootstrap fallback.
        """
        import os as _os
        from dynastore.tools.cache_valkey import build_valkey_client

        url: Optional[str] = None
        if self.connection_url is not None:
            url = self.connection_url.reveal()
        elif _os.getenv("VALKEY_URL"):
            url = _os.getenv("VALKEY_URL")

        host = self.discovery_host.reveal() if self.discovery_host is not None else None

        client, _pool = build_valkey_client(
            url=url,
            discovery_host=host,
            discovery_port=self.discovery_port,
            cluster_mode=self.cluster_mode,
            require_full_coverage=self.require_full_coverage,
            dynamic_startup_nodes=self.dynamic_startup_nodes,
            tls=self.tls,
            tls_ca_path=self.tls_ca_path,
            tls_cert_reqs=self.tls_cert_reqs,
            tls_check_hostname=self.tls_check_hostname,
            iam_auth=self.iam_auth,
            socket_connect_timeout=self.socket_connect_timeout_seconds,
            socket_timeout=self.socket_timeout_seconds,
            tcp_keepalive_idle=self.tcp_keepalive_idle_seconds,
            tcp_keepalive_interval=self.tcp_keepalive_interval_seconds,
            tcp_keepalive_count=self.tcp_keepalive_count,
        )
        # Stash the pool on the client so engine_release can close both.
        # ValkeyCluster owns its pools internally so _pool is None there.
        try:
            setattr(client, "_ds_engine_pool", _pool)
        except Exception:  # noqa: BLE001 — defensive; pool reference is best-effort
            pass
        return client

    async def engine_release(self, instance: Any) -> None:
        """Close the Valkey client (and its connection pool, if any).

        Idempotent and best-effort — failures are logged, not raised,
        because the cache surrounding this engine has its own circuit
        breaker.
        """
        logger = _logger()
        pool = getattr(instance, "_ds_engine_pool", None)
        aclose = getattr(instance, "aclose", None)
        if aclose is not None:
            try:
                await aclose()
            except Exception:
                logger.exception(
                    "ValkeyEngineConfig: client aclose raised; instance dropped."
                )
        if pool is not None:
            pool_aclose = getattr(pool, "aclose", None)
            if pool_aclose is not None:
                try:
                    await pool_aclose()
                except Exception:
                    logger.exception(
                        "ValkeyEngineConfig: pool aclose raised; pool dropped."
                    )


__all__ = [
    "EngineLifecycleConfig",
    "EngineConfig",
    "PostgresqlEngineConfig",
    "ElasticsearchEngineConfig",
    "DuckdbEngineConfig",
    "IcebergEngineConfig",
    "ValkeyEngineConfig",
]
