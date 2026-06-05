"""
OpenSearch singleton client — initialized once at lifespan startup.

Uses ``opensearch-py`` (Apache 2.0) which is wire-compatible with both
OpenSearch and Elasticsearch servers.  No product-check header is sent,
so it works against ES 9.x in production and OpenSearch in dev/on-premise.

Connection-target ES_* env vars (host, port, credentials, SSL) are read
directly from the environment — those are deployment-shape secrets.
Operator-tunable transport knobs (request timeout, pool size, retries)
come from ``ElasticsearchClientConfig`` (PluginConfig) and are read once
at lifespan startup. The single async client instance manages its own
connection pool.

Usage
-----
In ElasticsearchModule.lifespan:
    from dynastore.modules.elasticsearch import client as es_client
    await es_client.init()
    yield
    await es_client.close()

Everywhere else:
    from dynastore.modules.elasticsearch.client import get_client, get_index_prefix
    es = get_client()        # may be None when ES is not configured
    prefix = get_index_prefix()
"""
import logging
import os
from typing import Optional

from opensearchpy import AsyncOpenSearch

from dynastore.modules.elasticsearch._serializer import CustomOpenSearchSerializer
from dynastore.modules.elasticsearch.client_config import (
    ElasticsearchClientConfig,
    load as load_client_config,
)

logger = logging.getLogger(__name__)

_client: Optional[AsyncOpenSearch] = None
_index_prefix: str = "dynastore"
# Tri-state: None = not yet probed, True/False = detected at init() from the
# cluster's info() distribution. Drives the flattened→flat_object mapping shim.
_is_opensearch: Optional[bool] = None


def get_client() -> Optional[AsyncOpenSearch]:
    """Return the shared async client instance, or None if not initialized."""
    return _client


def is_opensearch_backend() -> bool:
    """True when the connected cluster reported the OpenSearch distribution at
    init(). False before init() or against Elasticsearch."""
    return bool(_is_opensearch)


def get_index_prefix() -> str:
    return _index_prefix


def _build_client(cfg: ElasticsearchClientConfig) -> AsyncOpenSearch:
    """
    Build an AsyncOpenSearch client.

    Connection target comes from env (deployment-shape secrets):
        ES_HOST           host name or IP   (default: localhost)
        ES_PORT           port              (default: 9200)
        ES_USE_SSL        true/false        (default: false)
        ES_VERIFY_CERTS   true/false        (default: true)
        ES_API_KEY        API key string    (preferred auth)
        ES_USERNAME       basic-auth user
        ES_PASSWORD       basic-auth password

    Transport tuning comes from ElasticsearchClientConfig.
    """
    host = os.environ.get("ES_HOST", "localhost")
    port = int(os.environ.get("ES_PORT", "9200"))
    use_ssl = os.environ.get("ES_USE_SSL", "false").strip().lower() in ("1", "true", "yes")
    scheme = "https" if use_ssl else "http"
    verify_certs = os.environ.get("ES_VERIFY_CERTS", "true").strip().lower() in ("1", "true", "yes")
    api_key = os.environ.get("ES_API_KEY")
    username = os.environ.get("ES_USERNAME")
    password = os.environ.get("ES_PASSWORD")

    kwargs = {
        "hosts": [f"{scheme}://{host}:{port}"],
        "verify_certs": verify_certs,
        "maxsize": cfg.connections_per_node,
        "timeout": cfg.request_timeout_seconds,
        "retry_on_timeout": True,
        "max_retries": cfg.max_retries,
        # Custom serializer tolerates pydantic models, pydantic v2 Url
        # types (HttpUrl/AnyUrl/…), __geo_interface__ objects, sets and
        # bytes — the stock JSONSerializer only handles datetime/UUID/Decimal
        # and crashed on `providers[*].url: HttpUrl(...)` in STAC Collection
        # upserts (prod incident 2026-04-22).
        "serializer": CustomOpenSearchSerializer(),
    }
    if api_key:
        kwargs["headers"] = {"Authorization": f"ApiKey {api_key}"}
    elif username and password:
        kwargs["http_auth"] = (username, password)

    return AsyncOpenSearch(**kwargs)


async def init(index_prefix: Optional[str] = None) -> None:
    """
    Initialize the singleton client.  Called once from ElasticsearchModule.lifespan.
    Safe to call when ES env vars are not set — client will be created but may
    fail at first use (lazy connection; does not block startup).

    A connectivity ping is attempted after client creation.  On success the
    cluster name and version are logged at INFO.  On failure a WARNING is emitted
    and startup continues — ES is optional; callers handle missing connectivity.
    """
    global _client, _index_prefix, _is_opensearch
    _index_prefix = index_prefix or os.environ.get("ES_INDEX_PREFIX", "dynastore")
    cfg = await load_client_config()
    _client = _build_client(cfg)
    host = os.environ.get("ES_HOST", "localhost")
    port = os.environ.get("ES_PORT", "9200")
    logger.info(
        "Client built: timeout=%ds maxsize=%d max_retries=%d",
        cfg.request_timeout_seconds,
        cfg.connections_per_node,
        cfg.max_retries,
    )
    try:
        info = await _client.info()
        cluster = info.get("cluster_name", "unknown")
        version = info.get("version", {}).get("number", "unknown")
        # Detect the server dialect and, on OpenSearch, install the
        # flattened→flat_object mapping shim so index-create paths that declare
        # the ES-only ``flattened`` type don't 400 (see _backend_compat).
        from dynastore.modules.elasticsearch._backend_compat import (
            install_opensearch_mapping_shim,
            is_opensearch_distribution,
        )
        _is_opensearch = is_opensearch_distribution(info)
        if _is_opensearch:
            install_opensearch_mapping_shim(_client)
        logger.info(
            "Connected: cluster=%r version=%s opensearch=%s host=%s:%s prefix=%r ssl=%s",
            cluster,
            version,
            _is_opensearch,
            host,
            port,
            _index_prefix,
            os.environ.get("ES_USE_SSL", "false"),
        )
    except Exception as exc:
        logger.warning(
            "Ping failed — %s:%s unreachable (%s). "
            "Indexing will be unavailable until the connection is restored.",
            host,
            port,
            exc,
        )


async def close() -> None:
    """Gracefully close the singleton client.  Called from lifespan teardown."""
    global _client
    if _client is not None:
        try:
            await _client.close()
        except Exception as exc:
            logger.warning("Client close error: %s", exc)
        finally:
            _client = None
        logger.info("Elasticsearch client closed.")
