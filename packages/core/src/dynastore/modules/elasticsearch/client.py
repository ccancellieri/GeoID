"""
OpenSearch singleton client — initialized once at lifespan startup.

Uses ``opensearch-py`` (Apache 2.0) which is wire-compatible with both
OpenSearch and Elasticsearch servers.  No product-check header is sent,
so it works against ES 9.x in production and OpenSearch in dev/on-premise.

All ES_* connection parameters are read directly from environment variables.
No Pydantic model; no per-request client creation. The single async client
instance manages its own connection pool.

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

logger = logging.getLogger(__name__)

_client: Optional[AsyncOpenSearch] = None
_index_prefix: str = "dynastore"


def get_client() -> Optional[AsyncOpenSearch]:
    """Return the shared async client instance, or None if not initialized."""
    return _client


def get_index_prefix() -> str:
    return _index_prefix


def _build_client() -> AsyncOpenSearch:
    """
    Build an AsyncOpenSearch client from ES_* environment variables.

    Supported variables:
        ES_HOST           host name or IP   (default: localhost)
        ES_PORT           port              (default: 9200)
        ES_USE_SSL        true/false        (default: false)
        ES_VERIFY_CERTS   true/false        (default: true)
        ES_API_KEY        API key string    (preferred auth)
        ES_USERNAME       basic-auth user
        ES_PASSWORD       basic-auth password
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
        "maxsize": int(os.environ.get("ES_CONNECTIONS_PER_NODE", "10")),
        "timeout": int(os.environ.get("ES_REQUEST_TIMEOUT", "30")),
        "retry_on_timeout": True,
        "max_retries": 3,
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
    global _client, _index_prefix
    _index_prefix = index_prefix or os.environ.get("ES_INDEX_PREFIX", "dynastore")
    _client = _build_client()
    host = os.environ.get("ES_HOST", "localhost")
    port = os.environ.get("ES_PORT", "9200")
    try:
        info = await _client.info()
        cluster = info.get("cluster_name", "unknown")
        version = info.get("version", {}).get("number", "unknown")
        logger.info(
            "Connected: cluster=%r version=%s host=%s:%s prefix=%r ssl=%s",
            cluster,
            version,
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
