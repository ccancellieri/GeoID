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

"""Cycle F.6 — engine_init / engine_release lifecycle on the 4 concrete
engine configs, plus snapshot resolver + DBConfigModule lifespan wiring.

Tests are unit-scoped (no real PG/ES/DuckDB/Iceberg).  Lazy-imported
libraries are mocked at the ``sys.modules`` boundary so ``engine_init``
calls hit the mock instead of the real library.
"""

from __future__ import annotations

import sys
import types
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from dynastore.modules.db_config.engine_config import (
    DuckdbEngineConfig,
    ElasticsearchEngineConfig,
    IcebergEngineConfig,
    PostgresqlEngineConfig,
)
from dynastore.modules.db_config.engine_instance_cache import (
    EngineInstanceProtocol,
)
from dynastore.modules.db_config.engine_resolver import (
    build_engine_snapshot,
    make_resolver,
)
from dynastore.tools.secrets import Secret


# ---------------------------------------------------------------------------
# All 4 engine configs implement EngineInstanceProtocol structurally.
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("cls", [
    PostgresqlEngineConfig,
    ElasticsearchEngineConfig,
    DuckdbEngineConfig,
    IcebergEngineConfig,
])
def test_engine_configs_satisfy_engine_instance_protocol(cls):
    """Every concrete engine config implements ``engine_init`` /
    ``engine_release`` so :class:`EngineInstanceCache` can use it."""
    cfg = cls()
    assert isinstance(cfg, EngineInstanceProtocol)


# ---------------------------------------------------------------------------
# PostgresqlEngineConfig
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_postgresql_engine_init_uses_dbconfig_fallback():
    """Without ``connection_url``, engine_init reads from ``DBConfig`` and
    normalises ``postgresql+asyncpg://`` → ``postgresql://``."""
    cfg = PostgresqlEngineConfig(pool_size=7, pool_timeout_sec=11)
    fake_pool = MagicMock(name="asyncpg_pool")
    fake_create = AsyncMock(return_value=fake_pool)
    fake_module = types.SimpleNamespace(create_pool=fake_create)

    with patch.dict(sys.modules, {"asyncpg": fake_module}), \
         patch(
             "dynastore.modules.db_config.db_config.DBConfig.database_url",
             "postgresql+asyncpg://u:p@h:5432/db",
         ):
        instance = await cfg.engine_init()

    assert instance is fake_pool
    kwargs = fake_create.await_args.kwargs
    assert kwargs["dsn"] == "postgresql://u:p@h:5432/db"
    assert kwargs["min_size"] == 1
    assert kwargs["max_size"] == 7
    assert kwargs["timeout"] == 11
    # #2898: every per-catalog physical engine now carries the same
    # lock-safety + clamped statement_timeout server_settings as the shared
    # serving engine (see test_postgresql_engine_server_settings.py for the
    # dedicated regression cover).
    assert "server_settings" in kwargs


@pytest.mark.asyncio
async def test_postgresql_engine_init_uses_explicit_connection_url():
    """``connection_url`` Secret takes precedence over DBConfig."""
    cfg = PostgresqlEngineConfig(
        connection_url=Secret("postgresql://override:pw@h:5432/d"),
    )
    fake_create = AsyncMock(return_value="POOL")
    fake_module = types.SimpleNamespace(create_pool=fake_create)

    with patch.dict(sys.modules, {"asyncpg": fake_module}):
        instance = await cfg.engine_init()

    assert instance == "POOL"
    kwargs = fake_create.await_args.kwargs
    assert kwargs["dsn"] == "postgresql://override:pw@h:5432/d"


@pytest.mark.asyncio
async def test_postgresql_engine_release_closes_pool():
    """``engine_release`` awaits ``pool.close()``."""
    cfg = PostgresqlEngineConfig()
    pool = MagicMock()
    pool.close = AsyncMock()

    await cfg.engine_release(pool)
    pool.close.assert_awaited_once()


@pytest.mark.asyncio
async def test_postgresql_engine_release_swallows_close_failure(caplog):
    """A failing ``close()`` is logged + swallowed (best-effort eviction)."""
    cfg = PostgresqlEngineConfig()
    pool = MagicMock()
    pool.close = AsyncMock(side_effect=RuntimeError("boom"))

    with caplog.at_level("ERROR"):
        await cfg.engine_release(pool)
    assert any("pool close raised" in rec.message for rec in caplog.records)


# ---------------------------------------------------------------------------
# ElasticsearchEngineConfig
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_elasticsearch_engine_init_returns_shared_client_when_no_override():
    """No ``cluster_url`` / ``api_key`` → reuse env-driven shared client."""
    cfg = ElasticsearchEngineConfig()
    sentinel = object()

    with patch(
        "dynastore.modules.elasticsearch.client.get_client",
        return_value=sentinel,
    ):
        instance = await cfg.engine_init()
    assert instance is sentinel


@pytest.mark.asyncio
async def test_elasticsearch_engine_init_raises_when_shared_client_uninitialised():
    """No override + no shared client → clear RuntimeError, not silent None."""
    cfg = ElasticsearchEngineConfig()

    with patch(
        "dynastore.modules.elasticsearch.client.get_client",
        return_value=None,
    ):
        with pytest.raises(RuntimeError, match="not initialised"):
            await cfg.engine_init()


@pytest.mark.asyncio
async def test_elasticsearch_engine_init_builds_dedicated_client_when_overridden():
    """``cluster_url`` + ``api_key`` Secrets construct an isolated client."""
    cfg = ElasticsearchEngineConfig(
        cluster_url=Secret("https://es.example/"),
        api_key=Secret("APIKEY"),
        request_timeout_sec=42,
    )
    fake_client = MagicMock(name="opensearch_client")

    fake_module = types.ModuleType("opensearchpy")
    fake_module.AsyncOpenSearch = MagicMock(return_value=fake_client)

    with patch.dict(sys.modules, {"opensearchpy": fake_module}):
        instance = await cfg.engine_init()

    assert instance is fake_client
    fake_module.AsyncOpenSearch.assert_called_once_with(
        timeout=42,
        hosts=["https://es.example/"],
        http_auth="APIKEY",
    )


@pytest.mark.asyncio
async def test_elasticsearch_engine_release_skips_shared_client():
    """No-override engine never owns the client — release is a no-op."""
    cfg = ElasticsearchEngineConfig()
    instance = MagicMock()
    instance.close = AsyncMock()

    await cfg.engine_release(instance)
    instance.close.assert_not_awaited()


@pytest.mark.asyncio
async def test_elasticsearch_engine_release_closes_dedicated_client():
    """Override engine owns the client — release calls close()."""
    cfg = ElasticsearchEngineConfig(cluster_url=Secret("https://es/"))
    instance = MagicMock()
    instance.close = AsyncMock()

    await cfg.engine_release(instance)
    instance.close.assert_awaited_once()


# ---------------------------------------------------------------------------
# DuckdbEngineConfig
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_duckdb_engine_init_applies_pragmas():
    """``engine_init`` opens a memory connection + applies threads / memory_limit."""
    cfg = DuckdbEngineConfig(threads=8, max_memory_gb=12)
    conn = MagicMock(name="duckdb_conn")
    fake_module = types.ModuleType("duckdb")
    fake_module.connect = MagicMock(return_value=conn)

    with patch.dict(sys.modules, {"duckdb": fake_module}):
        instance = await cfg.engine_init()

    assert instance is conn
    fake_module.connect.assert_called_once_with(":memory:")
    pragmas = [c.args[0] for c in conn.execute.call_args_list]
    assert "PRAGMA threads=8" in pragmas
    assert "PRAGMA memory_limit='12GB'" in pragmas


@pytest.mark.asyncio
async def test_duckdb_engine_release_calls_sync_close():
    """DuckDB ``close()`` is sync — engine_release calls it once, no await."""
    cfg = DuckdbEngineConfig()
    conn = MagicMock()
    conn.close = MagicMock()

    await cfg.engine_release(conn)
    conn.close.assert_called_once()


# ---------------------------------------------------------------------------
# IcebergEngineConfig
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_iceberg_engine_init_builds_properties_from_secrets():
    """``catalog_uri`` / ``warehouse_uri`` / ``catalog_properties`` Secrets
    are revealed and forwarded to ``load_catalog`` as plain strings."""
    cfg = IcebergEngineConfig(
        catalog_uri=Secret("rest://cat.example"),
        warehouse_uri=Secret("s3://wh"),
        catalog_properties={"s3.access-key-id": Secret("AKIA")},
    )
    fake_catalog = MagicMock(name="iceberg_catalog")

    fake_load = MagicMock(return_value=fake_catalog)
    fake_pyiceberg = types.ModuleType("pyiceberg")
    fake_pyiceberg_catalog = types.ModuleType("pyiceberg.catalog")
    fake_pyiceberg_catalog.load_catalog = fake_load

    with patch.dict(sys.modules, {
        "pyiceberg": fake_pyiceberg,
        "pyiceberg.catalog": fake_pyiceberg_catalog,
    }):
        instance = await cfg.engine_init()

    assert instance is fake_catalog
    fake_load.assert_called_once()
    name_arg, kwargs = fake_load.call_args.args, fake_load.call_args.kwargs
    assert name_arg[0] == cfg.__class__.class_key()
    assert kwargs == {
        "uri": "rest://cat.example",
        "warehouse": "s3://wh",
        "s3.access-key-id": "AKIA",
    }


@pytest.mark.asyncio
async def test_iceberg_engine_release_is_noop():
    """PyIceberg catalogs have no close — release returns None silently."""
    cfg = IcebergEngineConfig()
    result = await cfg.engine_release(MagicMock())
    assert result is None


# ---------------------------------------------------------------------------
# End-to-end: TTL eviction sweep fires the real engine_release
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_ttl_sweep_fires_real_engine_release():
    """End-to-end pin: a real ``DuckdbEngineConfig`` with ``ttl_lru``
    policy gets its ``engine_release`` called when the TTL expires.

    Wires F.6's engine_release impls into F.5's cache mechanics through
    a single test so a future refactor that breaks the contract surfaces
    here, not three layers deep."""
    from dynastore.modules.db_config.engine_config import EngineLifecycleConfig
    from dynastore.modules.db_config.engine_instance_cache import (
        EngineInstanceCache,
    )

    cfg = DuckdbEngineConfig(
        threads=2, max_memory_gb=1,
        lifecycle=EngineLifecycleConfig(policy="ttl_lru", ttl_seconds=30),
    )
    fake_conn = MagicMock(name="duckdb_conn")
    fake_module = types.ModuleType("duckdb")
    fake_module.connect = MagicMock(return_value=fake_conn)

    clock_value = [0.0]

    def fake_clock() -> float:
        return clock_value[0]

    cache = EngineInstanceCache(
        engine_resolver=lambda r: cfg if r == "duckdb_engine" else None,
        clock=fake_clock,
    )

    with patch.dict(sys.modules, {"duckdb": fake_module}):
        instance = await cache.get("duckdb_engine")
        assert instance is fake_conn
        # Advance past TTL.
        clock_value[0] = 60.0
        evicted = await cache.sweep()

    assert evicted == 1
    fake_conn.close.assert_called_once()
    # Subsequent get re-instantiates (sweep removed the cached entry).
    with patch.dict(sys.modules, {"duckdb": fake_module}):
        await cache.get("duckdb_engine")
    assert fake_module.connect.call_count == 2


# ---------------------------------------------------------------------------
# Engine snapshot resolver
# ---------------------------------------------------------------------------


class _StubPlatformConfigService:
    """Minimal stub satisfying ``await pcfg.get_config(cls)`` —
    returns whatever the test put in ``self._configs``, falls back to
    ``cls()`` defaults like the real service does."""

    def __init__(self, configs=None):
        self._configs = configs or {}

    async def get_config(self, cls):
        if cls in self._configs:
            return self._configs[cls]
        return cls()


@pytest.mark.asyncio
async def test_build_engine_snapshot_indexes_by_class_key_and_engine_class():
    """Snapshot is keyed under both ``class_key`` and ``engine_class``."""
    pcfg = _StubPlatformConfigService()
    snapshot = await build_engine_snapshot(pcfg)

    # Every concrete engine surfaces under both keys.
    pg_cls = PostgresqlEngineConfig
    assert pg_cls.class_key() in snapshot
    assert "postgresql_engine" in snapshot
    assert snapshot[pg_cls.class_key()] is snapshot["postgresql_engine"]

    es_cls = ElasticsearchEngineConfig
    assert es_cls.class_key() in snapshot
    assert "elasticsearch_engine" in snapshot


@pytest.mark.asyncio
async def test_build_engine_snapshot_skips_engines_that_raise(caplog):
    """A get_config failure logs + skips that engine, others still indexed."""

    class _PartiallyFailingPCfg:
        async def get_config(self, cls):
            if cls is PostgresqlEngineConfig:
                raise RuntimeError("DB unreachable")
            return cls()

    with caplog.at_level("WARNING"):
        snapshot = await build_engine_snapshot(_PartiallyFailingPCfg())

    assert PostgresqlEngineConfig.class_key() not in snapshot
    assert ElasticsearchEngineConfig.class_key() in snapshot
    assert any("failed to load" in rec.message for rec in caplog.records)


def test_make_resolver_returns_none_for_unknown_ref():
    snapshot = {"postgresql_engine": PostgresqlEngineConfig()}
    resolve = make_resolver(snapshot)
    assert resolve("postgresql_engine") is snapshot["postgresql_engine"]
    assert resolve("not_a_real_ref") is None


# ---------------------------------------------------------------------------
# DBConfigModule lifespan wiring
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_dbconfig_module_lifespan_wires_engine_cache():
    """After lifespan start, app_state.engine_cache is the live cache; on
    teardown it's closed and the reference dropped."""
    from dynastore.modules.db_config.db_config_service import DBConfigModule
    from dynastore.modules.db_config.engine_instance_cache import (
        EngineInstanceCache,
    )

    module = DBConfigModule()
    app_state = SimpleNamespace(
        engine=None, sync_engine=None, db_config=None, engine_cache=None,
    )

    # Stub PlatformConfigService so we don't touch real DB during lifespan.
    class _StubPCfgService:
        def __init__(self, *args, **kwargs):
            pass

        async def get_config(self, cls):
            return cls()

        def lifespan(self, _state):
            from contextlib import asynccontextmanager

            @asynccontextmanager
            async def _ctx():
                yield

            return _ctx()

    with patch(
        "dynastore.modules.db_config.db_config_service.PlatformConfigService",
        _StubPCfgService,
    ), patch(
        "dynastore.modules.db_config.db_config_service.register_plugin",
        lambda *a, **k: None,
    ), patch(
        "dynastore.modules.db_config.db_config_service.unregister_plugin",
        lambda *a, **k: None,
    ):
        async with module.lifespan(app_state):
            cache = app_state.engine_cache
            assert isinstance(cache, EngineInstanceCache)
            # Resolver pre-populated from the registry.
            resolved = cache._resolver(  # type: ignore[attr-defined]
                PostgresqlEngineConfig.class_key()
            )
            assert isinstance(resolved, PostgresqlEngineConfig)

        # After lifespan exit, cache is closed + cleared from app_state.
        assert app_state.engine_cache is None
        assert cache._closed is True  # type: ignore[attr-defined]
