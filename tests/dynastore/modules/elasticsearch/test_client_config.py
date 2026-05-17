"""Tests for `ElasticsearchClientConfig` and `_build_client` wiring (#616).

PR #612 dropped a per-call `request_timeout=60` kwarg on `es.bulk(...)`.
The replacement is a client-construction-time transport config routed
through the standard PluginConfig waterfall — these tests pin:

* defaults restore the historical 60s budget (and 10 connections / 3 retries),
* validation rejects out-of-range values,
* `_build_client` forwards the live config to AsyncOpenSearch kwargs,
* `load()` falls back to defaults when the protocol layer is missing or errors.
"""
from __future__ import annotations

from typing import Optional, Type

import pytest

from dynastore.modules.db_config.plugin_config import PluginConfig
from dynastore.modules.elasticsearch import client as es_client_mod
from dynastore.modules.elasticsearch.client_config import (
    ElasticsearchClientConfig,
    load as load_client_config,
)


class _StubPlatformConfigsProtocol:
    is_platform_manager = True

    def __init__(self, cfg: Optional[ElasticsearchClientConfig]) -> None:
        self._cfg = cfg

    async def get_config(
        self,
        config_cls: Type[PluginConfig],
        ctx=None,
    ) -> PluginConfig:
        if self._cfg is None or config_cls is not ElasticsearchClientConfig:
            return ElasticsearchClientConfig()
        return self._cfg

    async def set_config(self, *a, **kw) -> None: ...
    async def list_configs(self): return {}


def test_defaults_match_historical_bulk_budget():
    cfg = ElasticsearchClientConfig()
    assert cfg.request_timeout_seconds == 60
    assert cfg.connections_per_node == 10
    assert cfg.max_retries == 3


@pytest.mark.parametrize(
    "field,bad",
    [
        ("request_timeout_seconds", 0),
        ("request_timeout_seconds", 601),
        ("connections_per_node", 0),
        ("connections_per_node", 201),
        ("max_retries", -1),
        ("max_retries", 11),
    ],
)
def test_out_of_range_rejected(field, bad):
    with pytest.raises(Exception):
        ElasticsearchClientConfig(**{field: bad})


def test_build_client_forwards_config(monkeypatch):
    captured: dict = {}

    class _FakeAsyncOpenSearch:
        def __init__(self, **kwargs):
            captured.update(kwargs)

    monkeypatch.setattr(es_client_mod, "AsyncOpenSearch", _FakeAsyncOpenSearch)
    cfg = ElasticsearchClientConfig(
        request_timeout_seconds=120,
        connections_per_node=25,
        max_retries=5,
    )
    es_client_mod._build_client(cfg)
    assert captured["timeout"] == 120
    assert captured["maxsize"] == 25
    assert captured["max_retries"] == 5
    assert captured["retry_on_timeout"] is True


@pytest.mark.asyncio
async def test_load_falls_back_when_no_protocol(monkeypatch):
    from dynastore.tools import discovery
    monkeypatch.setattr(discovery, "get_protocol", lambda *a, **kw: None)
    cfg = await load_client_config()
    assert cfg.request_timeout_seconds == 60
    assert cfg.connections_per_node == 10


@pytest.mark.asyncio
async def test_load_uses_live_config(monkeypatch):
    stub = _StubPlatformConfigsProtocol(
        ElasticsearchClientConfig(request_timeout_seconds=90, connections_per_node=20)
    )
    from dynastore.tools import discovery
    from dynastore.models.protocols import platform_configs as pc_mod

    def fake_get_protocol(proto, *a, **kw):
        if proto is pc_mod.PlatformConfigsProtocol:
            return stub
        return None

    monkeypatch.setattr(discovery, "get_protocol", fake_get_protocol)
    cfg = await load_client_config()
    assert cfg.request_timeout_seconds == 90
    assert cfg.connections_per_node == 20


@pytest.mark.asyncio
async def test_load_falls_back_on_protocol_error(monkeypatch):
    class _Boom:
        is_platform_manager = True
        async def get_config(self, *a, **kw):
            raise RuntimeError("boom")
        async def set_config(self, *a, **kw): ...
        async def list_configs(self): return {}

    from dynastore.tools import discovery
    from dynastore.models.protocols import platform_configs as pc_mod
    monkeypatch.setattr(
        discovery,
        "get_protocol",
        lambda p, *a, **kw: _Boom() if p is pc_mod.PlatformConfigsProtocol else None,
    )
    cfg = await load_client_config()
    assert cfg.request_timeout_seconds == 60
