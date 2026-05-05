from __future__ import annotations
import logging
import sys
from typing import Protocol, runtime_checkable


def test_es_driver_is_available_returns_false_when_opensearchpy_missing(monkeypatch):
    from dynastore.modules.storage.drivers.elasticsearch import ItemsElasticsearchDriver

    class _ConcreteES(ItemsElasticsearchDriver):
        def get_config_service(self):  # stub for pyright; not invoked by is_available
            return None

    monkeypatch.setitem(sys.modules, "dynastore.modules.elasticsearch.client", None)
    assert _ConcreteES().is_available() is False


def test_es_private_driver_is_available_returns_false_when_opensearchpy_missing(monkeypatch):
    from dynastore.modules.storage.drivers.elasticsearch_private.driver import (
        ItemsElasticsearchPrivateDriver,
    )

    class _ConcretePrivate(ItemsElasticsearchPrivateDriver):
        def get_config_service(self):
            return None

    monkeypatch.setitem(sys.modules, "dynastore.modules.elasticsearch.client", None)
    assert _ConcretePrivate().is_available() is False


def test_misbehaving_driver_does_not_break_registry(caplog):
    from dynastore.tools.discovery import (
        get_protocols, register_plugin, unregister_plugin, _get_protocols_cached,
    )

    @runtime_checkable
    class _Avail(Protocol):
        def is_available(self) -> bool: ...

    class GoodDriver:
        def is_available(self) -> bool: return True

    class BadDriver:
        def is_available(self) -> bool: raise RuntimeError("synthetic")

    good, bad = GoodDriver(), BadDriver()
    register_plugin(good); register_plugin(bad)
    try:
        _get_protocols_cached.cache_clear()
        with caplog.at_level(logging.WARNING, logger="dynastore.tools.discovery"):
            result = get_protocols(_Avail)
    finally:
        unregister_plugin(good); unregister_plugin(bad)
        _get_protocols_cached.cache_clear()

    assert good in result and bad not in result
    assert any("synthetic" in r.message for r in caplog.records)
