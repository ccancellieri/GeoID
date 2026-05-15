"""Regression coverage for #803: SearchService no longer hardcodes the
Elasticsearch client and routes through the platform-registered items
driver — honoring ``ItemsRoutingConfig.operations[SEARCH]`` pins via
:func:`get_search_driver` when a single collection is in scope.
"""

from __future__ import annotations

import inspect

import pytest

from dynastore.extensions.search.search_models import SearchBody
from dynastore.extensions.search.search_service import SearchService


# ---------------------------------------------------------------------------
# Source-shape pins
# ---------------------------------------------------------------------------

def test_search_service_module_does_not_import_get_client() -> None:
    """Forbid the direct ``get_client`` import — the search extension must
    reach the ES engine through the resolved items driver instance, not
    via the module-level singleton accessor.
    """
    import dynastore.extensions.search.search_service as svc_module

    src = inspect.getsource(svc_module)
    assert (
        "from dynastore.modules.elasticsearch.client import get_client"
        not in src
    ), (
        "SearchService re-introduced a direct import of "
        "dynastore.modules.elasticsearch.client.get_client. #803 retired "
        "that bypass — reach the ES engine via "
        "_resolve_items_driver().es_client instead."
    )


def test_search_service_module_does_not_import_get_search_index() -> None:
    """``mappings.get_search_index`` was the dispatch helper retired in
    #803. The SearchService now derives the index inline."""
    import dynastore.extensions.search.search_service as svc_module

    src = inspect.getsource(svc_module)
    assert "get_search_index" not in src, (
        "SearchService re-introduced get_search_index — the helper was "
        "retired with #803. Use get_tenant_items_index / "
        "get_public_items_alias / get_index_name directly per call site."
    )


def test_get_search_index_helper_is_retired() -> None:
    """The :func:`get_search_index` dispatch helper had a single caller
    (``SearchService``) and shipped a dead ``{prefix}-items`` global alias
    branch that bypassed routing. It is gone."""
    from dynastore.modules.elasticsearch import mappings

    assert not hasattr(mappings, "get_search_index"), (
        "mappings.get_search_index was re-introduced. The retired helper "
        "papered over the items-routing bypass diagnosed in #803."
    )


# ---------------------------------------------------------------------------
# Routing dispatch
# ---------------------------------------------------------------------------

async def test_routing_driver_hint_consulted_for_scoped_search(monkeypatch):
    """When ``catalog_id`` + a single ``collection`` are in scope,
    SearchService consults :func:`get_search_driver` so an operator-pinned
    ``ItemsRoutingConfig.operations[SEARCH]`` entry is honored. The hint
    flows from ``SearchBody.driver`` through to that resolver call."""
    captured: dict = {}

    async def _fake_get_search_driver(catalog_id, *, entity, collection_id, driver_hint):
        captured["catalog_id"] = catalog_id
        captured["entity"] = entity
        captured["collection_id"] = collection_id
        captured["driver_hint"] = driver_hint
        return "items_elasticsearch_private_driver"

    monkeypatch.setattr(
        "dynastore.modules.storage.routing_config.get_search_driver",
        _fake_get_search_driver,
    )
    monkeypatch.setattr(
        "dynastore.modules.elasticsearch.client.get_index_prefix",
        lambda: "test",
    )

    svc = SearchService.__new__(SearchService)
    ref = await svc._resolve_items_search_driver_ref(
        catalog_id="acme",
        collections=["adm2"],
        driver_hint="items_elasticsearch_private_driver",
    )

    assert ref == "items_elasticsearch_private_driver"
    assert captured == {
        "catalog_id": "acme",
        "entity": "item",
        "collection_id": "adm2",
        "driver_hint": "items_elasticsearch_private_driver",
    }


async def test_routing_driver_ref_skipped_when_no_single_collection(monkeypatch):
    """Routing-config is keyed at (catalog, collection); cross-collection
    or unscoped queries cannot resolve via :func:`get_search_driver` and
    fall through to the platform default driver."""
    called = False

    async def _fake_get_search_driver(*args, **kwargs):
        nonlocal called
        called = True
        return "should-not-be-called"

    monkeypatch.setattr(
        "dynastore.modules.storage.routing_config.get_search_driver",
        _fake_get_search_driver,
    )

    svc = SearchService.__new__(SearchService)

    assert await svc._resolve_items_search_driver_ref(
        catalog_id="acme", collections=None, driver_hint=None,
    ) is None
    assert await svc._resolve_items_search_driver_ref(
        catalog_id="acme", collections=["a", "b"], driver_hint=None,
    ) is None
    assert await svc._resolve_items_search_driver_ref(
        catalog_id=None, collections=["only-collection"], driver_hint=None,
    ) is None
    assert called is False


async def test_search_body_carries_driver_hint() -> None:
    """``SearchBody.driver`` was added in #803 so the operator-facing
    request can pin a routing-config entry. Pre-#803 the field did not
    exist; this guards against a model regression."""
    body = SearchBody(catalog_id="acme", collections=["adm2"], driver="items_elasticsearch_private_driver")
    assert body.driver == "items_elasticsearch_private_driver"
    # Default still optional / None — no behavior change for callers
    # that don't pin.
    assert SearchBody().driver is None


# ---------------------------------------------------------------------------
# Driver-instance discovery
# ---------------------------------------------------------------------------

def test_resolve_items_driver_raises_without_registration(monkeypatch) -> None:
    """When :class:`ItemsElasticsearchDriver` is not registered (e.g.
    storage drivers never loaded), :meth:`SearchService._resolve_items_driver`
    raises a clear ``RuntimeError`` rather than returning ``None`` and
    crashing on the first attribute access."""
    monkeypatch.setattr(
        "dynastore.tools.discovery.get_protocol",
        lambda *args, **kwargs: None,
    )
    svc = SearchService.__new__(SearchService)
    with pytest.raises(RuntimeError, match="no ItemsElasticsearchDriver"):
        svc._resolve_items_driver()


def test_items_driver_exposes_es_client_property() -> None:
    """The driver instance exposes ``es_client`` as the public access
    point to the platform ES engine. SearchService reaches the engine
    via this property, so removing it would break the #803 wire-up."""
    from dynastore.modules.storage.drivers.elasticsearch import (
        ItemsElasticsearchDriver,
    )

    assert isinstance(
        inspect.getattr_static(ItemsElasticsearchDriver, "es_client"),
        property,
    )
