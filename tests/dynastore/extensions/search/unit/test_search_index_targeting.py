"""End-to-end index targeting through SearchService — item-only (#819).

Pins the actual index argument the service passes to ``es.search`` for
each public method. Helper-level naming contracts live in
``tests/dynastore/modules/elasticsearch/test_index_naming.py``.
"""

import pytest

from dynastore.extensions.search.search_models import SearchBody
from dynastore.extensions.search.search_service import SearchService


def _service() -> SearchService:
    return SearchService.__new__(SearchService)


def _capturing_driver():
    """Return a (fake_driver, captured) pair.

    The fake mimics the surface ``SearchService`` reaches for through
    :meth:`SearchService._resolve_items_driver` — only ``es_client`` is
    consulted, so we wrap a no-result ES double behind that property.
    """
    captured: dict = {}

    class _FakeES:
        async def search(self, *, index, body, **kwargs):
            captured["index"] = index
            captured["body"] = body
            captured["kwargs"] = kwargs
            return {"hits": {"hits": [], "total": {"value": 0}}}

    class _FakeDriver:
        es_client = _FakeES()

    return _FakeDriver(), captured


@pytest.fixture(autouse=True)
def _patch_index_prefix(monkeypatch):
    monkeypatch.setattr(
        "dynastore.modules.elasticsearch.client.get_index_prefix",
        lambda: "test",
    )


async def test_search_items_scoped_targets_per_catalog_index(monkeypatch):
    svc = _service()
    driver, captured = _capturing_driver()
    monkeypatch.setattr(svc, "_resolve_items_driver", lambda: driver)

    await svc.search_items(SearchBody(catalog_id="acme", limit=10))

    assert captured["index"] == "test-acme-items"


async def test_search_items_unscoped_targets_public_alias(monkeypatch):
    svc = _service()
    driver, captured = _capturing_driver()
    monkeypatch.setattr(svc, "_resolve_items_driver", lambda: driver)

    await svc.search_items(SearchBody(catalog_id=None, limit=10))

    assert captured["index"] == "test-items"


async def test_search_items_uses_ignore_unavailable(monkeypatch):
    svc = _service()
    driver, captured = _capturing_driver()
    monkeypatch.setattr(svc, "_resolve_items_driver", lambda: driver)

    await svc.search_items(SearchBody(catalog_id="missing-catalog", limit=10))

    assert captured["kwargs"].get("ignore_unavailable") is True
