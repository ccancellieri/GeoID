"""ReindexWorker input_transformers chain.

A secondary-index WRITE entry's ``input_transformers`` is the sole trigger for
feeding a transformed envelope (#990): the worker applies the entry's chain
(resolved via ``get_protocols(EntityTransformProtocol)``) before dispatch, and
surfaces transformer failures instead of swallowing them. With no
``input_transformers`` the raw envelope is dispatched.
"""

from __future__ import annotations

from unittest.mock import AsyncMock

import pytest

from dynastore.modules.catalog.reindex_worker import ReindexWorker
from dynastore.modules.storage.routing_config import OperationDriverEntry


class MarkerTransformer:
    """EntityTransformProtocol double — stamps the envelope on index."""

    async def transform_for_index(
        self, entity, *, catalog_id, collection_id, entity_kind, ctx,
    ):
        out = dict(entity)
        out["_transformed"] = True
        out["_seen_kind"] = entity_kind
        return out

    async def restore_from_index(
        self, doc, *, catalog_id, collection_id, entity_kind, ctx,
    ):
        out = dict(doc)
        out.pop("_transformed", None)
        return out


class BoomTransformer:
    """EntityTransformProtocol double — always fails on index."""

    async def transform_for_index(
        self, entity, *, catalog_id, collection_id, entity_kind, ctx,
    ):
        raise RuntimeError("boom")

    async def restore_from_index(
        self, doc, *, catalog_id, collection_id, entity_kind, ctx,
    ):
        return doc


def _worker() -> ReindexWorker:
    return ReindexWorker(get_catalog_metadata=AsyncMock(return_value=None))


@pytest.mark.asyncio
async def test_no_input_transformers_returns_envelope_unchanged():
    entry = OperationDriverEntry(driver_ref="catalog_elasticsearch_driver")
    env = {"title": "x"}
    out = await _worker()._transform_envelope_for_entry(
        entry=entry, envelope=env, catalog_id="c1",
    )
    assert out == env


@pytest.mark.asyncio
async def test_none_envelope_returns_none():
    entry = OperationDriverEntry(
        driver_ref="catalog_elasticsearch_driver",
        input_transformers=("marker_transformer",),
    )
    out = await _worker()._transform_envelope_for_entry(
        entry=entry, envelope=None, catalog_id="c1",
    )
    assert out is None


@pytest.mark.asyncio
async def test_input_transformers_applied(monkeypatch):
    monkeypatch.setattr(
        "dynastore.tools.discovery.get_protocols",
        lambda proto: [MarkerTransformer()],
    )
    entry = OperationDriverEntry(
        driver_ref="catalog_elasticsearch_driver",
        input_transformers=("marker_transformer",),
    )
    out = await _worker()._transform_envelope_for_entry(
        entry=entry, envelope={"title": "x"}, catalog_id="c1",
    )
    assert out["_transformed"] is True
    assert out["_seen_kind"] == "catalog"
    assert out["title"] == "x"


@pytest.mark.asyncio
async def test_unresolved_ref_is_skipped_not_fatal(monkeypatch):
    monkeypatch.setattr(
        "dynastore.tools.discovery.get_protocols",
        lambda proto: [],
    )
    entry = OperationDriverEntry(
        driver_ref="catalog_elasticsearch_driver",
        input_transformers=("missing_transformer",),
    )
    env = {"title": "x"}
    out = await _worker()._transform_envelope_for_entry(
        entry=entry, envelope=env, catalog_id="c1",
    )
    assert out == env


@pytest.mark.asyncio
async def test_transformer_failure_propagates(monkeypatch):
    monkeypatch.setattr(
        "dynastore.tools.discovery.get_protocols",
        lambda proto: [BoomTransformer()],
    )
    entry = OperationDriverEntry(
        driver_ref="catalog_elasticsearch_driver",
        input_transformers=("boom_transformer",),
    )
    with pytest.raises(RuntimeError, match="boom"):
        await _worker()._transform_envelope_for_entry(
            entry=entry, envelope={"a": 1}, catalog_id="c1",
        )
