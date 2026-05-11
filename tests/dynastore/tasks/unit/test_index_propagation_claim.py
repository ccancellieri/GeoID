"""Phase 1 (#491) — IndexPropagationTask.can_claim predicate.

A worker without the target Indexer module in its registry must return
``False`` so the dispatcher can release the claim back to ``PENDING``
for a capable worker, instead of executing and dead-lettering.
"""
from __future__ import annotations

from typing import Sequence
from unittest.mock import patch

from dynastore.models.protocols.indexer import (
    BulkResult, IndexContext, IndexOp,
)
from dynastore.tasks.index_propagation.task import IndexPropagationTask


class _Indexer:
    """Stand-in whose snake-cased class name is the indexer_id."""

    def __init__(self, name: str) -> None:
        self.__class__.__name__ = name

    async def ensure_indexer(self, ctx: IndexContext) -> None:  # noqa: D401
        return None

    async def index(self, ctx: IndexContext, op: IndexOp) -> None:  # noqa: D401
        return None

    async def index_bulk(  # noqa: D401
        self, ctx: IndexContext, ops: Sequence[IndexOp],
    ) -> BulkResult:
        return BulkResult()


def _row(indexer_id: str) -> dict:
    return {
        "task_type": "index_propagation",
        "inputs": {
            "indexer_id": indexer_id,
            "entity_type": "item",
            "catalog": "c",
            "ops": [{"entity_id": "e", "op_type": "upsert", "payload": {}}],
        },
    }


def test_can_claim_true_when_indexer_registered() -> None:
    with patch(
        "dynastore.tasks.index_propagation.task.get_protocols",
        return_value=[_Indexer("CollectionElasticsearchDriver")],
    ):
        assert IndexPropagationTask.can_claim(
            _row("collection_elasticsearch_driver")
        ) is True


def test_can_claim_false_when_registry_empty() -> None:
    with patch(
        "dynastore.tasks.index_propagation.task.get_protocols",
        return_value=[],
    ):
        assert IndexPropagationTask.can_claim(
            _row("collection_elasticsearch_driver")
        ) is False


def test_can_claim_false_on_indexer_id_mismatch() -> None:
    with patch(
        "dynastore.tasks.index_propagation.task.get_protocols",
        return_value=[_Indexer("ItemsElasticsearchDriver")],
    ):
        assert IndexPropagationTask.can_claim(
            _row("collection_elasticsearch_driver")
        ) is False


def test_can_claim_fail_open_when_indexer_id_missing() -> None:
    """Malformed row → return True so the run path surfaces the error
    explicitly rather than the row sitting PENDING forever."""
    with patch(
        "dynastore.tasks.index_propagation.task.get_protocols",
        return_value=[_Indexer("CollectionElasticsearchDriver")],
    ):
        assert IndexPropagationTask.can_claim({
            "task_type": "index_propagation",
            "inputs": {"entity_type": "item", "catalog": "c", "ops": []},
        }) is True


def test_can_claim_accepts_object_payload() -> None:
    """Row may arrive as an object with ``.inputs``, not just a dict."""

    class _RowObj:
        inputs = {
            "indexer_id": "collection_elasticsearch_driver",
            "entity_type": "item",
            "catalog": "c",
            "ops": [],
        }

    with patch(
        "dynastore.tasks.index_propagation.task.get_protocols",
        return_value=[_Indexer("CollectionElasticsearchDriver")],
    ):
        assert IndexPropagationTask.can_claim(_RowObj()) is True
