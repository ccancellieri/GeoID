#    Copyright 2026 FAO
#    Licensed under the Apache License, Version 2.0 (the "License");

"""``index_propagation`` task — runs one ``Indexer.index(ctx, op)`` call.

Enqueued by :class:`~dynastore.modules.storage.index_dispatcher.OutboxWriter`
when an in-process indexer attempt fails under ``FailurePolicy.OUTBOX``.
The task row lives in the same ``tasks.tasks`` table as every other
durable task; the dispatcher's drain semantics (``claim_batch`` →
``complete_task`` / ``fail_task``) provide retry-with-DLQ for free.
"""

from __future__ import annotations

import logging
from typing import Any, Dict, Literal, Optional

from pydantic import BaseModel, Field

from dynastore.tasks.protocols import TaskProtocol

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Inputs
# ---------------------------------------------------------------------------


class IndexPropagationInputs(BaseModel):
    """Payload shape for ``task_type='index_propagation'`` rows.

    Mirrors the slim :class:`~dynastore.models.protocols.indexer.IndexOp`
    plus the ``(catalog, collection)`` scope and the target indexer's
    ``indexer_id`` — everything the runner needs to reconstruct the
    original :class:`IndexContext` / :class:`IndexOp` and re-dispatch.
    """

    indexer_id: str = Field(
        description="Driver identifier of the target Indexer impl.",
    )
    op_type: Literal["upsert", "delete"] = Field(
        description="Op kind to replay.",
    )
    entity_type: Literal["catalog", "collection", "item", "asset"] = Field(
        description="Tier of the indexed entity.",
    )
    entity_id: str
    catalog: str
    collection: Optional[str] = None
    payload: Optional[Dict[str, Any]] = Field(
        default=None,
        description="Document body for upsert; ``None`` for delete.",
    )
    correlation_id: Optional[str] = None
    last_error: Optional[str] = Field(
        default=None,
        description=(
            "Error reported by the original in-process attempt — kept on "
            "the task row for operator triage."
        ),
    )


# ---------------------------------------------------------------------------
# Task
# ---------------------------------------------------------------------------


class IndexPropagationTask(TaskProtocol):
    """Drain one outbox row by re-invoking the target ``Indexer``.

    The runner re-resolves the :class:`Indexer` instance from the
    protocol registry by ``indexer_id`` — so a backend swap (e.g. ES →
    OpenSearch) is transparent to in-flight outbox rows.
    """

    task_type = "index_propagation"
    priority = 50

    def is_available(self) -> bool:  # pragma: no cover — discovery hook
        return True

    async def run(self, payload: Any) -> Dict[str, Any]:
        from dynastore.models.protocols.indexer import (
            Indexer, IndexContext, IndexOp,
        )
        from dynastore.tools.discovery import get_protocols

        # ``payload.inputs`` is the JSONB blob we serialised at enqueue time.
        inputs_raw = getattr(payload, "inputs", None) or {}
        inputs = IndexPropagationInputs.model_validate(inputs_raw)

        # Resolve the target Indexer by indexer_id from the registry.
        # Snake-case class-name match (same convention as routing config).
        from dynastore.modules.storage.routing_config import _to_snake

        target: Optional[Indexer] = None
        for impl in get_protocols(Indexer):
            if _to_snake(type(impl).__name__) == inputs.indexer_id:
                target = impl
                break

        if target is None:
            # The driver named in the task no longer exists — nothing the
            # runner can do.  Surface as a permanent failure so the row
            # moves to DEAD_LETTER instead of looping.
            raise RuntimeError(
                f"index_propagation: indexer '{inputs.indexer_id}' is not "
                f"registered in this process.  Task will be dead-lettered."
            )

        ctx = IndexContext(
            catalog=inputs.catalog,
            collection=inputs.collection,
            correlation_id=inputs.correlation_id or "",
            pg_conn=None,  # drain runs outside the original caller's TX
        )
        op = IndexOp(
            op_type=inputs.op_type,
            entity_type=inputs.entity_type,
            entity_id=inputs.entity_id,
            payload=inputs.payload,
        )

        await target.index(ctx, op)

        return {
            "status": "ok",
            "indexer_id": inputs.indexer_id,
            "entity_id": inputs.entity_id,
            "op_type": inputs.op_type,
        }
