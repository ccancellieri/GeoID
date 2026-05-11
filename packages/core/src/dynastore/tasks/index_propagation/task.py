#    Copyright 2026 FAO
#    Licensed under the Apache License, Version 2.0 (the "License");

"""``index_propagation`` task — runs one ``Indexer.index_bulk(ctx, ops)`` call.

Enqueued by :class:`~dynastore.modules.storage.index_dispatcher.OutboxWriter`
when an in-process indexer attempt fails under ``FailurePolicy.OUTBOX``.
The task row lives in the same ``tasks.tasks`` table as every other
durable task; the dispatcher's drain semantics (``claim_batch`` →
``complete_task`` / ``fail_task``) provide retry-with-DLQ for free.

The inputs always carry a list of ops — a single-row replay is just a
list of length 1.  Legacy scalar rows still in flight at upgrade time
are lifted into the list shape by a ``model_validator(mode='before')``
shim; the shim is removable once the queue has fully drained (see
follow-up issue #505).
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Literal, Optional

from pydantic import BaseModel, Field, model_validator

from dynastore.tasks.protocols import TaskProtocol
from dynastore.tools.discovery import get_protocols

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Inputs
# ---------------------------------------------------------------------------


class OpRecord(BaseModel):
    """One indexed op in an :class:`IndexPropagationInputs` batch."""

    entity_id: str
    op_type: Literal["upsert", "delete"]
    payload: Optional[Dict[str, Any]] = Field(
        default=None,
        description="Document body for upsert; ``None`` for delete.",
    )


class IndexPropagationInputs(BaseModel):
    """Payload shape for ``task_type='index_propagation'`` rows.

    The runner re-resolves the :class:`Indexer` impl from
    ``indexer_id`` and calls :meth:`Indexer.index_bulk` with the
    reconstructed list of :class:`IndexOp`.  ``entity_type``,
    ``catalog``, ``collection``, and ``correlation_id`` are shared
    across all ops in the row (they pin the dispatch scope).
    """

    indexer_id: str = Field(
        description="Driver identifier of the target Indexer impl.",
    )
    entity_type: Literal["catalog", "collection", "item", "asset"] = Field(
        description="Tier of the indexed entity (shared by all ops).",
    )
    catalog: str
    collection: Optional[str] = None
    ops: List[OpRecord] = Field(
        default_factory=list,
        description="One or more ops to replay against the target Indexer.",
    )
    correlation_id: Optional[str] = None
    last_error: Optional[str] = Field(
        default=None,
        description=(
            "Error reported by the original in-process attempt — kept on "
            "the task row for operator triage."
        ),
    )

    @model_validator(mode="before")
    @classmethod
    def _lift_legacy_scalar_shape(cls, data: Any) -> Any:
        """Legacy rows wrote ``op_type`` / ``entity_id`` / ``payload`` at the
        top level.  Lift those into ``ops=[{...}]`` so a single-row replay
        re-uses the bulk path with a list of length 1.
        """
        if not isinstance(data, dict):
            return data
        if data.get("ops"):
            return data
        op_type = data.pop("op_type", None)
        entity_id = data.pop("entity_id", None)
        payload = data.pop("payload", None)
        if op_type is not None and entity_id is not None:
            data["ops"] = [{
                "entity_id": entity_id,
                "op_type": op_type,
                "payload": payload,
            }]
        return data


# ---------------------------------------------------------------------------
# Task
# ---------------------------------------------------------------------------


def _registered_indexer_ids() -> List[str]:
    """Snake-cased class names of every :class:`Indexer` registered in this
    process.  Used by both :meth:`IndexPropagationTask.can_claim` and
    :meth:`IndexPropagationTask.run`.
    """
    from dynastore.models.protocols.indexer import Indexer
    from dynastore.modules.storage.routing_config import _to_snake

    return [_to_snake(type(impl).__name__) for impl in get_protocols(Indexer)]


class IndexPropagationTask(TaskProtocol):
    """Drain one outbox row by re-invoking the target ``Indexer.index_bulk``.

    The runner re-resolves the :class:`Indexer` instance from the
    protocol registry by ``indexer_id`` — so a backend swap (e.g. ES →
    OpenSearch) is transparent to in-flight outbox rows.
    """

    task_type = "index_propagation"
    priority = 50

    def is_available(self) -> bool:  # pragma: no cover — discovery hook
        return True

    @classmethod
    def can_claim(cls, payload: Any) -> bool:
        """Return ``True`` iff the target :class:`Indexer` is registered
        in this process.

        Lets workers without the required Indexer module leave the row
        ``PENDING`` for a capable worker to claim, instead of dead-lettering
        when the runtime resolution fails (see #491).
        """
        try:
            inputs_raw = (payload.get("inputs") if isinstance(payload, dict)
                          else getattr(payload, "inputs", None)) or {}
            indexer_id = inputs_raw.get("indexer_id") if isinstance(inputs_raw, dict) else None
            if not indexer_id:
                # Malformed row — let the runner fail it explicitly with a
                # meaningful error rather than silently leaving it PENDING.
                return True
            return indexer_id in _registered_indexer_ids()
        except Exception:  # noqa: BLE001 — fail-open to preserve legacy behaviour
            logger.warning(
                "IndexPropagationTask.can_claim: predicate raised; defaulting "
                "to claimable so the run path can surface the error.",
                exc_info=True,
            )
            return True

    async def run(self, payload: Any) -> Dict[str, Any]:
        from dynastore.models.protocols.indexer import (
            Indexer, IndexContext, IndexOp,
        )
        from dynastore.modules.storage.routing_config import _to_snake

        # ``payload.inputs`` is the JSONB blob we serialised at enqueue time.
        inputs_raw = getattr(payload, "inputs", None) or {}
        inputs = IndexPropagationInputs.model_validate(inputs_raw)

        # Resolve the target Indexer by indexer_id from the registry.
        # Snake-case class-name match (same convention as routing config).
        target: Optional[Indexer] = None
        for impl in get_protocols(Indexer):
            if _to_snake(type(impl).__name__) == inputs.indexer_id:
                target = impl
                break

        if target is None:
            # Dispatcher's claim-predicate should have kept the row PENDING.
            # If we reach this branch, the predicate either fell through
            # (fail-open) or the registry changed mid-claim.  Surface as
            # permanent failure so the row moves to DEAD_LETTER instead of
            # looping.
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
        ops = [
            IndexOp(
                op_type=rec.op_type,
                entity_type=inputs.entity_type,
                entity_id=rec.entity_id,
                payload=rec.payload,
            )
            for rec in inputs.ops
        ]

        result = await target.index_bulk(ctx, ops)

        return {
            "status": "ok" if result.failed == 0 else "partial",
            "indexer_id": inputs.indexer_id,
            "total": result.total,
            "succeeded": result.succeeded,
            "failed": result.failed,
            "failures": result.failures,
        }
