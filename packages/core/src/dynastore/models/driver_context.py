#    Copyright 2025 FAO
#
#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at
#
#        http://www.apache.org/licenses/LICENSE-2.0

"""
Typed per-call context for driver / protocol operations.

Replaces the weakly-typed `db_resource: Optional[Any]` and `processing_context:
Optional[Dict[str, Any]]` pair that currently thread through ~467 call sites.

See Part F of plan `lovely-twirling-pebble.md` for migration strategy.
"""

from typing import Any, Dict, Literal, Optional

from pydantic import BaseModel, ConfigDict, Field

from dynastore.modules.db_config.query_executor import DbResource


OperationKind = Literal["insert", "update", "upsert", "delete"]


class ProcessingHints(BaseModel):
    """Caller intent — temporal window, identity, operation mode.

    Kept separate from `DriverContext.db_resource` because semantics differ:
    `db_resource` is a stateful transaction handle bound to
    `managed_transaction`; hints are per-call caller intent with no lifetime.
    """

    asset_id: Optional[str] = None
    caller_id: Optional[str] = None
    valid_from: Optional[Any] = None
    valid_to: Optional[Any] = None
    operation: Optional[OperationKind] = None

    model_config = ConfigDict(extra="ignore")


class DriverContext(BaseModel):
    """Typed per-call context threaded through protocols and drivers.

    Slots
    -----
    db_resource
        PG-only stateful handle (Engine | AsyncEngine | Session | …).
        Non-PG drivers ignore; accepted for interface compliance.
    processing
        Caller intent (asset_id, caller_id, temporal window, operation).
    sidecar_data
        Namespaced blackboard — mirrors `FeaturePipelineContext._sidecar_store`.
        Sidecars publish under their own key; downstream sidecars read.
    extensions
        Driver-specific escape hatch (ES refresh mode, Iceberg catalog
        override, DuckDB pragma, …). Kept explicit so grep-ability survives.
    """

    db_resource: Optional[DbResource] = None
    processing: Optional[ProcessingHints] = None
    sidecar_data: Dict[str, Any] = Field(default_factory=dict)
    extensions: Dict[str, Any] = Field(default_factory=dict)

    model_config = ConfigDict(arbitrary_types_allowed=True)


def resolve_ctx(
    ctx: Optional["DriverContext"] = None,
    *,
    db_resource: Optional[DbResource] = None,
    processing_context: Optional[Dict[str, Any]] = None,
) -> "DriverContext":
    """Normalise legacy `db_resource` / `processing_context` kwargs into a `DriverContext`.

    Used at service-layer entry points during the one-release co-existence window.
    If `ctx` is provided it wins; otherwise a fresh `DriverContext` is built from
    the legacy kwargs.
    """
    if ctx is not None:
        return ctx
    hints: Optional[ProcessingHints] = None
    if processing_context:
        hints = ProcessingHints.model_validate(
            {k: v for k, v in processing_context.items() if k in ProcessingHints.model_fields}
        )
    return DriverContext(db_resource=db_resource, processing=hints)


__all__ = ["DriverContext", "ProcessingHints", "OperationKind", "resolve_ctx"]
