#    Copyright 2026 FAO
#    Licensed under the Apache License, Version 2.0 (the "License").

"""Admin task-dispatch surface — path-scoped task enqueue for catalog / collection.

Public API consumed by AdminService routes:
  - dispatch_catalog_task(catalog_id, action, params) -> dict
  - dispatch_collection_task(catalog_id, collection_id, action, params) -> dict
  - catalog_task_dispatch_supported_actions() -> list[str]

Each action entry in ``_ACTION_REGISTRY`` defines:
  - task_type    : the canonical OGC-Process task type string
  - catalog_only : True if the action is NOT valid on the collection route
  - build_inputs : callable(catalog_id, collection_id|None, params) -> dict

Adding a future action is a single registry entry — no new routes.
"""

from __future__ import annotations

import logging
from typing import Any, Dict, Optional

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Helpers that mirror search_service's engine-resolution pattern exactly
# ---------------------------------------------------------------------------

def _get_engine():
    from dynastore.models.protocols import DatabaseProtocol
    from dynastore.tools.discovery import get_protocol

    db = get_protocol(DatabaseProtocol)
    if not db:
        return None
    return db.engine if isinstance(db, DatabaseProtocol) else db


# ---------------------------------------------------------------------------
# Action registry
# ---------------------------------------------------------------------------

def _build_reindex_inputs(
    catalog_id: str,
    collection_id: Optional[str],
    params: Dict[str, Any],
) -> Dict[str, Any]:
    inputs: Dict[str, Any] = {"catalog_id": catalog_id}
    if collection_id is not None:
        inputs["collection_id"] = collection_id
    if params.get("driver"):
        inputs["driver"] = params["driver"]
    return inputs


def _build_backfill_inputs(
    catalog_id: str,
    collection_id: Optional[str],
    params: Dict[str, Any],
) -> Dict[str, Any]:
    # collection_id is guaranteed non-None by the registry guard before this
    # function is called; the assertion is a defensive belt-and-suspenders.
    assert collection_id is not None, "backfill_envelope_attrs requires collection_id"
    return {
        "catalog_id": catalog_id,
        "collection_id": collection_id,
        "dry_run": params.get("dry_run", False),
        "batch_size": params.get("batch_size", 500),
    }


# Registry entries — each value is a dict with:
#   task_type       : str
#   collection_only : bool  (True = not valid on the catalog-only route)
#   build_inputs    : callable
_ACTION_REGISTRY: Dict[str, Dict[str, Any]] = {
    "reindex": {
        "task_type": "elasticsearch_indexer",
        "collection_only": False,
        "build_inputs": _build_reindex_inputs,
    },
    "backfill_envelope_attrs": {
        "task_type": "envelope_attrs_backfill_collection",
        "collection_only": True,
        "build_inputs": _build_backfill_inputs,
    },
}


def catalog_task_dispatch_supported_actions() -> list:
    """Return the list of action names understood by the dispatch surface."""
    return sorted(_ACTION_REGISTRY.keys())


# ---------------------------------------------------------------------------
# Dispatch helpers (called by the route handlers in admin_service.py)
# ---------------------------------------------------------------------------

async def dispatch_catalog_task(
    catalog_id: str,
    action: str,
    params: Dict[str, Any],
) -> Dict[str, Any]:
    """Enqueue ``action`` scoped to ``catalog_id``.

    Raises :exc:`fastapi.HTTPException` 400 on unknown or collection-only actions.
    Raises :exc:`fastapi.HTTPException` 503 when the database is unavailable.
    Propagates any error from ``create_task_for_catalog``.
    """
    from fastapi import HTTPException
    from dynastore.modules.tasks import tasks_module
    from dynastore.modules.tasks.models import TaskCreate

    entry = _ACTION_REGISTRY.get(action)
    if entry is None:
        raise HTTPException(
            status_code=400,
            detail=(
                f"Unknown action {action!r}. "
                f"Supported actions: {catalog_task_dispatch_supported_actions()}."
            ),
        )
    if entry["collection_only"]:
        raise HTTPException(
            status_code=400,
            detail=(
                f"Action {action!r} requires a collection_id and is not valid "
                f"on the catalog-only route. Use "
                f"POST /admin/catalogs/{{catalog_id}}/collections/{{collection_id}}/tasks instead."
            ),
        )

    engine = _get_engine()
    if engine is None:
        raise HTTPException(status_code=503, detail="Database unavailable.")

    inputs = entry["build_inputs"](catalog_id, None, params)
    task = await tasks_module.create_task_for_catalog(
        engine=engine,
        task_data=TaskCreate(
            caller_id="system:admin",
            task_type=entry["task_type"],
            inputs=inputs,
        ),
        catalog_id=catalog_id,
    )
    if task is None:
        raise HTTPException(
            status_code=500,
            detail=(
                f"Task enqueue for action {action!r} returned None "
                "(possible dedup hit on a non-dedup task type)."
            ),
        )
    return {
        "task_id": str(task.task_id),
        "action": action,
        "target": {"catalog_id": catalog_id, "collection_id": None},
        "status": "queued",
    }


async def dispatch_collection_task(
    catalog_id: str,
    collection_id: str,
    action: str,
    params: Dict[str, Any],
) -> Dict[str, Any]:
    """Enqueue ``action`` scoped to ``catalog_id`` / ``collection_id``.

    Raises :exc:`fastapi.HTTPException` 400 on unknown actions.
    Raises :exc:`fastapi.HTTPException` 503 when the database is unavailable.
    Propagates any error from ``create_task_for_catalog``.
    """
    from fastapi import HTTPException
    from dynastore.modules.tasks import tasks_module
    from dynastore.modules.tasks.models import TaskCreate

    entry = _ACTION_REGISTRY.get(action)
    if entry is None:
        raise HTTPException(
            status_code=400,
            detail=(
                f"Unknown action {action!r}. "
                f"Supported actions: {catalog_task_dispatch_supported_actions()}."
            ),
        )

    engine = _get_engine()
    if engine is None:
        raise HTTPException(status_code=503, detail="Database unavailable.")

    inputs = entry["build_inputs"](catalog_id, collection_id, params)
    task = await tasks_module.create_task_for_catalog(
        engine=engine,
        task_data=TaskCreate(
            caller_id="system:admin",
            task_type=entry["task_type"],
            inputs=inputs,
        ),
        catalog_id=catalog_id,
    )
    if task is None:
        raise HTTPException(
            status_code=500,
            detail=(
                f"Task enqueue for action {action!r} returned None "
                "(possible dedup hit on a non-dedup task type)."
            ),
        )
    return {
        "task_id": str(task.task_id),
        "action": action,
        "target": {"catalog_id": catalog_id, "collection_id": collection_id},
        "status": "queued",
    }
