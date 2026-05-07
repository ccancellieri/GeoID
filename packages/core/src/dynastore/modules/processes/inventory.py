#    Copyright 2025 FAO
#
#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at
#
#        http://www.apache.org/licenses/LICENSE-2.0
"""Scope-agnostic process inventory.

Shared helpers used by the OGC ``/processes`` (and scoped siblings) list
endpoints when callers opt into the non-OGC ``scope`` / ``typology`` /
``runner`` query parameters. Keeps the extra work out of the route bodies
and lets the same logic back future clients (admin panels, notebooks).
"""
from __future__ import annotations

import logging
from typing import Dict, Iterable, List, Literal, Optional, Set

from fastapi import Request

from dynastore.models.protocols.asset_process import (
    AssetProcessProtocol,
    describe_asset_process_static,
)
from dynastore.models.tasks import TaskExecutionMode
from dynastore.modules.processes import models
from dynastore.modules.processes.protocols import ProcessRegistryProtocol
from dynastore.modules.tasks.runners import RunnerProtocol
from dynastore.tools.discovery import get_protocols

logger = logging.getLogger(__name__)


# Parametric URL templates per scope. Post-realignment the ASSET template
# follows the OGC `/processes/{process_id}/execution` convention.
_SCOPE_TEMPLATES = {
    models.ProcessScope.PLATFORM:
        "/processes/{process_id}/execution",
    models.ProcessScope.CATALOG:
        "/catalogs/{catalog_id}/processes/{process_id}/execution",
    models.ProcessScope.COLLECTION:
        "/catalogs/{catalog_id}/collections/{collection_id}"
        "/processes/{process_id}/execution",
    models.ProcessScope.ASSET:
        "/catalogs/{catalog_id}/collections/{collection_id}"
        "/assets/{asset_id}/processes/{process_id}/execution",
}

_ASSET_PROCESS_TYPOLOGY = models.ProcessTypology(
    runner_type="asset_process",
    mode=TaskExecutionMode.SYNCHRONOUS,
    priority=0,
    location="in_process",
)

# Runner -> execution location. Anything not listed defaults to "in_process".
_RUNNER_LOCATIONS: Dict[str, Literal["in_process", "cloud_run"]] = {
    "gcp_cloud_run": "cloud_run",
}


def _substitute(template: str, **overrides: Optional[str]) -> str:
    """Substitute placeholder tokens with concrete values when present.

    Unresolved placeholders remain in the returned template so callers can
    tell the URL apart from a fully-qualified one.
    """
    out = template
    for key, value in overrides.items():
        if value is None:
            continue
        out = out.replace("{" + key + "}", value)
    return out


def build_url_templates(
    process: models.Process,
    *,
    catalog_id: Optional[str] = None,
    collection_id: Optional[str] = None,
    asset_id: Optional[str] = None,
) -> List[models.ProcessUrlTemplate]:
    """Return one ``ProcessUrlTemplate`` per declared scope on ``process``."""
    templates: List[models.ProcessUrlTemplate] = []
    for scope in process.scopes:
        raw = _SCOPE_TEMPLATES[scope]
        url = _substitute(
            raw,
            catalog_id=catalog_id,
            collection_id=collection_id,
            asset_id=asset_id,
            process_id=process.id,
        )
        templates.append(
            models.ProcessUrlTemplate(
                scope=scope,
                method="POST",
                url_template=url,
                rel="execute",
            )
        )
    return templates


def resolve_typologies(task_type: str) -> List[models.ProcessTypology]:
    """Return the runners capable of executing ``task_type``, priority-desc.

    Iterates registered ``RunnerProtocol`` implementations and asks each
    ``can_handle(task_type)``. The first element of the returned list is
    the runner the dispatcher would pick by default.
    """
    runners: List[RunnerProtocol] = list(get_protocols(RunnerProtocol))
    capable = [r for r in runners if _safe_can_handle(r, task_type)]
    capable.sort(key=lambda r: getattr(r, "priority", 0), reverse=True)
    return [
        models.ProcessTypology(
            runner_type=getattr(r, "runner_type", "unknown"),
            mode=getattr(r, "mode", TaskExecutionMode.ASYNCHRONOUS),
            priority=getattr(r, "priority", 0),
            location=_RUNNER_LOCATIONS.get(
                getattr(r, "runner_type", "unknown"), "in_process"
            ),
        )
        for r in capable
    ]


def _safe_can_handle(runner: RunnerProtocol, task_type: str) -> bool:
    try:
        return bool(runner.can_handle(task_type))
    except Exception as e:
        logger.warning(
            f"Runner {type(runner).__name__}.can_handle({task_type!r}) raised: {e}"
        )
        return False


def _asset_process_as_process(process: AssetProcessProtocol) -> models.Process:
    """Synthesise a ``Process`` entry from an ``AssetProcessProtocol``.

    Asset processes don't register OGC-style ``Process`` definitions because
    they execute through the parametric asset surface. Building an ephemeral
    ``Process`` lets the inventory merge them into the same listing without
    touching the OGC registry.
    """
    descriptor = describe_asset_process_static(process)
    return models.Process(
        id=descriptor.process_id,
        title=descriptor.title or descriptor.process_id,
        description=descriptor.description,
        version="1.0.0",
        scopes=[models.ProcessScope.ASSET],
        jobControlOptions=[models.JobControlOptions.SYNC_EXECUTE],
        outputTransmission=[models.TransmissionMode.VALUE],
        inputs={},
        outputs={},
        links=[],
    )


def _matches_scope_filter(
    scopes: Iterable[models.ProcessScope],
    scope_filter: Optional[Set[models.ProcessScope]],
) -> bool:
    if not scope_filter:
        return True
    return any(s in scope_filter for s in scopes)


def _matches_runner_filter(
    typologies: Iterable[models.ProcessTypology],
    runner_filter: Optional[Set[str]],
) -> bool:
    if not runner_filter:
        return True
    return any(t.runner_type in runner_filter for t in typologies)


def parse_scope_filter(raw: Optional[str]) -> Optional[Set[models.ProcessScope]]:
    """Parse the ``?scope=a,b`` query value. ``None`` / ``"all"`` → no filter."""
    if raw is None:
        return None
    tokens = [t.strip().lower() for t in raw.split(",") if t.strip()]
    if not tokens or "all" in tokens:
        return None
    try:
        return {models.ProcessScope(t) for t in tokens}
    except ValueError as e:
        raise ValueError(
            f"Invalid scope filter {raw!r}: {e}. "
            f"Expected comma-separated values in "
            f"{[s.value for s in models.ProcessScope]} or 'all'."
        )


def parse_runner_filter(raw: Optional[str]) -> Optional[Set[str]]:
    """Parse the ``?runner=a,b`` query value. ``None`` → no filter."""
    if raw is None:
        return None
    tokens = {t.strip() for t in raw.split(",") if t.strip()}
    return tokens or None


async def build_process_inventory_entries(
    request: Request,
    *,
    catalog_id: Optional[str] = None,
    collection_id: Optional[str] = None,
    asset_id: Optional[str] = None,
    scope_filter: Optional[Set[models.ProcessScope]] = None,
    runner_filter: Optional[Set[str]] = None,
    include_typology: bool = True,
) -> List[models.ProcessSummary]:
    """Build the enriched process list used by ``GET /processes`` + siblings.

    - Queries all ``ProcessRegistryProtocol`` implementations (dedup by id).
    - When ``scope=asset`` / ``scope=all``, also includes
      ``AssetProcessProtocol`` implementations as synthetic entries.
    - Applies scope + runner filters.
    - Sets ``typologies`` / ``url_templates`` when ``include_typology=True``;
      leaves them empty otherwise (strict-OGC payload).
    """
    seen_ids: Set[str] = set()
    processes: List[models.Process] = []
    for registry in get_protocols(ProcessRegistryProtocol):
        for p in await registry.list_processes():
            if p.id not in seen_ids:
                processes.append(p)
                seen_ids.add(p.id)

    asset_wanted = scope_filter is None or models.ProcessScope.ASSET in scope_filter
    if asset_wanted:
        for ap in get_protocols(AssetProcessProtocol):
            try:
                processes.append(_asset_process_as_process(ap))
            except Exception as e:
                logger.warning(
                    f"Skipping AssetProcessProtocol {type(ap).__name__}: {e}"
                )

    entries: List[models.ProcessSummary] = []
    for process in processes:
        if not _matches_scope_filter(process.scopes, scope_filter):
            continue

        is_asset_process = process.scopes == [models.ProcessScope.ASSET]
        if is_asset_process:
            typologies = [_ASSET_PROCESS_TYPOLOGY]
        else:
            typologies = resolve_typologies(process.id)

        if not _matches_runner_filter(typologies, runner_filter):
            continue

        url_templates = build_url_templates(
            process,
            catalog_id=catalog_id,
            collection_id=collection_id,
            asset_id=asset_id,
        )

        summary_dict = process.model_dump(by_alias=True)
        summary_dict["typologies"] = (
            [t.model_dump() for t in typologies] if include_typology else []
        )
        summary_dict["url_templates"] = (
            [u.model_dump() for u in url_templates] if include_typology else []
        )
        entries.append(models.ProcessSummary.model_validate(summary_dict))

    return entries
