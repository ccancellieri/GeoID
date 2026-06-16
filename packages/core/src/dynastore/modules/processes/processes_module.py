#    Copyright 2026 FAO
#
#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at
#
#        http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS,
#    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#    See the License for the specific language governing permissions and
#    limitations under the License.
#
#    Author: Carlo Cancellieri (ccancellieri@gmail.com)
#    Company: FAO, Viale delle Terme di Caracalla, 00100 Rome, Italy
#    Contact: copyright@fao.org - http://fao.org/contact-us/terms/en/

import logging
from typing import Any, Dict, Optional
from pydantic import ValidationError

from dynastore.modules.db_config.query_executor import DbEngine
from dynastore.modules import get_protocol
from dynastore.modules.tasks.models import TaskExecutionMode
from dynastore.modules.processes import models
from dynastore.modules.processes.protocols import ProcessRegistryProtocol
from dynastore.models.auth_models import SYSTEM_USER_ID
from dynastore.models.localization import LocalizedText
from dynastore.models.protocols import CatalogsProtocol
from dynastore.tools.discovery import get_protocols
from dynastore.modules.tasks.execution import execution_engine

logger = logging.getLogger(__name__)


def resolve_stored_title(
    operator: Optional[LocalizedText],
    process_default: Optional[LocalizedText],
    generic: LocalizedText,
) -> Dict[str, str]:
    """Return a {lang: text} dict for the job title.

    Precedence: operator override -> process definition's own -> generic default.
    """
    chosen = operator or process_default or generic
    return chosen.model_dump(exclude_none=True)


def resolve_stored_description(
    operator: Optional[LocalizedText],
    process_default: Optional[LocalizedText],
) -> Optional[Dict[str, str]]:
    """Return a {lang: text} dict for the job description, or None.

    Precedence: operator override -> process definition's own.
    No generic default — when neither is provided the key is omitted.
    """
    chosen = operator or process_default
    return chosen.model_dump(exclude_none=True) if chosen is not None else None


def _is_invocable_process(process_id: str) -> bool:
    """True iff ``process_id`` maps to a registered task that ships a Process
    definition (kind=process). System tasks (kind=task) are NEVER invocable via
    the OGC Processes API — they are enqueued only by events/orchestration."""
    from dynastore.tasks import _DYNASTORE_TASKS, task_kind
    cfg = _DYNASTORE_TASKS.get(process_id)
    return cfg is not None and task_kind(cfg) == "process"


def _resolve_execution_mode(
    process: models.Process,
    preferred_mode: Optional[models.JobControlOptions],
    has_request_context: bool = False,
) -> TaskExecutionMode:
    """
    Determine execution mode from caller preference + process constraints.

    A mode is only viable if some registered runner can actually *handle this
    process* in that mode — not merely that some runner exists for the mode.
    This is what makes execution deployment-aware: e.g. ``gdal`` resolves to
    SYNCHRONOUS where the worker carries the osgeo runtime in-process, and to
    ASYNCHRONOUS where only a Cloud Run job runner can claim it. Picking a mode
    on bare existence would select an in-process runner that then can't run the
    task, surfacing as a late failure instead of correct routing.

    Raises NotImplementedError if no runner can handle the process in any
    supported mode.
    """
    candidate_modes = []
    if preferred_mode and preferred_mode in process.jobControlOptions:
        candidate_modes.append(preferred_mode)

    for option in process.jobControlOptions:
        if option not in candidate_modes:
            candidate_modes.append(option)

    for mode in candidate_modes:
        if mode == models.JobControlOptions.ASYNC_EXECUTE:
            check_mode = TaskExecutionMode.ASYNCHRONOUS
        elif mode == models.JobControlOptions.SYNC_EXECUTE:
            check_mode = TaskExecutionMode.SYNCHRONOUS
        else:
            continue

        if execution_engine.get_runners_for(
            process.id, check_mode, has_request_context=has_request_context
        ):
            return check_mode

    raise NotImplementedError(
        f"No runner can execute process '{process.id}' in any supported mode."
    )


def _validate_process_inputs(
    process: models.Process,
    execution_request: models.ExecuteRequest,
) -> None:
    """Validate inputs against the process definition's JSON Schema."""
    for input_name, input_def in process.inputs.items():
        if not input_def.schema_:
            continue
        payload = execution_request.inputs.get(input_name)
        if payload is None:
            continue
        try:
            from jsonschema import validate

            validate(instance=payload, schema=input_def.schema_)
        except Exception as e:
            try:
                from jsonschema.exceptions import (
                    ValidationError as JsonschemaValidationError,
                )

                is_jsonschema_error = isinstance(e, JsonschemaValidationError)
            except ImportError:
                is_jsonschema_error = False

            if is_jsonschema_error or isinstance(
                e, (ValidationError, ValueError)
            ):
                raise ValueError(f"Invalid input for '{input_name}': {e}") from e
            raise


async def execute_process(
    process_id: str,
    execution_request: models.ExecuteRequest,
    engine: DbEngine,
    caller_id: str = SYSTEM_USER_ID,
    preferred_mode: Optional[models.JobControlOptions] = None,
    background_tasks: Optional[Any] = None,
    catalog_id: Optional[str] = None,
    collection_id: Optional[str] = None,
    dedup_key: Optional[str] = None,
) -> Any:
    """
    Core logic for executing a process.

    Responsibilities (OGC-specific):
      1. Reject non-invocable ids (unknown / kind=task system tasks)
      2. Lookup process definition
      3. Validate inputs against JSON Schema
      4. Resolve execution mode from preference + process constraints
      5. Delegate to ExecutionEngine.execute()

    ``dedup_key``: optional idempotency token. When set, the runner passes it
    to ``TaskCreate`` so the DB partial unique index on
    ``(schema_name, dedup_key)`` for non-terminal tasks collapses redelivered
    events into a single task. Returns ``None`` on a dedup hit.
    """
    # 1. Reject anything that is not an invocable process BEFORE any lookup or
    # enqueue. This returns False both for unknown ids and for kind=task system
    # tasks (cascade_cleanup, etc.) — the latter are enqueued only by
    # events/orchestration. Raise the identical not-found error in both cases so
    # the public API never leaks the existence of a system task (404, not 403).
    if not _is_invocable_process(process_id):
        raise ValueError(f"Process '{process_id}' not found.")

    # 2. Find the requested process definition.
    process: Optional[models.Process] = None
    for registry in get_protocols(ProcessRegistryProtocol):
        process = await registry.get_process(process_id)
        if process:
            break
    if not process:
        raise ValueError(f"Process '{process_id}' not found.")

    # 3. Validate inputs.
    _validate_process_inputs(process, execution_request)

    # 4. Determine execution mode. ``background_tasks`` is the request-context
    # signal runners use to gate in-process execution, so thread it into the
    # capability check that picks the mode.
    execution_mode = _resolve_execution_mode(
        process, preferred_mode, has_request_context=background_tasks is not None
    )

    # 5. Resolve DB schema and delegate to ExecutionEngine.
    db_schema = "public"
    catalog_protocol = get_protocol(CatalogsProtocol)
    if catalog_protocol and catalog_id:
        from dynastore.models.driver_context import DriverContext
        db_schema = await catalog_protocol.resolve_physical_schema(
            catalog_id, ctx=DriverContext(db_resource=engine) if engine else None
        )

    from dynastore.models.tasks import DEFAULT_PROCESS_TITLE
    dumped = execution_request.model_dump()
    # Stamp title with precedence: operator override -> process definition's
    # own -> generic default. This ensures every job row carries a meaningful
    # human-readable label regardless of which runner claims it.
    dumped["title"] = resolve_stored_title(
        execution_request.title, process.title, DEFAULT_PROCESS_TITLE
    )
    # Stamp description with precedence: operator override -> process
    # definition's own. Omit the key entirely when neither provides one.
    _desc = resolve_stored_description(execution_request.description, process.description)
    if _desc is not None:
        dumped["description"] = _desc
    else:
        dumped.pop("description", None)

    return await execution_engine.execute(
        task_type=process_id,
        inputs=dumped,
        engine=engine,
        mode=execution_mode,
        caller_id=caller_id,
        db_schema=db_schema or "public",
        collection_id=collection_id,
        background_tasks=background_tasks,
        dedup_key=dedup_key,
    )