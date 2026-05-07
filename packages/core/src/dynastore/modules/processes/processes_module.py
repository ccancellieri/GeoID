#    Copyright 2025 FAO
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
from typing import Any, Optional
from pydantic import ValidationError

from dynastore.modules.db_config.query_executor import DbEngine
from dynastore.modules import get_protocol
from dynastore.modules.tasks import runners
from dynastore.modules.tasks.models import TaskExecutionMode
from dynastore.modules.processes import models
from dynastore.modules.processes.protocols import ProcessRegistryProtocol
from dynastore.models.auth_models import SYSTEM_USER_ID
from dynastore.models.protocols import CatalogsProtocol
from dynastore.tools.discovery import get_protocols
from dynastore.modules.tasks.execution import execution_engine

logger = logging.getLogger(__name__)


def _resolve_execution_mode(
    process: models.Process,
    preferred_mode: Optional[models.JobControlOptions],
) -> TaskExecutionMode:
    """
    Determine execution mode from caller preference + process constraints.

    Raises NotImplementedError if no runners are available for any
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

        if runners.get_runners(check_mode):
            return check_mode

    raise NotImplementedError(
        f"No available execution mode for process '{process.id}'."
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
                raise ValueError(f"Invalid input for '{input_name}': {e}")
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
) -> Any:
    """
    Core logic for executing a process.

    Responsibilities (OGC-specific):
      1. Lookup process definition
      2. Validate inputs against JSON Schema
      3. Resolve execution mode from preference + process constraints
      4. Delegate to ExecutionEngine.execute()
    """
    # 1. Find the requested process definition.
    process: Optional[models.Process] = None
    for registry in get_protocols(ProcessRegistryProtocol):
        process = await registry.get_process(process_id)
        if process:
            break
    if not process:
        raise ValueError(f"Process '{process_id}' not found.")

    # 2. Validate inputs.
    _validate_process_inputs(process, execution_request)

    # 3. Determine execution mode.
    execution_mode = _resolve_execution_mode(process, preferred_mode)

    # 4. Resolve DB schema and delegate to ExecutionEngine.
    db_schema = "public"
    catalog_protocol = get_protocol(CatalogsProtocol)
    if catalog_protocol and catalog_id:
        from dynastore.models.driver_context import DriverContext
        db_schema = await catalog_protocol.resolve_physical_schema(
            catalog_id, ctx=DriverContext(db_resource=engine) if engine else None
        )

    return await execution_engine.execute(
        task_type=process_id,
        inputs=execution_request.model_dump(),
        engine=engine,
        mode=execution_mode,
        caller_id=caller_id,
        db_schema=db_schema or "public",
        background_tasks=background_tasks,
    )