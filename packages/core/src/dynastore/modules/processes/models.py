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

from pydantic import BaseModel, Field, ConfigDict
from enum import Enum
from typing import List, Dict, Any, Literal, Optional, TYPE_CHECKING
from dynastore.models.localization import LocalizedText
from uuid import UUID
from datetime import datetime
from dynastore.models.shared_models import Link
import uuid
from dynastore.models.tasks import Task, TaskPayload, TaskExecutionMode

if TYPE_CHECKING:
    from dynastore.modules.tasks.models import Task

# --- OGC Process Definition Models ---

class JobControlOptions(str, Enum):
    """OGC standard values for job control."""
    SYNC_EXECUTE = "sync-execute"
    ASYNC_EXECUTE = "async-execute"
    DISMISS = "dismiss"

class TransmissionMode(str, Enum):
    """OGC standard values for output transmission."""
    VALUE = "value"
    REFERENCE = "reference"


class ProcessScope(str, Enum):
    """
    Declares the target resource scope a process is defined against.

    The scope determines:
      - which URL path(s) are legal for execution;
      - which path parameters (catalog_id, collection_id, asset_id) are required;
      - the DB schema where the task row is persisted;
      - the authorization subject (platform vs tenant-scoped).

    Values:
      - PLATFORM:   system-wide; sysadmin only. No catalog / collection / asset
                    in the URL. Task rows live in ``public``.
      - CATALOG:    targets a single catalog. Requires ``catalog_id`` in URL.
                    Task rows live in the tenant schema.
      - COLLECTION: targets a single collection. Requires ``catalog_id`` +
                    ``collection_id`` in URL. Task rows live in the tenant schema.
      - ASSET:      targets a single asset (e.g. gdalinfo, vector ingestion).
                    Requires ``catalog_id`` + ``collection_id`` in URL plus
                    ``asset_id`` in the inputs (or via ``AssetTasksSPI`` when
                    triggered by the AssetService).
    """

    PLATFORM = "platform"
    CATALOG = "catalog"
    COLLECTION = "collection"
    ASSET = "asset"


class ProcessTypology(BaseModel):
    """Describes one runner capable of executing a process.

    `ProcessSummary.typologies` is priority-descending: element 0 is the runner
    the dispatcher will pick first; subsequent entries are fallbacks.
    """

    runner_type: str = Field(
        ...,
        description=(
            "Stable runner identifier (e.g. 'fastapi_background', 'gcp_cloud_run', "
            "'sync', 'asset_process')."
        ),
    )
    mode: TaskExecutionMode
    priority: int
    location: Literal["in_process", "cloud_run"] = Field(
        default="in_process",
        description=(
            "Where the runner executes the work. 'in_process' runs inside the "
            "catalog service (FastAPI BackgroundTasks, sync, asset-process); "
            "'cloud_run' dispatches to a Google Cloud Run Job worker."
        ),
    )


class ProcessUrlTemplate(BaseModel):
    """URL at which a process can be invoked, concrete or parametric."""

    scope: ProcessScope
    method: Literal["GET", "POST"]
    url_template: str = Field(
        ...,
        description=(
            "Fully-qualified URL when IDs are resolved, else a template with "
            "`{catalog_id}` / `{collection_id}` / `{asset_id}` placeholders."
        ),
    )
    rel: str = "execute"


class ProcessSummary(BaseModel):
    model_config = ConfigDict(
        json_schema_extra={
            "examples": [
                {
                    "id": "ingest",
                    "title": "Data Ingestion",
                    "description": "Ingest geospatial data into a collection.",
                    "version": "1.0.0",
                    "scopes": ["collection"],
                    "jobControlOptions": ["async-execute"],
                    "outputTransmission": ["reference"],
                    "links": [],
                }
            ]
        },
    )

    id: str
    title: str
    description: Optional[str] = None
    version: str
    scopes: List[ProcessScope] = Field(
        ...,
        min_length=1,
        description=(
            "One or more resource scopes at which this process can be executed. "
            "A process declaring ['catalog', 'collection'] is executable at both "
            "catalog-level and collection-level URLs."
        ),
    )
    jobControlOptions: List[JobControlOptions] = [JobControlOptions.ASYNC_EXECUTE]
    outputTransmission: List[TransmissionMode] = [TransmissionMode.REFERENCE]
    links: List[Link] = []
    typologies: List[ProcessTypology] = Field(
        default_factory=list,
        description=(
            "Runners able to execute this process, priority-descending. Populated "
            "when the caller opts in with `?typology=true`; empty otherwise. "
            "Non-OGC additive field — clients ignore if unknown."
        ),
    )
    url_templates: List[ProcessUrlTemplate] = Field(
        default_factory=list,
        description=(
            "Invocation URLs for each scope this process supports. Concrete when "
            "IDs are bound (e.g. under `/catalogs/{cat}/.../processes`), else "
            "parametric templates. Non-OGC additive field."
        ),
    )

class ProcessInput(BaseModel):
    title: str
    description: Optional[str] = None
    schema_: Dict[str, Any] = Field(..., alias="schema")

class ProcessOutput(BaseModel):
    title: str
    description: Optional[str] = None
    schema_: Dict[str, Any] = Field(..., alias="schema")

class Process(ProcessSummary):
    inputs: Dict[str, ProcessInput]
    outputs: Dict[str, ProcessOutput]

    model_config = ConfigDict(from_attributes=True)

# --- OGC API Endpoint Models ---

class ProcessList(BaseModel):
    model_config = ConfigDict(
        json_schema_extra={
            "examples": [
                {
                    "processes": [
                        {
                            "id": "ingest",
                            "title": "Data Ingestion",
                            "version": "1.0.0",
                            "jobControlOptions": ["async-execute"],
                            "links": [],
                        }
                    ],
                    "links": [
                        {"rel": "self", "href": "/processes", "type": "application/json"},
                    ],
                }
            ]
        },
    )

    processes: List[ProcessSummary]
    links: List[Link]

class StatusInfo(BaseModel):
    """OGC API - Processes StatusInfo model for job status responses."""

    model_config = ConfigDict(
        json_schema_extra={
            "examples": [
                {
                    "jobID": "550e8400-e29b-41d4-a716-446655440000",
                    "status": "running",
                    "type": "process",
                    "progress": 45,
                    "created": "2026-03-30T10:00:00Z",
                    "links": [
                        {"rel": "self", "href": "/processes/jobs/550e8400-e29b-41d4-a716-446655440000", "type": "application/json"},
                    ],
                },
                {
                    "jobID": "550e8400-e29b-41d4-a716-446655440000",
                    "status": "successful",
                    "type": "process",
                    "progress": 100,
                    "created": "2026-03-30T10:00:00Z",
                    "updated": "2026-03-30T10:05:00Z",
                    "links": [
                        {"rel": "self", "href": "/processes/jobs/550e8400-e29b-41d4-a716-446655440000", "type": "application/json"},
                        {"rel": "http://www.opengis.net/def/rel/ogc/1.0/results", "href": "/processes/jobs/550e8400-e29b-41d4-a716-446655440000/results", "type": "application/json"},
                    ],
                },
            ]
        },
    )

    jobID: UUID
    status: str
    message: Optional[str] = None
    type: str = "process"
    progress: Optional[int] = None
    created: Optional[datetime] = None
    updated: Optional[datetime] = None
    links: List[Link]

class OutputExecutionRequest(BaseModel):
    """Describes how an output should be returned/processed."""
    format: Optional[Dict[str, Any]] = None
    transmissionMode: Optional[TransmissionMode] = None

class ExecuteRequest(BaseModel):
    """
    Represents the input for a process execution.
    This model is designed to be used as the `inputs` type for TaskPayload,
    enabling processes to be executed as tasks.
    """

    model_config = ConfigDict(
        arbitrary_types_allowed=True,
        json_schema_extra={
            "examples": [
                {
                    "inputs": {
                        "collection_id": "my_collection",
                        "source": "https://example.com/data.csv",
                        "format": "csv",
                    },
                    "outputs": {
                        "result": {"transmissionMode": "reference"},
                    },
                    "response": "document",
                },
            ]
        },
    )

    inputs: Dict[str, Any]
    outputs: Optional[Dict[str, OutputExecutionRequest]] = None
    response: str = Field(default="document", pattern="^(document|raw)$")

# Type alias for clarity: a process execution task payload
ProcessTaskPayload = TaskPayload[ExecuteRequest] # This remains for type hinting

def as_process_task_payload(
    task_id: UUID, 
    caller_id: str,
    execution_request: "ExecuteRequest"
) -> "ProcessTaskPayload":
    """
    Utility to wrap an ExecuteRequest as a ProcessTaskPayload for runner compatibility.
    This is now the canonical way to construct a task payload for a process.
    """
    return ProcessTaskPayload(
        task_id=task_id,
        caller_id=caller_id,
        inputs=execution_request
    )

def task_to_status_info(task: "Task", links: Optional[List[Link]] = None) -> StatusInfo:
    """
    Converts a generic Task model to an OGC API - Processes StatusInfo model.
    This is the translation layer between generic task execution and OGC API.
    
    Args:
        task: Generic Task model from tasks module
        links: Optional list of HATEOAS links to include
        
    Returns:
        OGC-compliant StatusInfo model
    """
    from dynastore.modules.tasks.models import TaskStatusEnum
    
    # Specialized OGC status mapping
    mapping = {
        TaskStatusEnum.CREATED:     "created",
        TaskStatusEnum.PENDING:     "accepted",
        TaskStatusEnum.ACTIVE:      "running",
        TaskStatusEnum.RUNNING:     "running",
        TaskStatusEnum.COMPLETED:   "successful",
        TaskStatusEnum.FAILED:      "failed",
        TaskStatusEnum.DISMISSED:   "dismissed",
        TaskStatusEnum.DEAD_LETTER: "failed",
    }
    api_status = mapping.get(task.status, "accepted")

    info = StatusInfo(
        jobID=task.jobID,
        status=api_status,
        message=task.error_message,
        type=task.type,
        progress=task.progress,
        created=task.timestamp,
        updated=task.finished_at or task.started_at or task.timestamp,
        links=links or task.links.copy() if task.links else []
    )

    # OGC Process ID mapping
    # Note: we use task.task_type as the processID when type is 'process'
    # This assumes the task_type string corresponds to the process identifier.
    
    # Ensure 'self' link is present
    has_self = any(l.rel == "self" for l in info.links)
    if not has_self:
        info.links.append(Link(rel="self", type="application/json", title=LocalizedText(en="this job"), href=""))

    # If successful, ensure results link is present
    if task.status == TaskStatusEnum.COMPLETED:
        has_results = any(l.rel == "http://www.opengis.net/def/rel/ogc/1.0/results" for l in info.links)
        if not has_results:
            info.links.append(Link(
                rel="http://www.opengis.net/def/rel/ogc/1.0/results",
                type="application/json",
                title=LocalizedText(en="job results"),
                href=""
            ))

    return info
