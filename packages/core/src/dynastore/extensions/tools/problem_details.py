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
"""RFC 9457 Problem Details — generic envelope shared across extensions.

Lives in ``extensions/tools`` so any extension can raise/handle a
``ProblemException`` without crossing extension boundaries. Per-domain
constructors (e.g., ``configs.problem_details.plugin_not_registered``)
wrap these generic types with domain-specific URIs and titles.
"""

from typing import Any, List, Optional

from pydantic import BaseModel, Field


class ProblemError(BaseModel):
    """Per-field validation error (RFC 9457 ``errors`` extension member).

    Mirrors the structure operators expect when a request body fails
    validation: which field, why, and the offending value.
    """

    field: str = Field(
        ..., description="Dotted path to the failing field, e.g. 'target_srid'."
    )
    message: str = Field(
        ..., description="Human-readable validation message."
    )
    value: Optional[Any] = Field(
        default=None,
        description="The offending value, when serialisable.",
    )


class ProblemDetails(BaseModel):
    """RFC 9457 Problem Details for HTTP APIs.

    `type` is a URI identifying the problem class (clients can dispatch on
    it without parsing `title`); `title` is a short human label; `detail`
    is the longer prose; `instance` identifies this specific occurrence
    (typically the request path).
    """

    type: str = Field(
        ...,
        description=(
            "URI identifying the problem class — clients should dispatch on this, "
            "not on `title`. Format: 'https://errors.dynastore.fao.org/<category>'."
        ),
    )
    title: str = Field(
        ..., description="Short, human-readable summary of the problem class."
    )
    status: int = Field(
        ..., description="HTTP status code for this occurrence."
    )
    detail: Optional[str] = Field(
        default=None,
        description="Human-readable explanation specific to this occurrence.",
    )
    instance: Optional[str] = Field(
        default=None,
        description="URI identifying this specific occurrence — typically the request path.",
    )
    errors: Optional[List[ProblemError]] = Field(
        default=None,
        description=(
            "Per-field validation errors (RFC 9457 extension). Populated for "
            "422 responses arising from Pydantic validation."
        ),
    )


class ProblemException(Exception):
    """Raised to surface a ``ProblemDetails`` response.

    Subclasses :class:`Exception` (not :class:`HTTPException`) so the
    custom handler picks it up before FastAPI's default exception path
    can convert it to a plain JSON body.

    ``status_code`` and ``detail`` are exposed at the exception level so
    the global ``GlobalExceptionHandlingMiddleware`` (which runs *before*
    FastAPI's per-class handler dispatch) can read them via the existing
    ``getattr(result, "status_code", 500)`` path and avoid demoting a
    well-formed 4xx Problem Details to a generic 500.
    """

    def __init__(self, problem: ProblemDetails) -> None:
        self.problem = problem
        self.status_code = problem.status
        self.detail = problem.detail or problem.title
        super().__init__(problem.title)
