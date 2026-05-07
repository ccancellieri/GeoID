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
"""Parametric asset-process protocol.

A DynaStore *asset process* is any named operation that can be invoked on a
specific asset:

* ``download`` — generate a short-lived access URL
* ``ingest``   — trigger ingestion into a storage driver
* ``validate`` — run a format / schema check
* ``convert``  — produce a derived asset in a new format
* ``inspect``  — return metadata extracted from the blob

Processes are registered as ``AssetProcessProtocol`` implementations discovered
via ``get_protocols(AssetProcessProtocol)``. The asset REST layer mounts each
registered process symmetrically under::

    POST /catalogs/{cid}/assets/{aid}/processes/{process_id}/execution
    POST /catalogs/{cid}/collections/{colid}/assets/{aid}/processes/{process_id}/execution

This mirrors OGC API - Processes (`process_id` as path parameter) but scopes
the input to an existing Asset, which is always the first parameter of
``execute``.

Discovery
---------
``GET /assets/catalogs/{cid}/assets/{aid}/processes`` returns the list of
processes *applicable* to that asset (backends may opt out per-asset based on
``owned_by``, URI scheme, asset type, etc.).
"""
from __future__ import annotations

from typing import Any, Dict, List, Literal, Optional, Protocol, TYPE_CHECKING, runtime_checkable

from pydantic import BaseModel, ConfigDict, Field

if TYPE_CHECKING:
    from dynastore.modules.catalog.asset_service import Asset

HTTPMethod = Literal["GET", "POST", "PUT", "DELETE"]
"""HTTP verb a process is invoked with.

``GET`` for idempotent reads (download, inspect), ``POST`` for state-changing
actions (ingest, convert, validate-and-store), ``DELETE`` for process-level
teardown (cancel a derived asset produced by a prior ``POST``).
"""


class AssetProcessOutput(BaseModel):
    """Polymorphic result of an asset-process execution.

    The concrete shape depends on ``type``:

    * ``signed_url`` — a pre-signed URL the client can follow directly.
    * ``redirect`` — an HTTP-level redirect target (used for non-owned assets
      whose ``uri`` is already a public URL).
    * ``job`` — an asynchronous job handle; the client polls a job endpoint.
    * ``inline`` — the payload is returned inline in ``data``.
    """

    model_config = ConfigDict(populate_by_name=True)

    type: Literal["signed_url", "redirect", "job", "inline"] = Field(
        ...,
        description="Shape of the result returned by this process invocation.",
    )
    url: Optional[str] = Field(
        default=None,
        description="Target URL (signed_url / redirect) the client should follow.",
    )
    method: Optional[HTTPMethod] = Field(
        default=None,
        description="HTTP method the client must use on ``url`` (signed_url only).",
    )
    expires_at: Optional[str] = Field(
        default=None,
        description="ISO-8601 UTC timestamp after which ``url`` is no longer valid.",
    )
    headers: Dict[str, str] = Field(
        default_factory=dict,
        description="Extra headers the client must include when calling ``url``.",
    )
    job_id: Optional[str] = Field(
        default=None,
        description="Job handle (``type=job``) used to poll the task queue.",
    )
    data: Optional[Any] = Field(
        default=None,
        description="Inline result payload (``type=inline``).",
    )


class AssetProcessDescriptor(BaseModel):
    """Process-discovery entry returned by ``GET .../processes``."""

    process_id: str = Field(..., description="Stable identifier used in the URL path.")
    title: str = Field(..., description="Short human-readable title.")
    description: str = Field(..., description="Longer description of what the process does.")
    http_method: HTTPMethod = Field(..., description="HTTP verb used to invoke the process.")
    applicable: bool = Field(
        ...,
        description=(
            "Whether this process is applicable to the asset in the current "
            "context (e.g. ``download`` is not applicable to a non-downloadable "
            "driver-native asset)."
        ),
    )
    reason: Optional[str] = Field(
        default=None,
        description="When ``applicable=false``, a short explanation for the caller.",
    )
    parameters_schema: Optional[Dict[str, Any]] = Field(
        default=None,
        description=(
            "Optional JSON Schema describing the ``params`` payload accepted by "
            "this process. ``None`` means the process takes no parameters."
        ),
    )


@runtime_checkable
class AssetProcessProtocol(Protocol):
    """Backend-agnostic contract for an asset-scoped operation.

    Implementors should be idempotent where possible and must not mutate the
    asset's canonical ``uri`` (use a new asset via ``AssetsProtocol.create_asset``
    when producing derivatives).
    """

    process_id: str
    """Stable URL segment used to route to this process (e.g. ``"download"``)."""

    http_method: HTTPMethod
    """HTTP method the asset router will expose this process under.

    Post-OGC-realignment all asset processes are invoked via
    ``POST /.../assets/{aid}/processes/{process_id}/execution`` with parameters
    in the JSON body — regardless of this attribute. ``http_method`` is retained
    for descriptor metadata only.
    """

    async def describe(self, asset: "Asset") -> AssetProcessDescriptor:
        """Return a discovery entry for this process and asset.

        Called by ``GET .../processes`` to build the per-asset catalog.
        Must return ``applicable=false`` (with a ``reason``) when the process
        cannot operate on ``asset`` in the current context — never raise.
        """
        ...

    async def execute(
        self,
        asset: "Asset",
        params: Dict[str, Any],
    ) -> AssetProcessOutput:
        """Execute the process against ``asset``.

        Args:
            asset: The target asset (already fetched + authorized by the router).
            params: Request query parameters (``GET``) or JSON body (``POST``).

        Returns:
            ``AssetProcessOutput`` describing the result (signed URL, redirect,
            job handle, or inline payload).

        Raises:
            HTTPException 400: ``params`` validation failed.
            HTTPException 409: process not applicable to this asset.
            HTTPException 503: backend required by the process is unavailable.
        """
        ...


def list_applicable_processes(
    asset: "Asset",
    processes: List[AssetProcessProtocol],
) -> List[AssetProcessDescriptor]:
    """Helper used by the discovery endpoint — awaits all ``describe`` calls.

    Kept as a free function so the router can import it without pulling in a
    circular dependency on the asset service.
    """
    raise NotImplementedError(
        "Call via `await` from an async context; see AssetService.list_processes"
    )


def describe_asset_process_static(
    process: AssetProcessProtocol,
) -> AssetProcessDescriptor:
    """Asset-free descriptor for deployment-wide discovery.

    Used by the scope-agnostic process inventory (e.g.
    ``GET /processes?scope=asset``) where no concrete asset exists.
    Derives the descriptor from the instance's ``process_id`` /
    ``http_method`` attributes plus optional ``title`` / ``description`` /
    ``parameters_schema`` attributes.

    Always returns ``applicable=True`` — per-asset applicability is only
    meaningful when a concrete asset is provided via ``describe``.
    """
    return AssetProcessDescriptor(
        process_id=process.process_id,
        title=getattr(process, "title", process.process_id),
        description=getattr(process, "description", ""),
        http_method=getattr(process, "http_method", "POST"),
        applicable=True,
        reason=None,
        parameters_schema=getattr(process, "parameters_schema", None),
    )
