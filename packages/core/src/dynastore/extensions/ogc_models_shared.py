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

"""Shared response models for OGC multi-item ingestion endpoints.

Used by Features, Records, and STAC extensions.  Not tied to any single
protocol — any OGC service that supports bulk creation can import from here.
"""

from typing import List, Optional, Literal

from pydantic import BaseModel, ConfigDict, Field


class RenameRequest(BaseModel):
    """Request body for rename endpoints."""

    model_config = ConfigDict(populate_by_name=True)

    new_id: str = Field(..., description="The desired new logical identifier.")


class RenameResponse(BaseModel):
    """Response for rename endpoints (catalog, collection, or asset)."""

    model_config = ConfigDict(populate_by_name=True)

    old_id: str = Field(..., description="The previous logical identifier.")
    new_id: str = Field(..., description="The new logical identifier (after normalization).")
    level: Literal["catalog", "collection", "asset"] = Field(
        ..., description="The entity level being renamed."
    )
    warnings: List[str] = Field(
        default_factory=list,
        description="Warning codes emitted by the rename operation.",
    )
    reindex_required: bool = Field(
        False,
        description=(
            "True when the secondary ES index contains the old logical id in "
            "document fields; a reindex is needed to reflect the new id there."
        ),
    )
    iam_manual_update_required: bool = Field(
        False,
        description=(
            "True for catalog and collection renames: IAM policy bindings that "
            "reference the old logical id must be updated manually."
        ),
    )


class BulkCreationResponse(BaseModel):
    """Returned on HTTP 201 when a multi-item payload is accepted in full."""

    ids: List[str]


class SidecarRejection(BaseModel):
    """Structured record of a feature rejected by a sidecar during ingestion.

    Emitted into :class:`IngestionReport.rejections`. ``reason`` is the
    sidecar's machine-readable reason code; ``policy_source`` points the
    caller at the effective write-policy endpoint so they can inspect or
    override the rule.
    """

    model_config = ConfigDict(populate_by_name=True)

    geoid: Optional[str] = Field(
        default=None,
        description="Resolved geoid of the candidate feature, if available.",
    )
    external_id: Optional[str] = Field(
        default=None,
        description="Submitted external identifier of the rejected feature.",
    )
    sidecar_id: Optional[str] = Field(
        default=None,
        description="Identifier of the sidecar that refused the write.",
    )
    matcher: Optional[str] = Field(
        default=None,
        description="Identity matcher that triggered the rejection "
        "(e.g. 'external_id', 'geometry_hash').",
    )
    reason: str = Field(
        ..., description="Machine-readable rejection reason code."
    )
    message: str = Field(
        ..., description="Human-readable rejection message."
    )
    policy_source: Optional[str] = Field(
        default=None,
        description=(
            "URL of the effective ItemsWritePolicy plugin payload for "
            "this collection, e.g. "
            "'/configs/catalogs/{cat}/collections/{col}/plugins/"
            "items_write_policy'."
        ),
    )


class IngestionReport(BaseModel):
    """Batch-level ingestion outcome returned by bulk endpoints.

    Returned with HTTP 201 when every feature was accepted and with HTTP 207
    when some were rejected by the collection write policy. A waterfall
    failure (policy itself unresolvable) is surfaced separately as
    ``ConfigResolutionError`` → HTTP 500.
    """

    model_config = ConfigDict(populate_by_name=True)

    accepted_ids: List[str] = Field(
        default_factory=list,
        description="Geoids of features that were successfully persisted.",
    )
    rejections: List[SidecarRejection] = Field(
        default_factory=list,
        description="Rejections produced by the collection write policy.",
    )
    total: int = Field(
        ..., description="Number of features submitted in the batch."
    )

    @property
    def is_partial(self) -> bool:
        return bool(self.rejections) and bool(self.accepted_ids)

    @property
    def is_fully_rejected(self) -> bool:
        return bool(self.rejections) and not self.accepted_ids
