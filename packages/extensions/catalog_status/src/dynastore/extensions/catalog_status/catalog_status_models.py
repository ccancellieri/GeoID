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

"""Wire DTOs for the catalog_status extension.

These models are intentionally kept separate from the domain ORM/protocol
models so no internal fields leak onto the wire.
"""
from __future__ import annotations

from datetime import datetime
from typing import Optional
from uuid import UUID

from pydantic import BaseModel


class ProvisioningTaskView(BaseModel):
    """Most-recent ``gcp_provision_catalog`` task summary for a catalog."""

    task_id: UUID
    status: str
    error_message: Optional[str] = None
    retry_count: int = 0
    max_retries: int = 3
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None


class CatalogStatusView(BaseModel):
    """Catalog provisioning status view.

    Exposes the catalog's lifecycle state and, when the task-queue is
    available, the most-recent ``gcp_provision_catalog`` task for
    operator troubleshooting.
    """

    catalog_id: str
    physical_schema: Optional[str] = None
    physical_id: Optional[str] = None
    """Immutable physical identifier of this catalog.  In P0 this always
    equals ``physical_schema``; a future migration step will decouple them
    once a dedicated column is added."""
    provisioning_status: str
    task: Optional[ProvisioningTaskView] = None
    provisioning_checklist: dict[str, str] = {}
    """Per-step provisioning state, e.g. ``{"gcp_bucket": "complete",
    "gcp_eventing": "degraded"}``.  Empty when the catalog has no checklist
    (pre-#1175 catalogs or catalogs where provisioning was skipped)."""


class CollectionStatusView(BaseModel):
    """Collection status view.

    The ``Collection`` domain model carries no per-collection provisioning
    fields (lifecycle state lives at the catalog level). This view therefore
    exposes registry existence (the collection was found and is readable) and
    parent catalog context.  Per-collection provisioning information would
    require a dedicated collection-lifecycle table — that is out of scope for
    this extension.
    """

    catalog_id: str
    collection_id: str
    physical_schema: Optional[str] = None
    """Physical schema of the *parent catalog*, resolved via
    ``CatalogsProtocol.resolve_physical_schema``.  ``None`` when the catalog
    is still provisioning or the schema resolver is unavailable."""
    catalog_physical_id: Optional[str] = None
    """Immutable physical identifier of the *parent catalog*.  In P0 this
    always equals ``physical_schema``."""
    physical_id: Optional[str] = None
    """Immutable physical identifier of *this collection* (the physical table
    name).  Bare ``physical_id`` always refers to the entity the view
    represents — here, the collection; the parent catalog's id is the
    qualified ``catalog_physical_id``.  ``None`` when not yet resolved or when
    the collection has no dedicated physical table."""

    catalog_provisioning_status: str
    """Lifecycle state of the parent catalog (``provisioning`` | ``ready`` |
    ``failed``).  Collection writes are rejected while the parent catalog is
    not ``ready``."""
