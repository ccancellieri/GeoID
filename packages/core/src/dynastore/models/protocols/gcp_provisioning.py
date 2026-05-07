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

"""
GCP-specific provisioning capability — sub-Protocol opt-in for vendor methods.

Producer: ``GCPModule`` (declares the method structurally; no inheritance).
Consumers: ``GcpProvisionCatalogTask`` and similar GCP bootstrap tasks check
``isinstance(storage, GcpCatalogProvisioning)`` before invoking the combined
bucket+eventing setup; mocks and non-GCP storage backends opt out by simply
not implementing the method, and the task falls back to the cross-vendor
``StorageProtocol.ensure_storage_for_catalog`` + ``EventingProtocol.setup_catalog_eventing``
sequence.

Keeps the cross-vendor ``StorageProtocol`` lean — backends like S3 / Azure
Blob / local FS aren't forced to stub a GCS-specific combined method.
"""

from typing import Any, Optional, Protocol, Tuple, runtime_checkable


@runtime_checkable
class GcpCatalogProvisioning(Protocol):
    """Vendor-specific catalog-bootstrap capability provided by the GCP module.

    A storage implementation that satisfies this Protocol can provision the
    bucket and eventing topology for a catalog in a single call — more
    efficient than chaining ``StorageProtocol.ensure_storage_for_catalog`` and
    ``EventingProtocol.setup_catalog_eventing`` separately because the GCP
    implementation interleaves the GCS resource creation with the Pub/Sub
    notification wiring.

    Returns ``(bucket_name, eventing_config)`` so the caller can log/observe
    both resources in one round-trip.
    """

    async def setup_catalog_gcp_resources(
        self,
        catalog_id: str,
        context: Optional[Any] = None,
    ) -> Tuple[str, Any]:
        """Idempotently provision bucket + eventing for a catalog.

        Args:
            catalog_id: The catalog whose GCP resources should be provisioned.
            context: Optional lifecycle/request context the producer may consult
                (e.g. for per-tenant config overrides). Implementations that
                don't need it ignore the kwarg.

        Returns:
            ``(bucket_name, eventing_config)``.
        """
        ...
