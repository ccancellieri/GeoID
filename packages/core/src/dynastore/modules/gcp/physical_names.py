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

"""GCP-specific PhysicalNameResolver implementations.

Two implementations are provided:

* :class:`GcsPhysicalNames` — resolves names for GCS resources (BUCKET,
  OBJECT_PREFIX).  Delegates to :class:`~dynastore.modules.gcp.bucket_service.BucketService`
  for bucket name generation so the sanitisation logic lives in one place.

* :class:`PubSubPhysicalNames` — resolves names for Cloud Pub/Sub resources
  (TOPIC, SUBSCRIPTION).  Uses the same deterministic scheme as
  :meth:`~dynastore.modules.gcp.gcp_eventing_ops.GcpEventingOpsMixin.generate_default_topic_id`
  and
  :meth:`~dynastore.modules.gcp.gcp_eventing_ops.GcpEventingOpsMixin.generate_default_subscription_id`.

Both classes implement :class:`~dynastore.models.protocols.physical_names.PhysicalNameResolver`
and accept ONLY resolved ``physical_id`` values — never mutable logical ids.
"""

from __future__ import annotations

from typing import ClassVar, FrozenSet, Optional

from dynastore.models.protocols.physical_names import PhysicalNameResolver, ResourceKind


class GcsPhysicalNames:
    """Maps catalog/collection physical ids to GCS resource names.

    Supported kinds: :attr:`~ResourceKind.BUCKET`, :attr:`~ResourceKind.OBJECT_PREFIX`.

    BUCKET
        The deterministic GCS bucket name for a catalog, produced by the same
        algorithm as
        :meth:`~dynastore.modules.gcp.bucket_service.BucketService.generate_bucket_name`.
        Requires ``prefix`` to be the GCP project id (used as the bucket-name
        prefix).

    OBJECT_PREFIX
        The relative object-name prefix for a collection's folder inside a
        catalog bucket (e.g. ``collections/s_2ka8fbc3/``).  Requires
        ``collection_physical_id``.
    """

    backend: ClassVar[str] = "gcs"
    supported_kinds: ClassVar[FrozenSet[ResourceKind]] = frozenset(
        {ResourceKind.BUCKET, ResourceKind.OBJECT_PREFIX}
    )

    def physical_name(
        self,
        kind: ResourceKind,
        *,
        catalog_physical_id: str,
        collection_physical_id: Optional[str] = None,
        prefix: Optional[str] = None,
    ) -> str:
        """Return the concrete GCS resource name for ``kind``.

        Parameters
        ----------
        kind:
            ``BUCKET`` or ``OBJECT_PREFIX``.
        catalog_physical_id:
            Immutable physical identifier of the catalog (e.g. ``"s_2ka8fbc3"``).
        collection_physical_id:
            Immutable physical identifier of the collection.  Required for
            ``OBJECT_PREFIX``; ignored for ``BUCKET``.
        prefix:
            GCP project id.  Required for ``BUCKET``; ignored for
            ``OBJECT_PREFIX`` (the bucket is not part of the prefix string).
        """
        if kind == ResourceKind.BUCKET:
            if not prefix:
                raise ValueError(
                    "GcsPhysicalNames.physical_name: 'prefix' (GCP project id) "
                    "is required for ResourceKind.BUCKET."
                )
            # Delegate to BucketService so all sanitisation stays in one place.
            from dynastore.modules.gcp.bucket_service import BucketService

            svc = BucketService(
                engine=None,
                config_service=None,
                storage_client=None,  # type: ignore[arg-type]
                project_id=prefix,
                region="",
            )
            return svc.generate_bucket_name(catalog_physical_id)

        if kind == ResourceKind.OBJECT_PREFIX:
            if not collection_physical_id:
                raise ValueError(
                    "GcsPhysicalNames.physical_name: 'collection_physical_id' "
                    "is required for ResourceKind.OBJECT_PREFIX."
                )
            from dynastore.modules.gcp.tools.bucket import get_blob_path_for_collection_folder

            return get_blob_path_for_collection_folder(collection_physical_id)

        raise ValueError(
            f"GcsPhysicalNames does not support ResourceKind.{kind.name}. "
            f"Supported: {sorted(k.name for k in self.supported_kinds)}"
        )


# Verify structural conformance at import time (runtime_checkable protocol).
assert isinstance(GcsPhysicalNames(), PhysicalNameResolver), (
    "GcsPhysicalNames must satisfy the PhysicalNameResolver protocol"
)


class PubSubPhysicalNames:
    """Maps catalog physical ids to Cloud Pub/Sub resource names.

    Supported kinds: :attr:`~ResourceKind.TOPIC`, :attr:`~ResourceKind.SUBSCRIPTION`.

    TOPIC
        ``ds-{catalog_physical_id}-events``

    SUBSCRIPTION
        ``ds-{catalog_physical_id}-default-sub``

    Both match the scheme used by
    :meth:`~dynastore.modules.gcp.gcp_eventing_ops.GcpEventingOpsMixin.generate_default_topic_id`
    and
    :meth:`~dynastore.modules.gcp.gcp_eventing_ops.GcpEventingOpsMixin.generate_default_subscription_id`.
    """

    backend: ClassVar[str] = "pubsub"
    supported_kinds: ClassVar[FrozenSet[ResourceKind]] = frozenset(
        {ResourceKind.TOPIC, ResourceKind.SUBSCRIPTION}
    )

    def physical_name(
        self,
        kind: ResourceKind,
        *,
        catalog_physical_id: str,
        collection_physical_id: Optional[str] = None,
        prefix: Optional[str] = None,
    ) -> str:
        """Return the concrete Pub/Sub resource name for ``kind``.

        Parameters
        ----------
        kind:
            ``TOPIC`` or ``SUBSCRIPTION``.
        catalog_physical_id:
            Immutable physical identifier of the catalog.
        collection_physical_id:
            Not used for Pub/Sub resources (catalog-level only).
        prefix:
            Not used; Pub/Sub resource names are project-scoped via the
            client, not embedded in the id string.
        """
        if not catalog_physical_id:
            raise ValueError(
                "PubSubPhysicalNames.physical_name: catalog_physical_id must not be empty."
            )

        if kind == ResourceKind.TOPIC:
            return f"ds-{catalog_physical_id}-events"

        if kind == ResourceKind.SUBSCRIPTION:
            return f"ds-{catalog_physical_id}-default-sub"

        raise ValueError(
            f"PubSubPhysicalNames does not support ResourceKind.{kind.name}. "
            f"Supported: {sorted(k.name for k in self.supported_kinds)}"
        )


# Verify structural conformance at import time.
assert isinstance(PubSubPhysicalNames(), PhysicalNameResolver), (
    "PubSubPhysicalNames must satisfy the PhysicalNameResolver protocol"
)
