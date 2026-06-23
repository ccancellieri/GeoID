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

"""Unit tests for GcsPhysicalNames and PubSubPhysicalNames."""

from __future__ import annotations

import pytest

from dynastore.models.protocols.physical_names import PhysicalNameResolver, ResourceKind
from dynastore.modules.gcp.physical_names import GcsPhysicalNames, PubSubPhysicalNames


# ---------------------------------------------------------------------------
# Protocol conformance
# ---------------------------------------------------------------------------


def test_gcs_physical_names_satisfies_protocol():
    assert isinstance(GcsPhysicalNames(), PhysicalNameResolver)


def test_pubsub_physical_names_satisfies_protocol():
    assert isinstance(PubSubPhysicalNames(), PhysicalNameResolver)


def test_gcs_backend_identifier():
    assert GcsPhysicalNames.backend == "gcs"


def test_pubsub_backend_identifier():
    assert PubSubPhysicalNames.backend == "pubsub"


def test_gcs_supported_kinds():
    kinds = GcsPhysicalNames.supported_kinds
    assert ResourceKind.BUCKET in kinds
    assert ResourceKind.OBJECT_PREFIX in kinds
    assert ResourceKind.TOPIC not in kinds
    assert ResourceKind.SUBSCRIPTION not in kinds


def test_pubsub_supported_kinds():
    kinds = PubSubPhysicalNames.supported_kinds
    assert ResourceKind.TOPIC in kinds
    assert ResourceKind.SUBSCRIPTION in kinds
    assert ResourceKind.BUCKET not in kinds
    assert ResourceKind.OBJECT_PREFIX not in kinds


# ---------------------------------------------------------------------------
# GcsPhysicalNames — BUCKET
# ---------------------------------------------------------------------------


def test_gcs_bucket_name_uses_physical_id():
    resolver = GcsPhysicalNames()
    name = resolver.physical_name(
        ResourceKind.BUCKET,
        catalog_physical_id="s_2ka8fbc3",
        prefix="my-gcp-project",
    )
    assert name == "my-gcp-project-s-2ka8fbc3"


def test_gcs_bucket_name_sanitises_underscores():
    resolver = GcsPhysicalNames()
    name = resolver.physical_name(
        ResourceKind.BUCKET,
        catalog_physical_id="s_2ka8fbc3",
        prefix="my-project",
    )
    # underscores in physical_id are normalised to dashes
    assert "_" not in name


def test_gcs_bucket_name_requires_prefix():
    resolver = GcsPhysicalNames()
    with pytest.raises(ValueError, match="prefix"):
        resolver.physical_name(
            ResourceKind.BUCKET,
            catalog_physical_id="s_2ka8fbc3",
        )


def test_gcs_bucket_name_physical_id_only_no_logical_id():
    # Producing a name from a logical id is a caller error — but the resolver
    # itself has no way to enforce this; it trusts the caller to pass a
    # physical_id.  What we verify here is that the output is purely
    # physical_id-based and byte-identical to BucketService.generate_bucket_name.
    from unittest.mock import MagicMock
    from dynastore.modules.gcp.bucket_service import BucketService

    physical_id = "s_abc12345"
    project_id = "test-project"
    svc = BucketService(
        engine=None,
        config_service=None,
        storage_client=MagicMock(),
        project_id=project_id,
        region="europe-west1",
    )
    expected = svc.generate_bucket_name(physical_id)

    resolver = GcsPhysicalNames()
    result = resolver.physical_name(
        ResourceKind.BUCKET,
        catalog_physical_id=physical_id,
        prefix=project_id,
    )
    assert result == expected


# ---------------------------------------------------------------------------
# GcsPhysicalNames — OBJECT_PREFIX
# ---------------------------------------------------------------------------


def test_gcs_object_prefix_uses_collection_physical_id():
    resolver = GcsPhysicalNames()
    prefix = resolver.physical_name(
        ResourceKind.OBJECT_PREFIX,
        catalog_physical_id="s_2ka8fbc3",
        collection_physical_id="t_9xz12345",
    )
    assert prefix == "collections/t_9xz12345/"


def test_gcs_object_prefix_requires_collection_physical_id():
    resolver = GcsPhysicalNames()
    with pytest.raises(ValueError, match="collection_physical_id"):
        resolver.physical_name(
            ResourceKind.OBJECT_PREFIX,
            catalog_physical_id="s_2ka8fbc3",
        )


def test_gcs_object_prefix_consistent_with_bucket_tool():
    from dynastore.modules.gcp.tools.bucket import get_blob_path_for_collection_folder

    collection_physical_id = "t_9xz12345"
    resolver = GcsPhysicalNames()
    result = resolver.physical_name(
        ResourceKind.OBJECT_PREFIX,
        catalog_physical_id="s_2ka8fbc3",
        collection_physical_id=collection_physical_id,
    )
    assert result == get_blob_path_for_collection_folder(collection_physical_id)


def test_gcs_unsupported_kind_raises():
    resolver = GcsPhysicalNames()
    with pytest.raises(ValueError, match="does not support"):
        resolver.physical_name(
            ResourceKind.TOPIC,
            catalog_physical_id="s_2ka8fbc3",
        )


# ---------------------------------------------------------------------------
# PubSubPhysicalNames — TOPIC
# ---------------------------------------------------------------------------


def test_pubsub_topic_id_format():
    resolver = PubSubPhysicalNames()
    topic_id = resolver.physical_name(
        ResourceKind.TOPIC,
        catalog_physical_id="s_2ka8fbc3",
    )
    assert topic_id == "ds-s_2ka8fbc3-events"


def test_pubsub_topic_id_consistent_with_eventing_ops():
    """Topic id must match generate_default_topic_id."""
    from unittest.mock import MagicMock, patch

    # generate_default_topic_id is a method on the mixin; instantiate a stub.
    from dynastore.modules.gcp.gcp_eventing_ops import GcpEventingOpsMixin

    class _Stub(GcpEventingOpsMixin):
        def get_project_id(self): return "p"
        def get_region(self): return "r"
        def get_account_email(self): return "e@e"
        async def get_self_url(self): return "https://x"
        def get_publisher_client(self): return MagicMock()
        def get_storage_client(self): return MagicMock()
        def get_bucket_service(self): return MagicMock()
        def get_subscriber_client(self): return MagicMock()
        def get_config_service(self): return MagicMock()
        async def setup_catalog_gcp_resources(self, *a, **kw): return ("b", None)
        @property
        def engine(self): return MagicMock()

    stub = _Stub()
    physical_id = "s_2ka8fbc3"
    expected = stub.generate_default_topic_id(physical_id)

    resolver = PubSubPhysicalNames()
    result = resolver.physical_name(
        ResourceKind.TOPIC,
        catalog_physical_id=physical_id,
    )
    assert result == expected


# ---------------------------------------------------------------------------
# PubSubPhysicalNames — SUBSCRIPTION
# ---------------------------------------------------------------------------


def test_pubsub_subscription_id_format():
    resolver = PubSubPhysicalNames()
    sub_id = resolver.physical_name(
        ResourceKind.SUBSCRIPTION,
        catalog_physical_id="s_2ka8fbc3",
    )
    assert sub_id == "ds-s_2ka8fbc3-default-sub"


def test_pubsub_subscription_id_consistent_with_eventing_ops():
    """Subscription id must match generate_default_subscription_id."""
    from unittest.mock import MagicMock
    from dynastore.modules.gcp.gcp_eventing_ops import GcpEventingOpsMixin

    class _Stub(GcpEventingOpsMixin):
        def get_project_id(self): return "p"
        def get_region(self): return "r"
        def get_account_email(self): return "e@e"
        async def get_self_url(self): return "https://x"
        def get_publisher_client(self): return MagicMock()
        def get_storage_client(self): return MagicMock()
        def get_bucket_service(self): return MagicMock()
        def get_subscriber_client(self): return MagicMock()
        def get_config_service(self): return MagicMock()
        async def setup_catalog_gcp_resources(self, *a, **kw): return ("b", None)
        @property
        def engine(self): return MagicMock()

    stub = _Stub()
    physical_id = "s_2ka8fbc3"
    expected = stub.generate_default_subscription_id(physical_id)

    resolver = PubSubPhysicalNames()
    result = resolver.physical_name(
        ResourceKind.SUBSCRIPTION,
        catalog_physical_id=physical_id,
    )
    assert result == expected


def test_pubsub_empty_physical_id_raises():
    resolver = PubSubPhysicalNames()
    with pytest.raises(ValueError, match="catalog_physical_id must not be empty"):
        resolver.physical_name(
            ResourceKind.TOPIC,
            catalog_physical_id="",
        )


def test_pubsub_unsupported_kind_raises():
    resolver = PubSubPhysicalNames()
    with pytest.raises(ValueError, match="does not support"):
        resolver.physical_name(
            ResourceKind.BUCKET,
            catalog_physical_id="s_2ka8fbc3",
        )


# ---------------------------------------------------------------------------
# Byte-identity assertion for bucket names
# (existing catalogs: physical_id == physical_schema == old identifier)
# ---------------------------------------------------------------------------


def test_gcs_bucket_name_byte_identical_for_existing_catalog():
    """Existing catalogs that used physical_schema produce the same bucket name.

    Before this change, generate_bucket_name preferred physical_schema over
    catalog_id.  Existing provisioned catalogs stored the result in
    GcpCatalogBucketConfig.bucket_name, so the bucket name persisted in the DB
    is never recalculated.  This test asserts that IF a new catalog were
    provisioned with the same physical_id string (as the old physical_schema),
    the name would be byte-identical — proving the rename-safety contract for
    existing data.
    """
    from unittest.mock import MagicMock
    from dynastore.modules.gcp.bucket_service import BucketService

    project_id = "my-test-project"
    physical_schema = "s_2ka8fbc3"

    # Old code: generate_bucket_name(any_id, physical_schema=physical_schema)
    # New code: generate_bucket_name(physical_id=physical_schema)
    # Both must produce the same bucket name.
    svc = BucketService(
        engine=None,
        config_service=None,
        storage_client=MagicMock(),
        project_id=project_id,
        region="europe-west1",
    )
    new_name = svc.generate_bucket_name(physical_schema)
    resolver = GcsPhysicalNames()
    resolver_name = resolver.physical_name(
        ResourceKind.BUCKET,
        catalog_physical_id=physical_schema,
        prefix=project_id,
    )
    assert new_name == resolver_name == f"{project_id}-{physical_schema.replace('_', '-')}"
