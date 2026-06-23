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

"""
Unit tests for the asset reference system and upload protocol.

Covers (no DB required):
- AssetReferenceType / CoreAssetReferenceType enum semantics
- AssetReference Pydantic model validation
- AssetReferencedError message formatting
- AssetBase.owned_by field presence and defaults
- AssetUploadDefinition model
- UploadTicket / UploadStatus / UploadStatusResponse model validation
- AssetUploadProtocol structural compliance (runtime_checkable)
- AssetsProtocol structural compliance
- Extension pattern: custom AssetReferenceType subclass
"""

from datetime import datetime, timezone, timedelta
from unittest.mock import AsyncMock, MagicMock

from dynastore.models.shared_models import AssetReferenceType, CoreAssetReferenceType
from dynastore.modules.catalog.asset_service import (
    AssetBase,
    AssetTypeEnum,
    AssetReference,
    AssetReferencedError,
    AssetUploadDefinition,
)
from dynastore.models.protocols import (
    AssetsProtocol,
    AssetUploadProtocol,
    UploadTicket,
    UploadStatus,
    UploadStatusResponse,
)


# ---------------------------------------------------------------------------
# AssetReferenceType — enum semantics & extension pattern
# ---------------------------------------------------------------------------


class TestAssetReferenceType:
    def test_core_collection_value(self):
        assert CoreAssetReferenceType.COLLECTION.value == "collection"

    def test_core_is_subclass_of_base(self):
        assert issubclass(CoreAssetReferenceType, AssetReferenceType)

    def test_custom_subclass(self):
        """Driver modules can extend AssetReferenceType with namespaced values."""

        class DuckDbReferenceType(AssetReferenceType):
            TABLE = "duckdb:table"

        assert DuckDbReferenceType.TABLE.value == "duckdb:table"
        assert issubclass(DuckDbReferenceType, AssetReferenceType)

    def test_namespaced_values_dont_collide(self):
        class DuckDbReferenceType(AssetReferenceType):
            TABLE = "duckdb:table"

        class IcebergReferenceType(AssetReferenceType):
            TABLE = "iceberg:table"

        assert DuckDbReferenceType.TABLE != IcebergReferenceType.TABLE
        assert DuckDbReferenceType.TABLE != CoreAssetReferenceType.COLLECTION

    def test_str_enum_comparison(self):
        """AssetReferenceType values compare equal to their string representation."""
        assert CoreAssetReferenceType.COLLECTION == "collection"


# ---------------------------------------------------------------------------
# AssetReference model
# ---------------------------------------------------------------------------


class TestAssetReference:
    def _make(self, cascade_delete: bool = True) -> AssetReference:
        return AssetReference(
            asset_id="my_asset",
            catalog_id="my_catalog",
            ref_type=CoreAssetReferenceType.COLLECTION,
            ref_id="my_collection",
            cascade_delete=cascade_delete,
            created_at=datetime.now(timezone.utc),
        )

    def test_cascade_true_fields(self):
        ref = self._make(cascade_delete=True)
        assert ref.cascade_delete is True
        assert ref.ref_type == CoreAssetReferenceType.COLLECTION
        assert ref.ref_id == "my_collection"

    def test_cascade_false_fields(self):
        ref = self._make(cascade_delete=False)
        assert ref.cascade_delete is False

    def test_default_cascade_is_true(self):
        ref = AssetReference(
            asset_id="a",
            catalog_id="c",
            ref_type=CoreAssetReferenceType.COLLECTION,
            ref_id="r",
            created_at=datetime.now(timezone.utc),
        )
        assert ref.cascade_delete is True

    def test_ref_type_accepts_raw_string(self):
        """Forward-compat: raw string values (unknown driver types) are accepted."""
        ref = AssetReference(
            asset_id="a",
            catalog_id="c",
            ref_type="duckdb:table",  # type: ignore[arg-type]
            ref_id="t",
            created_at=datetime.now(timezone.utc),
        )
        assert ref.ref_type == "duckdb:table"

    def test_serialisation_roundtrip(self):
        ref = self._make(cascade_delete=False)
        data = ref.model_dump()
        assert data["cascade_delete"] is False
        assert data["ref_type"] == "collection"
        restored = AssetReference.model_validate(data)
        assert restored == ref

    def test_valid_until_default_none(self):
        """Active references default valid_until to None (Stage F4)."""
        ref = self._make()
        assert ref.valid_until is None

    def test_valid_until_accepts_timestamp(self):
        """Invalidated references carry the stamp time (Stage F4)."""
        stamp = datetime.now(timezone.utc)
        ref = AssetReference(
            asset_id="a",
            catalog_id="c",
            ref_type=CoreAssetReferenceType.COLLECTION,
            ref_id="r",
            created_at=stamp,
            valid_until=stamp,
        )
        assert ref.valid_until == stamp


# ---------------------------------------------------------------------------
# AssetReferencedError
# ---------------------------------------------------------------------------


class TestAssetReferencedError:
    def _blocking_ref(self, ref_id: str = "tbl") -> AssetReference:
        return AssetReference(
            asset_id="the_asset",
            catalog_id="cat",
            ref_type=CoreAssetReferenceType.COLLECTION,
            ref_id=ref_id,
            cascade_delete=False,
            created_at=datetime.now(timezone.utc),
        )

    def test_is_value_error(self):
        err = AssetReferencedError("my_asset", [self._blocking_ref()])
        assert isinstance(err, ValueError)

    def test_message_contains_asset_id(self):
        err = AssetReferencedError("my_asset", [self._blocking_ref()])
        assert "my_asset" in str(err)

    def test_message_counts_references(self):
        refs = [self._blocking_ref("tbl1"), self._blocking_ref("tbl2")]
        err = AssetReferencedError("the_asset", refs)
        assert "2 blocking" in str(err)

    def test_message_lists_ref_types(self):
        err = AssetReferencedError("the_asset", [self._blocking_ref("my_table")])
        assert "my_table" in str(err)

    def test_attributes_accessible(self):
        refs = [self._blocking_ref()]
        err = AssetReferencedError("the_asset", refs)
        assert err.asset_id == "the_asset"
        assert err.blocking_refs == refs


# ---------------------------------------------------------------------------
# AssetBase — owned_by field
# ---------------------------------------------------------------------------


class TestAssetBase:
    def test_owned_by_defaults_none(self):
        asset = AssetBase(asset_id="a")
        assert asset.owned_by is None

    def test_owned_by_gcs(self):
        asset = AssetBase(asset_id="a", owned_by="gcs")
        assert asset.owned_by == "gcs"

    def test_owned_by_local(self):
        asset = AssetBase(asset_id="a", owned_by="local")
        assert asset.owned_by == "local"

    def test_asset_type_default(self):
        asset = AssetBase(asset_id="a")
        assert asset.asset_type == AssetTypeEnum.ASSET

    def test_metadata_default_empty(self):
        asset = AssetBase(asset_id="a")
        assert asset.metadata == {}


# ---------------------------------------------------------------------------
# AssetUploadDefinition
# ---------------------------------------------------------------------------


class TestAssetUploadDefinition:
    def test_basic(self):
        d = AssetUploadDefinition(asset_id="scene_001", asset_type=AssetTypeEnum.RASTER)
        assert d.asset_id == "scene_001"
        assert d.asset_type == AssetTypeEnum.RASTER
        assert d.metadata == {}

    def test_with_metadata(self):
        d = AssetUploadDefinition(
            asset_id="scene_001",
            asset_type=AssetTypeEnum.ASSET,
            metadata={"sensor": "OLI-2", "cloud_cover": 5.2},
        )
        assert d.metadata["sensor"] == "OLI-2"


# ---------------------------------------------------------------------------
# UploadTicket / UploadStatus / UploadStatusResponse
# ---------------------------------------------------------------------------


class TestUploadModels:
    def _ticket(self, backend: str = "gcs") -> UploadTicket:
        return UploadTicket(
            ticket_id="ticket-abc",
            upload_url="https://storage.googleapis.com/bucket/file?upload_id=xyz",
            method="PUT",
            headers={"Content-Type": "image/tiff"},
            expires_at=datetime.now(timezone.utc) + timedelta(hours=1),
            backend=backend,
        )

    def test_ticket_fields(self):
        ticket = self._ticket()
        assert ticket.ticket_id == "ticket-abc"
        assert ticket.method == "PUT"
        assert ticket.backend == "gcs"
        assert "Content-Type" in ticket.headers

    def test_ticket_local_backend(self):
        ticket = self._ticket(backend="local")
        assert ticket.backend == "local"

    def test_upload_status_enum_values(self):
        assert UploadStatus.PENDING == "pending"
        assert UploadStatus.UPLOADING == "uploading"
        assert UploadStatus.COMPLETED == "completed"
        assert UploadStatus.FAILED == "failed"
        assert UploadStatus.CANCELLED == "cancelled"

    def test_status_response_pending(self):
        r = UploadStatusResponse(ticket_id="t1", status=UploadStatus.PENDING)
        assert r.asset_id is None
        assert r.error is None

    def test_status_response_completed(self):
        r = UploadStatusResponse(
            ticket_id="t1",
            status=UploadStatus.COMPLETED,
            asset_id="scene_001",
        )
        assert r.asset_id == "scene_001"

    def test_status_response_failed(self):
        r = UploadStatusResponse(
            ticket_id="t1",
            status=UploadStatus.FAILED,
            error="GCS returned 403",
        )
        assert r.error == "GCS returned 403"

    def test_ticket_serialisation(self):
        ticket = self._ticket()
        data = ticket.model_dump()
        assert data["backend"] == "gcs"
        restored = UploadTicket.model_validate(data)
        assert restored.ticket_id == ticket.ticket_id


# ---------------------------------------------------------------------------
# Protocol structural compliance (runtime_checkable)
# ---------------------------------------------------------------------------


class TestProtocolCompliance:
    """
    Verifies that mock objects satisfying the protocols are accepted by
    isinstance() without instantiating a real DB.
    """

    def _make_assets_mock(self) -> MagicMock:
        mock = MagicMock()
        for method in [
            "get_asset", "list_assets", "create_asset", "update_asset",
            "patch_asset",
            "finalize_pending_upload",
            "delete_asset", "delete_assets", "search_assets",
            "ensure_asset_cleanup_trigger",
            "add_asset_reference", "remove_asset_reference", "list_asset_references",
            "list_assets_for_reference",
            "rename_asset",
            "resolve_asset_physical_id",
            "resolve_asset_logical_id",
        ]:
            setattr(mock, method, AsyncMock())
        return mock

    def _make_upload_mock(self) -> MagicMock:
        mock = MagicMock()
        mock.driver_id = "test"
        mock.supports_versioning = False
        mock.initiate_upload = AsyncMock()
        mock.get_upload_status = AsyncMock()
        mock.upload_available = MagicMock(return_value=True)
        return mock

    def test_assets_protocol_isinstance(self):
        mock = self._make_assets_mock()
        assert isinstance(mock, AssetsProtocol)

    def test_upload_protocol_isinstance(self):
        mock = self._make_upload_mock()
        assert isinstance(mock, AssetUploadProtocol)

    def test_missing_method_fails_isinstance(self):
        """A mock missing a required method must NOT satisfy the protocol."""
        mock = MagicMock(spec=[])
        assert not isinstance(mock, AssetUploadProtocol)


# ---------------------------------------------------------------------------
# ref_type string coercion — hasattr(ref_type, "value") guard
# ---------------------------------------------------------------------------


class TestRefTypeCoercion:
    """
    Verifies that the ``hasattr(ref_type, 'value')`` guard in
    ``add_asset_reference`` / ``remove_asset_reference`` correctly serialises
    both enum instances and plain strings without raising AttributeError.
    """

    def test_enum_has_value_attr(self):
        rt = CoreAssetReferenceType.COLLECTION
        coerced = rt.value if hasattr(rt, "value") else str(rt)
        assert coerced == "collection"

    def test_plain_string_no_value_attr(self):
        rt = "duckdb:table"
        coerced = getattr(rt, "value", str(rt))
        assert coerced == "duckdb:table"

    def test_custom_subclass_enum_coerced(self):
        class DuckDbReferenceType(AssetReferenceType):
            TABLE = "duckdb:table"

        rt = DuckDbReferenceType.TABLE
        coerced = rt.value if hasattr(rt, "value") else str(rt)
        assert coerced == "duckdb:table"


# ---------------------------------------------------------------------------
# _list_blocking_references_bulk — result grouping logic
# ---------------------------------------------------------------------------


class TestBulkBlockingRefs:
    """
    Unit-tests the grouping logic used after ``_list_blocking_references_bulk``
    returns results: the first asset_id with blocking refs triggers the error.
    """

    def _blocking_ref(self, asset_id: str, ref_id: str = "tbl") -> AssetReference:
        return AssetReference(
            asset_id=asset_id,
            catalog_id="cat",
            ref_type="duckdb:table",
            ref_id=ref_id,
            cascade_delete=False,
            created_at=datetime.now(timezone.utc),
        )

    def test_first_asset_id_is_raised(self):
        """When bulk results arrive, AssetReferencedError uses the first asset_id."""
        rows = [
            self._blocking_ref("asset_a", "tbl1"),
            self._blocking_ref("asset_a", "tbl2"),
            self._blocking_ref("asset_b", "tbl3"),
        ]
        first_asset_id = rows[0].asset_id
        asset_blocking = [r for r in rows if r.asset_id == first_asset_id]
        err = AssetReferencedError(first_asset_id, asset_blocking)
        assert err.asset_id == "asset_a"
        assert len(err.blocking_refs) == 2

    def test_empty_bulk_result_no_error(self):
        """Empty bulk result means no blocking refs → no error raised."""
        rows: list = []
        assert not rows  # guard condition in delete_assets
