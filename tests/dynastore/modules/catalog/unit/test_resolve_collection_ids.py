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

"""Tests for resolve_collection_ids method (Issue #2430).

The resolve_collection_ids method resolves a collection ID (either external
or internal form) to a ResolvedCollectionIds model containing both forms.
This is the canonical resolution point for config persistence to ensure
configs are keyed on immutable internal IDs.
"""

import pytest
from unittest.mock import AsyncMock, MagicMock, patch

from dynastore.models.resolved_ids import ResolvedCollectionIds


class TestResolveCollectionIds:
    """Tests for CollectionService.resolve_collection_ids."""

    @pytest.fixture
    def mock_collection_service(self):
        """Mock CollectionService for testing."""
        from dynastore.modules.catalog.collection_service import CollectionService
        svc = CollectionService(engine=MagicMock())
        return svc

    @pytest.mark.asyncio
    async def test_resolve_from_external_id(self, mock_collection_service):
        """Resolve external_id to get both internal and external IDs."""
        # Mock the catalog resolution
        mock_catalogs = MagicMock()
        mock_catalogs.resolve_catalog_id = AsyncMock(return_value="c_internal123")

        # Patch get_protocol at the location where it's imported inside the method
        with patch(
            "dynastore.tools.discovery.get_protocol",
            return_value=mock_catalogs
        ):
            # Mock the collection resolution
            with patch.object(
                mock_collection_service,
                "resolve_collection_id",
                AsyncMock(return_value="col_abc123xyz4567")
            ):
                result = await mock_collection_service.resolve_collection_ids(
                    "catalog_external", "my_collection"
                )

        assert result.id == "col_abc123xyz4567"
        assert result.external_id == "my_collection"
        assert result.catalog_id == "c_internal123"

    @pytest.mark.asyncio
    async def test_resolve_from_internal_id(self, mock_collection_service):
        """Resolve internal_id to get both internal and external IDs."""
        # Mock the catalog resolution
        mock_catalogs = MagicMock()
        mock_catalogs.resolve_catalog_id = AsyncMock(return_value="c_internal123")

        with patch(
            "dynastore.tools.discovery.get_protocol",
            return_value=mock_catalogs
        ):
            # Mock the collection resolution
            with patch.object(
                mock_collection_service,
                "resolve_collection_external_id",
                AsyncMock(return_value="my_collection")
            ):
                # Use a valid internal ID format: col_ + 13 chars from [2-9a-x]
                result = await mock_collection_service.resolve_collection_ids(
                    "catalog_external", "col_tooimv7odhd9k"
                )

        assert result.id == "col_tooimv7odhd9k"
        assert result.external_id == "my_collection"
        assert result.catalog_id == "c_internal123"

    @pytest.mark.asyncio
    async def test_resolve_missing_external_id_raises(self, mock_collection_service):
        """Resolving missing external_id raises ValueError when allow_missing=False."""
        mock_catalogs = MagicMock()
        mock_catalogs.resolve_catalog_id = AsyncMock(return_value="c_internal123")

        with patch(
            "dynastore.tools.discovery.get_protocol",
            return_value=mock_catalogs
        ):
            with patch.object(
                mock_collection_service,
                "resolve_collection_id",
                AsyncMock(return_value=None)
            ):
                with pytest.raises(ValueError, match="not found"):
                    await mock_collection_service.resolve_collection_ids(
                        "catalog_external", "missing_collection", allow_missing=False
                    )

    @pytest.mark.asyncio
    async def test_resolve_missing_with_allow_missing(self, mock_collection_service):
        """Resolving missing ID with allow_missing=True returns original ID for both fields."""
        mock_catalogs = MagicMock()
        mock_catalogs.resolve_catalog_id = AsyncMock(return_value="c_internal123")

        with patch(
            "dynastore.tools.discovery.get_protocol",
            return_value=mock_catalogs
        ):
            with patch.object(
                mock_collection_service,
                "resolve_collection_id",
                AsyncMock(return_value=None)
            ):
                result = await mock_collection_service.resolve_collection_ids(
                    "catalog_external", "missing_collection", allow_missing=True
                )

        # When not found, both id and external_id are set to the input
        assert result.id == "missing_collection"
        assert result.external_id == "missing_collection"


class TestResolvedCollectionIdsModel:
    """Tests for the ResolvedCollectionIds Pydantic model."""

    def test_model_creation(self):
        """Test creating a ResolvedCollectionIds model."""
        resolved = ResolvedCollectionIds(
            id="col_abc123xyz4567",
            external_id="my_collection",
            catalog_id="c_xyz789"
        )
        assert resolved.id == "col_abc123xyz4567"
        assert resolved.external_id == "my_collection"
        assert resolved.catalog_id == "c_xyz789"

    def test_model_serialization(self):
        """Test serializing a ResolvedCollectionIds model."""
        resolved = ResolvedCollectionIds(
            id="col_abc123xyz4567",
            external_id="my_collection",
            catalog_id="c_xyz789"
        )
        data = resolved.model_dump()
        assert data["id"] == "col_abc123xyz4567"
        assert data["external_id"] == "my_collection"
        assert data["catalog_id"] == "c_xyz789"
