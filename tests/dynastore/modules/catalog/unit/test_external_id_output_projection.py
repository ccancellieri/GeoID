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

"""Unit tests for Phase 3: output-boundary projection of external_id as the public 'id'.

Covers:
(a) Catalog/Collection with id=<internal> + external_id=<public> serialize to
    JSON/dict with "id" == "<public>" and NO internal token / no external_id key.
(b) STAC generator _public_id() helper returns the external_id when set.
(c) Round-trip: a client that reads the serialized form and sends it back
    constructs a model whose serialized "id" is still the public label.
(d) Catalog/Collection without external_id (legacy path) still serialize "id"
    as the plain id field without leaking anything.
"""

from __future__ import annotations

import json
from unittest.mock import AsyncMock, patch

import pytest

from dynastore.models.shared_models import Catalog, Collection


# ---------------------------------------------------------------------------
# (a) Serialization: external_id projected as "id"; no leakage
# ---------------------------------------------------------------------------

class TestCatalogOutputProjection:
    """Catalog model serialization with the output-boundary projection."""

    def _make_catalog(self, internal: str = "c_a1b2c3d4", public: str = "my_catalog") -> Catalog:
        return Catalog(
            id=internal,
            external_id=public,
            type="Catalog",
            provisioning_status="ready",
        )

    def test_model_dump_json_emits_public_id(self):
        cat = self._make_catalog()
        data = json.loads(cat.model_dump_json())
        assert data["id"] == "my_catalog", (
            f"Expected public label 'my_catalog', got '{data['id']}'"
        )

    def test_model_dump_json_no_internal_token(self):
        cat = self._make_catalog(internal="c_a1b2c3d4", public="my_catalog")
        data = json.loads(cat.model_dump_json())
        assert "c_a1b2c3d4" not in json.dumps(data), (
            "Internal token must not appear anywhere in the JSON output"
        )

    def test_model_dump_json_no_external_id_key(self):
        cat = self._make_catalog()
        data = json.loads(cat.model_dump_json())
        assert "external_id" not in data, (
            "The raw 'external_id' field must not appear in the serialized output"
        )

    def test_model_dump_python_emits_public_id(self):
        """model_dump() (Python mode) also projects the public label so that
        localize() → OGCCollection round-trips work correctly."""
        cat = self._make_catalog()
        data = cat.model_dump(exclude_none=True)
        assert data["id"] == "my_catalog"
        assert "external_id" not in data

    def test_internal_id_attribute_unchanged(self):
        """The in-memory .id attribute must stay as the internal token."""
        cat = self._make_catalog(internal="c_a1b2c3d4", public="my_catalog")
        assert cat.id == "c_a1b2c3d4", (
            "In-memory .id must remain the immutable internal token"
        )

    def test_external_id_attribute_unchanged(self):
        """The in-memory .external_id attribute must stay as the public label."""
        cat = self._make_catalog(internal="c_a1b2c3d4", public="my_catalog")
        assert cat.external_id == "my_catalog"

    def test_legacy_catalog_no_external_id(self):
        """A Catalog without external_id still serializes id normally."""
        cat = Catalog(id="my_catalog", type="Catalog", provisioning_status="ready")
        data = json.loads(cat.model_dump_json())
        assert data["id"] == "my_catalog"
        assert "external_id" not in data

    def test_round_trip_same_public_id(self):
        """GET → serialize → client sends body back: the 'id' in the body is the
        public label; creating a Catalog from it serializes to the same public id."""
        cat = self._make_catalog(internal="c_a1b2c3d4", public="my_catalog")
        wire = json.loads(cat.model_dump_json())

        # Simulate a client PUT of the same body back
        reconstructed = Catalog.model_validate(wire)
        wire2 = json.loads(reconstructed.model_dump_json())
        assert wire2["id"] == "my_catalog"
        assert "external_id" not in wire2


class TestCollectionOutputProjection:
    """Collection model serialization with the output-boundary projection."""

    def _make_collection(
        self, internal: str = "c_x9y8z7w6", public: str = "sentinel_2"
    ) -> Collection:
        return Collection(
            id=internal,
            external_id=public,
            type="Collection",
        )

    def test_model_dump_json_emits_public_id(self):
        coll = self._make_collection()
        data = json.loads(coll.model_dump_json())
        assert data["id"] == "sentinel_2"

    def test_model_dump_json_no_external_id_key(self):
        coll = self._make_collection()
        data = json.loads(coll.model_dump_json())
        assert "external_id" not in data

    def test_model_dump_json_no_internal_token(self):
        coll = self._make_collection(internal="c_x9y8z7w6", public="sentinel_2")
        data = json.loads(coll.model_dump_json())
        assert "c_x9y8z7w6" not in json.dumps(data)

    def test_model_dump_python_emits_public_id(self):
        coll = self._make_collection()
        data = coll.model_dump(exclude_none=True)
        assert data["id"] == "sentinel_2"
        assert "external_id" not in data

    def test_internal_id_attribute_unchanged(self):
        coll = self._make_collection(internal="c_x9y8z7w6", public="sentinel_2")
        assert coll.id == "c_x9y8z7w6"

    def test_legacy_collection_no_external_id(self):
        coll = Collection(id="sentinel_2", type="Collection")
        data = json.loads(coll.model_dump_json())
        assert data["id"] == "sentinel_2"
        assert "external_id" not in data

    def test_round_trip_same_public_id(self):
        coll = self._make_collection(internal="c_x9y8z7w6", public="sentinel_2")
        wire = json.loads(coll.model_dump_json())
        reconstructed = Collection.model_validate(wire)
        wire2 = json.loads(reconstructed.model_dump_json())
        assert wire2["id"] == "sentinel_2"
        assert "external_id" not in wire2


# ---------------------------------------------------------------------------
# (b) STAC generator _public_id() helper
# ---------------------------------------------------------------------------

class TestPublicIdHelper:
    """_public_id() returns external_id when present, falls back to id."""

    def test_returns_external_id_when_set(self):
        from dynastore.extensions.stac.stac_generator import _public_id

        cat = Catalog(
            id="c_internal",
            external_id="my_catalog",
            type="Catalog",
            provisioning_status="ready",
        )
        assert _public_id(cat) == "my_catalog"

    def test_falls_back_to_id_when_no_external_id(self):
        from dynastore.extensions.stac.stac_generator import _public_id

        cat = Catalog(id="my_catalog", type="Catalog", provisioning_status="ready")
        assert _public_id(cat) == "my_catalog"

    def test_collection_returns_external_id_when_set(self):
        from dynastore.extensions.stac.stac_generator import _public_id

        coll = Collection(id="c_x9y8", external_id="sentinel_2", type="Collection")
        assert _public_id(coll) == "sentinel_2"

    def test_collection_falls_back_to_id(self):
        from dynastore.extensions.stac.stac_generator import _public_id

        coll = Collection(id="sentinel_2", type="Collection")
        assert _public_id(coll) == "sentinel_2"


# ---------------------------------------------------------------------------
# (c) Localize round-trip (simulates features_service OGCCollection path)
# ---------------------------------------------------------------------------

class TestLocalizeRoundTrip:
    """Verify that localize() output carries the public id for OGCCollection construction."""

    def test_localize_emits_public_id(self):
        """Collection.localize() must produce a dict with id == external_id."""
        coll = Collection(
            id="c_x9y8z7w6",
            external_id="sentinel_2",
            type="Collection",
        )
        data, _ = coll.localize("en")
        assert data["id"] == "sentinel_2", (
            f"localize() should yield public id 'sentinel_2', got '{data.get('id')}'"
        )
        assert "external_id" not in data

    def test_localize_catalog_emits_public_id(self):
        cat = Catalog(
            id="c_internal99",
            external_id="my_catalog",
            type="Catalog",
            provisioning_status="ready",
        )
        data, _ = cat.localize("en")
        assert data["id"] == "my_catalog"
        assert "external_id" not in data


# ---------------------------------------------------------------------------
# (d) list_catalogs link-building must use the public external label
# ---------------------------------------------------------------------------

class TestListCatalogsLinkBuilding:
    """The href links built in list_catalogs must use external_id, not the
    internal c_… token, so clients receive routable public URLs."""

    def _make_catalog(
        self,
        internal: str = "c_a1b2c3d4e5f6g",
        public: str = "my_catalog",
    ) -> Catalog:
        return Catalog(
            id=internal,
            external_id=public,
            type="Catalog",
            provisioning_status="ready",
        )

    def test_localize_id_is_public_label(self):
        """catalog_dict['id'] from localize() is the external label, not the
        internal token — this is the value used to build hrefs."""
        cat = self._make_catalog()
        catalog_dict, _ = cat.localize("en")
        assert catalog_dict["id"] == "my_catalog", (
            f"Expected 'my_catalog' from localize(), got '{catalog_dict['id']}'"
        )

    def test_links_use_public_label_not_internal_token(self):
        """Simulate the link-building block from list_catalogs and assert the
        hrefs contain the public label and NOT the c_… token."""
        from dynastore.models.shared_models import Link

        cat = self._make_catalog(internal="c_a1b2c3d4e5f6g", public="my_catalog")
        catalog_dict, _ = cat.localize("en")
        cat_pub = catalog_dict["id"]

        self_url = "https://example.com/features/catalogs"
        links = [
            Link(href=f"{self_url}/{cat_pub}", rel="self", type="application/json").model_dump(),
            Link(
                href=f"{self_url}/{cat_pub}/collections",
                rel="items",
                type="application/json",
            ).model_dump(),
        ]

        hrefs = [lnk["href"] for lnk in links]
        for href in hrefs:
            assert "c_a1b2c3d4e5f6g" not in href, (
                f"Internal token leaked into href: {href!r}"
            )
            assert "my_catalog" in href, (
                f"Public label missing from href: {href!r}"
            )

    def test_no_internal_token_in_any_href_or_id(self):
        """Full end-to-end shape: the catalog dict produced by localize() and
        the links built from cat_pub must both be free of any c_… token."""
        import json
        from dynastore.models.shared_models import Link

        cat = self._make_catalog(internal="c_a1b2c3d4e5f6g", public="my_catalog")
        catalog_dict, _ = cat.localize("en")
        cat_pub = catalog_dict["id"]

        self_url = "https://example.com/features/catalogs"
        catalog_dict["links"] = [
            Link(href=f"{self_url}/{cat_pub}", rel="self", type="application/json").model_dump(),
            Link(
                href=f"{self_url}/{cat_pub}/collections",
                rel="items",
                type="application/json",
            ).model_dump(),
        ]

        serialized = json.dumps(catalog_dict)
        assert "c_a1b2c3d4e5f6g" not in serialized, (
            "Internal c_… token must not appear anywhere in the catalog output"
        )
        assert not catalog_dict["id"].startswith("c_"), (
            f"Public id must not start with 'c_', got {catalog_dict['id']!r}"
        )


# ---------------------------------------------------------------------------
# (e) rename validates the new external_id before opening a transaction
# ---------------------------------------------------------------------------

class TestRenameValidatesExternalId:
    """rename_catalog and rename_collection must call validate_sql_identifier
    on the new label BEFORE touching the database.  An invalid label (empty,
    too long, illegal chars) must raise without issuing any SQL."""

    def _make_catalog_svc(self):
        from dynastore.modules.catalog.catalog_service import CatalogService

        svc = CatalogService.__new__(CatalogService)
        svc.engine = object()  # sentinel; must not be used
        svc._collection_service = None
        svc._item_service = None
        svc._cascade_orchestrator = None
        return svc

    def _make_collection_svc(self):
        from dynastore.modules.catalog.collection_service import CollectionService

        svc = CollectionService.__new__(CollectionService)
        svc.engine = object()
        return svc

    @pytest.mark.asyncio
    async def test_rename_catalog_empty_label_raises_before_db(self):
        svc = self._make_catalog_svc()
        with patch(
            "dynastore.modules.catalog.catalog_service.managed_transaction"
        ) as mock_tx:
            with pytest.raises(Exception):
                await svc.rename_catalog("c_internal1", "")
            # managed_transaction must NOT have been entered.
            mock_tx.assert_not_called()

    @pytest.mark.asyncio
    async def test_rename_catalog_overlength_label_raises_before_db(self):
        svc = self._make_catalog_svc()
        too_long = "a" * 64  # exceeds PG 63-char limit
        with patch(
            "dynastore.modules.catalog.catalog_service.managed_transaction"
        ) as mock_tx:
            with pytest.raises(Exception):
                await svc.rename_catalog("c_internal1", too_long)
            mock_tx.assert_not_called()

    @pytest.mark.asyncio
    async def test_rename_collection_empty_label_raises_before_db(self):
        svc = self._make_collection_svc()
        with patch(
            "dynastore.modules.catalog.collection_service.managed_transaction"
        ) as mock_tx, patch(
            "dynastore.modules.catalog.collection_service.get_protocol"
        ) as mock_gp:
            mock_catalog_svc = AsyncMock()
            mock_catalog_svc.resolve_physical_schema = AsyncMock(return_value="s_abc")
            mock_gp.return_value = mock_catalog_svc

            with pytest.raises(Exception):
                await svc.rename_collection("c_cat1", "col_int1", "")
            mock_tx.assert_not_called()

    @pytest.mark.asyncio
    async def test_rename_collection_overlength_label_raises_before_db(self):
        svc = self._make_collection_svc()
        too_long = "b" * 64
        with patch(
            "dynastore.modules.catalog.collection_service.managed_transaction"
        ) as mock_tx, patch(
            "dynastore.modules.catalog.collection_service.get_protocol"
        ) as mock_gp:
            mock_catalog_svc = AsyncMock()
            mock_catalog_svc.resolve_physical_schema = AsyncMock(return_value="s_abc")
            mock_gp.return_value = mock_catalog_svc

            with pytest.raises(Exception):
                await svc.rename_collection("c_cat1", "col_int1", too_long)
            mock_tx.assert_not_called()


# ---------------------------------------------------------------------------
# (f) merge_localized_updates preserves identity fields
# ---------------------------------------------------------------------------

class TestMergeLocalizedUpdatesPreservesIdentity:
    """merge_localized_updates must not corrupt .id / .external_id.

    model_dump(by_alias=True) fires _serialize_public_id which replaces the
    "id" key with the external_id value.  Without the post-validate restoration,
    the reconstructed model's .id would equal the public label instead of the
    internal token.
    """

    def test_internal_id_preserved_after_localized_update(self):
        """After merge via a BaseModel update, .id is still the internal token."""
        from dynastore.models.shared_models import CatalogUpdate
        cat = Catalog(
            id="c_internaltoken",
            external_id="prod",
            type="Catalog",
            provisioning_status="ready",
            title={"en": "Before"},
        )
        update = CatalogUpdate(title={"en": "After"})
        merged = cat.merge_localized_updates(update, lang="*")
        assert merged.id == "c_internaltoken", (
            f"merge_localized_updates corrupted .id; got {merged.id!r}"
        )

    def test_external_id_preserved_after_localized_update(self):
        """After merge, .external_id is still the public label."""
        from dynastore.models.shared_models import CatalogUpdate
        cat = Catalog(
            id="c_internaltoken",
            external_id="prod",
            type="Catalog",
            provisioning_status="ready",
            title={"en": "Before"},
        )
        update = CatalogUpdate(title={"en": "After"})
        merged = cat.merge_localized_updates(update, lang="*")
        assert merged.external_id == "prod", (
            f"merge_localized_updates corrupted .external_id; got {merged.external_id!r}"
        )

    def test_serialization_projection_intact_after_merge(self):
        """model_dump() of the merged model still emits id == external_id value."""
        import json
        from dynastore.models.shared_models import CatalogUpdate
        cat = Catalog(
            id="c_internaltoken",
            external_id="prod",
            type="Catalog",
            provisioning_status="ready",
            title={"en": "Before"},
        )
        update = CatalogUpdate(title={"en": "After"})
        merged = cat.merge_localized_updates(update, lang="*")
        wire = json.loads(merged.model_dump_json())
        assert wire["id"] == "prod", (
            f"Serialization projection broken after merge; wire id={wire['id']!r}"
        )
        assert "external_id" not in wire
        assert "c_internaltoken" not in json.dumps(wire)

    def test_merge_model_update_preserves_identity(self):
        """Passing a BaseModel update (the primary real-world call path) also preserves identity."""
        from dynastore.models.shared_models import CatalogUpdate
        cat = Catalog(
            id="c_internaltoken",
            external_id="prod",
            type="Catalog",
            provisioning_status="ready",
        )
        update = CatalogUpdate(title={"en": "New Title"})
        merged = cat.merge_localized_updates(update, lang="en")
        assert merged.id == "c_internaltoken"
        assert merged.external_id == "prod"

    def test_collection_identity_preserved_after_merge(self):
        """Works for Collection as well (same mixin)."""
        import json
        from dynastore.models.shared_models import Collection, CollectionUpdate
        coll = Collection(
            id="c_x9y8z7w6",
            external_id="sentinel_2",
            type="Collection",
            title={"en": "Old Title"},
        )
        update = CollectionUpdate(title={"en": "New Title"})
        merged = coll.merge_localized_updates(update, lang="*")
        assert merged.id == "c_x9y8z7w6"
        assert merged.external_id == "sentinel_2"
        wire = json.loads(merged.model_dump_json())
        assert wire["id"] == "sentinel_2"
        assert "external_id" not in wire
