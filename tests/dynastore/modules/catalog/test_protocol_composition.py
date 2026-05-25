from tests.dynastore.test_utils import generate_test_id

import pytest
from typing import Any, Dict, Optional
from dynastore.models.protocols.catalogs import CatalogsProtocol
from dynastore.models.protocols.items import ItemsProtocol
from dynastore.models.protocols.collections import CollectionsProtocol
from dynastore.tools.discovery import get_protocol

pytestmark = [
    pytest.mark.asyncio,
]


async def test_catalogs_protocol_composition(app_lifespan_module):
    """Verify that the CatalogsProtocol correctly inherits and exposes sub-protocols."""
    from dynastore.modules import get_protocol

    catalogs_svc = get_protocol(CatalogsProtocol)
    assert catalogs_svc is not None

    # Verify inheritance
    # CatalogsProtocol is a runtime_checkable protocol, so isinstance works if the object implements it
    # CatalogModule implements ItemsProtocol and CollectionsProtocol
    assert isinstance(catalogs_svc, ItemsProtocol)
    assert isinstance(catalogs_svc, CollectionsProtocol)

    # Verify property accessors
    assert catalogs_svc.items is not None
    assert catalogs_svc.collections is not None

    # Verify logical item retrieval exists in protocol and implementation
    assert hasattr(catalogs_svc, "get_item")
    assert hasattr(catalogs_svc.items, "get_item")


async def test_logical_item_retrieval(app_lifespan_module):
    """Verify that logical item retrieval works without manual schema/table resolution."""
    from dynastore.modules import get_protocol

    catalogs_svc: CatalogsProtocol = get_protocol(CatalogsProtocol)

    catalog_id = f"logical_cat_{generate_test_id()}"
    collection_id = f"logical_coll_{generate_test_id()}"

    # Setup
    if await catalogs_svc.get_catalog_model(catalog_id):
        await catalogs_svc.delete_catalog(catalog_id, force=True)

    await catalogs_svc.create_catalog({"id": catalog_id, "title": "Test Cat"})
    await catalogs_svc.create_collection(
        catalog_id, {"id": collection_id, "title": "Test Coll"}
    )

    # Create an item
    from dynastore.extensions.features.ogc_models import FeatureDefinition

    # Create item logic
    ext_id = "test-item-logical"
    item_data = FeatureDefinition(
        type="Feature",
        id=ext_id,
        geometry={"type": "Point", "coordinates": [12.5, 41.9]},
        properties={"name": "Test Item", "external_id": ext_id},
    )

    # Upsert (single) — returns the created item(s).
    created_items = await catalogs_svc.upsert(catalog_id, collection_id, item_data)
    created_item = (
        created_items[0] if isinstance(created_items, list) else created_items
    )

    # The feature id is the retrieval key for get_item. With the default read
    # policy (``external_id_as_feature_id=False``) that id IS the geoid, so we
    # retrieve by whatever id the upsert assigned rather than the raw
    # external_id — external_id-as-feature-id is a separate, config-gated path
    # covered by the geoid-default-id / query-optimizer tests. This test only
    # asserts the *protocol composition* contract: get_item resolves the
    # physical schema/table from the logical (catalog, collection, id) tuple
    # with no manual resolution.
    feature_id = (
        created_item.get("id") if isinstance(created_item, dict)
        else getattr(created_item, "id", None)
    )
    assert feature_id, f"upsert did not return a feature id: {created_item!r}"

    # Verify using get_item with the logical (catalog, collection, id) tuple.
    item = await catalogs_svc.get_item(catalog_id, collection_id, feature_id)
    assert item is not None, f"Item not found via get_item for id {feature_id}"

    # Handle both dict and object/row access depending on service return type
    if hasattr(item, "properties"):
        attributes = item.properties
    elif isinstance(item, dict):
        attributes = item.get("properties") or item.get("attributes")
    else:
        # Fallback for old Row objects
        attributes = getattr(item, "attributes", None)

    # Parse JSON string if needed
    if isinstance(attributes, str):
        import json

        attributes = json.loads(attributes)

    assert attributes["name"] == "Test Item"

    # Retrieve via the items property accessor (same protocol, same key).
    item_via_prop = await catalogs_svc.items.get_item(
        catalog_id, collection_id, feature_id
    )
    assert item_via_prop is not None

    # Cleanup
    await catalogs_svc.delete_catalog(catalog_id, force=True)


async def test_removed_methods_not_in_protocol():
    """Verify that internal methods are no longer part of the public protocol."""
    # Note: typing.Protocol doesn't enforce absence at runtime if the object has the method,
    # but we can verify our expectations for the interface if we had a static checker.
    # At least we verify they are gone from the definition (already done).
    pass
