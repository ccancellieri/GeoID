"""#1216 P4 — the items_schema derive-proposal endpoint.

`POST /configs/catalogs/{cat}/collections/{col}/items-schema/derive` turns a
vector asset's stored gdalinfo metadata into a *proposed* items_schema, without
persisting. It merges the derivation onto the collection's current schema:
admin tuning is preserved, new columns are added, fields the asset lacks are
kept. Error mapping: missing collection/asset -> 404, asset without a usable
gdalinfo blob -> 422.
"""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from dynastore.extensions.configs import problem_details
from dynastore.extensions.configs.service import (
    ConfigsService,
    ItemsSchemaDeriveRequest,
)
from dynastore.models.protocols import AssetsProtocol
from dynastore.models.protocols.collections import CollectionsProtocol
from dynastore.models.protocols.field_definition import FieldDefinition
from dynastore.modules.storage.driver_config import ItemsSchema

_GDALINFO = {
    "driverShortName": "GPKG",
    "layers": [{
        "name": "roads",
        "geometryType": "Line String",
        "fields": [
            {"name": "name", "type": "String"},
            {"name": "osm_id", "type": "Integer64"},
            {"name": "is_paved", "type": "Integer", "subtype": "Boolean"},
        ],
    }],
}


def _service(*, current_schema, collection, asset):
    """A bare ConfigsService with protocols/helpers stubbed for the handler."""
    svc = object.__new__(ConfigsService)
    store = MagicMock()
    store.get_config = AsyncMock(return_value=current_schema)

    collections = MagicMock()
    collections.get_collection = AsyncMock(return_value=collection)
    assets = MagicMock()
    assets.get_asset = AsyncMock(return_value=asset)

    def fake_get_protocol(proto):
        if proto is CollectionsProtocol:
            return collections
        if proto is AssetsProtocol:
            return assets
        raise AssertionError(f"unexpected protocol {proto!r}")

    ctx = [
        patch.object(type(svc), "configs", property(lambda self: store)),
        patch("dynastore.extensions.configs.service.require_catalog_ready", new=AsyncMock()),
        patch("dynastore.extensions.configs.service.get_protocol", side_effect=fake_get_protocol),
    ]
    return svc, ctx


def _asset(metadata):
    a = MagicMock()
    a.metadata = metadata
    return a


async def _run(svc, ctx, *, asset_id="a1", layer=None):
    for c in ctx:
        c.start()
    try:
        return await svc.derive_items_schema_proposal(
            "cat", "coll", ItemsSchemaDeriveRequest(asset_id=asset_id, layer=layer),
        )
    finally:
        for c in reversed(ctx):
            c.stop()


# ---------------------------------------------------------------------------
# Happy path — derive + merge
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_happy_path_merges_and_preserves_tuning() -> None:
    # Admin already tuned "name" (forced column) at the collection.
    current = ItemsSchema(fields={
        "name": FieldDefinition(name="name", data_type="string", materialize=True),
    })
    svc, ctx = _service(
        current_schema=current,
        collection=object(),
        asset=_asset({"gdalinfo": _GDALINFO}),
    )
    out = await _run(svc, ctx)

    assert out["catalog_id"] == "cat"
    assert out["collection_id"] == "coll"
    assert out["asset_id"] == "a1"
    fields = out["fields"]
    assert set(fields) == {"geometry", "name", "osm_id", "is_paved"}
    # types from the derivation (no narrowing; subtype promotion)
    assert fields["osm_id"]["data_type"] == "bigint"
    assert fields["is_paved"]["data_type"] == "boolean"
    # admin tuning preserved on the pre-existing field
    assert fields["name"]["materialize"] is True
    assert fields["name"]["data_type"] == "string"

    summary = out["summary"]
    assert "name" in summary["unchanged"]
    assert set(summary["added"]) == {"geometry", "osm_id", "is_paved"}
    assert summary["preserved"] == []


@pytest.mark.asyncio
async def test_existing_field_not_in_asset_is_preserved() -> None:
    current = ItemsSchema(fields={
        "legacy": FieldDefinition(name="legacy", data_type="string", materialize=True),
    })
    svc, ctx = _service(
        current_schema=current,
        collection=object(),
        asset=_asset({"gdalinfo": _GDALINFO}),
    )
    out = await _run(svc, ctx)
    assert "legacy" in out["fields"]
    assert out["fields"]["legacy"]["materialize"] is True
    assert out["summary"]["preserved"] == ["legacy"]


# ---------------------------------------------------------------------------
# Error mapping
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_missing_collection_404() -> None:
    svc, ctx = _service(
        current_schema=ItemsSchema(), collection=None,
        asset=_asset({"gdalinfo": _GDALINFO}),
    )
    with pytest.raises(problem_details.ProblemException) as ei:
        await _run(svc, ctx)
    assert ei.value.problem.status == 404


@pytest.mark.asyncio
async def test_missing_asset_404() -> None:
    svc, ctx = _service(
        current_schema=ItemsSchema(), collection=object(), asset=None,
    )
    with pytest.raises(problem_details.ProblemException) as ei:
        await _run(svc, ctx)
    assert ei.value.problem.status == 404


@pytest.mark.asyncio
async def test_asset_without_gdalinfo_422() -> None:
    svc, ctx = _service(
        current_schema=ItemsSchema(), collection=object(),
        asset=_asset({"owner": "x"}),  # no gdalinfo key
    )
    with pytest.raises(problem_details.ProblemException) as ei:
        await _run(svc, ctx)
    assert ei.value.problem.status == 422
    assert "gdalinfo" in (ei.value.problem.detail or "")


@pytest.mark.asyncio
async def test_gdalinfo_without_layers_422() -> None:
    svc, ctx = _service(
        current_schema=ItemsSchema(), collection=object(),
        asset=_asset({"gdalinfo": {"layers": []}}),
    )
    with pytest.raises(problem_details.ProblemException) as ei:
        await _run(svc, ctx)
    assert ei.value.problem.status == 422


@pytest.mark.asyncio
async def test_unknown_layer_name_422() -> None:
    svc, ctx = _service(
        current_schema=ItemsSchema(), collection=object(),
        asset=_asset({"gdalinfo": _GDALINFO}),
    )
    with pytest.raises(problem_details.ProblemException) as ei:
        await _run(svc, ctx, layer="does_not_exist")
    assert ei.value.problem.status == 422
