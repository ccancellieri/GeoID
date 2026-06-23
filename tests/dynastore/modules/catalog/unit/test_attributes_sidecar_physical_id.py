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

"""Unit tests: FeatureAttributeSidecar stores the physical_id in its asset
tracking column (#2296).

Pure-unit tests — no live DB.  The three scenarios:

1. Context carries ``_asset_physical_id`` (pre-resolved by the async
   orchestrator) → physical_id is written to the sidecar column (COLUMNAR
   and JSONB modes).
2. Context carries only ``asset_id`` (no pre-resolution, e.g. legacy call
   site) → logical id is written as fallback — row is never silently dropped.
3. After a rename the ``_asset_physical_id`` in context is still the same
   uuid → the sidecar row key is unchanged; the virtual-asset→items join
   keeps working on the new logical name.
"""
from __future__ import annotations

import pytest

from dynastore.modules.storage.drivers.pg_sidecars.attributes import (
    FeatureAttributeSidecar,
)
from dynastore.modules.storage.drivers.pg_sidecars.attributes_config import (
    FeatureAttributeSidecarConfig,
    AttributeStorageMode,
)

# A stable fake UUID representing the immutable physical identity.
_PHYSICAL_ID = "01900000-dead-beef-dead-beefdeadbeef"
_LOGICAL_ID = "my-logical-asset-name"
_GEOID = "01900000-0000-0000-0000-000000000001"


def _make_sidecar(storage_mode: AttributeStorageMode = AttributeStorageMode.COLUMNAR):
    cfg = FeatureAttributeSidecarConfig(
        asset_id_field="asset_id",
        storage_mode=storage_mode,
    )
    return FeatureAttributeSidecar(config=cfg)


def _minimal_feature() -> dict:
    return {
        "type": "Feature",
        "id": "ext-001",
        "geometry": {"type": "Point", "coordinates": [0.0, 0.0]},
        "properties": {},
    }


# ---------------------------------------------------------------------------
# 1. COLUMNAR mode — physical_id preferred over logical id
# ---------------------------------------------------------------------------


class TestColumnarModeAssetIdStamping:
    def test_physical_id_written_when_pre_resolved(self):
        """When _asset_physical_id is in context the sidecar writes the uuid."""
        sidecar = _make_sidecar(AttributeStorageMode.COLUMNAR)
        ctx = {
            "geoid": _GEOID,
            "asset_id": _LOGICAL_ID,
            "_asset_physical_id": _PHYSICAL_ID,
        }
        payload = sidecar.prepare_upsert_payload(_minimal_feature(), ctx)
        assert payload is not None
        assert payload.get("asset_id") == _PHYSICAL_ID, (
            "Sidecar must store the physical_id (UUIDv7) not the logical id"
        )

    def test_logical_id_used_as_fallback_when_no_pre_resolution(self):
        """When only asset_id is in context (no physical pre-resolution) the
        logical id is written — no row is silently dropped."""
        sidecar = _make_sidecar(AttributeStorageMode.COLUMNAR)
        ctx = {
            "geoid": _GEOID,
            "asset_id": _LOGICAL_ID,
        }
        payload = sidecar.prepare_upsert_payload(_minimal_feature(), ctx)
        assert payload is not None
        assert payload.get("asset_id") == _LOGICAL_ID, (
            "Logical id must be the fallback when _asset_physical_id is absent"
        )

    def test_rename_leaves_physical_id_unchanged(self):
        """After a rename the physical_id in context must remain the same uuid.

        This simulates: before rename context["_asset_physical_id"] = PHYSICAL_ID
        (logical id was "old-name"), after rename logical id becomes "new-name"
        but physical_id stays PHYSICAL_ID.  The sidecar must still write
        PHYSICAL_ID so the virtual-asset→items join is unaffected.
        """
        sidecar = _make_sidecar(AttributeStorageMode.COLUMNAR)
        ctx_before = {
            "geoid": _GEOID,
            "asset_id": "old-name",
            "_asset_physical_id": _PHYSICAL_ID,
        }
        ctx_after = {
            "geoid": _GEOID,
            "asset_id": "new-name",          # logical id changed by rename
            "_asset_physical_id": _PHYSICAL_ID,  # physical_id unchanged
        }
        payload_before = sidecar.prepare_upsert_payload(_minimal_feature(), ctx_before)
        payload_after = sidecar.prepare_upsert_payload(_minimal_feature(), ctx_after)
        assert payload_before is not None and payload_after is not None
        assert payload_before["asset_id"] == payload_after["asset_id"] == _PHYSICAL_ID, (
            "Physical_id must be identical before and after a rename"
        )

    def test_no_asset_id_in_context_skips_column(self):
        """When no asset_id at all is in context the column is not included."""
        sidecar = _make_sidecar(AttributeStorageMode.COLUMNAR)
        ctx = {"geoid": _GEOID}
        payload = sidecar.prepare_upsert_payload(_minimal_feature(), ctx)
        assert payload is not None
        assert "asset_id" not in payload, (
            "asset_id column must not be included when context carries no asset id"
        )


# ---------------------------------------------------------------------------
# 2. JSONB mode — same expectations, different storage path
# ---------------------------------------------------------------------------


class TestJsonbModeAssetIdStamping:
    def test_physical_id_written_when_pre_resolved(self):
        sidecar = _make_sidecar(AttributeStorageMode.JSONB)
        ctx = {
            "geoid": _GEOID,
            "asset_id": _LOGICAL_ID,
            "_asset_physical_id": _PHYSICAL_ID,
        }
        payload = sidecar.prepare_upsert_payload(_minimal_feature(), ctx)
        assert payload is not None
        assert payload.get("asset_id") == _PHYSICAL_ID, (
            "JSONB-mode sidecar must store the physical_id"
        )

    def test_logical_id_fallback_when_no_pre_resolution(self):
        sidecar = _make_sidecar(AttributeStorageMode.JSONB)
        ctx = {
            "geoid": _GEOID,
            "asset_id": _LOGICAL_ID,
        }
        payload = sidecar.prepare_upsert_payload(_minimal_feature(), ctx)
        assert payload is not None
        assert payload.get("asset_id") == _LOGICAL_ID

    def test_rename_leaves_physical_id_unchanged(self):
        sidecar = _make_sidecar(AttributeStorageMode.JSONB)
        ctx_before = {
            "geoid": _GEOID,
            "asset_id": "old-name",
            "_asset_physical_id": _PHYSICAL_ID,
        }
        ctx_after = {
            "geoid": _GEOID,
            "asset_id": "new-name",
            "_asset_physical_id": _PHYSICAL_ID,
        }
        p_before = sidecar.prepare_upsert_payload(_minimal_feature(), ctx_before)
        p_after = sidecar.prepare_upsert_payload(_minimal_feature(), ctx_after)
        assert p_before is not None and p_after is not None
        assert p_before["asset_id"] == p_after["asset_id"] == _PHYSICAL_ID


# ---------------------------------------------------------------------------
# 3. resolve_physical_id_for_context async helper
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_resolve_physical_id_for_context_injects_physical_id(monkeypatch):
    """resolve_physical_id_for_context should call the protocol resolver and
    inject _asset_physical_id into the context."""
    from unittest.mock import AsyncMock
    import dynastore.modules.storage.drivers.pg_sidecars.attributes as attr_mod

    sidecar = _make_sidecar(AttributeStorageMode.COLUMNAR)
    ctx = {"asset_id": _LOGICAL_ID}

    mock_assets = AsyncMock()
    mock_assets.resolve_asset_physical_id = AsyncMock(return_value=_PHYSICAL_ID)
    monkeypatch.setattr(attr_mod, "get_protocol", lambda _p: mock_assets)

    await sidecar.resolve_physical_id_for_context(
        ctx, catalog_id="mycat", collection_id="mycoll"
    )

    assert ctx.get("_asset_physical_id") == _PHYSICAL_ID, (
        "resolve_physical_id_for_context must inject _asset_physical_id into context"
    )


@pytest.mark.asyncio
async def test_resolve_physical_id_for_context_leaves_context_unchanged_on_miss(monkeypatch):
    """When the resolver returns None the context must be left unchanged so
    prepare_upsert_payload falls back to the logical id gracefully."""
    from unittest.mock import AsyncMock
    import dynastore.modules.storage.drivers.pg_sidecars.attributes as attr_mod

    sidecar = _make_sidecar(AttributeStorageMode.COLUMNAR)
    ctx = {"asset_id": _LOGICAL_ID}

    mock_assets = AsyncMock()
    mock_assets.resolve_asset_physical_id = AsyncMock(return_value=None)
    monkeypatch.setattr(attr_mod, "get_protocol", lambda _p: mock_assets)

    await sidecar.resolve_physical_id_for_context(
        ctx, catalog_id="mycat", collection_id="mycoll"
    )

    assert "_asset_physical_id" not in ctx, (
        "Context must not contain _asset_physical_id when resolver returns None"
    )


@pytest.mark.asyncio
async def test_resolve_physical_id_for_context_no_op_when_no_asset_id_field(monkeypatch):
    """When the sidecar has no asset_id_field the helper is a no-op."""
    from unittest.mock import AsyncMock
    import dynastore.modules.storage.drivers.pg_sidecars.attributes as attr_mod

    cfg = FeatureAttributeSidecarConfig(asset_id_field=None)
    sidecar = FeatureAttributeSidecar(config=cfg)
    ctx = {"asset_id": _LOGICAL_ID}

    mock_assets = AsyncMock()
    monkeypatch.setattr(attr_mod, "get_protocol", lambda _p: mock_assets)

    await sidecar.resolve_physical_id_for_context(
        ctx, catalog_id="mycat", collection_id="mycoll"
    )

    mock_assets.resolve_asset_physical_id.assert_not_called()
    assert "_asset_physical_id" not in ctx


@pytest.mark.asyncio
async def test_resolve_physical_id_for_context_no_op_when_protocol_unavailable(monkeypatch):
    """When get_protocol returns None the helper is a no-op — no AttributeError."""
    import dynastore.modules.storage.drivers.pg_sidecars.attributes as attr_mod

    sidecar = _make_sidecar(AttributeStorageMode.COLUMNAR)
    ctx = {"asset_id": _LOGICAL_ID}

    monkeypatch.setattr(attr_mod, "get_protocol", lambda _p: None)

    await sidecar.resolve_physical_id_for_context(
        ctx, catalog_id="mycat", collection_id="mycoll"
    )

    assert "_asset_physical_id" not in ctx
