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

"""Unit tests for ``ItemsWritePolicy.resolve_external_id`` and the
``_walk_external_id_path`` helper, plus the ``_IndexStampContext`` /
``_apply_index_stamp`` stamp behaviour.

Covers:
 * ``resolve_external_id`` with no path → feature.id fallback
 * ``resolve_external_id`` with a configured path → path-walk value
 * Dot-walk (``properties.code``)
 * Properties fallback (dot-less path missing at root → ``properties[path]``)
 * Missing value → ``None``
 * Stamp: no path configured → dispatched payload ``_external_id`` == inbound id
 * Stamp: configured path → path value extracted from payload
 * Stamp: geoid-swapped result Feature still yields inbound id when
   ``ctx.external_id`` is pre-resolved (regression guard)
"""
from __future__ import annotations

from typing import Any, Dict, Optional

from dynastore.modules.storage.driver_config import (
    ItemsWritePolicy,
    _walk_external_id_path,
)
from dynastore.modules.storage.computed_fields import DeriveSpec


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _policy_with_path(path: str) -> ItemsWritePolicy:
    """Build an ``ItemsWritePolicy`` that uses ``path`` as the external_id source."""
    return ItemsWritePolicy(derive=DeriveSpec(external_id=path))


def _policy_no_path() -> ItemsWritePolicy:
    """Policy with no external_id path configured."""
    return ItemsWritePolicy()


# ---------------------------------------------------------------------------
# _walk_external_id_path
# ---------------------------------------------------------------------------


class TestWalkExternalIdPath:
    def test_top_level_key(self):
        feature = {"myid": "ROOT", "properties": {"myid": "PROPS"}}
        assert _walk_external_id_path(feature, "myid") == "ROOT"

    def test_dot_walk_nested(self):
        feature = {"properties": {"code": "GLOSIS_01"}}
        assert _walk_external_id_path(feature, "properties.code") == "GLOSIS_01"

    def test_properties_fallback_when_root_missing(self):
        feature = {
            "type": "Feature",
            "geometry": None,
            "properties": {"ADM2_PCODE": "TG0309"},
        }
        assert _walk_external_id_path(feature, "ADM2_PCODE") == "TG0309"

    def test_no_properties_fallback_for_dot_path(self):
        # Dot paths do NOT trigger the properties fallback; they resolve literally.
        feature = {"properties": {"code": "X"}}
        # "code" is not at feature["c"]["o"]["d"]["e"]
        result = _walk_external_id_path(feature, "c.o.d.e")
        assert result is None

    def test_missing_everywhere_returns_none(self):
        feature = {"type": "Feature", "properties": {"other": "y"}}
        assert _walk_external_id_path(feature, "ADM2_PCODE") is None

    def test_exception_swallowed_returns_none(self):
        # Non-dict type at a node → returns None without raising.
        feature = {"a": "string_not_a_dict"}
        assert _walk_external_id_path(feature, "a.b") is None


# ---------------------------------------------------------------------------
# ItemsWritePolicy.resolve_external_id
# ---------------------------------------------------------------------------


class TestResolveExternalId:
    def test_no_path_uses_feature_id(self):
        policy = _policy_no_path()
        feature = {"id": "geo-abc", "properties": {}}
        assert policy.resolve_external_id(feature) == "geo-abc"

    def test_no_path_no_id_returns_none(self):
        policy = _policy_no_path()
        feature = {"properties": {}}
        assert policy.resolve_external_id(feature) is None

    def test_configured_path_wins_over_id(self):
        policy = _policy_with_path("ADM2_PCODE")
        feature = {"id": "geo-xyz", "properties": {"ADM2_PCODE": "TG0309"}}
        assert policy.resolve_external_id(feature) == "TG0309"

    def test_configured_dot_path(self):
        policy = _policy_with_path("properties.CODE")
        feature = {"id": "geo-1", "properties": {"CODE": "ITA_01"}}
        assert policy.resolve_external_id(feature) == "ITA_01"

    def test_path_missing_returns_none(self):
        policy = _policy_with_path("nonexistent_field")
        feature = {"id": "geo-1", "properties": {}}
        assert policy.resolve_external_id(feature) is None

    def test_result_is_str(self):
        policy = _policy_with_path("ADM2_PCODE")
        feature = {"properties": {"ADM2_PCODE": 42}}
        result = policy.resolve_external_id(feature)
        assert result == "42"
        assert isinstance(result, str)

    def test_id_is_str(self):
        policy = _policy_no_path()
        feature = {"id": 99, "properties": {}}
        result = policy.resolve_external_id(feature)
        assert result == "99"
        assert isinstance(result, str)


# ---------------------------------------------------------------------------
# _apply_index_stamp — stamp behaviour
# ---------------------------------------------------------------------------


class TestApplyIndexStamp:
    """Test ``_apply_index_stamp`` via ``ItemService`` seam."""

    def _stamp(
        self,
        payload: Dict[str, Any],
        *,
        external_id: Optional[str] = None,
        external_id_path: Optional[str] = None,
        asset_id: Optional[Any] = None,
        access_envelope: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        from dynastore.modules.catalog.item_service import (
            ItemService,
            _IndexStampContext,
        )
        ctx = _IndexStampContext(
            external_id=external_id,
            external_id_path=external_id_path,
            asset_id=asset_id,
            access_envelope=access_envelope,
        )
        svc = ItemService.__new__(ItemService)
        return svc._apply_index_stamp(dict(payload), ctx)

    def test_no_path_no_ctx_ext_id_no_stamp(self):
        """When neither ctx.external_id nor ctx.external_id_path is set,
        ``_external_id`` is not added to the payload."""
        payload = {"id": "geo-1", "properties": {}}
        result = self._stamp(payload)
        assert "_external_id" not in result

    def test_no_path_ctx_ext_id_stamps(self):
        """When ctx.external_id is set (pre-resolved from inbound item),
        ``_external_id`` is stamped even if the payload has no such path."""
        payload = {"id": "geo-1", "properties": {}}
        result = self._stamp(payload, external_id="ext-abc")
        assert result["_external_id"] == "ext-abc"

    def test_configured_path_stamps_from_payload(self):
        """When ctx.external_id is None but path is set, extraction runs."""
        payload = {"id": "geo-1", "properties": {"CODE": "ITA_01"}}
        result = self._stamp(payload, external_id_path="properties.CODE")
        assert result["_external_id"] == "ITA_01"

    def test_ctx_ext_id_wins_over_path(self):
        """ctx.external_id takes precedence over path extraction."""
        payload = {"id": "geo-1", "properties": {"CODE": "ITA_01"}}
        result = self._stamp(
            payload,
            external_id="override-val",
            external_id_path="properties.CODE",
        )
        assert result["_external_id"] == "override-val"

    def test_geoid_swap_regression(self):
        """After PG read-back the payload id == geoid.  When ctx.external_id
        carries the inbound id (pre-resolved before the geoid-swap), it wins
        over path extraction — ensuring the correct identity is stamped.

        Without ctx.external_id (old behaviour), path="id" would extract the
        geoid and produce the wrong ``_external_id``."""
        inbound_id = "user-supplied-id"
        geoid_after_pg = "geo-uuid-from-pg"
        # Simulate payload after PG read-back: id == geoid.
        payload_after_pg = {"id": geoid_after_pg, "properties": {}}
        # ctx.external_id was resolved from the INBOUND item before the swap.
        result = self._stamp(
            payload_after_pg,
            external_id=inbound_id,
            external_id_path="id",  # path would extract geoid — wrong
        )
        assert result["_external_id"] == inbound_id  # ctx wins

    def test_inbound_feature_id_no_path_no_stamp(self):
        """No path + no ctx.external_id → no _external_id in payload.
        This documents the current no-op behaviour for collections that
        do not configure a write policy."""
        payload = {"id": "original-id", "properties": {}}
        result = self._stamp(payload)
        assert "_external_id" not in result


# ---------------------------------------------------------------------------
# _dispatch_index_upsert — per-item external_id override (the live-path fix)
# ---------------------------------------------------------------------------


class _StubFeature:
    """Minimal Feature stand-in: ``.id`` + ``.model_dump()``."""

    def __init__(self, fid: str, props: Optional[Dict[str, Any]] = None):
        self.id = fid
        self._props = props or {}

    def model_dump(self, **_kw: Any) -> Dict[str, Any]:
        return {"id": self.id, "properties": dict(self._props)}


class _CapturingDispatcher:
    def __init__(self) -> None:
        self.ops: Any = None

    async def fan_out_bulk(
        self, _ctx: Any, ops: Any, *, tx_factory: Any = None,
    ) -> Dict[str, Any]:
        self.ops = ops
        return {}


class TestDispatchExternalIdOverride:
    """``external_id_by_id`` makes the index op carry the inbound external_id
    even though ``results`` carry the geoid as ``id`` (the geoid-swap) and the
    batch-level stamp context has no per-item value.  This is the fix for the
    private-index/external_id lookup on PG-primary catalogs."""

    def _run_dispatch(self, monkeypatch, results, external_id_by_id):
        import asyncio
        from dynastore.modules.catalog.item_service import (
            ItemService,
            _IndexStampContext,
        )
        import dynastore.modules.storage.index_dispatcher as disp_mod

        svc = ItemService.__new__(ItemService)
        svc.engine = None  # forces the non-atomic _do_dispatch path (pg_conn=None)

        async def _noop_ctx(_self, _c, _col, _pc):
            return _IndexStampContext(
                external_id=None, external_id_path=None,
                asset_id=None, access_envelope=None,
            )

        monkeypatch.setattr(
            ItemService, "_resolve_index_stamp_context", _noop_ctx,
        )
        cap = _CapturingDispatcher()
        monkeypatch.setattr(disp_mod, "get_index_dispatcher", lambda: cap)

        asyncio.run(svc._dispatch_index_upsert(
            "cat", "col", results,
            db_resource=None,
            external_id_by_id=external_id_by_id,
        ))
        return cap

    def test_override_stamps_inbound_external_id(self, monkeypatch):
        # Result id == geoid (post read-back); map carries the inbound id.
        results = [_StubFeature("geo-uuid-1")]
        cap = self._run_dispatch(
            monkeypatch, results, {"geo-uuid-1": "USER-EXT-1"},
        )
        assert cap.ops is not None and len(cap.ops) == 1
        assert cap.ops[0].payload["_external_id"] == "USER-EXT-1"

    def test_no_map_no_external_id_stamped(self, monkeypatch):
        # Without the map and with a no-op stamp ctx, nothing is stamped.
        results = [_StubFeature("geo-uuid-2")]
        cap = self._run_dispatch(monkeypatch, results, None)
        assert cap.ops is not None and len(cap.ops) == 1
        assert "_external_id" not in cap.ops[0].payload

    def test_override_only_for_matching_geoid(self, monkeypatch):
        # A result whose geoid is absent from the map is left unstamped.
        results = [_StubFeature("geo-A"), _StubFeature("geo-B")]
        cap = self._run_dispatch(monkeypatch, results, {"geo-A": "EXT-A"})
        by_id = {op.entity_id: op.payload for op in cap.ops}
        assert by_id["geo-A"]["_external_id"] == "EXT-A"
        assert "_external_id" not in by_id["geo-B"]
