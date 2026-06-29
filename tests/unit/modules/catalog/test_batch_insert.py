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

"""Unit tests for the batched bulk-insert fast path.

Covers:
- ``_batch_hub_insert``: multi-row hub INSERT SQL shape.
- ``_batch_upsert_sidecar_rows``: multi-row sidecar INSERT SQL shape,
  geometry-column wrapping, range-column expansion.
- ``batch_insert_or_update_distributed``: INSERT-partition batching,
  UPDATE-partition per-row fallback, sidecar rejection, REFUSE_RETURN,
  REFUSE_FAIL / ConflictError, multiple sidecars, mix of new+existing.
- ``ItemsWritePolicy.enable_batch_insert``: config field default and
  mutability.
"""

from __future__ import annotations

import re
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from dynastore.modules.catalog.item_distributed import ItemDistributedMixin
from dynastore.modules.storage.computed_fields import ComputedField, ComputedKind
from dynastore.modules.storage.driver_config import (
    ItemsWritePolicy,
    ItemsPostgresqlDriverConfig,
    ResolvedIdentityRule,
    WriteConflictPolicy,
)
from dynastore.modules.storage.errors import ConflictError


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _geoid() -> str:
    return str(uuid.uuid4())


def _make_plan(
    geoid: Optional[str] = None,
    external_id: Optional[str] = None,
    geometry_hash: Optional[str] = None,
) -> Dict[str, Any]:
    """Minimal prepared plan matching Phase 2 output shape."""
    g = geoid or _geoid()
    return {
        "geoid": g,
        "hub_payload": {
            "geoid": g,
            "transaction_time": datetime(2026, 1, 1, tzinfo=timezone.utc),
            "deleted_at": None,
        },
        "sidecar_payloads": {},
        "item_context": {
            "geoid": g,
            "operation": "insert",
            "_raw_item": {"type": "Feature"},
            **({"external_id": external_id} if external_id else {}),
            **({"geometry_hash": geometry_hash} if geometry_hash else {}),
        },
    }


def _make_sidecar(
    sidecar_id: str = "attributes",
    mandatory: bool = True,
    acceptable: bool = True,
    identity_cols: Optional[List[str]] = None,
) -> MagicMock:
    sc = MagicMock()
    sc.sidecar_id = sidecar_id
    sc.is_mandatory.return_value = mandatory
    sc.is_acceptable.return_value = acceptable
    sc.get_identity_columns.return_value = list(identity_cols or ["geoid"])
    sc.geometry_value_columns = MagicMock(return_value=set())
    sc.finalize_upsert_payload = MagicMock(
        side_effect=lambda payload, hub, ctx: dict(payload)
    )
    sc.validate_insert = MagicMock(return_value=MagicMock(valid=True))
    # Suppress the place-stats path: getattr(sidecar, "prepare_place_upsert_payload", None)
    # returns a MagicMock on a bare MagicMock — set it to None so place code is skipped.
    sc.prepare_place_upsert_payload = None
    return sc


def _make_col_config() -> MagicMock:
    cfg = MagicMock(spec=ItemsPostgresqlDriverConfig)
    cfg.partitioning = MagicMock(enabled=False, partition_keys=[])
    return cfg


class _FakeMixin(ItemDistributedMixin):
    """Minimal ItemDistributedMixin host that provides the required _Host stubs."""

    async def _resolve_physical_schema(
        self, catalog_id: str, *, db_resource: Any = None
    ) -> str:
        return "test_schema"

    async def _resolve_physical_table(
        self, catalog_id: str, collection_id: str, *, db_resource: Any = None
    ) -> Optional[str]:
        return "test_hub"

    async def _resolve_read_policy(
        self, catalog_id: str, collection_id: str
    ) -> Optional[Any]:
        return None

    def map_row_to_feature(self, row: Dict, col_config: Any, read_policy: Any = None) -> Any:
        return row

    async def insert_or_update_distributed(self, *args: Any, **kwargs: Any) -> Optional[Dict]:
        # Overridden in tests that inspect per-row fallback.
        geoid = kwargs.get("hub_payload", args[3] if len(args) > 3 else {}).get("geoid")
        return {"geoid": geoid}

    def _strip_undeclared_columns(self, sidecar: Any, payload: Dict, col_config: Any) -> Dict:
        return payload


@pytest.fixture
def mixin() -> _FakeMixin:
    return _FakeMixin()


@pytest.fixture
def default_write_policy() -> ItemsWritePolicy:
    return ItemsWritePolicy()


# ---------------------------------------------------------------------------
# Config field
# ---------------------------------------------------------------------------


class TestConfigFlag:
    def test_defaults_to_true(self) -> None:
        policy = ItemsWritePolicy()
        assert policy.enable_batch_insert is True

    def test_can_be_set_true(self) -> None:
        policy = ItemsWritePolicy(enable_batch_insert=True)
        assert policy.enable_batch_insert is True

    def test_field_is_mutable_after_materialisation(self) -> None:
        # Mutable fields survive model_copy even if other fields are frozen.
        policy = ItemsWritePolicy(enable_batch_insert=False)
        updated = policy.model_copy(update={"enable_batch_insert": True})
        assert updated.enable_batch_insert is True


# ---------------------------------------------------------------------------
# _batch_hub_insert — SQL shape
# ---------------------------------------------------------------------------


class TestBatchHubInsert:
    @pytest.mark.asyncio
    async def test_empty_payloads_returns_empty(self, mixin: _FakeMixin) -> None:
        result = await mixin._batch_hub_insert(MagicMock(), "s", "t", [])
        assert result == []

    @pytest.mark.asyncio
    async def test_single_row_sql_shape(self, mixin: _FakeMixin) -> None:
        """Single-row batch insert produces valid INSERT ... VALUES (...) RETURNING *."""
        g = _geoid()
        payload = {"geoid": g, "transaction_time": datetime(2026, 1, 1, tzinfo=timezone.utc), "deleted_at": None}
        captured: List[Dict] = []

        # When patching an instance method, `self` is the first positional arg.
        async def _fake_execute(self_: Any, conn: Any, **kwargs: Any) -> List[Dict]:
            captured.append({"kwargs": kwargs})
            return [{"geoid": g, "transaction_time": payload["transaction_time"], "deleted_at": None}]

        with patch(
            "dynastore.modules.catalog.item_distributed.DQLQuery.execute",
            new=_fake_execute,
        ):
            result = await mixin._batch_hub_insert(MagicMock(), "myschema", "myhub", [payload])

        assert len(result) == 1
        assert result[0]["geoid"] == g
        # geoid bind-param suffix _h_geoid_0
        assert captured[0]["kwargs"].get("_h_geoid_0") == g

    @pytest.mark.asyncio
    async def test_three_rows_three_returned(self, mixin: _FakeMixin) -> None:
        geoids = [_geoid() for _ in range(3)]
        payloads = [
            {"geoid": g, "transaction_time": datetime(2026, 1, 1, tzinfo=timezone.utc), "deleted_at": None}
            for g in geoids
        ]

        async def _fake_execute(self_: Any, conn: Any, **kwargs: Any) -> List[Dict]:
            return [{"geoid": g} for g in geoids]

        with patch(
            "dynastore.modules.catalog.item_distributed.DQLQuery.execute",
            new=_fake_execute,
        ):
            result = await mixin._batch_hub_insert(MagicMock(), "s", "t", payloads)

        assert [r.get("geoid") for r in result] == geoids

    @pytest.mark.asyncio
    async def test_union_columns_null_for_missing(self, mixin: _FakeMixin) -> None:
        """When some rows lack a column the others have, SQL uses NULL filler."""
        g1, g2 = _geoid(), _geoid()
        p1 = {"geoid": g1, "transaction_time": datetime(2026, 1, 1, tzinfo=timezone.utc), "deleted_at": None}
        p2 = {"geoid": g2, "transaction_time": datetime(2026, 1, 1, tzinfo=timezone.utc), "deleted_at": None, "extra_col": "val"}
        captured_sql: List[str] = []

        class _CaptureDQL:
            def __init__(self, sql: str, **_kw: Any) -> None:
                captured_sql.append(sql)

            async def execute(self, conn: Any, **kwargs: Any) -> List[Dict]:
                return [{"geoid": g1}, {"geoid": g2}]

        with patch(
            "dynastore.modules.catalog.item_distributed.DQLQuery",
            new=_CaptureDQL,
        ):
            await mixin._batch_hub_insert(MagicMock(), "s", "t", [p1, p2])

        sql = captured_sql[0]
        # Row 1 (p1) has NULL for extra_col because it's absent.
        assert "NULL" in sql


# ---------------------------------------------------------------------------
# _batch_upsert_sidecar_rows — SQL shape
# ---------------------------------------------------------------------------


class TestBatchUpsertSidecarRows:
    @pytest.mark.asyncio
    async def test_empty_payloads_is_noop(self, mixin: _FakeMixin) -> None:
        # Should return without calling the DB.
        with patch(
            "dynastore.modules.catalog.item_distributed.DQLQuery.execute",
            new=AsyncMock(side_effect=AssertionError("should not call DB")),
        ):
            await mixin._batch_upsert_sidecar_rows(MagicMock(), "s", "t", [])

    @pytest.mark.asyncio
    async def test_geometry_col_gets_st_geomfromewkb_wrapper(
        self, mixin: _FakeMixin
    ) -> None:
        payloads = [
            {"geoid": _geoid(), "geom": "deadbeef01020304"},
        ]
        captured_sql: List[str] = []

        class _Cap:
            def __init__(self, sql: str, **_kw: Any) -> None:
                captured_sql.append(sql)

            async def execute(self, conn: Any, **kwargs: Any) -> None:
                return None

        with patch("dynastore.modules.catalog.item_distributed.DQLQuery", new=_Cap):
            await mixin._batch_upsert_sidecar_rows(
                MagicMock(), "s", "t", payloads,
                conflict_cols=["geoid"], geom_cols={"geom"},
            )

        assert "ST_GeomFromEWKB(decode(" in captured_sql[0]
        assert "deadbeef" not in captured_sql[0]  # value is in params, not literal

    @pytest.mark.asyncio
    async def test_on_conflict_do_update_generated(self, mixin: _FakeMixin) -> None:
        payloads = [{"geoid": _geoid(), "external_id": "eid-1"}]
        captured_sql: List[str] = []

        class _Cap:
            def __init__(self, sql: str, **_kw: Any) -> None:
                captured_sql.append(sql)

            async def execute(self, conn: Any, **kwargs: Any) -> None:
                return None

        with patch("dynastore.modules.catalog.item_distributed.DQLQuery", new=_Cap):
            await mixin._batch_upsert_sidecar_rows(
                MagicMock(), "s", "t", payloads, conflict_cols=["geoid"],
            )

        sql = captured_sql[0]
        assert "ON CONFLICT" in sql
        assert "DO UPDATE SET" in sql
        assert '"external_id" = EXCLUDED."external_id"' in sql

    @pytest.mark.asyncio
    async def test_multi_row_produces_multiple_value_tuples(
        self, mixin: _FakeMixin
    ) -> None:
        g1, g2, g3 = _geoid(), _geoid(), _geoid()
        payloads = [
            {"geoid": g, "external_id": f"eid-{i}"}
            for i, g in enumerate([g1, g2, g3])
        ]
        captured_sql: List[str] = []

        class _Cap:
            def __init__(self, sql: str, **_kw: Any) -> None:
                captured_sql.append(sql)

            async def execute(self, conn: Any, **kwargs: Any) -> None:
                return None

        with patch("dynastore.modules.catalog.item_distributed.DQLQuery", new=_Cap):
            await mixin._batch_upsert_sidecar_rows(
                MagicMock(), "s", "t", payloads, conflict_cols=["geoid"],
            )

        # The VALUES clause contains exactly 3 row tuples.  Extract only the
        # VALUES section (stop before ON CONFLICT) to avoid counting the
        # conflict-target parenthesis as an extra tuple.
        sql = captured_sql[0]
        values_section = sql.split("VALUES", 1)[1].split("ON CONFLICT", 1)[0]
        # Each row tuple starts with "(" — count them.
        tuple_count = len(re.findall(r"\(", values_section))
        assert tuple_count == 3

    @pytest.mark.asyncio
    async def test_param_names_are_suffixed_per_row(self, mixin: _FakeMixin) -> None:
        """Bind params must be unique per row to avoid collision."""
        payloads = [
            {"geoid": _geoid(), "external_id": "a"},
            {"geoid": _geoid(), "external_id": "b"},
        ]
        captured_params: Dict[str, Any] = {}

        class _Cap:
            def __init__(self, sql: str, **_kw: Any) -> None:
                pass

            async def execute(self, conn: Any, **kwargs: Any) -> None:
                captured_params.update(kwargs)
                return None

        with patch("dynastore.modules.catalog.item_distributed.DQLQuery", new=_Cap):
            await mixin._batch_upsert_sidecar_rows(
                MagicMock(), "s", "t", payloads, conflict_cols=["geoid"],
            )

        # Row 0 and row 1 bind external_id under different names.
        assert "_s_external_id_0" in captured_params
        assert "_s_external_id_1" in captured_params
        assert captured_params["_s_external_id_0"] == "a"
        assert captured_params["_s_external_id_1"] == "b"


# ---------------------------------------------------------------------------
# batch_insert_or_update_distributed — semantics
# ---------------------------------------------------------------------------


class TestBatchInsertOrUpdateDistributed:
    """Integration tests for the full batched path using mock sidecars and DB."""

    def _patch_batch_identity_empty(self) -> Any:
        """Return empty identity maps (all plans → INSERT partition)."""
        return patch.multiple(
            _FakeMixin,
            _batch_resolve_by_external_id=AsyncMock(return_value={}),
            _batch_resolve_by_geometry_hash=AsyncMock(return_value={}),
        )

    @pytest.mark.asyncio
    async def test_all_new_features_go_to_insert_partition(
        self, mixin: _FakeMixin
    ) -> None:
        """All-INSERT chunk: batch hub INSERT + batch sidecar INSERT called once each."""
        plans = [_make_plan(external_id=f"eid-{i}") for i in range(3)]
        sidecar = _make_sidecar("attributes")
        col_config = _make_col_config()
        policy = ItemsWritePolicy(enable_batch_insert=True)

        hub_rows = [{"geoid": p["geoid"]} for p in plans]

        with self._patch_batch_identity_empty():
            with patch.object(mixin, "_batch_hub_insert", AsyncMock(return_value=hub_rows)) as mock_hub:
                with patch.object(mixin, "_batch_upsert_sidecar_rows", AsyncMock()) as mock_sc:
                    results, rejections = await mixin.batch_insert_or_update_distributed(
                        conn=MagicMock(),
                        catalog_id="cat", collection_id="col",
                        plans=plans,
                        col_config=col_config,
                        sidecars=[sidecar],
                        write_policy=policy,
                    )

        assert rejections == []
        assert len(results) == 3
        assert all(r is not None for r in results)
        # Hub batch INSERT called once with all 3 hub payloads.
        mock_hub.assert_awaited_once()
        hub_call_payloads = mock_hub.call_args[0][3]  # positional arg: payloads list
        assert len(hub_call_payloads) == 3
        # Sidecar batch INSERT called once (one sidecar).
        mock_sc.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_sidecar_rejection_surfaces_in_rejections(
        self, mixin: _FakeMixin
    ) -> None:
        """A feature rejected by is_acceptable appears in rejections, not results."""
        plans = [
            _make_plan(external_id="eid-0"),
            _make_plan(external_id="eid-1"),
        ]
        # Second sidecar rejects the second feature.
        sidecar = _make_sidecar("attributes", acceptable=True)
        sidecar.is_acceptable = MagicMock(side_effect=[True, False])
        col_config = _make_col_config()

        hub_rows = [{"geoid": plans[0]["geoid"]}]

        with self._patch_batch_identity_empty():
            with patch.object(mixin, "_batch_hub_insert", AsyncMock(return_value=hub_rows)):
                with patch.object(mixin, "_batch_upsert_sidecar_rows", AsyncMock()):
                    results, rejections = await mixin.batch_insert_or_update_distributed(
                        conn=MagicMock(),
                        catalog_id="cat", collection_id="col",
                        plans=plans,
                        col_config=col_config,
                        sidecars=[sidecar],
                        write_policy=ItemsWritePolicy(),
                    )

        assert len(rejections) == 1
        assert rejections[0]["reason"] == "sidecar_not_acceptable"
        assert results[0] is not None   # plan 0 accepted
        assert results[1] is None       # plan 1 rejected

    @pytest.mark.asyncio
    async def test_existing_feature_goes_to_update_partition(
        self, mixin: _FakeMixin
    ) -> None:
        """A feature whose external_id maps to an existing geoid goes per-row."""
        existing_geoid = _geoid()
        new_geoid = _geoid()
        plan_existing = _make_plan(external_id="eid-0")
        plan_new = _make_plan(external_id="eid-1")

        sidecar = _make_sidecar("attributes")
        col_config = _make_col_config()

        # Identity map: eid-0 matches existing_geoid.
        ext_id_map = {
            "eid-0": {"external_id": "eid-0", "geoid": existing_geoid, "geometry_hash": None},
        }

        per_row_called_for: List[str] = []

        async def _fake_per_row(conn: Any, cat: str, col: str, hub: Dict, sc: Dict,
                                col_config: Any, sidecars: List, processing_context: Dict,
                                **_kw: Any) -> Dict:
            per_row_called_for.append(hub.get("geoid"))
            return {"geoid": existing_geoid}

        with patch.object(
            mixin, "_batch_resolve_by_external_id", AsyncMock(return_value=ext_id_map)
        ):
            with patch.object(
                mixin, "_batch_resolve_by_geometry_hash", AsyncMock(return_value={})
            ):
                with patch.object(
                    mixin, "_batch_hub_insert",
                    AsyncMock(return_value=[{"geoid": new_geoid}])
                ):
                    with patch.object(mixin, "_batch_upsert_sidecar_rows", AsyncMock()):
                        with patch.object(
                            mixin, "insert_or_update_distributed",
                            new=AsyncMock(side_effect=_fake_per_row)
                        ):
                            results, rejections = await mixin.batch_insert_or_update_distributed(
                                conn=MagicMock(),
                                catalog_id="cat", collection_id="col",
                                plans=[plan_existing, plan_new],
                                col_config=col_config,
                                sidecars=[sidecar],
                                write_policy=ItemsWritePolicy(),
                            )

        assert rejections == []
        # plan_existing → UPDATE partition → per-row → returns existing_geoid
        assert results[0]["geoid"] == existing_geoid
        # plan_new → INSERT partition → batch → returns new_geoid
        assert results[1]["geoid"] == new_geoid
        # Per-row was called for the existing plan's hub geoid.
        assert plan_existing["geoid"] in per_row_called_for

    @pytest.mark.asyncio
    async def test_refuse_return_echoes_existing_record(
        self, mixin: _FakeMixin
    ) -> None:
        """REFUSE_RETURN: existing record is echoed without any DB write."""
        existing_geoid = _geoid()
        plan = _make_plan(external_id="eid-0")
        ext_id_map = {
            "eid-0": {"external_id": "eid-0", "geoid": existing_geoid},
        }
        policy = ItemsWritePolicy(on_conflict=WriteConflictPolicy.REFUSE_RETURN)

        with patch.object(
            mixin, "_batch_resolve_by_external_id", AsyncMock(return_value=ext_id_map)
        ):
            with patch.object(
                mixin, "_batch_resolve_by_geometry_hash", AsyncMock(return_value={})
            ):
                with patch.object(
                    mixin, "_batch_hub_insert",
                    AsyncMock(side_effect=AssertionError("should not be called"))
                ):
                    results, rejections = await mixin.batch_insert_or_update_distributed(
                        conn=MagicMock(),
                        catalog_id="cat", collection_id="col",
                        plans=[plan],
                        col_config=_make_col_config(),
                        sidecars=[_make_sidecar()],
                        write_policy=policy,
                    )

        assert rejections == []
        assert results[0] == {"geoid": existing_geoid, "_refuse_return": True}

    @pytest.mark.asyncio
    async def test_refuse_fail_raises_conflict_error(self, mixin: _FakeMixin) -> None:
        """REFUSE_FAIL: ConflictError propagates immediately."""
        existing_geoid = _geoid()
        plan = _make_plan(external_id="eid-0")
        ext_id_map = {
            "eid-0": {"external_id": "eid-0", "geoid": existing_geoid},
        }
        policy = ItemsWritePolicy(on_conflict=WriteConflictPolicy.REFUSE_FAIL)

        with patch.object(
            mixin, "_batch_resolve_by_external_id", AsyncMock(return_value=ext_id_map)
        ):
            with patch.object(
                mixin, "_batch_resolve_by_geometry_hash", AsyncMock(return_value={})
            ):
                with pytest.raises(ConflictError):
                    await mixin.batch_insert_or_update_distributed(
                        conn=MagicMock(),
                        catalog_id="cat", collection_id="col",
                        plans=[plan],
                        col_config=_make_col_config(),
                        sidecars=[_make_sidecar()],
                        write_policy=policy,
                    )

    @pytest.mark.asyncio
    async def test_multiple_sidecars_each_gets_batch_call(
        self, mixin: _FakeMixin
    ) -> None:
        """Two sidecars each trigger one batched sidecar INSERT."""
        plans = [_make_plan(external_id=f"eid-{i}") for i in range(2)]
        sc_a = _make_sidecar("attributes")
        sc_b = _make_sidecar("geometries")
        col_config = _make_col_config()
        hub_rows = [{"geoid": p["geoid"]} for p in plans]

        batch_sc_calls: List[str] = []

        async def _fake_batch_sc(conn: Any, schema: str, table: str,
                                  payloads: List, **_kw: Any) -> None:
            batch_sc_calls.append(table)

        with self._patch_batch_identity_empty():
            with patch.object(mixin, "_batch_hub_insert", AsyncMock(return_value=hub_rows)):
                with patch.object(
                    mixin, "_batch_upsert_sidecar_rows",
                    AsyncMock(side_effect=_fake_batch_sc)
                ):
                    results, rejections = await mixin.batch_insert_or_update_distributed(
                        conn=MagicMock(),
                        catalog_id="cat", collection_id="col",
                        plans=plans,
                        col_config=col_config,
                        sidecars=[sc_a, sc_b],
                        write_policy=ItemsWritePolicy(),
                    )

        assert rejections == []
        assert len(results) == 2
        # One batch call per sidecar.
        assert len(batch_sc_calls) == 2
        assert "test_hub_attributes" in batch_sc_calls
        assert "test_hub_geometries" in batch_sc_calls

    @pytest.mark.asyncio
    async def test_non_batchable_rule_uses_per_row_identity(
        self, mixin: _FakeMixin
    ) -> None:
        """A rule with attributes_hash matcher falls back to per-row identity resolution."""
        plan = _make_plan()
        # attributes_hash is not in the batchable set.
        rule = ResolvedIdentityRule(
            match_on=[ComputedField(kind=ComputedKind.ATTRIBUTES_HASH)]
        )
        # Override resolved_identity to return a non-batchable rule.
        policy = MagicMock(spec=ItemsWritePolicy)
        policy.resolved_identity.return_value = [rule]
        policy.enable_validity = False
        policy.on_batch_conflict = None
        policy.on_conflict = WriteConflictPolicy.UPDATE

        # _resolve_rule is called per-row since the rule is non-batchable.
        resolve_rule_calls: List[Any] = []

        async def _fake_resolve_rule(rule: Any, conn: Any, schema: Any, table: Any,
                                      ctx: Any, sidecars: Any) -> Optional[Dict]:
            resolve_rule_calls.append(ctx)
            return None  # no match → INSERT partition

        hub_rows = [{"geoid": plan["geoid"]}]

        with patch(
            "dynastore.modules.catalog.item_distributed._resolve_rule",
            new=_fake_resolve_rule,
        ):
            with patch.object(mixin, "_batch_hub_insert", AsyncMock(return_value=hub_rows)):
                with patch.object(mixin, "_batch_upsert_sidecar_rows", AsyncMock()):
                    await mixin.batch_insert_or_update_distributed(
                        conn=MagicMock(),
                        catalog_id="cat", collection_id="col",
                        plans=[plan],
                        col_config=_make_col_config(),
                        sidecars=[_make_sidecar()],
                        write_policy=policy,
                    )

        # per-row identity resolution was invoked.
        assert len(resolve_rule_calls) == 1

    @pytest.mark.asyncio
    async def test_mixed_chunk_new_and_existing_and_rejected(
        self, mixin: _FakeMixin
    ) -> None:
        """Three plans: one INSERT, one UPDATE, one rejection — all in one chunk."""
        existing_geoid = _geoid()
        plan_new = _make_plan(external_id="eid-new")
        plan_existing = _make_plan(external_id="eid-existing")
        plan_rejected = _make_plan(external_id="eid-rejected")

        # Sidecar rejects plan_rejected; accepts the other two.
        sidecar = _make_sidecar("attributes")
        call_count = [0]

        def _acceptable(feature: Dict, ctx: Dict) -> bool:
            call_count[0] += 1
            return ctx.get("external_id") != "eid-rejected"

        sidecar.is_acceptable = _acceptable

        ext_id_map = {
            "eid-existing": {
                "external_id": "eid-existing",
                "geoid": existing_geoid,
                "geometry_hash": None,
            },
        }

        per_row_results: List[Dict] = []

        async def _per_row(conn: Any, cat: str, col: str, hub: Dict, sc: Dict,
                            col_config: Any, sidecars: List, processing_context: Dict,
                            **_kw: Any) -> Dict:
            r = {"geoid": existing_geoid}
            per_row_results.append(r)
            return r

        hub_rows = [{"geoid": plan_new["geoid"]}]  # only the new plan's hub row

        with patch.object(
            mixin, "_batch_resolve_by_external_id", AsyncMock(return_value=ext_id_map)
        ):
            with patch.object(
                mixin, "_batch_resolve_by_geometry_hash", AsyncMock(return_value={})
            ):
                with patch.object(
                    mixin, "_batch_hub_insert", AsyncMock(return_value=hub_rows)
                ):
                    with patch.object(mixin, "_batch_upsert_sidecar_rows", AsyncMock()):
                        with patch.object(
                            mixin, "insert_or_update_distributed",
                            new=AsyncMock(side_effect=_per_row),
                        ):
                            results, rejections = (
                                await mixin.batch_insert_or_update_distributed(
                                    conn=MagicMock(),
                                    catalog_id="cat", collection_id="col",
                                    plans=[plan_new, plan_existing, plan_rejected],
                                    col_config=_make_col_config(),
                                    sidecars=[sidecar],
                                    write_policy=ItemsWritePolicy(),
                                )
                            )

        # Rejected plan → None in results + one rejection dict.
        assert results[2] is None
        assert len(rejections) == 1
        assert rejections[0]["reason"] == "sidecar_not_acceptable"
        # New plan → INSERT partition → batch hub row.
        assert results[0]["geoid"] == plan_new["geoid"]
        # Existing plan → UPDATE partition → per-row.
        assert results[1]["geoid"] == existing_geoid
        assert len(per_row_results) == 1

    @pytest.mark.asyncio
    async def test_and_rule_intersection_geometry_hash(
        self, mixin: _FakeMixin
    ) -> None:
        """AND rule: both external_id AND geometry_hash must map to the same geoid."""
        existing_geoid = _geoid()
        g_hash = "abc123"
        plan = _make_plan(external_id="eid-0", geometry_hash=g_hash)

        # Both matchers map to the SAME existing_geoid → AND succeeds → UPDATE.
        ext_id_map = {
            "eid-0": {"external_id": "eid-0", "geoid": existing_geoid},
        }
        geom_hash_map = {
            g_hash: {"geometry_hash": g_hash, "geoid": existing_geoid},
        }

        and_rule = ResolvedIdentityRule(
            match_on=[
                ComputedField(kind=ComputedKind.EXTERNAL_ID),
                ComputedField(kind=ComputedKind.GEOMETRY_HASH),
            ]
        )
        policy = MagicMock(spec=ItemsWritePolicy)
        policy.resolved_identity.return_value = [and_rule]
        policy.enable_validity = False
        policy.on_batch_conflict = None
        policy.on_conflict = WriteConflictPolicy.UPDATE
        # Matched rule's on_match is None so effective_on_conflict falls back to policy.on_conflict.
        and_rule_with_no_on_match = ResolvedIdentityRule(
            match_on=[
                ComputedField(kind=ComputedKind.EXTERNAL_ID),
                ComputedField(kind=ComputedKind.GEOMETRY_HASH),
            ],
            on_match=None,
        )
        policy.resolved_identity.return_value = [and_rule_with_no_on_match]

        per_row_geoids: List[str] = []

        async def _per_row(conn: Any, cat: str, col: str, hub: Dict, sc: Dict,
                            col_config: Any, sidecars: List, processing_context: Dict,
                            **_kw: Any) -> Dict:
            per_row_geoids.append(hub.get("geoid"))
            return {"geoid": existing_geoid}

        with patch.object(
            mixin, "_batch_resolve_by_external_id", AsyncMock(return_value=ext_id_map)
        ):
            with patch.object(
                mixin, "_batch_resolve_by_geometry_hash", AsyncMock(return_value=geom_hash_map)
            ):
                with patch.object(
                    mixin, "insert_or_update_distributed",
                    new=AsyncMock(side_effect=_per_row),
                ):
                    results, rejections = await mixin.batch_insert_or_update_distributed(
                        conn=MagicMock(),
                        catalog_id="cat", collection_id="col",
                        plans=[plan],
                        col_config=_make_col_config(),
                        sidecars=[_make_sidecar()],
                        write_policy=policy,
                    )

        # AND rule matched → UPDATE partition → per-row.
        assert rejections == []
        assert results[0]["geoid"] == existing_geoid
        assert plan["geoid"] in per_row_geoids

    @pytest.mark.asyncio
    async def test_and_rule_no_geoid_intersection_goes_to_insert(
        self, mixin: _FakeMixin
    ) -> None:
        """AND rule: different geoids for each field → no intersection → INSERT."""
        geoid_a = _geoid()
        geoid_b = _geoid()
        g_hash = "xyz789"
        plan = _make_plan(external_id="eid-0", geometry_hash=g_hash)

        # external_id maps to geoid_a; geometry_hash maps to geoid_b → no intersection.
        ext_id_map = {"eid-0": {"external_id": "eid-0", "geoid": geoid_a}}
        geom_hash_map = {g_hash: {"geometry_hash": g_hash, "geoid": geoid_b}}

        and_rule = ResolvedIdentityRule(
            match_on=[
                ComputedField(kind=ComputedKind.EXTERNAL_ID),
                ComputedField(kind=ComputedKind.GEOMETRY_HASH),
            ]
        )
        policy = MagicMock(spec=ItemsWritePolicy)
        policy.resolved_identity.return_value = [and_rule]
        policy.enable_validity = False
        policy.on_batch_conflict = None
        # _select_effective_on_conflict reads policy.on_conflict when matched_rule is None.
        policy.on_conflict = WriteConflictPolicy.UPDATE

        hub_rows = [{"geoid": plan["geoid"]}]

        with patch.object(
            mixin, "_batch_resolve_by_external_id", AsyncMock(return_value=ext_id_map)
        ):
            with patch.object(
                mixin, "_batch_resolve_by_geometry_hash", AsyncMock(return_value=geom_hash_map)
            ):
                with patch.object(
                    mixin, "_batch_hub_insert", AsyncMock(return_value=hub_rows)
                ) as mock_hub:
                    with patch.object(mixin, "_batch_upsert_sidecar_rows", AsyncMock()):
                        results, rejections = await mixin.batch_insert_or_update_distributed(
                            conn=MagicMock(),
                            catalog_id="cat", collection_id="col",
                            plans=[plan],
                            col_config=_make_col_config(),
                            sidecars=[_make_sidecar()],
                            write_policy=policy,
                        )

        # No AND match → INSERT partition.
        assert rejections == []
        mock_hub.assert_awaited_once()
        assert results[0]["geoid"] == plan["geoid"]


# ---------------------------------------------------------------------------
# Fix 1 — _batch_hub_insert empty-RETURNING contract
# ---------------------------------------------------------------------------


class TestBatchHubInsertEmptyReturning:
    @pytest.mark.asyncio
    async def test_empty_returning_raises_runtime_error(
        self, mixin: _FakeMixin
    ) -> None:
        """PG INSERT…RETURNING never silently drops rows.  An empty result is
        an invariant violation and must raise RuntimeError, not return [{}] * N,
        which would crash at result_geoids = [r['geoid'] for r in write_results]
        with a silent KeyError."""
        g = _geoid()
        payload = {
            "geoid": g,
            "transaction_time": datetime(2026, 1, 1, tzinfo=timezone.utc),
            "deleted_at": None,
        }

        async def _empty_execute(self_: Any, conn: Any, **kwargs: Any) -> List[Dict]:
            return []

        with patch(
            "dynastore.modules.catalog.item_distributed.DQLQuery.execute",
            new=_empty_execute,
        ):
            with pytest.raises(RuntimeError, match="no rows"):
                await mixin._batch_hub_insert(MagicMock(), "s", "t", [payload])

    @pytest.mark.asyncio
    async def test_caller_guard_rejects_row_without_geoid(
        self, mixin: _FakeMixin
    ) -> None:
        """Even if _batch_hub_insert returns a row without 'geoid', the caller
        in batch_insert_or_update_distributed catches it before it can propagate
        as a KeyError in result_geoids."""
        plan = _make_plan(external_id="eid-0")
        sidecar = _make_sidecar("attributes")

        # Return a row that has no 'geoid' key.
        with self._patch_batch_identity_empty():
            with patch.object(
                mixin,
                "_batch_hub_insert",
                AsyncMock(return_value=[{"not_geoid": "x"}]),
            ):
                with pytest.raises(RuntimeError, match="without 'geoid'"):
                    await mixin.batch_insert_or_update_distributed(
                        conn=MagicMock(),
                        catalog_id="cat",
                        collection_id="col",
                        plans=[plan],
                        col_config=_make_col_config(),
                        sidecars=[sidecar],
                        write_policy=ItemsWritePolicy(),
                    )

    def _patch_batch_identity_empty(self) -> Any:
        return patch.multiple(
            _FakeMixin,
            _batch_resolve_by_external_id=AsyncMock(return_value={}),
            _batch_resolve_by_geometry_hash=AsyncMock(return_value={}),
        )


# ---------------------------------------------------------------------------
# Fix 2 — place-stats loop must respect the non-mandatory sidecar guard
# ---------------------------------------------------------------------------


class TestBatchPlaceStatsMandatoryGuard:
    def _patch_batch_identity_empty(self) -> Any:
        return patch.multiple(
            _FakeMixin,
            _batch_resolve_by_external_id=AsyncMock(return_value={}),
            _batch_resolve_by_geometry_hash=AsyncMock(return_value={}),
        )

    @pytest.mark.asyncio
    async def test_no_place_row_for_plan_with_no_sidecar_data(
        self, mixin: _FakeMixin
    ) -> None:
        """A non-mandatory sidecar with no data for a feature must produce no
        main sidecar row AND no place row for that feature.  This mirrors the
        per-row path (_execute_distributed_insert) where the `continue` at the
        top of the sidecar block exits both the main INSERT and the place stats
        call for the same plan."""
        plan_with_data = _make_plan(external_id="eid-0")
        plan_without_data = _make_plan(external_id="eid-1")

        place_calls: List[str] = []

        def _prep_place(raw_item: Dict, ctx: Dict) -> Optional[Dict]:
            ext_id = ctx.get("external_id")
            place_calls.append(str(ext_id))
            return {"place": "somewhere"}

        # Non-mandatory sidecar: only plan_with_data has payload data.
        optional_sidecar = _make_sidecar("geometries", mandatory=False)
        # Restore prepare_place_upsert_payload to a real function so the path runs.
        optional_sidecar.prepare_place_upsert_payload = _prep_place
        # plan_with_data has sidecar data; plan_without_data does not.
        plan_with_data["sidecar_payloads"]["geometries"] = {"geom": "deadbeef"}

        hub_rows = [
            {"geoid": plan_with_data["geoid"]},
            {"geoid": plan_without_data["geoid"]},
        ]
        place_batch_calls: List[List] = []

        async def _fake_batch_upsert(
            conn: Any, schema: Any, table: str, payloads: List, **_kw: Any
        ) -> None:
            if "place" in table:
                place_batch_calls.append(list(payloads))

        with self._patch_batch_identity_empty():
            with patch.object(
                mixin, "_batch_hub_insert", AsyncMock(return_value=hub_rows)
            ):
                with patch.object(
                    mixin,
                    "_batch_upsert_sidecar_rows",
                    AsyncMock(side_effect=_fake_batch_upsert),
                ):
                    await mixin.batch_insert_or_update_distributed(
                        conn=MagicMock(),
                        catalog_id="cat",
                        collection_id="col",
                        plans=[plan_with_data, plan_without_data],
                        col_config=_make_col_config(),
                        sidecars=[optional_sidecar],
                        write_policy=ItemsWritePolicy(),
                    )

        # prepare_place_upsert_payload should have been called ONLY for the
        # plan that has sidecar data (plan_with_data).
        assert "eid-0" in place_calls
        assert "eid-1" not in place_calls

        # Only one place row should have been written.
        assert len(place_batch_calls) == 1
        assert len(place_batch_calls[0]) == 1


# ---------------------------------------------------------------------------
# Regression — UUID type mismatch between DB RETURNING and payload geoid
# ---------------------------------------------------------------------------


class TestBatchHubInsertUuidNormalization:
    """asyncpg returns UUID columns as uuid.UUID objects; payload geoids are
    pre-generated strings.  Without str()-normalization on both sides of the
    by_geoid dict lookup, every row silently falls back to per-row (the dict
    miss returns {} and the caller's 'geoid not in hub_row' guard triggers).

    These tests use uuid.UUID objects on the DB side to reflect real asyncpg
    behaviour and verify the fix resolves all rows correctly.
    """

    @pytest.mark.asyncio
    async def test_uuid_returning_maps_to_string_payload_geoid(
        self, mixin: _FakeMixin
    ) -> None:
        """DB returns uuid.UUID, payload carries str — must resolve, not fall back."""
        g_str = str(uuid.uuid4())
        g_uuid = uuid.UUID(g_str)  # what asyncpg actually delivers

        payload = {
            "geoid": g_str,  # string, as generated by _geoid() in Phase 2
            "transaction_time": datetime(2026, 1, 1, tzinfo=timezone.utc),
            "deleted_at": None,
        }

        async def _uuid_execute(self_: Any, conn: Any, **kwargs: Any) -> List[Dict]:
            # Simulate asyncpg RETURNING: geoid is a uuid.UUID object, not str.
            return [{"geoid": g_uuid, "transaction_time": payload["transaction_time"], "deleted_at": None}]

        with patch(
            "dynastore.modules.catalog.item_distributed.DQLQuery.execute",
            new=_uuid_execute,
        ):
            result = await mixin._batch_hub_insert(MagicMock(), "s", "t", [payload])

        assert len(result) == 1
        # The returned hub-row dict must be found (non-empty) and contain geoid.
        assert result[0], "mapping miss: UUID key vs str lookup returned empty dict"
        assert "geoid" in result[0], "hub row missing geoid key after UUID normalization"

    @pytest.mark.asyncio
    async def test_three_uuid_returning_rows_map_in_order(
        self, mixin: _FakeMixin
    ) -> None:
        """Three rows returned in arbitrary order by PG — all must map back to the
        correct payload index even when RETURNING row geoids are uuid.UUID objects."""
        g_strs = [str(uuid.uuid4()) for _ in range(3)]
        payloads = [
            {"geoid": g, "transaction_time": datetime(2026, 1, 1, tzinfo=timezone.utc)}
            for g in g_strs
        ]
        # DB returns rows in reverse order with UUID objects.
        reversed_uuid_rows = [
            {"geoid": uuid.UUID(g)} for g in reversed(g_strs)
        ]

        async def _uuid_execute(self_: Any, conn: Any, **kwargs: Any) -> List[Dict]:
            return reversed_uuid_rows

        with patch(
            "dynastore.modules.catalog.item_distributed.DQLQuery.execute",
            new=_uuid_execute,
        ):
            result = await mixin._batch_hub_insert(MagicMock(), "s", "t", payloads)

        assert len(result) == 3
        for i, g_str in enumerate(g_strs):
            assert result[i], f"row {i} mapping miss (geoid={g_str})"
            assert "geoid" in result[i], f"row {i} missing geoid"

    @pytest.mark.asyncio
    async def test_old_string_returning_still_works(self, mixin: _FakeMixin) -> None:
        """str()-normalization on the key side is idempotent: str(str_val) == str_val,
        so existing callers that already return string geoids are unaffected."""
        g = str(uuid.uuid4())
        payload = {
            "geoid": g,
            "transaction_time": datetime(2026, 1, 1, tzinfo=timezone.utc),
            "deleted_at": None,
        }

        async def _str_execute(self_: Any, conn: Any, **kwargs: Any) -> List[Dict]:
            return [{"geoid": g}]  # string, as before

        with patch(
            "dynastore.modules.catalog.item_distributed.DQLQuery.execute",
            new=_str_execute,
        ):
            result = await mixin._batch_hub_insert(MagicMock(), "s", "t", [payload])

        assert len(result) == 1
        assert result[0]["geoid"] == g
