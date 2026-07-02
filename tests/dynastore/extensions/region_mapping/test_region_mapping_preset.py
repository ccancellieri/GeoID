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

"""Unit tests for the ``region_mapping`` collection-scoped preset
(dynastore#443 Phase 1). All external I/O is mocked — no DB.
"""
from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest


def _make_ctx(catalogs: Any = None, config: Any = None, scope: str = "catalog:fao/collection:countries") -> Any:
    from dynastore.modules.storage.presets.preset import PresetContext

    return PresetContext(
        db=MagicMock(),
        iam=None,
        policy=None,
        config=config,
        tasks=None,
        cron=None,
        libs=None,
        principal=None,
        scope=scope,
        catalogs=catalogs,
    )


def _catalogs_stub(existing_catalog: Any = MagicMock(), existing_collection: Any = MagicMock()) -> MagicMock:
    catalogs = MagicMock()
    catalogs.get_catalog_model = AsyncMock(return_value=existing_catalog)
    catalogs.get_collection = AsyncMock(return_value=existing_collection)
    catalogs.create_catalog = AsyncMock()
    catalogs.create_collection = AsyncMock()
    catalogs.upsert = AsyncMock()
    catalogs.delete_item = AsyncMock()
    return catalogs


class _FakeRegistryCatalogs:
    """In-memory, stateful CatalogsProtocol stand-in for the mappings
    RECORDS collection only — exercises apply()'s stale-claim cleanup and
    revoke()'s authoritative-by-mapping-id deletion end-to-end (real state
    transitions, not canned mock returns).

    ``fetch_claims_for_mapping_uncached`` resolves its catalogs handle via
    the global protocol registry (not ``ctx.catalogs``), so tests using this
    fake must route ``get_protocol(CatalogsProtocol)`` to the same instance
    passed as ``ctx.catalogs`` (see the module-scoped patch in the test that
    uses this fixture).
    """

    def __init__(self) -> None:
        self.rows: dict = {}  # item_id -> properties dict
        self.get_catalog_model = AsyncMock(return_value=MagicMock())
        self.get_collection = AsyncMock(return_value=MagicMock())
        self.create_catalog = AsyncMock()
        self.create_collection = AsyncMock()

    async def upsert(self, catalog_id: str, collection_id: str, item: dict) -> None:
        self.rows[item["id"]] = dict(item["properties"])

    async def delete_item(self, catalog_id: str, collection_id: str, item_id: str) -> None:
        self.rows.pop(item_id, None)

    async def search_items(self, catalog_id: str, collection_id: str, request: Any) -> list:
        matched = []
        for props in self.rows.values():
            if all(props.get(f.field) == f.value for f in request.filters):
                feature = MagicMock()
                feature.properties = props
                matched.append(feature)
        return matched


# ---------------------------------------------------------------------------
# apply()
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_apply_requires_collection_scope() -> None:
    from dynastore.extensions.region_mapping.presets.region_mapping import (
        REGION_MAPPING_PRESET, RegionMappingParams,
    )

    ctx = _make_ctx(catalogs=_catalogs_stub(), scope="platform")
    params = RegionMappingParams(column="adm0_code")

    with pytest.raises(ValueError, match="requires a collection scope"):
        await REGION_MAPPING_PRESET.apply(params, "platform", ctx)


@pytest.mark.asyncio
async def test_apply_ensures_registry_provisioned() -> None:
    from dynastore.extensions.region_mapping.presets.region_mapping import (
        REGION_MAPPING_PRESET, RegionMappingParams,
    )

    catalogs = _catalogs_stub()
    ctx = _make_ctx(catalogs=catalogs)
    params = RegionMappingParams(column="adm0_code", alias="country")

    with patch(
        "dynastore.extensions.region_mapping.presets.region_mapping.ensure_registry_provisioned",
        AsyncMock(),
    ) as ensure_mock, patch(
        "dynastore.extensions.region_mapping.presets.region_mapping.invalidate_serving_caches",
    ):
        await REGION_MAPPING_PRESET.apply(params, "catalog:fao/collection:countries", ctx)

    ensure_mock.assert_awaited_once_with(ctx)


@pytest.mark.asyncio
async def test_apply_upserts_one_record_per_claim_with_scoped_ids() -> None:
    from dynastore.extensions.region_mapping.presets.region_mapping import (
        REGION_MAPPING_PRESET, RegionMappingParams,
    )

    catalogs = _catalogs_stub()
    ctx = _make_ctx(catalogs=catalogs)
    params = RegionMappingParams(
        column="adm0_code", alias="country", extra_aliases=["adm0"],
    )

    with patch(
        "dynastore.extensions.region_mapping.presets.region_mapping.ensure_registry_provisioned",
        AsyncMock(),
    ), patch(
        "dynastore.extensions.region_mapping.presets.region_mapping.invalidate_serving_caches",
    ) as invalidate_mock:
        descriptor = await REGION_MAPPING_PRESET.apply(
            params, "catalog:fao/collection:countries", ctx,
        )

    # 4 distinct claims: adm0_code, country, adm0, fao_country.
    assert catalogs.upsert.await_count == 4
    written_ids = {call.args[2]["id"] for call in catalogs.upsert.await_args_list}
    assert all(i.startswith("fao_countries__") for i in written_ids)
    assert len(written_ids) == 4

    assert descriptor.payload["mapping_id"] == "fao_countries"
    assert descriptor.payload["catalog_id"] == "fao"
    assert descriptor.payload["collection_id"] == "countries"
    assert set(descriptor.payload["item_ids"]) == written_ids

    invalidate_mock.assert_called_once()


@pytest.mark.asyncio
async def test_apply_primary_record_carries_role_and_metadata() -> None:
    from dynastore.extensions.region_mapping.presets.region_mapping import (
        REGION_MAPPING_PRESET, RegionMappingParams,
    )

    catalogs = _catalogs_stub()
    ctx = _make_ctx(catalogs=catalogs)
    params = RegionMappingParams(column="adm0_code", alias="country", title="Countries")

    with patch(
        "dynastore.extensions.region_mapping.presets.region_mapping.ensure_registry_provisioned",
        AsyncMock(),
    ), patch(
        "dynastore.extensions.region_mapping.presets.region_mapping.invalidate_serving_caches",
    ):
        await REGION_MAPPING_PRESET.apply(params, "catalog:fao/collection:countries", ctx)

    records = [call.args[2] for call in catalogs.upsert.await_args_list]
    primary = next(r for r in records if r["properties"]["role"] == "primary")
    assert primary["properties"]["claim"] == "country"
    assert primary["properties"]["src_catalog"] == "fao"
    assert primary["properties"]["src_collection"] == "countries"
    assert primary["properties"]["region_prop"] == "adm0_code"
    assert primary["properties"]["title"] == "Countries"


@pytest.mark.asyncio
async def test_apply_rejects_regex_metacharacter_claims() -> None:
    from dynastore.extensions.region_mapping.presets.region_mapping import (
        REGION_MAPPING_PRESET, RegionMappingParams,
    )

    catalogs = _catalogs_stub()
    ctx = _make_ctx(catalogs=catalogs)
    params = RegionMappingParams(column="adm0.code")

    with patch(
        "dynastore.extensions.region_mapping.presets.region_mapping.ensure_registry_provisioned",
        AsyncMock(),
    ):
        with pytest.raises(ValueError, match="regex metacharacters"):
            await REGION_MAPPING_PRESET.apply(params, "catalog:fao/collection:countries", ctx)

    catalogs.upsert.assert_not_awaited()


@pytest.mark.asyncio
async def test_apply_lets_duplicate_claim_error_propagate() -> None:
    """A UniqueViolationError from upsert (23505) must NOT be caught here —
    the global exception-handler chain maps it to HTTP 409."""
    from dynastore.extensions.region_mapping.presets.region_mapping import (
        REGION_MAPPING_PRESET, RegionMappingParams,
    )
    from dynastore.modules.db_config.exceptions import UniqueViolationError

    catalogs = _catalogs_stub()
    catalogs.upsert = AsyncMock(side_effect=UniqueViolationError("claim_ci already claimed"))
    ctx = _make_ctx(catalogs=catalogs)
    params = RegionMappingParams(column="adm0_code")

    with patch(
        "dynastore.extensions.region_mapping.presets.region_mapping.ensure_registry_provisioned",
        AsyncMock(),
    ):
        with pytest.raises(UniqueViolationError):
            await REGION_MAPPING_PRESET.apply(params, "catalog:fao/collection:countries", ctx)


# ---------------------------------------------------------------------------
# apply() — stale-claim cleanup on a force re-apply (review finding 2)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_apply_deletes_stale_claims_dropped_from_new_set() -> None:
    """A force re-apply with a changed alias must delete the claims that
    fell out of the new set — they must not be orphaned (squatting the
    claim_ci UNIQUE constraint, still appearing in Terria's aliases)."""
    from dynastore.extensions.region_mapping.presets.region_mapping import (
        REGION_MAPPING_PRESET, RegionMappingParams,
    )

    catalogs = _catalogs_stub()
    ctx = _make_ctx(catalogs=catalogs)
    # Previously applied with alias="country"; now re-applying alias="nation".
    params = RegionMappingParams(column="adm0_code", alias="nation")

    existing_claims = [
        {"claim_ci": "adm0_code", "claim": "adm0_code"},    # stays in the new set
        {"claim_ci": "country", "claim": "country"},         # dropped -> must be deleted
        {"claim_ci": "fao_country", "claim": "fao_country"}, # dropped -> must be deleted
    ]

    with patch(
        "dynastore.extensions.region_mapping.presets.region_mapping.ensure_registry_provisioned",
        AsyncMock(),
    ), patch(
        "dynastore.extensions.region_mapping.presets.region_mapping.fetch_claims_for_mapping_uncached",
        AsyncMock(return_value=existing_claims),
    ), patch(
        "dynastore.extensions.region_mapping.presets.region_mapping.invalidate_serving_caches",
    ):
        await REGION_MAPPING_PRESET.apply(params, "catalog:fao/collection:countries", ctx)

    deleted_ids = {call.args[2] for call in catalogs.delete_item.await_args_list}
    assert deleted_ids == {"fao_countries__country", "fao_countries__fao_country"}

    written_ids = {call.args[2]["id"] for call in catalogs.upsert.await_args_list}
    assert written_ids == {
        "fao_countries__adm0_code", "fao_countries__nation", "fao_countries__fao_nation",
    }


@pytest.mark.asyncio
async def test_apply_deletes_nothing_when_claim_set_unchanged() -> None:
    """A plain idempotent re-apply (same params) must not delete any claims."""
    from dynastore.extensions.region_mapping.presets.region_mapping import (
        REGION_MAPPING_PRESET, RegionMappingParams,
    )

    catalogs = _catalogs_stub()
    ctx = _make_ctx(catalogs=catalogs)
    params = RegionMappingParams(column="adm0_code", alias="country")

    existing_claims = [
        {"claim_ci": "adm0_code", "claim": "adm0_code"},
        {"claim_ci": "country", "claim": "country"},
        {"claim_ci": "fao_country", "claim": "fao_country"},
    ]

    with patch(
        "dynastore.extensions.region_mapping.presets.region_mapping.ensure_registry_provisioned",
        AsyncMock(),
    ), patch(
        "dynastore.extensions.region_mapping.presets.region_mapping.fetch_claims_for_mapping_uncached",
        AsyncMock(return_value=existing_claims),
    ), patch(
        "dynastore.extensions.region_mapping.presets.region_mapping.invalidate_serving_caches",
    ):
        await REGION_MAPPING_PRESET.apply(params, "catalog:fao/collection:countries", ctx)

    catalogs.delete_item.assert_not_awaited()


@pytest.mark.asyncio
async def test_force_reapply_then_revoke_leaves_zero_claims_for_mapping() -> None:
    """End-to-end (fake, stateful catalogs): re-apply with a changed alias
    frees the dropped claim for another mapping to register, and revoke
    after the re-apply removes every claim currently owned by the mapping —
    not just the ids recorded by whichever apply produced the descriptor."""
    from dynastore.extensions.region_mapping.presets.region_mapping import (
        REGION_MAPPING_PRESET, RegionMappingParams,
    )
    from dynastore.models.protocols.catalogs import CatalogsProtocol

    fake = _FakeRegistryCatalogs()

    with patch(
        "dynastore.extensions.region_mapping.registry_data.get_protocol",
        lambda proto: fake if proto is CatalogsProtocol else None,
    ), patch(
        "dynastore.extensions.region_mapping.presets.region_mapping.ensure_registry_provisioned",
        AsyncMock(),
    ), patch(
        "dynastore.extensions.region_mapping.presets.region_mapping.invalidate_serving_caches",
    ):
        ctx = _make_ctx(catalogs=fake)

        await REGION_MAPPING_PRESET.apply(
            RegionMappingParams(column="adm0_code", alias="country"),
            "catalog:fao/collection:countries", ctx,
        )
        assert set(fake.rows) == {
            "fao_countries__adm0_code", "fao_countries__country", "fao_countries__fao_country",
        }

        descriptor = await REGION_MAPPING_PRESET.apply(
            RegionMappingParams(column="adm0_code", alias="nation"),
            "catalog:fao/collection:countries", ctx,
        )
        assert set(fake.rows) == {
            "fao_countries__adm0_code", "fao_countries__nation", "fao_countries__fao_nation",
        }, "the dropped 'country'/'fao_country' claims must be gone, not orphaned"

        # The freed claim_ci is now registrable by a different mapping.
        await REGION_MAPPING_PRESET.apply(
            RegionMappingParams(column="country_code", alias="country"),
            "catalog:who/collection:regions", ctx,
        )
        assert "who_regions__country" in fake.rows

        await REGION_MAPPING_PRESET.revoke(descriptor, ctx)

    remaining_for_mapping = {k for k in fake.rows if k.startswith("fao_countries__")}
    assert remaining_for_mapping == set(), (
        "revoke after a re-apply must leave zero claims for the mapping"
    )
    assert "who_regions__country" in fake.rows, (
        "revoking one mapping must not touch another mapping's claims"
    )


# ---------------------------------------------------------------------------
# revoke()
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_revoke_deletes_recorded_item_ids() -> None:
    from dynastore.extensions.region_mapping.presets.region_mapping import REGION_MAPPING_PRESET
    from dynastore.modules.storage.presets.preset import AppliedDescriptor

    catalogs = _catalogs_stub()
    ctx = _make_ctx(catalogs=catalogs)
    descriptor = AppliedDescriptor(payload={
        "item_ids": ["fao_countries__country", "fao_countries__adm0_code"],
    })

    with patch(
        "dynastore.extensions.region_mapping.presets.region_mapping.invalidate_serving_caches",
    ) as invalidate_mock:
        await REGION_MAPPING_PRESET.revoke(descriptor, ctx)

    assert catalogs.delete_item.await_count == 2
    deleted = {call.args[2] for call in catalogs.delete_item.await_args_list}
    assert deleted == {"fao_countries__country", "fao_countries__adm0_code"}
    invalidate_mock.assert_called_once()


@pytest.mark.asyncio
async def test_revoke_is_authoritative_by_mapping_id_not_descriptor_ids() -> None:
    """revoke() must delete every claim CURRENTLY sharing the mapping_id —
    not just the (possibly stale) ids recorded on the descriptor it was
    handed, since lifecycle.py overwrites the stored descriptor on every
    apply and an intervening re-apply may have added/removed claims."""
    from dynastore.extensions.region_mapping.presets.region_mapping import REGION_MAPPING_PRESET
    from dynastore.modules.storage.presets.preset import AppliedDescriptor

    catalogs = _catalogs_stub()
    ctx = _make_ctx(catalogs=catalogs)
    # Descriptor only knows one id from an old apply; the registry's CURRENT
    # state (as of a later re-apply) has three claims for this mapping.
    descriptor = AppliedDescriptor(payload={
        "mapping_id": "fao_countries",
        "item_ids": ["fao_countries__adm0_code"],
    })
    current_claims = [
        {"claim_ci": "adm0_code"}, {"claim_ci": "nation"}, {"claim_ci": "fao_nation"},
    ]

    with patch(
        "dynastore.extensions.region_mapping.presets.region_mapping.fetch_claims_for_mapping_uncached",
        AsyncMock(return_value=current_claims),
    ), patch(
        "dynastore.extensions.region_mapping.presets.region_mapping.invalidate_serving_caches",
    ):
        await REGION_MAPPING_PRESET.revoke(descriptor, ctx)

    deleted = {call.args[2] for call in catalogs.delete_item.await_args_list}
    assert deleted == {
        "fao_countries__adm0_code", "fao_countries__nation", "fao_countries__fao_nation",
    }


@pytest.mark.asyncio
async def test_revoke_falls_back_to_descriptor_ids_when_query_returns_empty() -> None:
    from dynastore.extensions.region_mapping.presets.region_mapping import REGION_MAPPING_PRESET
    from dynastore.modules.storage.presets.preset import AppliedDescriptor

    catalogs = _catalogs_stub()
    ctx = _make_ctx(catalogs=catalogs)
    descriptor = AppliedDescriptor(payload={
        "mapping_id": "fao_countries",
        "item_ids": ["fao_countries__country"],
    })

    with patch(
        "dynastore.extensions.region_mapping.presets.region_mapping.fetch_claims_for_mapping_uncached",
        AsyncMock(return_value=[]),
    ), patch(
        "dynastore.extensions.region_mapping.presets.region_mapping.invalidate_serving_caches",
    ):
        await REGION_MAPPING_PRESET.revoke(descriptor, ctx)

    deleted = {call.args[2] for call in catalogs.delete_item.await_args_list}
    assert deleted == {"fao_countries__country"}


# ---------------------------------------------------------------------------
# dry_run() — warnings
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_dry_run_warns_on_conflicting_claim() -> None:
    from dynastore.extensions.region_mapping.presets.region_mapping import (
        REGION_MAPPING_PRESET, RegionMappingParams,
    )

    catalogs = _catalogs_stub()
    catalogs.get_collection = AsyncMock(return_value=MagicMock(extent=None))
    ctx = _make_ctx(catalogs=catalogs)
    params = RegionMappingParams(column="adm0_code", alias="country")

    conflicting = {"mapping_id": "who_countries"}

    async def _fake_fetch(claim_ci: str):
        return conflicting if claim_ci == "country" else None

    with patch(
        "dynastore.extensions.region_mapping.presets.region_mapping.fetch_claim_by_ci",
        _fake_fetch,
    ):
        plan = await REGION_MAPPING_PRESET.dry_run(
            params, "catalog:fao/collection:countries", ctx,
        )

    assert any("already registered to mapping 'who_countries'" in w for w in plan.warnings)


@pytest.mark.asyncio
async def test_dry_run_warns_on_missing_column_in_declared_schema() -> None:
    from dynastore.extensions.region_mapping.presets.region_mapping import (
        REGION_MAPPING_PRESET, RegionMappingParams,
    )
    from dynastore.modules.storage.driver_config import ItemsSchema
    from dynastore.models.protocols.field_definition import FieldDefinition

    catalogs = _catalogs_stub()
    catalogs.get_collection = AsyncMock(return_value=MagicMock(extent=None))
    config = MagicMock()
    config.get_config = AsyncMock(
        return_value=ItemsSchema(
            fields={"some_other_field": FieldDefinition(name="some_other_field", data_type="string")}
        )
    )
    ctx = _make_ctx(catalogs=catalogs, config=config)
    params = RegionMappingParams(column="adm0_code")

    with patch(
        "dynastore.extensions.region_mapping.presets.region_mapping.fetch_claim_by_ci",
        AsyncMock(return_value=None),
    ):
        plan = await REGION_MAPPING_PRESET.dry_run(
            params, "catalog:fao/collection:countries", ctx,
        )

    assert any("not found in fao/countries" in w for w in plan.warnings)


@pytest.mark.asyncio
async def test_dry_run_warns_on_degenerate_extent() -> None:
    from dynastore.extensions.region_mapping.presets.region_mapping import (
        REGION_MAPPING_PRESET, RegionMappingParams,
    )

    collection = MagicMock()
    collection.extent.spatial.bbox = [[0.0, 0.0, 0.0, 0.0]]
    catalogs = _catalogs_stub()
    catalogs.get_collection = AsyncMock(return_value=collection)
    ctx = _make_ctx(catalogs=catalogs)
    params = RegionMappingParams(column="adm0_code")

    with patch(
        "dynastore.extensions.region_mapping.presets.region_mapping.fetch_claim_by_ci",
        AsyncMock(return_value=None),
    ):
        plan = await REGION_MAPPING_PRESET.dry_run(
            params, "catalog:fao/collection:countries", ctx,
        )

    assert any("world-bounds fallback" in w for w in plan.warnings)


@pytest.mark.asyncio
async def test_dry_run_no_warnings_for_clean_apply() -> None:
    from dynastore.extensions.region_mapping.presets.region_mapping import (
        REGION_MAPPING_PRESET, RegionMappingParams,
    )

    collection = MagicMock()
    collection.extent.spatial.bbox = [[-180.0, -90.0, 180.0, 90.0]]
    catalogs = _catalogs_stub()
    catalogs.get_collection = AsyncMock(return_value=collection)
    ctx = _make_ctx(catalogs=catalogs)
    params = RegionMappingParams(column="adm0_code")

    with patch(
        "dynastore.extensions.region_mapping.presets.region_mapping.fetch_claim_by_ci",
        AsyncMock(return_value=None),
    ):
        plan = await REGION_MAPPING_PRESET.dry_run(
            params, "catalog:fao/collection:countries", ctx,
        )

    assert plan.warnings == ()


def test_preset_registered_in_registry() -> None:
    import dynastore.extensions.region_mapping.presets  # noqa: F401 -- side-effect import
    from dynastore.modules.storage.presets.registry import get_preset

    preset = get_preset("region_mapping")
    assert preset is not None
    assert preset.name == "region_mapping"
    assert preset.catalog_scopable is False
