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

"""Unit tests for the ``region_mappings_registry`` platform preset
(dynastore#443 Phase 1). All external I/O is mocked — no DB.
"""
from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest


def _make_ctx(
    catalogs: Any = None, config: Any = None, policy: Any = None, iam: Any = None,
) -> Any:
    from dynastore.modules.storage.presets.preset import PresetContext

    return PresetContext(
        db=MagicMock(),
        iam=iam,
        policy=policy,
        config=config,
        tasks=None,
        cron=None,
        libs=None,
        principal=None,
        scope="platform",
        catalogs=catalogs,
    )


def _empty_role(name: str) -> MagicMock:
    role = MagicMock()
    role.name = name
    role.policies = []
    role.model_copy = MagicMock(
        side_effect=lambda update: _copied_role(role, update)
    )
    return role


def _copied_role(role: MagicMock, update: dict) -> MagicMock:
    new_role = MagicMock()
    new_role.name = role.name
    new_role.policies = update.get("policies", role.policies)
    new_role.model_copy = role.model_copy
    return new_role


# ---------------------------------------------------------------------------
# apply()
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_apply_creates_catalog_and_collection_bucket_free_when_absent() -> None:
    from dynastore.extensions.region_mapping.presets.region_mappings_registry import (
        REGION_MAPPINGS_REGISTRY_PRESET,
    )
    from dynastore.modules.storage.hints import Hint

    catalogs = MagicMock()
    catalogs.get_catalog_model = AsyncMock(return_value=None)
    catalogs.get_collection = AsyncMock(return_value=None)
    catalogs.create_catalog = AsyncMock(return_value=MagicMock())
    catalogs.create_collection = AsyncMock(return_value=MagicMock())

    ctx = _make_ctx(catalogs=catalogs, config=None, policy=None, iam=None)

    await REGION_MAPPINGS_REGISTRY_PRESET.apply(
        REGION_MAPPINGS_REGISTRY_PRESET.params_model(), "platform", ctx,
    )

    catalogs.create_catalog.assert_awaited_once()
    call = catalogs.create_catalog.await_args
    assert call.args[0]["id"] == "_region_mappings_"
    assert call.kwargs["hints"] == frozenset({Hint.DEFER})

    catalogs.create_collection.assert_awaited_once()
    coll_call = catalogs.create_collection.await_args
    assert coll_call.args[0] == "_region_mappings_"
    assert coll_call.args[1]["id"] == "mappings"
    assert coll_call.args[1]["layer_config"]["collection_type"] == "RECORDS"
    assert "schema" in coll_call.args[1]


@pytest.mark.asyncio
async def test_apply_leaves_existing_catalog_and_collection_untouched() -> None:
    from dynastore.extensions.region_mapping.presets.region_mappings_registry import (
        REGION_MAPPINGS_REGISTRY_PRESET,
    )

    catalogs = MagicMock()
    catalogs.get_catalog_model = AsyncMock(return_value=MagicMock())
    catalogs.get_collection = AsyncMock(return_value=MagicMock())
    catalogs.create_catalog = AsyncMock()
    catalogs.create_collection = AsyncMock()

    ctx = _make_ctx(catalogs=catalogs)

    await REGION_MAPPINGS_REGISTRY_PRESET.apply(
        REGION_MAPPINGS_REGISTRY_PRESET.params_model(), "platform", ctx,
    )

    catalogs.create_catalog.assert_not_awaited()
    catalogs.create_collection.assert_not_awaited()


# ---------------------------------------------------------------------------
# apply() — TOCTOU recovery on a concurrent first-time create (review
# finding 3): the apply lock is per-collection scope_key, not the shared
# registry, so two first-time collection applies can race the
# check-then-act create_catalog/create_collection pair.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_apply_recovers_from_concurrent_catalog_create_race() -> None:
    from dynastore.extensions.region_mapping.presets.region_mappings_registry import (
        REGION_MAPPINGS_REGISTRY_PRESET,
    )
    from dynastore.modules.db_config.exceptions import UniqueViolationError

    catalogs = MagicMock()
    # 1st get_catalog_model: absent (race not yet visible to us). create_catalog
    # loses the race. 2nd get_catalog_model (inside the except handler):
    # the peer's catalog is now visible.
    catalogs.get_catalog_model = AsyncMock(side_effect=[None, MagicMock()])
    catalogs.create_catalog = AsyncMock(side_effect=UniqueViolationError("catalog already exists"))
    catalogs.get_collection = AsyncMock(return_value=MagicMock())
    catalogs.create_collection = AsyncMock()

    ctx = _make_ctx(catalogs=catalogs)

    # Must not raise.
    await REGION_MAPPINGS_REGISTRY_PRESET.apply(
        REGION_MAPPINGS_REGISTRY_PRESET.params_model(), "platform", ctx,
    )

    assert catalogs.get_catalog_model.await_count == 2
    catalogs.create_collection.assert_not_awaited()  # collection already existed per stub


@pytest.mark.asyncio
async def test_apply_recovers_from_concurrent_collection_create_race() -> None:
    from dynastore.extensions.region_mapping.presets.region_mappings_registry import (
        REGION_MAPPINGS_REGISTRY_PRESET,
    )
    from dynastore.modules.db_config.exceptions import UniqueViolationError

    catalogs = MagicMock()
    catalogs.get_catalog_model = AsyncMock(return_value=MagicMock())
    catalogs.get_collection = AsyncMock(side_effect=[None, MagicMock()])
    catalogs.create_collection = AsyncMock(side_effect=UniqueViolationError("collection already exists"))
    catalogs.create_catalog = AsyncMock()

    ctx = _make_ctx(catalogs=catalogs)

    await REGION_MAPPINGS_REGISTRY_PRESET.apply(
        REGION_MAPPINGS_REGISTRY_PRESET.params_model(), "platform", ctx,
    )

    assert catalogs.get_collection.await_count == 2
    catalogs.create_catalog.assert_not_awaited()


@pytest.mark.asyncio
async def test_apply_reraises_catalog_race_recovery_when_still_absent() -> None:
    """A genuine failure (not a benign concurrent-create race) must
    propagate, never be swallowed as if it were a race."""
    from dynastore.extensions.region_mapping.presets.region_mappings_registry import (
        REGION_MAPPINGS_REGISTRY_PRESET,
    )
    from dynastore.modules.db_config.exceptions import UniqueViolationError

    catalogs = MagicMock()
    catalogs.get_catalog_model = AsyncMock(side_effect=[None, None])
    catalogs.create_catalog = AsyncMock(side_effect=UniqueViolationError("boom"))
    catalogs.get_collection = AsyncMock(return_value=MagicMock())
    catalogs.create_collection = AsyncMock()

    ctx = _make_ctx(catalogs=catalogs)

    with pytest.raises(UniqueViolationError):
        await REGION_MAPPINGS_REGISTRY_PRESET.apply(
            REGION_MAPPINGS_REGISTRY_PRESET.params_model(), "platform", ctx,
        )


@pytest.mark.asyncio
async def test_apply_reraises_collection_race_recovery_when_still_absent() -> None:
    from dynastore.extensions.region_mapping.presets.region_mappings_registry import (
        REGION_MAPPINGS_REGISTRY_PRESET,
    )
    from dynastore.modules.db_config.exceptions import UniqueViolationError

    catalogs = MagicMock()
    catalogs.get_catalog_model = AsyncMock(return_value=MagicMock())
    catalogs.get_collection = AsyncMock(side_effect=[None, None])
    catalogs.create_collection = AsyncMock(side_effect=UniqueViolationError("boom"))
    catalogs.create_catalog = AsyncMock()

    ctx = _make_ctx(catalogs=catalogs)

    with pytest.raises(UniqueViolationError):
        await REGION_MAPPINGS_REGISTRY_PRESET.apply(
            REGION_MAPPINGS_REGISTRY_PRESET.params_model(), "platform", ctx,
        )


@pytest.mark.asyncio
async def test_apply_sets_catalog_lookup_audience_public() -> None:
    from dynastore.extensions.region_mapping.presets.region_mappings_registry import (
        REGION_MAPPINGS_REGISTRY_PRESET,
    )
    from dynastore.modules.iam.audience_configs import CatalogLookupAudience

    catalogs = MagicMock()
    catalogs.get_catalog_model = AsyncMock(return_value=MagicMock())
    catalogs.get_collection = AsyncMock(return_value=MagicMock())
    config = MagicMock()
    config.set_config = AsyncMock()

    ctx = _make_ctx(catalogs=catalogs, config=config)

    await REGION_MAPPINGS_REGISTRY_PRESET.apply(
        REGION_MAPPINGS_REGISTRY_PRESET.params_model(), "platform", ctx,
    )

    config.set_config.assert_awaited_once()
    call = config.set_config.await_args
    assert call.args[0] is CatalogLookupAudience
    assert call.args[1].is_public is True
    assert call.kwargs["catalog_id"] == "_region_mappings_"


@pytest.mark.asyncio
async def test_apply_binds_direct_policy_to_unauthenticated_role() -> None:
    from dynastore.extensions.region_mapping.presets.region_mappings_registry import (
        REGION_MAPPINGS_REGISTRY_PRESET,
    )

    catalogs = MagicMock()
    catalogs.get_catalog_model = AsyncMock(return_value=MagicMock())
    catalogs.get_collection = AsyncMock(return_value=MagicMock())

    policy = MagicMock()
    policy.update_policy = AsyncMock(return_value=None)
    policy.create_policy = AsyncMock()

    iam = MagicMock()
    iam.list_roles = AsyncMock(return_value=[])
    iam.create_role = AsyncMock()

    ctx = _make_ctx(catalogs=catalogs, policy=policy, iam=iam)

    await REGION_MAPPINGS_REGISTRY_PRESET.apply(
        REGION_MAPPINGS_REGISTRY_PRESET.params_model(), "platform", ctx,
    )

    policy.create_policy.assert_awaited_once()
    created_policy = policy.create_policy.await_args.args[0]
    assert created_policy.actions == ["GET"]
    assert created_policy.resources == [r"/region-mappings/.*"]
    assert created_policy.effect == "ALLOW"

    iam.create_role.assert_awaited_once()
    created_role = iam.create_role.await_args.args[0]
    assert created_role.name == "unauthenticated"
    assert "region_mappings_public_read" in created_role.policies


@pytest.mark.asyncio
async def test_apply_unions_policy_into_existing_role() -> None:
    from dynastore.extensions.region_mapping.presets.region_mappings_registry import (
        REGION_MAPPINGS_REGISTRY_PRESET,
    )

    catalogs = MagicMock()
    catalogs.get_catalog_model = AsyncMock(return_value=MagicMock())
    catalogs.get_collection = AsyncMock(return_value=MagicMock())

    policy = MagicMock()
    policy.update_policy = AsyncMock(return_value=MagicMock())  # already exists

    existing_role = _empty_role("unauthenticated")
    existing_role.policies = ["some_other_policy"]
    iam = MagicMock()
    iam.list_roles = AsyncMock(return_value=[existing_role])
    iam.update_role = AsyncMock()
    iam.create_role = AsyncMock()

    ctx = _make_ctx(catalogs=catalogs, policy=policy, iam=iam)

    await REGION_MAPPINGS_REGISTRY_PRESET.apply(
        REGION_MAPPINGS_REGISTRY_PRESET.params_model(), "platform", ctx,
    )

    iam.create_role.assert_not_awaited()
    iam.update_role.assert_awaited_once()
    merged = iam.update_role.await_args.args[0]
    assert "some_other_policy" in merged.policies
    assert "region_mappings_public_read" in merged.policies


# ---------------------------------------------------------------------------
# revoke()
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_revoke_strips_policy_from_role_and_deletes_policy_and_config() -> None:
    from dynastore.extensions.region_mapping.presets.region_mappings_registry import (
        REGION_MAPPINGS_REGISTRY_PRESET,
    )
    from dynastore.modules.storage.presets.preset import AppliedDescriptor
    from dynastore.modules.iam.audience_configs import CatalogLookupAudience

    existing_role = _empty_role("unauthenticated")
    existing_role.policies = ["region_mappings_public_read", "other_policy"]
    iam = MagicMock()
    iam.list_roles = AsyncMock(return_value=[existing_role])
    iam.update_role = AsyncMock()

    policy = MagicMock()
    policy.delete_policy = AsyncMock()

    config = MagicMock()
    config.delete_config = AsyncMock()

    ctx = _make_ctx(policy=policy, iam=iam, config=config)
    descriptor = AppliedDescriptor(payload={
        "policy_id": "region_mappings_public_read", "role_name": "unauthenticated",
    })

    await REGION_MAPPINGS_REGISTRY_PRESET.revoke(descriptor, ctx)

    iam.update_role.assert_awaited_once()
    remaining = iam.update_role.await_args.args[0]
    assert "region_mappings_public_read" not in remaining.policies
    assert "other_policy" in remaining.policies

    policy.delete_policy.assert_awaited_once_with("region_mappings_public_read")
    config.delete_config.assert_awaited_once()
    assert config.delete_config.await_args.args[0] is CatalogLookupAudience
    assert config.delete_config.await_args.kwargs["catalog_id"] == "_region_mappings_"


@pytest.mark.asyncio
async def test_revoke_does_not_delete_catalog_or_collection() -> None:
    """Registry data is preserved on revoke — only the public-read grant is undone."""
    from dynastore.extensions.region_mapping.presets.region_mappings_registry import (
        REGION_MAPPINGS_REGISTRY_PRESET,
    )
    from dynastore.modules.storage.presets.preset import AppliedDescriptor

    catalogs = MagicMock()
    catalogs.delete_catalog = AsyncMock()
    catalogs.delete_collection = AsyncMock()

    ctx = _make_ctx(catalogs=catalogs)
    descriptor = AppliedDescriptor(payload={})

    await REGION_MAPPINGS_REGISTRY_PRESET.revoke(descriptor, ctx)

    catalogs.delete_catalog.assert_not_awaited()
    catalogs.delete_collection.assert_not_awaited()


# ---------------------------------------------------------------------------
# dry_run() / registration
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_dry_run_returns_expected_entry_kinds() -> None:
    from dynastore.extensions.region_mapping.presets.region_mappings_registry import (
        REGION_MAPPINGS_REGISTRY_PRESET,
    )

    ctx = _make_ctx()
    plan = await REGION_MAPPINGS_REGISTRY_PRESET.dry_run(
        REGION_MAPPINGS_REGISTRY_PRESET.params_model(), "platform", ctx,
    )

    kinds = [e.kind for e in plan.entries]
    assert "create_catalog" in kinds
    assert "create_collection" in kinds
    assert "set_config" in kinds
    assert "upsert_policy" in kinds
    assert "upsert_role_binding" in kinds


def test_preset_registered_in_registry() -> None:
    import dynastore.extensions.region_mapping.presets  # noqa: F401 -- side-effect import
    from dynastore.modules.storage.presets.registry import get_preset

    preset = get_preset("region_mappings_registry")
    assert preset is not None
    assert preset.name == "region_mappings_registry"
    assert "region-mapping" in preset.keywords
