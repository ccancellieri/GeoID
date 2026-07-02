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

No IAM policy is registered by this preset (dynastore#443 hotfix) — every
test here constructs its ``PresetContext`` with ``iam=None`` / ``policy=None``
(the ``_make_ctx`` default), which doubles as coverage that ``apply()`` /
``revoke()`` never touch those fields and so succeed unchanged on an
IAM-less deployment.
"""
from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest


def _make_ctx(catalogs: Any = None, config: Any = None) -> Any:
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
        scope="platform",
        catalogs=catalogs,
    )


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

    ctx = _make_ctx(catalogs=catalogs, config=None)

    await REGION_MAPPINGS_REGISTRY_PRESET.apply(
        REGION_MAPPINGS_REGISTRY_PRESET.params_model(), "platform", ctx,
    )

    catalogs.create_catalog.assert_awaited_once()
    call = catalogs.create_catalog.await_args
    assert call.args[0]["id"] == "_region_mappings_"
    assert call.kwargs["hints"] == frozenset({Hint.DEFER})
    # Live 500 fix: title/description are multilanguage dicts, so lang="*"
    # must be passed — create_catalog otherwise rejects a dict input for its
    # lang="en" default.
    assert call.kwargs["lang"] == "*"

    catalogs.create_collection.assert_awaited_once()
    coll_call = catalogs.create_collection.await_args
    assert coll_call.args[0] == "_region_mappings_"
    assert coll_call.args[1]["id"] == "mappings"
    assert coll_call.args[1]["layer_config"]["collection_type"] == "RECORDS"
    assert "schema" in coll_call.args[1]
    assert coll_call.kwargs["lang"] == "*"


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


# ---------------------------------------------------------------------------
# apply() — PG-only routing (review hotfix finding C)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_apply_sets_pg_only_routing_configs() -> None:
    from dynastore.extensions.region_mapping.presets.region_mappings_registry import (
        REGION_MAPPINGS_REGISTRY_PRESET,
    )
    from dynastore.modules.storage.routing_config import (
        CatalogRoutingConfig,
        CollectionRoutingConfig,
        ItemsRoutingConfig,
        Operation,
    )

    catalogs = MagicMock()
    catalogs.get_catalog_model = AsyncMock(return_value=MagicMock())
    catalogs.get_collection = AsyncMock(return_value=MagicMock())
    config = MagicMock()
    config.set_config = AsyncMock()

    ctx = _make_ctx(catalogs=catalogs, config=config)

    await REGION_MAPPINGS_REGISTRY_PRESET.apply(
        REGION_MAPPINGS_REGISTRY_PRESET.params_model(), "platform", ctx,
    )

    calls_by_cls = {c.args[0]: c for c in config.set_config.await_args_list}
    assert CatalogRoutingConfig in calls_by_cls
    assert CollectionRoutingConfig in calls_by_cls
    assert ItemsRoutingConfig in calls_by_cls

    catalog_call = calls_by_cls[CatalogRoutingConfig]
    assert catalog_call.kwargs["catalog_id"] == "_region_mappings_"
    assert "collection_id" not in catalog_call.kwargs

    collection_call = calls_by_cls[CollectionRoutingConfig]
    assert collection_call.kwargs["catalog_id"] == "_region_mappings_"
    assert collection_call.kwargs["collection_id"] == "mappings"

    items_call = calls_by_cls[ItemsRoutingConfig]
    assert items_call.kwargs["catalog_id"] == "_region_mappings_"
    assert items_call.kwargs["collection_id"] == "mappings"

    # No ES/secondary index anywhere — every pinned driver_ref is the PG one.
    catalog_routing = catalog_call.args[1]
    for entries in catalog_routing.operations.values():
        for entry in entries:
            assert entry.driver_ref == "catalog_postgresql_driver"
    assert Operation.SEARCH in catalog_routing.operations

    collection_routing = collection_call.args[1]
    for entries in collection_routing.operations.values():
        for entry in entries:
            assert entry.driver_ref == "collection_postgresql_driver"
    assert Operation.SEARCH in collection_routing.operations

    items_routing = items_call.args[1]
    for entries in items_routing.operations.values():
        for entry in entries:
            assert entry.driver_ref == "items_postgresql_driver"


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

    # 4 set_config calls total: 3 PG-only routing configs + the audience flag.
    assert config.set_config.await_count == 4
    audience_call = next(
        c for c in config.set_config.await_args_list if c.args[0] is CatalogLookupAudience
    )
    assert audience_call.args[1].is_public is True
    assert audience_call.kwargs["catalog_id"] == "_region_mappings_"


# ---------------------------------------------------------------------------
# revoke()
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_revoke_deletes_audience_config_only() -> None:
    """No IAM policy is registered by this preset (dynastore#443 hotfix) —
    revoke only needs to undo the CatalogLookupAudience opt-in."""
    from dynastore.extensions.region_mapping.presets.region_mappings_registry import (
        REGION_MAPPINGS_REGISTRY_PRESET,
    )
    from dynastore.modules.storage.presets.preset import AppliedDescriptor
    from dynastore.modules.iam.audience_configs import CatalogLookupAudience

    config = MagicMock()
    config.delete_config = AsyncMock()

    ctx = _make_ctx(config=config)
    descriptor = AppliedDescriptor(payload={
        "catalog_id": "_region_mappings_", "collection_id": "mappings",
    })

    await REGION_MAPPINGS_REGISTRY_PRESET.revoke(descriptor, ctx)

    config.delete_config.assert_awaited_once()
    assert config.delete_config.await_args.args[0] is CatalogLookupAudience
    assert config.delete_config.await_args.kwargs["catalog_id"] == "_region_mappings_"


@pytest.mark.asyncio
async def test_revoke_does_not_delete_catalog_or_collection() -> None:
    """Registry data is preserved on revoke — only the audience config is undone."""
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
    assert kinds.count("create_catalog") == 1
    assert kinds.count("create_collection") == 1
    assert kinds.count("set_config") == 4
    assert "upsert_policy" not in kinds
    assert "upsert_role_binding" not in kinds

    set_config_targets = {e.target for e in plan.entries if e.kind == "set_config"}
    assert set_config_targets == {
        "CatalogRoutingConfig", "CollectionRoutingConfig", "ItemsRoutingConfig",
        "CatalogLookupAudience",
    }


def test_preset_registered_in_registry() -> None:
    import dynastore.extensions.region_mapping.presets  # noqa: F401 -- side-effect import
    from dynastore.modules.storage.presets.registry import get_preset

    preset = get_preset("region_mappings_registry")
    assert preset is not None
    assert preset.name == "region_mappings_registry"
    assert "region-mapping" in preset.keywords
