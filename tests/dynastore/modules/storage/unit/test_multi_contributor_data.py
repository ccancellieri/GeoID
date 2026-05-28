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

"""Unit tests for the data-contributor kind of ``MultiContributorPreset``.

Pure-Python — a fake ``CatalogsProtocol`` records calls; no DB, no FastAPI.
Covers the load-bearing invariants:

- apply ensures catalog + collection exist (created only when absent) and
  upserts items;
- revoke removes items always, but deletes the catalog/collection only when
  THIS apply created it AND ``manage_catalog`` / ``manage_collection`` allow it;
- a data contributor with no ``ctx.catalogs`` raises a clear error.
"""
from __future__ import annotations

import asyncio
from types import SimpleNamespace
from typing import Any, List

import pytest

from dynastore.modules.storage.presets.multi_contributor import MultiContributorPreset
from dynastore.modules.storage.presets.preset import DataSeed, NoParams


class _FakeCatalogs:
    """Minimal in-memory stand-in for the slice of CatalogsProtocol the
    data-contributor dispatch touches."""

    def __init__(self, existing_catalogs=(), existing_collections=()):
        self._catalogs = set(existing_catalogs)
        self._collections = set(existing_collections)  # (catalog_id, collection_id)
        self.calls: List[tuple] = []

    async def get_catalog_model(self, catalog_id, ctx=None):
        self.calls.append(("get_catalog_model", catalog_id))
        return SimpleNamespace(id=catalog_id) if catalog_id in self._catalogs else None

    async def create_catalog(self, catalog_data, lang="en", ctx=None):
        cid = catalog_data["id"]
        self.calls.append(("create_catalog", cid))
        self._catalogs.add(cid)
        return SimpleNamespace(id=cid)

    async def get_collection(self, catalog_id, collection_id, lang="en", ctx=None):
        self.calls.append(("get_collection", catalog_id, collection_id))
        key = (catalog_id, collection_id)
        return SimpleNamespace(id=collection_id) if key in self._collections else None

    async def create_collection(self, catalog_id, collection_data, lang="en", ctx=None, **kw):
        cid = collection_data["id"]
        self.calls.append(("create_collection", catalog_id, cid))
        self._collections.add((catalog_id, cid))
        return SimpleNamespace(id=cid)

    async def upsert(self, catalog_id, collection_id, items, ctx=None, processing_context=None):
        self.calls.append(("upsert", catalog_id, collection_id, len(list(items))))
        return list(items)

    async def delete_item(self, catalog_id, collection_id, item_id, ctx=None, caller_id=None):
        self.calls.append(("delete_item", catalog_id, collection_id, item_id))
        return True

    async def delete_collection(self, catalog_id, collection_id, force=False, ctx=None):
        self.calls.append(("delete_collection", catalog_id, collection_id))
        self._collections.discard((catalog_id, collection_id))
        return True

    async def delete_catalog(self, catalog_id, force=False, ctx=None):
        self.calls.append(("delete_catalog", catalog_id))
        self._catalogs.discard(catalog_id)
        return True


def _ctx(catalogs: Any) -> Any:
    """A PresetContext-shaped object — only ``catalogs`` is exercised here."""
    return SimpleNamespace(catalogs=catalogs, config=None, iam=None, policy=None)


def _preset(seeds) -> MultiContributorPreset:
    contributor = SimpleNamespace(get_data=lambda: list(seeds))
    return MultiContributorPreset(
        name="_test_data_preset",
        description="test",
        keywords=("data",),
        contributors_factory=lambda: [contributor],
    )


def _names(catalogs: _FakeCatalogs):
    return [c[0] for c in catalogs.calls]


def test_apply_creates_catalog_collection_and_upserts_when_absent():
    catalogs = _FakeCatalogs()
    seed = DataSeed(
        catalog_id="demo_catalog",
        collection_id="demo_collection",
        items=({"id": "a"}, {"id": "b"}),
    )
    preset = _preset([seed])
    descriptor = asyncio.run(preset.apply(NoParams(), "platform", _ctx(catalogs)))

    assert ("create_catalog", "demo_catalog") in catalogs.calls
    assert ("create_collection", "demo_catalog", "demo_collection") in catalogs.calls
    assert ("upsert", "demo_catalog", "demo_collection", 2) in catalogs.calls
    rec = descriptor.payload["data"][0]
    assert rec["created_catalog"] is True
    assert rec["created_collection"] is True
    assert rec["item_ids"] == ["a", "b"]


def test_apply_is_idempotent_when_catalog_and_collection_exist():
    catalogs = _FakeCatalogs(
        existing_catalogs=("demo_catalog",),
        existing_collections=(("demo_catalog", "demo_collection"),),
    )
    seed = DataSeed(catalog_id="demo_catalog", collection_id="demo_collection", items=({"id": "a"},))
    preset = _preset([seed])
    descriptor = asyncio.run(preset.apply(NoParams(), "platform", _ctx(catalogs)))

    assert "create_catalog" not in _names(catalogs)
    assert "create_collection" not in _names(catalogs)
    assert ("upsert", "demo_catalog", "demo_collection", 1) in catalogs.calls
    rec = descriptor.payload["data"][0]
    assert rec["created_catalog"] is False
    assert rec["created_collection"] is False


def test_revoke_deletes_only_what_apply_created():
    catalogs = _FakeCatalogs()
    seed = DataSeed(
        catalog_id="demo_catalog",
        collection_id="demo_collection",
        items=({"id": "a"},),
        manage_catalog=True,
        manage_collection=True,
    )
    preset = _preset([seed])
    descriptor = asyncio.run(preset.apply(NoParams(), "platform", _ctx(catalogs)))
    catalogs.calls.clear()
    asyncio.run(preset.revoke(descriptor, _ctx(catalogs)))

    assert ("delete_item", "demo_catalog", "demo_collection", "a") in catalogs.calls
    assert ("delete_collection", "demo_catalog", "demo_collection") in catalogs.calls
    assert ("delete_catalog", "demo_catalog") in catalogs.calls


def test_revoke_never_deletes_shared_catalog_when_manage_catalog_false():
    # Mirrors the common_dimensions case: the _dimensions_ catalog is shared.
    catalogs = _FakeCatalogs()
    seed = DataSeed(
        catalog_id="_dimensions_",
        collection_id="temporal-dekadal",
        items=(),
        manage_catalog=False,
        manage_collection=True,
    )
    preset = _preset([seed])
    descriptor = asyncio.run(preset.apply(NoParams(), "platform", _ctx(catalogs)))
    catalogs.calls.clear()
    asyncio.run(preset.revoke(descriptor, _ctx(catalogs)))

    assert ("delete_collection", "_dimensions_", "temporal-dekadal") in catalogs.calls
    assert "delete_catalog" not in _names(catalogs)


def test_revoke_leaves_preexisting_catalog_and_collection():
    catalogs = _FakeCatalogs(
        existing_catalogs=("demo_catalog",),
        existing_collections=(("demo_catalog", "demo_collection"),),
    )
    seed = DataSeed(catalog_id="demo_catalog", collection_id="demo_collection", items=({"id": "a"},))
    preset = _preset([seed])
    descriptor = asyncio.run(preset.apply(NoParams(), "platform", _ctx(catalogs)))
    catalogs.calls.clear()
    asyncio.run(preset.revoke(descriptor, _ctx(catalogs)))

    # The item we upserted is still removed, but neither pre-existing row is.
    assert ("delete_item", "demo_catalog", "demo_collection", "a") in catalogs.calls
    assert "delete_collection" not in _names(catalogs)
    assert "delete_catalog" not in _names(catalogs)


def test_apply_with_data_contributor_but_no_catalogs_raises():
    seed = DataSeed(catalog_id="c", collection_id="x", items=())
    preset = _preset([seed])
    with pytest.raises(RuntimeError, match="catalogs"):
        asyncio.run(preset.apply(NoParams(), "platform", _ctx(None)))


def test_dry_run_emits_seed_data_entries():
    seed = DataSeed(catalog_id="demo_catalog", collection_id="demo_collection", items=({"id": "a"},))
    preset = _preset([seed])
    plan = asyncio.run(preset.dry_run(NoParams(), "platform", _ctx(_FakeCatalogs())))
    seed_entries = [e for e in plan.entries if e.kind == "seed_data"]
    assert len(seed_entries) == 1
    assert seed_entries[0].target == "demo_catalog/demo_collection"
    assert seed_entries[0].detail["items"] == 1
