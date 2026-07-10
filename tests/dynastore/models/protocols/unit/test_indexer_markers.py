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

"""Index-tier marker discrimination.

Pins the contract that :class:`IndexTierDriver` — the single Protocol that
replaced the six per-tier boolean markers (``CatalogIndexer``,
``CollectionIndexer``, ``AssetIndexer``, ``ItemIndexer``,
``ItemAssetIndexer``, ``PlatformAssetIndexer``) — discriminates BY VALUE:
structural ``isinstance`` only tests that ``index_tiers`` is present, so
callers must additionally check tier membership in the frozenset.

A driver indexing multiple tiers lists them all in one ``index_tiers``
frozenset; a driver indexing none does not satisfy any tier check. The
split-by-tier is independent of the data/metadata distinction — both are
indexable.
"""

from __future__ import annotations

from typing import ClassVar, FrozenSet

from dynastore.models.protocols.indexer import IndexTierDriver


# ---------------------------------------------------------------------------
# Marker discrimination — minimal stubs
# ---------------------------------------------------------------------------


def test_marker_requires_index_tiers_attribute():
    """A class without ``index_tiers`` at all does NOT satisfy
    :class:`IndexTierDriver` structurally — presence is the isinstance gate."""

    class _NoAttr:
        pass

    assert not isinstance(_NoAttr(), IndexTierDriver)


def test_marker_presence_alone_does_not_imply_any_tier():
    """The GOTCHA this design fixes: a class DOES satisfy IndexTierDriver
    once ``index_tiers`` exists, even with an empty frozenset — presence is
    the structural gate, but callers MUST check tier membership BY VALUE to
    classify which tier(s) (if any) the driver actually claims."""

    class _EmptyTiers:
        index_tiers: ClassVar[FrozenSet[str]] = frozenset()

    obj = _EmptyTiers()
    assert isinstance(obj, IndexTierDriver)
    assert "catalog" not in obj.index_tiers
    assert "item" not in obj.index_tiers


def test_single_tier_opt_in():
    """Declaring one tier in ``index_tiers`` opts in to that tier only."""

    class _CatOnly:
        index_tiers: ClassVar[FrozenSet[str]] = frozenset({"catalog"})

    obj = _CatOnly()
    assert isinstance(obj, IndexTierDriver)
    assert "catalog" in obj.index_tiers
    assert "collection" not in obj.index_tiers
    assert "asset" not in obj.index_tiers
    assert "item" not in obj.index_tiers


def test_multi_tier_opt_in():
    """A driver indexing multiple tiers lists them all in one frozenset."""

    class _CatAndCol:
        index_tiers: ClassVar[FrozenSet[str]] = frozenset({"catalog", "collection"})

    obj = _CatAndCol()
    assert {"catalog", "collection"}.issubset(obj.index_tiers)
    assert "asset" not in obj.index_tiers
    assert "item" not in obj.index_tiers


def test_reserved_tiers_are_valid_values_with_no_shipping_implementer():
    """``item_asset`` / ``platform_asset`` are reserved tokens in the tier
    vocabulary — a class MAY declare them (extension axis), but no shipped
    driver does yet (pinned by the driver-shape tests below)."""

    class _ItemAssetOnly:
        index_tiers: ClassVar[FrozenSet[str]] = frozenset({"item_asset"})

    class _PlatformAssetOnly:
        index_tiers: ClassVar[FrozenSet[str]] = frozenset({"platform_asset"})

    assert "item_asset" in _ItemAssetOnly().index_tiers
    assert "asset" not in _ItemAssetOnly().index_tiers
    assert "platform_asset" in _PlatformAssetOnly().index_tiers
    assert "asset" not in _PlatformAssetOnly().index_tiers


# ---------------------------------------------------------------------------
# Existing ES drivers — self-declared tiers
# ---------------------------------------------------------------------------


def test_catalog_es_driver_indexes_catalog_only():
    """``CatalogElasticsearchDriver`` indexes ONE tier — catalog metadata,
    keyed by ``catalog_id``.  It claims the ``catalog`` tier only.
    """
    from dynastore.modules.elasticsearch.catalog_es_driver import (
        CatalogElasticsearchDriver,
    )

    assert CatalogElasticsearchDriver.index_tiers == frozenset({"catalog"})


def test_collection_es_driver_indexes_collection_only():
    """``CollectionElasticsearchDriver`` indexes ONE tier — collection
    metadata, keyed by ``(catalog_id, collection_id)``.  It claims the
    ``collection`` tier only.  Catalog-tier indexing is a separate driver
    class (NEW; not part of the catch-all rename).
    """
    from dynastore.modules.elasticsearch.collection_es_driver import (
        CollectionElasticsearchDriver,
    )

    assert CollectionElasticsearchDriver.index_tiers == frozenset({"collection"})


def test_items_es_driver_indexes_items_only():
    from dynastore.modules.storage.drivers.elasticsearch import (
        ItemsElasticsearchDriver,
    )

    assert ItemsElasticsearchDriver.index_tiers == frozenset({"item"})


def test_asset_es_driver_indexes_assets_only():
    from dynastore.modules.storage.drivers.elasticsearch import (
        AssetElasticsearchDriver,
    )

    assert AssetElasticsearchDriver.index_tiers == frozenset({"asset"})


def test_items_private_and_envelope_drivers_index_items_only():
    """The private and envelope items drivers both claim the ``item`` tier
    (opting out of auto-registration via ``auto_register_for_routing``, not
    via the tier marker — the two axes are independent)."""
    from dynastore.modules.storage.drivers.elasticsearch_envelope.driver import (
        ItemsElasticsearchEnvelopeDriver,
    )
    from dynastore.modules.storage.drivers.elasticsearch_private.driver import (
        ItemsElasticsearchPrivateDriver,
    )

    assert ItemsElasticsearchPrivateDriver.index_tiers == frozenset({"item"})
    assert ItemsElasticsearchEnvelopeDriver.index_tiers == frozenset({"item"})


# ---------------------------------------------------------------------------
# Repository hygiene — the six retired markers must not reappear
# ---------------------------------------------------------------------------


def test_no_retired_marker_classvars_remain_in_source():
    """Guard against a future edit reintroducing one of the six retired
    ``is_*_indexer`` boolean ClassVars (the presence-not-value trap this
    design retired) anywhere under ``packages/*/src``."""
    import re
    from pathlib import Path

    import pytest

    here = Path(__file__).resolve()
    repo_root = None
    for parent in here.parents:
        if (parent / "packages").is_dir() and (parent / "tests").is_dir():
            repo_root = parent
            break
    if repo_root is None:
        pytest.skip("could not locate repo root from test file location")

    pattern = re.compile(
        r"is_catalog_indexer|is_collection_indexer|is_item_indexer|"
        r"is_asset_indexer|is_item_asset_indexer|is_platform_asset_indexer"
    )
    hits = []
    for path in (repo_root / "packages").rglob("*.py"):
        if "src" not in path.parts:
            continue
        text = path.read_text(encoding="utf-8")
        if pattern.search(text):
            hits.append(str(path.relative_to(repo_root)))

    assert not hits, f"retired marker ClassVars still referenced: {hits}"
