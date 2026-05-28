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

"""DB-free unit tests for the ``common_dimensions`` preset (dynastore#307).

All external I/O is mocked via ``monkeypatch``; no DB, no ogc_dimensions
providers, no network access required.  DO NOT run under the repo-root
conftest (it wipes the local ``gis_dev`` DB during collection).
"""
from __future__ import annotations

from typing import Any, Dict
from unittest.mock import MagicMock

import pytest

from dynastore.models.dimensions import DIMENSIONS_CATALOG_ID
from dynastore.modules.storage.presets.preset import DataSeed


# ---------------------------------------------------------------------------
# Fake dimension data mirroring the real DimensionConfig + provider shape
# ---------------------------------------------------------------------------

def _make_fake_generator(
    provider_type: str = "DailyPeriodProvider",
    invertible: bool = False,
    hierarchical: bool = False,
) -> Any:
    """Minimal fake generator matching the attrs queried by _build_provider /
    _build_cube_dimensions / _infer_dim_type in dimensions_extension.py."""
    gen = MagicMock()
    gen.provider_type = provider_type
    gen.invertible = invertible
    gen.hierarchical = hierarchical
    # config_as_dict and search_protocols mirror the base-class property API.
    gen.config_as_dict = MagicMock(return_value={"period_days": 10})
    gen.search_protocols = []
    # __name__ used by _infer_dim_type via type(generator).__name__
    type(gen).__name__ = "DailyPeriodProvider"
    return gen


def _make_fake_dim_config(
    generator: Any,
    description: str = "A fake temporal dimension",
    extent_min: str = "1950-01-01",
    extent_max: str = "2050-12-31",
) -> Any:
    cfg = MagicMock()
    cfg.provider = generator
    cfg.description = description
    cfg.extent_min = extent_min
    cfg.extent_max = extent_max
    return cfg


# Two fake entries: one temporal, one nominal (tree-like)
_FAKE_TEMPORAL_GEN = _make_fake_generator(
    provider_type="DailyPeriodProvider", invertible=False, hierarchical=False,
)

_FAKE_NOMINAL_GEN = _make_fake_generator(
    provider_type="StaticTreeProvider", invertible=False, hierarchical=True,
)
type(_FAKE_NOMINAL_GEN).__name__ = "StaticTreeProvider"

FAKE_DIMENSIONS: Dict[str, Any] = {
    "fake-temporal-dekadal": _make_fake_dim_config(
        generator=_FAKE_TEMPORAL_GEN,
        description="Fake dekadal temporal dimension for testing",
    ),
    "fake-indicator-tree": _make_fake_dim_config(
        generator=_FAKE_NOMINAL_GEN,
        description="Fake indicator tree for testing",
        extent_min="",
        extent_max="",
    ),
}


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(autouse=True)
def _patch_get_registered_dimensions(monkeypatch: pytest.MonkeyPatch) -> None:
    """Replace the real get_registered_dimensions with the fake registry so
    _CommonDimensionsContributor.get_data() does not import ogc_dimensions
    providers or touch any external state.

    get_data() imports get_registered_dimensions from the
    dimensions_extension module at call time (lazy import inside the
    method body), so patching the module attribute is sufficient.
    """
    monkeypatch.setattr(
        "dynastore.extensions.dimensions.dimensions_extension.get_registered_dimensions",
        lambda: FAKE_DIMENSIONS,
    )


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

def test_get_data_yields_one_seed_per_dimension() -> None:
    """get_data() must yield exactly one DataSeed per fake dimension entry."""
    # Import after monkeypatch has been applied (autouse fixture runs first).
    from dynastore.extensions.dimensions.presets import _CommonDimensionsContributor

    seeds = list(_CommonDimensionsContributor().get_data())

    assert len(seeds) == len(FAKE_DIMENSIONS), (
        f"Expected {len(FAKE_DIMENSIONS)} seeds, got {len(seeds)}"
    )


def test_seeds_target_dimensions_catalog() -> None:
    """Every DataSeed must target the shared _dimensions_ catalog."""
    from dynastore.extensions.dimensions.presets import _CommonDimensionsContributor

    seeds = list(_CommonDimensionsContributor().get_data())

    for seed in seeds:
        assert seed.catalog_id == DIMENSIONS_CATALOG_ID, (
            f"catalog_id must be {DIMENSIONS_CATALOG_ID!r}, got {seed.catalog_id!r}"
        )


def test_seeds_have_manage_catalog_false() -> None:
    """manage_catalog must be False — the shared catalog must never be deleted
    on preset revoke."""
    from dynastore.extensions.dimensions.presets import _CommonDimensionsContributor

    seeds = list(_CommonDimensionsContributor().get_data())

    for seed in seeds:
        assert seed.manage_catalog is False, (
            f"manage_catalog must be False for shared catalog; seed {seed.collection_id!r}"
        )


def test_seeds_have_empty_items() -> None:
    """items must be empty — member materialization is delegated to the
    dimensions_materialize task (dynastore#307 design split)."""
    from dynastore.extensions.dimensions.presets import _CommonDimensionsContributor

    seeds = list(_CommonDimensionsContributor().get_data())

    for seed in seeds:
        assert seed.items == (), (
            f"items must be () for light registration; seed {seed.collection_id!r}"
        )


def test_seeds_collection_data_has_cube_dimensions() -> None:
    """collection_data must carry a non-empty ``cube:dimensions`` key in
    ``extra_metadata`` for STAC / datacube clients (dynastore#307)."""
    from dynastore.extensions.dimensions.presets import _CommonDimensionsContributor

    seeds = list(_CommonDimensionsContributor().get_data())

    for seed in seeds:
        em = seed.collection_data.get("extra_metadata", {})
        cube_dims = em.get("cube:dimensions")
        assert cube_dims, (
            f"cube:dimensions must be present and non-empty; seed {seed.collection_id!r}"
        )


def test_seeds_collection_data_has_records_layer_config() -> None:
    """collection_data must declare ``layer_config.collection_type == 'RECORDS'``
    to match the shape produced by materialize_dimension."""
    from dynastore.extensions.dimensions.presets import _CommonDimensionsContributor

    seeds = list(_CommonDimensionsContributor().get_data())

    for seed in seeds:
        lc = seed.collection_data.get("layer_config", {})
        assert lc.get("collection_type") == "RECORDS", (
            f"layer_config.collection_type must be 'RECORDS'; seed {seed.collection_id!r}"
        )


def test_seeds_collection_ids_match_dim_names() -> None:
    """collection_id of each seed must equal the dimension name key."""
    from dynastore.extensions.dimensions.presets import _CommonDimensionsContributor

    seeds = list(_CommonDimensionsContributor().get_data())
    seed_ids = {s.collection_id for s in seeds}

    assert seed_ids == set(FAKE_DIMENSIONS.keys()), (
        f"Seed collection IDs {seed_ids!r} do not match fake dim names "
        f"{set(FAKE_DIMENSIONS.keys())!r}"
    )


def test_seeds_are_dataseed_instances() -> None:
    """Confirm get_data() yields DataSeed instances (not plain dicts)."""
    from dynastore.extensions.dimensions.presets import _CommonDimensionsContributor

    seeds = list(_CommonDimensionsContributor().get_data())

    for seed in seeds:
        assert isinstance(seed, DataSeed), (
            f"Expected DataSeed, got {type(seed)!r}"
        )


def test_common_dimensions_preset_is_registered() -> None:
    """Importing the presets module must register ``common_dimensions`` in the
    global preset registry (side-effect import contract)."""
    # Import the presets module explicitly; the __init__.py of the dimensions
    # extension also triggers this, but direct import is safer in unit tests.
    import dynastore.extensions.dimensions.presets  # noqa: F401

    from dynastore.modules.storage.presets.registry import get_preset

    preset = get_preset("common_dimensions")
    assert preset is not None
    assert preset.name == "common_dimensions"
    assert "dimensions" in preset.keywords
    assert "datacube" in preset.keywords
