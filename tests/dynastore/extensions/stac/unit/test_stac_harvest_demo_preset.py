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

"""Unit tests for the stac_harvest_demo preset parameterization.

Pure-Python tests — no DB, no FastAPI, no network. Suite covers:

- StacHarvestDemoParams: defaults reproduce the Earth Search v1 / 1 / 25 values.
- StacHarvestDemoParams: overrides change url / target_catalog / limits independently.
- _StacHarvestDemoContributor.get_data() yields a bucket-free DataSeed
  (defer_provisioning=True).
- _StacHarvestDemoContributor.get_tasks() yields a TaskSeed whose stac_harvest
  inputs match the resolved params.
- param-less contributor still uses Earth Search v1 / demo_harvest / 1 / 25
  (backward compat — this preset is used in CI).
- STAC_HARVEST_DEMO_PRESET metadata: name, tier, params_model.
- Registry: preset registered as "stac_harvest_demo" at PLATFORM tier.
"""
from __future__ import annotations

from dynastore.extensions.stac.presets.stac_harvest_demo import (
    STAC_HARVEST_DEMO_PRESET,
    StacHarvestDemoParams,
    _StacHarvestDemoContributor,
    _CATALOG_ID,
    _COLLECTION_ID,
    _EARTH_SEARCH_URL,
)
from dynastore.modules.storage.presets.preset import DataSeed, TaskSeed
from dynastore.modules.storage.presets.protocol import PresetTier
from dynastore.modules.storage.presets.registry import find_preset


# ---------------------------------------------------------------------------
# StacHarvestDemoParams — defaults
# ---------------------------------------------------------------------------

def test_params_url_and_target_catalog_default_none() -> None:
    p = StacHarvestDemoParams()
    assert p.url is None
    assert p.target_catalog is None


def test_params_default_limits_match_current_behavior() -> None:
    p = StacHarvestDemoParams()
    assert p.max_collections == 1
    assert p.max_items == 25
    assert p.with_assets is True
    assert p.drivers == "es"


def test_params_extra_fields_ignored() -> None:
    """Extra keys in the JSON body are silently ignored (model_config extra=ignore)."""
    p = StacHarvestDemoParams.model_validate({"url": "https://x/y", "unknown_key": 99})
    assert p.url == "https://x/y"
    assert not hasattr(p, "unknown_key")


def test_params_partial_override() -> None:
    """Only the provided fields are overridden; others stay at their defaults."""
    p = StacHarvestDemoParams(target_catalog="my_harvest")
    assert p.target_catalog == "my_harvest"
    assert p.url is None
    assert p.max_collections == 1


# ---------------------------------------------------------------------------
# _StacHarvestDemoContributor — param-less (Earth Search v1 defaults)
# ---------------------------------------------------------------------------

def _default_contributor() -> _StacHarvestDemoContributor:
    """Contributor built with default params — mirrors what the preset does."""
    p = StacHarvestDemoParams()
    return _StacHarvestDemoContributor(
        catalog_id=p.target_catalog or _CATALOG_ID,
        url=p.url or _EARTH_SEARCH_URL,
        max_collections=p.max_collections,
        max_items=p.max_items,
        with_assets=p.with_assets,
        drivers=p.drivers,
    )


def test_default_contributor_catalog_id() -> None:
    c = _default_contributor()
    assert c.catalog_id == "demo_harvest"


def test_default_contributor_url_is_earth_search() -> None:
    c = _default_contributor()
    assert c.url == "https://earth-search.aws.element84.com/v1"


def test_default_contributor_limits() -> None:
    c = _default_contributor()
    assert c.max_collections == 1
    assert c.max_items == 25


# ---------------------------------------------------------------------------
# _StacHarvestDemoContributor.get_data()
# ---------------------------------------------------------------------------

def test_get_data_yields_one_seed() -> None:
    seeds = list(_default_contributor().get_data())
    assert len(seeds) == 1


def test_get_data_seed_catalog_and_collection() -> None:
    seed: DataSeed = list(_default_contributor().get_data())[0]
    assert seed.catalog_id == _CATALOG_ID
    assert seed.collection_id == _COLLECTION_ID


def test_get_data_seed_items_empty() -> None:
    """No inline items — the stac_harvest task writes them asynchronously."""
    seed = list(_default_contributor().get_data())[0]
    assert seed.items == ()


def test_get_data_seed_manage_catalog_and_collection_true() -> None:
    seed = list(_default_contributor().get_data())[0]
    assert seed.manage_catalog is True
    assert seed.manage_collection is True


def test_get_data_seed_catalog_data_id_matches_catalog_id() -> None:
    seed = list(_default_contributor().get_data())[0]
    assert seed.catalog_data["id"] == seed.catalog_id


def test_get_data_seed_collection_data_id_matches_collection_id() -> None:
    seed = list(_default_contributor().get_data())[0]
    assert seed.collection_data["id"] == seed.collection_id


def test_data_seed_is_bucket_free() -> None:
    """A STAC harvest routes items to Elasticsearch and stores assets as hrefs
    (never bytes), so the catalog is created bucket-free
    (defer_provisioning=True) — no GCS bucket is provisioned for this preset."""
    seed = list(_default_contributor().get_data())[0]
    assert seed.defer_provisioning is True


def test_custom_target_catalog_in_data_seed() -> None:
    c = _StacHarvestDemoContributor(
        catalog_id="pc_harvest", url=_EARTH_SEARCH_URL,
        max_collections=1, max_items=25, with_assets=True, drivers="es",
    )
    seed = list(c.get_data())[0]
    assert seed.catalog_id == "pc_harvest"
    assert seed.catalog_data["id"] == "pc_harvest"
    # Placeholder collection id stays fixed regardless of target catalog.
    assert seed.collection_id == "demo_harvest_index"


# ---------------------------------------------------------------------------
# _StacHarvestDemoContributor.get_tasks()
# ---------------------------------------------------------------------------

def test_get_tasks_yields_one_task() -> None:
    tasks = list(_default_contributor().get_tasks())
    assert len(tasks) == 1


def test_get_tasks_process_id_is_stac_harvest() -> None:
    tseed: TaskSeed = list(_default_contributor().get_tasks())[0]
    assert tseed.process_id == "stac_harvest"


def test_get_tasks_async_mode() -> None:
    tseed = list(_default_contributor().get_tasks())[0]
    assert tseed.async_mode is True


def test_get_tasks_inputs_match_default_behavior() -> None:
    """Param-less contributor must reproduce Earth Search v1 / 1 / 25 exactly."""
    tseed = list(_default_contributor().get_tasks())[0]
    assert tseed.inputs["catalog_url"] == "https://earth-search.aws.element84.com/v1"
    assert tseed.inputs["target_catalog"] == "demo_harvest"
    assert tseed.inputs["max_collections"] == 1
    assert tseed.inputs["max_items"] == 25
    assert tseed.inputs["with_assets"] is True
    assert tseed.inputs["drivers"] == "es"


def test_get_tasks_dedup_key_contains_catalog_and_url() -> None:
    c = _default_contributor()
    tseed = list(c.get_tasks())[0]
    assert tseed.dedup_key is not None
    assert c.catalog_id in tseed.dedup_key
    assert c.url in tseed.dedup_key


# ---------------------------------------------------------------------------
# Parameterized overrides
# ---------------------------------------------------------------------------

_PLANETARY_URL = "https://planetarycomputer.microsoft.com/api/stac/v1"


def _custom_contributor(
    url=_PLANETARY_URL,
    catalog_id="pc_harvest",
    max_collections=3,
    max_items=100,
    with_assets=True,
    drivers="es",
) -> _StacHarvestDemoContributor:
    return _StacHarvestDemoContributor(
        catalog_id=catalog_id, url=url,
        max_collections=max_collections, max_items=max_items,
        with_assets=with_assets, drivers=drivers,
    )


def test_custom_url_overrides_catalog_url() -> None:
    tseed = list(_custom_contributor().get_tasks())[0]
    assert tseed.inputs["catalog_url"] == _PLANETARY_URL


def test_custom_target_catalog_propagates_to_inputs() -> None:
    tseed = list(_custom_contributor(catalog_id="pc_harvest").get_tasks())[0]
    assert tseed.inputs["target_catalog"] == "pc_harvest"


def test_custom_limits_propagate_to_inputs() -> None:
    tseed = list(_custom_contributor(max_collections=3, max_items=100).get_tasks())[0]
    assert tseed.inputs["max_collections"] == 3
    assert tseed.inputs["max_items"] == 100


def test_different_targets_have_different_dedup_keys() -> None:
    c1 = _custom_contributor(catalog_id="cat1")
    c2 = _custom_contributor(catalog_id="cat2")
    k1 = list(c1.get_tasks())[0].dedup_key
    k2 = list(c2.get_tasks())[0].dedup_key
    assert k1 != k2


# ---------------------------------------------------------------------------
# STAC_HARVEST_DEMO_PRESET — metadata + _resolve
# ---------------------------------------------------------------------------

def test_preset_name() -> None:
    assert STAC_HARVEST_DEMO_PRESET.name == "stac_harvest_demo"


def test_preset_tier_is_platform() -> None:
    assert STAC_HARVEST_DEMO_PRESET.tier == PresetTier.PLATFORM


def test_preset_params_model_is_stac_harvest_demo_params() -> None:
    assert STAC_HARVEST_DEMO_PRESET.params_model is StacHarvestDemoParams


def test_preset_params_model_instantiable_without_args() -> None:
    """Param-less POST must still work: params_model() must succeed."""
    p = STAC_HARVEST_DEMO_PRESET.params_model()
    assert isinstance(p, StacHarvestDemoParams)
    assert p.url is None


def test_preset_description_non_empty() -> None:
    assert STAC_HARVEST_DEMO_PRESET.description


def test_preset_keywords_contain_expected() -> None:
    kws = set(STAC_HARVEST_DEMO_PRESET.keywords)
    assert {"demo", "stac", "harvest", "platform"} <= kws


def test_preset_resolve_defaults_with_no_params() -> None:
    """_resolve(params-with-defaults) must reproduce the Earth Search v1 contributor."""
    p = StacHarvestDemoParams()
    c = STAC_HARVEST_DEMO_PRESET._resolve(p)
    assert c.catalog_id == _CATALOG_ID
    assert c.url == _EARTH_SEARCH_URL
    assert c.max_collections == 1
    assert c.max_items == 25
    assert c.with_assets is True
    assert c.drivers == "es"


def test_preset_resolve_with_overrides() -> None:
    p = StacHarvestDemoParams(
        url=_PLANETARY_URL,
        target_catalog="pc_harvest",
        max_collections=3,
        max_items=100,
    )
    c = STAC_HARVEST_DEMO_PRESET._resolve(p)
    assert c.url == _PLANETARY_URL
    assert c.catalog_id == "pc_harvest"
    assert c.max_collections == 3
    assert c.max_items == 100


# ---------------------------------------------------------------------------
# Registry
# ---------------------------------------------------------------------------

def test_preset_registered_in_registry() -> None:
    import dynastore.extensions.stac.presets  # noqa: F401 — side-effect registers all

    preset = find_preset("stac_harvest_demo")
    assert preset.name == "stac_harvest_demo"


def test_preset_registry_tier_is_platform() -> None:
    import dynastore.extensions.stac.presets  # noqa: F401

    preset = find_preset("stac_harvest_demo")
    assert preset.tier == PresetTier.PLATFORM


def test_preset_registry_params_model_is_stac_harvest_demo_params() -> None:
    import dynastore.extensions.stac.presets  # noqa: F401

    preset = find_preset("stac_harvest_demo")
    assert preset.params_model is StacHarvestDemoParams
