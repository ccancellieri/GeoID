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

"""Unit tests for the combined volumes_demo preset.

volumes_demo orchestrates tiles3d_samples + geovolumes_demo against the shared
demo-3d catalog. These tests assert the orchestration delegates to both
sub-presets in the right order and threads their descriptors into revoke.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, patch

import pytest


# ---------------------------------------------------------------------------
# Preset metadata
# ---------------------------------------------------------------------------


def test_preset_name_and_metadata():
    from dynastore.extensions.volumes.presets.volumes_demo import VOLUMES_DEMO_PRESET

    assert VOLUMES_DEMO_PRESET.name == "volumes_demo"
    assert "demo" in VOLUMES_DEMO_PRESET.keywords
    assert "cityjson" in VOLUMES_DEMO_PRESET.keywords
    assert "3dtiles" in VOLUMES_DEMO_PRESET.keywords


def test_params_model_defaults():
    from dynastore.extensions.volumes.presets.volumes_demo import VolumesDemoParams

    p = VolumesDemoParams()
    assert p.catalog_id == "demo-3d"
    assert p.include_3dbag is False
    assert p.max_features is None


def test_preset_is_registered():
    from dynastore.modules.storage.presets.registry import get_preset

    preset = get_preset("volumes_demo")
    assert preset is not None
    assert preset.name == "volumes_demo"


# ---------------------------------------------------------------------------
# apply() — delegates to both sub-presets, samples first
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_apply_delegates_to_both_subpresets_in_order():
    from dynastore.extensions.volumes.presets import volumes_demo as mod
    from dynastore.extensions.volumes.presets.volumes_demo import (
        VOLUMES_DEMO_PRESET,
        VolumesDemoParams,
    )
    from dynastore.modules.storage.presets.preset import AppliedDescriptor

    call_order: list[str] = []

    async def _samples_apply(params, scope, ctx):
        call_order.append("samples")
        return AppliedDescriptor(payload={"created_catalog": True})

    async def _cityjson_apply(params, scope, ctx):
        call_order.append("cityjson")
        return AppliedDescriptor(payload={"created_catalog": False})

    with (
        patch.object(mod.TILES3D_SAMPLES_PRESET, "apply", side_effect=_samples_apply),
        patch.object(mod.GEOVOLUMES_DEMO_PRESET, "apply", side_effect=_cityjson_apply),
    ):
        descriptor = await VOLUMES_DEMO_PRESET.apply(
            VolumesDemoParams(), "platform", object(),
        )

    assert call_order == ["samples", "cityjson"]
    assert descriptor.payload["catalog_id"] == "demo-3d"
    assert descriptor.payload["samples"] == {"created_catalog": True}
    assert descriptor.payload["cityjson"] == {"created_catalog": False}


@pytest.mark.asyncio
async def test_apply_forwards_params_to_subpresets():
    from dynastore.extensions.volumes.presets import volumes_demo as mod
    from dynastore.extensions.volumes.presets.volumes_demo import (
        VOLUMES_DEMO_PRESET,
        VolumesDemoParams,
    )
    from dynastore.modules.storage.presets.preset import AppliedDescriptor

    samples_mock = AsyncMock(return_value=AppliedDescriptor(payload={}))
    cityjson_mock = AsyncMock(return_value=AppliedDescriptor(payload={}))

    with (
        patch.object(mod.TILES3D_SAMPLES_PRESET, "apply", samples_mock),
        patch.object(mod.GEOVOLUMES_DEMO_PRESET, "apply", cityjson_mock),
    ):
        await VOLUMES_DEMO_PRESET.apply(
            VolumesDemoParams(catalog_id="my-3d", include_3dbag=True, max_features=10),
            "platform",
            object(),
        )

    samples_params = samples_mock.call_args.args[0]
    assert samples_params.catalog_id == "my-3d"
    assert samples_params.include_3dbag is True

    cityjson_params = cityjson_mock.call_args.args[0]
    assert cityjson_params.catalog_id == "my-3d"
    assert cityjson_params.max_features == 10


# ---------------------------------------------------------------------------
# revoke() — delegates to both, cityjson first (never owns shared catalog)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_revoke_delegates_cityjson_first():
    from dynastore.extensions.volumes.presets import volumes_demo as mod
    from dynastore.extensions.volumes.presets.volumes_demo import VOLUMES_DEMO_PRESET
    from dynastore.modules.storage.presets.preset import AppliedDescriptor

    call_order: list[str] = []

    async def _samples_revoke(desc, ctx):
        call_order.append("samples")

    async def _cityjson_revoke(desc, ctx):
        call_order.append("cityjson")

    descriptor = AppliedDescriptor(payload={
        "catalog_id": "demo-3d",
        "samples": {"created_catalog": True},
        "cityjson": {"created_catalog": False},
    })

    with (
        patch.object(mod.TILES3D_SAMPLES_PRESET, "revoke", side_effect=_samples_revoke),
        patch.object(mod.GEOVOLUMES_DEMO_PRESET, "revoke", side_effect=_cityjson_revoke),
    ):
        await VOLUMES_DEMO_PRESET.revoke(descriptor, object())

    assert call_order == ["cityjson", "samples"]


@pytest.mark.asyncio
async def test_revoke_threads_subdescriptors():
    from dynastore.extensions.volumes.presets import volumes_demo as mod
    from dynastore.extensions.volumes.presets.volumes_demo import VOLUMES_DEMO_PRESET
    from dynastore.modules.storage.presets.preset import AppliedDescriptor

    samples_mock = AsyncMock()
    cityjson_mock = AsyncMock()

    descriptor = AppliedDescriptor(payload={
        "catalog_id": "demo-3d",
        "samples": {"created_collections": ["tiles3d-dragon-lod"]},
        "cityjson": {"collection_id": "denhaag", "created_collection": True},
    })

    with (
        patch.object(mod.TILES3D_SAMPLES_PRESET, "revoke", samples_mock),
        patch.object(mod.GEOVOLUMES_DEMO_PRESET, "revoke", cityjson_mock),
    ):
        await VOLUMES_DEMO_PRESET.revoke(descriptor, object())

    samples_desc = samples_mock.call_args.args[0]
    assert samples_desc.payload == {"created_collections": ["tiles3d-dragon-lod"]}
    cityjson_desc = cityjson_mock.call_args.args[0]
    assert cityjson_desc.payload == {"collection_id": "denhaag", "created_collection": True}


@pytest.mark.asyncio
async def test_revoke_tolerates_missing_subpayloads():
    from dynastore.extensions.volumes.presets import volumes_demo as mod
    from dynastore.extensions.volumes.presets.volumes_demo import VOLUMES_DEMO_PRESET
    from dynastore.modules.storage.presets.preset import AppliedDescriptor

    samples_mock = AsyncMock()
    cityjson_mock = AsyncMock()

    with (
        patch.object(mod.TILES3D_SAMPLES_PRESET, "revoke", samples_mock),
        patch.object(mod.GEOVOLUMES_DEMO_PRESET, "revoke", cityjson_mock),
    ):
        await VOLUMES_DEMO_PRESET.revoke(AppliedDescriptor(payload={}), object())

    samples_mock.assert_not_awaited()
    cityjson_mock.assert_not_awaited()
