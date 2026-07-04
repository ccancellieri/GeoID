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

"""Unit tests for PATCH /configs — ConfigApiService.patch_config."""
# Registering TilesConfig / FeaturesPluginConfig in the PluginConfig registry
# requires importing their extension packages (which transitively import
# config.py). The test identifies configs by class_key, so the registry must
# be populated before patch_config resolves them.
import dynastore.extensions.features  # noqa: F401
import dynastore.modules.tiles  # noqa: F401

import pytest
from unittest.mock import AsyncMock, MagicMock
from typing import Any, Dict


def _make_config_service(stored: Dict[str, Any]):
    """Build a mock ConfigsProtocol that returns stored config values."""
    svc = MagicMock()

    async def _get_config(cls, catalog_id=None, collection_id=None, **_):
        return cls()  # always return defaults

    async def _get_persisted_config(cls, catalog_id=None, collection_id=None, **_):
        key = (cls.__name__, catalog_id, collection_id)
        value = stored.get(key)
        if value is None:
            return None
        return value.model_dump(exclude_unset=True) if hasattr(value, "model_dump") else value

    async def _set_config(cls, value, catalog_id=None, collection_id=None, **_):
        key = (cls.__name__, catalog_id, collection_id)
        stored[key] = value
        return value

    async def _delete_config(cls, catalog_id=None, collection_id=None, **_):
        key = (cls.__name__, catalog_id, collection_id)
        stored.pop(key, None)

    async def _list_configs(catalog_id=None, collection_id=None, limit=10, offset=0, **_):
        results = [
            {"plugin_id": k[0], "config": v.model_dump() if hasattr(v, "model_dump") else {}}
            for k, v in stored.items()
            if k[1] == catalog_id and k[2] == collection_id
        ]
        return {"results": results, "total": len(results)}

    svc.get_config = AsyncMock(side_effect=_get_config)
    svc.get_persisted_config = AsyncMock(side_effect=_get_persisted_config)
    svc.set_config = AsyncMock(side_effect=_set_config)
    svc.delete_config = AsyncMock(side_effect=_delete_config)
    svc.list_configs = AsyncMock(side_effect=_list_configs)
    return svc


@pytest.mark.asyncio
async def test_patch_platform_updates_multiple_configs():
    """PATCH with multiple keys updates each config independently."""
    from dynastore.extensions.configs.config_api_service import ConfigApiService

    stored: Dict = {}
    config_svc = _make_config_service(stored)
    api = ConfigApiService(config_service=config_svc)

    result = await api.patch_config(
        catalog_id=None,
        body={
            "tiles_config": {"enabled": False},
            "features_plugin_config": {"enabled": True},
        },
    )

    assert "updated" in result
    assert "tiles_config" in result["updated"]
    assert "features_plugin_config" in result["updated"]

    # `result["updated"]` only lists the touched plugin_ids, not the values
    # written — check the persisted store to confirm each config landed
    # with its own new value rather than one overwriting the other.
    assert stored[("TilesConfig", None, None)].enabled is False
    assert stored[("FeaturesPluginConfig", None, None)].enabled is True


@pytest.mark.asyncio
async def test_patch_null_deletes_config():
    """PATCH with null value calls delete_config for that key."""
    from dynastore.extensions.configs.config_api_service import ConfigApiService

    stored: Dict = {}
    config_svc = _make_config_service(stored)
    api = ConfigApiService(config_service=config_svc)

    result = await api.patch_config(
        catalog_id="my-catalog",
        body={"tiles_config": None},
    )

    assert "tiles_config" in result["updated"]
    config_svc.delete_config.assert_called_once()


@pytest.mark.asyncio
async def test_patch_unknown_plugin_raises_value_error():
    """PATCH with an unknown body key + no discriminator raises a clear
    "Unknown config class" ValueError before any write fires.

    A body with no ``class_key``/``driver_class`` discriminator is *not* a
    multi-instance ref-create attempt, so an unregistered key is a plain
    unknown single-instance config class (e.g. a typo).  The composer must
    surface it as "Unknown config class" (mapped to a 404 at the route
    layer, ``problem_details.value_error``) rather than implying the caller
    meant to create a ref.  The offending key appears in the message so
    operators can spot the typo.  Guards the #1102 (item 2) regression.
    """
    from dynastore.extensions.configs.config_api_service import ConfigApiService

    stored: Dict = {}
    config_svc = _make_config_service(stored)
    api = ConfigApiService(config_service=config_svc)

    with pytest.raises(ValueError, match=r"Unknown config class") as exc:
        await api.patch_config(
            catalog_id=None,
            body={"no_such_plugin_config": {"enabled": False}},
        )
    # Offending key is surfaced (the integration assertion in
    # test_exposure_control checks the same).
    assert "no_such_plugin_config" in str(exc.value)

    # No writes should have occurred
    config_svc.set_config.assert_not_called()
    config_svc.delete_config.assert_not_called()


@pytest.mark.asyncio
async def test_patch_ref_create_with_bogus_discriminator_still_errors():
    """A body that *does* carry a ``class_key``/``driver_class`` discriminator
    is a genuine multi-instance ref-create attempt — it must keep the
    distinct "is not a registered config class" error (not the unknown-class
    path), preserving the ref-create contract alongside the #1102 fix.
    """
    from dynastore.extensions.configs.config_api_service import ConfigApiService

    stored: Dict = {}
    config_svc = _make_config_service(stored)
    api = ConfigApiService(config_service=config_svc)

    with pytest.raises(
        ValueError, match=r"is not a registered config class"
    ):
        await api.patch_config(
            catalog_id=None,
            body={
                "my_ref": {
                    "class_key": "NonExistentConfig",
                    "enabled": False,
                },
            },
        )

    config_svc.set_config.assert_not_called()


@pytest.mark.asyncio
async def test_patch_validation_failure_prevents_writes():
    """PATCH with invalid value raises ValidationError before any write occurs."""
    import pydantic
    from dynastore.extensions.configs.config_api_service import ConfigApiService

    stored: Dict = {}
    config_svc = _make_config_service(stored)
    api = ConfigApiService(config_service=config_svc)

    # "not-a-bool" is not a valid bool for `enabled`
    with pytest.raises((pydantic.ValidationError, ValueError)):
        await api.patch_config(
            catalog_id=None,
            body={"tiles_config": {"enabled": "not-a-bool"}},
        )

    # Because we validate before writing, no writes should have occurred
    config_svc.set_config.assert_not_called()


@pytest.mark.asyncio
async def test_patch_catalog_scope():
    """PATCH at catalog scope passes catalog_id to set_config."""
    from dynastore.extensions.configs.config_api_service import ConfigApiService

    stored: Dict = {}
    config_svc = _make_config_service(stored)
    api = ConfigApiService(config_service=config_svc)

    await api.patch_config(
        catalog_id="test-catalog",
        body={"tiles_config": {"enabled": False}},
    )

    # Verify catalog_id was passed to set_config
    call_kwargs = config_svc.set_config.call_args
    assert call_kwargs.kwargs.get("catalog_id") == "test-catalog"


@pytest.mark.asyncio
async def test_patch_strips_response_envelopes_on_ingress():
    """#946: PATCH bodies that include ``_meta`` / ``_links`` (e.g. copied
    from a GET with ``meta=field`` / ``links=minimal``) must round-trip.

    Without the defensive strip the merged dict would be handed to
    ``cls.model_validate`` which rejects extras (#918 ``extra="forbid"``).
    The user-facing flow is "fetch resolved config → tweak one field →
    PATCH back" so any envelope keys in the response must not be a trap.
    """
    from dynastore.extensions.configs.config_api_service import ConfigApiService

    stored: Dict = {}
    config_svc = _make_config_service(stored)
    api = ConfigApiService(config_service=config_svc)

    await api.patch_config(
        catalog_id=None,
        body={
            "tiles_config": {
                "enabled": False,
                "_meta": {"tier": "platform", "source": "default"},
                "_links": [{"rel": "self", "href": "/configs/plugins/tiles_config"}],
            },
        },
    )

    call_args = config_svc.set_config.call_args
    written = call_args.args[1]
    dumped = written.model_dump() if hasattr(written, "model_dump") else dict(written)
    assert "_meta" not in dumped
    assert "_links" not in dumped
    assert dumped.get("enabled") is False
