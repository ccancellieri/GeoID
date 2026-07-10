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

"""Regression test for geoid#3199: the durable IAM cold-boot leg (the
``auth_bootstrap`` ``IdpConfig`` DB seed and the foundational preset
self-heal) must keep running exactly where it ran before the fix — inside
``IamColdBootContributor.run()``, which is only ever invoked via
``run_cold_boot()`` (``modules/presets/cold_boot.py``), itself only called
from ``main.py``'s fleet-once ``_ColdBootReconciliationService``.

The fix moves per-process OIDC identity-provider *registration* into
``IamModule.lifespan`` (covered by ``test_idp_lifespan_registration.py``)
but must NOT move the durable seeding/self-heal work out of the fleet-once
path — provisioning demo catalogs/collections or re-applying baseline
presets on every worker process would reintroduce the boot-time cost
``run_cold_boot`` was made a background one-shot to avoid (geoid#3002).

This test drives ``IamColdBootContributor.run()`` directly (the same entry
point ``run_cold_boot`` uses) and asserts:
- the ``auth_bootstrap`` preset (the durable IdpConfig seed) is still
  applied from within this contributor, unchanged;
- the contributor still ends by calling the module's
  ``_register_identity_provider`` — a self-heal so the fleet-once leader
  picks up a row it just seeded without waiting for its own next
  per-process registration.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock, patch

import pytest

iam_cold_boot = pytest.importorskip(
    "dynastore.modules.iam.cold_boot_contributor",
    reason="dynastore-ext-iam distribution not installed — skipping cold-boot contributor tests",
    exc_type=ImportError,
)
IamColdBootContributor = iam_cold_boot.IamColdBootContributor


class _FakeModule:
    """Stands in for ``IamModule``; only tracks registration calls."""

    def __init__(self) -> None:
        self.register_calls = 0

    async def _register_identity_provider(self) -> None:
        self.register_calls += 1


@pytest.mark.asyncio
async def test_run_still_seeds_auth_bootstrap_and_self_heals_registration():
    preset_calls: list[str] = []

    async def _fake_bootstrap(engine: Any, *, preset_name: str, force: bool = False, **kwargs: Any) -> bool:
        preset_calls.append(preset_name)
        return True

    fake_module = _FakeModule()
    contributor = IamColdBootContributor(fake_module)

    with patch(
        "dynastore.modules.iam.module._seed_oidc_role_sync_config",
        AsyncMock(return_value=None),
    ), patch(
        "dynastore.modules.iam.module._warn_jwt_attr_no_issuer_allowlist",
        AsyncMock(return_value=None),
    ), patch(
        "dynastore.modules.storage.presets.lifecycle.bootstrap_preset_if_absent",
        side_effect=_fake_bootstrap,
    ):
        await contributor.run(engine=None)

    assert "auth_bootstrap" in preset_calls, (
        "IamColdBootContributor.run() must still seed the durable "
        "IdpConfig row via the auth_bootstrap preset — this leg stays "
        "fleet-once, only per-process registration moved."
    )
    assert fake_module.register_calls == 1, (
        "IamColdBootContributor.run() must still call "
        "_register_identity_provider as a self-heal after seeding a fresh "
        "IdpConfig row on the fleet-once leader."
    )
