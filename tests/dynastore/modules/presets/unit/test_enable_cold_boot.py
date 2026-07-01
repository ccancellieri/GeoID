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

"""Unit tests for ``make_enable_cold_boot_contributor``.

Covers the tiles_enable self-heal design (#2659):

* A service with an IAM policy writer AND a prior application attempt
  self-heals the preset idempotently (force=True).
* A service with an IAM policy writer but no prior attempt recorded skips —
  it must never force-open a capability this deployment never asked for.
* A service with no IAM policy writer (e.g. a maps-only tier) skips cleanly —
  this is the regression check for the ``NoneType.update_policy`` crash.
"""
from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest


@pytest.mark.asyncio
async def test_self_heals_when_iam_present_and_previously_applied() -> None:
    """IAM writer present + prior attempt recorded → force-reapplies idempotently."""
    from dynastore.modules.presets.enable_cold_boot import make_enable_cold_boot_contributor

    contributor = make_enable_cold_boot_contributor(
        name="tiles", priority=35, preset_name="tiles_enable",
    )
    assert contributor.name == "tiles"
    assert contributor.priority == 35

    sentinel_engine = object()
    bootstrap_mock = AsyncMock(return_value=True)

    with patch(
        "dynastore.tools.discovery.get_protocol", return_value=MagicMock()
    ), patch(
        "dynastore.modules.presets.bootstrap.preset_previously_applied",
        AsyncMock(return_value=True),
    ), patch(
        "dynastore.modules.presets.bootstrap.bootstrap_preset_if_absent",
        bootstrap_mock,
    ):
        await contributor.run(sentinel_engine)

    bootstrap_mock.assert_awaited_once_with(
        sentinel_engine, preset_name="tiles_enable", scope_key="platform", force=True,
    )

    # Idempotent: a second cold boot re-asserts the grant again without error.
    with patch(
        "dynastore.tools.discovery.get_protocol", return_value=MagicMock()
    ), patch(
        "dynastore.modules.presets.bootstrap.preset_previously_applied",
        AsyncMock(return_value=True),
    ), patch(
        "dynastore.modules.presets.bootstrap.bootstrap_preset_if_absent",
        bootstrap_mock,
    ):
        await contributor.run(sentinel_engine)

    assert bootstrap_mock.await_count == 2


@pytest.mark.asyncio
async def test_skips_when_iam_present_but_never_previously_applied() -> None:
    """IAM writer present but no prior attempt → must not force-open the surface."""
    from dynastore.modules.presets.enable_cold_boot import make_enable_cold_boot_contributor

    contributor = make_enable_cold_boot_contributor(
        name="tiles", priority=35, preset_name="tiles_enable",
    )

    bootstrap_mock = AsyncMock()

    with patch(
        "dynastore.tools.discovery.get_protocol", return_value=MagicMock()
    ), patch(
        "dynastore.modules.presets.bootstrap.preset_previously_applied",
        AsyncMock(return_value=False),
    ), patch(
        "dynastore.modules.presets.bootstrap.bootstrap_preset_if_absent",
        bootstrap_mock,
    ):
        await contributor.run(object())

    bootstrap_mock.assert_not_called()


@pytest.mark.asyncio
async def test_skips_cleanly_when_no_iam_writer_in_process() -> None:
    """No PermissionProtocol registered (e.g. maps-only tier) → clean skip, no crash.

    This is the regression check for the pre-fix behaviour: applying
    tiles_enable against a service with no IAM policy writer raised
    ``AttributeError: 'NoneType' object has no attribute 'update_policy'``.
    The contributor must never even attempt the preset apply in that case.
    """
    from dynastore.modules.presets.enable_cold_boot import make_enable_cold_boot_contributor

    contributor = make_enable_cold_boot_contributor(
        name="tiles", priority=35, preset_name="tiles_enable",
    )

    previously_applied_mock = AsyncMock()
    bootstrap_mock = AsyncMock()

    with patch(
        "dynastore.tools.discovery.get_protocol", return_value=None
    ), patch(
        "dynastore.modules.presets.bootstrap.preset_previously_applied",
        previously_applied_mock,
    ), patch(
        "dynastore.modules.presets.bootstrap.bootstrap_preset_if_absent",
        bootstrap_mock,
    ):
        await contributor.run(object())  # must not raise

    previously_applied_mock.assert_not_called()
    bootstrap_mock.assert_not_called()


@pytest.mark.asyncio
async def test_self_heal_failure_is_swallowed() -> None:
    """A genuine self-heal error must never abort cold boot."""
    from dynastore.modules.presets.enable_cold_boot import make_enable_cold_boot_contributor

    contributor = make_enable_cold_boot_contributor(
        name="tiles", priority=35, preset_name="tiles_enable",
    )

    with patch(
        "dynastore.tools.discovery.get_protocol", return_value=MagicMock()
    ), patch(
        "dynastore.modules.presets.bootstrap.preset_previously_applied",
        AsyncMock(return_value=True),
    ), patch(
        "dynastore.modules.presets.bootstrap.bootstrap_preset_if_absent",
        AsyncMock(side_effect=RuntimeError("db unavailable")),
    ):
        await contributor.run(object())  # must not raise
