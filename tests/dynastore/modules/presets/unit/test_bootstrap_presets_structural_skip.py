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

"""On a slim SCOPE (e.g. the maps service) ``demo_data`` writes an item whose
``ItemMetadataSidecar.prepare_upsert_payload`` requires the STAC extension and
raises ``ConfigResolutionError(missing_key="extension:stac")`` when it isn't
installed. That is expected (#3033) — but ``bootstrap_presets`` runs on
essentially every service boot/rebuild tick, so re-attempting the preset and
re-logging the same "cannot run here" fact at ERROR every time buries real
incidents under noise.

These tests assert that a ``ConfigResolutionError`` whose ``missing_key``
starts with ``"extension:"`` is cached per ``preset_name`` after a single
INFO-level log, and later ``bootstrap_presets`` calls skip re-invoking
``bootstrap_preset_if_absent`` entirely for that preset. A separate test
covers the opposite case: a ``ConfigResolutionError`` for an unrelated
``missing_key`` (a genuine missing platform default) must keep warning loudly
at ERROR and keep retrying every call, exactly like any other exception.
"""
from __future__ import annotations

import logging
from typing import Any
from unittest.mock import patch

import pytest

from dynastore.modules.db_config.exceptions import ConfigResolutionError
from dynastore.modules.presets import bootstrap as bootstrap_module
from dynastore.modules.presets.bootstrap import bootstrap_presets


class TestBootstrapPresetsStructuralSkip:
    def setup_method(self):
        bootstrap_module._structurally_unavailable_presets.clear()

    def teardown_method(self):
        bootstrap_module._structurally_unavailable_presets.clear()

    @pytest.mark.asyncio
    async def test_extension_missing_key_logged_once_and_not_retried(
        self, caplog
    ):
        call_log: list = []

        async def _fake_bootstrap(engine: Any, *, preset_name: str, scope_key: str,
                                   force: bool, params: Any, **_kw: Any) -> bool:
            call_log.append(preset_name)
            raise ConfigResolutionError(
                "ItemMetadataSidecar.prepare_upsert_payload requires the STAC "
                "extension",
                missing_key="extension:stac",
            )

        with patch(
            "dynastore.modules.presets.bootstrap.bootstrap_preset_if_absent",
            _fake_bootstrap,
        ), caplog.at_level(
            logging.INFO, logger="dynastore.modules.presets.bootstrap"
        ):
            for _ in range(3):
                result = await bootstrap_presets(
                    object(), [{"preset_name": "demo_data"}],
                )

        # bootstrap_preset_if_absent is only actually invoked once — the
        # cache short-circuits the remaining two bootstrap_presets calls.
        assert call_log == ["demo_data"]
        assert result == {"demo_data": False}
        assert "demo_data" in bootstrap_module._structurally_unavailable_presets

        skip_records = [
            r for r in caplog.records
            if r.levelno == logging.INFO and "skipped" in r.message
        ]
        assert len(skip_records) == 1

        error_records = [r for r in caplog.records if r.levelno == logging.ERROR]
        assert error_records == []

    @pytest.mark.asyncio
    async def test_unrelated_missing_key_keeps_warning_and_retrying(self, caplog):
        call_log: list = []

        async def _fake_bootstrap(engine: Any, *, preset_name: str, scope_key: str,
                                   force: bool, params: Any, **_kw: Any) -> bool:
            call_log.append(preset_name)
            raise ConfigResolutionError(
                "No usable default for config 'platform.default_role'",
                missing_key="platform.default_role",
            )

        with patch(
            "dynastore.modules.presets.bootstrap.bootstrap_preset_if_absent",
            _fake_bootstrap,
        ), caplog.at_level(
            logging.ERROR, logger="dynastore.modules.presets.bootstrap"
        ):
            for _ in range(3):
                result = await bootstrap_presets(
                    object(), [{"preset_name": "some_preset"}],
                )

        assert call_log == ["some_preset"] * 3
        assert result == {"some_preset": False}
        assert "some_preset" not in bootstrap_module._structurally_unavailable_presets

        error_records = [
            r for r in caplog.records
            if r.levelno == logging.ERROR and "raised unexpectedly" in r.message
        ]
        assert len(error_records) == 3

    @pytest.mark.asyncio
    async def test_other_exception_types_keep_loud_retry_behaviour(self, caplog):
        call_log: list = []

        async def _fake_bootstrap(engine: Any, *, preset_name: str, scope_key: str,
                                   force: bool, params: Any, **_kw: Any) -> bool:
            call_log.append(preset_name)
            raise RuntimeError("simulated unrelated failure")

        with patch(
            "dynastore.modules.presets.bootstrap.bootstrap_preset_if_absent",
            _fake_bootstrap,
        ), caplog.at_level(
            logging.ERROR, logger="dynastore.modules.presets.bootstrap"
        ):
            for _ in range(2):
                result = await bootstrap_presets(
                    object(), [{"preset_name": "flaky_preset"}],
                )

        assert call_log == ["flaky_preset"] * 2
        assert result == {"flaky_preset": False}
        assert "flaky_preset" not in bootstrap_module._structurally_unavailable_presets

        error_records = [
            r for r in caplog.records
            if r.levelno == logging.ERROR and "raised unexpectedly" in r.message
        ]
        assert len(error_records) == 2
