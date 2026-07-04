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

"""DB-free unit tests for ``registry.load_preset_entry_points`` (#2601).

Verifies the ``dynastore.presets`` entry-point loader's fail-soft behaviour
without touching real package metadata: ``importlib.metadata.entry_points``
is monkeypatched to return fake ``EntryPoint``-shaped objects.
"""
from __future__ import annotations

from types import SimpleNamespace
from typing import Any, Callable, List
from unittest.mock import MagicMock

import pytest

from dynastore.modules.storage.presets import registry


def _fake_entry_point(name: str, load: Callable[[], Any]) -> Any:
    return SimpleNamespace(name=name, load=load)


def _patch_entry_points(monkeypatch: pytest.MonkeyPatch, entries: List[Any]) -> None:
    monkeypatch.setattr(
        "importlib.metadata.entry_points",
        lambda group=None: entries if group == "dynastore.presets" else [],
    )


def test_load_preset_entry_points_loads_each_entry(monkeypatch: pytest.MonkeyPatch) -> None:
    loaded: List[str] = []
    entries = [
        _fake_entry_point("a", lambda: loaded.append("a")),
        _fake_entry_point("b", lambda: loaded.append("b")),
    ]
    _patch_entry_points(monkeypatch, entries)

    registry.load_preset_entry_points()

    assert loaded == ["a", "b"]


def test_load_preset_entry_points_skips_missing_optional_deps(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An entry-point whose module raises ImportError must be skipped, not raised."""
    loaded: List[str] = []

    def _raise_import_error() -> None:
        raise ImportError("optional dep not installed")

    entries = [
        _fake_entry_point("broken", _raise_import_error),
        _fake_entry_point("ok", lambda: loaded.append("ok")),
    ]
    _patch_entry_points(monkeypatch, entries)

    registry.load_preset_entry_points()  # must not raise

    assert loaded == ["ok"]


def test_load_preset_entry_points_logs_unexpected_errors_without_raising(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A non-ImportError failure is logged, not propagated — one bad preset
    module must never block the others or the process boot importing it."""
    loaded: List[str] = []

    def _raise_value_error() -> None:
        raise ValueError("boom")

    entries = [
        _fake_entry_point("broken", _raise_value_error),
        _fake_entry_point("ok", lambda: loaded.append("ok")),
    ]
    _patch_entry_points(monkeypatch, entries)

    registry.load_preset_entry_points()  # must not raise

    assert loaded == ["ok"]


def test_load_preset_entry_points_queries_the_dynastore_presets_group(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    mock_entry_points = MagicMock(return_value=[])
    monkeypatch.setattr("importlib.metadata.entry_points", mock_entry_points)

    registry.load_preset_entry_points()

    mock_entry_points.assert_called_once_with(group="dynastore.presets")
