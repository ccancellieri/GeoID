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

"""Unit tests for FastAPIBackgroundRunner.can_handle.

The runner used to inherit the default ``return True`` from RunnerProtocol,
which caused it to appear in typologies for processes it could not actually
run in-process (because the task instance was absent on that service).
Now it delegates to ``get_task_instance`` so typologies reflect reality.
"""

from __future__ import annotations

import dynastore.extensions.processes.runners as runners_module
from dynastore.extensions.processes.runners import FastAPIBackgroundRunner


def test_can_handle_returns_false_when_no_task_instance(monkeypatch):
    monkeypatch.setattr(runners_module, "get_task_instance", lambda _: None)
    runner = FastAPIBackgroundRunner()
    assert runner.can_handle("gdal") is False


def test_can_handle_returns_true_when_task_instance_present(monkeypatch):
    sentinel = object()
    monkeypatch.setattr(runners_module, "get_task_instance", lambda _: sentinel)
    runner = FastAPIBackgroundRunner()
    assert runner.can_handle("gdal") is True


def test_can_handle_task_type_is_forwarded(monkeypatch):
    """The exact task_type string received is what gets_task_instance is called with."""
    received = {}

    def _capture(task_type):
        received["task_type"] = task_type
        return None

    monkeypatch.setattr(runners_module, "get_task_instance", _capture)
    runner = FastAPIBackgroundRunner()
    runner.can_handle("my_custom_process")
    assert received["task_type"] == "my_custom_process"
