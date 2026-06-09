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

"""Regression pin: ``platform_config_service`` imports cleanly from a fresh
interpreter, with no circular-import ImportError (#686).

History: the mutability marker types and the ``PluginConfig`` base used to
live inside ``platform_config_service`` itself.  Any Protocol-contracts
module that subclassed ``PluginConfig`` — notably ``IamRolesConfig`` in
``models/protocols/authorization.py`` — therefore imported the heavy
``platform_config_service`` (and the ``models.protocols`` eager hub it
pulls), forming a load-order cycle.  Pytest collection masked it by
importing ``dynastore.models.protocols`` to completion first; external
entry points hit the failure:

  - ``import dynastore.modules.db_config.platform_config_service``
    (operator scripts, notebooks, REPL)
  - ``import dynastore.modules.tasks.models`` via the task runner
    (``main_task.report_failure``) — observed in production

The fix (#686) extracted the markers into the dependency-light leaf
``dynastore.models.mutability`` and ``PluginConfig`` into
``dynastore.models.plugin_config``.  ``authorization.py`` /
``exposure_mixin.py`` / ``tasks_config.py`` now import from those leaves,
so subclassing ``PluginConfig`` no longer drags the protocols hub into a
half-initialised config stack.  ``platform_config_service`` re-exports
both for backward compatibility.

Each case runs in a subprocess with a fresh interpreter so the in-process
module cache from pytest collection cannot mask a regression.
"""

import subprocess
import sys

import pytest

# Entry points that bypass the pytest-collection import order and would
# trip the cycle if it ever regressed.
_FRESH_IMPORT_CASES = [
    "import dynastore.modules.db_config.platform_config_service",
    "import dynastore.extensions.tools.exposure_mixin",
    "import dynastore.models.protocols.authorization",
    "import dynastore.modules.tasks.models",
]


@pytest.mark.parametrize("import_stmt", _FRESH_IMPORT_CASES)
def test_imports_cleanly_from_fresh_interpreter(import_stmt: str):
    """Spawn ``python -c '<import>'`` and assert exit 0 (no circular import)."""
    result = subprocess.run(
        [sys.executable, "-c", import_stmt],
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, (
        f"`{import_stmt}` failed in a fresh interpreter "
        f"(exit {result.returncode}) — likely a reintroduced circular import. "
        f"stderr tail:\n{result.stderr[-2000:]}"
    )
