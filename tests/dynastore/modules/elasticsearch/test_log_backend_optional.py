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

"""Tests that the ES log backend is import-safe without the logs extension.

``ElasticsearchLogBackend`` uses ``LogEntryCreate`` only as a type annotation;
it must never appear in the module's runtime namespace so that
``dynastore.modules.elasticsearch.log_backend`` can be imported on any SCOPE
regardless of whether ``dynastore.extensions.logs`` is installed.
"""

from __future__ import annotations

import importlib
import sys
from contextlib import contextmanager
from types import ModuleType
from typing import Generator


@contextmanager
def _block_logs_extension() -> Generator[None, None, None]:
    """Temporarily hide ``dynastore.extensions.logs`` and its submodules.

    Setting ``sys.modules[name] = None`` makes ``importlib.util.find_spec``
    and the import machinery treat the package as absent, matching a SCOPE
    where ``extension_logs`` is not pip-installed.
    """
    blocked = {
        "dynastore.extensions.logs": None,
        "dynastore.extensions.logs.models": None,
    }
    saved = {name: sys.modules.get(name) for name in blocked}
    sys.modules.update(blocked)  # type: ignore[arg-type]
    try:
        yield
    finally:
        for name, old in saved.items():
            if old is None:
                sys.modules.pop(name, None)
            else:
                sys.modules[name] = old


def test_log_backend_imports_without_logs_extension() -> None:
    """Importing log_backend must not raise when the logs extension is absent.

    A clean reload of the module inside the blocked-extension context is used
    so the test does not depend on import-order relative to other tests that
    may have already imported (and cached) the module with the extension
    present.
    """
    with _block_logs_extension():
        # Evict any cached version so the reload actually re-executes the
        # module top-level code under our blocked-extension context.
        sys.modules.pop("dynastore.modules.elasticsearch.log_backend", None)

        # Must not raise ImportError / ModuleNotFoundError.
        mod: ModuleType = importlib.import_module(
            "dynastore.modules.elasticsearch.log_backend"
        )

        assert hasattr(mod, "ElasticsearchLogBackend"), (
            "ElasticsearchLogBackend must be defined after import"
        )

        # Instantiating the class must also succeed — the constructor does not
        # touch LogEntryCreate.
        backend = mod.ElasticsearchLogBackend()
        assert backend.name == "elasticsearch"

    # Restore the module from the real package for subsequent tests.
    sys.modules.pop("dynastore.modules.elasticsearch.log_backend", None)
    importlib.import_module("dynastore.modules.elasticsearch.log_backend")


def test_log_entry_create_not_in_log_backend_runtime_namespace() -> None:
    """``LogEntryCreate`` must not be bound in log_backend's module namespace.

    It is referenced only in a ``TYPE_CHECKING`` block, so it should never
    appear in ``vars(module)`` at runtime — confirming that importing the
    module has no dependency on ``dynastore.extensions.logs``.
    """
    # Use the cached module if available; it must be importable regardless.
    if "dynastore.modules.elasticsearch.log_backend" not in sys.modules:
        importlib.import_module("dynastore.modules.elasticsearch.log_backend")

    mod = sys.modules["dynastore.modules.elasticsearch.log_backend"]

    assert "LogEntryCreate" not in vars(mod), (
        "LogEntryCreate must not be present in log_backend's runtime namespace; "
        "it is a type-only annotation guarded by TYPE_CHECKING."
    )
