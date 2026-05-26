#    Copyright 2025 FAO
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

"""Contract tests: sidecar_bounds routes SQL through DQLQuery.

Verifies that ``SidecarBoundsSource.get_bounds`` uses DQLQuery instead
of raw ``conn.execute`` so it benefits from the standardized executor
infrastructure (connection-lock scope, retry, error mapping).
"""

from __future__ import annotations

import inspect

from dynastore.modules.volumes import sidecar_bounds as _mod


def test_get_bounds_uses_dqlquery_not_raw_execute():
    """``SidecarBoundsSource.get_bounds`` must not call conn.execute directly."""
    src = inspect.getsource(_mod.SidecarBoundsSource.get_bounds)
    assert "conn.execute(" not in src, (
        "conn.execute( found in get_bounds — use DQLQuery instead"
    )
    assert "DQLQuery" in src, "DQLQuery must be used in get_bounds"


def test_module_imports_dqlquery():
    """The sidecar_bounds module must import DQLQuery from the query executor."""
    src = inspect.getsource(_mod)
    assert "DQLQuery" in src, "DQLQuery import not found in sidecar_bounds module"
    assert "ResultHandler" in src, "ResultHandler import not found in sidecar_bounds module"
