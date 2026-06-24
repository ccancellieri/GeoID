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

"""Regression coverage for the per-catalog /logs read returning [] for every
catalog.

`search_logs` unions system logs (catalog.system_logs) with tenant logs
({schema}.logs). The two tables declare their columns in a DIFFERENT physical
order — `timestamp` is column 2 in the tenant table but the LAST column in
system_logs. A `SELECT * ... UNION ALL SELECT *` aligns columns positionally,
so the branches collide (catalog_id VARCHAR vs timestamp TIMESTAMPTZ) and the
whole query raises a type-mismatch error that search_logs swallows, returning
[] — hiding ALL per-catalog logs, system and tenant alike.

These tests pin both halves of the contract:

1. The DDLs really do declare columns in a divergent order (so a positional
   `SELECT *` union is unsafe — documents WHY the explicit projection exists).

2. search_logs no longer selects `*` from the raw tables; it projects an
   explicit, identically-ordered column list from each branch.
"""

from __future__ import annotations

import inspect
import re

from dynastore.extensions.logs.log_extension import LogExtension
from dynastore.modules.catalog.log_manager import SYSTEM_LOGS_DDL, TENANT_LOGS_DDL


def _column_order(ddl: str) -> list[str]:
    """Extract column names in declaration order from a CREATE TABLE DDL body."""
    body = ddl[ddl.index("(") + 1 :]
    cols: list[str] = []
    for line in body.splitlines():
        token = line.strip().split(" ", 1)[0].strip(",")
        # column lines start with an identifier; skip constraints / empties
        if re.fullmatch(r"[a-z_][a-z0-9_]*", token) and token not in {
            "primary",
            "partition",
        }:
            cols.append(token)
    return cols


def test_system_and_tenant_log_columns_diverge() -> None:
    sys_cols = _column_order(SYSTEM_LOGS_DDL)
    tenant_cols = _column_order(TENANT_LOGS_DDL)
    # Same set of columns ...
    assert set(sys_cols) == set(tenant_cols)
    # ... but a DIFFERENT order — which is exactly why a positional `SELECT *`
    # UNION ALL collides and must not be used.
    assert sys_cols != tenant_cols, (
        "If the column orders were unified, the SELECT * union would be safe; "
        "until then search_logs must project explicit columns."
    )


def test_search_logs_projects_explicit_columns() -> None:
    src = inspect.getsource(LogExtension.search_logs)
    # No `SELECT *` straight off the physical tables (that's the bug).
    assert "SELECT * FROM catalog." not in src
    assert 'SELECT * FROM "' not in src
    # The explicit projection must name timestamp so both branches align.
    assert "event_type" in src and "timestamp" in src
