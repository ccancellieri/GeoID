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

"""Contract tests: geometry_fetcher routes SQL through DQLQuery.

Verifies that ``SidecarGeometryFetcher.get_geometries`` uses DQLQuery
with named bind parameters (``ANY(:feature_ids)``) instead of the
legacy positional ``$1::text[]`` placeholder and raw ``conn.execute``.
"""

from __future__ import annotations

import inspect

from dynastore.modules.volumes import geometry_fetcher as _mod


def test_build_geometry_query_uses_named_placeholder():
    """``build_geometry_query`` must emit ``:feature_ids`` not ``$1::text[]``."""
    from dynastore.modules.volumes.geometry_fetcher import (
        GeometryQuerySpec,
        build_geometry_query,
    )

    spec = GeometryQuerySpec(
        schema="s",
        hub_table="h",
        geometries_table="h_g",
        feature_ids=["f1"],
    )
    sql = build_geometry_query(spec)
    assert ":feature_ids" in sql, "Expected named placeholder :feature_ids in SQL"
    assert "$1" not in sql, "Positional $1 placeholder must not appear in SQL"


def test_get_geometries_uses_dqlquery_not_raw_execute():
    """``SidecarGeometryFetcher.get_geometries`` must not call conn.execute(text(...)."""
    src = inspect.getsource(_mod.SidecarGeometryFetcher.get_geometries)
    assert "conn.execute(" not in src, (
        "conn.execute( found in get_geometries — use DQLQuery instead"
    )
    assert "DQLQuery" in src, "DQLQuery must be used in get_geometries"


def test_module_uses_named_feature_ids_param():
    """The full module source must not contain the old positional placeholder."""
    src = inspect.getsource(_mod)
    assert "$1::text[]" not in src, (
        "Positional $1::text[] placeholder found — use ANY(:feature_ids) with DQLQuery"
    )
