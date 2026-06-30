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

"""Unit tests: principals-family queries always target the platform ``iam`` schema.

Regression for: ``GET /admin/principals`` 500 —
``42P01: relation "{tenant}.principals" does not exist``.

Root cause: ``build_search_principals_query`` used ``{schema}.principals``
for the FROM clause even when a tenant schema was passed as the grants-join
scope.  Principals are platform-global and live only in ``iam.principals``;
the FROM clause must never be rewritten to a tenant schema.

No DB or Valkey required — all assertions are on the generated SQL string.
"""
from __future__ import annotations


# ---------------------------------------------------------------------------
# build_search_principals_query — FROM clause always uses ``iam``
# ---------------------------------------------------------------------------


def test_principals_table_always_iam_no_role_no_identifier():
    """With no filters the FROM clause must reference ``iam.principals``."""
    from dynastore.modules.iam.iam_queries import build_search_principals_query

    query, _params = build_search_principals_query(
        identifier=None, role=None, limit=50, offset=0, schema="iam"
    )
    assert "iam.principals" in query.template, (
        "principals FROM clause must use iam.principals, got: " + query.template
    )


def test_principals_table_always_iam_when_tenant_schema_passed():
    """Passing a tenant schema must NOT redirect the FROM clause away from ``iam``."""
    from dynastore.modules.iam.iam_queries import build_search_principals_query

    tenant = "c_af5eseh9n53pf"
    query, _params = build_search_principals_query(
        identifier=None, role=None, limit=50, offset=0, schema=tenant
    )
    # The tenant schema must not appear in the principals FROM reference.
    assert f"{tenant}.principals" not in query.template, (
        f"tenant schema {tenant!r} must not be used for principals table; "
        "got: " + query.template
    )
    assert "iam.principals" in query.template, (
        "principals FROM clause must always be iam.principals regardless of schema arg; "
        "got: " + query.template
    )


def test_principals_table_always_iam_with_identifier_filter():
    """Identifier filter + tenant schema: FROM still targets ``iam.principals``."""
    from dynastore.modules.iam.iam_queries import build_search_principals_query

    tenant = "c_af5eseh9n53pf"
    query, params = build_search_principals_query(
        identifier="alice", role=None, limit=10, offset=0, schema=tenant
    )
    assert "iam.principals" in query.template
    assert f"{tenant}.principals" not in query.template
    assert "identifier_pattern" in params


def test_grants_join_uses_provided_schema_for_role_filter():
    """When a role filter is active, the grants JOIN must use the provided schema.

    This is intentional: the caller can pass a tenant schema to filter by
    catalog-scoped role grants while still reading principals from ``iam``.
    """
    from dynastore.modules.iam.iam_queries import build_search_principals_query

    tenant = "c_af5eseh9n53pf"
    query, params = build_search_principals_query(
        identifier=None, role="viewer", limit=50, offset=0, schema=tenant
    )
    # Principals still from iam
    assert "iam.principals" in query.template, (
        "principals FROM must still be iam.principals when role filter is set"
    )
    # Grants join uses the requested scope schema
    assert f"{tenant}.grants" in query.template, (
        f"grants JOIN should reference {tenant}.grants for catalog-scoped role filter; "
        "got: " + query.template
    )
    assert "role" in params and params["role"] == "viewer"


def test_grants_join_uses_iam_schema_for_platform_role_filter():
    """Platform-scope role filter: grants JOIN uses ``iam.grants``."""
    from dynastore.modules.iam.iam_queries import build_search_principals_query

    query, params = build_search_principals_query(
        identifier=None, role="sysadmin", limit=50, offset=0, schema="iam"
    )
    assert "iam.principals" in query.template
    assert "iam.grants" in query.template
    assert params["role"] == "sysadmin"


def test_identifier_and_role_with_tenant_schema():
    """Combined identifier + role filter with tenant schema."""
    from dynastore.modules.iam.iam_queries import build_search_principals_query

    tenant = "c_af5eseh9n53pf"
    query, params = build_search_principals_query(
        identifier="bob", role="editor", limit=25, offset=10, schema=tenant
    )
    assert "iam.principals" in query.template
    assert f"{tenant}.principals" not in query.template
    assert f"{tenant}.grants" in query.template
    assert params["identifier_pattern"] == "%bob%"
    assert params["role"] == "editor"
    assert params["limit"] == 25
    assert params["offset"] == 10
