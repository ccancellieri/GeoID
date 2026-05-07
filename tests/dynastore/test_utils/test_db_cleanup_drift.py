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

"""Drift detection between the two cleanup-constant declarations.

The cleanup constants live in two places — by design (see the module
docstring at ``tests/dynastore/test_utils/db_cleanup.py``):

1. ``tests/dynastore/test_utils/db_cleanup.py`` — TEST-ONLY, never
   shipped in the production wheel. Used by the test-suite cleanup
   fixture (``tests/dynastore/modules/catalog/cleanup.py``).
2. ``packages/core/src/dynastore/scripts/db_reset.py`` — production wheel, invoked
   by the ``Reset database (review only)`` Cloud Run job.

The duplication is intentional: importing the test-utils module from a
production module would defeat the structural separation. These tests
pin the two declarations byte-for-byte so they cannot diverge silently.
"""
from __future__ import annotations

import re
from pathlib import Path

import pytest

from tests.dynastore.test_utils import db_cleanup as dc
from dynastore.scripts import db_reset as dbr


# ── Test-utils module: shape + semantics ───────────────────────────────────────


def test_tenant_pattern_matches_generated_physical_names() -> None:
    rx = re.compile(dc.TENANT_SCHEMA_PATTERN)
    # Anchored: only the exact 8-char base36 form matches.
    assert rx.fullmatch("s_abcd1234")
    assert rx.fullmatch("s_00000000")
    assert rx.fullmatch("s_zzzzzzzz")
    # Misses
    assert not rx.fullmatch("s_abcd123")     # 7 chars
    assert not rx.fullmatch("s_abcd12345")   # 9 chars
    assert not rx.fullmatch("S_ABCD1234")    # uppercase prefix
    assert not rx.fullmatch("public")
    assert not rx.fullmatch("s_ABCD1234")    # uppercase digits


def test_preserved_schemas_includes_required_system_set() -> None:
    """Preserved schemas MUST include the PostgreSQL system catalogs +
    cron + the keycloak side table + Cloud SQL ML extensions."""
    required = {
        "pg_catalog", "information_schema", "pg_toast", "cron", "public",
        "keycloak", "ai", "google_ml", "topology",
    }
    assert required.issubset(dc.DEFAULT_PRESERVED_SCHEMAS), (
        f"Missing required preserved schemas: "
        f"{required - dc.DEFAULT_PRESERVED_SCHEMAS}"
    )


def test_system_cron_jobs_includes_known_jobs() -> None:
    expected = {
        "system_cleanup_orphaned_cron_jobs",
        "monthly_cleanup_system_logs",
    }
    assert expected.issubset(set(dc.DEFAULT_SYSTEM_CRON_JOBS))


def test_drop_schemas_batch_sql_quotes_each_name() -> None:
    sql = dc.drop_schemas_batch_sql(["s_aaaa1111", "s_bbbb2222"])
    assert 'DROP SCHEMA IF EXISTS "s_aaaa1111" CASCADE;' in sql
    assert 'DROP SCHEMA IF EXISTS "s_bbbb2222" CASCADE;' in sql


def test_drop_schemas_batch_sql_empty_iterable() -> None:
    assert dc.drop_schemas_batch_sql([]) == ""


# ── Drift detection: test-utils vs db_reset.py ────────────────────────────────


def test_preserved_schemas_match_db_reset_default() -> None:
    """The test-utils module and db_reset.py must declare the same
    preserved-schemas allowlist. Drift means a real reset would either
    drop a system schema (outage) or skip a tenant schema (stale data)."""
    assert dc.DEFAULT_PRESERVED_SCHEMAS == dbr._DEFAULT_PRESERVED_SCHEMAS, (
        "Drift between test_utils.db_cleanup.DEFAULT_PRESERVED_SCHEMAS "
        "and dynastore.scripts.db_reset._DEFAULT_PRESERVED_SCHEMAS — "
        "update both."
    )


def test_system_cron_jobs_match_db_reset_default() -> None:
    """Same drift rule for the cron-jobs allowlist."""
    assert dc.DEFAULT_SYSTEM_CRON_JOBS == dbr._DEFAULT_SYSTEM_CRON_JOBS, (
        "Drift between test_utils.db_cleanup.DEFAULT_SYSTEM_CRON_JOBS "
        "and dynastore.scripts.db_reset._DEFAULT_SYSTEM_CRON_JOBS — "
        "update both."
    )


# ── Production-safety guard on db_reset ───────────────────────────────────────


def test_refuse_in_production_blocks_when_env_label_is_prod(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("DYNASTORE_ENV", "production")
    monkeypatch.delenv("DYNASTORE_RESET_ALLOW_PRODUCTION", raising=False)
    with pytest.raises(SystemExit) as ei:
        dbr._refuse_in_production()
    assert ei.value.code == 2


def test_refuse_in_production_blocks_when_environment_is_prod(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("DYNASTORE_ENV", raising=False)
    monkeypatch.setenv("ENVIRONMENT", "PROD")  # case-insensitive
    monkeypatch.delenv("DYNASTORE_RESET_ALLOW_PRODUCTION", raising=False)
    with pytest.raises(SystemExit) as ei:
        dbr._refuse_in_production()
    assert ei.value.code == 2


def test_refuse_in_production_passes_when_explicit_opt_out(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("DYNASTORE_ENV", "production")
    monkeypatch.setenv("DYNASTORE_RESET_ALLOW_PRODUCTION", "1")
    # Returns None (no SystemExit raised) when explicitly opted out.
    assert dbr._refuse_in_production() is None


def test_refuse_in_production_passes_when_env_is_review(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("DYNASTORE_ENV", "review")
    monkeypatch.delenv("DYNASTORE_RESET_ALLOW_PRODUCTION", raising=False)
    assert dbr._refuse_in_production() is None


def test_refuse_in_production_passes_when_env_unset(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("DYNASTORE_ENV", raising=False)
    monkeypatch.delenv("ENVIRONMENT", raising=False)
    monkeypatch.delenv("DYNASTORE_RESET_ALLOW_PRODUCTION", raising=False)
    assert dbr._refuse_in_production() is None


# ── Structural separation: production wheel does NOT contain db_cleanup ───────


def test_db_cleanup_not_importable_from_production_namespace() -> None:
    """The production package ``dynastore.tools`` must NOT expose
    ``db_cleanup``. If this test starts failing, someone re-introduced
    the cleanup constants into the production wheel — which means
    production-traffic code paths could import them and accidentally
    trigger destructive logic."""
    with pytest.raises(ImportError):
        from dynastore.tools import db_cleanup  # noqa: F401  type: ignore[import-not-found]


def test_db_cleanup_lives_under_tests_directory() -> None:
    """The test-utils module must be under tests/ — outside the
    production wheel build path (``[tool.setuptools.packages.find]
    where = ['src']`` in pyproject.toml)."""
    module_path = Path(dc.__file__).resolve()
    parts = module_path.parts
    assert "tests" in parts, (
        f"db_cleanup.py is at {module_path} — expected a path containing "
        f"'tests/' so it stays out of the production wheel."
    )
    assert "src" not in parts, (
        f"db_cleanup.py is at {module_path} — the 'src/' segment means it "
        f"would ship in the production wheel."
    )


# ── Wired-through smoke: the test-suite cleanup imports from test_utils ───────


def test_test_cleanup_imports_canonical_constants() -> None:
    from tests.dynastore.modules.catalog import cleanup as cl

    assert cl.TENANT_SCHEMA_PATTERN == dc.TENANT_SCHEMA_PATTERN
    assert cl.SCHEMA_DROP_BATCH_SIZE == dc.SCHEMA_DROP_BATCH_SIZE
    assert cl.CATALOG_METADATA_TABLES == dc.CATALOG_METADATA_TABLES
    assert (
        cl.DELETE_ORPHAN_GCP_BUCKET_RECORDS_SQL
        == dc.DELETE_ORPHAN_GCP_BUCKET_RECORDS_SQL
    )
