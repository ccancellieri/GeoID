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

"""Pin the contract on the SSOT cleanup constants.

Drift between the test-suite cleanup and the operational reset script
costs an outage every time it happens (the reset wipes a system schema,
or the cleanup leaves a tenant schema behind). These tests keep both
callers reading the same values.
"""
from __future__ import annotations

import re
from pathlib import Path

import pytest

from dynastore.tools import db_cleanup as dc


def test_tenant_pattern_matches_generated_physical_names() -> None:
    rx = re.compile(dc.TENANT_SCHEMA_PATTERN)
    # Anchored: only the exact 8-char base36 form matches.
    assert rx.fullmatch("s_abcd1234")
    assert rx.fullmatch("s_00000000")
    assert rx.fullmatch("s_zzzzzzzz")
    # Misses
    assert not rx.fullmatch("s_abcd123")     # 7 chars
    assert not rx.fullmatch("s_abcd12345")   # 9 chars
    assert not rx.fullmatch("S_ABCD1234")    # uppercase
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
    # No leakage of unquoted names.
    assert "s_aaaa1111 " not in sql.replace('"s_aaaa1111"', "")


def test_drop_schemas_batch_sql_empty_iterable() -> None:
    assert dc.drop_schemas_batch_sql([]) == ""


def test_load_reset_policy_overrides_returns_defaults_when_file_missing(
    tmp_path: Path,
) -> None:
    missing = tmp_path / "does-not-exist.env"
    preserved, cron = dc.load_reset_policy_overrides(missing)
    assert preserved == dc.DEFAULT_PRESERVED_SCHEMAS
    assert cron == dc.DEFAULT_SYSTEM_CRON_JOBS


def test_load_reset_policy_overrides_reads_overrides(tmp_path: Path) -> None:
    policy_file = tmp_path / "reset_policy.env"
    policy_file.write_text(
        '# comment\n'
        'PRESERVED_SCHEMAS="pg_catalog public special_extra_one"\n'
        'SYSTEM_CRON_JOBS="job_one job_two"\n'
    )
    preserved, cron = dc.load_reset_policy_overrides(policy_file)
    assert preserved == frozenset({"pg_catalog", "public", "special_extra_one"})
    assert cron == ("job_one", "job_two")


def test_load_reset_policy_overrides_skips_blank_and_unknown_keys(
    tmp_path: Path,
) -> None:
    policy_file = tmp_path / "reset_policy.env"
    policy_file.write_text(
        "\n# pure comment\n"
        "RANDOM_OTHER_KEY=ignored\n"
        '   PRESERVED_SCHEMAS=" only_this "\n'
    )
    preserved, cron = dc.load_reset_policy_overrides(policy_file)
    assert preserved == frozenset({"only_this"})
    # Cron untouched → defaults.
    assert cron == dc.DEFAULT_SYSTEM_CRON_JOBS


def test_db_reset_module_imports_canonical_constants() -> None:
    """db_reset.py must read PRESERVED_SCHEMAS / SYSTEM_CRON_JOBS from
    db_cleanup, not maintain its own copy. Drift would mean the test
    cleanup and the prod reset wouldn't agree on what's preserved."""
    from dynastore.scripts import db_reset as dbr

    # The constants come from db_cleanup; their default values must
    # equal db_cleanup's defaults until reset_policy.env overrides them.
    # Since we run inside the source tree, the policy file IS present
    # at scripts/reset_policy.env — check that PRESERVED_SCHEMAS at
    # least includes the canonical core set.
    core_set = {
        "pg_catalog", "information_schema", "pg_toast", "public",
    }
    assert core_set.issubset(dbr.PRESERVED_SCHEMAS)


def test_test_cleanup_imports_canonical_constants() -> None:
    """The test-suite cleanup must reference the same SSOT constants."""
    from tests.dynastore.modules.catalog import cleanup as cl

    # Smoke: the imports should be visible in the module namespace.
    assert cl.TENANT_SCHEMA_PATTERN == dc.TENANT_SCHEMA_PATTERN
    assert cl.SCHEMA_DROP_BATCH_SIZE == dc.SCHEMA_DROP_BATCH_SIZE
    assert cl.CATALOG_METADATA_TABLES == dc.CATALOG_METADATA_TABLES
    assert cl.DELETE_ORPHAN_GCP_BUCKET_RECORDS_SQL == dc.DELETE_ORPHAN_GCP_BUCKET_RECORDS_SQL
