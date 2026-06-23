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

"""Unit tests for the M2.1 domain-scoped Primary metadata drivers.

Focuses on pure-python invariants that don't require a live database:

- Declared capabilities / domain / implemented protocol.
- ``_filter_payload`` correctly strips keys outside the driver's domain.
- Domain column sets match the M2.0 DDL.
- SQL-builder smoke tests with a mocked engine confirm the driver uses
  the right table, no column leakage across domains, and correctly
  stamps ``updated_at = NOW()`` on upsert.
- Entry-points expose the four new drivers under canonical names.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from dynastore.models.protocols.entity_store import (
    CatalogStore,
    CollectionStore,
    EntityStoreCapability,
)


# ---------------------------------------------------------------------------
# Static invariants
# ---------------------------------------------------------------------------


class TestStructuralInvariants:
    def test_collection_core_driver_identifies_as_core_collection(self):
        from dynastore.modules.storage.drivers.core_postgresql import (
            CollectionCorePostgresqlDriver,
        )

        d = CollectionCorePostgresqlDriver()
        assert d._table == "collection_core"
        assert EntityStoreCapability.READ in d.capabilities
        assert EntityStoreCapability.WRITE in d.capabilities
        assert EntityStoreCapability.SEARCH in d.capabilities
        assert EntityStoreCapability.SOFT_DELETE in d.capabilities
        # Structural typing — CollectionStore is @runtime_checkable.
        assert isinstance(d, CollectionStore)

    def test_catalog_core_driver_identifies_as_core_catalog(self):
        from dynastore.modules.storage.drivers.core_postgresql import (
            CatalogCorePostgresqlDriver,
        )

        d = CatalogCorePostgresqlDriver()
        assert d._table == "catalog_core"
        assert isinstance(d, CatalogStore)

class TestDomainColumnSets:
    """Column sets must match the M2.0 DDL exactly.

    Guards against drift between the driver's ``_columns`` tuple and the
    CREATE TABLE statements in ``modules/catalog/db_init/core_tables.py``.
    A stray column on the Python side silently succeeds on UPDATE (the
    EXCLUDED alias accepts any column) but fails on INSERT with
    UndefinedColumn.  Pinning the column sets here means the test fails
    loudly at unit-test time.
    """

    def test_collection_core_columns(self):
        from dynastore.modules.storage.drivers.core_postgresql import (
            _COLLECTION_CORE_COLUMNS,
        )

        assert _COLLECTION_CORE_COLUMNS == (
            "title", "description", "keywords", "license", "extra_metadata",
        )

    def test_catalog_core_columns(self):
        from dynastore.modules.storage.drivers.core_postgresql import (
            _CATALOG_CORE_COLUMNS,
        )

        assert _CATALOG_CORE_COLUMNS == (
            "title", "description", "keywords", "license", "extra_metadata",
        )

class TestPayloadFilter:
    def test_filter_payload_drops_unowned_keys(self):
        from dynastore.modules.storage.drivers.core_postgresql import (
            _COLLECTION_CORE_COLUMNS, _filter_payload,
        )

        full = {
            # CORE
            "title": {"en": "T"}, "description": {"en": "D"},
            "keywords": ["k"], "license": "CC-BY-4.0",
            "extra_metadata": {"_internal": 1},
            # STAC (should be dropped by the CORE filter)
            "extent": {"spatial": {"bbox": [[0, 0, 1, 1]]}},
            "stac_version": "1.1.0",
            "providers": [{"name": "FAO"}],
        }
        filtered = _filter_payload(full, _COLLECTION_CORE_COLUMNS)
        assert set(filtered) == set(_COLLECTION_CORE_COLUMNS)
        assert "extent" not in filtered
        assert "stac_version" not in filtered
        assert filtered["title"] == {"en": "T"}

    def test_filter_payload_empty_input_yields_empty_dict(self):
        """Partial-update semantics: absent keys are DROPPED, not set to None.

        An empty input must yield an empty payload — the upsert path
        then falls to the 'pure updated_at bump' branch and NEVER writes
        SET col = NULL for columns the caller did not supply.  Without
        this, a subsequent upsert with ``{"title": "T2"}`` on a row that
        previously had description / keywords would overwrite both to
        NULL (data loss).
        """
        from dynastore.modules.storage.drivers.core_postgresql import (
            _COLLECTION_CORE_COLUMNS, _filter_payload,
        )

        filtered = _filter_payload({}, _COLLECTION_CORE_COLUMNS)
        assert filtered == {}

    def test_filter_payload_drops_none_values(self):
        """Explicit None values are dropped — same as absent keys."""
        from dynastore.modules.storage.drivers.core_postgresql import (
            _COLLECTION_CORE_COLUMNS, _filter_payload,
        )

        filtered = _filter_payload(
            {"title": "T", "description": None, "keywords": ["k"]},
            _COLLECTION_CORE_COLUMNS,
        )
        # Only the two keys with non-None values survive.
        assert filtered == {"title": "T", "keywords": ["k"]}


# ---------------------------------------------------------------------------
# Isolated CRUD behaviour (mocked engine)
# ---------------------------------------------------------------------------


@pytest.fixture
def fake_conn_with_dql():
    """Return (fake_engine, AsyncMock dql_execute) and patch DQLQuery.

    Lets each test inspect the exact SQL string and bind params the
    driver issued without needing a live DB.
    """
    import dynastore.modules.storage.drivers.core_postgresql as mod

    fake_engine = MagicMock()
    dql_execute = AsyncMock(return_value=None)

    class _FakeDQL:
        def __init__(self, sql, result_handler=None):
            self.sql = sql
            self.result_handler = result_handler

        async def execute(self, conn, **kwargs):
            return await dql_execute(self.sql, **kwargs)

    fake_txn = AsyncMock()
    fake_txn.__aenter__ = AsyncMock(return_value=MagicMock())
    fake_txn.__aexit__ = AsyncMock(return_value=None)

    patches = [
        patch.object(mod, "DQLQuery", _FakeDQL),
        patch.object(mod, "managed_transaction", lambda engine: fake_txn),
        patch.object(mod, "_get_engine", return_value=fake_engine),
        patch.object(mod, "_resolve_physical_schema", AsyncMock(return_value="t_alpha")),
        patch.object(
            mod, "_resolve_collection_physical_id", AsyncMock(return_value="c_col"),
        ),
    ]
    for p in patches:
        p.start()
    try:
        yield fake_engine, dql_execute
    finally:
        for p in patches:
            p.stop()


@pytest.mark.asyncio
async def test_collection_core_upsert_uses_core_columns_only(fake_conn_with_dql):
    """Full CORE payload → INSERT column list matches supplied CORE keys."""
    from dynastore.modules.storage.drivers.core_postgresql import (
        CollectionCorePostgresqlDriver,
    )

    _, dql_execute = fake_conn_with_dql
    d = CollectionCorePostgresqlDriver()
    await d.upsert_metadata(
        "cat", "col",
        metadata={
            "title": {"en": "T"},
            "description": {"en": "D"},
            "keywords": ["k"],
            "license": "CC-BY-4.0",
            "extra_metadata": {"foo": "bar"},
            "extent": "SHOULD BE IGNORED — STAC COLUMN",
            "providers": ["IGNORE"],
        },
    )

    dql_execute.assert_awaited_once()
    sql, params = dql_execute.call_args.args[0], dql_execute.call_args.kwargs
    assert '"t_alpha".collection_core' in sql
    # STAC columns never appear in the SQL (domain filtered).
    assert "extent" not in sql
    assert "providers" not in sql
    # CORE columns appear in both the column list and the ON CONFLICT
    # UPDATE set — so "title" shows up twice.  Assert both directions.
    assert sql.count("title") >= 2
    assert "NOW()" in sql  # updated_at stamping
    # Bind params cover only the five CORE keys the caller supplied (plus :id).
    assert set(params) == {"id", "title", "description", "keywords", "license", "extra_metadata"}


@pytest.mark.asyncio
async def test_collection_core_upsert_partial_update_preserves_other_columns(
    fake_conn_with_dql,
):
    """PATCH semantics: supplying only ``title`` must NOT emit SET description=NULL.

    Regression for C1 (2026-04-21 code review): a partial upsert that
    previously wrote a full column list with None placeholders would
    NULL-out the existing description/keywords/license/extra_metadata
    on any follow-up write.  The post-fix SQL builder only references
    the supplied columns, so the DO UPDATE SET clause leaves the
    other columns untouched.
    """
    from dynastore.modules.storage.drivers.core_postgresql import (
        CollectionCorePostgresqlDriver,
    )

    _, dql_execute = fake_conn_with_dql
    d = CollectionCorePostgresqlDriver()
    await d.upsert_metadata(
        "cat", "col", metadata={"title": {"en": "T2"}},
    )
    dql_execute.assert_awaited_once()
    sql, params = dql_execute.call_args.args[0], dql_execute.call_args.kwargs
    assert '"t_alpha".collection_core' in sql
    # title appears (it was supplied) …
    assert "title" in sql
    # … but no other CORE column is referenced in the SQL.  A ``SET
    # description = EXCLUDED.description`` would overwrite existing data.
    for untouched in ("description", "keywords", "license", "extra_metadata"):
        assert untouched not in sql, (
            f"SQL still references {untouched!r} on partial update — "
            f"this would NULL-out the existing column value.  Check "
            f"_filter_payload / _upsert_collection_row."
        )
    # Bind params contain only :id + the one supplied column.
    assert set(params) == {"id", "title"}


@pytest.mark.asyncio
async def test_collection_core_upsert_empty_payload_only_bumps_updated_at(
    fake_conn_with_dql,
):
    """Empty filtered payload → INSERT ... (collection_id, updated_at) ONLY.

    Without a payload, we still want the INSERT to fire because
    INDEX / BACKUP propagation reads ``updated_at`` as the freshness
    token — a silent no-op here would hide the activity.
    """
    from dynastore.modules.storage.drivers.core_postgresql import (
        CollectionCorePostgresqlDriver,
    )

    _, dql_execute = fake_conn_with_dql
    d = CollectionCorePostgresqlDriver()
    await d.upsert_metadata("cat", "col", metadata={})
    dql_execute.assert_awaited_once()
    sql, params = dql_execute.call_args.args[0], dql_execute.call_args.kwargs
    assert sql.startswith(
        'INSERT INTO "t_alpha".collection_core '
        '(collection_physical_id, updated_at)'
    )
    assert (
        "ON CONFLICT (collection_physical_id) DO UPDATE SET updated_at = NOW()"
        in sql
    )
    assert set(params) == {"id"}


@pytest.mark.asyncio
async def test_catalog_core_upsert_targets_global_schema(fake_conn_with_dql):
    from dynastore.modules.storage.drivers.core_postgresql import (
        CatalogCorePostgresqlDriver,
    )

    _, dql_execute = fake_conn_with_dql
    d = CatalogCorePostgresqlDriver()
    await d.upsert_catalog_metadata(
        "cat",
        metadata={"title": {"en": "T"}, "description": {"en": "D"}},
    )

    dql_execute.assert_awaited_once()
    sql = dql_execute.call_args.args[0]
    # catalog-tier driver uses the global catalog schema, NOT {schema}.
    assert "catalog.catalog_core" in sql
    assert '"t_alpha"' not in sql  # no tenant schema reference
    assert "NOW()" in sql


# ---------------------------------------------------------------------------
# Entry-point discovery
# ---------------------------------------------------------------------------


class TestColumnTupleAlignment:
    """N2 regression: the ``_*_COLUMNS`` tuples must stay in lock-step with
    the CREATE TABLE column lists in ``core_tables.py``.

    Without this guard, a future DDL change that adds a column but
    forgets to extend the tuple would have ``_filter_payload`` silently
    drop that column on every upsert — a data-loss class bug that
    would only manifest in production.
    """

    @staticmethod
    def _parse_ddl_columns(ddl: str) -> set[str]:
        """Best-effort column extraction from a CREATE TABLE statement.

        Not a real SQL parser — just enough to pull ``name TYPE`` pairs
        out of the known DDL shape.  Skips the PK column and bookkeeping
        columns (``created_at`` / ``updated_at``).
        """
        import re

        cols: set[str] = set()
        for line in ddl.splitlines():
            line = line.strip().rstrip(",")
            # Skip CREATE, closing `);`, empty lines, and the first column (PK).
            if not line or line.startswith(("CREATE", "/*", "--", ")", "(")):
                continue
            m = re.match(r"([a-z_]+)\s+[A-Z]", line)
            if not m:
                continue
            name = m.group(1)
            if name in {
                # PKs — not in the column tuple. Tenant metadata tables now
                # key on the immutable collection_physical_id; the global
                # catalog metadata tables still key on catalog_id.
                "catalog_id", "collection_id", "collection_physical_id",
                "created_at", "updated_at",     # bookkeeping — not in the tuple
            }:
                continue
            cols.add(name)
        return cols

    def test_collection_core_columns_match_ddl(self):
        from dynastore.modules.catalog.db_init.core_tables import (
            TENANT_METADATA_CORE_DDL,
        )
        from dynastore.modules.storage.drivers.core_postgresql import (
            _COLLECTION_CORE_COLUMNS,
        )

        assert set(_COLLECTION_CORE_COLUMNS) == self._parse_ddl_columns(
            TENANT_METADATA_CORE_DDL,
        )

    def test_catalog_core_columns_match_ddl(self):
        from dynastore.modules.catalog.db_init.core_tables import (
            CATALOG_METADATA_CORE_DDL,
        )
        from dynastore.modules.storage.drivers.core_postgresql import (
            _CATALOG_CORE_COLUMNS,
        )

        assert set(_CATALOG_CORE_COLUMNS) == self._parse_ddl_columns(
            CATALOG_METADATA_CORE_DDL,
        )

class TestEntryPoints:
    def test_core_entry_points_registered(self):
        """Both raw CORE driver entry-points were retired in PR 1e steps
        3b (collection) and 3c (catalog) — the wrappers
        ``CollectionPostgresqlDriver`` (entry-point ``collection_postgresql``)
        and ``CatalogPostgresqlDriver`` (entry-point ``catalog_postgresql``)
        are now the only PG-tier metadata plugins.  Each wrapper composes
        the raw CORE driver internally via its per-tier
        ``*CollectionPgSidecarRegistry``.

        This test now asserts that the wrappers ARE registered.
        """
        from importlib.metadata import entry_points

        # Entry-point ``value`` strings reference the *Python class* (PascalCase
        # — that's the Python convention), not the snake_case wire ``class_key()``.
        expected = {
            "collection_postgresql": "CollectionPostgresqlDriver",
            "catalog_postgresql": "CatalogPostgresqlDriver",
        }

        got = {
            ep.name: ep.value
            for ep in entry_points(group="dynastore.modules")
            if ep.name in expected
        }
        assert set(got) == set(expected), (
            "Missing entry points — run `pip install -e .` to refresh "
            "the installed metadata: "
            f"expected {set(expected) - set(got)}"
        )
        for name, expected_cls in expected.items():
            assert expected_cls in got[name], (
                f"Entry point {name} points at {got[name]!r}, "
                f"expected ...:{expected_cls}"
            )
