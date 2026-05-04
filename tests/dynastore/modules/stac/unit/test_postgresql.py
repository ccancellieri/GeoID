"""Unit tests for the relocated STAC PG metadata drivers.

Mirror of the STAC-specific subset of
``tests/dynastore/modules/storage/unit/test_metadata_postgresql.py``,
relocated alongside the drivers that moved to ``modules/stac/`` in
PR 1b of the STAC-decoupling sequence.

Covers: structural typing, declared capabilities, ``stac_metadata_columns()``
marker method, sub-Protocol satisfaction (``StacCollectionEntityStoreCapability``
/ ``StacCatalogEntityStoreCapability``), STAC column tuples, payload filter
behaviour, SQL builder smoke tests with a mocked engine, DDL alignment,
and entry-point discovery at the new location.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from dynastore.extensions.stac.protocols import (
    StacCatalogEntityStoreCapability,
    StacCollectionEntityStoreCapability,
)
from dynastore.models.protocols.entity_store import (
    CatalogStore,
    CollectionStore,
    EntityStoreCapability,
)


class TestStructuralInvariants:
    def test_collection_stac_driver_identifies_as_stac_collection(self):
        from dynastore.modules.stac.drivers.postgresql import (
            CollectionStacPostgresqlDriver,
        )

        d = CollectionStacPostgresqlDriver()
        assert d._table == "collection_metadata_stac"
        assert EntityStoreCapability.SPATIAL_FILTER in d.capabilities
        # STAC collection driver does NOT advertise SEARCH — the CORE
        # driver owns title/description; STAC carries structured fields.
        assert EntityStoreCapability.SEARCH not in d.capabilities
        # Structural typing — base Protocol + STAC sub-Protocol.
        assert isinstance(d, CollectionStore)
        assert isinstance(d, StacCollectionEntityStoreCapability)

    def test_catalog_stac_driver_identifies_as_stac_catalog(self):
        from dynastore.modules.stac.drivers.postgresql import (
            CatalogStacPostgresqlDriver,
        )

        d = CatalogStacPostgresqlDriver()
        assert d._table == "catalog_metadata_stac"
        assert isinstance(d, CatalogStore)
        assert isinstance(d, StacCatalogEntityStoreCapability)

    def test_stac_metadata_columns_marker_returns_columns(self):
        """The ``stac_metadata_columns()`` marker method on the sub-Protocol
        is what makes ``isinstance(d, StacCollectionEntityStoreCapability)``
        discriminate STAC drivers from CORE drivers at runtime.
        """
        from dynastore.modules.stac.drivers.postgresql import (
            CatalogStacPostgresqlDriver,
            CollectionStacPostgresqlDriver,
            _CATALOG_STAC_COLUMNS,
            _COLLECTION_STAC_COLUMNS,
        )

        assert (
            CollectionStacPostgresqlDriver().stac_metadata_columns()
            == _COLLECTION_STAC_COLUMNS
        )
        assert (
            CatalogStacPostgresqlDriver().stac_metadata_columns()
            == _CATALOG_STAC_COLUMNS
        )

    def test_core_driver_does_not_satisfy_stac_capability(self):
        """``isinstance(core_driver, StacCollectionEntityStoreCapability)`` must
        be False — the marker method is the discriminator.
        """
        from dynastore.modules.storage.drivers.core_postgresql import (
            CatalogCorePostgresqlDriver,
            CollectionCorePostgresqlDriver,
        )

        assert not isinstance(
            CollectionCorePostgresqlDriver(), StacCollectionEntityStoreCapability
        )
        assert not isinstance(
            CatalogCorePostgresqlDriver(), StacCatalogEntityStoreCapability
        )


class TestDomainColumnSets:
    def test_collection_stac_columns(self):
        from dynastore.modules.stac.drivers.postgresql import (
            _COLLECTION_STAC_COLUMNS,
        )

        assert _COLLECTION_STAC_COLUMNS == (
            "stac_version", "stac_extensions", "extent", "providers",
            "summaries", "links", "assets", "item_assets",
        )

    def test_catalog_stac_columns(self):
        from dynastore.modules.stac.drivers.postgresql import (
            _CATALOG_STAC_COLUMNS,
        )

        assert _CATALOG_STAC_COLUMNS == (
            "stac_version", "stac_extensions", "conforms_to", "links", "assets",
        )


@pytest.fixture
def fake_conn_with_dql():
    """Patch DQLQuery + transaction + engine + schema resolver in the
    *core* PG metadata module (where ``_CollectionMetadataDomainBase`` /
    ``_CatalogMetadataDomainBase`` live — STAC drivers inherit the CRUD
    bodies from those bases, so the patches must target their module).
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
    ]
    for p in patches:
        p.start()
    try:
        yield fake_engine, dql_execute
    finally:
        for p in patches:
            p.stop()


@pytest.mark.asyncio
async def test_collection_stac_upsert_uses_stac_columns_only(fake_conn_with_dql):
    """Full STAC payload → INSERT columns match the supplied STAC keys."""
    from dynastore.modules.stac.drivers.postgresql import (
        CollectionStacPostgresqlDriver,
    )

    _, dql_execute = fake_conn_with_dql
    d = CollectionStacPostgresqlDriver()
    await d.upsert_metadata(
        "cat", "col",
        metadata={
            "extent": {"spatial": {"bbox": [[0, 0, 1, 1]]}},
            "providers": [{"name": "FAO"}],
            "summaries": {"gsd": [10]},
            "links": [],
            "assets": {},
            "item_assets": {},
            "stac_version": "1.1.0",
            "stac_extensions": ["https://…/datacube/v2"],
            "title": "SHOULD BE IGNORED — CORE COLUMN",
            "license": "IGNORE",
        },
    )

    dql_execute.assert_awaited_once()
    sql, params = dql_execute.call_args.args[0], dql_execute.call_args.kwargs
    assert '"t_alpha".collection_metadata_stac' in sql
    assert "title" not in sql
    assert "license" not in sql
    assert "extent" in sql
    assert "stac_version" in sql
    assert set(params) == {
        "id", "stac_version", "stac_extensions", "extent", "providers",
        "summaries", "links", "assets", "item_assets",
    }


@pytest.mark.asyncio
async def test_catalog_stac_upsert_carries_catalog_stac_columns(fake_conn_with_dql):
    """Catalog STAC domain carries conforms_to but not item_assets.

    Partial-update semantics: only supplied keys appear in the SQL.
    """
    from dynastore.modules.stac.drivers.postgresql import (
        CatalogStacPostgresqlDriver,
    )

    _, dql_execute = fake_conn_with_dql
    d = CatalogStacPostgresqlDriver()
    await d.upsert_catalog_metadata(
        "cat",
        metadata={
            "stac_version": "1.1.0",
            "conforms_to": ["http://…/core"],
            "item_assets": "SHOULD BE IGNORED — catalog STAC has no item_assets",
        },
    )
    dql_execute.assert_awaited_once()
    sql, params = dql_execute.call_args.args[0], dql_execute.call_args.kwargs
    assert "catalog.catalog_metadata_stac" in sql
    assert "conforms_to" in sql
    assert "item_assets" not in sql
    for untouched in ("stac_extensions", "links", "assets"):
        assert untouched not in sql, (
            f"SQL references {untouched!r} on partial update — would "
            f"NULL-out existing column value"
        )
    assert set(params) == {"id", "stac_version", "conforms_to"}


class TestColumnTupleAlignment:
    """Guard against drift between the column tuples and the DDL."""

    @staticmethod
    def _parse_ddl_columns(ddl: str) -> set[str]:
        import re

        cols: set[str] = set()
        for line in ddl.splitlines():
            line = line.strip().rstrip(",")
            if not line or line.startswith(("CREATE", "/*", "--", ")", "(")):
                continue
            m = re.match(r"([a-z_]+)\s+[A-Z]", line)
            if not m:
                continue
            name = m.group(1)
            if name in {
                "catalog_id", "collection_id",
                "created_at", "updated_at",
            }:
                continue
            cols.add(name)
        return cols

    def test_collection_stac_columns_match_ddl(self):
        from dynastore.modules.stac.db_init.metadata_stac_tables import (
            TENANT_METADATA_STAC_DDL,
        )
        from dynastore.modules.stac.drivers.postgresql import (
            _COLLECTION_STAC_COLUMNS,
        )

        assert set(_COLLECTION_STAC_COLUMNS) == self._parse_ddl_columns(
            TENANT_METADATA_STAC_DDL,
        )

    def test_catalog_stac_columns_match_ddl(self):
        from dynastore.modules.stac.db_init.metadata_stac_tables import (
            CATALOG_METADATA_STAC_DDL,
        )
        from dynastore.modules.stac.drivers.postgresql import (
            _CATALOG_STAC_COLUMNS,
        )

        assert set(_CATALOG_STAC_COLUMNS) == self._parse_ddl_columns(
            CATALOG_METADATA_STAC_DDL,
        )


class TestEntryPoints:
    def test_stac_driver_entry_points_at_new_location(self):
        from importlib.metadata import entry_points

        expected = {
            "stac": "dynastore.modules.stac.module:StacModule",
        }
        # NOTE: BOTH ``collection_postgresql`` (retired in
        # PR 1e step 3b) and ``catalog_postgresql`` (retired
        # in PR 1e step 3c) are gone.  The STAC slice is now composed
        # inside the per-tier wrapper drivers
        # (``CollectionPostgresqlDriver`` / ``CatalogPostgresqlDriver``)
        # via their sidecar registries' try-imports.  StacModule itself
        # remains entry-point-registered as the lifespan owner of the
        # global ``catalog.catalog_metadata_stac`` DDL.
        got = {
            ep.name: ep.value
            for ep in entry_points(group="dynastore.modules")
            if ep.name in expected
        }
        assert got == expected, (
            "STAC driver / module entry-points missing or stale — "
            "run `pip install -e .` to refresh the installed metadata: "
            f"expected {expected}, got {got}"
        )
