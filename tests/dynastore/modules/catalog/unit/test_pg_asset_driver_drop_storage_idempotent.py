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

"""Regression: ``AssetPostgresqlDriver.drop_storage`` is idempotent when the
catalog is already gone.

The routing-driven hard-delete cascade runs ``drop_storage`` AFTER the catalog
row and its per-tenant schema (which owns the ``assets`` table) have already
been dropped. ``resolve_physical_schema`` raises ``ValueError("Catalog '…' not
found.")`` for an absent catalog unless ``allow_missing=True``. Before this fix
the asset driver resolved the schema with the default (strict) flag, so
``drop_storage`` raised, the cascade owner returned RETRY, and after exhausting
the per-ref retries the ref was marked permanently DEAD — failing the whole
cascade_cleanup task even though there was nothing left to clean. Every sibling
routing-driven driver already tolerates the deleted catalog; these tests pin the
asset driver to the same idempotent contract while keeping the strict default
for the write/read hot path.
"""
from __future__ import annotations

from typing import Any, Optional

import pytest

import dynastore.tools.discovery as discovery
from dynastore.modules.catalog.drivers import pg_asset_driver as mod


class _FakeCatalogs:
    """Stub CatalogsProtocol mirroring ``resolve_physical_schema`` semantics.

    ``schema=None`` models an absent (already-deleted) catalog: strict
    resolution raises, ``allow_missing=True`` returns ``None``.

    ``resolve_physical_id`` returns a deterministic ``c_{collection_id}``
    token so tests can assert the derived partition name without a live DB.
    """

    def __init__(self, schema: Optional[str]):
        self._schema = schema
        self.calls: list[dict[str, Any]] = []

    async def resolve_physical_schema(
        self,
        catalog_id: str,
        ctx: Any = None,
        allow_missing: bool = False,
    ) -> Optional[str]:
        self.calls.append({"catalog_id": catalog_id, "allow_missing": allow_missing})
        if self._schema is None:
            if not allow_missing:
                raise ValueError(f"Catalog '{catalog_id}' not found.")
            return None
        return self._schema

    async def resolve_physical_id(
        self,
        catalog_id: str,
        collection_id: Optional[str],
        ctx: Any = None,
        allow_missing: bool = False,
    ) -> Optional[str]:
        """Return a deterministic physical id token for the given collection."""
        if collection_id is None:
            return None
        return f"c_{collection_id}"


class _FakeTxCM:
    def __init__(self, conn: Any):
        self._conn = conn

    async def __aenter__(self) -> Any:
        return self._conn

    async def __aexit__(self, *exc: Any) -> bool:
        return False


@pytest.fixture
def patched(monkeypatch):
    """Record DB-touching helpers so we can assert they are NOT invoked when
    the catalog is gone, and capture them when it is present."""
    state: dict[str, Any] = {"deletes": [], "drops": []}

    class _FakeDQLQuery:
        def __init__(self, sql: str, **_: Any):
            self.sql = sql

        async def execute(self, conn: Any, **kwargs: Any) -> int:
            state["deletes"].append({"sql": self.sql, "params": kwargs})
            return 0

    async def _fake_safe_drop(conn: Any, schema: str, name: str, **kw: Any) -> None:
        state["drops"].append({"schema": schema, "name": name, "kind": kw.get("kind")})

    monkeypatch.setattr(mod, "managed_transaction", lambda engine: _FakeTxCM(object()), raising=True)
    monkeypatch.setattr(mod, "DQLQuery", _FakeDQLQuery, raising=True)
    monkeypatch.setattr(mod, "safe_drop_relation", _fake_safe_drop, raising=True)
    return state


def _driver_with_catalogs(monkeypatch, schema: Optional[str]) -> tuple[Any, _FakeCatalogs]:
    drv = mod.AssetPostgresqlDriver(engine=object())  # type: ignore[arg-type]
    fake = _FakeCatalogs(schema)
    monkeypatch.setattr(discovery, "get_protocol", lambda _proto: fake, raising=True)
    # DriverContext is a Pydantic model that validates ``db_resource`` against
    # real SQLAlchemy types; the stub catalogs ignore ``ctx`` entirely, so a
    # trivial passthrough keeps the test free of a live engine.
    monkeypatch.setattr(mod, "DriverContext", lambda **_kw: object(), raising=True)
    return drv, fake


# ---------------------------------------------------------------------------
# The bug: absent catalog must be a no-op, never an exception.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_drop_storage_absent_catalog_catalog_scope_is_noop(monkeypatch, patched):
    drv, fake = _driver_with_catalogs(monkeypatch, schema=None)
    # Catalog-scope drop (collection_id=None) on a deleted catalog: no raise.
    await drv.drop_storage("gone-cat", None)
    # Resolved with allow_missing=True and short-circuited — no DELETE issued.
    assert fake.calls and fake.calls[-1]["allow_missing"] is True
    assert patched["deletes"] == []


@pytest.mark.asyncio
async def test_drop_storage_absent_catalog_collection_scope_is_noop(monkeypatch, patched):
    drv, _ = _driver_with_catalogs(monkeypatch, schema=None)
    # Collection-scope drop on a deleted catalog: no raise, no partition DROP.
    await drv.drop_storage("gone-cat", "some-col")
    assert patched["drops"] == []


# ---------------------------------------------------------------------------
# _resolve_schema flag semantics: idempotent for cleanup, strict by default.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_resolve_schema_allow_missing_returns_none(monkeypatch):
    drv, _ = _driver_with_catalogs(monkeypatch, schema=None)
    assert await drv._resolve_schema("gone-cat", allow_missing=True) is None


@pytest.mark.asyncio
async def test_resolve_schema_default_is_strict(monkeypatch):
    # Hot-path callers (index/get/list) must still surface a missing catalog.
    drv, _ = _driver_with_catalogs(monkeypatch, schema=None)
    with pytest.raises(ValueError, match="not found"):
        await drv._resolve_schema("gone-cat")


# ---------------------------------------------------------------------------
# Happy path unchanged: a present catalog still drops the partition.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_drop_storage_present_catalog_drops_partition(monkeypatch, patched):
    drv, _ = _driver_with_catalogs(monkeypatch, schema="s_live")
    await drv.drop_storage("live-cat", "col-1")
    # Partition is named after the collection's physical id (c_… token), not
    # the logical collection_id. _FakeCatalogs.resolve_physical_id returns
    # "c_{collection_id}" so the expected partition name is "assets_p_c_col-1".
    assert patched["drops"] == [
        {"schema": "s_live", "name": "assets_p_c_col-1", "kind": "table"}
    ]


@pytest.mark.asyncio
async def test_drop_storage_present_catalog_catalog_scope_deletes_refs(monkeypatch, patched):
    drv, _ = _driver_with_catalogs(monkeypatch, schema="s_live")
    await drv.drop_storage("live-cat", None)
    assert len(patched["deletes"]) == 1
    assert "asset_references" in patched["deletes"][0]["sql"]
    # Schema scopes the catalog; catalog_id is not a WHERE predicate here —
    # the schema already isolates the tenant's rows.
    assert "catalog_id" not in patched["deletes"][0]["params"]
