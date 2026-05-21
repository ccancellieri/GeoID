"""Regression pin (#792): collection/catalog ``is_materialized`` dispatch
must call ``CatalogsProtocol.resolve_physical_schema`` directly — NOT via
a non-existent ``.catalog_manager`` indirection.

The bug shipped in #738/PR #746 because the broad ``except Exception`` in
``is_materialized`` swallows ``AttributeError`` and demotes it to a DEBUG
log, then returns ``False`` (fail-open).  Operators only saw a noisy
``DEBUG`` traceback on every ``PUT /configs/.../plugins/{plugin_id}``;
the gate silently treated every resource as not-materialized, neutering
the Immutable-field enforcement the 3-phase lifecycle was supposed to
add.

The dispatch must therefore be exercised against a strict-spec mock so
that an ``AttributeError`` would fail the test loudly instead of being
swallowed by the same except clause that hid the original bug.

#796 follow-up: the broad ``except Exception`` has been narrowed to a
``_EXPECTED_ABSENT_LAYER`` allow-list (``OSError``, ``KeyError``,
``ValidationError``, ``asyncpg.UndefinedTableError`` and siblings) and
the fail-open log promoted to WARNING. The bottom of this file pins
both halves of the new contract: legitimate absent-layer errors are
still swallowed and surfaced as a structured WARNING, but unexpected
exception types (``AttributeError``, ``TypeError``, ``ImportError``,
``NameError``) now propagate so the next #792-class regression cannot
hide.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import asyncpg
import pytest
from sqlalchemy.ext.asyncio import AsyncConnection

from dynastore.models.protocols import CatalogsProtocol
from dynastore.modules.db_config import platform_config_service as svc


class _FakeCollectionConfig:
    _freeze_at = "collection"


class _FakeCatalogConfig:
    _freeze_at = "catalog"


def _fake_conn() -> MagicMock:
    """An AsyncConnection-shaped mock that satisfies ``DriverContext``'s
    Pydantic union validator (``is_instance_of[AsyncConnection]``)."""
    return MagicMock(spec=AsyncConnection)


@pytest.mark.asyncio
async def test_collection_dispatch_calls_resolve_physical_schema_directly():
    """Bug: ``catalogs.catalog_manager.resolve_physical_schema(...)`` —
    `CatalogService` has no ``catalog_manager`` attr.  Fix: call
    ``catalogs.resolve_physical_schema(...)`` directly.
    """
    catalogs = MagicMock(spec=CatalogsProtocol)
    catalogs.resolve_physical_schema = AsyncMock(return_value=None)

    with patch("dynastore.tools.discovery.get_protocol", return_value=catalogs):
        result = await svc.is_materialized(
            _FakeCollectionConfig, "cat1", "col1", conn=_fake_conn()
        )

    assert result is False  # phys_schema=None → not materialized
    catalogs.resolve_physical_schema.assert_awaited_once()
    # The bug — `.catalog_manager` indirection — would have surfaced as
    # AttributeError on the spec'd mock and been swallowed by the broad
    # ``except Exception`` in ``is_materialized``.  Asserting the await
    # happened proves the dispatch reached the right method.


@pytest.mark.asyncio
async def test_catalog_dispatch_calls_resolve_physical_schema_directly():
    """Same regression, catalog-tier sibling at line 237."""
    catalogs = MagicMock(spec=CatalogsProtocol)
    catalogs.resolve_physical_schema = AsyncMock(return_value=None)

    with patch("dynastore.tools.discovery.get_protocol", return_value=catalogs):
        result = await svc.is_materialized(
            _FakeCatalogConfig, "cat1", None, conn=_fake_conn()
        )

    assert result is False
    catalogs.resolve_physical_schema.assert_awaited_once()


@pytest.mark.asyncio
async def test_no_catalog_manager_attribute_access():
    """Belt-and-braces: source-level grep — neither dispatch helper may
    reference ``.catalog_manager`` (the historical bug shape)."""
    import inspect

    for fn in (svc._collection_is_materialized, svc._catalog_is_materialized):
        source = inspect.getsource(fn)
        assert ".catalog_manager" not in source, (
            f"{fn.__qualname__} reintroduced the #792 `.catalog_manager` "
            f"indirection — call `catalogs.resolve_physical_schema(...)` "
            f"directly instead."
        )


# ---------------------------------------------------------------------------
# #796 — narrowed fail-open contract on is_materialized
# ---------------------------------------------------------------------------


class _BoomDispatchConfig:
    """Config class whose freeze tier forces the platform-tier dispatch.

    Pairs with ``patch.object(svc, "_platform_is_materialized", side_effect=…)``
    to inject any exception class through the real except-clause without
    needing a live DB.
    """

    _freeze_at = "platform"


@pytest.mark.parametrize(
    "exc",
    [
        OSError("disk gone"),
        KeyError("missing protocol"),
        asyncpg.UndefinedTableError("relation missing"),
        asyncpg.UndefinedColumnError("column missing"),
        asyncpg.InvalidSchemaNameError("schema missing"),
    ],
    ids=["OSError", "KeyError", "UndefinedTable", "UndefinedColumn", "InvalidSchema"],
)
@pytest.mark.asyncio
async def test_expected_absent_layer_errors_still_fail_open(exc, caplog):
    """The narrowed allow-list must still swallow legitimate absent-layer
    errors so pre-materialization edits keep working — losing fail-open
    on these would freeze every config-update against a half-provisioned
    catalog."""
    import logging
    caplog.set_level(logging.WARNING, logger=svc.__name__)
    with patch.object(svc, "_platform_is_materialized", side_effect=exc):
        result = await svc.is_materialized(
            _BoomDispatchConfig, None, None, conn=_fake_conn()
        )
    assert result is False
    assert any(
        "is_materialized_fail_open" in r.message
        and "site=dispatch" in r.message
        and r.levelname == "WARNING"
        for r in caplog.records
    ), (
        "Expected a structured WARNING on legitimate absent-layer fail-open; "
        f"got: {[(r.levelname, r.message) for r in caplog.records]}"
    )


@pytest.mark.parametrize(
    "exc",
    [
        AttributeError("typo'd attribute"),
        TypeError("wrong argument type"),
        ImportError("module not found"),
        NameError("undefined name"),
    ],
    ids=["AttributeError", "TypeError", "ImportError", "NameError"],
)
@pytest.mark.asyncio
async def test_unexpected_exception_types_propagate(exc):
    """The #792 / #796 contract: code bugs surfacing as Attribute/Type/
    Import/Name errors must propagate to the caller — not be silently
    demoted to ``False`` like the old broad ``except Exception``."""
    with patch.object(svc, "_platform_is_materialized", side_effect=exc):
        with pytest.raises(type(exc)):
            await svc.is_materialized(
                _BoomDispatchConfig, None, None, conn=_fake_conn()
            )


@pytest.mark.parametrize(
    "exc",
    [
        AttributeError("typo'd attribute"),
        TypeError("wrong argument type"),
    ],
    ids=["AttributeError", "TypeError"],
)
@pytest.mark.asyncio
async def test_override_unexpected_exception_propagates(exc):
    """Same contract on the ``_materialization_check`` override branch:
    unexpected exceptions from a class-supplied check propagate, only
    the absent-layer allow-list is swallowed.
    """

    class _WithOverride:
        @classmethod
        def _materialization_check(cls, cat, col, conn):
            raise exc

    with pytest.raises(type(exc)):
        await svc.is_materialized(_WithOverride, "c", "x", conn=_fake_conn())


@pytest.mark.asyncio
async def test_override_absent_layer_error_is_warned(caplog):
    """Override branch fail-open log emits at WARNING level with the
    ``is_materialized_fail_open`` token + ``site=override`` so SREs can
    distinguish dispatch vs override skips in log-based metrics."""
    import logging
    caplog.set_level(logging.WARNING, logger=svc.__name__)

    class _WithOverride:
        @classmethod
        def _materialization_check(cls, cat, col, conn):
            raise asyncpg.UndefinedTableError("relation missing")

    result = await svc.is_materialized(
        _WithOverride, "c", "x", conn=_fake_conn()
    )
    assert result is False
    assert any(
        "is_materialized_fail_open" in r.message
        and "site=override" in r.message
        and r.levelname == "WARNING"
        for r in caplog.records
    ), (
        "Expected a structured WARNING on override fail-open; "
        f"got: {[(r.levelname, r.message) for r in caplog.records]}"
    )


def test_no_bare_except_exception_in_is_materialized():
    """Belt-and-braces source check: the broad ``except Exception`` that
    hid #792 must not creep back into ``is_materialized``. Any future
    refactor that re-broadens the catch trips this assertion before it
    ships.
    """
    import inspect

    source = inspect.getsource(svc.is_materialized)
    assert "except Exception" not in source, (
        "is_materialized must not catch bare Exception — narrow the catch "
        "via _EXPECTED_ABSENT_LAYER so code bugs propagate (see #796)."
    )


# ---------------------------------------------------------------------------
# #917 — _collection/_catalog/_platform_is_materialized must route through
# the DQLQuery/DDLQuery polymorphic dispatcher, never asyncpg-only conn API
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "fn_name",
    ["_collection_is_materialized", "_catalog_is_materialized", "_platform_is_materialized"],
)
def test_no_raw_asyncpg_conn_calls_in_is_materialized_helpers(fn_name):
    """Issue #917: the three ``_*_is_materialized`` helpers used to call
    ``await conn.fetchrow(...)`` directly. ``fetchrow`` / ``fetch`` /
    ``fetchval`` are asyncpg-only — the HTTP service path passes a
    SQLAlchemy ``AsyncConnection`` and sync tasks pass a SQLAlchemy
    ``SAConnection``; both lack those attributes, so any direct call
    500s with ``AttributeError`` on the very first immutable-config
    update against a materialised resource.

    The fix routes every PG query through ``DQLQuery(...).execute(conn)``
    (the same abstraction ``check_table_exists`` uses one line above),
    which dispatches on conn type and works for both runtimes.
    """
    import inspect

    fn = getattr(svc, fn_name)
    source = inspect.getsource(fn)
    for forbidden in ("conn.fetchrow", "conn.fetch(", "conn.fetchval"):
        assert forbidden not in source, (
            f"{fn.__qualname__} contains `{forbidden}` — asyncpg-only API. "
            f"Route through DQLQuery/DDLQuery so both async services "
            f"(AsyncConnection) and sync tasks (SAConnection) work. "
            f"See issue #917."
        )


@pytest.mark.asyncio
async def test_platform_is_materialized_does_not_touch_fetchrow_attr():
    """End-to-end exec proof: drive ``_platform_is_materialized`` against
    a strict ``AsyncConnection``-spec mock (which raises AttributeError
    on ``.fetchrow``) and assert it returns cleanly. The pre-#917 code
    would AttributeError here; the DQLQuery rewrite goes through the
    SQLAlchemy ``conn.execute()`` path."""
    from sqlalchemy.engine.cursor import CursorResult

    fake_conn = MagicMock(spec=AsyncConnection)

    async def _fake_execute(*_a, **_kw):
        cursor = MagicMock(spec=CursorResult)
        cursor.scalar.return_value = 1
        return cursor

    fake_conn.execute = AsyncMock(side_effect=_fake_execute)
    # check_table_exists must say "table exists" so we reach the SELECT-1.
    # Patched at the import source — _platform_is_materialized does a
    # lazy ``from .locking_tools import check_table_exists`` inside its body.
    with patch(
        "dynastore.modules.db_config.locking_tools.check_table_exists",
        new=AsyncMock(return_value=True),
    ):
        result = await svc._platform_is_materialized(fake_conn)
    assert result is True
    assert not hasattr(fake_conn, "fetchrow") or not fake_conn.fetchrow.called, (
        "_platform_is_materialized must not call asyncpg-only conn.fetchrow"
    )
