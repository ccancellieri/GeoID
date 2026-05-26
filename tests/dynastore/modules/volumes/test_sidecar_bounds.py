import asyncio
import inspect
from unittest.mock import patch

import pytest

from dynastore.modules.volumes.sidecar_bounds import (
    BoundsQuerySpec,
    SidecarBoundsSource,
    build_bounds_query,
    row_to_feature_bounds,
    rows_to_bounds,
)


@pytest.fixture(autouse=True)
def _patch_is_async_resource_for_fake_conns():
    """Make DQLQuery treat fake test connections as async resources.

    DQLQuery dispatches via ``is_async_resource(conn)`` — an isinstance check
    against SQLAlchemy async types. Fake test conns don't pass that check.
    This fixture patches the predicate to also return True for any object
    whose ``execute`` attribute is a coroutine function, so fake conns with
    ``async def execute(...)`` are dispatched via the async code path.
    """
    import dynastore.modules.db_config.query_executor as _qe

    _orig = _qe.is_async_resource

    def _patched(conn):
        if not _orig(conn):
            exec_attr = getattr(conn, "execute", None)
            if exec_attr is not None and (
                asyncio.iscoroutinefunction(exec_attr)
                or inspect.iscoroutinefunction(exec_attr)
            ):
                return True
        return _orig(conn)

    with patch.object(_qe, "is_async_resource", side_effect=_patched):
        yield


@pytest.fixture(autouse=True)
def _clear_get_bounds_cache():
    """Reset the @cached layer on `get_bounds` before each test so cache
    state from one test doesn't leak hits into the next."""
    SidecarBoundsSource.get_bounds.cache_clear()
    yield
    SidecarBoundsSource.get_bounds.cache_clear()


def test_query_emits_schema_qualified_join():
    spec = BoundsQuerySpec(
        schema="tenant1", hub_table="assets", geometries_table="assets_geometries",
    )
    sql = build_bounds_query(spec)
    assert '"tenant1"."assets"' in sql
    assert '"tenant1"."assets_geometries"' in sql
    assert 'ST_XMin(g."geom")' in sql
    assert 'WHERE g."geom" IS NOT NULL' in sql
    # Default feature id is geoid.
    assert 'h."geoid" AS feature_id' in sql
    assert "LIMIT" not in sql  # no limit by default


def test_query_honors_limit_and_custom_ids():
    spec = BoundsQuerySpec(
        schema="s", hub_table="t", geometries_table="t_g",
        feature_id_column="fid", limit=42,
    )
    sql = build_bounds_query(spec)
    assert 'h."fid"' in sql
    assert "LIMIT 42" in sql


def test_query_with_height_column_widens_z_range():
    spec = BoundsQuerySpec(
        schema="s", hub_table="t", geometries_table="t_g",
        height_column="height",
    )
    sql = build_bounds_query(spec)
    # Without height fallback: plain ST_ZMin/ST_ZMax.
    # With height fallback: wrapped in LEAST / GREATEST COALESCE.
    assert "LEAST" in sql and "GREATEST" in sql
    assert 'COALESCE(h."height", 0)' in sql


def test_row_to_feature_bounds_roundtrip():
    fb = row_to_feature_bounds({
        "feature_id": "abc", "min_x": 0, "min_y": 1, "min_z": 2,
        "max_x": 10, "max_y": 11, "max_z": 12,
    })
    assert fb.feature_id == "abc"
    assert (fb.min_x, fb.min_y, fb.min_z) == (0.0, 1.0, 2.0)
    assert (fb.max_x, fb.max_y, fb.max_z) == (10.0, 11.0, 12.0)


def test_rows_to_bounds_skips_null_rows():
    rows = [
        {"feature_id": "a", "min_x": 0, "min_y": 0, "min_z": 0,
         "max_x": 1, "max_y": 1, "max_z": 1},
        {"feature_id": "b", "min_x": None, "min_y": 0, "min_z": 0,
         "max_x": 1, "max_y": 1, "max_z": 1},
        {"feature_id": "c", "min_x": 2, "min_y": 2, "min_z": 2,
         "max_x": 3, "max_y": 3, "max_z": 3},
    ]
    out = rows_to_bounds(rows)
    assert [f.feature_id for f in out] == ["a", "c"]


def test_row_to_feature_bounds_stringifies_numeric_ids():
    fb = row_to_feature_bounds({
        "feature_id": 12345, "min_x": 0, "min_y": 0, "min_z": 0,
        "max_x": 1, "max_y": 1, "max_z": 1,
    })
    assert fb.feature_id == "12345"


# --- SidecarBoundsSource I/O wrapper tests ----------------------------


@pytest.mark.asyncio
async def test_sidecar_bounds_source_is_protocol_compliant():
    from dynastore.models.protocols.bounds_source import BoundsSourceProtocol
    from dynastore.modules.volumes.sidecar_bounds import SidecarBoundsSource

    src = SidecarBoundsSource(
        connection_factory=_fake_connection_factory_returning([]),
        schema_resolver=_make_resolver("s"),
        hub_table_for_collection=_make_table("t"),
        geometries_table_for_collection=_make_table("t_g"),
    )
    assert isinstance(src, BoundsSourceProtocol)


@pytest.mark.asyncio
async def test_sidecar_bounds_source_executes_and_parses():
    from dynastore.modules.volumes.sidecar_bounds import SidecarBoundsSource

    fake_rows = [
        {"feature_id": "a", "min_x": 0, "min_y": 0, "min_z": 0,
         "max_x": 1, "max_y": 1, "max_z": 1},
        {"feature_id": "b", "min_x": 10, "min_y": 10, "min_z": 0,
         "max_x": 11, "max_y": 11, "max_z": 2},
    ]
    executed_sql = []

    src = SidecarBoundsSource(
        connection_factory=_fake_connection_factory_returning(
            fake_rows, sql_sink=executed_sql,
        ),
        schema_resolver=_make_resolver("tenant1"),
        hub_table_for_collection=_make_table("assets"),
        geometries_table_for_collection=_make_table("assets_geometries"),
    )
    bounds = await src.get_bounds("cat1", "col1", limit=100)
    assert [b.feature_id for b in bounds] == ["a", "b"]
    assert len(executed_sql) == 1
    assert '"tenant1"."assets"' in executed_sql[0]
    assert "LIMIT 100" in executed_sql[0]


@pytest.mark.asyncio
async def test_sidecar_bounds_source_rejects_unsafe_identifiers():
    from dynastore.modules.volumes.sidecar_bounds import SidecarBoundsSource

    src = SidecarBoundsSource(
        connection_factory=_fake_connection_factory_returning([]),
        schema_resolver=_make_resolver("s"),
        hub_table_for_collection=_make_table("t"),
        geometries_table_for_collection=_make_table("t_g"),
    )
    with pytest.raises(ValueError):
        await src.get_bounds("cat; DROP TABLE", "col")
    with pytest.raises(ValueError):
        await src.get_bounds("cat", "col; --")


# --- Test helpers ------------------------------------------------------

def _make_resolver(schema: str):
    async def _r(cat_id):
        return schema
    return _r


def _make_table(name: str):
    async def _t(cat_id, col_id):
        return name
    return _t


class _FakeRow:
    """Mimics a SQLAlchemy ``Row`` — supports ``_asdict()`` and dict-like access."""

    def __init__(self, data: dict):
        self._data = data

    def _asdict(self):
        return dict(self._data)

    def __getitem__(self, key):
        return self._data[key]

    def get(self, key, default=None):
        return self._data.get(key, default)


class _FakeResult:
    """Mimics a SQLAlchemy ``Result`` — supports ``.all()`` and ``.fetchall()``."""

    def __init__(self, rows):
        # Wrap plain dicts so _asdict() calls (from ResultHandler.ALL_DICTS) work.
        self._rows = [_FakeRow(r) if isinstance(r, dict) else r for r in rows]

    def all(self):
        return list(self._rows)

    def fetchall(self):
        return list(self._rows)


def _fake_connection_factory_returning(rows, sql_sink=None):
    """Build an async-context-manager connection factory for tests.

    Accepts the test's expected row list + an optional sink list that
    captures each executed SQL string. DQLQuery calls
    ``conn.execute(text_clause, params_dict)``; the fake's ``execute``
    accepts ``(sql_clause, params=None, **kw)`` and returns a
    ``_FakeResult`` that supports ``.all()`` / ``.fetchall()``.
    """
    class _Conn:
        async def execute(self, sql_clause, params=None, **_kw):
            if sql_sink is not None:
                sql_sink.append(str(sql_clause))
            return _FakeResult(rows)

    class _FactoryCM:
        _conn_cls = _Conn  # exposed for autouse patch

        async def __aenter__(self):
            return _Conn()

        async def __aexit__(self, *a):
            return None

    def _call(*args, **kwargs):
        return _FactoryCM()

    _call._conn_cls = _Conn  # expose for autouse patch
    return _call


# --- @cached behaviour ------------------------------------------------


@pytest.mark.asyncio
async def test_get_bounds_caches_repeated_calls():
    """Second call with identical args must hit cache — DB executed once."""
    fake_rows = [
        {"feature_id": "a", "min_x": 0, "min_y": 0, "min_z": 0,
         "max_x": 1, "max_y": 1, "max_z": 1},
    ]
    executed_sql: list = []

    src = SidecarBoundsSource(
        connection_factory=_fake_connection_factory_returning(
            fake_rows, sql_sink=executed_sql,
        ),
        schema_resolver=_make_resolver("tenant_cache_a"),
        hub_table_for_collection=_make_table("assets"),
        geometries_table_for_collection=_make_table("assets_geometries"),
    )

    first = await src.get_bounds("cat_cache_a", "col_cache_a")
    second = await src.get_bounds("cat_cache_a", "col_cache_a")

    assert [b.feature_id for b in first] == ["a"]
    assert [b.feature_id for b in second] == ["a"]
    assert len(executed_sql) == 1, (
        "expected one DB execution, got " + str(len(executed_sql))
    )


@pytest.mark.asyncio
async def test_get_bounds_distinct_keys_dont_collide():
    """Different (catalog_id, collection_id) tuples must be cached separately."""
    fake_rows = [
        {"feature_id": "x", "min_x": 0, "min_y": 0, "min_z": 0,
         "max_x": 1, "max_y": 1, "max_z": 1},
    ]
    executed_sql: list = []

    src = SidecarBoundsSource(
        connection_factory=_fake_connection_factory_returning(
            fake_rows, sql_sink=executed_sql,
        ),
        schema_resolver=_make_resolver("tenant_cache_b"),
        hub_table_for_collection=_make_table("assets"),
        geometries_table_for_collection=_make_table("assets_geometries"),
    )

    await src.get_bounds("cat_cache_b1", "col_cache_b")
    await src.get_bounds("cat_cache_b2", "col_cache_b")
    await src.get_bounds("cat_cache_b1", "col_cache_b_other")

    assert len(executed_sql) == 3


@pytest.mark.asyncio
async def test_get_bounds_distinct_limits_dont_collide():
    """``limit`` is part of the cache key — different limits = different entries."""
    fake_rows = [
        {"feature_id": "x", "min_x": 0, "min_y": 0, "min_z": 0,
         "max_x": 1, "max_y": 1, "max_z": 1},
    ]
    executed_sql: list = []

    src = SidecarBoundsSource(
        connection_factory=_fake_connection_factory_returning(
            fake_rows, sql_sink=executed_sql,
        ),
        schema_resolver=_make_resolver("tenant_cache_c"),
        hub_table_for_collection=_make_table("assets"),
        geometries_table_for_collection=_make_table("assets_geometries"),
    )

    await src.get_bounds("cat_cache_c", "col_cache_c", limit=10)
    await src.get_bounds("cat_cache_c", "col_cache_c", limit=20)
    await src.get_bounds("cat_cache_c", "col_cache_c", limit=10)  # repeat -> hit

    assert len(executed_sql) == 2


@pytest.mark.asyncio
async def test_get_bounds_failed_validation_is_not_cached():
    """Identifier validation raises before the DB call; the failure must
    not cache an entry that would shadow a later valid call with the
    same key."""
    fake_rows = [
        {"feature_id": "y", "min_x": 0, "min_y": 0, "min_z": 0,
         "max_x": 1, "max_y": 1, "max_z": 1},
    ]
    executed_sql: list = []

    src = SidecarBoundsSource(
        connection_factory=_fake_connection_factory_returning(
            fake_rows, sql_sink=executed_sql,
        ),
        schema_resolver=_make_resolver("tenant_cache_d"),
        hub_table_for_collection=_make_table("assets"),
        geometries_table_for_collection=_make_table("assets_geometries"),
    )

    with pytest.raises(ValueError):
        await src.get_bounds("cat_cache_d; DROP", "col_cache_d")

    # A subsequent VALID call with safe identifiers must execute against
    # the DB rather than returning a cached error/sentinel.
    out = await src.get_bounds("cat_cache_d", "col_cache_d")
    assert [b.feature_id for b in out] == ["y"]
    assert len(executed_sql) == 1

