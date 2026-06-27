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

import inspect
import re
import logging
import asyncio
import functools
import random
import weakref
import contextvars
import hashlib
import os
import time
from abc import abstractmethod, ABC
from contextlib import asynccontextmanager, contextmanager

from dynastore.modules.db_config.connection_health_config import (
    resolve_connection_retry_config,
    resolve_provisioning_retry_config,
    resolve_slow_pool_acquire_threshold,
)
from sqlalchemy import text, DDL
from sqlalchemy.engine import Engine
from sqlalchemy.engine.base import Connection as SAConnection
from sqlalchemy.engine.result import Result

# SASession is renamed here to avoid confusion; sessionmaker usually returns it
from sqlalchemy.orm import Session as SASession
from sqlalchemy.sql.elements import TextClause
from sqlalchemy.ext.asyncio import (
    AsyncConnection,
    AsyncSession,
    AsyncEngine,
    AsyncTransaction,
)
from sqlalchemy.exc import (
    InterfaceError,
    OperationalError,
    PendingRollbackError,
    InvalidRequestError,
)
from geoalchemy2.shape import to_shape
from geoalchemy2.elements import WKBElement, WKTElement
from sqlalchemy import Table, MetaData
from typing import (
    Iterator,
    Union,
    List,
    Callable,
    Any,
    Awaitable,
    Tuple,
    TypeAlias,
    TypeVar,
    ParamSpec,
    Optional,
    cast,
    Type,
    Dict,
    TypeGuard,
)
from pydantic import BaseModel
from .exceptions import (
    QueryExecutionError,
    PGCODE_EXCEPTION_MAP,
    DatabaseConnectionError,
)

# InternalClientError carries asyncpg's "cannot switch to state N" — a wire
# returned to the pool while still mid-operation. Both this and
# ConnectionDoesNotExistError must be retryable at pool-checkout so the
# decorator can invalidate the poisoned wire and acquire a fresh one.
try:
    from asyncpg.exceptions import (
        ConnectionDoesNotExistError as AsyncpgConnectionDoesNotExistError,
        InternalClientError as AsyncpgInternalClientError,
        InterfaceError as AsyncpgInterfaceError,
    )
except ImportError:
    AsyncpgConnectionDoesNotExistError = type("AsyncpgConnectionDoesNotExistError", (Exception,), {})
    AsyncpgInternalClientError = type("AsyncpgInternalClientError", (Exception,), {})
    AsyncpgInterfaceError = type("AsyncpgInterfaceError", (Exception,), {})


# Class names of asyncpg client-side errors that indicate transient connection
# / state-machine issues (no pgcode — they predate the query reaching PG).
# Used by ``_handle_db_exception`` to surface them as
# ``DatabaseConnectionError`` so callers (e.g. ``tasks/dispatcher.py``) can
# apply transient back-off + retry instead of treating them as hard failures.
_TRANSIENT_ASYNCPG_ERROR_CLASS_NAMES = frozenset({
    "ConnectionDoesNotExistError",       # connection closed mid-operation
    "InternalClientError",               # "cannot switch to state N" — concurrent use
    "ConnectionFailureError",
    "InterfaceError",                    # asyncpg base client error
})

_TRANSIENT_ASYNCPG_MESSAGE_FRAGMENTS = (
    "cannot switch to state",            # concurrent use of one connection
    "another operation",                 # ditto, alternative phrasing
    "connection was closed",             # mid-operation disconnect
)


def _is_transient_asyncpg_error(exc: Optional[BaseException]) -> bool:
    """Return True if ``exc`` is an asyncpg transient error safe to retry.

    Covers:
    - ``InterfaceError`` (connection factory not ready during DB warm-up)
    - ``ConnectionDoesNotExistError`` (server terminated the connection)
    - ``InternalClientError`` (internal asyncpg state machine error)
    - Any exception with message containing known transient fragments.

    Used by :func:`retry_on_transient_connect` to decide whether to retry
    a failed connection acquisition, and by the cancel-drain path in
    :func:`managed_transaction` to decide whether to invalidate a dead wire.
    """
    if exc is None:
        return False
    # asyncpg raises its own exception hierarchy for transient failures.
    # SQLAlchemy wraps these inside DBAPIError with .orig set to the asyncpg exc.
    if isinstance(
        exc,
        (
            AsyncpgInterfaceError,
            AsyncpgConnectionDoesNotExistError,
            AsyncpgInternalClientError,
        ),
    ):
        return True
    # Check wrapped exceptions (SQLAlchemy DBAPIError.orig)
    if isinstance(
        getattr(exc, "orig", None),
        (
            AsyncpgInterfaceError,
            AsyncpgConnectionDoesNotExistError,
            AsyncpgInternalClientError,
        ),
    ):
        return True
    # Fallback: message matching for cases where the exception class is not
    # directly importable or is wrapped in an unexpected way.
    msg = str(exc)
    return any(fragment in msg for fragment in _TRANSIENT_ASYNCPG_MESSAGE_FRAGMENTS)


def _is_autocommit_connection(conn: Any) -> bool:
    """Detect if connection has AUTOCOMMIT isolation level.

    AUTOCOMMIT connections (used by pg_advisory_leadership for advisory locks)
    have no active PostgreSQL transaction, so attempting begin_nested()
    (SAVEPOINT) raises NoActiveSQLTransactionError.

    This function checks SQLAlchemy's execution_options for isolation_level.
    Works for both async (AsyncConnection) and sync (Connection) paths.

    Args:
        conn: SQLAlchemy connection (async or sync)

    Returns:
        True if connection is in AUTOCOMMIT mode, False otherwise.
    """
    # Check async connection's execution_options
    exec_opts = getattr(conn, "_execution_options", None)
    if exec_opts and exec_opts.get("isolation_level") == "AUTOCOMMIT":
        return True

    # For sync connections, check the underlying connection
    sync_conn = getattr(conn, "sync_connection", None) or getattr(conn, "connection", None)
    if sync_conn:
        exec_opts = getattr(sync_conn, "_execution_options", None)
        if exec_opts and exec_opts.get("isolation_level") == "AUTOCOMMIT":
            return True

    return False


# --- Lock-not-available detection (pgcode 55P03) ----------------------------
# Covers both the async asyncpg path (LockNotAvailableError class) and the
# sync psycopg2 path (OperationalError wrapping pgcode 55P03 in its .orig).
# Used by ``is_transient_db_error`` for the provisioning retry wrapper.

_LOCK_NOT_AVAILABLE_PGCODE = "55P03"
_LOCK_NOT_AVAILABLE_CLASS_NAMES = frozenset({"LockNotAvailableError"})
_LOCK_TIMEOUT_MESSAGE_FRAGMENTS = (
    "canceling statement due to lock timeout",
    "lock timeout",
)


def _is_lock_not_available_error(exc: Optional[BaseException]) -> bool:
    """Return True if ``exc`` is a PG lock-not-available / lock-timeout error (55P03).

    Walks the ``.orig`` / ``__cause__`` chain so asyncpg errors wrapped inside
    SQLAlchemy ``DBAPIError`` are still detected.
    """
    if exc is None:
        return False
    if type(exc).__name__ in _LOCK_NOT_AVAILABLE_CLASS_NAMES:
        return True
    seen: set[int] = set()
    candidate: Optional[BaseException] = exc
    while candidate is not None and id(candidate) not in seen:
        seen.add(id(candidate))
        pgcode = getattr(candidate, "pgcode", None) or getattr(candidate, "sqlstate", None)
        if pgcode == _LOCK_NOT_AVAILABLE_PGCODE:
            return True
        candidate = getattr(candidate, "orig", None) or getattr(candidate, "__cause__", None)
    msg = str(exc)
    return any(fragment in msg for fragment in _LOCK_TIMEOUT_MESSAGE_FRAGMENTS)


# --- Sync (psycopg2 / SQLAlchemy) closed-connection detection ----------------

_SYNC_CLOSED_CONN_MESSAGE_FRAGMENTS = (
    "server closed the connection",
    "connection already closed",
)


def _is_sync_closed_connection_error(exc: Optional[BaseException]) -> bool:
    """Return True for sync SQLAlchemy/psycopg2 connection-closed errors.

    Matches only ``OperationalError`` with ``connection_invalidated=True`` (the
    flag SQLAlchemy's pool sets on a detected disconnect) or one of the known
    server-closed message fragments from psycopg2.  Generic ``OperationalError``
    without these markers is NOT matched — it must surface as a real bug.
    """
    if exc is None:
        return False
    if isinstance(exc, OperationalError):
        if getattr(exc, "connection_invalidated", False):
            return True
        msg = str(exc)
        return any(f in msg for f in _SYNC_CLOSED_CONN_MESSAGE_FRAGMENTS)
    return False


def is_transient_db_error(exc: Optional[BaseException]) -> bool:
    """Return True if ``exc`` is a transient DB error safe to retry on a fresh connection.

    Covers:
    - Async asyncpg: ``InterfaceError``, ``ConnectionDoesNotExistError``,
      ``InternalClientError``, "connection was closed" (via ``_is_transient_asyncpg_error``).
    - Async asyncpg: ``LockNotAvailableError`` (pgcode 55P03 / lock timeout).
    - Sync SQLAlchemy/psycopg2: ``OperationalError.connection_invalidated``,
      "server closed the connection" / "connection already closed".
    - Sync: lock timeout via pgcode 55P03 in the ``.orig`` chain.

    Conservative by design: does NOT match a bare ``OperationalError`` without
    the above markers, or any ``IntegrityError`` / ``ProgrammingError``.

    Used exclusively by :func:`provisioning_write_with_retry`.  Existing callers
    of ``_is_transient_asyncpg_error`` are unaffected.
    """
    if exc is None:
        return False
    orig = getattr(exc, "orig", None)
    return (
        _is_transient_asyncpg_error(exc)
        or (orig is not None and _is_transient_asyncpg_error(orig))
        or _is_lock_not_available_error(exc)
        or (orig is not None and _is_lock_not_available_error(orig))
        or _is_sync_closed_connection_error(exc)
    )


# PG SQLSTATEs that signal "object already exists" — fired when a concurrent
# worker created the same DDL object between our existence check and our
# CREATE attempt. PG's IF NOT EXISTS clause is not perfectly race-free at the
# system catalog level (pg_namespace / pg_class), and 23505 surfaces when two
# workers race past the advisory-lock coordination on snapshot-visibility
# edge cases. See issue #821.
_DUPLICATE_OBJECT_PGCODES = frozenset({
    "42P06",  # duplicate_schema
    "42P07",  # duplicate_table
    "42710",  # duplicate_object (general — types, functions, etc.)
    "23505",  # unique_violation (catches pg_namespace_nspname_index races)
})


def _is_duplicate_object_error(exc: Optional[BaseException]) -> bool:
    """Return True if ``exc`` signals an already-exists DDL conflict from PG.

    Walks the SQLAlchemy ``.orig`` chain so asyncpg-raised pgcodes are still
    detected when wrapped in ``DBAPIError``. Also accepts asyncpg's
    ``.sqlstate`` attribute as a fallback.
    """
    seen: set[int] = set()
    candidate: Optional[BaseException] = exc
    while candidate is not None and id(candidate) not in seen:
        seen.add(id(candidate))
        pgcode = getattr(candidate, "pgcode", None) or getattr(candidate, "sqlstate", None)
        if pgcode in _DUPLICATE_OBJECT_PGCODES:
            return True
        candidate = getattr(candidate, "orig", None) or getattr(candidate, "__cause__", None)
    return False

# --- Type Definitions ---
# Canonical definitions live in dynastore.models.db_resource (#1555).
# Re-exported here for backward compatibility — all existing callers that
# import from this module continue to work without changes.
from dynastore.models.db_resource import (  # noqa: E402, F401
    DbSyncConnection,
    DbAsyncConnection,
    DbEngine,
    DbConnection,
    DbSyncResource,
    DbAsyncResource,
    DbResource,
)
BuilderResult = Tuple[TextClause, dict]
QueryBuilderFunction: TypeAlias = Callable[
    [DbResource, dict], Union[BuilderResult, Awaitable[BuilderResult]]
]

R = TypeVar("R")
P = ParamSpec("P")

logger = logging.getLogger(__name__)


# DDL execution timeouts — short by default to surface deadlocks fast in
# prod, but tunable for CI where xdist parallelism + shared PG instance
# create lock contention that the default ceiling routinely exceeds. Set
# ``DYNASTORE_DDL_STATEMENT_TIMEOUT`` / ``DYNASTORE_DDL_LOCK_TIMEOUT``
# (PG interval syntax: ``30s``, ``2min``, ``"120s"``) in test compose to
# raise the ceiling without weakening prod behaviour.
#
# lock_timeout is kept SMALL on purpose: a DDL statement takes an
# AccessExclusiveLock, and a *pending* exclusive request queues ahead of
# every reader — so a DDL that waits N seconds for its lock freezes the
# whole application for N seconds. Bounding acquisition to a small window
# (mirrors DBConfig.lock_timeout, the engine-wide default) means a DDL can
# never convoy the app: it fails fast with 55P03 and is retried instead.
# statement_timeout stays larger — it bounds DDL *execution* (so a long
# build can finish) while still guaranteeing the lock is released and never
# left open indefinitely.
_DDL_STATEMENT_TIMEOUT = os.environ.get("DYNASTORE_DDL_STATEMENT_TIMEOUT", "30s")
_DDL_LOCK_TIMEOUT = os.environ.get("DYNASTORE_DDL_LOCK_TIMEOUT", "5s")

_metadata = MetaData()

# --- Connection Serialization (Re-entrant Async Wire Lock) ---

# Stores one asyncio.Lock per underlying physical connection wire (asyncpg.Connection).
_conn_locks = weakref.WeakKeyDictionary()
# Track which wire is locked by which asyncio task (wire_id -> task_id)
_locked_ids: contextvars.ContextVar[Dict[int, int]] = contextvars.ContextVar(
    "_locked_ids", default={}
)


def _get_wire_identity(conn: Any) -> Any:
    """
    Safely drills down to find a stable identity for the connection wire
    without triggering prohibited SQLAlchemy properties like .connection.

    Uses isinstance checks against concrete SQLAlchemy types instead of
    hasattr duck typing — faster and type-checker friendly.
    """
    curr = conn
    for _ in range(15):
        # 1. Handle Async Wrappers — unwrap to sync counterparts
        if isinstance(curr, AsyncSession):
            curr = curr.sync_session
            continue
        if isinstance(curr, AsyncConnection):
            curr = curr.sync_connection
            continue

        # 2. Handle Session bound to Connection
        if isinstance(curr, SASession) and curr.bind is not None and not isinstance(curr.bind, (Engine, AsyncEngine)):
            if isinstance(curr.bind, (SAConnection, AsyncConnection)):
                curr = curr.bind
                continue

        # 3. Drill to driver connection via standard attributes.
        # driver_connection is the public API; _connection and _proxied
        # are SQLAlchemy internals needed to traverse proxy layers to reach
        # the actual asyncpg wire (required for correct wire-lock identity).
        nxt = (
            getattr(curr, "driver_connection", None)
            or getattr(curr, "_connection", None)
            or getattr(curr, "_proxied", None)
        )

        # 4. Fallback to dbapi_connection / _dbapi_connection (on Connection objects)
        if nxt is None and isinstance(curr, SAConnection):
            nxt = getattr(curr, "dbapi_connection", None) or getattr(curr, "_dbapi_connection", None)

        if nxt is None or nxt is curr:
            break

        # If we hit the asyncpg connection, we're at the bottom
        if type(nxt).__module__.startswith("asyncpg"):
            curr = nxt
            break

        curr = nxt

    return curr


@asynccontextmanager
async def _connection_lock_scope(conn: DbResource):
    """
    Serializes access to the physical connection wire to prevent asyncpg InterfaceErrors.
    Nested sequential calls within the SAME coroutine proceed immediately (re-entrancy).
    Spawning concurrent tasks (e.g. via gather) on same wire will correctly wait.
    """
    # Engines create fresh wires for each request, so we only lock on connection instances.
    if not is_async_resource(conn) or isinstance(conn, (AsyncEngine, Engine)):
        yield
        return

    wire = _get_wire_identity(conn)
    wire_id = id(wire)
    # logger.warning(
    #     f"DEBUG: lock_scope wire_id={wire_id} type={type(wire)} conn_type={type(conn)}"
    # )

    # Identify current execution context

    current_task = asyncio.current_task()
    task_id = id(current_task) if current_task else 0

    locked_map = _locked_ids.get()

    # Re-entrant ONLY if it's the SAME task holding the lock for this wire
    if wire_id in locked_map and locked_map[wire_id] == task_id:
        yield
    else:
        # Use a stable lock for this physical wire instance
        if wire not in _conn_locks:
            _conn_locks[wire] = asyncio.Lock()

        async with _conn_locks[wire]:
            # Register this wire as locked by THIS task
            token = _locked_ids.set({**locked_map, wire_id: task_id})
            try:
                yield
            finally:
                _locked_ids.reset(token)
                # Brief yield to allow driver state to settle
                await asyncio.sleep(0)


# A process-wide global reference to the main application's event loop.
_main_app_loop: Optional[asyncio.AbstractEventLoop] = None


def set_main_app_loop(loop: asyncio.AbstractEventLoop):
    """Sets the main application event loop for thread-safe calls."""
    global _main_app_loop
    _main_app_loop = loop


# --- Helper Functions ---


def is_async_resource(db_resource: DbResource) -> TypeGuard[DbAsyncResource]:
    """Determines if a resource supports asynchronous operations."""
    return isinstance(db_resource, (AsyncEngine, AsyncConnection, AsyncSession, AsyncTransaction))


def _is_in_transaction(conn: Any) -> bool:
    """Helper to check if a database resource is currently in a transaction."""
    if isinstance(conn, (SAConnection, SASession, AsyncConnection, AsyncSession)):
        return conn.in_transaction()
    return False


def serialize_geom(item):
    """Converts geometry elements to GeoJSON-compatible dictionaries."""
    if not isinstance(item, dict) and not hasattr(item, "_asdict"):
        return item
    data = item if isinstance(item, dict) else item._asdict()
    for geom_col in ["geom", "bbox_geom", "simplified_geom"]:
        if geom_col in data and data[geom_col] is not None:
            if isinstance(data[geom_col], (WKBElement, WKTElement)):
                data[geom_col] = to_shape(data[geom_col]).__geo_interface__
    return data


# --- Result Handling ---


class ResultHandler:
    """Standard recipes for processing SQLAlchemy Results."""

    SCALAR = lambda r: r.scalar()
    SCALAR_ONE = lambda r: r.scalar_one()
    SCALAR_ONE_OR_NONE = lambda r: r.scalar_one_or_none()
    ONE = lambda r: r.one()
    ONE_OR_NONE = lambda r: r.fetchone()
    ALL = lambda r: r.all()
    ALL_SCALARS = lambda r: r.scalars().all()
    ROWCOUNT = lambda r: r.rowcount
    ALL_DICTS = lambda r: [row._asdict() for row in r.all()]
    ONE_DICT = lambda r: row._asdict() if (row := r.fetchone()) else None
    NONE = lambda r: None


class PydanticResultHandler(ResultHandler):
    """Extends ResultHandler to include Pydantic model conversion."""

    @staticmethod
    def pydantic_one(model_class: Type[BaseModel]):
        def handler(result_proxy: Result) -> Optional[BaseModel]:
            row = result_proxy.fetchone()
            if row:
                return model_class.model_validate(row._asdict())
            return None

        return handler

    @staticmethod
    def pydantic_all(model_class: Type[BaseModel]):
        def handler(result_proxy: Result) -> List[BaseModel]:
            return [
                model_class.model_validate(row._asdict())
                for row in result_proxy.fetchall()
            ]

        return handler


# --- Query Builder Strategies ---


class QueryBuilderStrategy(ABC):
    @abstractmethod
    def build(
        self, db_resource: DbResource, raw_params: dict
    ) -> Union[BuilderResult, Awaitable[BuilderResult]]:
        pass


class TemplateQueryBuilder(QueryBuilderStrategy):
    """Builds a query from a string template with {identifier} substitutions."""

    def __init__(self, query_template: Union[str, DDL]):
        self.query_template = query_template

    def build(self, db_resource: DbResource, raw_params: dict):
        is_ddl = isinstance(self.query_template, DDL)
        template_str = str(self.query_template)

        template_identifiers = re.findall(r"{(\w+)}", template_str)
        quoted_identifiers, params = {}, {}

        if template_identifiers:
            # Identifier substitutions require a dialect to quote names safely.
            if isinstance(db_resource, (Engine, AsyncEngine, SAConnection, AsyncConnection)):
                dialect = db_resource.dialect
            elif isinstance(db_resource, (SASession, AsyncSession)):
                bind = db_resource.bind
                if bind is not None:
                    dialect = bind.dialect
                else:
                    raise TypeError(
                        f"TemplateQueryBuilder: Session has no bind, cannot resolve dialect."
                    )
            else:
                raise TypeError(
                    f"TemplateQueryBuilder: Unable to resolve dialect from {type(db_resource)}."
                )

            for key, value in raw_params.items():
                if key in template_identifiers:
                    val_str = str(value)
                    try:
                        quoted_identifiers[key] = dialect.identifier_preparer.quote(val_str)
                    except Exception:
                        quoted_identifiers[key] = f'"{val_str.replace('"', '""')}"'
                else:
                    params[key] = value
        else:
            # No identifier slots — all params are bind parameters; no dialect needed.
            params = dict(raw_params)

        final_query_str = template_str
        for key, value in quoted_identifiers.items():
            final_query_str = final_query_str.replace(f"{{{key}}}", value)
        query_obj = DDL(final_query_str) if is_ddl else text(final_query_str)
        return query_obj, params


class CommentQueryBuilder(TemplateQueryBuilder):
    """Specialized builder for COMMENT ON statements."""

    def build(self, db_resource: DbResource, raw_params: dict):
        comment_text = raw_params.pop("comment", "")
        query_obj, params = super().build(db_resource, raw_params)
        final_sql = f"{str(query_obj)} $${comment_text}$$"
        compiled_sql = text(final_sql).compile(compile_kwargs={"literal_binds": True})
        return compiled_sql, params


class FunctionQueryBuilder(QueryBuilderStrategy):
    def __init__(self, query_builder_func: QueryBuilderFunction):
        self.query_builder_func = query_builder_func

    def build(self, db_resource, raw_params: dict):
        return self.query_builder_func(db_resource, raw_params)


# --- Executors ---


class BaseExecutor:
    """Core executor logic with wire serialization and post-processing."""

    def __init__(
        self,
        query_builder_strategy: QueryBuilderStrategy,
        post_processor: Optional[Callable] = None,
        **kwargs,
    ):
        self.query_builder_strategy = query_builder_strategy
        self.post_processors = [post_processor] if post_processor else []

    @classmethod
    def from_template(cls, query_template: Union[str, DDL], **kwargs):
        return cls(TemplateQueryBuilder(query_template), **kwargs)

    @classmethod
    def from_builder(cls, query_builder: QueryBuilderFunction, **kwargs):
        return cls(FunctionQueryBuilder(query_builder), **kwargs)

    async def __call__(self, db_resource: DbResource, *args, **kwargs):
        raw_params = args[0] if args else kwargs
        if isinstance(db_resource, str):
            raise TypeError(
                f"BaseExecutor: Expected database resource, got string '{db_resource}'."
            )

        if is_async_resource(db_resource):
            return await self._execute_async_workflow(db_resource, raw_params)
        else:
            return self._execute_sync_workflow(db_resource, raw_params)

    def _execute_sync_workflow(self, db_resource, raw_params):
        if isinstance(db_resource, Engine):
            with db_resource.connect() as conn:
                return self._build_and_execute_sync(conn, raw_params)
        return self._build_and_execute_sync(db_resource, raw_params)

    async def _execute_async_workflow(self, db_resource: DbAsyncResource, raw_params):
        if isinstance(db_resource, AsyncEngine):
            # Manual management to extend lock scope over close().
            # Connection acquisition routes through _acquire_async_engine_connection
            # so transient pool/connect failures (DB warming, OperationalError,
            # asyncio.TimeoutError, OSError) trigger bounded retry instead of
            # crashing the caller's lifespan / request.
            conn = await _acquire_async_engine_connection(db_resource)
            try:
                async with _connection_lock_scope(conn):
                    result = await self._build_and_execute_async(conn, raw_params)
                    # Paranoid cleanup: ensure no transaction lingers before return to pool
                    # This helps with StaticPool where the wire is reused immediately
                    if conn.in_transaction():
                        await conn.rollback()
                    await conn.close()
                    return result
            except Exception:
                # Ensure closed if something failed
                try:
                    await conn.close()
                except Exception as close_exc:
                    # Connection is likely dead; log so operators can correlate
                    # pool-slot leaks with the originating error.
                    logger.warning(
                        "query_executor: conn.close() failed during error cleanup: %s",
                        close_exc,
                    )
                raise
        return await self._build_and_execute_async(db_resource, raw_params)

    async def stream_async_workflow(self, db_resource, raw_params):
        if isinstance(db_resource, (AsyncEngine, Engine)):
            raise TypeError(
                "Cannot stream from an Engine. Please acquire a connection first."
            )
        return await self._build_and_stream_async(db_resource, raw_params)

    def _build_and_execute_sync(self, conn, raw_params: dict):
        if inspect.iscoroutinefunction(self.query_builder_strategy.build):
            raise TypeError(
                "Cannot use an async query builder with a synchronous connection."
            )
        # Store raw_params so DDLExecutor existence checks can access
        # identifier values (e.g. schema) that TemplateQueryBuilder consumes.
        self._raw_params = raw_params
        build_result = self.query_builder_strategy.build(conn, raw_params)
        query_obj, params = cast(BuilderResult, build_result)
        return self._execute_sync(conn, query_obj, params)

    async def _build_and_execute_async(self, conn: DbAsyncConnection, raw_params: dict):
        async with _connection_lock_scope(conn):
            # Store raw_params so DDLExecutor existence checks can access
            # identifier values (e.g. schema) that TemplateQueryBuilder consumes.
            self._raw_params = raw_params
            build_result = self.query_builder_strategy.build(conn, raw_params)
            query_obj, params = (
                await build_result
                if inspect.isawaitable(build_result)
                else build_result
            )
            return await self._execute_async(conn, query_obj, params)

    async def _build_and_stream_async(self, conn: DbAsyncConnection, raw_params: dict):
        async with _connection_lock_scope(conn):
            build_result = self.query_builder_strategy.build(conn, raw_params)
            query_obj, params = (
                await build_result
                if inspect.isawaitable(build_result)
                else build_result
            )
            return self._stream_async(conn, query_obj, params)

    def _handle_db_exception(self, e: Exception) -> None:
        original_exc = getattr(e, "orig", None)
        pgcode = getattr(original_exc, "pgcode", None)
        if pgcode in PGCODE_EXCEPTION_MAP:
            exception_class = PGCODE_EXCEPTION_MAP[pgcode]
            raise exception_class(
                f"Database error ({pgcode})", original_exception=original_exc
            ) from e
        # Transient asyncpg client-state errors carry no pgcode — they
        # predate the query reaching PG (e.g. connection closed mid-op,
        # connection used concurrently from two coroutines).  Surface as
        # ``DatabaseConnectionError`` so callers (e.g. tasks/dispatcher.py)
        # can apply back-off + retry instead of escalating to ERROR.
        # Tracks #235 (ConnectionDoesNotExistError) and #239
        # ("cannot switch to state N").
        if _is_transient_asyncpg_error(original_exc) or _is_transient_asyncpg_error(e):
            raise DatabaseConnectionError(
                "Transient asyncpg client error",
                original_exception=original_exc,
            ) from e
        raise QueryExecutionError(
            "Database query failed.", original_exception=original_exc
        ) from e

    @abstractmethod
    def _execute_sync(self, conn: DbSyncConnection, query_obj: TextClause, params: dict):
        pass

    @abstractmethod
    async def _execute_async(self, conn: DbAsyncConnection, query_obj: TextClause, params: dict):
        pass

    async def _stream_async(self, conn: DbAsyncConnection, query_obj: TextClause, params: dict):
        raise NotImplementedError(
            f"Streaming not supported by {self.__class__.__name__}"
        )

    def _apply_post_processing_sync(self, result: Any) -> Any:
        for p in self.post_processors:
            result = (
                run_in_event_loop(p(result))
                if inspect.iscoroutinefunction(p)
                else p(result)
            )
        return result

    async def _apply_post_processing_async(self, result: Any) -> Any:
        for p in self.post_processors:
            result = await p(result) if inspect.iscoroutinefunction(p) else p(result)
        return result


class DQLExecutor(BaseExecutor):
    def __init__(self, query_builder_strategy, result_handler, **kwargs):
        super().__init__(query_builder_strategy, **kwargs)
        self.result_handler = result_handler

    def _execute_sync(self, conn: DbSyncConnection, query_obj: TextClause, params: dict):
        try:
            result = conn.execute(query_obj, params)
            processed = self.result_handler(result)
            return self._apply_post_processing_sync(processed)
        except Exception as e:
            self._handle_db_exception(e)

    async def _execute_async(
        self, conn: DbAsyncConnection, query_obj: TextClause, params: dict
    ):
        try:
            result = await conn.execute(query_obj, params)
            processed = self.result_handler(result)

            return await self._apply_post_processing_async(processed)
        except Exception as e:
            self._handle_db_exception(e)

    async def _stream_async(
        self, conn: DbAsyncConnection, query_obj: TextClause, params: dict
    ):
        try:
            stream_result = await conn.stream(query_obj, params)
            async for row in stream_result.mappings():
                yield await self._apply_post_processing_async(dict(row))
        except Exception as e:
            self._handle_db_exception(e)


def _strip_line_comments(ddl_text: str) -> str:
    """Strip ``-- ...`` line comments while preserving them inside string
    literals and dollar-quoted blocks.

    Required before ``split_ddl`` so a ``;`` appearing inside a comment does
    not produce a spurious statement boundary.
    """
    if "--" not in ddl_text:
        return ddl_text

    out: list[str] = []
    i = 0
    n = len(ddl_text)
    active_dollar_tag: str | None = None
    in_quote = False

    while i < n:
        ch = ddl_text[i]
        # Inside dollar-quoted block: only exit on matching tag
        if active_dollar_tag is not None:
            if ch == "$":
                m = re.match(r"\$[a-zA-Z0-9_]*\$", ddl_text[i:])
                if m and m.group(0) == active_dollar_tag:
                    out.append(m.group(0))
                    i += len(m.group(0))
                    active_dollar_tag = None
                    continue
            out.append(ch)
            i += 1
            continue
        # Inside single-quoted literal: only exit on closing quote
        if in_quote:
            out.append(ch)
            if ch == "'":
                in_quote = False
            i += 1
            continue
        # Outside any quote: detect entries
        if ch == "$":
            m = re.match(r"\$[a-zA-Z0-9_]*\$", ddl_text[i:])
            if m:
                active_dollar_tag = m.group(0)
                out.append(m.group(0))
                i += len(m.group(0))
                continue
        if ch == "'":
            in_quote = True
            out.append(ch)
            i += 1
            continue
        if ch == "-" and i + 1 < n and ddl_text[i + 1] == "-":
            # Skip until newline (or EOF)
            nl = ddl_text.find("\n", i)
            if nl == -1:
                break
            i = nl  # keep the newline itself
            continue
        out.append(ch)
        i += 1

    return "".join(out)


def split_ddl(ddl_text: str) -> List[str]:
    """
    Smarter split that respects dollar-quoting (e.g. $$, $BODY$), ``''`` string
    literals, and ``-- ...`` line comments. This avoids breaking function
    bodies or complex DDL containing semicolons in comments or strings.
    """
    if not ddl_text or ";" not in ddl_text:
        return [ddl_text] if ddl_text else []

    ddl_text = _strip_line_comments(ddl_text)

    statements = []
    parts = re.split(r"(\$[a-zA-Z0-9_]*\$|'|;)", ddl_text)
    current_stmt = []
    active_dollar_tag = None
    in_quote = False

    for part in parts:
        if re.match(r"^\$[a-zA-Z0-9_]*\$$", part):
            if not in_quote:
                if active_dollar_tag is None:
                    active_dollar_tag = part
                elif active_dollar_tag == part:
                    active_dollar_tag = None
            current_stmt.append(part)
        elif part == "'" and active_dollar_tag is None:
            in_quote = not in_quote
            current_stmt.append(part)
        elif part == ";" and active_dollar_tag is None and not in_quote:
            stmt = "".join(current_stmt).strip()
            if stmt:
                statements.append(stmt)
            current_stmt = []
        else:
            current_stmt.append(part)

    final_stmt = "".join(current_stmt).strip()
    if final_stmt:
        statements.append(final_stmt)

    return statements


class DDLExecutor(BaseExecutor):
    """
    Transparently implements DDL Coordination:
    1. In-process deduplication (via StartupCoordinator).
    2. DB-level advisory locking (via query hash) for cross-instance safety.
    3. Retries on conflict.
    4. Guarded by existence checks to avoid redundant locking.
    """

    def __init__(self, query_builder_strategy, existence_check=None, **kwargs):
        super().__init__(query_builder_strategy, **kwargs)
        self.existence_check = existence_check

    async def _call_existence_check(self, conn, params):
        """Invoke existence_check, passing raw_params for inferred checks."""
        check = self.existence_check
        assert check is not None, "_call_existence_check called with no existence_check set"
        if getattr(check, "_needs_raw_params", False):
            res = check(conn, params, self._raw_params)
        else:
            res = check(conn, params)
        if inspect.isawaitable(res):
            res = await res
        return res

    def _call_existence_check_sync(self, conn, params) -> bool:
        """Sync sibling of _call_existence_check.

        For sync existence checks: call directly.

        For async existence checks (the auto-inferred case from
        ddl_inference._infer_existence_check): drive the coroutine manually
        with ``coro.send(None)``. This is intentional — sync DDL execution
        is sometimes invoked from inside an async lifespan handler whose
        thread already owns a running loop, so neither ``asyncio.run`` nor
        ``run_in_event_loop`` is usable here. The inferred check chain
        eventually reaches ``DQLQuery.execute`` → ``BaseExecutor.__call__``
        which dispatches to ``_execute_sync_workflow`` for sync conns —
        the coroutine wraps sync work and never actually awaits real I/O,
        so manual driving completes in one step.
        """
        check = self.existence_check
        assert check is not None, "_call_existence_check_sync called with no existence_check set"
        if getattr(check, "_needs_raw_params", False):
            res = check(conn, params, self._raw_params)
        else:
            res = check(conn, params)
        if inspect.iscoroutine(res):
            try:
                while True:
                    res.send(None)
            except StopIteration as stop:
                res = stop.value
        elif inspect.isawaitable(res):
            # Non-coroutine awaitable — fall back to asyncio.run only if no
            # loop is currently running. Manual driving doesn't apply here.
            try:
                asyncio.get_running_loop()
                raise RuntimeError(
                    "_call_existence_check_sync received a non-coroutine awaitable "
                    "while a loop is running; cannot dispatch safely."
                )
            except RuntimeError:
                pass

                async def _consume():
                    return await res

                res = asyncio.run(_consume())
        return bool(res)

    # #821 peer-race recovery. A concurrent worker may have created the DDL
    # object between our post-wait re-check and our CREATE. PG's IF NOT
    # EXISTS is not perfectly race-free at the catalog level
    # (pg_namespace / pg_class) and the advisory-lock coordination can miss
    # the peer's commit under specific snapshot-visibility conditions.
    # Re-check on the outer conn (fresh statement-level snapshot under
    # READ COMMITTED): if the object now exists, treat as success rather
    # than failing module init.
    async def _try_peer_race_recovery_async(
        self, conn: DbAsyncConnection, params: dict, exc: BaseException
    ) -> bool:
        """Return True iff the failed DDL was a duplicate-object race the
        peer already won and the outer-conn re-check now reports success."""
        if not self.existence_check or not _is_duplicate_object_error(exc):
            return False
        try:
            if await self._call_existence_check(conn, params):
                pgcode = (
                    getattr(getattr(exc, "orig", None), "pgcode", None)
                    or getattr(exc, "pgcode", None)
                    or getattr(exc, "sqlstate", None)
                )
                logger.info(
                    "DDL peer-race resolved: object exists after concurrent creation (pgcode=%s).",
                    pgcode,
                )
                return True
        except Exception as recheck_exc:
            logger.warning(
                "DDL peer-race recheck failed: %s; surfacing original error.",
                recheck_exc,
            )
        return False

    def _try_peer_race_recovery_sync(
        self, conn: DbSyncConnection, params: dict, exc: BaseException
    ) -> bool:
        """Sync sibling of :meth:`_try_peer_race_recovery_async`."""
        if not self.existence_check or not _is_duplicate_object_error(exc):
            return False
        try:
            if self._call_existence_check_sync(conn, params):
                logger.info(
                    "DDL peer-race resolved (sync): object exists after concurrent creation."
                )
                return True
        except Exception as recheck_exc:
            logger.warning(
                "DDL peer-race recheck failed (sync): %s; surfacing original error.",
                recheck_exc,
            )
        return False

    def _execute_sync(self, conn: DbSyncConnection, query_obj: TextClause, params: dict):
        """Execute DDL with centralized coordination and timeout guards."""
        from .locking_tools import sync_acquire_startup_lock
        import json

        # 1. Optimistic existence check (outside any lock).
        # Supports both sync and async existence_check callables — async ones
        # are dispatched via run_in_event_loop. Failures here are logged and
        # we proceed to the lock + in-tx re-check below.
        if self.existence_check:
            try:
                if self._call_existence_check_sync(conn, params):
                    return self._apply_post_processing_sync(None)
            except Exception as e:
                logger.warning(
                    "DDL optimistic existence check failed (sync): %s; proceeding to lock + re-check.",
                    e,
                )

        stmt_text = query_obj.text if isinstance(query_obj, TextClause) else str(query_obj)
        # Include parameters in hash for proper coordination
        param_str = json.dumps(params, sort_keys=True, default=str) if params else ""
        combined = f"{stmt_text.strip()}|{param_str}"
        stmt_hash = hashlib.sha256(combined.encode()).hexdigest()[:16]
        lock_key = f"ddl.{stmt_hash}"

        with sync_managed_transaction(conn) as tx_conn:
            with sync_acquire_startup_lock(
                tx_conn, lock_key, timeout="10s"
            ) as active_conn:
                if active_conn:
                    _active: DbSyncConnection = cast(DbSyncConnection, active_conn)

                    # 2. In-tx re-check after lock acquisition. Required for
                    # idempotency: another worker may have created the object
                    # between our optimistic check and our lock acquisition.
                    # Mirrors _execute_async:776-787.
                    if self.existence_check:
                        try:
                            if self._call_existence_check_sync(_active, params):
                                return self._apply_post_processing_sync(None)
                        except Exception as e:
                            logger.warning(
                                "DDL post-lock existence re-check failed (sync): %s; proceeding to DDL.",
                                e,
                            )

                    try:
                        # Timeout guard to prevent deadlocks
                        _active.execute(text(f"SET LOCAL statement_timeout = '{_DDL_STATEMENT_TIMEOUT}'"))

                        # Support multi-statement DDL by splitting
                        statements = split_ddl(stmt_text)
                        if len(statements) > 1:
                            for stmt in statements:
                                _active.execute(text(stmt), params)
                        else:
                            _active.execute(query_obj, params)
                    except Exception as e:
                        if self._try_peer_race_recovery_sync(conn, params, e):
                            return self._apply_post_processing_sync(None)
                        self._handle_db_exception(e)
        return self._apply_post_processing_sync(None)

    async def _execute_async(self, conn: DbAsyncConnection, query_obj: TextClause, params: dict):
        """Execute DDL with centralized coordination and timeout guards."""
        from .locking_tools import _get_stable_lock_id
        import json

        # 1. Faster Optimistic Check
        # IMPORTANT: must run inside a SAVEPOINT so that any query failure rolls back
        # only to the savepoint and does NOT poison the outer transaction.
        if self.existence_check:
            try:
                # Use a savepoint if we're already inside a transaction to prevent
                # a failed existence check from aborting the outer transaction.
                if isinstance(conn, (AsyncConnection, AsyncSession)) and conn.in_transaction():
                    try:
                        res = False
                        async with conn.begin_nested() as sp:
                            res = await self._call_existence_check(conn, params)
                            # Force a rollback of this savepoint.
                            # If `existence_check` executed a query that failed and swallowed the error,
                            # the asyncpg connection is in an "aborted" state. If we exit the block
                            # gracefully, SQLAlchemy emits RELEASE SAVEPOINT which fails and poisons
                            # everything. Rolling back explicitly guarantees health restoration.
                            await sp.rollback()

                        if res:
                            return await self._apply_post_processing_async(None)
                    except Exception as e:
                        # SAVEPOINT was rolled back cleanly; outer tx remains healthy.
                        logger.debug(
                            "DDL existence check savepoint rolled back (expected on aborted check): %s",
                            e,
                        )
                else:
                    res = await self._call_existence_check(conn, params)
                    # The SELECT in the existence check triggers SQLAlchemy autobegin.
                    # Reset it now so managed_transaction below starts a proper top-level
                    # transaction; if it sees in_transaction()=True it uses begin_nested()
                    # (SAVEPOINT) whose outer autobegin _execute_async_workflow later rolls
                    # back, silently discarding the DDL.
                    if isinstance(conn, (AsyncConnection, AsyncSession)):
                        try:
                            if conn.in_transaction():
                                await conn.rollback()
                        except Exception as rb_exc:
                            # Rollback of the autobegin transaction failed — the
                            # connection is likely dead or in a bad state. Log so
                            # operators can correlate DDL-skip symptoms with the
                            # underlying wire failure.
                            logger.warning(
                                "query_executor: autobegin rollback failed before DDL lock: %s",
                                rb_exc,
                            )
                    if res:
                        return await self._apply_post_processing_async(None)
            except Exception as e:
                logger.warning(
                    "DDL optimistic existence check failed (async): %s; proceeding to lock + re-check.",
                    e,
                )

        stmt_text = query_obj.text if isinstance(query_obj, TextClause) else str(query_obj)
        # Include parameters in hash for proper coordination
        param_str = json.dumps(params, sort_keys=True, default=str) if params else ""
        combined = f"{stmt_text.strip()}|{param_str}"
        stmt_hash = hashlib.sha256(combined.encode()).hexdigest()[:16]
        lock_id = _get_stable_lock_id(f"ddl.{stmt_hash}")

        # Inner attempt: open inner tx + acquire advisory lock + recheck +
        # execute. Wrapped in retry_on_transient_connect so a brief
        # OperationalError / TimeoutError between attempts triggers a
        # bounded retry. CRITICAL: the retry sleep happens AFTER this
        # closure has returned, which means the inner managed_transaction
        # has exited and the xact-scoped advisory lock has been released
        # — we never sleep while holding the lock (which would re-create
        # the consumer-lock pile-up bug fixed earlier on this branch).
        # Each retry attempt runs the existence re-check first, so a peer
        # that won the race during the prior attempt's failure is
        # detected and the DDL is skipped.
        @retry_on_transient_connect()
        async def _attempt_ddl():
            async with managed_transaction(conn) as tx_conn:
                assert isinstance(tx_conn, (AsyncConnection, AsyncSession)), "DDL async executor requires async connection"
                # 2. Re-check after acquiring transaction but before locking
                if self.existence_check:
                    res = False
                    if isinstance(tx_conn, (AsyncConnection, AsyncSession)) and tx_conn.in_transaction():
                        async with tx_conn.begin_nested() as sp:
                            res = await self._call_existence_check(tx_conn, params)
                            await sp.rollback()
                    else:
                        res = await self._call_existence_check(tx_conn, params)

                    if res:
                        return True  # already exists

                # 3. Try-lock first for fast failure
                result = await tx_conn.execute(
                    text("SELECT pg_try_advisory_xact_lock(:lock_id)"),
                    {"lock_id": lock_id},
                )
                acquired = result.scalar()

                if not acquired:
                    # Another worker holds the lock. Wait for them to finish so that
                    # their transaction is committed before we re-check existence.
                    await tx_conn.execute(text(f"SET LOCAL lock_timeout = '{_DDL_LOCK_TIMEOUT}'"))
                    await tx_conn.execute(
                        text("SELECT pg_advisory_xact_lock(:lock_id)"),
                        {"lock_id": lock_id},
                    )
                    # Re-check: the other worker should have committed the object by now.
                    if self.existence_check:
                        res_post = await self._call_existence_check(tx_conn, params)
                        if res_post:
                            return True  # peer created it during our wait

                # Timeout guard to prevent DDL hangs
                await tx_conn.execute(text(f"SET LOCAL statement_timeout = '{_DDL_STATEMENT_TIMEOUT}'"))

                # Support multi-statement DDL by splitting (asyncpg limitation)
                statements = split_ddl(stmt_text)
                if len(statements) > 1:
                    for i, stmt in enumerate(statements):
                        await tx_conn.execute(text(stmt), params)
                else:
                    await tx_conn.execute(query_obj, params)
                return False  # we created it

        try:
            await _attempt_ddl()
            return await self._apply_post_processing_async(None)
        except Exception as e:
            if await self._try_peer_race_recovery_async(conn, params, e):
                return await self._apply_post_processing_async(None)
            self._handle_db_exception(e)


class GeoDQLExecutor(DQLExecutor):
    def __init__(
        self, query_builder_strategy, result_handler, post_processor=None, **kwargs
    ):
        super().__init__(
            query_builder_strategy, result_handler=result_handler, **kwargs
        )

        def geo_p(data):
            if data is None:
                return None
            items = [data] if not isinstance(data, list) else data
            processed = [serialize_geom(item) for item in items]
            return (
                processed[0] if not isinstance(data, list) and processed else processed
            )

        self.post_processors = [geo_p] + (
            post_processor
            if isinstance(post_processor, list)
            else ([post_processor] if post_processor else [])
        )
        self.post_processors = [geo_p] + (
            post_processor
            if isinstance(post_processor, list)
            else ([post_processor] if post_processor else [])
        )


# --- Public API Functions ---


# Exception types that signal a transient failure to *acquire* a database
# connection (engine.connect()) or to execute idempotent DDL: pool timeout,
# socket reset, server still warming up, asyncpg wire-protocol churn, etc.
# Distinct from `retry_on_lock_conflict`, which targets in-flight lock /
# deadlock contention on an already-acquired connection. Kept narrow on
# purpose: IntegrityError / ProgrammingError / DataError MUST NOT be in
# this set — they signal real bugs and must surface, not retry.
_TRANSIENT_CONNECT_EXCEPTIONS: tuple = (
    asyncio.TimeoutError,
    OSError,
    OperationalError,
    InterfaceError,
    # asyncpg wire-state errors raised during pool-hygiene rollback poison the
    # pool unless the checkout decorator can retry and invalidate the wire.
    AsyncpgConnectionDoesNotExistError,
    AsyncpgInternalClientError,
)


def _err_repr(exc: BaseException) -> str:
    """One-line representation of an exception for structured log lines."""
    return f"{type(exc).__name__}: {exc}"


async def _run_with_retry_policy(
    call: "Callable[[], Awaitable[Any]]",
    *,
    classify: "Callable[[BaseException], bool]",
    kind: str,
    max_attempts: int,
    compute_delay: "Callable[[int, BaseException], float]",
    level: int = logging.WARNING,
    log_exhaustion: bool = False,
) -> Any:
    """Private async retry core shared by all retry wrappers in this module.

    Not part of the public API — use :func:`retry_on_transient_connect` or
    :func:`provisioning_write_with_retry`.

    Emits one unified per-attempt log line at ``level`` (default WARNING):

        retry attempt=N/M kind=<domain> backoff_s=F err=<Type>: <msg>

    A single GCP log-based metric expression on the TEXT covers all retry
    sites regardless of per-site severity. Callers choose the level to
    preserve their existing log-flood characteristics.

    When ``log_exhaustion`` is True, emits an additional WARNING on the
    final failed attempt before re-raising so operators can identify
    exhausted retry budgets independently of exception tracing.

    :param call: Zero-arg async thunk to attempt on each iteration.
    :param classify: Return True for exceptions safe to retry; non-matching
        exceptions propagate immediately without consuming retry budget.
    :param kind: Domain tag for the log line (e.g. ``"db_connect"``).
    :param max_attempts: Total attempts (1 = no retry).
    :param compute_delay: ``(attempt, exc) -> float`` backoff in seconds,
        called only for non-final attempts.
    :param level: Log level for per-attempt retry lines (default WARNING).
    :param log_exhaustion: If True, emit an extra WARNING when all attempts
        are consumed before re-raising the final exception.
    """
    last: Optional[BaseException] = None
    for attempt in range(max_attempts):
        try:
            return await call()
        except BaseException as exc:
            if not classify(exc):
                raise
            last = exc
            if attempt == max_attempts - 1:
                if log_exhaustion:
                    logger.warning(
                        "retry exhausted attempts=%d kind=%s err=%s",
                        max_attempts,
                        kind,
                        _err_repr(exc),
                    )
                raise
            delay = compute_delay(attempt, exc)
            logger.log(
                level,
                "retry attempt=%d/%d kind=%s backoff_s=%.2f err=%s",
                attempt + 1,
                max_attempts,
                kind,
                delay,
                _err_repr(exc),
            )
            await asyncio.sleep(delay)
    assert last is not None
    raise last


def retry_on_transient_connect(
    max_retries: int | None = None,
    base_delay: float | None = None,
    max_delay: float | None = None,
    jitter: float | None = None,
):
    """Retry a coroutine on transient connection-acquisition / DDL infra errors.

    Distinct from :func:`retry_on_lock_conflict` — the latter retries inside
    a held connection on lock/deadlock; this one retries the *infrastructure*
    layer (pool checkout, wire connect, idempotent DDL execution against a
    briefly unavailable server). Composes safely with `retry_on_lock_conflict`
    on the same callable; total worst-case attempts are the product, but in
    practice the two failure modes do not overlap on the same call.

    Backoff: ``base_delay * 2 ** attempt``, clamped at ``max_delay``, with
    ±``jitter`` multiplicative spread. Five attempts at the defaults give
    ~0.5/1/2/4/8s spaced retries (~15s budget).

    Only retries the exception types in :data:`_TRANSIENT_CONNECT_EXCEPTIONS`.
    Anything else propagates immediately so genuine bugs are not masked.

    Configuration: Values are resolved from (1) explicit function parameters,
    else (2) the module-global ``ConnectionRetryConfig`` defaults.
    Resolved at CALL TIME. See :mod:`connection_health_config` for details.
    """

    def decorator(func):
        @functools.wraps(func)
        async def wrapper(*args, **kwargs):
            # Resolve config at call time so live config edits are picked up.
            cfg_max_retries, cfg_base_delay, cfg_max_delay, cfg_jitter = resolve_connection_retry_config()
            _max_retries = max_retries if max_retries is not None else cfg_max_retries
            _base_delay = base_delay if base_delay is not None else cfg_base_delay
            _max_delay = max_delay if max_delay is not None else cfg_max_delay
            _jitter = jitter if jitter is not None else cfg_jitter

            def _compute_delay(attempt: int, exc: BaseException) -> float:
                delay = min(_base_delay * (2 ** attempt), _max_delay)
                if _jitter:
                    delay *= random.uniform(1.0 - _jitter, 1.0 + _jitter)
                return delay

            return await _run_with_retry_policy(
                lambda: func(*args, **kwargs),
                classify=lambda e: isinstance(e, _TRANSIENT_CONNECT_EXCEPTIONS),
                kind="db_connect",
                max_attempts=_max_retries,
                compute_delay=_compute_delay,
                level=logging.DEBUG,
                log_exhaustion=True,
            )

        return wrapper

    return decorator


# M3 (issue #486): emit a structured log line on every async pool acquire so a
# GCP log-based metric can compute db_pool_wait_seconds histograms per service
# without needing a prometheus_client dep + scrape endpoint. INFO when slow,
# DEBUG otherwise. The threshold is read at call time via
# ``resolve_slow_pool_acquire_threshold()`` so tests that override the module
# global see the updated value immediately.


def _resolve_service_name() -> str:
    """Same source-of-truth used by dispatcher service-affinity routing
    (``instance.json``). Falls back to ``SERVICE_NAME`` env var, then to the
    literal ``"unknown"`` so the log line is never empty.
    """
    try:
        from dynastore.modules.db_config.instance import get_service_name
        name = get_service_name()
        if name:
            return name
    except Exception:  # noqa: BLE001 — never let metrics setup crash imports
        pass
    return os.getenv("SERVICE_NAME") or "unknown"


_SERVICE_NAME_FOR_METRICS = _resolve_service_name()


# Optional per-acquire workload tag — pushed by callers that have richer
# context than "this process is service X" (e.g. a task runner pushes
# ``task_type=…``; a request middleware pushes ``catalog_id=…``). Surfaces in
# the slow-acquire log line so a 5-min log slice can be partitioned by who
# actually held the pool. Issue #699.
_pool_acquire_scope: contextvars.ContextVar[str] = contextvars.ContextVar(
    "pool_acquire_scope", default=""
)


@contextmanager
def pool_acquire_scope(**tags: object):
    """Push key=value tags onto the next ``db_pool_acquire`` log line(s)
    inside this block. Tags are merged with any outer scope (newer wins on
    duplicate keys). Empty/None values are skipped so callers can pass
    optional context unconditionally.
    """
    parts = [f"{k}={v}" for k, v in tags.items() if v not in (None, "")]
    if not parts:
        yield
        return
    suffix = " ".join(parts)
    prev = _pool_acquire_scope.get()
    merged = f"{prev} {suffix}".strip() if prev else suffix
    token = _pool_acquire_scope.set(merged)
    try:
        yield
    finally:
        _pool_acquire_scope.reset(token)


def _acquire_scope_suffix() -> str:
    s = _pool_acquire_scope.get()
    return f" {s}" if s else ""


async def _drain_rollback_exit(txn_cm: Any, exc: BaseException) -> None:
    """Best-effort ROLLBACK drain used by :func:`managed_transaction` on
    the cancelled / exception exit path. Invokes the transaction context
    manager's ``__aexit__`` so SQLAlchemy emits the wire-level ROLLBACK,
    then swallows any error — the caller is already re-raising the
    original exception; a failed rollback must not mask it. See #628 for
    the IN_QUERY state-15 cascade this drain prevents.
    """
    try:
        await txn_cm.__aexit__(type(exc), exc, exc.__traceback__)
    except Exception as drain_exc:  # noqa: BLE001
        logger.debug(
            "managed_transaction: rollback drain failed: %s", drain_exc,
        )


@retry_on_transient_connect()
async def _acquire_async_engine_connection(engine: AsyncEngine) -> AsyncConnection:
    """Pool-hygienized async connection from an :class:`AsyncEngine`.

    A connection returned from the pool may carry a pending rollback from a
    prior task that exited without cleanly closing its transaction. We issue
    a cheap rollback as reset-on-checkout to eliminate that poisoning before
    the caller opens a new transaction. On
    :class:`PendingRollbackError` / :class:`InvalidRequestError` we invalidate
    the connection and retry once with a fresh one; the wire_id is logged so
    the upstream leak can be located.

    The :func:`retry_on_transient_connect` decorator wraps this whole
    sequence so a brief :class:`OperationalError` / :class:`asyncio.TimeoutError`
    / :class:`OSError` during pool checkout (DB warming up, transient socket
    failure) does not crash a module's lifespan. On retry we close any
    half-acquired connection so we do not leak pool slots.
    """
    t0 = time.monotonic()
    try:
        conn = await engine.connect()
    except BaseException:
        wait_s = time.monotonic() - t0
        logger.info(
            "db_pool_acquire failed service=%s wait_seconds=%.4f%s",
            _SERVICE_NAME_FOR_METRICS, wait_s, _acquire_scope_suffix(),
        )
        raise
    wait_s = time.monotonic() - t0
    slow_threshold_s = resolve_slow_pool_acquire_threshold()
    if wait_s >= slow_threshold_s:
        logger.info(
            "db_pool_acquire slow service=%s wait_seconds=%.4f threshold=%.2f%s",
            _SERVICE_NAME_FOR_METRICS, wait_s, slow_threshold_s,
            _acquire_scope_suffix(),
        )
    else:
        logger.debug(
            "db_pool_acquire service=%s wait_seconds=%.4f%s",
            _SERVICE_NAME_FOR_METRICS, wait_s, _acquire_scope_suffix(),
        )
    try:
        try:
            await conn.rollback()
        except (
            PendingRollbackError,
            InvalidRequestError,
            # asyncpg raises its own exception hierarchy when a connection is
            # dead at the wire level (server terminated, DB restart).  These
            # errors bypass SQLAlchemy's wrapper so they are not caught by the
            # SA-level exceptions above.  Treat them identically: invalidate
            # the poisoned slot and acquire a fresh one.
            AsyncpgConnectionDoesNotExistError,
            AsyncpgInternalClientError,
        ) as exc:
            wire_id = id(_get_wire_identity(conn))
            logger.warning(
                "managed_transaction pool-hygiene: invalidating poisoned "
                "pooled connection wire_id=%s (%s)",
                wire_id, exc.__class__.__name__,
            )
            await conn.invalidate()
            await conn.close()
            conn = await engine.connect()
            await conn.rollback()
        return conn
    except BaseException:
        # Invalidate before close so a wire that raised an asyncpg state-machine
        # error is detached from pool bookkeeping and not handed to the next
        # consumer in a poisoned state.
        try:
            await conn.invalidate()
        except Exception as inv_exc:
            logger.warning(
                "db_pool_acquire: conn.invalidate() failed during cleanup: %s",
                inv_exc,
            )
        try:
            await conn.close()
        except Exception as close_exc:
            logger.warning(
                "db_pool_acquire: conn.close() failed during cleanup: %s",
                close_exc,
            )
        raise


@contextmanager
def sync_managed_transaction(db_resource: DbSyncResource) -> Iterator[Any]:
    """Sync re-entrant transaction manager."""
    if isinstance(db_resource, Engine):
        with db_resource.begin() as conn:
            yield conn
        return

    conn = db_resource
    wire_id = id(_get_wire_identity(conn))
    if conn.in_transaction():
        # Check for poisoned state
        if not getattr(conn, "is_active", True):
            raise DatabaseConnectionError(
                f"Cannot start nested transaction on connection {wire_id}: state is poisoned. "
                "The parent transaction must be rolled back."
            )
        with conn.begin_nested():
            yield conn
    else:
        with conn.begin():
            yield conn


@asynccontextmanager
async def managed_transaction(db_resource: Optional[DbResource]):
    """Async-native re-entrant transaction manager.

    Handles three connection scenarios:

    1. **Engine input**: Acquires a connection from the pool and starts a
       transaction with ``conn.begin()``.

    2. **Regular connection input**: If the connection is already in a
       transaction, starts a nested SAVEPOINT. Otherwise starts a new
       transaction with ``conn.begin()``.

    3. **AUTOCOMMIT connection input**: Connections with isolation_level
       AUTOCOMMIT (e.g., from :func:`pg_advisory_leadership`) have no active
       PostgreSQL transaction. Attempting ``begin_nested()`` (SAVEPOINT) on
       such connections raises ``NoActiveSQLTransactionError``. This function
       detects AUTOCOMMIT mode and uses ``begin()`` instead, starting a real
       transaction.

    The AUTOCOMMIT handling is critical for LEADER_ONLY background services
    that reuse the advisory-lock connection for database work during their
    tick cycle. See :class:`~dynastore.tools.background_service.ServiceContext`
    for details on ``lock_connection`` usage.

    Args:
        db_resource: AsyncEngine, Engine, AsyncConnection, Connection,
            AsyncSession, or Session.

    Yields:
        The connection/session ready for transactional work.

    Raises:
        ValueError: If ``db_resource`` is ``None``.
        DatabaseConnectionError: If the connection is closed or in a poisoned
            transaction state.
    """
    if db_resource is None:
        raise ValueError("Cannot start managed_transaction: db_resource is None.")
    if isinstance(db_resource, (AsyncEngine, Engine)):
        if isinstance(db_resource, AsyncEngine):
            # Connection acquisition (with pool-hygiene + transient-connect
            # retry) is delegated to :func:`_acquire_async_engine_connection`.
            # Once we hold a healthy connection, the user body runs inside
            # a regular ``conn.begin()`` block — body errors propagate without
            # retry, since DML/DQL idempotency is the caller's responsibility.
            conn = await _acquire_async_engine_connection(db_resource)
            try:
                txn_cm = conn.begin()
                await txn_cm.__aenter__()
                try:
                    yield conn
                except BaseException as exc:
                    # Drain the rollback to completion before letting the
                    # connection go back to the pool. Without this, a
                    # CancelledError fired mid-query (e.g. by a leader-
                    # loop cancellation or task timeout) propagates before
                    # SQLAlchemy's transactional ``__aexit__`` can finish
                    # the wire-level ROLLBACK; the underlying asyncpg
                    # connection then lands in pool inventory still in
                    # state 15 (IN_QUERY), and the next acquire hits the
                    # invalidate-recover path added by PR #619. ``shield``
                    # keeps the rollback awaitable alive across a re-fired
                    # cancellation on this task. See #628.
                    # Observability hook (#640): one structured WARN per
                    # shielded drain. Cancellation cascades from leader-
                    # loop / task-timeout / external cancel are the main
                    # operational reason this path fires; SQLAlchemy may
                    # also wrap the original CancelledError into a
                    # ``DBAPIError`` before it reaches here, so we log
                    # the actual exc class regardless. Key=value format
                    # matches project standard (#504/#528).
                    try:
                        _wire_id = id(_get_wire_identity(conn))
                    except Exception:
                        _wire_id = -1
                    logger.warning(
                        "managed_transaction_cancel_drain "
                        "wire_id=%s exc=%s",
                        _wire_id, exc.__class__.__name__,
                    )
                    # If the triggering exception is a dead-wire asyncpg
                    # error (server terminated, DB restart), mark the
                    # connection for eviction before attempting the drain.
                    # This ensures that even if the drain rollback fails
                    # (the wire is gone), conn.close() below removes the
                    # slot from the pool instead of returning it dirty.
                    # Detecting by cause-chain: SA wraps asyncpg errors
                    # inside DBAPIError with .orig set to the asyncpg exc.
                    _orig = getattr(exc, "orig", exc)
                    if _is_transient_asyncpg_error(_orig) or _is_transient_asyncpg_error(exc):
                        try:
                            await conn.invalidate()
                        except Exception:
                            # Best-effort eviction on a dead wire during cancel
                            # drain; conn.close() below still removes the slot.
                            pass
                    drain_fut = asyncio.ensure_future(
                        _drain_rollback_exit(txn_cm, exc),
                    )
                    try:
                        await asyncio.shield(drain_fut)
                    except asyncio.CancelledError:
                        # Re-fired cancellation during drain. Let the
                        # rollback task continue; ``conn.close()`` below
                        # will block on the same wire lock so the protocol
                        # finishes draining before the pool sees it.
                        #
                        # Edge case (#640): if a *third* cancellation
                        # arrives while the shield itself is awaiting,
                        # this except-block exits before the rollback
                        # finishes. ``drain_fut`` then races against
                        # ``conn.close()`` below; the connection-level
                        # lock serialises them but the rollback may be
                        # abandoned. Acceptable: pool-hygiene at next
                        # acquire (#619) catches the orphaned wire.
                        pass
                    raise
                else:
                    await txn_cm.__aexit__(None, None, None)
            finally:
                await conn.close()
        else:
            with db_resource.begin() as conn:
                yield conn
        return

    conn = db_resource
    async with _connection_lock_scope(conn):
        # 0. Check if connection is already closed
        is_closed = False
        wire_id = id(_get_wire_identity(conn))

        # Perform health check
        try:
            # Check common connection-closed attributes (SQLAlchemy + asyncpg)
            if (
                getattr(conn, "closed", False) is True
                or getattr(conn, "invalidated", False) is True
            ):
                is_closed = True
            elif (
                getattr(getattr(conn, "connection", None), "closed", False) is True
            ):
                is_closed = True
            elif isinstance(conn, AsyncConnection):
                # Try to access driver state safely (attribute name may vary by SQLAlchemy version)
                drv = getattr(conn, "driver_connection", None) or getattr(conn, "sync_connection", None)
                if (
                    getattr(drv, "is_closed", lambda: False)
                    if callable(getattr(drv, "is_closed", None))
                    else getattr(drv, "is_closed", False)
                ):
                    is_closed = True
                elif getattr(drv, "_closed", False):  # asyncpg internal
                    is_closed = True
        except Exception:
            # If we can't check, don't assume it's broken yet.
            pass

        if is_closed:
            raise DatabaseConnectionError(
                f"Cannot start transaction: Connection {wire_id} is closed."
            )

        # 1. Transactional State Guard (SQLAlchemy 2.0 re-entrancy)
        # We rely on the connection's own state. If it is already in a transaction,
        # we start a nested SAVEPOINT. If not, we start a new transaction.
        # We NO LONGER attempt to "fix" poisoned state by calling rollback() here,
        # because if this connection belongs to a parent context manager,
        # an explicit rollback would terminate its transaction logically but
        # leave its context manager open, causing subsequent InvalidRequestErrors.
        if isinstance(conn, (AsyncConnection, AsyncSession)):
            if conn.in_transaction():
                # SPECIAL CASE: AUTOCOMMIT connections
                # AUTOCOMMIT connections (used by pg_advisory_leadership) have no
                # active PostgreSQL transaction. SQLAlchemy's in_transaction() may
                # return True due to autobegin, but begin_nested() (SAVEPOINT) fails
                # with NoActiveSQLTransactionError. Use begin() instead.
                if _is_autocommit_connection(conn):
                    logger.debug(
                        "managed_transaction_autocommit_detected wire_id=%s",
                        wire_id,
                    )
                    # AUTOCOMMIT mode: start a real transaction with begin()
                    async with conn.begin():
                        yield conn
                    return

                # Check for poisoned state (SQLAlchemy 2.0)
                if not getattr(conn, "is_active", True):
                    raise DatabaseConnectionError(
                        f"Cannot start nested transaction on connection {wire_id}: state is poisoned. "
                        "The parent transaction must be rolled back."
                    )
                # Check the asyncpg wire-level state. SQLAlchemy's is_active only tracks
                # its own rollback; asyncpg may independently mark a transaction as aborted
                # (e.g., after a failed DDL or DML statement). If we call begin_nested() on
                # an asyncpg-aborted transaction, the SAVEPOINT statement itself fails with
                # InFailedSQLTransactionError, which poisons the outer transaction further.
                try:
                    drv = getattr(conn, "driver_connection", None) or getattr(
                        getattr(conn, "connection", None), "driver_connection", None
                    )
                    if drv is not None:
                        proto = getattr(drv, "_protocol", None)
                        if proto is not None and hasattr(proto, "_is_in_transaction"):
                            # asyncpg marks this True only if transaction is active and healthy
                            if not proto._is_in_transaction():
                                raise DatabaseConnectionError(
                                    f"Connection {wire_id} has an asyncpg-aborted transaction. "
                                    "Cannot open a nested SAVEPOINT. The outer transaction must be rolled back."
                                )
                except DatabaseConnectionError:
                    raise
                except Exception:
                    pass  # Safe: if we can't inspect, let asyncpg fail naturally

                # Manual savepoint scope so we can intercept release/rollback
                # errors. ``async with conn.begin_nested()`` hides RELEASE
                # failures behind SQLAlchemy's __aexit__ commit() path, which
                # surfaces as a cryptic PendingRollbackError when the outer
                # transaction was invalidated between enter and exit — the
                # actual poisoning happens earlier, in the nested body, via
                # a swallowed exception or explicit rollback/invalidate.
                savepoint = await conn.begin_nested()
                try:
                    yield conn
                except BaseException:
                    try:
                        await savepoint.rollback()
                    except (PendingRollbackError, InvalidRequestError):
                        # Outer tx already aborted — SAVEPOINT is gone.
                        pass
                    except Exception:
                        # asyncpg raises InFailedSQLTransactionError when the
                        # wire-level transaction is in error state and any SQL
                        # (including ROLLBACK TO SAVEPOINT) is attempted. The
                        # outer managed_transaction will issue the real ROLLBACK
                        # when it exits, so swallow this secondary failure and
                        # let the original exception propagate via `raise`.
                        pass
                    raise
                else:
                    try:
                        await savepoint.commit()
                    except (PendingRollbackError, InvalidRequestError) as exc:
                        raise DatabaseConnectionError(
                            f"Cannot release SAVEPOINT on connection {wire_id}: "
                            f"outer transaction was invalidated during the nested "
                            f"scope ({type(exc).__name__}). An earlier statement "
                            "inside the nested body poisoned the outer transaction "
                            "without propagating — check for swallowed exceptions "
                            "or explicit rollback/invalidate calls inside it."
                        ) from exc
            else:
                async with conn.begin():
                    yield conn

        else:
            assert isinstance(conn, (SAConnection, SASession))
            if conn.in_transaction():
                # SPECIAL CASE: AUTOCOMMIT connections (sync path)
                if _is_autocommit_connection(conn):
                    logger.debug(
                        "managed_transaction_autocommit_detected wire_id=%s (sync)",
                        wire_id,
                    )
                    with conn.begin():
                        yield conn
                    return

                # Check for poisoned state
                if not getattr(conn, "is_active", True):
                    raise DatabaseConnectionError(
                        f"Cannot start nested transaction on connection {wire_id}: state is poisoned. "
                        "The parent transaction must be rolled back."
                    )
                # Manual savepoint scope (sync variant of the async branch above).
                savepoint = conn.begin_nested()
                try:
                    yield conn
                except BaseException:
                    try:
                        savepoint.rollback()
                    except (PendingRollbackError, InvalidRequestError):
                        pass
                    except Exception:
                        pass  # absorb asyncpg-level rollback failure; outer tx handles it
                    raise
                else:
                    try:
                        savepoint.commit()
                    except (PendingRollbackError, InvalidRequestError) as exc:
                        raise DatabaseConnectionError(
                            f"Cannot release SAVEPOINT on connection {wire_id}: "
                            f"outer transaction was invalidated during the nested "
                            f"scope ({type(exc).__name__}). An earlier statement "
                            "inside the nested body poisoned the outer transaction "
                            "without propagating — check for swallowed exceptions "
                            "or explicit rollback/invalidate calls inside it."
                        ) from exc
            else:
                with conn.begin():
                    yield conn


@asynccontextmanager
async def managed_nested_transaction(conn: DbResource):
    """
    Standardizes nested transaction (SAVEPOINT) management.
    Uses managed_transaction internally for robustness.
    """
    async with managed_transaction(conn) as active_conn:
        yield active_conn


_T = TypeVar("_T")


async def provisioning_write_with_retry(
    engine: DbResource,
    fn: Callable[[Any], Awaitable["_T"]],
    *,
    attempts: int | None = None,
    lock_backoff: float | None = None,
) -> "_T":
    """Run ``fn(conn)`` in a short committed transaction, retrying on transient errors.

    Designed for idempotent provisioning operations that follow a long GCP API
    call window, during which the pooled connection may have been closed by
    ``idle_in_transaction_session_timeout`` or a lock-wait may have fired.

    Each retry acquires a FRESH connection from the pool — the stale or
    lock-blocked one is never reused.  Works with both async (``AsyncEngine`` /
    asyncpg) and sync (``Engine`` / psycopg2) engines via
    :func:`managed_transaction`.

    ``lock_backoff`` controls the sleep before retrying a
    ``LockNotAvailableError``: ``lock_backoff * attempt`` seconds, giving PG
    time to release the conflicting locks.  Transient connection-closed errors
    use a zero-length yield (``asyncio.sleep(0)``) — the connection is simply
    dead and a fresh one is available immediately from the pool.

    Must NOT be used for non-idempotent writes.  The caller is responsible for
    ON CONFLICT / upsert semantics.

    Configuration: Values are resolved from (1) explicit function parameters,
    else (2) the module-global ``ProvisioningRetryConfig`` defaults.
    Resolved at CALL TIME. See :mod:`connection_health_config` for details.
    """
    # Resolve config at call time so live config edits are picked up.
    cfg_attempts, cfg_lock_backoff = resolve_provisioning_retry_config()
    _attempts = attempts if attempts is not None else cfg_attempts
    _lock_backoff = lock_backoff if lock_backoff is not None else cfg_lock_backoff

    def _compute_delay(attempt: int, exc: BaseException) -> float:
        orig = getattr(exc, "orig", None)
        is_lock = _is_lock_not_available_error(exc) or (
            orig is not None and _is_lock_not_available_error(orig)
        )
        return _lock_backoff * (attempt + 1) if is_lock else 0.0

    async def _call() -> "_T":
        async with managed_transaction(engine) as conn:
            return await fn(conn)

    return await _run_with_retry_policy(
        _call,
        classify=is_transient_db_error,
        kind="provisioning_write",
        max_attempts=_attempts,
        compute_delay=_compute_delay,
        level=logging.WARNING,
        log_exhaustion=False,
    )


def run_in_event_loop(awaitable: Awaitable[R]) -> R:
    """Safely runs awaitable from sync, preventing nested loop deadlocks."""

    async def _wrapper():
        return await awaitable

    try:
        asyncio.get_running_loop()
        raise RuntimeError("Recursive loop entry detected in run_in_event_loop.")
    except RuntimeError as e:
        if "no current event loop" not in str(e):
            raise e
        if _main_app_loop and _main_app_loop.is_running():
            return asyncio.run_coroutine_threadsafe(_wrapper(), _main_app_loop).result()
    return asyncio.run(_wrapper())


async def reflect_table(schema: str, table_name: str, db_resource: DbResource) -> Table:
    def _load(sync_conn):
        return Table(table_name, _metadata, schema=schema, autoload_with=sync_conn)

    async with managed_transaction(db_resource) as conn:
        if isinstance(conn, AsyncConnection):
            return await conn.run_sync(_load)
        return _load(conn)


class BaseQuery:
    def __init__(self, sql_template, executor_class, **kwargs):
        self._executor = executor_class.from_template(sql_template, **kwargs)

    @property
    def template(self) -> str:
        return str(getattr(self._executor.query_builder_strategy, "query_template", ""))

    async def execute(self, conn: DbResource, **kwargs) -> Any:
        return await self._executor(conn, **kwargs)

    async def stream(self, conn: DbResource, **kwargs):
        if not is_async_resource(conn):
            raise TypeError("Async resources only.")
        return await self._executor.stream_async_workflow(conn, kwargs)


class DQLQuery(BaseQuery):
    def __init__(self, sql_template, *, result_handler, post_processor=None):
        super().__init__(
            sql_template,
            executor_class=DQLExecutor,
            result_handler=result_handler,
            post_processor=post_processor,
        )

    @classmethod
    def from_builder(cls, builder, **kwargs):
        inst = cls("", result_handler=kwargs.get("result_handler", ResultHandler.NONE))
        inst._executor = DQLExecutor(FunctionQueryBuilder(builder), **kwargs)
        return inst


from .ddl_inference import _infer_existence_check



class DDLQuery(BaseQuery):
    def __init__(self, sql_template, check_query=None):
        # We wrap check_query into a function that DDLExecutor can use
        existence_check: Optional[Any] = None
        if check_query:

            async def _existence_check_impl(conn, params):
                if callable(check_query):
                    # Accept either zero-arg ``check_query()`` closures (the
                    # simple, single-site pattern) or ``check_query(conn)``
                    # callables used by module-level DDLBatch sentinels where
                    # ``conn`` is not known at construction time.
                    try:
                        sig = inspect.signature(check_query)
                        needs_conn = any(
                            p.kind in (p.POSITIONAL_OR_KEYWORD, p.POSITIONAL_ONLY)
                            for p in sig.parameters.values()
                            if p.default is inspect.Parameter.empty
                        )
                    except (TypeError, ValueError):
                        needs_conn = False
                    res = check_query(conn) if needs_conn else check_query()
                    if inspect.isawaitable(res):
                        return await res
                    return res
                if isinstance(check_query, BaseQuery):
                    return await check_query.execute(conn, **params)
                # Handle raw SQL string existence check
                from .locking_tools import DQLQuery, ResultHandler

                return await DQLQuery(
                    check_query, result_handler=ResultHandler.SCALAR
                ).execute(conn, **params)

            existence_check = _existence_check_impl

        elif isinstance(sql_template, str):
            # Auto-infer existence check from CREATE DDL patterns.
            # The inferred check has signature (conn, params, raw_params)
            # where raw_params contains identifier values like schema.
            inferred = _infer_existence_check(sql_template)
            if inferred:
                existence_check = inferred
                existence_check._needs_raw_params = True

        super().__init__(
            sql_template,
            executor_class=DDLExecutor,
            existence_check=existence_check,
        )
        self.check_query = check_query

    @classmethod
    def from_builder(cls, builder, **kwargs):
        inst = cls("")
        inst._executor = DDLExecutor(FunctionQueryBuilder(builder), **kwargs)
        return inst

    async def execute(self, conn: DbResource, **kwargs):
        # Delegate entirely to the executor which now handles locking and checks
        return await super().execute(conn, **kwargs)


class DDLBatch:
    """Execute a group of DDL statements under a single sentinel check.

    On warm startup (sentinel exists), the entire batch is skipped in
    **one** DB round-trip instead of N individual existence checks.

    Usage::

        batch = DDLBatch(
            sentinel=DDLQuery("CREATE TABLE IF NOT EXISTS {schema}.my_last_table (id INT);"),
            steps=[
                DDLQuery("CREATE TABLE IF NOT EXISTS {schema}.table_a (id INT);"),
                DDLQuery("CREATE INDEX IF NOT EXISTS idx_a ON {schema}.table_a (id);"),
                DDLQuery("CREATE TABLE IF NOT EXISTS {schema}.my_last_table (id INT);"),
            ],
        )
        await batch.execute(conn, schema="myschema")

    The *sentinel* is typically the last object created in the group.
    If it already exists, all *steps* are skipped. Otherwise, each step
    is executed in order (each with its own auto-inferred existence check
    for idempotency).
    """

    def __init__(self, sentinel: DDLQuery, steps: list[DDLQuery]):
        self.sentinel = sentinel
        self.steps = steps

    async def execute(self, conn: DbResource, **kwargs):
        # Fast-path: check if sentinel object already exists.
        #
        # The sentinel SELECT is wrapped in a SAVEPOINT when the conn is
        # already inside a transaction — asyncpg leaves the connection in an
        # aborted state if the SELECT fails inside an existing tx, which
        # would poison every subsequent statement in the batch. Mirrors
        # DDLExecutor._execute_async:728-764.
        sentinel_executor = self.sentinel._executor
        if sentinel_executor.existence_check:
            sentinel_executor._raw_params = kwargs
            try:
                if isinstance(conn, (AsyncConnection, AsyncSession)) and conn.in_transaction():
                    res = False
                    async with conn.begin_nested() as sp:
                        res = await sentinel_executor._call_existence_check(conn, kwargs)
                        # Always rollback the savepoint: if the SELECT failed
                        # silently, RELEASE SAVEPOINT will throw and poison
                        # the outer tx. Explicit rollback guarantees health.
                        await sp.rollback()
                    if res:
                        return  # All DDL already applied — skip entire batch
                elif isinstance(conn, SAConnection) and conn.in_transaction():
                    res = False
                    with conn.begin_nested() as sp:
                        res = sentinel_executor._call_existence_check_sync(conn, kwargs)
                        sp.rollback()
                    if res:
                        return
                else:
                    res = await sentinel_executor._call_existence_check(conn, kwargs)
                    if res:
                        return
            except Exception as e:
                logger.warning(
                    "DDLBatch sentinel existence check failed (%s); falling through to per-step execution. "
                    "Each step still has its own existence check, but silent failures here mask root cause. "
                    "Sentinel SQL: %r",
                    e,
                    getattr(self.sentinel._executor.query_builder_strategy, "query_template", "<unknown>"),
                )

        # Cold path: execute each DDL in order
        for step in self.steps:
            await step.execute(conn, **kwargs)


class GeoDQLQuery(BaseQuery):
    def __init__(self, sql_template, *, result_handler, post_processor=None):
        super().__init__(
            sql_template,
            executor_class=GeoDQLExecutor,
            result_handler=result_handler,
            post_processor=post_processor,
        )

    @classmethod
    def from_builder(cls, builder, **kwargs):
        inst = cls("", result_handler=kwargs.get("result_handler", ResultHandler.NONE))
        inst._executor = GeoDQLExecutor(FunctionQueryBuilder(builder), **kwargs)
        return inst
