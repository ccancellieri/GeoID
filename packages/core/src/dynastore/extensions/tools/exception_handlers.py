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

"""
Pluggable exception handling system for REST API endpoints.

Provides a centralized, extensible mechanism to handle and convert application
exceptions to appropriate HTTP responses, allowing modules to register custom
exception handlers without coupling to specific services.

Integration with FastAPI:
    1. Call setup_exception_handlers(app) during app initialization
    2. Optionally decorate endpoints with @auto_handle_exceptions
    3. Or manually call handle_exception() in try-except blocks
"""

import logging
import functools
from contextlib import asynccontextmanager
from typing import Callable, ClassVar, Optional, List, Dict, Any, TypeVar, Awaitable
from fastapi import FastAPI, HTTPException, status, Response, Request
from starlette.middleware.base import BaseHTTPMiddleware, RequestResponseEndpoint
from fastapi.responses import JSONResponse
import traceback

logger = logging.getLogger(__name__)

F = TypeVar("F", bound=Callable[..., Awaitable[Any]])


class ExceptionHandler:
    """
    Base handler interface for converting application exceptions to HTTP responses.

    Handlers are checked in registration order. The first handler that returns
    a non-None result is used. If no handler matches, the exception is re-raised.
    """

    def can_handle(self, exception: Exception) -> bool:
        """Check if this handler can process the exception."""
        raise NotImplementedError

    def handle(
        self, exception: Exception, context: Optional[Dict[str, Any]] = None
    ) -> Optional[HTTPException]:
        """
        Convert the exception to an HTTPException, or return None if unable to handle.

        Args:
            exception: The exception to handle
            context: Optional context dict with keys like 'resource_name', 'resource_id', 'operation'

        Returns:
            HTTPException or Response with appropriate status code and detail, or None to defer to next handler
        """
        raise NotImplementedError


class ProblemExceptionHandler(ExceptionHandler):
    """Surfaces ``ProblemException`` (RFC 9457) with its explicit status.

    Without this, the global middleware demotes every 4xx ``ProblemException``
    raised by the configs extension to a generic 500 because the registry
    falls through to its default ``HTTPException(500, "An unexpected error
    occurred.")`` fallback. The configs extension's ``problem_exception_handler``
    (registered with FastAPI directly) is bypassed because the middleware
    catches the exception before FastAPI's per-class dispatch can fire.
    """

    def can_handle(self, exception: Exception) -> bool:
        from dynastore.extensions.tools.problem_details import ProblemException

        return isinstance(exception, ProblemException)

    def handle(
        self, exception: Exception, context: Optional[Dict[str, Any]] = None
    ) -> Optional[HTTPException]:
        from dynastore.extensions.tools.problem_details import ProblemException

        assert isinstance(exception, ProblemException)
        problem = exception.problem
        return HTTPException(
            status_code=problem.status,
            detail=problem.detail or problem.title,
        )


class ConflictExceptionHandler(ExceptionHandler):
    """Handles database constraint violations (unique, foreign key, duplicate)."""

    def can_handle(self, exception: Exception) -> bool:
        from dynastore.modules.db_config.exceptions import is_conflict_error

        return is_conflict_error(exception)

    def handle(
        self, exception: Exception, context: Optional[Dict[str, Any]] = None
    ) -> Optional[HTTPException]:
        from sqlalchemy.exc import IntegrityError

        from dynastore.extensions.tools.conflict_handler import conflict_to_409
        from dynastore.modules.db_config.exceptions import DatabaseError

        context = context or {}
        resource_name = context.get("resource_name", "Resource")
        resource_id = context.get("resource_id")
        assert isinstance(exception, (DatabaseError, IntegrityError))
        return conflict_to_409(
            exception, resource_name=resource_name, resource_id=resource_id
        )


class ProgrammingErrorHandler(ExceptionHandler):
    """
    Handles low-level programming errors that indicate a server-side issue,
    like KeyError, AttributeError, or TypeError, which often point to
    misconfiguration or bugs.
    """

    def can_handle(self, exception: Exception) -> bool:
        # Catches common developer errors that shouldn't leak as raw tracebacks.
        return isinstance(exception, (KeyError, AttributeError, TypeError))

    def handle(
        self, exception: Exception, context: Optional[Dict[str, Any]] = None
    ) -> Optional[HTTPException]:
        err_type = exception.__class__.__name__
        operation = (context or {}).get("operation", "the requested operation")
        # Surface the correlation id in the 5xx body so operators can
        # grep the structured log line tied to this exact request.
        # Falls back to "(no correlation id)" when middleware didn't set
        # one (e.g. internal task path); the textual marker is itself
        # greppable.
        from dynastore.tools.correlation import get_correlation_id
        cid = get_correlation_id() or "(no correlation id)"
        detail = (
            f"Could not complete {operation} due to an internal server "
            f"configuration error. Please contact the administrator. "
            f"(Reference: {err_type}; correlation_id={cid})"
        )

        logger.error(
            f"Internal Programming Error (500): {err_type}: {str(exception)} during {operation}",
            exc_info=True,
        )

        return HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=detail
        )


class DatabaseInputExceptionHandler(ExceptionHandler):
    """
    Handles database errors that are likely due to invalid user input,
    such as referencing non-existent columns in filters.
    """

    def can_handle(self, exception: Exception) -> bool:
        # Check for SQLAlchemy ProgrammingError
        name = exception.__class__.__name__
        if name == "ProgrammingError" or name == "UndefinedColumnError":
            msg = str(exception).lower()
            return "column" in msg and "does not exist" in msg
        return False

    def handle(
        self, exception: Exception, context: Optional[Dict[str, Any]] = None
    ) -> Optional[HTTPException]:
        # Clean up the error message for the user
        msg = str(exception)

        return HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Unknown property in filter: {msg}",
        )


def _extract_pgcode(exception: Exception) -> Optional[str]:
    """Extract the raw PostgreSQL error code from a DatabaseError without leaking str(exception).

    Walks the ``original_exception`` chain the same way ``get_conflict_context``
    does, but avoids calling ``str()`` on any exception along the path so no
    physical schema/table names are touched.
    """
    original = getattr(exception, "original_exception", None)
    if original is None:
        return None
    # asyncpg: ``pgcode`` / ``sqlstate`` on the raw error object
    pgcode = getattr(original, "pgcode", None) or getattr(original, "sqlstate", None)
    if pgcode:
        return str(pgcode)
    # SQLAlchemy wraps asyncpg/psycopg2 as ``.orig``
    orig_inner = getattr(original, "orig", None)
    if orig_inner is not None:
        pgcode = getattr(orig_inner, "pgcode", None) or getattr(orig_inner, "sqlstate", None)
        if pgcode:
            return str(pgcode)
    return None


class TableNotFoundExceptionHandler(ExceptionHandler):
    """Maps ``TableNotFoundError`` (pgcode 42P01) to a clean HTTP 404.

    A collection can be fully registered in the catalog/STAC index (served
    from Elasticsearch) while its PostgreSQL feature table was never
    persisted — e.g. an ingest job that OOM-crashed and rolled back before
    the table-create transaction committed. Read surfaces that query the
    PG hub table directly (maps render, vector tiles) then hit a raw
    ``UndefinedTableError`` from asyncpg. Without this handler that bubbles
    to the generic ``DatabaseErrorHandler`` below as an opaque 500.

    The missing table means "nothing to render yet for this collection",
    not a server malfunction, so this is surfaced as 404 with an actionable
    message instead. Registered ahead of ``DatabaseErrorHandler`` so this
    more specific mapping wins.
    """

    def can_handle(self, exception: Exception) -> bool:
        from dynastore.modules.db_config.exceptions import TableNotFoundError

        return isinstance(exception, TableNotFoundError)

    def handle(
        self, exception: Exception, context: Optional[Dict[str, Any]] = None
    ) -> Optional[HTTPException]:
        from dynastore.tools.correlation import get_correlation_id

        context = context or {}
        collection_id = context.get("collection_id") or context.get("resource_id")
        catalog_id = context.get("catalog_id")
        cid = get_correlation_id() or "(no correlation id)"

        # Full exception (may embed the physical table name) logged server-side only.
        logger.error(
            "Missing PostgreSQL feature table (catalog=%s, collection=%s, "
            "correlation_id=%s): %s",
            catalog_id, collection_id, cid, str(exception), exc_info=True,
        )

        if collection_id:
            detail = (
                f"Collection '{collection_id}' has no PostgreSQL feature table; "
                f"ingest has not populated a renderable store."
            )
        else:
            detail = (
                "The requested collection has no PostgreSQL feature table; "
                "ingest has not populated a renderable store."
            )
        return HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=detail)


class PoolSaturationExceptionHandler(ExceptionHandler):
    """Maps ``PoolSaturationError`` (DB pool-acquire timeout) to HTTP 503.

    Raised by ``query_executor._acquire_async_engine_connection`` when the
    bounded pool-acquire wait (``DBConfig.pool_acquire_timeout``, #1894)
    elapses before a free connection becomes available. Without this
    handler the exception falls through to the generic ``DatabaseErrorHandler``
    below as an opaque 500. Instead this maps it to HTTP 503 + Retry-After —
    "the pool is momentarily saturated, back off and retry" rather than
    "something broke". Registered ahead of ``DatabaseErrorHandler`` so this
    more specific mapping wins.
    """

    def can_handle(self, exception: Exception) -> bool:
        from dynastore.modules.db_config.exceptions import PoolSaturationError

        return isinstance(exception, PoolSaturationError)

    def handle(
        self, exception: Exception, context: Optional[Dict[str, Any]] = None
    ) -> Optional[HTTPException]:
        from dynastore.modules.db_config.exceptions import PoolSaturationError

        assert isinstance(exception, PoolSaturationError)
        logger.warning("Database pool saturated: %s", exception)
        return HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Database connection pool saturated, try again shortly.",
            headers={"Retry-After": str(exception.retry_after)},
        )


class DatabaseErrorHandler(ExceptionHandler):
    """Handles database errors with a sanitized HTTP response.

    Logs the full ``str(exception)`` (which may embed raw asyncpg/psycopg2
    messages containing internal physical schema/table names such as
    ``c_<hash>`` and ``items_<hash>``) server-side only.  The HTTP 500 detail
    is limited to: the exception type name (a safe error category), the bare
    PostgreSQL error code when available (a standard identifier with no physical
    names), and the correlation-id so operators can cross-reference the server
    log for the full message.
    """

    def can_handle(self, exception: Exception) -> bool:
        # Import locally to avoid circular dependencies
        from dynastore.modules.db_config.exceptions import DatabaseError
        return isinstance(exception, DatabaseError)

    def handle(
        self, exception: Exception, context: Optional[Dict[str, Any]] = None
    ) -> Optional[HTTPException]:
        from dynastore.tools.correlation import get_correlation_id

        context = context or {}
        resource_name = context.get("resource_name", "Resource")
        operation = context.get("operation", "operation")
        resource_id = context.get("resource_id")

        cid = get_correlation_id() or "(no correlation id)"

        # Log the FULL exception string server-side (may contain physical names).
        logger.error(
            "Database error during %s on %s: %s (correlation_id=%s, resource_id=%s)",
            operation,
            resource_name,
            str(exception),
            cid,
            resource_id,
            exc_info=True,
        )

        # Build a sanitized HTTP detail: safe category + pgcode + correlation-id.
        # Physical schema/table names from the raw PG message are deliberately omitted.
        err_type = exception.__class__.__name__
        pgcode = _extract_pgcode(exception)
        pgcode_hint = f", pgcode={pgcode}" if pgcode else ""
        sanitized_detail = (
            f"Database error during {operation}"
            f" ({err_type}{pgcode_hint}; correlation_id={cid})"
        )

        return HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=sanitized_detail,
        )


class ValidationExceptionHandler(ExceptionHandler):
    """Handles validation errors (ValueError, Pydantic ValidationError)."""

    def can_handle(self, exception: Exception) -> bool:
        from pydantic import ValidationError

        return isinstance(exception, (ValueError, ValidationError))

    def handle(
        self, exception: Exception, context: Optional[Dict[str, Any]] = None
    ) -> Optional[HTTPException]:
        from dynastore.tools.db import InvalidIdentifierError

        context = context or {}
        resource_id = context.get("resource_id", "")
        operation = context.get("operation", "Operation")

        # #1191: an unsubstituted ``{{...}}`` template placeholder in a resource
        # id is a malformed request, never a real lookup. Reject it as a 400 with
        # an actionable message — covering both the typed InvalidIdentifierError
        # raised by ``validate_sql_identifier`` at a guarded boundary, and a raw
        # ``"<id> not found"`` ValueError from a route that let the literal token
        # fall through to the resolver as a lookup miss. This must win over the
        # ``"not found"`` → 404 downgrade below (an opaque 404 is exactly the
        # "routed-resolve unavailable" symptom we want to replace).
        msg = str(exception)
        if isinstance(exception, InvalidIdentifierError) or ("{{" in msg and "}}" in msg):
            detail = (
                f"{operation} failed: resource id contains an unsubstituted "
                f"template placeholder ('{{{{...}}}}'); substitute it with a real "
                f"value before issuing the request. ({msg})"
            )
            logger.warning(detail)
            return HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST, detail=detail
            )

        if resource_id:
            detail = (
                f"{operation} failed validation for '{resource_id}': {str(exception)}"
            )
        else:
            detail = f"{operation} failed validation: {str(exception)}"

        logger.warning(detail)
        # ValueError("X 'id' not found.") is a NotFound, not a validation
        # failure — services raise it for missing catalogs/collections from
        # service-layer call sites that haven't migrated to a typed
        # NotFoundError yet. 422 confuses OGC clients (e.g. WFS expects 404
        # for a missing typename); detect the conventional message and
        # downgrade to 404.
        msg_lower = str(exception).lower()
        if "not found" in msg_lower:
            return HTTPException(
                status_code=status.HTTP_404_NOT_FOUND, detail=detail
            )
        return HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail=detail
        )


class ConstraintViolationExceptionHandler(ExceptionHandler):
    """Maps PG schema-level constraint violations to HTTP 422.

    Pre-#200, every IntegrityError was greedy-mapped to 409 by
    ``ConflictExceptionHandler`` regardless of its pgcode — masking
    NOT NULL / CHECK violations behind a misleading "duplicate" status.
    With ``is_conflict_error`` now restricted to genuine conflict
    pgcodes (23505, 23503, 42710), this handler claims the rest of the
    DB-schema-validation family and emits 422.

    Registered ahead of ``ValidationExceptionHandler`` so its richer
    detail body (with pgcode hint) takes precedence over the generic
    ``ValueError`` mapping.
    """

    def can_handle(self, exception: Exception) -> bool:
        from dynastore.modules.db_config.exceptions import (
            CheckViolationError,
            NotNullViolationError,
        )

        return isinstance(exception, (NotNullViolationError, CheckViolationError))

    def handle(
        self, exception: Exception, context: Optional[Dict[str, Any]] = None
    ) -> Optional[HTTPException]:
        from dynastore.modules.db_config.exceptions import NotNullViolationError

        kind = "NOT NULL" if isinstance(exception, NotNullViolationError) else "CHECK"
        return HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail={
                "message": f"{kind} constraint violation: {exception}",
                "constraint_kind": kind,
            },
        )


class UnknownFieldsExceptionHandler(ExceptionHandler):
    """Maps ``UnknownFieldsError`` to HTTP 422 with structured offender list.

    Raised by ``item_service.upsert`` when a write batch carries properties
    not declared in ``ItemsSchema.fields`` and ``strict_unknown_fields``
    is enabled.
    """

    def can_handle(self, exception: Exception) -> bool:
        from dynastore.modules.storage.errors import UnknownFieldsError

        return isinstance(exception, UnknownFieldsError)

    def handle(
        self, exception: Exception, context: Optional[Dict[str, Any]] = None
    ) -> Optional[HTTPException]:
        return HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail={
                "message": str(exception),
                "unknown_fields": list(getattr(exception, "unknown_fields", [])),
                "allowed_fields": list(getattr(exception, "allowed_fields", [])),
            },
        )


class IndexMappingMismatchExceptionHandler(ExceptionHandler):
    """Maps ``IndexMappingMismatchError`` to HTTP 503 + Retry-After.

    Raised when an ES write fails with ``illegal_argument_exception``
    because the live index mapping is missing a field the writer just
    sent. The request payload is well-formed; operator action (re-roll
    the index / reindex) fixes it. 503 + Retry-After tells clients
    "transient — try again later" instead of demoting to a generic 500.
    """

    def can_handle(self, exception: Exception) -> bool:
        from dynastore.modules.storage.errors import IndexMappingMismatchError

        return isinstance(exception, IndexMappingMismatchError)

    def handle(
        self, exception: Exception, context: Optional[Dict[str, Any]] = None
    ) -> Optional[HTTPException]:
        return HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail={
                "type": "https://docs.dynastore.io/errors/index-mapping-mismatch",
                "title": "Index mapping is out of date",
                "status": status.HTTP_503_SERVICE_UNAVAILABLE,
                "detail": str(exception),
                "index": getattr(exception, "index", None),
                "field": getattr(exception, "field", None),
            },
            headers={"Retry-After": "60"},
        )


class AssetSidecarRejectedExceptionHandler(ExceptionHandler):
    """Maps ``AssetSidecarRejectedError`` to HTTP 409 with structured body.

    Raised by the asset write-policy chain in
    :mod:`dynastore.modules.catalog.asset_distributed` when an incoming asset
    matches an existing one and the resolved :class:`AssetsWritePolicy` says
    ``REFUSE_FAIL`` (or when ``NEW_VERSION`` is requested without driver
    versioning). The body shape follows the structured-error convention used
    by other 409 paths so REST clients can react uniformly:

    * ``asset_id`` — the incoming payload's id
    * ``matcher`` — which matcher hit (``asset_id`` / ``filename`` /
      ``metadata_field`` / ``url`` / ``content_hash``)
    * ``reason`` — short machine code (``conflict`` /
      ``versioning_unsupported``)
    * ``detail`` — the human-readable message
    * ``existing_id`` — the id of the existing row that triggered the
      rejection (``None`` when no probe matched and the rejection comes from
      a defensive branch).
    """

    def can_handle(self, exception: Exception) -> bool:
        from dynastore.modules.catalog.asset_distributed import (
            AssetSidecarRejectedError,
        )

        return isinstance(exception, AssetSidecarRejectedError)

    def handle(
        self, exception: Exception, context: Optional[Dict[str, Any]] = None
    ) -> Optional[HTTPException]:
        from dynastore.modules.catalog.asset_distributed import (
            AssetSidecarRejectedError,
        )

        assert isinstance(exception, AssetSidecarRejectedError)
        return HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail={
                "type": "https://docs.dynastore.io/errors/asset-conflict",
                "title": "Asset write rejected",
                "status": status.HTTP_409_CONFLICT,
                "asset_id": exception.asset_id,
                "matcher": exception.matcher,
                "reason": exception.reason,
                "detail": exception.message,
                "existing_id": exception.existing_id,
            },
        )


class CollectionNotAliveExceptionHandler(ExceptionHandler):
    """Maps ``CollectionNotAliveError`` to a lifecycle-aware HTTP status.

    Raised by the write-path liveness gate when a request targets a
    collection that can no longer accept writes:

    * ``missing``      → 404 — no registry row (hard-deleted or never created)
    * ``tombstoned``   → 410 — soft-deleted; the resource existed and is gone
    * ``provisioning`` → 409 + Retry-After — async init in flight; transient
    * ``deleting``     → 409 + Retry-After — hard-delete purge in flight; transient
    * anything else    → 503 — the gate failed closed (e.g. ``lookup-error``);
      the collection state could not be established, so the write is
      refused as transient rather than misreported as a client error.

    The transitional reasons carry a ``Retry-After`` header (#2066): the state
    is temporary and the same request should succeed once the window closes,
    so clients can back off rather than treat it as a terminal client error.
    """

    # Transitional (in-flight) states the caller should retry, with their
    # Retry-After hint in seconds.  The window is normally sub-second for
    # provisioning and bounded by the purge duration for deleting.
    _RETRYABLE_REASONS: ClassVar[Dict[str, str]] = {"provisioning": "5", "deleting": "5"}

    def can_handle(self, exception: Exception) -> bool:
        from dynastore.modules.catalog.collection_service import (
            CollectionNotAliveError,
        )

        return isinstance(exception, CollectionNotAliveError)

    def handle(
        self, exception: Exception, context: Optional[Dict[str, Any]] = None
    ) -> Optional[HTTPException]:
        from dynastore.modules.catalog.collection_service import (
            CollectionNotAliveError,
        )

        assert isinstance(exception, CollectionNotAliveError)
        status_by_reason = {
            "missing": status.HTTP_404_NOT_FOUND,
            "tombstoned": status.HTTP_410_GONE,
            "provisioning": status.HTTP_409_CONFLICT,
            "deleting": status.HTTP_409_CONFLICT,
        }
        status_code = status_by_reason.get(
            exception.reason, status.HTTP_503_SERVICE_UNAVAILABLE
        )
        headers: Optional[Dict[str, str]] = None
        retry_after = self._RETRYABLE_REASONS.get(exception.reason)
        if retry_after is not None:
            headers = {"Retry-After": retry_after}
        return HTTPException(
            status_code=status_code,
            detail={
                "type": "https://docs.dynastore.io/errors/collection-not-alive",
                "title": "Collection cannot accept writes",
                "status": status_code,
                "catalog_id": exception.catalog_id,
                "collection_id": exception.collection_id,
                "reason": exception.reason,
                "detail": str(exception),
            },
            headers=headers,
        )


class JobLockedExceptionHandler(ExceptionHandler):
    """Maps ``JobLockedError`` to HTTP 423 Locked.

    Raised by ``ExecutionEngine.update_job`` when the job has already
    left the CREATED state and its inputs can no longer be changed.
    """

    def can_handle(self, exception: Exception) -> bool:
        from dynastore.modules.tasks.exceptions import JobLockedError

        return isinstance(exception, JobLockedError)

    def handle(
        self, exception: Exception, context: Optional[Dict[str, Any]] = None
    ) -> Optional[HTTPException]:
        return HTTPException(status_code=423, detail=str(exception))


class JobStateConflictExceptionHandler(ExceptionHandler):
    """Maps ``JobStateConflictError`` to HTTP 409 Conflict.

    Raised by ``ExecutionEngine.start_job`` and ``ExecutionEngine.dismiss_job``
    when the job's current status does not permit the requested transition
    (e.g. starting an already-running job or dismissing a terminal one).
    """

    def can_handle(self, exception: Exception) -> bool:
        from dynastore.modules.tasks.exceptions import JobStateConflictError

        return isinstance(exception, JobStateConflictError)

    def handle(
        self, exception: Exception, context: Optional[Dict[str, Any]] = None
    ) -> Optional[HTTPException]:
        return HTTPException(status_code=409, detail=str(exception))


class ImmutableConfigExceptionHandler(ExceptionHandler):
    """Handles immutable configuration modification attempts."""

    def can_handle(self, exception: Exception) -> bool:
        from dynastore.modules.db_config.exceptions import ImmutableConfigError

        return isinstance(exception, ImmutableConfigError)

    def handle(
        self, exception: Exception, context: Optional[Dict[str, Any]] = None
    ) -> Optional[HTTPException]:
        return HTTPException(
            status_code=status.HTTP_409_CONFLICT, detail=str(exception)
        )


class PluginNotFoundExceptionHandler(ExceptionHandler):
    """Handles plugin not found errors."""

    def can_handle(self, exception: Exception) -> bool:
        from dynastore.modules.db_config.exceptions import PluginNotRegisteredError

        return isinstance(exception, PluginNotRegisteredError)

    def handle(
        self, exception: Exception, context: Optional[Dict[str, Any]] = None
    ) -> Optional[HTTPException]:
        return HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail=str(exception)
        )


class ConfigValidationExceptionHandler(ExceptionHandler):
    """Handles configuration validation errors."""

    def can_handle(self, exception: Exception) -> bool:
        from dynastore.modules.db_config.exceptions import ConfigValidationError

        return isinstance(exception, ConfigValidationError)

    def handle(
        self, exception: Exception, context: Optional[Dict[str, Any]] = None
    ) -> Optional[HTTPException]:
        return HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST, detail=str(exception)
        )


class ServiceUnavailableExceptionHandler(ExceptionHandler):
    """Maps ``ServiceUnavailableError`` (e.g. DB engine not ready) to HTTP 503.

    Registered ahead of the generic ``ValidationExceptionHandler`` so the
    core preset layer can signal infrastructure unavailability without
    coupling to FastAPI.
    """

    def can_handle(self, exception: Exception) -> bool:
        from dynastore.modules.storage.presets.errors import ServiceUnavailableError

        return isinstance(exception, ServiceUnavailableError)

    def handle(
        self, exception: Exception, context: Optional[Dict[str, Any]] = None
    ) -> Optional[HTTPException]:
        from dynastore.modules.storage.presets.errors import ServiceUnavailableError

        assert isinstance(exception, ServiceUnavailableError)
        return HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=exception.detail,
        )


class PresetConflictExceptionHandler(ExceptionHandler):
    """Maps ``PresetConflictError`` to HTTP 409 Conflict.

    Detail may be a plain string or a structured dict — both are preserved
    verbatim in the ``HTTPException.detail`` field.
    """

    def can_handle(self, exception: Exception) -> bool:
        from dynastore.modules.storage.presets.errors import PresetConflictError

        return isinstance(exception, PresetConflictError)

    def handle(
        self, exception: Exception, context: Optional[Dict[str, Any]] = None
    ) -> Optional[HTTPException]:
        from dynastore.modules.storage.presets.errors import PresetConflictError

        assert isinstance(exception, PresetConflictError)
        return HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=exception.detail,
        )


class PresetOperationExceptionHandler(ExceptionHandler):
    """Maps ``PresetOperationError`` (apply/revoke failure) to HTTP 500.

    The structured ``detail`` dict is passed through as-is so the operator
    can see both the human message and the truncated error string.
    """

    def can_handle(self, exception: Exception) -> bool:
        from dynastore.modules.storage.presets.errors import PresetOperationError

        return isinstance(exception, PresetOperationError)

    def handle(
        self, exception: Exception, context: Optional[Dict[str, Any]] = None
    ) -> Optional[HTTPException]:
        from dynastore.modules.storage.presets.errors import PresetOperationError

        assert isinstance(exception, PresetOperationError)
        return HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=exception.detail,
        )


class PresetNotFoundExceptionHandler(ExceptionHandler):
    """Maps ``PresetNotFoundError`` to HTTP 404 Not Found.

    Preserves the exact detail string from the lifecycle layer without
    wrapping it in the generic ``ValidationExceptionHandler`` template.
    """

    def can_handle(self, exception: Exception) -> bool:
        from dynastore.modules.storage.presets.errors import PresetNotFoundError

        return isinstance(exception, PresetNotFoundError)

    def handle(
        self, exception: Exception, context: Optional[Dict[str, Any]] = None
    ) -> Optional[HTTPException]:
        from dynastore.modules.storage.presets.errors import PresetNotFoundError

        assert isinstance(exception, PresetNotFoundError)
        return HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=exception.detail,
        )


class ConfigResolutionExceptionHandler(ExceptionHandler):
    """Handles ``ConfigResolutionError`` — waterfall produced no usable default.

    This is a system/ops misconfiguration (500, not 4xx): a deploy/bootstrap
    step is missing — either register a platform default or supply mandatory
    fields at a higher scope. The structured body tells ops exactly which key
    and which fields.
    """

    def can_handle(self, exception: Exception) -> bool:
        from dynastore.modules.db_config.exceptions import ConfigResolutionError

        return isinstance(exception, ConfigResolutionError)

    def handle(
        self, exception: Exception, context: Optional[Dict[str, Any]] = None
    ) -> Optional[HTTPException]:
        from dynastore.modules.db_config.exceptions import ConfigResolutionError

        assert isinstance(exception, ConfigResolutionError)
        logger.error(
            "Config waterfall exhausted for '%s' (required=%s, tried=%s): %s",
            exception.missing_key,
            exception.required_fields,
            exception.scope_tried,
            exception,
        )
        return HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail={
                "type": "about:blank#config-resolution",
                "title": "Configuration waterfall produced no usable default",
                "status": 500,
                "detail": str(exception),
                "missing_key": exception.missing_key,
                "required_fields": exception.required_fields,
                "scope_tried": exception.scope_tried,
                "hint": exception.hint,
            },
        )


class GcpServiceUnavailableExceptionHandler(ExceptionHandler):
    """Maps ``GcpServiceUnavailableError`` to HTTP 503.

    Optionally forwards ``retry_after`` as a ``Retry-After`` response header
    when the caller knows the back-off window.
    """

    def can_handle(self, exception: Exception) -> bool:
        from dynastore.modules.gcp.errors import GcpServiceUnavailableError

        return isinstance(exception, GcpServiceUnavailableError)

    def handle(
        self, exception: Exception, context: Optional[Dict[str, Any]] = None
    ) -> Optional[HTTPException]:
        from dynastore.modules.gcp.errors import GcpServiceUnavailableError

        assert isinstance(exception, GcpServiceUnavailableError)
        headers = {}
        if exception.retry_after is not None:
            headers["Retry-After"] = str(exception.retry_after)
        return HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=str(exception),
            headers=headers or None,
        )


class GcpFailedDependencyExceptionHandler(ExceptionHandler):
    """Maps ``GcpFailedDependencyError`` to HTTP 424 (Failed Dependency)."""

    def can_handle(self, exception: Exception) -> bool:
        from dynastore.modules.gcp.errors import GcpFailedDependencyError

        return isinstance(exception, GcpFailedDependencyError)

    def handle(
        self, exception: Exception, context: Optional[Dict[str, Any]] = None
    ) -> Optional[HTTPException]:
        return HTTPException(
            status_code=424,
            detail=str(exception),
        )


class GcpStorageDeferredExceptionHandler(ExceptionHandler):
    """Maps ``GcpStorageDeferredError`` to HTTP 409 (Conflict)."""

    def can_handle(self, exception: Exception) -> bool:
        from dynastore.modules.gcp.errors import GcpStorageDeferredError

        return isinstance(exception, GcpStorageDeferredError)

    def handle(
        self, exception: Exception, context: Optional[Dict[str, Any]] = None
    ) -> Optional[HTTPException]:
        return HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=str(exception),
        )


class GcpInternalErrorHandler(ExceptionHandler):
    """Maps ``GcpInternalError`` to HTTP 500 with the error message preserved.

    Used for internal consistency failures within the GCP module that are
    genuinely unexpected (e.g. GCS returns no session URI despite a
    successful API call, or storage metadata is inconsistent with readiness
    state).
    """

    def can_handle(self, exception: Exception) -> bool:
        from dynastore.modules.gcp.errors import GcpInternalError

        return isinstance(exception, GcpInternalError)

    def handle(
        self, exception: Exception, context: Optional[Dict[str, Any]] = None
    ) -> Optional[HTTPException]:
        logger.error(
            "GcpInternalError (500): %s", str(exception), exc_info=True
        )
        return HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(exception),
        )


class ExceptionHandlerRegistry:
    """Central registry for managing pluggable exception handlers."""

    def __init__(self):
        self._handlers: List[ExceptionHandler] = []
        self._register_builtin_handlers()

    def _register_builtin_handlers(self):
        """Register built-in handlers in order of specificity."""
        # Register in specific → generic order

        # ProblemException carries an explicit RFC 9457 status — must run
        # first so its 4xx isn't shadowed by a more generic handler downstream.
        self.register(ProblemExceptionHandler())
        # Tasks domain: job-state transitions (423 Locked, 409 Conflict) must
        # run before the generic ConflictExceptionHandler so their distinct
        # status codes are preserved.
        self.register(JobLockedExceptionHandler())
        self.register(JobStateConflictExceptionHandler())
        self.register(ConflictExceptionHandler())
        self.register(GcpServiceUnavailableExceptionHandler())
        self.register(GcpFailedDependencyExceptionHandler())
        self.register(GcpStorageDeferredExceptionHandler())
        self.register(ConstraintViolationExceptionHandler())
        self.register(UnknownFieldsExceptionHandler())
        self.register(IndexMappingMismatchExceptionHandler())
        self.register(AssetSidecarRejectedExceptionHandler())
        self.register(CollectionNotAliveExceptionHandler())  # 404/410/503 — write gate
        self.register(ImmutableConfigExceptionHandler())
        self.register(PluginNotFoundExceptionHandler())
        self.register(ConfigResolutionExceptionHandler())  # 500 — ops misconfig
        self.register(ConfigValidationExceptionHandler())
        # Preset lifecycle domain exceptions (core layer, no FastAPI import)
        self.register(ServiceUnavailableExceptionHandler())  # 503 — infra not ready
        self.register(PresetConflictExceptionHandler())  # 409 — preset state conflict
        self.register(PresetOperationExceptionHandler())  # 500 — apply/revoke failure
        self.register(PresetNotFoundExceptionHandler())  # 404 — no audit row found
        self.register(
            ProgrammingErrorHandler()
        )  # Catch programming errors before generic validation
        self.register(GcpInternalErrorHandler())  # GcpInternalError → 500 with message preserved
        self.register(DatabaseInputExceptionHandler())  # Catch DB input errors (400)
        self.register(TableNotFoundExceptionHandler())  # 404 — missing PG hub table, not a server error
        self.register(PoolSaturationExceptionHandler())  # 503 — bounded pool-acquire timeout (#1894)
        self.register(DatabaseErrorHandler())  # Surface original DB exception details for better debugging
        self.register(ValidationExceptionHandler())  # Generic - must be last

    def register(self, handler: ExceptionHandler, prepend: bool = False) -> None:
        """
        Register a new exception handler.

        Args:
            handler: The handler instance to register
            prepend: If True, add to front of list for higher priority
        """
        if prepend:
            self._handlers.insert(0, handler)
            logger.debug(
                f"Registered exception handler (priority): {handler.__class__.__name__}"
            )
        else:
            self._handlers.append(handler)
            logger.debug(f"Registered exception handler: {handler.__class__.__name__}")

    def handle(
        self,
        exception: Exception,
        context: Optional[Dict[str, Any]] = None,
        reraise_unhandled: bool = True,
    ) -> HTTPException:
        """
        Process exception through registered handlers.

        Args:
            exception: The exception to handle
            context: Optional context dict with keys like 'resource_name', 'resource_id', 'operation'
            reraise_unhandled: If True, re-raise if no handler matches

        Returns:
            HTTPException or Response with appropriate status code and detail

        Raises:
            Exception: The original exception if no handler matches and reraise_unhandled=True
        """
        context = context or {}

        # Try each handler in order
        for handler in self._handlers:
            try:
                if handler.can_handle(exception):
                    result = handler.handle(exception, context)
                    if result:
                        logger.debug(
                            f"Exception handled by {handler.__class__.__name__}"
                        )
                        return result
            except Exception as e:
                logger.error(
                    f"Error in exception handler {handler.__class__.__name__}: {e}",
                    exc_info=True,
                )
                continue

        # No handler matched
        if reraise_unhandled:
            raise exception

        # Fallback to generic 500
        logger.error(f"Unhandled exception: {exception}", exc_info=True)
        return HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="An unexpected error occurred.",
        )


# Global singleton registry
_global_registry = ExceptionHandlerRegistry()


def register_handler(handler: ExceptionHandler, prepend: bool = False) -> None:
    """Register a custom exception handler globally."""
    _global_registry.register(handler, prepend=prepend)


def handle_exception(
    exception: Exception,
    resource_name: Optional[str] = None,
    resource_id: Optional[str] = None,
    operation: Optional[str] = None,
    reraise_unhandled: bool = True,
) -> HTTPException:
    """
    Handle an exception using registered handlers.

    This is the main entry point for service endpoints.

    Args:
        exception: The exception to handle
        resource_name: Name of the resource (e.g., "Catalog", "Collection")
        resource_id: ID/code of the resource (optional)
        operation: Operation being performed (e.g., "Catalog creation")
        reraise_unhandled: If True, re-raise if no handler matches

    Returns:
        HTTPException or Response with appropriate status code and detail

    Example:
        try:
            result = await catalog_manager.create_catalog(data)
        except Exception as e:
            raise handle_exception(
                e,
                resource_name="Catalog",
                resource_id=data.get("id"),
                operation="Catalog creation"
            )
    """
    context = {}
    if resource_name:
        context["resource_name"] = resource_name
    if resource_id:
        context["resource_id"] = resource_id
    if operation:
        context["operation"] = operation

    return _global_registry.handle(
        exception, context=context, reraise_unhandled=reraise_unhandled
    )


def handle_or_raise(
    exception: Exception,
    resource_name: Optional[str] = None,
    resource_id: Optional[str] = None,
    operation: Optional[str] = None,
) -> Response:
    """Handle an exception, raising HTTPExceptions and returning Response bodies.

    Convenience wrapper over :func:`handle_exception` for the common service
    ``except`` block: an ``HTTPException`` result is raised (so FastAPI's
    handlers map it), while a ``Response`` result (e.g. a structured error body)
    is returned to the client. Use as the block's final statement::

        except Exception as e:
            return handle_or_raise(e, resource_name="Catalog", operation="create")
    """
    result = handle_exception(
        exception,
        resource_name=resource_name,
        resource_id=resource_id,
        operation=operation,
    )
    if isinstance(result, HTTPException):
        raise result
    return result


class GlobalExceptionHandlingMiddleware(BaseHTTPMiddleware):
    """
    A middleware that wraps the entire application stack to catch any unhandled
    exception that occurs, including those from other middleware. It then uses
    the application's centralized `generic_exception_handler` to ensure a
    consistent, managed error response is returned.
    """

    async def dispatch(
        self, request: Request, call_next: RequestResponseEndpoint
    ) -> Response:
        try:
            return await call_next(request)
        except Exception as exc:
            return await generic_exception_handler(request, exc)


def auto_handle_exceptions(
    resource_name: Optional[str] = None,
    resource_id_param: Optional[str] = None,
    operation: Optional[str] = None,
) -> Callable[[F], F]:
    """
    Decorator to automatically handle exceptions in endpoint functions.

    Wraps endpoint to catch all exceptions and route through the handler registry.
    Reduces boilerplate by eliminating manual try-except blocks.

    Args:
        resource_name: Name of the resource (e.g., "Catalog", "Collection")
        resource_id_param: Parameter name from function args to extract resource_id
                          (e.g., "definition" to get definition.code)
        operation: Operation being performed (e.g., "Catalog creation")

    Returns:
        Decorated async function with automatic exception handling

    Example:
        @router.post("/catalogs")
        @auto_handle_exceptions(resource_name="Catalog", resource_id_param="definition", operation="Creation")
        async def create_catalog(definition: CatalogRequest):
            # No try-except needed! Exceptions are handled automatically
            result = await catalog_manager.create_catalog(definition.model_dump())
            return result
    """

    def decorator(func: F) -> F:
        @functools.wraps(func)
        async def wrapper(*args, **kwargs):
            try:
                return await func(*args, **kwargs)
            except Exception as e:
                # Extract resource_id from function argument if specified
                extracted_resource_id = None
                if resource_id_param:
                    # Try to get from kwargs first
                    if resource_id_param in kwargs:
                        obj = kwargs[resource_id_param]
                        extracted_resource_id = _extract_resource_id(obj)
                    # Then try to get from positional args by matching parameter names
                    else:
                        import inspect

                        sig = inspect.signature(func)
                        params = list(sig.parameters.keys())
                        if resource_id_param in params:
                            idx = params.index(resource_id_param)
                            if idx < len(args):
                                obj = args[idx]
                                extracted_resource_id = _extract_resource_id(obj)

                raise handle_exception(
                    e,
                    resource_name=resource_name,
                    resource_id=extracted_resource_id,
                    operation=operation,
                ) from e

        return wrapper  # type: ignore

    return decorator


def _extract_resource_id(obj: Any) -> Optional[str]:
    """
    Extract resource ID from an object.

    Tries common patterns like .code, .id, .get("code"), .get("id")
    """
    if obj is None:
        return None

    if isinstance(obj, str):
        return obj

    # Try .id attribute
    if hasattr(obj, "id"):
        return getattr(obj, "id")

    # Try dict-like access
    if isinstance(obj, dict):
        return obj.get("id") or obj.get("code")

    # Try model_dump for Pydantic models
    if hasattr(obj, "model_dump"):
        try:
            data = obj.model_dump()
            return data.get("id") or data.get("code")
        except Exception:
            pass

    # Try .code attribute
    if hasattr(obj, "code"):
        return getattr(obj, "code")

    return None


async def generic_exception_handler(request: Request, exc: Exception) -> Response:
    """
    Central handler to log an exception and then format it into a JSON response.
    This is the primary exception handling entry point for the application, called
    by the GlobalExceptionHandlingMiddleware.
    """
    logger.warning(
        f"Unhandled exception caught by global handler: {exc}", exc_info=True
    )

    # Extract request context for logging
    request_context = {
        "method": request.method,
        "url": str(request.url),
        "path": request.url.path,
        "query_params": dict(request.query_params),
    }

    # Try to extract headers (excluding sensitive ones)
    try:
        headers = dict(request.headers)
        for sensitive_key in ["authorization", "cookie"]:
            headers.pop(sensitive_key, None)
        request_context["headers"] = headers
    except Exception:
        pass

    # Try to extract catalog/collection from path
    try:
        path_parts = request.url.path.strip("/").split("/")
        if "catalogs" in path_parts:
            idx = path_parts.index("catalogs")
            if idx + 1 < len(path_parts):
                request_context["catalog_id"] = path_parts[idx + 1]
        if "collections" in path_parts:
            idx = path_parts.index("collections")
            if idx + 1 < len(path_parts):
                request_context["collection_id"] = path_parts[idx + 1]
    except Exception:
        pass

    # Build context for exception handlers
    context = {
        "request_context": request_context,
        "catalog_id": request_context.get("catalog_id") or "_system_",
        "collection_id": request_context.get("collection_id"),
        "operation": f"{request.method} {request.url.path}",
    }

    # Log the exception (Elasticsearch-backed, #2749) and get a log_id for
    # the response. immediate=True dispatches straight to the backend
    # instead of buffering, so log_id is available before we build the
    # response below.
    log_id = None
    try:
        from dynastore.models.protocols.logs import LogsProtocol
        from dynastore.tools.discovery import get_protocol

        log_event = get_protocol(LogsProtocol)
        if log_event:
            log_id = await log_event.log_event(
                catalog_id=context.get("catalog_id", "_system_"),
                event_type="exception",
                level="ERROR",
                message=f"Error during {context.get('operation', 'Unknown Operation')}: {str(exc)}",
                collection_id=context.get("collection_id"),
                details={
                    "error_type": exc.__class__.__name__,
                    "traceback": traceback.format_exc(),
                    "request_context": context.get("request_context"),
                },
                immediate=True,
            )
            if log_id:
                context["log_id"] = log_id
                context["log_catalog"] = context.get("catalog_id", "_system_")
    except Exception as log_exc:
        logger.critical(
            f"CRITICAL: Failed to log original exception: {log_exc}",
            exc_info=True,
        )
        logger.error(f"Original unlogged exception: {exc}", exc_info=True)

    # Use the registry to format the exception into a structured error, but don't re-raise it.
    result = _global_registry.handle(exc, context=context, reraise_unhandled=False)

    try:
        if isinstance(result, Response):
            return result

        # Use only the handler-produced detail (already sanitized).
        # str(exc) is intentionally omitted here: it may contain internal
        # physical schema/table names that must not appear in HTTP responses.
        # The full exception string is already captured by the logger.warning
        # call above and (when available) in the database log entry.
        response_content = {
            "detail": getattr(result, "detail", "An unexpected error occurred.")
        }
        status_code = getattr(result, "status_code", 500)

        if log_id and status_code >= 500:
            _root = request.scope.get("root_path", "").rstrip("/")
            log_url = f"{_root}/web/#/logs?catalog={context['log_catalog']}&log_id={log_id}"
            response_content["log_reference"] = {  # type: ignore[assignment]
                "log_id": log_id,
                "catalog_id": context["log_catalog"],
                "collection_id": context.get("log_collection"),
                "url": log_url,
            }

        return JSONResponse(
            status_code=status_code,
            content=response_content,
            headers=getattr(result, "headers", None),
        )
    except Exception as fallback_exc:
        logger.error(
            f"Error in exception handler fallback: {fallback_exc}", exc_info=True
        )
        return JSONResponse(
            status_code=500,
            content={
                "detail": "An unexpected error occurred in the exception handler."
            },
        )


def setup_exception_handlers(app: FastAPI) -> None:
    """
    Initialize exception handling integration with FastAPI app.

    Sets up FastAPI exception handlers to use the pluggable registry.
    Call this during app startup/initialization.

    Args:
        app: FastAPI application instance

    Example:
        app = FastAPI()

        @app.on_event("startup")
        async def startup():
            setup_exception_handlers(app)

        # Or simply:
        setup_exception_handlers(app)
    """
    # Add our custom middleware to the application stack.
    # This will catch exceptions from all subsequent middleware and the app itself.
    app.add_middleware(GlobalExceptionHandlingMiddleware)

    # Register the generic handler for any exceptions that might still
    # be caught by FastAPI's built-in ExceptionMiddleware. This provides
    # defense-in-depth.
    app.add_exception_handler(Exception, generic_exception_handler)

    logger.info("Exception handling system initialized for FastAPI app")


def register_extension_handler(
    handler: ExceptionHandler, prepend: bool = False
) -> None:
    """
    Register a custom exception handler from an extension.

    This is a convenience wrapper for modules that want to register their own handlers
    without importing the registry directly.

    Args:
        handler: Custom ExceptionHandler implementation
        prepend: If True, add to front of list for higher priority

    Example:
        from my_extension import MyCustomException, MyCustomHandler

        register_extension_handler(MyCustomHandler(), prepend=True)
    """
    register_handler(handler, prepend=prepend)


@asynccontextmanager
async def http_errors(
    mapping: Optional[Dict[type, int]] = None,
):
    """Context manager that catches exceptions and re-raises as HTTPException.

    Usage::

        async with http_errors({ValueError: 400}):
            result = await some_operation()

        async with http_errors({ValueError: 404, KeyError: 400}):
            result = await find_or_fail()

    ``HTTPException`` is always re-raised as-is (never wrapped).
    """
    if mapping is None:
        mapping = {ValueError: 400}
    try:
        yield
    except HTTPException:
        raise
    except tuple(mapping) as e:  # type: ignore[reportGeneralTypeIssues]
        raise HTTPException(
            status_code=mapping[type(e)], detail=str(e)
        ) from e
