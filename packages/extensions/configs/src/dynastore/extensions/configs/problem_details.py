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
"""RFC 9457 Problem Details for the configs extension.

Replaces ``raise HTTPException(detail=str(e))`` with structured error
responses that match the RFC 9457 envelope:

    {
      "type":     "https://errors.dynastore.fao.org/config/<category>",
      "title":    "<short human label>",
      "status":   422,
      "detail":   "<longer human prose>",
      "instance": "/configs/.../GeometryStorage",
      "errors":   [{"field": "...", "message": "...", "value": ...}]
    }

Rendered with media type ``application/problem+json`` per the spec.
The handler is registered in ``ConfigsService.lifespan`` so the wrapper
is local to the configs extension; no global side effects.
"""

from typing import List, Optional

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from pydantic import ValidationError

from dynastore.extensions.tools.problem_details import (
    ProblemDetails,
    ProblemError,
    ProblemException,
)

__all__ = [
    "ProblemDetails",
    "ProblemError",
    "ProblemException",
    "plugin_not_registered",
    "validation_failed",
    "value_error",
    "invalid_identifier",
    "collection_not_found",
    "unexpected_failure",
    "problem_exception_handler",
    "register",
]


_TYPE_BASE = "https://errors.dynastore.fao.org/config"


# ---------------------------------------------------------------------------
# Constructors — one per problem class kept in this extension's surface
# ---------------------------------------------------------------------------

def plugin_not_registered(plugin_id: str, *, instance: Optional[str] = None) -> ProblemException:
    return ProblemException(ProblemDetails(
        type=f"{_TYPE_BASE}/plugin-not-registered",
        title="Configuration plugin not registered",
        status=404,
        detail=f"Configuration plugin '{plugin_id}' is not registered.",
        instance=instance,
    ))


def validation_failed(
    exc: ValidationError, *, instance: Optional[str] = None,
) -> ProblemException:
    """Wrap a Pydantic ``ValidationError`` as a 422 Problem Details."""
    errors: List[ProblemError] = []
    for err in exc.errors():
        loc = err.get("loc", ())
        field = ".".join(str(p) for p in loc) or "<root>"
        errors.append(ProblemError(
            field=field,
            message=str(err.get("msg", "")),
            value=err.get("input"),
        ))
    return ProblemException(ProblemDetails(
        type=f"{_TYPE_BASE}/validation-failed",
        title="Configuration payload failed validation",
        status=422,
        detail=str(exc),
        instance=instance,
        errors=errors,
    ))


def value_error(
    exc: ValueError, *, instance: Optional[str] = None,
) -> ProblemException:
    """Wrap a domain-layer ``ValueError`` (e.g. unknown plugin) as 404."""
    return ProblemException(ProblemDetails(
        type=f"{_TYPE_BASE}/not-found",
        title="Configuration resource not found",
        status=404,
        detail=str(exc),
        instance=instance,
    ))


def invalid_identifier(
    exc: ValueError, *, instance: Optional[str] = None,
) -> ProblemException:
    """Wrap a malformed-identifier ``ValueError`` (``InvalidIdentifierError``)
    as 400.

    A resource id that fails ``validate_sql_identifier`` — most commonly an
    unsubstituted templating placeholder like ``{{m.catalog}}`` (#1196) — is a
    *client* input error, not a missing resource (404) or a backend failure
    (500). 400 matches the write path, which already maps it via
    ``handle_exception`` (#1191).
    """
    return ProblemException(ProblemDetails(
        type=f"{_TYPE_BASE}/invalid-identifier",
        title="Invalid resource identifier",
        status=400,
        detail=str(exc),
        instance=instance,
    ))


def collection_not_found(
    catalog_id: str, collection_id: str, *, instance: Optional[str] = None,
) -> ProblemException:
    """404 for ``PUT /catalogs/{cat}/collections/{col}/plugins/{plugin_id}``
    when ``collection_id`` does not exist and the caller did not opt in to
    JIT creation with ``?create_if_missing=true`` (issue #918).

    Default-off is the safer wire contract: a fat-fingered collection
    name (``sentinal`` for ``sentinel``) used to silently create a brand
    new collection on PUT; now the caller has to explicitly ask for
    upfront-configure behaviour.
    """
    return ProblemException(ProblemDetails(
        type=f"{_TYPE_BASE}/collection-not-found",
        title="Collection not found",
        status=404,
        detail=(
            f"Collection '{collection_id}' does not exist in catalog "
            f"'{catalog_id}'. Re-check the name, create the collection "
            f"first (POST /stac/catalogs/{catalog_id}/collections), or "
            f"opt in to JIT creation by re-issuing the request with "
            f"``?create_if_missing=true``."
        ),
        instance=instance,
    ))


def engine_write_forbidden_at_tenant_scope(
    plugin_id: str, *, scope: str, instance: Optional[str] = None,
) -> ProblemException:
    """Cycle F.4b — reject tenant-scope writes to ``platform.protocols.storage.*``.

    Engines are sysadmin-only platform-tier resources (tenant configs
    cannot influence platform resource policy — see decision #15 / #18).
    The existing ``configs_access`` policy gates the entire
    ``/configs/.*`` surface to SYSADMIN, so this 403 is defence-in-
    depth: even if the policy is misconfigured the routing layer
    itself rejects engine writes at any scope below platform.

    ``scope`` is a label like ``"catalog"`` or ``"collection"`` that
    surfaces in the message so operators see exactly where the write
    was attempted.
    """
    return ProblemException(ProblemDetails(
        type=f"{_TYPE_BASE}/engine-write-forbidden-at-tenant-scope",
        title="Engine configuration is sysadmin-only",
        status=403,
        detail=(
            f"Engine '{plugin_id}' lives at the platform tier and cannot "
            f"be written at {scope} scope.  Engines are managed by "
            f"sysadmin only; PATCH or PUT them under "
            f"``/configs/plugins/{plugin_id}`` (platform tier) instead."
        ),
        instance=instance,
    ))


def unexpected_failure(
    exc: BaseException, *, instance: Optional[str] = None,
) -> ProblemException:
    """Wrap an otherwise-uncaught exception as a 500 Problem Details.

    The body intentionally carries the exception's str() in `detail` so
    operators can debug; the `type` URI lets clients dispatch on the
    problem class without parsing the message.
    """
    return ProblemException(ProblemDetails(
        type=f"{_TYPE_BASE}/internal",
        title="Configuration backend failure",
        status=500,
        detail=str(exc),
        instance=instance,
    ))


# ---------------------------------------------------------------------------
# Exception handler — rendered by FastAPI per registration in lifespan
# ---------------------------------------------------------------------------

async def problem_exception_handler(
    request: Request, exc: Exception,
) -> JSONResponse:
    """FastAPI exception handler for :class:`ProblemException`.

    Renders the body as ``application/problem+json`` per RFC 9457 §3.
    `instance` defaults to the request path when the raiser didn't set
    one explicitly (most common case).
    """
    if not isinstance(exc, ProblemException):
        raise exc  # pragma: no cover — registered only for ProblemException
    problem = exc.problem
    if problem.instance is None:
        # Default instance to the request path (RFC 9457 §3.1.5)
        problem = problem.model_copy(update={"instance": str(request.url.path)})
    return JSONResponse(
        content=problem.model_dump(exclude_none=True),
        status_code=problem.status,
        media_type="application/problem+json",
    )


def register(app: FastAPI) -> None:
    """Wire the handler. Called from ``ConfigsService.lifespan``."""
    app.add_exception_handler(ProblemException, problem_exception_handler)
