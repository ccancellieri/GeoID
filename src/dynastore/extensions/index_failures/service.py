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

"""``IndexFailureService`` — tenant-visible REST surface for the
per-tenant ``index_failure_log`` table.

One GET endpoint:
``/index_failures/catalogs/{catalog_id}/index-failures``

Returns a paginated JSON list of :class:`IndexFailureRecord` rows
(``failed`` + ``retrying`` events emitted by the outbox drain task)
plus HATEOAS-style ``_links`` (``self`` + optional ``next`` for
offset-based pagination). Filterable by ``collection`` /
``driver`` / ``since`` / ``status``.

Authz is delegated to ``IamMiddleware`` via the policy registered in
:mod:`policies` (``catalog_membership_required`` condition on the
``{catalog_id}`` path segment). The handler treats the absence of
``request.state.principal`` as 401 in case the middleware is wired in
permissive mode for tests but the route is still gated.

The asyncpg pool is reused from the dispatcher's lazy pool
(:func:`dynastore.modules.storage.index_dispatcher._LazyPoolProxy`)
so the API process does not need to maintain its own. The pool DSN
is derived from :class:`DBConfig` — same env source SQLAlchemy uses.
"""
from __future__ import annotations

import logging
from contextlib import asynccontextmanager
from datetime import datetime
from typing import Annotated, Any, AsyncGenerator, Dict, List, Literal, Optional

from fastapi import APIRouter, FastAPI, HTTPException, Query, Request
from pydantic import BaseModel

from dynastore.extensions.protocols import ExtensionProtocol
from dynastore.models.protocols.indexing import (
    IndexFailureLog,
    IndexFailureRecord,
)

logger = logging.getLogger(__name__)


class _IndexFailureItem(BaseModel):
    """Wire shape for a single failure row."""

    failure_id: str
    occurred_at: datetime
    collection_id: str
    driver_id: str
    driver_instance_id: str
    op_id: Optional[str]
    item_id: Optional[str]
    op: str
    attempts: int
    error_class: str
    error_message: str
    status: str
    correlation_id: Optional[str]

    @classmethod
    def from_record(cls, record: IndexFailureRecord) -> "_IndexFailureItem":
        return cls(
            failure_id=str(record.failure_id),
            occurred_at=record.occurred_at,
            collection_id=record.collection_id,
            driver_id=record.driver_id,
            driver_instance_id=record.driver_instance_id,
            op_id=str(record.op_id) if record.op_id is not None else None,
            item_id=record.item_id,
            op=record.op,
            attempts=record.attempts,
            error_class=record.error_class,
            error_message=record.error_message,
            status=record.status,
            correlation_id=record.correlation_id,
        )


class IndexFailureService(ExtensionProtocol):
    """Extension exposing the tenant-visible failure-log REST surface."""

    priority: int = 100  # Same band as logs/stats — registers after IAM.
    router: APIRouter

    # Allow tests / DI to inject a stub :class:`IndexFailureLog` without
    # going through the lazy asyncpg pool. Production leaves this ``None``;
    # ``_get_log`` falls back to constructing :class:`PgIndexFailureLog`
    # against the dispatcher's lazy pool proxy on first use.
    log_factory: Optional[Any] = None

    def __init__(self, app: Any = None) -> None:
        self.app = app
        self.router = APIRouter(
            prefix="/index_failures", tags=["Index Failures"],
        )
        self._setup_routes()

    def _setup_routes(self) -> None:
        self.router.add_api_route(
            "/catalogs/{catalog_id}/index-failures",
            self.list_failures,
            methods=["GET"],
            summary="List recent indexing failures for this catalog",
        )

    @asynccontextmanager
    async def lifespan(self, app: FastAPI) -> AsyncGenerator[None, None]:
        # Register the per-catalog policy unconditionally — the
        # PermissionProtocol may not be available in narrow SCOPE
        # configurations; the helper logs and no-ops in that case.
        from dynastore.extensions.index_failures.policies import (
            register_index_failures_policies,
        )

        register_index_failures_policies()
        logger.info("IndexFailureService initialized.")
        yield

    def _get_log(self) -> IndexFailureLog:
        """Return an :class:`IndexFailureLog`.

        Test paths inject ``log_factory`` (a callable returning a
        ``IndexFailureLog``) directly on the service instance. Production
        falls back to :class:`PgIndexFailureLog` over the dispatcher's
        lazy asyncpg pool proxy — no per-request connection setup.
        """
        if self.log_factory is not None:
            return self.log_factory()

        from dynastore.modules.storage.index_dispatcher import _LazyPoolProxy
        from dynastore.modules.storage.pg_index_failure_log import (
            PgIndexFailureLog,
        )

        return PgIndexFailureLog(pool=_LazyPoolProxy())

    async def list_failures(
        self,
        request: Request,
        catalog_id: str,
        collection: Annotated[Optional[str], Query()] = None,
        driver: Annotated[Optional[str], Query()] = None,
        since: Annotated[Optional[datetime], Query()] = None,
        status: Annotated[
            Optional[Literal["retrying", "failed"]], Query()
        ] = None,
        limit: Annotated[int, Query(ge=1, le=500)] = 100,
        offset: Annotated[int, Query(ge=0)] = 0,
    ) -> Dict[str, Any]:
        """Return paginated failure rows for ``catalog_id``.

        Filters compose with AND inside :class:`IndexFailureLog`. The
        ``_links`` dict surfaces ``self`` + optional ``next`` (only
        emitted when the underlying page was full — same convention as
        the catalog REST surface). ``next`` is a relative-friendly
        absolute URL because ``request.url`` already reflects the
        proxy's root_path.
        """
        principal = getattr(request.state, "principal", None)
        if principal is None:
            # Final defense in depth — IamMiddleware should already have
            # rejected anonymous requests at the policy gate. If the
            # request reaches here without a principal, fail closed
            # rather than leaking the failure log.
            raise HTTPException(status_code=401, detail="auth required")

        log = self._get_log()
        records = await log.list_failures(
            catalog_id=catalog_id,
            collection_id=collection,
            driver_id=driver,
            since=since,
            status=status,
            limit=limit,
            offset=offset,
        )

        items: List[Dict[str, Any]] = [
            _IndexFailureItem.from_record(r).model_dump(mode="json")
            for r in records
        ]
        next_offset = offset + limit if len(records) >= limit else None

        next_url: Optional[str] = None
        if next_offset is not None:
            next_url = str(
                request.url.include_query_params(offset=next_offset)
            )

        return {
            "items": items,
            "_links": {
                "self": str(request.url),
                "next": next_url,
            },
        }
