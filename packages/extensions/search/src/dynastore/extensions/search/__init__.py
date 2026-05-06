"""
DynaStore 'search' extension.

Provides a STAC API Item Search-compliant `GET /search` and `POST /search`
endpoint backed by Elasticsearch. The implementation detail (ES) is hidden
from the API surface – the extension is named 'search'.

Enable by adding 'search' to SCOPE.
"""
from __future__ import annotations

import logging
from contextlib import asynccontextmanager
from typing import Any

from fastapi import FastAPI

from .router import router
from .search_service import SearchService

logger = logging.getLogger(__name__)


class SearchExtension:
    """
    Protocol-discoverable extension that mounts the STAC search router
    and exposes the SearchService.
    """

    _registered_name = "search"

    def __init__(self, app_state: Any = None):
        self.service = SearchService()  # type: ignore[abstract]
        self.router = router

    @asynccontextmanager
    async def lifespan(self, app: FastAPI):
        logger.info("SearchExtension: STAC search endpoint is active at /search")
        yield

    def configure_app(self, app: FastAPI) -> None:
        # Declare STAC Item Search conformance class
        conformance = getattr(app.state, "conformance_classes", [])
        if "https://api.stacspec.org/v1.0.0/item-search" not in conformance:
            conformance.append("https://api.stacspec.org/v1.0.0/item-search")
            app.state.conformance_classes = conformance


from . import config  # noqa: F401  -- service-exposure plugin registration

__all__ = ["SearchExtension"]
