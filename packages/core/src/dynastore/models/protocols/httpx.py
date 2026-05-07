from __future__ import annotations

from typing import Any, Protocol, runtime_checkable


@runtime_checkable
class HttpxProtocol(Protocol):
    """Protocol for the shared HTTPX client extension."""

    def get_httpx_client(self) -> Any:
        """Returns the shared, singleton httpx.AsyncClient."""
        ...

    def get_proxy_httpx_client(self) -> Any:
        """Returns the shared, non-pooling httpx.AsyncClient for proxying."""
        ...
