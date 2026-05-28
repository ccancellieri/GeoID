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

"""Regression for geoid#1492: audit writes must target the platform schema.

When an unauthenticated request is denied by policy, the middleware resolves
the *tenant* schema for policy evaluation and then emits an ``authz_denied``
audit event. The ``audit_log`` table is platform-only (lives in the ``iam``
schema, never in tenant schemas), so forwarding the tenant schema to
``log_audit_event`` raised ``relation "<tenant>.audit_log" does not exist``
and turned a clean 403 into a 500.

``_emit_audit`` no longer accepts a ``schema`` argument: it never forwards
one to ``log_audit_event``, which defaults ``schema="iam"``. These pure unit
tests pin that contract so the footgun can't be reintroduced.
"""

from __future__ import annotations

import asyncio
import inspect

import pytest

from dynastore.extensions.iam.middleware import IamMiddleware


class _CapturingStorage:
    """Minimal IamStorage stub recording the kwargs of log_audit_event."""

    def __init__(self) -> None:
        self.calls: list[dict] = []

    async def log_audit_event(self, **kwargs) -> None:
        self.calls.append(kwargs)


class _FakeManager:
    def __init__(self, storage) -> None:
        self.storage = storage


def _build_middleware(storage) -> IamMiddleware:
    """Construct a bare IamMiddleware without running BaseHTTPMiddleware.__init__."""
    mw = object.__new__(IamMiddleware)
    mw._iam_manager = _FakeManager(storage)
    return mw


def test_emit_audit_signature_has_no_schema_param() -> None:
    """The ``schema`` footgun must stay removed — audit_log is platform-only."""
    params = inspect.signature(IamMiddleware._emit_audit).parameters
    assert "schema" not in params, (
        "_emit_audit must not accept a tenant schema (geoid#1492): "
        f"got params {list(params)}"
    )


@pytest.mark.asyncio
async def test_emit_audit_does_not_forward_tenant_schema() -> None:
    """log_audit_event is called WITHOUT a schema kwarg, so it defaults to
    the platform ``iam`` schema rather than a tenant schema."""
    storage = _CapturingStorage()
    mw = _build_middleware(storage)

    mw._emit_audit(
        "authz_denied",
        "principal-123",
        "203.0.113.7",
        {"path": "/search/catalogs/abc/items-search", "method": "POST",
         "reason": "denied"},
    )
    # _emit_audit schedules a fire-and-forget task; let it run.
    pending = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]
    if pending:
        await asyncio.gather(*pending)

    assert len(storage.calls) == 1
    call = storage.calls[0]
    assert "schema" not in call, (
        f"audit write must not pin a tenant schema; got schema={call.get('schema')!r}"
    )
    assert call["event_type"] == "authz_denied"
    assert call["principal_id"] == "principal-123"
    assert call["ip_address"] == "203.0.113.7"


@pytest.mark.asyncio
async def test_emit_audit_noop_without_storage() -> None:
    """No storage / no log_audit_event → silent no-op, never raises."""
    class _NoStorage:
        storage = None

    mw = object.__new__(IamMiddleware)
    mw._iam_manager = _NoStorage()
    # Must not raise even though there is nothing to write to.
    mw._emit_audit("authz_denied", "p", "ip", {"path": "/x"})
