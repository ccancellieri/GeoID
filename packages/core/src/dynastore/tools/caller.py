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

"""Resolve the *caller* for system-initiated task / event work.

System-internal enqueues (the co-transactional ``event_drain`` trigger, drain
and cascade tasks) run deep below the request handler — there is no ``Request``
object in scope to read ``request.state.principal`` from.  They resolve the
caller through the request-scoped *caller snapshot* that the authorization
middleware already publishes: :class:`RequestVisibility` ("who is asking",
read via :func:`get_request_visibility`).  When no authenticated principal is
in context — no authorization middleware loaded, an anonymous caller, or a
background worker with no request — the resolver falls back to
:data:`~dynastore.models.auth_models.SYSTEM_USER_ID`.

This keeps caller attribution working without threading a principal through
every internal API, and keeps the authorization layer optional: with nothing
publishing a snapshot, the resolver simply yields the system id.  The return
value is always non-empty, so it satisfies ``RunnerContext.caller_id``'s
``min_length=1`` invariant at the dispatch boundary.
"""

from __future__ import annotations

from typing import Any, Optional


def current_caller_id() -> str:
    """Best-effort caller id for system-initiated work; never empty.

    Returns ``"{provider}:{subject_id}"`` (or the bare ``subject_id`` when the
    principal carries no provider) for the authenticated principal in the
    current request context, mirroring the request-path attribution wiring
    (``OGCServiceMixin._principal_caller_id``).  Falls back to
    :data:`~dynastore.models.auth_models.SYSTEM_USER_ID` when no principal is
    resolvable.
    """
    from dynastore.models.auth_models import SYSTEM_USER_ID

    principal = _current_principal()
    if principal is not None:
        subject_id: Optional[str] = getattr(principal, "subject_id", None)
        if subject_id:
            provider = getattr(principal, "provider", None)
            return f"{provider}:{subject_id}" if provider else subject_id
    return SYSTEM_USER_ID


def _current_principal() -> Optional[Any]:
    """The authenticated principal from the request caller snapshot, or None.

    Absence of the snapshot (no authorization middleware, background worker,
    or a resolution error) is a valid, handled state — the caller then defaults
    to the system id.
    """
    try:
        from dynastore.models.protocols.visibility import get_request_visibility

        visibility = get_request_visibility()
    except Exception:  # noqa: BLE001 — absence of the snapshot is a valid state
        return None
    return getattr(visibility, "principal", None) if visibility else None
