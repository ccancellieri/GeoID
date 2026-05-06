#    Copyright 2026 FAO
#
#    Licensed under the Apache License, Version 2.0 (the "License").

"""``PageVisibilityFilter`` — IAM-mediated nav filtering for web routes.

Architectural rule (per project feedback): authorization decisions live
in middleware/IAM. Plugin route handlers must NOT inspect
``request.state.principal_role`` or compare role names — every
auth-shaped concern crosses through a Protocol so the route stays
agnostic.

The web ``/web/config/pages`` route returns the list of admissible nav
entries for the caller. Computing admissibility requires reading the
caller's roles and matching them against each page's ``required_roles``
— an auth concept. This Protocol moves that logic into the IAM
extension: the web route asks "filter these pages for this request"
and gets back the visible subset; the route never names a role.

When no implementation is registered (IAM module unloaded), the
default is to return only pages that are anonymously visible — i.e.
those without ``required_roles`` or with ``anonymous`` listed. The
web route applies that fallback locally so it has zero IAM imports.
"""

from __future__ import annotations

from typing import Any, Dict, List, Protocol, runtime_checkable

from starlette.requests import Request


@runtime_checkable
class PageVisibilityFilter(Protocol):
    """Filter a flat page list down to entries the caller may see.

    Each page dict is expected to carry an ``id`` and an optional
    ``required_roles: list[str]``. Implementations decide admissibility
    however they like (role match, sysadmin bypass, condition
    evaluation) — the contract here is purely "given these pages and
    this request, which should the caller see?".
    """

    def filter_visible(
        self,
        pages: List[Dict[str, Any]],
        request: Request,
    ) -> List[Dict[str, Any]]:
        """Return the subset of ``pages`` admissible to ``request`` caller.

        Order is preserved. The same dict instances may be returned —
        callers must not assume copies. Raises only on programmer error
        (e.g. malformed page dict); auth/IAM unavailability returns the
        anonymous-visible subset instead of failing.
        """
        ...
