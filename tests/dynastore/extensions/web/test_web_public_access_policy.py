"""Regression guard: anonymous role must include root ``/health``.

Cloud Run startup probe targets the canonical root ``/health`` route
declared in ``dynastore/main.py``. The web extension's
``web_public_access`` policy is the anonymous role's binding; if
``/health$`` is missing from its resources, the IAM middleware denies
the probe with ``Deny by Default`` and the revision never starts.

Pins the resource list against future re-edits that might drop the
entry — same regression class as geoid#902.
"""
from __future__ import annotations

import re

from dynastore.extensions.web.web import _web_policies, _web_role_bindings


def test_web_public_access_whitelists_root_health() -> None:
    policies = {p.id: p for p in _web_policies()}
    assert "web_public_access" in policies
    public = policies["web_public_access"]

    assert "/health$" in public.resources, (
        "web_public_access must whitelist the canonical root /health route "
        "(Cloud Run startup probe path). See dynastore/main.py:228."
    )
    assert "GET" in public.actions

    pattern = re.compile("/health$")
    assert pattern.match("/health"), (
        "anchored /health$ regex must match the literal probe path under "
        "PolicyService.re.match semantics."
    )


def test_web_public_access_drops_legacy_web_health() -> None:
    """``/web/health`` was a duplicate of root ``/health`` and is removed."""
    policies = {p.id: p for p in _web_policies()}
    public = policies["web_public_access"]
    assert "/web/health$" not in public.resources
    assert "/web/health" not in public.resources


def test_unauthenticated_role_always_bound_to_web_public_access() -> None:
    """Literal ``"unauthenticated"`` is bound to ``web_public_access``
    regardless of ``IamRolesConfig.anonymous_role_name`` overrides.

    Defence against operator renames of the anonymous role: the Cloud Run
    / load-balancer startup probe maps to the platform's default
    ``"unauthenticated"`` role, so that literal must always hold the
    ``web_public_access`` binding (whitelists root ``/health``).
    """
    # Default config: anonymous_role_name == "unauthenticated" — single
    # binding covers both.
    bindings = _web_role_bindings()
    bound = [
        b for b in bindings
        if b.name == "unauthenticated" and "web_public_access" in b.policies
    ]
    assert bound, (
        "literal 'unauthenticated' role must be bound to web_public_access "
        "so anonymous /health stays open under default config."
    )

    # Operator override: anonymous_role_name renamed away from
    # "unauthenticated" — literal binding must still be emitted.
    bindings = _web_role_bindings(anonymous_role_name="anon")
    names_with_public = {
        b.name for b in bindings if "web_public_access" in b.policies
    }
    assert "anon" in names_with_public
    assert "unauthenticated" in names_with_public, (
        "literal 'unauthenticated' binding must persist even when an "
        "operator renames IamRolesConfig.anonymous_role_name."
    )
