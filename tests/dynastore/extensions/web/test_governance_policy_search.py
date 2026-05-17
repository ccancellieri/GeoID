"""
Source-level regression pins for the admin Governance page policy-name search.

These guards ensure the `#policies-query` input fires its filter on every
keystroke and matches a broad set of policy fields. The reported failure
mode (#644) was that typing into the search box produced no visible effect
because the input listener short-circuited when `state.policies` was empty.
"""

from pathlib import Path

import pytest


@pytest.fixture(scope="module")
def governance_js() -> str:
    path = Path(__file__).resolve().parents[3].parent / (
        "packages/extensions/web/src/dynastore/extensions/web/static/admin/governance.js"
    )
    return path.read_text(encoding="utf-8")


@pytest.fixture(scope="module")
def governance_html() -> str:
    path = Path(__file__).resolve().parents[3].parent / (
        "packages/extensions/web/src/dynastore/extensions/web/static/admin/governance.html"
    )
    return path.read_text(encoding="utf-8")


def test_policies_query_input_always_re_renders(governance_js: str) -> None:
    """The input listener must not short-circuit on empty state.policies.

    Previously the handler was `if (state.policies.length) renderPolicies()`,
    which silently swallowed every keystroke when the policy list had not
    yet loaded (or had failed to load) — the visible symptom that triggered
    this fix.
    """
    assert "if (state.policies.length) renderPolicies();" not in governance_js, (
        "policies-query listener still short-circuits on empty state.policies; "
        "typing produces no visible effect when the list has not yet loaded."
    )
    # The handler must still be wired.
    assert '$("#policies-query").addEventListener("input"' in governance_js


def test_policies_query_filter_matches_actions_and_resources(governance_js: str) -> None:
    """Client-side filter must match id, description, actions, and resources.

    Operators searching by an action verb ("GET") or a resource path fragment
    must see live filtering — matching on id+description alone is too narrow.
    """
    # The new filter must reference all four fields.
    assert "p.actions" in governance_js
    assert "p.resources" in governance_js
    assert "p.id" in governance_js
    assert "p.description" in governance_js


def test_policies_query_placeholder_advertises_full_field_set(governance_html: str) -> None:
    """Placeholder should tell operators what the input actually matches."""
    assert "action" in governance_html.lower()
    assert "resource" in governance_html.lower()
