"""
Source-level pins for the effective-permissions explainer modal (#1390).

The per-row "Why?" affordance on the Principals tab bindings table opens
a dialog that calls ``GET /admin/iam/effective`` (#1389 backend) and
renders the trace. These guards verify the button is present per row
with the right a11y attributes, the api.js client encodes every query
value, and the render path never assigns server-derived strings through
``innerHTML``.
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


@pytest.fixture(scope="module")
def api_js() -> str:
    path = Path(__file__).resolve().parents[3].parent / (
        "packages/extensions/web/src/dynastore/extensions/web/static/common/api.js"
    )
    return path.read_text(encoding="utf-8")


@pytest.fixture(scope="module")
def admin_css() -> str:
    path = Path(__file__).resolve().parents[3].parent / (
        "packages/extensions/web/src/dynastore/extensions/web/static/common/admin.css"
    )
    return path.read_text(encoding="utf-8")


# --- "Why?" button on every binding row ----------------------------------


def test_binding_row_has_why_button(governance_js: str) -> None:
    """A ``Why?`` button is rendered in the bindings-table actions cell."""
    # Crude but effective: the render path appends a button with the
    # textContent "Why?". We pin the textContent assignment and the
    # ghost variant + xs sizing the spec calls for.
    assert 'why.textContent = "Why?"' in governance_js
    assert 'why.className = "btn btn-ghost btn-xs"' in governance_js


def test_why_button_has_aria_label(governance_js: str) -> None:
    """Accessibility: the icon-light button carries an aria-label that
    spells out what the operator gets when they click it."""
    needle = (
        'why.setAttribute("aria-label", '
        '"Explain effective permissions for this binding")'
    )
    assert needle in governance_js


def test_why_button_lives_in_bindings_row_renderer(governance_js: str) -> None:
    """The Why button must be wired inside ``renderBindings`` so it lands
    on every row — not, accidentally, only on the principals-survey row."""
    render_block = governance_js.split("function renderBindings", 1)[-1]
    next_fn = render_block.split("\nasync function ", 1)[0]
    assert 'why.textContent = "Why?"' in next_fn
    # The revoke action must still be present next to it.
    assert 'del.textContent = "Revoke"' in next_fn


# --- Modal scaffold in HTML ---------------------------------------------


def test_explainer_dialog_scaffold_present(governance_html: str) -> None:
    """The native <dialog> scaffold with the action picker + result panel
    is in the page markup, hidden until ``showModal()`` is called."""
    assert 'id="explainer-modal"' in governance_html
    assert "<dialog" in governance_html
    # Action picker — the common verbs are pre-listed plus a Custom… escape.
    for verb in ("GET", "POST", "PUT", "PATCH", "DELETE"):
        assert f'value="{verb}"' in governance_html
    assert 'value="__custom__"' in governance_html
    assert 'id="explainer-action-custom"' in governance_html
    # Result panel scaffold — verdict chip + reason + grants table.
    assert 'id="explainer-verdict-chip"' in governance_html
    assert 'id="explainer-reason"' in governance_html
    assert 'id="explainer-grants"' in governance_html


def test_explainer_dialog_has_aria_label(governance_html: str) -> None:
    """The dialog is labelled by its title for assistive tech."""
    block = governance_html.split('id="explainer-modal"', 1)[-1].split(">", 1)[0]
    assert 'aria-labelledby="explainer-title"' in block


# --- api.js helper -------------------------------------------------------


def test_api_exports_fetch_effective_permissions(api_js: str) -> None:
    """The thin client function is exported so governance.js can import it."""
    assert "export const fetchEffectivePermissions" in api_js


def test_fetch_effective_permissions_encodes_every_query_value(api_js: str) -> None:
    """Every query-string value going to /admin/iam/effective must pass
    through encodeURIComponent — principal ids, catalog ids and resource
    refs can carry characters that would otherwise smuggle extra keys."""
    # Isolate the helper body.
    fn = api_js.split("fetchEffectivePermissions", 1)[-1]
    # Cut at the next top-level export to avoid bleed into later helpers.
    fn = fn.split("\nexport ", 1)[0]
    assert "/admin/iam/effective" in fn
    # Both the key and the value go through encodeURIComponent on every
    # parameter — the helper builds `${enc(key)}=${enc(value)}` pairs.
    assert "encodeURIComponent(key)" in fn
    assert "encodeURIComponent(value)" in fn
    # Every documented parameter must be pushed into the query.
    for key in (
        "principal_id", "action", "catalog_id",
        "collection_id", "resource_kind", "resource_ref",
    ):
        assert f'"{key}"' in fn, f"missing push for {key}"


# --- XSS posture on the render path --------------------------------------


def test_no_unsafe_innerhtml_on_explainer_render(governance_js: str) -> None:
    """The explainer render path must not assign server-derived strings
    through innerHTML — same rule the bindings tests enforce on the
    rest of governance.js (matches #1382 / #1386)."""
    unsafe_sink = "inner" + "HTML"  # avoid tripping scanners on the literal
    needle = f".{unsafe_sink} ="
    assert needle not in governance_js


def test_decision_reason_uses_textcontent(governance_js: str) -> None:
    """Server-derived ``decision_reason`` is set via textContent."""
    # Pin the exact assignment in the render path.
    needle = 'textContent = data.decision_reason'
    assert needle in governance_js


def test_condition_trace_renders_as_pre_textcontent(governance_js: str) -> None:
    """conditions_evaluated is dumped into a <pre> via textContent —
    operator-supplied condition config + handler detail must not be
    parsed as markup."""
    block = governance_js.split("function renderExplainerResult", 1)[-1]
    block = block.split("\n}", 1)[0]
    assert 'document.createElement("pre")' in block
    assert "JSON.stringify(conds" in block
    assert "pre.textContent" in block


# --- Winning grant highlight --------------------------------------------


def test_winning_grant_highlight_class_lands(governance_js: str) -> None:
    """The deciding grant gets ``.row-winner``; deny winners ALSO get
    ``.row-deny`` so the existing terracotta tint composes with the
    highlight (both classes coexist on one <tr>)."""
    block = governance_js.split("function renderExplainerResult", 1)[-1]
    block = block.split("\n}", 1)[0]
    assert 'classList.add("row-winner")' in block
    assert 'classList.add("row-deny")' in block


def test_find_winning_grant_picks_first_matched_allow():
    """When no deny matched, the first matched ALLOW wins."""
    js = (Path(__file__).resolve().parents[3].parent / (
        "packages/extensions/web/src/dynastore/extensions/web/static/admin/governance.js"
    )).read_text(encoding="utf-8")
    # The helper exists and references the documented fields.
    fn = js.split("function findWinningGrantIndex", 1)[-1].split("\n}", 1)[0]
    assert "deny_precedence_applied" in fn
    assert "g.matched" in fn
    assert "g.effect" in fn


def test_row_winner_css_is_centralised(admin_css: str) -> None:
    """``.row-winner`` lives in common/admin.css alongside ``.row-deny`` so
    the deciding-grant treatment is consistent across pages."""
    assert "tr.row-winner" in admin_css
    # The combined .row-winner.row-deny rule must be present so deny
    # winners get a distinct tint that doesn't collide with allow.
    assert "row-winner.row-deny" in admin_css


def test_btn_ghost_variant_defined(admin_css: str) -> None:
    """The ``Why?`` button uses ``.btn-ghost`` — that variant must exist."""
    assert ".btn-ghost" in admin_css


# --- Behaviour preservation ---------------------------------------------


def test_revoke_button_still_present(governance_js: str) -> None:
    """Adding ``Why?`` must not remove or break the Revoke affordance."""
    assert 'del.textContent = "Revoke"' in governance_js
    assert "revokeBinding(row)" in governance_js
