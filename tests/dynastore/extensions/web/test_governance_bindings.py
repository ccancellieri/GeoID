"""
Source-level pins for the Governance Principals tab binding editor (#1346).

These guards verify that the principals tab exposes the full binding shape
backed by ``POST/GET/DELETE /admin/{platform,catalogs/{cat}}/grants``:
effect (allow|deny), validity window, object kind (role|policy), and
per-binding quota. The legacy allow-only ``/admin/.../roles`` write path
is no longer the principals-tab write surface.
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


# --- HTML form surfaces --------------------------------------------------


def test_binding_form_exposes_all_five_field_surfaces(governance_html: str) -> None:
    """The five #1346 surfaces (effect, valid_from, valid_until, kind, quota)
    must each be present in the principals tab markup."""
    # effect — allow / deny dropdown
    assert 'id="binding-effect"' in governance_html
    assert 'value="allow"' in governance_html
    assert 'value="deny"' in governance_html
    # validity window
    assert 'id="binding-valid-from"' in governance_html
    assert 'id="binding-valid-until"' in governance_html
    assert 'type="datetime-local"' in governance_html
    # object_kind selector
    assert 'id="binding-object-kind"' in governance_html
    assert 'value="role"' in governance_html
    assert 'value="policy"' in governance_html
    # quota editor — both structured fields and JSON escape hatch
    assert 'id="binding-quota-rate-limit"' in governance_html
    assert 'id="binding-quota-max-count"' in governance_html
    assert 'id="binding-quota-json"' in governance_html


def test_binding_form_lives_under_principals_tab(governance_html: str) -> None:
    """The binding editor is anchored on the Principals tab section."""
    principals_section = governance_html.split('id="tab-principals"', 1)[-1]
    next_section = principals_section.split('id="tab-provisioning"', 1)[0]
    assert 'id="binding-plate"' in next_section
    assert 'id="binding-create"' in next_section
    assert 'id="bindings-table"' in next_section


def test_bindings_table_has_seven_columns(governance_html: str) -> None:
    """Kind / Object / Effect / Valid from / Valid until / Quota / actions."""
    table_block = governance_html.split('id="bindings-table"', 1)[-1].split("</table>", 1)[0]
    for header in ["Kind", "Object", "Effect", "Valid from", "Valid until", "Quota"]:
        assert f">{header}<" in table_block, f"Missing column header: {header}"


# --- JS wiring -----------------------------------------------------------


def test_binding_writes_hit_new_grants_endpoints_not_legacy_roles(governance_js: str) -> None:
    """The principals-tab write path is the generic /grants surface, NOT the
    legacy allow-only /admin/.../roles POST."""
    # New endpoints (via api.js helpers, called by name).
    assert "createGrant" in governance_js
    assert "deleteGrant" in governance_js
    assert "listGrants" in governance_js
    # Legacy allow-only role assign/remove must not be imported into this
    # module anymore — that surface is backcompat only.
    assert "assignGlobalRole" not in governance_js
    assert "removeGlobalRole" not in governance_js
    assert "assignCatalogRole" not in governance_js
    assert "removeCatalogRole" not in governance_js


def test_binding_form_submits_full_create_binding_request(governance_js: str) -> None:
    """The submit handler must build a CreateBindingRequest body with the
    new fields, not a legacy {role: ...} payload."""
    assert "subject_id" in governance_js
    assert "object_kind" in governance_js
    assert "object_ref" in governance_js
    assert "effect" in governance_js
    assert "valid_from" in governance_js
    assert "valid_until" in governance_js
    assert "quota" in governance_js


def test_deny_rows_are_visually_distinct(governance_js: str) -> None:
    """Deny bindings must be styled differently from allow ones — operators
    have to spot a deny in a long mixed list at a glance."""
    # Deny rows are marked with the canonical .row-deny class; the actual
    # left-border + tint live in common/admin.css so the treatment is shared
    # across pages.
    assert 'effect === "deny"' in governance_js
    assert '"row-deny"' in governance_js


def test_deny_row_style_lives_in_shared_css() -> None:
    """The .row-deny treatment is centralised in common/admin.css so that the
    deny look is consistent across every bindings/grants table. The rule set
    must include both the tinted background and the terracotta left border so
    deny rows pop visually."""
    css_path = Path(__file__).resolve().parents[3].parent / (
        "packages/extensions/web/src/dynastore/extensions/web/static/common/admin.css"
    )
    css = css_path.read_text(encoding="utf-8")
    assert "tr.row-deny" in css
    # Grab every rule selector that mentions .row-deny and concatenate the
    # rule bodies. The treatment must include both a tinted background and a
    # left border accent.
    bodies: list[str] = []
    cursor = css
    while "row-deny" in cursor:
        head, rest = cursor.split("row-deny", 1)
        body, after = rest.split("}", 1)
        bodies.append(body)
        cursor = after
    joined = "\n".join(bodies)
    assert "background" in joined
    assert "border-left" in joined


def test_no_unsafe_dom_string_sink(governance_js: str) -> None:
    """XSS safety: governance.js must not feed server-controlled strings
    through the unsafe DOM string sink — use textContent + DOM APIs only."""
    unsafe_sink = "inner" + "HTML"  # avoid tripping security scanners on the literal
    needle = f".{unsafe_sink} ="
    assert needle not in governance_js, (
        f"governance.js should not assign {unsafe_sink} — use textContent + DOM APIs "
        "for any server-supplied string."
    )


# --- api.js helpers ------------------------------------------------------


def test_api_exposes_listgrants_creategrant_deletegrant(api_js: str) -> None:
    """The three /grants helpers must be exported for governance.js to import."""
    assert "export const listGrants" in api_js
    assert "export const createGrant" in api_js
    assert "export const deleteGrant" in api_js


def test_api_paths_use_encodeuricomponent(api_js: str) -> None:
    """The catalog segment of the /grants URL must be URL-encoded."""
    # grantsBasePath() is the single source of the URL — confirm it encodes
    # the catalog id before it lands in a path segment.
    grants_fn = api_js.split("function grantsBasePath", 1)[-1].split("\n}\n", 1)[0]
    assert "encodeURIComponent" in grants_fn
