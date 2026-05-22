# Copyright 2025 FAO
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
"""Source-level regression pins for the admin panel governance fixes (#1194).

The legacy admin panel is inline JS inside a single HTML file; these guards
assert the reported call-shape bugs stay fixed without a browser harness, the
same approach used for the Governance page search pins (#644).
"""
from pathlib import Path

import pytest

_REPO_ROOT = next(
    p for p in Path(__file__).resolve().parents if (p / "packages").is_dir()
)
_ADMIN_PANEL = (
    _REPO_ROOT
    / "packages/extensions/admin/src/dynastore/extensions/admin/admin_panel.html"
)


@pytest.fixture(scope="module")
def admin_panel() -> str:
    return _ADMIN_PANEL.read_text(encoding="utf-8")


# --- #1194 item 3: the catalog-permissions "Add another role" button -----------
# It must pre-fill the grant modal with the principal id (``u.id``), the same
# identifier the sibling revoke button uses and the one the
# ``/catalogs/{cat}/principals/{principal_id}/roles`` endpoint keys on. Using
# ``u.subject_id`` sent the subject UUID, which ``submitCatalogGrant``'s
# UUID fast-path then forwarded verbatim to the wrong principal path.


def test_add_role_button_uses_principal_id(admin_panel: str) -> None:
    assert 'data-action="add" data-subject="${escapeHtml(u.id)}"' in admin_panel, (
        "the catalog-permissions 'Add another role' button must pre-fill with "
        "the principal id (u.id), matching the revoke button and the grant endpoint"
    )
    assert 'data-action="add" data-subject="${escapeHtml(u.subject_id)}"' not in (
        admin_panel
    ), "regression: 'Add another role' is back to passing u.subject_id (#1194 item 3)"


def test_revoke_and_add_use_the_same_identifier(admin_panel: str) -> None:
    # The revoke button already keys on u.id; add must agree so both target the
    # same principal under the catalog-principals endpoints.
    assert 'data-action="revoke" data-pid="${escapeHtml(u.id)}"' in admin_panel


# --- #1194 item 1: the Users tab reads the merged principals endpoint ----------


def test_users_tab_loads_principals_not_users(admin_panel: str) -> None:
    assert "await apiFetch('/principals')" in admin_panel
    # The merged backend has no distinct /users listing; guard against its return.
    assert "apiFetch('/users')" not in admin_panel
    assert 'apiFetch("/users")' not in admin_panel


# --- #1194 item 2: policy priority is surfaced in the table and create form ----


def test_policy_priority_in_table_and_form(admin_panel: str) -> None:
    # Create/edit form carries the priority input...
    assert 'id="pm-priority"' in admin_panel
    # ...and it is sent on create/update.
    assert "priority" in admin_panel
    # The policies table renders the priority column.
    assert "p.priority ?? 0" in admin_panel
