// Governance page — roles, policies, principal role-bindings.
// Scope = platform or catalog; sysadmin gets both, catalog-admin only their own.

import {
  fetchMe,
  fetchMyCatalogs,
  listRoles, createRole, updateRole, deleteRole,
  listPolicies, createPolicy, updatePolicy, deletePolicy,
  searchPrincipals,
  assignGlobalRole, removeGlobalRole,
  assignCatalogRole, removeCatalogRole,
} from "../common/api.js";
import { mountContextBar } from "../common/context-bar.js";

const $ = (s, root = document) => root.querySelector(s);

const state = {
  scope: { kind: "platform" },
  isSysadmin: false,
  ownedCatalogs: [],   // list of {catalog_id, roles}
  roles: [],
  policies: [],
  principals: [],
  editingRoleName: null,
};

function csv(s) {
  return String(s || "")
    .split(",")
    .map((t) => t.trim())
    .filter(Boolean);
}

function clearNode(node) {
  while (node.firstChild) node.removeChild(node.firstChild);
}

function setStatus(sel, msg, cls = "") {
  const el = $(sel);
  if (!el) return;
  el.textContent = msg || "";
  el.className = "status " + cls;
}

function scopeCatalogId() {
  if (state.scope.kind === "catalog") return state.scope.catalogId;
  if (state.scope.kind === "collection") return state.scope.catalogId;
  return null;
}

function canWriteAtScope() {
  if (state.isSysadmin) return true;
  const cid = scopeCatalogId();
  if (!cid) return false;
  return state.ownedCatalogs.some((c) => c.catalog_id === cid);
}

// --- Roles --------------------------------------------------------------

async function refreshRoles() {
  const tbody = $("#roles-table tbody");
  clearNode(tbody);
  const loading = document.createElement("tr");
  loading.className = "empty-row";
  const td = document.createElement("td");
  td.colSpan = 5;
  td.textContent = "Loading…";
  loading.appendChild(td);
  tbody.appendChild(loading);

  try {
    state.roles = await listRoles(scopeCatalogId());
  } catch (e) {
    setStatus("#role-status", `Load failed: ${e.message}`, "err");
    return;
  }
  clearNode(tbody);

  if (!state.roles.length) {
    const tr = document.createElement("tr");
    tr.className = "empty-row";
    const td = document.createElement("td");
    td.colSpan = 5;
    td.textContent = "No roles at this scope.";
    tr.appendChild(td);
    tbody.appendChild(tr);
    return;
  }

  const canWrite = canWriteAtScope();
  for (const r of state.roles) {
    const tr = document.createElement("tr");
    const name = document.createElement("td");
    name.textContent = r.name;
    const desc = document.createElement("td");
    desc.textContent = r.description || "";
    const pols = document.createElement("td");
    pols.className = "muted";
    pols.textContent = (r.policies || []).join(", ");
    const parents = document.createElement("td");
    parents.className = "muted";
    parents.textContent = (r.parent_roles || []).join(", ");

    const actions = document.createElement("td");
    actions.style.textAlign = "right";
    const edit = document.createElement("button");
    edit.className = "btn btn-secondary btn-xs";
    edit.textContent = "Edit";
    edit.style.marginRight = "4px";
    edit.disabled = !canWrite;
    edit.addEventListener("click", () => enterEditRoleMode(r));
    actions.appendChild(edit);

    const del = document.createElement("button");
    del.className = "btn btn-danger btn-xs";
    del.textContent = "Delete";
    del.disabled = !canWrite;
    del.addEventListener("click", async () => {
      if (!confirm(`Delete role "${r.name}"?`)) return;
      try {
        await deleteRole(r.name, scopeCatalogId());
        setStatus("#role-status", `Deleted ${r.name}.`, "ok");
        if (state.editingRoleName === r.name) exitEditRoleMode();
        refreshRoles();
      } catch (e) {
        setStatus("#role-status", `Delete failed: ${e.message}`, "err");
      }
    });
    actions.appendChild(del);

    tr.append(name, desc, pols, parents, actions);
    tbody.appendChild(tr);
  }
}

function enterEditRoleMode(r) {
  state.editingRoleName = r.name;
  $("#role-name").value = r.name;
  $("#role-name").readOnly = true;
  $("#role-description").value = r.description || "";
  $("#role-policies").value = (r.policies || []).join(", ");
  $("#role-parents").value = (r.parent_roles || []).join(", ");
  $("#role-form-title").textContent = `Edit role '${r.name}'`;
  $("#role-form-route").textContent = `PUT /admin/roles/${r.name}`;
  $("#role-submit-btn").textContent = "Update";
  $("#role-cancel-btn").style.display = "";
  setStatus("#role-status", "", "");
  $("#role-description").focus();
}

function exitEditRoleMode() {
  state.editingRoleName = null;
  $("#role-create").reset();
  $("#role-name").readOnly = false;
  $("#role-form-title").textContent = "Charter a role";
  $("#role-form-route").textContent = "POST /admin/roles";
  $("#role-submit-btn").textContent = "Charter";
  $("#role-cancel-btn").style.display = "none";
}

async function onSubmitRole(e) {
  e.preventDefault();
  if (!canWriteAtScope()) {
    setStatus("#role-status", "You cannot modify roles at this scope.", "err");
    return;
  }
  const editing = state.editingRoleName;
  if (editing) {
    const patch = {
      description: $("#role-description").value.trim() || null,
      policies: csv($("#role-policies").value),
      parent_roles: csv($("#role-parents").value),
    };
    try {
      await updateRole(editing, patch, scopeCatalogId());
      setStatus("#role-status", `Updated ${editing}.`, "ok");
      exitEditRoleMode();
      refreshRoles();
    } catch (err) {
      setStatus("#role-status", `Update failed: ${err.message}`, "err");
    }
    return;
  }
  const body = {
    name: $("#role-name").value.trim(),
    description: $("#role-description").value.trim() || null,
    policies: csv($("#role-policies").value),
    parent_roles: csv($("#role-parents").value),
  };
  if (!body.name) {
    setStatus("#role-status", "Name is required.", "err");
    return;
  }
  try {
    await createRole(body, scopeCatalogId());
    setStatus("#role-status", "Created.", "ok");
    $("#role-create").reset();
    refreshRoles();
  } catch (err) {
    setStatus("#role-status", `Create failed: ${err.message}`, "err");
  }
}

// --- Policies -----------------------------------------------------------

async function refreshPolicies() {
  const tbody = $("#policies-table tbody");
  clearNode(tbody);
  const loading = document.createElement("tr");
  loading.className = "empty-row";
  const td = document.createElement("td");
  td.colSpan = 5;
  td.textContent = "Loading…";
  loading.appendChild(td);
  tbody.appendChild(loading);

  try {
    state.policies = await listPolicies(scopeCatalogId());
  } catch (e) {
    setStatus("#policy-status", `Load failed: ${e.message}`, "err");
    return;
  }
  renderPolicies();
}

function renderPolicies() {
  const tbody = $("#policies-table tbody");
  clearNode(tbody);

  if (!state.policies.length) {
    const tr = document.createElement("tr");
    tr.className = "empty-row";
    const td = document.createElement("td");
    td.colSpan = 5;
    td.textContent = "No policies at this scope.";
    tr.appendChild(td);
    tbody.appendChild(tr);
    return;
  }

  const queryInput = $("#policies-query");
  const q = queryInput ? queryInput.value.trim().toLowerCase() : "";
  const rows = q
    ? state.policies.filter((p) =>
        (p.id || "").toLowerCase().includes(q) ||
        (p.description || "").toLowerCase().includes(q),
      )
    : state.policies;

  if (!rows.length) {
    const tr = document.createElement("tr");
    tr.className = "empty-row";
    const td = document.createElement("td");
    td.colSpan = 5;
    td.textContent = `No policies match "${q}".`;
    tr.appendChild(td);
    tbody.appendChild(tr);
    return;
  }

  const canWrite = canWriteAtScope();
  for (const p of rows) {
    const tr = document.createElement("tr");
    const id = document.createElement("td");
    id.textContent = p.id;
    // Surface rate_limit / max_count conditions inline so operators
    // picking a policy for an ad-hoc role can see at-a-glance which
    // policies gate traffic. Type slugs come from a fixed handler set.
    const conds = p.conditions || [];
    if (conds.some((c) => c.type === "rate_limit")) {
      const b = document.createElement("span");
      b.className = "chip limit-rate";
      b.textContent = "rate";
      b.title = "Has rate_limit condition";
      b.style.marginLeft = "6px";
      id.appendChild(b);
    }
    if (conds.some((c) => c.type === "max_count")) {
      const b = document.createElement("span");
      b.className = "chip limit-quota";
      b.textContent = "quota";
      b.title = "Has max_count condition";
      b.style.marginLeft = "6px";
      id.appendChild(b);
    }
    const effect = document.createElement("td");
    const eChip = document.createElement("span");
    eChip.className = `chip effect-${p.effect}`;
    eChip.textContent = p.effect;
    effect.appendChild(eChip);
    const acts = document.createElement("td");
    acts.className = "muted";
    acts.textContent = (p.actions || []).join(", ");
    const res = document.createElement("td");
    res.className = "muted";
    res.style.maxWidth = "420px";
    res.style.overflow = "hidden";
    res.style.textOverflow = "ellipsis";
    res.style.whiteSpace = "nowrap";
    res.textContent = (p.resources || []).join(", ");
    res.title = (p.resources || []).join("\n");

    const actions = document.createElement("td");
    actions.style.textAlign = "right";
    const del = document.createElement("button");
    del.className = "btn btn-danger btn-xs";
    del.textContent = "Delete";
    del.disabled = !canWrite;
    del.addEventListener("click", async () => {
      if (!confirm(`Delete policy "${p.id}"?`)) return;
      try {
        await deletePolicy(p.id, scopeCatalogId());
        setStatus("#policy-status", `Deleted ${p.id}.`, "ok");
        refreshPolicies();
      } catch (e) {
        setStatus("#policy-status", `Delete failed: ${e.message}`, "err");
      }
    });
    actions.appendChild(del);

    tr.append(id, effect, acts, res, actions);
    tbody.appendChild(tr);
  }
}

async function onCreatePolicy(e) {
  e.preventDefault();
  if (!canWriteAtScope()) {
    setStatus("#policy-status", "You cannot create policies at this scope.", "err");
    return;
  }
  const body = {
    id: $("#policy-id").value.trim(),
    description: $("#policy-description").value.trim() || null,
    actions: csv($("#policy-actions").value),
    resources: csv($("#policy-resources").value),
    effect: $("#policy-effect").value,
  };
  if (!body.id) {
    setStatus("#policy-status", "ID is required.", "err");
    return;
  }
  if (!body.actions.length || !body.resources.length) {
    setStatus("#policy-status", "Actions and resources are required.", "err");
    return;
  }
  try {
    await createPolicy(body, scopeCatalogId());
    setStatus("#policy-status", "Created.", "ok");
    $("#policy-create").reset();
    refreshPolicies();
  } catch (e) {
    setStatus("#policy-status", `Create failed: ${e.message}`, "err");
  }
}

// --- Principals ---------------------------------------------------------

async function refreshPrincipals() {
  const q = $("#principals-query").value.trim();
  setStatus("#principals-status", "Loading…", "");
  try {
    state.principals = await searchPrincipals({
      q: q || undefined,
      catalogId: scopeCatalogId() || undefined,
      limit: 50,
    });
    setStatus("#principals-status", `${state.principals.length} result(s)`, "ok");
  } catch (e) {
    setStatus("#principals-status", `Search failed: ${e.message}`, "err");
    return;
  }
  renderPrincipals();
}

function renderPrincipals() {
  const tbody = $("#principals-table tbody");
  clearNode(tbody);
  if (!state.principals.length) {
    const tr = document.createElement("tr");
    tr.className = "empty-row";
    const td = document.createElement("td");
    td.colSpan = 4;
    td.textContent = "No principals found.";
    tr.appendChild(td);
    tbody.appendChild(tr);
    return;
  }
  const canWrite = canWriteAtScope();
  for (const p of state.principals) {
    const tr = document.createElement("tr");
    const ident = document.createElement("td");
    ident.textContent = p.display_name || p.subject_id || p.id;
    const provider = document.createElement("td");
    provider.textContent = p.provider || "—";
    provider.className = "muted";

    const rolesCell = document.createElement("td");
    for (const rn of p.roles || []) {
      const chip = document.createElement("span");
      chip.className = "chip";
      chip.textContent = rn;
      if (canWrite) {
        const x = document.createElement("button");
        x.className = "btn btn-xs";
        x.style.cssText = "margin-left:4px;padding:0 4px;background:transparent;color:inherit;border:none;";
        x.textContent = "×";
        x.title = "Remove role";
        x.addEventListener("click", () => unassign(p, rn));
        chip.appendChild(x);
      }
      rolesCell.appendChild(chip);
    }
    if (!(p.roles || []).length) {
      const em = document.createElement("span");
      em.className = "muted";
      em.textContent = "—";
      rolesCell.appendChild(em);
    }

    const actions = document.createElement("td");
    actions.style.textAlign = "right";

    const sel = document.createElement("select");
    sel.style.marginRight = "6px";
    const opt0 = document.createElement("option");
    opt0.value = "";
    opt0.textContent = "Choose role…";
    sel.appendChild(opt0);
    for (const r of state.roles) {
      const o = document.createElement("option");
      o.value = r.name;
      o.textContent = r.name;
      sel.appendChild(o);
    }

    const add = document.createElement("button");
    add.className = "btn btn-secondary btn-xs";
    add.textContent = "Assign";
    add.disabled = !canWrite;
    add.addEventListener("click", () => {
      if (!sel.value) return;
      assign(p, sel.value);
    });

    actions.appendChild(sel);
    actions.appendChild(add);

    tr.append(ident, provider, rolesCell, actions);
    tbody.appendChild(tr);
  }
}

async function assign(principal, role) {
  const cid = scopeCatalogId();
  try {
    if (cid) {
      await assignCatalogRole(principal.id, cid, role);
    } else {
      await assignGlobalRole(principal.id, role);
    }
    setStatus("#principals-status", `Assigned ${role} to ${principal.display_name || principal.id}.`, "ok");
    refreshPrincipals();
  } catch (e) {
    setStatus("#principals-status", `Assign failed: ${e.message}`, "err");
  }
}

async function unassign(principal, role) {
  if (!confirm(`Remove role "${role}" from ${principal.display_name || principal.id}?`)) return;
  const cid = scopeCatalogId();
  try {
    if (cid) {
      await removeCatalogRole(principal.id, cid, role);
    } else {
      await removeGlobalRole(principal.id, role);
    }
    setStatus("#principals-status", `Removed ${role}.`, "ok");
    refreshPrincipals();
  } catch (e) {
    setStatus("#principals-status", `Remove failed: ${e.message}`, "err");
  }
}

// --- Tabs ---------------------------------------------------------------

function switchTab(name) {
  document.querySelectorAll(".tab-btn").forEach((b) => {
    b.classList.toggle("active", b.dataset.tab === name);
  });
  document.querySelectorAll(".tab-panel").forEach((p) => {
    p.classList.toggle("active", p.id === `tab-${name}`);
  });
}

// --- Boot ---------------------------------------------------------------

async function boot() {
  const me = await fetchMe();
  const roles = me.roles || [];
  state.isSysadmin = roles.includes("sysadmin");

  try {
    state.ownedCatalogs = await fetchMyCatalogs();
  } catch (_) {
    state.ownedCatalogs = [];
  }
  const hasCatalogScope = state.ownedCatalogs.length > 0;

  if (!state.isSysadmin && !hasCatalogScope) {
    const main = document.querySelector("main");
    clearNode(main);
    const p = document.createElement("p");
    p.className = "subtitle";
    p.textContent =
      "You need the 'sysadmin' role, or a role on at least one catalog, to access Governance.";
    main.appendChild(p);
    return;
  }

  // Footnote about missing rate-limit / quota
  if (state.isSysadmin) $("#footnote").style.display = "";

  mountContextBar($("#context-bar"), {
    onChange: async (scope) => {
      // Catalog admins can't browse platform scope.
      if (!state.isSysadmin && scope.kind === "platform") {
        scope = { kind: "catalog", catalogId: state.ownedCatalogs[0].catalog_id };
      }
      state.scope = scope;
      if (state.editingRoleName) exitEditRoleMode();
      await Promise.all([refreshRoles(), refreshPolicies()]);
      if ($("#tab-principals").classList.contains("active")) refreshPrincipals();
    },
  });

  document.querySelectorAll(".tab-btn").forEach((b) => {
    b.addEventListener("click", () => {
      const name = b.dataset.tab;
      switchTab(name);
      if (name === "principals" && !state.principals.length) refreshPrincipals();
    });
  });

  $("#role-create").addEventListener("submit", onSubmitRole);
  $("#role-cancel-btn").addEventListener("click", exitEditRoleMode);
  $("#roles-refresh").addEventListener("click", refreshRoles);
  $("#policy-create").addEventListener("submit", onCreatePolicy);
  $("#policies-refresh").addEventListener("click", refreshPolicies);
  $("#policies-query").addEventListener("input", () => {
    if (state.policies.length) renderPolicies();
  });
  $("#principals-refresh").addEventListener("click", refreshPrincipals);
  $("#principals-query").addEventListener("keydown", (e) => {
    if (e.key === "Enter") {
      e.preventDefault();
      refreshPrincipals();
    }
  });
}

boot();
