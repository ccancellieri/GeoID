// Governance page — roles, policies, principal role-bindings.
// Scope = platform or catalog; sysadmin gets both, catalog-admin only their own.

import {
  fetchMe,
  fetchMyCatalogs,
  listRoles, createRole, updateRole, deleteRole,
  listPolicies, createPolicy, updatePolicy, deletePolicy,
  searchPrincipals,
  listGrants, createGrant, deleteGrant,
  fetchEffectivePermissions,
  getCatalogProvisioning,
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
  // Principals → binding editor (#1346) — picked subject for the binding form.
  bindingSubject: null,    // { id, label }
  bindings: [],            // list of grant rows for the picked subject at scope
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
  td.colSpan = 4;
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
    td.colSpan = 4;
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

    const actions = document.createElement("td");
    actions.className = "table-actions";
    const edit = document.createElement("button");
    edit.className = "btn btn-secondary btn-xs";
    edit.textContent = "Edit";
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

    tr.append(name, desc, pols, actions);
    tbody.appendChild(tr);
  }
}

function enterEditRoleMode(r) {
  state.editingRoleName = r.name;
  $("#role-name").value = r.name;
  $("#role-name").readOnly = true;
  $("#role-description").value = r.description || "";
  $("#role-policies").value = (r.policies || []).join(", ");
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
    td.colSpan = 6;
    td.textContent = "No policies at this scope.";
    tr.appendChild(td);
    tbody.appendChild(tr);
    return;
  }

  const queryInput = $("#policies-query");
  const q = queryInput ? queryInput.value.trim().toLowerCase() : "";
  const rows = q
    ? state.policies.filter((p) => {
        const id = (p.id || "").toLowerCase();
        const desc = (p.description || "").toLowerCase();
        const acts = (p.actions || []).join(" ").toLowerCase();
        const res = (p.resources || []).join(" ").toLowerCase();
        return id.includes(q) || desc.includes(q) || acts.includes(q) || res.includes(q);
      })
    : state.policies;

  if (!rows.length) {
    const tr = document.createElement("tr");
    tr.className = "empty-row";
    const td = document.createElement("td");
    td.colSpan = 6;
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

    const prio = document.createElement("td");
    const prioInput = document.createElement("input");
    prioInput.type = "number";
    prioInput.min = "-1000";
    prioInput.max = "1000";
    prioInput.step = "1";
    prioInput.value = String(p.priority ?? 0);
    prioInput.disabled = !canWrite;
    prioInput.className = "input-num";
    prioInput.title = "Higher wins; ties → DENY. Range [-1000, 1000].";
    prioInput.addEventListener("change", async () => {
      const next = Number(prioInput.value);
      if (!Number.isFinite(next) || next < -1000 || next > 1000) {
        setStatus("#policy-status", "Priority must be an integer in [-1000, 1000].", "err");
        prioInput.value = String(p.priority ?? 0);
        return;
      }
      if (next === (p.priority ?? 0)) return;
      try {
        await updatePolicy(p.id, { priority: next }, scopeCatalogId());
        p.priority = next;
        setStatus("#policy-status", `Updated ${p.id} priority → ${next}.`, "ok");
      } catch (e) {
        setStatus("#policy-status", `Update failed: ${e.message}`, "err");
        prioInput.value = String(p.priority ?? 0);
      }
    });
    prio.appendChild(prioInput);

    const acts = document.createElement("td");
    acts.className = "muted";
    acts.textContent = (p.actions || []).join(", ");
    const res = document.createElement("td");
    res.className = "muted cell-truncate";
    res.textContent = (p.resources || []).join(", ");
    res.title = (p.resources || []).join("\n");

    const actions = document.createElement("td");
    actions.className = "table-actions";
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

    tr.append(id, effect, prio, acts, res, actions);
    tbody.appendChild(tr);
  }
}

async function onCreatePolicy(e) {
  e.preventDefault();
  if (!canWriteAtScope()) {
    setStatus("#policy-status", "You cannot create policies at this scope.", "err");
    return;
  }
  const prioRaw = $("#policy-priority").value;
  const priority = prioRaw === "" ? 0 : Number(prioRaw);
  if (!Number.isFinite(priority) || priority < -1000 || priority > 1000) {
    setStatus("#policy-status", "Priority must be an integer in [-1000, 1000].", "err");
    return;
  }
  const body = {
    id: $("#policy-id").value.trim(),
    description: $("#policy-description").value.trim() || null,
    actions: csv($("#policy-actions").value),
    resources: csv($("#policy-resources").value),
    effect: $("#policy-effect").value,
    priority,
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
  const pickedId = state.bindingSubject && state.bindingSubject.id;
  for (const p of state.principals) {
    const tr = document.createElement("tr");
    const ident = document.createElement("td");
    ident.textContent = p.display_name || p.subject_id || p.id;
    const provider = document.createElement("td");
    provider.textContent = p.provider || "—";
    provider.className = "muted";

    // "Bindings (at scope)" column — kept compact: shows the role names the
    // principal currently has at this scope as plain chips. Per-binding
    // mutation (deny / validity / quota / kind=policy) happens in the
    // dedicated editor below, not inline here.
    const rolesCell = document.createElement("td");
    for (const rn of p.roles || []) {
      const chip = document.createElement("span");
      chip.className = "chip";
      chip.textContent = rn;
      rolesCell.appendChild(chip);
    }
    if (!(p.roles || []).length) {
      const em = document.createElement("span");
      em.className = "muted";
      em.textContent = "—";
      rolesCell.appendChild(em);
    }

    const actions = document.createElement("td");
    actions.className = "table-actions";
    const pick = document.createElement("button");
    pick.className = "btn btn-secondary btn-xs";
    pick.textContent = pickedId === p.id ? "Editing" : "Manage bindings";
    pick.disabled = !canWrite;
    pick.addEventListener("click", () => pickSubject(p));
    actions.appendChild(pick);

    tr.append(ident, provider, rolesCell, actions);
    tbody.appendChild(tr);
  }
}

// ---- Binding editor (#1346) ---------------------------------------------
//
// The legacy assign/remove role surface (POST /admin/.../roles +
// DELETE) is allow-only by contract. The new endpoints —
// POST/GET/DELETE /admin/{platform,catalogs/{cat}}/grants — take the
// generic CreateBindingRequest shape (effect, valid_from, valid_until,
// object_kind=role|policy, quota). We drive the form from those.

function currentGrantScope() {
  const cid = scopeCatalogId();
  if (cid) return { kind: "catalog", catalogId: cid };
  return { kind: "platform" };
}

function grantRouteHint() {
  const cid = scopeCatalogId();
  if (cid) return `POST /admin/catalogs/${cid}/grants`;
  return "POST /admin/platform/grants";
}

function subjectLabel(p) {
  return p.display_name || p.subject_id || p.id;
}

function pickSubject(p) {
  state.bindingSubject = { id: p.id, label: subjectLabel(p) };
  $("#binding-plate").style.display = "";
  $("#binding-form-subject").textContent = `Subject: ${state.bindingSubject.label}  (${state.bindingSubject.id})`;
  $("#binding-form-route").textContent = grantRouteHint();
  resetBindingForm();
  setStatus("#binding-status", "", "");
  refreshBindings();
  // Re-render the principals table so the "Editing" label updates.
  renderPrincipals();
  // Refresh the object_ref datalist for the current object_kind.
  refreshObjectRefSuggestions();
  $("#binding-plate").scrollIntoView({ behavior: "smooth", block: "start" });
}

function closeBindingEditor() {
  state.bindingSubject = null;
  state.bindings = [];
  $("#binding-plate").style.display = "none";
  renderPrincipals();
}

function resetBindingForm() {
  $("#binding-create").reset();
  $("#binding-object-kind").value = "role";
  $("#binding-effect").value = "allow";
}

function refreshObjectRefSuggestions() {
  const kind = $("#binding-object-kind").value;
  const list = $("#binding-object-ref-list");
  clearNode(list);
  const source = kind === "role" ? state.roles : state.policies;
  for (const r of source) {
    const opt = document.createElement("option");
    opt.value = kind === "role" ? r.name : r.id;
    list.appendChild(opt);
  }
}

function buildQuotaPayload() {
  // Raw JSON wins if non-empty — operator escape hatch for the
  // shapes the backend accepts but the two structured fields don't
  // express (e.g. burst windows, named buckets).
  const raw = $("#binding-quota-json").value.trim();
  if (raw) {
    try { return JSON.parse(raw); }
    catch (_e) { throw new Error("Quota JSON is not valid JSON."); }
  }
  const rate = $("#binding-quota-rate-limit").value.trim();
  const max = $("#binding-quota-max-count").value.trim();
  const out = {};
  if (rate !== "") {
    const n = Number(rate);
    if (!Number.isFinite(n) || n < 0) throw new Error("Rate limit must be a non-negative number.");
    out.rate_limit = { limit: n, window_seconds: 1 };
  }
  if (max !== "") {
    const n = Number(max);
    if (!Number.isFinite(n) || n < 0) throw new Error("Lifetime max count must be a non-negative number.");
    out.max_count = { limit: n };
  }
  return Object.keys(out).length ? out : null;
}

function fmtDateTime(value) {
  if (!value) return "—";
  try {
    return new Date(value).toISOString().replace("T", " ").slice(0, 19) + "Z";
  } catch (_e) {
    return String(value);
  }
}

async function refreshBindings() {
  const tbody = $("#bindings-table tbody");
  clearNode(tbody);
  const loading = document.createElement("tr");
  loading.className = "empty-row";
  const td = document.createElement("td");
  td.colSpan = 7;
  td.textContent = "Loading…";
  loading.appendChild(td);
  tbody.appendChild(loading);

  if (!state.bindingSubject) {
    clearNode(tbody);
    return;
  }

  try {
    state.bindings = await listGrants(currentGrantScope(), state.bindingSubject.id) || [];
  } catch (e) {
    clearNode(tbody);
    const tr = document.createElement("tr");
    tr.className = "empty-row";
    const errTd = document.createElement("td");
    errTd.colSpan = 7;
    errTd.textContent = `Load failed: ${e.message}`;
    tr.appendChild(errTd);
    tbody.appendChild(tr);
    return;
  }
  renderBindings();
}

function renderBindings() {
  const tbody = $("#bindings-table tbody");
  clearNode(tbody);
  if (!state.bindings.length) {
    const tr = document.createElement("tr");
    tr.className = "empty-row";
    const td = document.createElement("td");
    td.colSpan = 7;
    td.textContent = "No bindings on this principal at this scope.";
    tr.appendChild(td);
    tbody.appendChild(tr);
    return;
  }

  const canWrite = canWriteAtScope();
  for (const row of state.bindings) {
    const tr = document.createElement("tr");
    const effect = row.effect || "allow";
    if (effect === "deny") {
      // Deny rows: terracotta-tinted with a faint left border so they pop
      // visually against allow rows in a long list. Styling is centralised
      // in admin.css under .row-deny — we just toggle the class here.
      tr.className = "row-deny";
    }

    const kind = document.createElement("td");
    kind.textContent = row.object_kind || "role";

    const obj = document.createElement("td");
    const code = document.createElement("code");
    code.textContent = row.object_ref || "—";
    obj.appendChild(code);

    const eff = document.createElement("td");
    const chip = document.createElement("span");
    chip.className = `chip effect-${effect === "deny" ? "DENY" : "ALLOW"}`;
    chip.textContent = effect;
    eff.appendChild(chip);

    const vf = document.createElement("td");
    vf.className = "muted";
    vf.textContent = fmtDateTime(row.valid_from);
    const vu = document.createElement("td");
    vu.className = "muted";
    vu.textContent = fmtDateTime(row.valid_until);

    const quota = document.createElement("td");
    if (row.quota == null) {
      quota.className = "muted";
      quota.textContent = "—";
    } else {
      // Render via <pre>textContent so any operator-supplied JSON cannot
      // smuggle markup into the table.
      const pre = document.createElement("pre");
      pre.className = "cell-json";
      pre.textContent = typeof row.quota === "object"
        ? JSON.stringify(row.quota)
        : String(row.quota);
      quota.appendChild(pre);
    }

    const actions = document.createElement("td");
    actions.className = "table-actions";
    // "Why?" affordance — opens the effective-permissions explainer
    // pre-seeded with this binding's scope. Available regardless of
    // canWrite: it's a read-only diagnostic, useful exactly when the
    // operator can't mutate but needs to understand the verdict.
    const why = document.createElement("button");
    why.type = "button";
    why.className = "btn btn-ghost btn-xs";
    why.textContent = "Why?";
    why.setAttribute("aria-label", "Explain effective permissions for this binding");
    why.addEventListener("click", () => openExplainer(row));
    actions.appendChild(why);
    const del = document.createElement("button");
    del.className = "btn btn-danger btn-xs";
    del.textContent = "Revoke";
    del.disabled = !canWrite;
    del.addEventListener("click", () => revokeBinding(row));
    actions.appendChild(del);

    tr.append(kind, obj, eff, vf, vu, quota, actions);
    tbody.appendChild(tr);
  }
}

async function onSubmitBinding(e) {
  e.preventDefault();
  if (!state.bindingSubject) {
    setStatus("#binding-status", "Pick a principal first.", "err");
    return;
  }
  if (!canWriteAtScope()) {
    setStatus("#binding-status", "You cannot grant bindings at this scope.", "err");
    return;
  }
  const objectRef = $("#binding-object-ref").value.trim();
  if (!objectRef) {
    setStatus("#binding-status", "Object ref is required.", "err");
    return;
  }
  const body = {
    subject_id: state.bindingSubject.id,
    object_kind: $("#binding-object-kind").value,
    object_ref: objectRef,
    effect: $("#binding-effect").value,
  };
  const vf = $("#binding-valid-from").value;
  const vu = $("#binding-valid-until").value;
  if (vf) body.valid_from = new Date(vf).toISOString();
  if (vu) body.valid_until = new Date(vu).toISOString();
  let quota;
  try { quota = buildQuotaPayload(); }
  catch (err) { setStatus("#binding-status", err.message, "err"); return; }
  if (quota != null) body.quota = quota;

  try {
    await createGrant(currentGrantScope(), body);
    setStatus("#binding-status", "Binding granted.", "ok");
    resetBindingForm();
    refreshBindings();
    // Refresh the principal row so the "Bindings (at scope)" chips reflect
    // the new role/policy. Cheap: same search the user already ran.
    refreshPrincipals();
  } catch (err) {
    setStatus("#binding-status", `Grant failed: ${err.message}`, "err");
  }
}

async function revokeBinding(row) {
  if (!state.bindingSubject) return;
  const label = `${row.object_kind || "role"}:${row.object_ref}`;
  if (!confirm(`Revoke ${label} (${row.effect || "allow"}) from ${state.bindingSubject.label}?`)) return;
  try {
    await deleteGrant(currentGrantScope(), {
      subjectId: state.bindingSubject.id,
      objectKind: row.object_kind || "role",
      objectRef: row.object_ref,
      effect: row.effect || "allow",
    });
    setStatus("#binding-status", `Revoked ${label}.`, "ok");
    refreshBindings();
    refreshPrincipals();
  } catch (e) {
    setStatus("#binding-status", `Revoke failed: ${e.message}`, "err");
  }
}

// ---- Effective-permissions explainer (#1390) ----------------------------
//
// "Why?" per binding row → opens a dialog that asks for the action verb,
// then calls GET /admin/iam/effective and renders the trace returned by
// the same evaluate_access walk the hot path uses. Every server-derived
// field is set via textContent — no innerHTML on response data.

// Scope captured from the row at click time; the modal carries it across
// the "Re-run with different action" loop.
const explainer = {
  principalId: null,
  principalLabel: null,
  catalogId: null,
  collectionId: null,
  resourceKind: null,
  resourceRef: null,
};

function openExplainer(row) {
  if (!state.bindingSubject) return;
  explainer.principalId = state.bindingSubject.id;
  explainer.principalLabel = state.bindingSubject.label;
  // Scope = the same axis the bindings table is scoped to (platform vs
  // catalog), not the binding's own resource scope. resource_kind /
  // resource_ref reflect the binding's narrower scope when present so
  // the trace lines up with a collection-scoped grant.
  explainer.catalogId = scopeCatalogId();
  explainer.collectionId = (row && row.resource_kind === "collection")
    ? (row.resource_ref || null)
    : null;
  explainer.resourceKind = (row && row.resource_kind) || null;
  explainer.resourceRef = (row && row.resource_ref) || null;

  const subjectLine =
    `Subject: ${explainer.principalLabel}  (${explainer.principalId})`
    + (explainer.catalogId ? `  ·  catalog ${explainer.catalogId}` : "  ·  platform")
    + (explainer.resourceKind && explainer.resourceRef
        ? `  ·  ${explainer.resourceKind} ${explainer.resourceRef}`
        : "");
  $("#explainer-subject").textContent = subjectLine;
  $("#explainer-action").value = "GET";
  $("#explainer-action-custom").value = "";
  $("#explainer-action-custom").disabled = true;
  $("#explainer-result").style.display = "none";
  clearNode($("#explainer-grants tbody"));
  setStatus("#explainer-status", "", "");

  const dlg = $("#explainer-modal");
  if (typeof dlg.showModal === "function") dlg.showModal();
  else dlg.setAttribute("open", "");
}

function closeExplainer() {
  const dlg = $("#explainer-modal");
  if (typeof dlg.close === "function") dlg.close();
  else dlg.removeAttribute("open");
}

function selectedExplainerAction() {
  const sel = $("#explainer-action").value;
  if (sel === "__custom__") return $("#explainer-action-custom").value.trim();
  return sel;
}

async function runExplainer(e) {
  if (e) e.preventDefault();
  const action = selectedExplainerAction();
  if (!action) {
    setStatus("#explainer-status", "Pick or type an action to evaluate.", "err");
    return;
  }
  setStatus("#explainer-status", "Evaluating…", "");
  try {
    const data = await fetchEffectivePermissions({
      principalId: explainer.principalId,
      catalogId: explainer.catalogId,
      collectionId: explainer.collectionId,
      action,
      resourceKind: explainer.resourceKind,
      resourceRef: explainer.resourceRef,
    });
    setStatus("#explainer-status", "", "");
    renderExplainerResult(data);
  } catch (err) {
    setStatus("#explainer-status", `Failed: ${err.message}`, "err");
  }
}

// Returns the index of the winning grant in `grants_considered`, or -1
// if no grant matched. When deny_precedence_applied=true the winner is
// the first matched DENY; otherwise it's the first matched ALLOW. This
// mirrors the same ranking the engine logs as the decision reason.
function findWinningGrantIndex(data) {
  if (!data || !Array.isArray(data.grants_considered)) return -1;
  const winnerEffect = (data.decision === "deny"
                        && data.deny_precedence_applied)
    ? "deny"
    : (data.decision === "allow" ? "allow" : null);
  if (!winnerEffect) return -1;
  for (let i = 0; i < data.grants_considered.length; i++) {
    const g = data.grants_considered[i];
    if (g.matched && (g.effect || "allow") === winnerEffect) return i;
  }
  return -1;
}

function renderExplainerResult(data) {
  const result = $("#explainer-result");
  result.style.display = "";

  const verdict = data.decision === "allow" ? "ALLOW" : "DENY";
  const chip = $("#explainer-verdict-chip");
  chip.textContent = verdict;
  chip.className = `chip effect-${verdict}`;

  $("#explainer-deny-note").style.display =
    data.deny_precedence_applied ? "" : "none";

  // decision_reason is server-derived — textContent only.
  $("#explainer-reason").textContent = data.decision_reason || "";

  const tbody = $("#explainer-grants tbody");
  clearNode(tbody);
  const grants = Array.isArray(data.grants_considered) ? data.grants_considered : [];
  if (!grants.length) {
    const tr = document.createElement("tr");
    tr.className = "empty-row";
    const td = document.createElement("td");
    td.colSpan = 8;
    td.textContent = "No grants were walked for this request.";
    tr.appendChild(td);
    tbody.appendChild(tr);
    return;
  }
  const winnerIdx = findWinningGrantIndex(data);
  grants.forEach((g, idx) => {
    const tr = document.createElement("tr");
    const effect = (g.effect === "deny") ? "deny" : "allow";
    if (idx === winnerIdx) {
      // Mark the deciding grant. row-deny already exists for deny rows;
      // row-winner is an additive highlight so the operator can spot the
      // chosen row at a glance regardless of effect.
      tr.classList.add("row-winner");
      if (effect === "deny") tr.classList.add("row-deny");
    }

    const kindCell = document.createElement("td");
    kindCell.textContent = g.object_kind || "policy";

    const refCell = document.createElement("td");
    const refCode = document.createElement("code");
    refCode.textContent = g.object_ref || "—";
    refCell.appendChild(refCode);

    const effCell = document.createElement("td");
    const effChip = document.createElement("span");
    effChip.className = `chip effect-${effect === "deny" ? "DENY" : "ALLOW"}`;
    effChip.textContent = effect;
    effCell.appendChild(effChip);

    const scopeCell = document.createElement("td");
    if (g.resource_kind && g.resource_ref) {
      scopeCell.textContent = `${g.resource_kind} ${g.resource_ref}`;
    } else {
      scopeCell.className = "muted";
      scopeCell.textContent = "whole-catalog";
    }

    const matchedCell = document.createElement("td");
    matchedCell.textContent = g.matched ? "✓" : "✗";
    matchedCell.className = g.matched ? "" : "muted";

    const whyCell = document.createElement("td");
    whyCell.className = "muted";
    whyCell.textContent = g.why_not || "";

    const validityCell = document.createElement("td");
    validityCell.className = "muted";
    if (!g.valid_from && !g.valid_until) {
      validityCell.textContent = "—";
    } else {
      const inWin = g.in_validity_window !== false;
      validityCell.textContent = `${fmtDateTime(g.valid_from)} → ${fmtDateTime(g.valid_until)}`
        + (inWin ? "" : " (out of window)");
    }

    const condCell = document.createElement("td");
    const conds = Array.isArray(g.conditions_evaluated) ? g.conditions_evaluated : [];
    if (!conds.length) {
      condCell.className = "muted";
      condCell.textContent = "—";
    } else {
      const pre = document.createElement("pre");
      pre.className = "cell-json";
      // Server-derived condition trace: pretty-print as JSON via
      // textContent so neither the type strings nor the handler
      // detail can carry markup into the DOM.
      pre.textContent = JSON.stringify(conds, null, 2);
      condCell.appendChild(pre);
    }

    tr.append(kindCell, refCell, effCell, scopeCell, matchedCell,
              whyCell, validityCell, condCell);
    tbody.appendChild(tr);
  });
}

// --- Provisioning -------------------------------------------------------

const PROV_POLLING_STATES = new Set(["pending", "provisioning"]);
let _provPollTimer = null;

function _provBadgeClass(status) {
  if (status === "ready") return "chip effect-ALLOW";
  if (status === "provisioning" || status === "pending") return "chip limit-rate";
  if (status === "failed" || status === "conflict") return "chip effect-DENY";
  return "chip";
}

function _taskBadgeClass(status) {
  const s = (status || "").toUpperCase();
  if (s === "COMPLETED") return "chip effect-ALLOW";
  if (s === "FAILED" || s === "DEAD_LETTER") return "chip effect-DENY";
  if (s === "ACTIVE" || s === "RUNNING" || s === "PENDING") return "chip limit-rate";
  return "chip";
}

function _renderProvisioningResult(data) {
  const result = $("#prov-result");
  result.style.display = "";

  const badge = $("#prov-badge");
  badge.textContent = data.provisioning_status;
  badge.className = _provBadgeClass(data.provisioning_status);

  $("#prov-schema").textContent = data.physical_schema || "—";

  const taskBlock = $("#prov-task-block");
  if (data.task) {
    taskBlock.style.display = "";
    $("#prov-task-id").textContent = data.task.task_id;
    const tb = $("#prov-task-badge");
    tb.textContent = data.task.status;
    tb.className = _taskBadgeClass(data.task.status);
    $("#prov-task-retries").textContent =
      `${data.task.retry_count} / ${data.task.max_retries}`;
    const updated = data.task.updated_at || data.task.created_at;
    $("#prov-task-updated").textContent = updated
      ? new Date(updated).toLocaleString()
      : "—";
    const errRow = $("#prov-task-error-row");
    if (data.task.error_message) {
      errRow.style.display = "";
      $("#prov-task-error").textContent = data.task.error_message.slice(0, 500);
    } else {
      errRow.style.display = "none";
    }
  } else {
    taskBlock.style.display = "none";
  }
}

function _stopProvPoll() {
  if (_provPollTimer !== null) {
    clearTimeout(_provPollTimer);
    _provPollTimer = null;
  }
}

async function checkProvisioning() {
  _stopProvPoll();
  const catalogId = $("#prov-catalog-id").value.trim();
  if (!catalogId) {
    setStatus("#prov-status", "Enter a catalog ID.", "err");
    return;
  }
  setStatus("#prov-status", "Checking…", "");
  try {
    const data = await getCatalogProvisioning(catalogId);
    setStatus("#prov-status", "", "");
    _renderProvisioningResult(data);
    if (PROV_POLLING_STATES.has(data.provisioning_status)) {
      _provPollTimer = setTimeout(checkProvisioning, 5000);
    }
  } catch (e) {
    setStatus("#prov-status", `Error: ${e.message}`, "err");
    $("#prov-result").style.display = "none";
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
      // Scope change invalidates the picked subject's binding view —
      // bindings are scope-specific (platform vs catalog).
      if (state.bindingSubject) closeBindingEditor();
      await Promise.all([refreshRoles(), refreshPolicies()]);
      if ($("#tab-principals").classList.contains("active")) refreshPrincipals();
      refreshObjectRefSuggestions();
    },
  });

  document.querySelectorAll(".tab-btn").forEach((b) => {
    b.addEventListener("click", () => {
      const name = b.dataset.tab;
      if (name !== "provisioning") _stopProvPoll();
      switchTab(name);
      if (name === "principals" && !state.principals.length) refreshPrincipals();
    });
  });

  $("#role-create").addEventListener("submit", onSubmitRole);
  $("#role-cancel-btn").addEventListener("click", exitEditRoleMode);
  $("#roles-refresh").addEventListener("click", refreshRoles);
  $("#policy-create").addEventListener("submit", onCreatePolicy);
  $("#policies-refresh").addEventListener("click", refreshPolicies);
  $("#policies-query").addEventListener("input", async () => {
    if (!state.policies.length) {
      await refreshPolicies();
      return;
    }
    renderPolicies();
  });
  $("#principals-refresh").addEventListener("click", refreshPrincipals);
  $("#principals-query").addEventListener("keydown", (e) => {
    if (e.key === "Enter") {
      e.preventDefault();
      refreshPrincipals();
    }
  });

  // Binding editor (#1346)
  $("#binding-create").addEventListener("submit", onSubmitBinding);
  $("#binding-cancel-btn").addEventListener("click", closeBindingEditor);
  $("#binding-object-kind").addEventListener("change", () => {
    refreshObjectRefSuggestions();
    // Clear the previous suggestion when switching kinds — a role name
    // is rarely a valid policy id and vice versa, so leaving the value
    // sitting in the box would just cause a 422 on submit.
    $("#binding-object-ref").value = "";
  });

  $("#prov-check-btn").addEventListener("click", checkProvisioning);
  $("#prov-catalog-id").addEventListener("keydown", (e) => {
    if (e.key === "Enter") {
      e.preventDefault();
      checkProvisioning();
    }
  });

  // Explainer modal (#1390) — submit triggers the GET, close button
  // dismisses, action select toggles the free-text fallback.
  const explainerForm = $("#explainer-form");
  if (explainerForm) {
    explainerForm.addEventListener("submit", runExplainer);
    $("#explainer-close-btn").addEventListener("click", closeExplainer);
    $("#explainer-action").addEventListener("change", () => {
      const custom = $("#explainer-action").value === "__custom__";
      const input = $("#explainer-action-custom");
      input.disabled = !custom;
      if (custom) input.focus();
    });
  }
}

boot();
