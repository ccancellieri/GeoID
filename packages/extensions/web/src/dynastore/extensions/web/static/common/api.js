// Thin fetch helper for the admin Hub. Non-2xx responses throw with the
// response body attached. Handles the three config scopes (platform,
// catalog, collection) uniformly so page code never builds URLs by hand.
//
// Every absolute path passed in is run through apiUrl() so the proxy prefix
// (e.g. /geospatial/v2/api/catalog) is preserved when served behind a
// reverse proxy. Call sites can keep passing absolute paths like
// "/admin/roles" — the prefix is applied transparently.

import { apiUrl } from "./url.js";

// Build an Authorization header from the Bearer token stored by the login
// flow in custom.js. Platform may override the storage key via
// window.DS_TOKEN_KEY (set from the platform config); we fall back to the
// shipped default "ds_token". Returns {} when no token is present so the
// call still goes out and surfaces the 401 to the page.
function authHeader() {
  if (typeof window === "undefined") return {};
  const key = window.DS_TOKEN_KEY || "ds_token";
  const ls = (typeof localStorage !== "undefined") ? localStorage : null;
  const ss = (typeof sessionStorage !== "undefined") ? sessionStorage : null;
  const token = (ls && ls.getItem(key))
    || (ss && ss.getItem(key))
    || (ls && ls.getItem("ds_token"))
    || (ss && ss.getItem("ds_token"));
  return token ? { Authorization: `Bearer ${token}` } : {};
}

export async function getJSON(url) {
  const target = apiUrl(url);
  const r = await fetch(target, {
    credentials: "same-origin",
    headers: { ...authHeader() },
  });
  if (!r.ok) {
    const text = await r.text().catch(() => "");
    const err = new Error(`${r.status} ${target}: ${text.slice(0, 400)}`);
    err.status = r.status;
    err.body = text;
    throw err;
  }
  return r.json();
}

export async function patchJSON(url, body) {
  const target = apiUrl(url);
  const r = await fetch(target, {
    method: "PATCH",
    headers: { "Content-Type": "application/json", ...authHeader() },
    credentials: "same-origin",
    body: JSON.stringify(body),
  });
  if (!r.ok) {
    const text = await r.text().catch(() => "");
    const err = new Error(`${r.status} ${target}: ${text.slice(0, 400)}`);
    err.status = r.status;
    err.body = text;
    throw err;
  }
  return r.json();
}

// scope: {kind: "platform"} | {kind: "catalog", catalogId} | {kind: "collection", catalogId, collectionId}
export function scopeBasePath(scope) {
  if (!scope || scope.kind === "platform") return "/configs";
  if (scope.kind === "catalog") {
    return `/configs/catalogs/${encodeURIComponent(scope.catalogId)}`;
  }
  if (scope.kind === "collection") {
    return `/configs/catalogs/${encodeURIComponent(scope.catalogId)}`
      + `/collections/${encodeURIComponent(scope.collectionId)}`;
  }
  throw new Error(`unknown scope kind: ${scope.kind}`);
}

// GET the composed/resolved or explicit config set at a scope.
// resolved=true returns inherited + overrides; resolved=false returns only
// overrides set at this scope.
//
// `meta` accepts the server's three-valued projection ("none" | "field" |
// "schema") OR a boolean for back-compat (true → "field", false → "none").
// `view` is the #947/#1112 provenance filter ("effective" | "delta" |
// "inherited"); omitted unless explicitly passed so older callers keep the
// server default. The returned querystring is exposed verbatim on the
// response under a non-enumerable `__qs` marker so the Wire tab can echo
// the exact GET it issued.
export function configSetQuery({ resolved = true, meta = false, view } = {}) {
  const metaVal = typeof meta === "string"
    ? meta
    : (meta ? "field" : "none");
  const params = {
    resolved: resolved ? "true" : "false",
    meta: metaVal,
  };
  if (view) params.view = view;
  return new URLSearchParams(params);
}

export async function fetchConfigSet(scope, opts = {}) {
  const base = scopeBasePath(scope);
  const qs = configSetQuery(opts);
  const path = `${base}?${qs.toString()}`;
  const body = await getJSON(path);
  if (body && typeof body === "object") {
    try {
      Object.defineProperty(body, "__path", {
        value: path, enumerable: false, configurable: true,
      });
    } catch { /* frozen response — ignore */ }
  }
  return body;
}

// Registry entry for one plugin class (snake_case plugin_id).
//   meta="none"  → { plugin_id, json_schema, description, scope }
//   meta="schema"→ raw JSON Schema 2020-12
//   meta="field" → { field_name: description } docs map
export function fetchSchema(pluginId, { meta = "none" } = {}) {
  const qs = meta && meta !== "none" ? `?meta=${encodeURIComponent(meta)}` : "";
  return getJSON(`/configs/registry/${encodeURIComponent(pluginId)}${qs}`);
}

// PATCH the composed endpoint at a scope. body is a shallow
// {ClassName: partial_dict | null} mapping. null deletes the override.
export function patchConfigSet(scope, body) {
  const base = scopeBasePath(scope);
  return patchJSON(base, body);
}

// Per-plugin CRUD: /configs[/catalogs/{id}[/collections/{c}]]/plugins/{plugin_id}
// (plugin_id is snake_case per the registry — see fetchSchemas)
export function classPath(scope, pluginId) {
  return `${scopeBasePath(scope)}/plugins/${encodeURIComponent(pluginId)}`;
}

// Plugin registry endpoint. Response shape:
// { plugin_id: { json_schema, description, scope } }
// plugin_id is snake_case (auto-derived from PascalCase class name).
export function fetchSchemas() {
  return getJSON("/configs/registry");
}

// Identity — used to gate admin pages. Returns { principal, roles }.
// `/iam/me` is the canonical "who am I + roles" surface. The OIDC-spec
// `/auth/userinfo` returns only the token's claim set (no platform roles).
export async function fetchMe() {
  try {
    return await getJSON("/iam/me");
  } catch (e) {
    if (e.status === 401) return { principal: null, roles: [] };
    throw e;
  }
}

export function fetchCatalogs() {
  return getJSON("/catalogs");
}

// ----- HTTP verbs used by admin pages -----

async function sendJSON(method, url, body, { expectJSON = true } = {}) {
  const target = apiUrl(url);
  const init = {
    method,
    credentials: "same-origin",
    headers: body == null
      ? { ...authHeader() }
      : { "Content-Type": "application/json", ...authHeader() },
  };
  if (body != null) init.body = JSON.stringify(body);
  const r = await fetch(target, init);
  if (!r.ok) {
    const text = await r.text().catch(() => "");
    const err = new Error(`${r.status} ${target}: ${text.slice(0, 400)}`);
    err.status = r.status;
    err.body = text;
    throw err;
  }
  if (r.status === 204 || !expectJSON) return null;
  const ct = r.headers.get("content-type") || "";
  if (!ct.includes("json")) return null;
  return r.json();
}

export const postJSON = (url, body) => sendJSON("POST", url, body);
export const putJSON = (url, body) => sendJSON("PUT", url, body);
export const deleteJSON = (url) => sendJSON("DELETE", url, null);

// ----- Governance: roles / policies / principals -----

const qs = (params) => {
  const out = new URLSearchParams();
  for (const [k, v] of Object.entries(params || {})) {
    if (v != null && v !== "") out.set(k, v);
  }
  const s = out.toString();
  return s ? `?${s}` : "";
};

export const listRoles = (catalogId) =>
  getJSON(`/admin/roles${qs({ catalog_id: catalogId })}`);
export const createRole = (role, catalogId) =>
  postJSON(`/admin/roles${qs({ catalog_id: catalogId })}`, role);
export const updateRole = (name, patch, catalogId) =>
  putJSON(
    `/admin/roles/${encodeURIComponent(name)}${qs({ catalog_id: catalogId })}`,
    patch,
  );
export const deleteRole = (name, catalogId) =>
  deleteJSON(
    `/admin/roles/${encodeURIComponent(name)}${qs({ catalog_id: catalogId })}`,
  );

export const listPolicies = (catalogId) =>
  getJSON(`/admin/policies${qs({ catalog_id: catalogId })}`);
export const createPolicy = (policy, catalogId) =>
  postJSON(`/admin/policies${qs({ catalog_id: catalogId })}`, policy);
export const updatePolicy = (id, patch, catalogId) =>
  putJSON(
    `/admin/policies/${encodeURIComponent(id)}${qs({ catalog_id: catalogId })}`,
    patch,
  );
export const deletePolicy = (id, catalogId) =>
  deleteJSON(
    `/admin/policies/${encodeURIComponent(id)}${qs({ catalog_id: catalogId })}`,
  );

export const searchPrincipals = ({ q, role, catalogId, limit = 50, offset = 0 } = {}) =>
  getJSON(
    `/admin/principals${qs({ q, role, catalog_id: catalogId, limit, offset })}`,
  );

export const listCatalogPrincipals = (catalogId) =>
  getJSON(`/admin/catalogs/${encodeURIComponent(catalogId)}/principals`);

export const assignGlobalRole = (principalId, role) =>
  postJSON(
    `/admin/platform/principals/${encodeURIComponent(principalId)}/roles`,
    { role },
  );
export const removeGlobalRole = (principalId, role) =>
  deleteJSON(
    `/admin/platform/principals/${encodeURIComponent(principalId)}`
      + `/roles/${encodeURIComponent(role)}`,
  );
export const assignCatalogRole = (principalId, catalogId, role) =>
  postJSON(
    `/admin/catalogs/${encodeURIComponent(catalogId)}`
      + `/principals/${encodeURIComponent(principalId)}/roles`,
    { role },
  );
export const removeCatalogRole = (principalId, catalogId, role) =>
  deleteJSON(
    `/admin/catalogs/${encodeURIComponent(catalogId)}`
      + `/principals/${encodeURIComponent(principalId)}`
      + `/roles/${encodeURIComponent(role)}`,
  );

// ----- Generic IAM bindings (#1346): platform & catalog scope -----
//
// These are the write path that lets operators author bindings the legacy
// /admin/.../roles endpoints can't express: effect=deny, valid_from /
// valid_until time windows, direct object_kind=policy bindings, and
// per-binding quota. The legacy /roles endpoints stay as backcompat for
// allow-only role grants. See packages/extensions/admin/.../admin_service.py.

// scope: {kind: "platform"} | {kind: "catalog", catalogId}
function grantsBasePath(scope) {
  if (!scope || scope.kind === "platform") return "/admin/platform/grants";
  if (scope.kind === "catalog") {
    return `/admin/catalogs/${encodeURIComponent(scope.catalogId)}/grants`;
  }
  throw new Error(`unsupported grant scope kind: ${scope.kind}`);
}

// List the bindings a principal has at this scope.
export const listGrants = (scope, principalId) =>
  getJSON(`${grantsBasePath(scope)}${qs({ principal_id: principalId })}`);

// Create a binding. `body` is a CreateBindingRequest payload — principal_id
// (internal UUID, NOT the OIDC subject_id), object_kind ("role"|"policy"),
// object_ref, effect ("allow"|"deny"), optional valid_from / valid_until
// (ISO-8601), optional quota (JSON object).
export const createGrant = (scope, body) =>
  postJSON(grantsBasePath(scope), body);

// Revoke a binding by match — see admin_service.py revoke_*_binding query
// params: principal_id, object_kind, object_ref, effect.
export const deleteGrant = (scope, { principalId, objectKind, objectRef, effect = "allow" }) =>
  deleteJSON(
    `${grantsBasePath(scope)}${qs({
      principal_id: principalId,
      object_kind: objectKind,
      object_ref: objectRef,
      effect,
    })}`,
  );

// ----- IAM effective-permissions explainer (#1390 frontend half) -----
//
// Diagnostic: "why can / can't principal P perform action A on resource R?".
// Mirrors the backend GET /admin/iam/effective contract (#1389). Every
// query value is run through encodeURIComponent explicitly so principal
// ids, catalog ids and resource refs containing unusual characters
// (spaces, ampersands, slashes) can't smuggle extra query keys or path
// fragments past the server's typed query params. Sysadmin-only on the
// server; we let the 403 surface to the page rather than gating here.
export const fetchEffectivePermissions = ({
  principalId,
  catalogId,
  collectionId,
  action,
  resourceKind,
  resourceRef,
} = {}) => {
  const parts = [];
  const push = (key, value) => {
    if (value == null || value === "") return;
    parts.push(`${encodeURIComponent(key)}=${encodeURIComponent(value)}`);
  };
  push("principal_id", principalId);
  push("action", action);
  push("catalog_id", catalogId);
  push("collection_id", collectionId);
  push("resource_kind", resourceKind);
  push("resource_ref", resourceRef);
  const query = parts.length ? `?${parts.join("&")}` : "";
  return getJSON(`/admin/iam/effective${query}`);
};

// ----- IAM per-binding live counter view (#1342 / #1346) -----
//
// Diagnostic: live rate-limit / lifetime-quota counter state per binding
// for a given principal. Mirrors the backend GET /admin/iam/usage/grants
// contract. Every query value runs through encodeURIComponent so principal
// ids and catalog ids carrying unusual characters (spaces, ampersands,
// slashes) can't smuggle extra query keys past the server's typed query
// params. Sysadmin-only on the server; the 403 surfaces to the page.
export const fetchGrantUsage = ({
  principalId,
  catalogId,
} = {}) => {
  const parts = [];
  const push = (key, value) => {
    if (value == null || value === "") return;
    parts.push(`${encodeURIComponent(key)}=${encodeURIComponent(value)}`);
  };
  push("principal_id", principalId);
  push("catalog_id", catalogId);
  const query = parts.length ? `?${parts.join("&")}` : "";
  return getJSON(`/admin/iam/usage/grants${query}`);
};

// ----- STAC write endpoints (create catalog / collection / features) -----

export const createStacCatalog = (definition) => postJSON(`/catalogs`, definition);
export const createStacCollection = (catalogId, definition) =>
  postJSON(`/catalogs/${encodeURIComponent(catalogId)}/collections`, definition);

// POST features to a collection. Returns the 201 BulkCreationResponse on
// full success, or a 207 IngestionReport on partial failure (thrown as error
// with .body payload — caller can inspect). Always returns whatever the
// server sent as a parsed JSON object for a 2xx.
export async function postFeatures(catalogId, collectionId, payload) {
  const url = `/catalogs/${encodeURIComponent(catalogId)}`
    + `/collections/${encodeURIComponent(collectionId)}/items`;
  const target = apiUrl(url);
  const r = await fetch(target, {
    method: "POST",
    credentials: "same-origin",
    headers: { "Content-Type": "application/json", ...authHeader() },
    body: JSON.stringify(payload),
  });
  const text = await r.text().catch(() => "");
  let data = null;
  try { data = text ? JSON.parse(text) : null; } catch (_) { /* ignore */ }
  if (!r.ok) {
    const err = new Error(`${r.status} ${target}: ${text.slice(0, 400)}`);
    err.status = r.status;
    err.body = data ?? text;
    throw err;
  }
  return { status: r.status, body: data };
}

// /iam/me/catalogs: returns list of {catalog_id, roles, ...} for the
// signed-in principal. Used to discover "catalog admin" scope.
export const fetchMyCatalogs = () => getJSON("/iam/me/catalogs");

export const getCatalogProvisioning = (catalogId) =>
  getJSON(`/admin/catalogs/${encodeURIComponent(catalogId)}`);
