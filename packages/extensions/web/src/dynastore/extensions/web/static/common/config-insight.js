// Configuration Hub — teaching-grade insight layer (#948).
//
// Pure, DOM-free helpers that turn the composed-config API into the data a
// teaching UI needs: per-tier deltas, per-field provenance, a layered tier
// stack, an RFC-7396 PATCH preview, and a copy-paste `curl`.
//
// All knowledge of the API lives in common/api.js — this module only shapes
// already-fetched payloads. No plugin-specific code; everything is derived
// from the registry (class_key keys) + the composed tree.

import { fetchConfigSet, scopeBasePath } from "./api.js";
import { apiUrl } from "./url.js";

export const TIERS = ["default", "platform", "catalog", "collection"];

const TIER_RANK = { default: 0, platform: 1, catalog: 2, collection: 3 };

// Which tiers exist for a given scope. A collection scope stacks all four;
// platform scope only default → platform.
export function tiersForScope(scope) {
  const kind = scope?.kind || "platform";
  if (kind === "collection") return ["default", "platform", "catalog", "collection"];
  if (kind === "catalog") return ["default", "platform", "catalog"];
  return ["default", "platform"];
}

// The set of registry class_keys, so we can recognise composed-tree leaves
// regardless of nesting depth. The composed tree keys leaves by class_key
// (snake_case) — NOT PascalCase — so matching on the registry set is exact.
function classKeySet(schemas) {
  return new Set(Object.keys(schemas || {}));
}

// Flatten the composed scope→topic→…→class_key→payload tree into a plain
// { class_key -> payload } map. A leaf is any object whose KEY is a
// registered class_key; everything above it is a topic container. This is
// the corrected flatten — the previous PascalCase heuristic never matched
// the snake_case leaves the server actually emits, so provenance was lost.
export function flattenComposed(payload, schemas) {
  if (!payload) return {};
  const known = classKeySet(schemas);
  const tree = payload.configs && typeof payload.configs === "object"
    ? payload.configs
    : payload;
  const out = {};
  function walk(node) {
    if (!node || typeof node !== "object" || Array.isArray(node)) return;
    for (const [k, v] of Object.entries(node)) {
      if (known.has(k) && v && typeof v === "object" && !Array.isArray(v)) {
        out[k] = v;
      } else {
        walk(v);
      }
    }
  }
  walk(tree);
  return out;
}

// Strip the response-only envelopes the API injects under meta/links modes
// so a payload is a clean delta safe to compare or PATCH.
export function stripEnvelopes(obj) {
  if (!obj || typeof obj !== "object" || Array.isArray(obj)) return obj;
  const out = {};
  for (const [k, v] of Object.entries(obj)) {
    if (k === "_meta" || k === "_links") continue;
    out[k] = v;
  }
  return out;
}

// Code-default value set for a class, derived from its JSON Schema's
// per-field `default`. No network — the registry schema is already loaded.
export function defaultsFromSchema(jsonSchema) {
  const out = {};
  const props = jsonSchema?.properties || {};
  for (const [field, spec] of Object.entries(props)) {
    if (spec && Object.prototype.hasOwnProperty.call(spec, "default")) {
      out[field] = spec.default;
    }
  }
  return out;
}

// Fetch the explicit (delta-only) config set at each tier in scope, in
// parallel. Returns { default:{}, platform:{}, catalog:{}, collection:{} }
// of flattened class_key → delta maps, and the GET path used per tier so the
// Wire tab can echo it. `view=delta` + `resolved=false` gives exactly the
// rows stored AT that tier (no inherited bleed-through).
export async function fetchTierStack(scope, schemas) {
  const tiers = tiersForScope(scope);
  const jobs = [];
  if (tiers.includes("platform")) {
    jobs.push(["platform", { kind: "platform" }]);
  }
  if (tiers.includes("catalog")) {
    jobs.push(["catalog", { kind: "catalog", catalogId: scope.catalogId }]);
  }
  if (tiers.includes("collection")) {
    jobs.push(["collection", {
      kind: "collection",
      catalogId: scope.catalogId,
      collectionId: scope.collectionId,
    }]);
  }
  const results = await Promise.all(
    jobs.map(async ([tier, tierScope]) => {
      const body = await fetchConfigSet(tierScope, {
        resolved: false, meta: "field", view: "delta",
      });
      return [tier, flattenComposed(body, schemas), body.__path || ""];
    }),
  );
  const stack = { default: {}, platform: {}, catalog: {}, collection: {} };
  const paths = {};
  for (const [tier, flat, path] of results) {
    stack[tier] = flat;
    paths[tier] = path;
  }
  return { stack, paths, tiers };
}

// For one class, build a per-field lineage: which tiers set the field and
// what the effective (right-most-wins) source is. Returns:
//   { fields: { name: { value, source, layers: {tier: value} } },
//     fieldOrder: [...], classSource: "default|platform|catalog|collection" }
// `defaults` is the code-default value map; `stack` is fetchTierStack().stack;
// `tiers` is the subset in scope.
export function lineageForClass(classKey, defaults, stack, tiers) {
  const fields = {};
  const order = [];
  const note = (name) => {
    if (!(name in fields)) { fields[name] = { layers: {} }; order.push(name); }
    return fields[name];
  };

  for (const [name, val] of Object.entries(defaults || {})) {
    note(name).layers.default = val;
  }
  for (const tier of tiers) {
    if (tier === "default") continue;
    const delta = stripEnvelopes((stack[tier] || {})[classKey] || {});
    for (const [name, val] of Object.entries(delta)) {
      note(name).layers[tier] = val;
    }
  }

  let classSource = "default";
  for (const name of order) {
    const f = fields[name];
    let src = "default";
    let value = f.layers.default;
    for (const tier of tiers) {
      if (tier === "default") continue;
      if (Object.prototype.hasOwnProperty.call(f.layers, tier)) {
        src = tier;
        value = f.layers[tier];
      }
    }
    f.source = src;
    f.value = value;
    if (TIER_RANK[src] > TIER_RANK[classSource]) classSource = src;
  }
  return { fields, fieldOrder: order, classSource };
}

// Compute the before/after of one Save:
//   override: explicit delta at the active scope before vs after PATCH
//   resolved: effective value before vs after (override merged onto inherited)
// `patch` is the RFC-7396 body (field → value, or null to inherit).
// `explicitBefore` is the active-scope delta; `inherited` is the merged
// upstream value (everything below the active scope).
export function diffPatch({ patch, explicitBefore, inherited }) {
  const before = stripEnvelopes(explicitBefore || {});
  const after = { ...before };
  for (const [k, v] of Object.entries(patch || {})) {
    if (v === null) delete after[k];
    else after[k] = v;
  }
  const baseResolved = { ...stripEnvelopes(inherited || {}), ...before };
  const afterResolved = { ...stripEnvelopes(inherited || {}), ...after };
  const changed = [];
  const allKeys = new Set([
    ...Object.keys(baseResolved), ...Object.keys(afterResolved),
  ]);
  for (const k of allKeys) {
    const a = baseResolved[k];
    const b = afterResolved[k];
    if (JSON.stringify(a) !== JSON.stringify(b)) {
      changed.push({ field: k, before: a, after: b });
    }
  }
  return {
    overrideBefore: before,
    overrideAfter: after,
    resolvedBefore: baseResolved,
    resolvedAfter: afterResolved,
    changed,
  };
}

// Build the exact `curl` for the PATCH Save will send. `scope` selects the
// path; `body` is the { class_key: patch } map. Absolute URL via apiUrl so
// the proxy prefix is preserved — implementers can copy-paste verbatim.
export function curlForPatch(scope, body) {
  const url = apiUrl(scopeBasePath(scope));
  const json = JSON.stringify(body, null, 2);
  return [
    `curl -X PATCH '${url}' \\`,
    `  -H 'Authorization: Bearer $DS_TOKEN' \\`,
    `  -H 'Content-Type: application/json' \\`,
    `  -d '${json}'`,
  ].join("\n");
}

// Build a GET curl for the composed read at the active scope, reflecting
// the display knobs. Teaches the read API surface (meta/view/resolved).
export function curlForGet(scope, knobs) {
  const base = apiUrl(scopeBasePath(scope));
  const qs = new URLSearchParams();
  if (knobs?.resolved != null) qs.set("resolved", knobs.resolved ? "true" : "false");
  if (knobs?.meta) qs.set("meta", knobs.meta);
  if (knobs?.view) qs.set("view", knobs.view);
  const q = qs.toString();
  return [
    `curl '${base}${q ? "?" + q : ""}' \\`,
    `  -H 'Authorization: Bearer $DS_TOKEN'`,
  ].join("\n");
}
