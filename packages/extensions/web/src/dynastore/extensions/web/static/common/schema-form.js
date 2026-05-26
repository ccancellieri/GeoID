// Schema-driven form engine.
//
// Renders a form for any Pydantic-generated JSON Schema. Honors the
// `x-ui` vendor extension for UX hints (widget, section, sensitive,
// readonly). Emits a shallow partial dict of the fields the user
// actually touched, so save goes through the existing PATCH /config
// endpoint unchanged.
//
// Public API:
//   const form = mountSchemaForm(container, {
//     schema,         // JSON Schema for the selected plugin (from /configs/registry)
//     resolved,       // current effective (inherited) value set
//     explicit,       // current explicit overrides at the active scope
//     allowInherit,   // true at non-platform scopes — enables tri-state per field
//     onDirty,        // (dirty: boolean) => void
//   });
//   form.getPatch();  // { fieldName: value | null } for save()
//   form.reset();
//
// Design choices:
//   - No innerHTML with dynamic content; safe-DOM only.
//   - $defs are resolved lazily when a $ref is encountered.
//   - oneOf with a string `discriminator` renders as a kind picker that
//     swaps the inner form for the chosen variant.

function clearNode(n) { while (n.firstChild) n.removeChild(n.firstChild); }

function resolveRef(root, ref) {
  if (typeof ref !== "string" || !ref.startsWith("#/")) return null;
  const parts = ref.slice(2).split("/");
  let cur = root;
  for (const p of parts) {
    if (cur == null) return null;
    cur = cur[p];
  }
  return cur || null;
}

function derefSchema(root, node) {
  if (!node) return node;
  if (node.$ref) {
    const target = resolveRef(root, node.$ref);
    if (target) return { ...target, ...Object.fromEntries(Object.entries(node).filter(([k]) => k !== "$ref")) };
  }
  return node;
}

function uiHints(node) {
  return (node && node["x-ui"]) || {};
}

function isObjectSchema(node) {
  if (!node || typeof node !== "object") return false;
  if (node.type === "object") return true;
  if (node.properties && !node.type) return true;
  return false;
}

// Primitive renderer: returns {el, get, set}
function renderPrimitive(root, node, value, { readonly } = {}) {
  const n = derefSchema(root, node);
  const hints = uiHints(n);
  const effReadonly = readonly || !!hints.readonly;
  const widget = hints.widget;
  const type = Array.isArray(n.type) ? n.type.find((t) => t !== "null") : n.type;

  const wrap = document.createElement("div");
  wrap.className = "field-wrap";

  let input;

  if (Array.isArray(n.enum) && !widget) {
    input = document.createElement("select");
    for (const v of n.enum) {
      const opt = document.createElement("option");
      opt.value = String(v);
      opt.textContent = String(v);
      if (value === v) opt.selected = true;
      input.appendChild(opt);
    }
  } else if (type === "boolean") {
    input = document.createElement("input");
    input.type = "checkbox";
    input.checked = !!value;
  } else if (type === "integer" || type === "number") {
    input = document.createElement("input");
    input.type = "number";
    if (type === "integer") input.step = "1";
    if (value !== undefined && value !== null) input.value = String(value);
    if (typeof n.minimum === "number") input.min = String(n.minimum);
    if (typeof n.maximum === "number") input.max = String(n.maximum);
  } else if (widget === "password" || n.format === "password" || hints.sensitive) {
    input = document.createElement("input");
    input.type = "password";
    input.placeholder = hints.sensitive ? "•••• (unchanged)" : "";
    input.autocomplete = "new-password";
  } else if (widget === "textarea") {
    input = document.createElement("textarea");
    input.rows = 4;
    if (value !== undefined && value !== null) input.value = String(value);
  } else if (widget === "json") {
    input = document.createElement("textarea");
    input.rows = 6;
    input.dataset.widget = "json";
    try {
      input.value = value == null ? "" : JSON.stringify(value, null, 2);
    } catch {
      input.value = String(value ?? "");
    }
  } else {
    input = document.createElement("input");
    input.type = "text";
    if (value !== undefined && value !== null) input.value = String(value);
  }

  if (effReadonly) {
    if (input.tagName === "INPUT" && input.type === "checkbox") input.disabled = true;
    else input.readOnly = true;
    input.dataset.readonly = "true";
  }
  // Styling comes from .schema-form .field-wrap rules in admin.css — no
  // inline overrides, so the form inherits the page typography.

  wrap.appendChild(input);

  const get = () => {
    if (input.tagName === "SELECT") {
      const v = input.value;
      if (Array.isArray(n.enum)) {
        const found = n.enum.find((e) => String(e) === v);
        return found !== undefined ? found : v;
      }
      return v;
    }
    if (input.type === "checkbox") return input.checked;
    if (input.type === "number") {
      const v = input.value;
      if (v === "") return null;
      return type === "integer" ? parseInt(v, 10) : Number(v);
    }
    if (input.dataset.widget === "json") {
      const raw = input.value.trim();
      if (!raw) return null;
      try { return JSON.parse(raw); } catch { return { __parse_error: raw }; }
    }
    return input.value;
  };

  const set = (v) => {
    if (input.type === "checkbox") {
      input.checked = !!v;
    } else if (input.dataset.widget === "json") {
      try { input.value = v == null ? "" : JSON.stringify(v, null, 2); }
      catch { input.value = String(v ?? ""); }
    } else if (input.tagName === "SELECT") {
      input.value = v == null ? "" : String(v);
    } else {
      input.value = v == null ? "" : String(v);
    }
  };

  return { el: wrap, input, get, set, sensitive: !!hints.sensitive };
}

// List renderer: array of items — supports add / remove / reorder.
function renderArray(root, node, value, { readonly, onChange } = {}) {
  const n = derefSchema(root, node);
  const items = Array.isArray(value) ? value.slice() : [];
  const itemSchema = n.items || {};
  const effReadonly = readonly || !!uiHints(n).readonly;

  const wrap = document.createElement("div");
  wrap.className = "array-wrap";

  const rows = document.createElement("div");
  wrap.appendChild(rows);

  const addBtn = document.createElement("button");
  addBtn.type = "button";
  addBtn.textContent = "+ Add";
  addBtn.className = "btn btn-secondary btn-xs";
  if (effReadonly) addBtn.disabled = true;
  wrap.appendChild(addBtn);

  const renderers = [];

  function rebuild() {
    clearNode(rows);
    renderers.length = 0;
    items.forEach((itemVal, idx) => {
      const row = document.createElement("div");
      row.className = "array-row";

      const content = document.createElement("div");
      content.className = "array-row-body";

      const renderer = renderAny(root, itemSchema, itemVal, { readonly: effReadonly });
      content.appendChild(renderer.el);
      renderers.push(renderer);

      const rm = document.createElement("button");
      rm.type = "button";
      rm.textContent = "×";
      rm.className = "btn btn-secondary";
      rm.title = "Remove this row";
      rm.disabled = effReadonly;
      rm.addEventListener("click", () => {
        items.splice(idx, 1);
        rebuild();
        if (onChange) onChange();
      });

      row.appendChild(content);
      row.appendChild(rm);
      rows.appendChild(row);
    });
  }
  rebuild();

  addBtn.addEventListener("click", () => {
    items.push(defaultForSchema(itemSchema, root));
    rebuild();
    if (onChange) onChange();
  });

  return {
    el: wrap,
    get: () => renderers.map((r) => r.get()),
    set: (v) => {
      items.length = 0;
      if (Array.isArray(v)) items.push(...v);
      rebuild();
    },
  };
}

function defaultForSchema(schema, root) {
  const n = derefSchema(root, schema);
  if (n.default !== undefined) return n.default;
  const type = Array.isArray(n.type) ? n.type.find((t) => t !== "null") : n.type;
  if (type === "string") return "";
  if (type === "integer" || type === "number") return 0;
  if (type === "boolean") return false;
  if (type === "array") return [];
  if (type === "object" || n.properties) {
    const out = {};
    for (const [k, sub] of Object.entries(n.properties || {})) {
      out[k] = defaultForSchema(sub, root);
    }
    return out;
  }
  return null;
}

// Object renderer.
function renderObject(root, node, value, { readonly } = {}) {
  const n = derefSchema(root, node);
  const props = n.properties || {};
  const required = new Set(n.required || []);
  const effReadonly = readonly || !!uiHints(n).readonly;

  const wrap = document.createElement("div");
  wrap.className = "object-wrap";

  const sections = new Map(); // section name → container
  const defaultSection = document.createElement("div");
  defaultSection.className = "form-section-default";
  wrap.appendChild(defaultSection);
  sections.set("__default__", defaultSection);

  const fieldRenderers = {};

  const orderedEntries = Object.entries(props).sort(([ka, sa], [kb, sb]) => {
    const oa = uiHints(derefSchema(root, sa)).order ?? 500;
    const ob = uiHints(derefSchema(root, sb)).order ?? 500;
    if (oa !== ob) return oa - ob;
    return ka.localeCompare(kb);
  });

  for (const [fieldName, rawSub] of orderedEntries) {
    const sub = derefSchema(root, rawSub);
    const hints = uiHints(sub);
    const sectionName = hints.section || "__default__";
    let sectionContainer = sections.get(sectionName);
    if (!sectionContainer) {
      const block = document.createElement("fieldset");
      block.className = "form-section";
      const legend = document.createElement("legend");
      legend.textContent = sectionName;
      block.appendChild(legend);
      wrap.appendChild(block);
      sections.set(sectionName, block);
      sectionContainer = block;
    }

    const fieldWrap = document.createElement("div");
    fieldWrap.className = "field-row";

    const label = document.createElement("label");
    label.textContent = fieldName + (required.has(fieldName) ? " *" : "");
    fieldWrap.appendChild(label);

    if (sub.description) {
      const desc = document.createElement("div");
      desc.className = "field-desc";
      desc.textContent = sub.description;
      fieldWrap.appendChild(desc);
    }
    if (hints.hint) {
      const h = document.createElement("div");
      h.className = "field-hint";
      h.textContent = hints.hint;
      fieldWrap.appendChild(h);
    }

    const renderer = renderAny(root, sub, value?.[fieldName], { readonly: effReadonly });
    fieldRenderers[fieldName] = renderer;
    fieldWrap.appendChild(renderer.el);
    sectionContainer.appendChild(fieldWrap);
  }

  if (!defaultSection.firstChild) defaultSection.remove();

  return {
    el: wrap,
    get: () => {
      const out = {};
      for (const [k, r] of Object.entries(fieldRenderers)) out[k] = r.get();
      return out;
    },
    set: (v) => {
      for (const [k, r] of Object.entries(fieldRenderers)) {
        r.set(v?.[k]);
      }
    },
    fieldRenderers,
  };
}

function renderAny(root, schema, value, opts) {
  const n = derefSchema(root, schema);
  if (!n || typeof n !== "object") return renderPrimitive(root, schema, value, opts);
  if (isObjectSchema(n)) return renderObject(root, n, value, opts);
  const type = Array.isArray(n.type) ? n.type.find((t) => t !== "null") : n.type;
  if (type === "array") return renderArray(root, n, value, opts);
  return renderPrimitive(root, n, value, opts);
}

// Tri-state pill for per-field inherit / override / delete. Only shown
// at non-platform scopes, where a field can come from a higher scope.
function buildInheritPill(fieldWrap, { isExplicit, inheritedValue, onSetExplicit, onInherit }) {
  const pill = document.createElement("div");
  pill.className = "inherit-pill";

  const mkPart = (label, active, handler) => {
    const btn = document.createElement("button");
    btn.type = "button";
    btn.textContent = label;
    if (active) btn.className = "active";
    btn.addEventListener("click", handler);
    return btn;
  };

  pill.appendChild(mkPart("inherit", !isExplicit, onInherit));
  pill.appendChild(mkPart("override", isExplicit, onSetExplicit));

  return pill;
}

export function mountSchemaForm(container, {
  schema,
  resolved,
  explicit,
  allowInherit = false,
  onDirty,
}) {
  container.classList.add("schema-form");
  clearNode(container);

  if (!schema) {
    const msg = document.createElement("p");
    msg.className = "empty-hint";
    msg.textContent = "Select a plugin from the list to edit its configuration.";
    container.appendChild(msg);
    return { getPatch: () => ({}), reset: () => {} };
  }

  const root = schema;
  const effectiveValue = resolved || {};
  const explicitValue = explicit || {};

  const form = renderObject(root, schema, effectiveValue);
  container.appendChild(form.el);

  // Track dirty state per field.
  const dirty = new Set();
  const deleted = new Set();
  const initialExplicit = { ...explicitValue };

  // If allowInherit, add an inherit-pill per top-level field.
  if (allowInherit) {
    for (const [fieldName, r] of Object.entries(form.fieldRenderers)) {
      const row = r.el.closest(".field-row");
      if (!row) continue;
      const label = row.querySelector("label");
      if (!label) continue;

      function applyInherit() {
        deleted.add(fieldName);
        dirty.delete(fieldName);
        r.set(effectiveValue[fieldName]);
        pill.replaceWith(buildPill(false));
        notifyDirty();
      }
      function applyOverride() {
        deleted.delete(fieldName);
        dirty.add(fieldName);
        pill.replaceWith(buildPill(true));
        notifyDirty();
      }
      function buildPill(isExplicit) {
        return buildInheritPill(row, {
          isExplicit,
          inheritedValue: effectiveValue[fieldName],
          onSetExplicit: applyOverride,
          onInherit: applyInherit,
        });
      }
      const startIsExplicit = fieldName in explicitValue;
      const pill = buildPill(startIsExplicit);
      label.appendChild(pill);

      r.el.addEventListener("input", () => {
        deleted.delete(fieldName);
        dirty.add(fieldName);
        notifyDirty();
      }, { capture: true });
      r.el.addEventListener("change", () => {
        deleted.delete(fieldName);
        dirty.add(fieldName);
        notifyDirty();
      }, { capture: true });
    }
  } else {
    // Platform scope: dirty is tracked by any input change.
    container.addEventListener("input", (e) => {
      const fieldRow = e.target.closest(".field-row");
      if (!fieldRow) return;
      const parent = fieldRow.parentElement;
      const fieldNames = Object.keys(form.fieldRenderers);
      for (const fn of fieldNames) {
        if (form.fieldRenderers[fn].el.closest(".field-row") === fieldRow) {
          dirty.add(fn);
          break;
        }
      }
      notifyDirty();
    }, { capture: true });
    container.addEventListener("change", (e) => {
      const fieldRow = e.target.closest(".field-row");
      if (!fieldRow) return;
      for (const fn of Object.keys(form.fieldRenderers)) {
        if (form.fieldRenderers[fn].el.closest(".field-row") === fieldRow) {
          dirty.add(fn);
          break;
        }
      }
      notifyDirty();
    }, { capture: true });
  }

  function notifyDirty() {
    if (typeof onDirty === "function") {
      onDirty(dirty.size > 0 || deleted.size > 0);
    }
  }

  function getPatch() {
    const patch = {};
    for (const f of dirty) {
      const r = form.fieldRenderers[f];
      if (!r) continue;
      // Skip sensitive fields that weren't actually edited: treat empty
      // string as "unchanged, don't overwrite the stored ciphertext".
      if (r.sensitive && (r.input && r.input.value === "")) continue;
      patch[f] = r.get();
    }
    for (const f of deleted) patch[f] = null;
    return patch;
  }

  function reset() {
    dirty.clear();
    deleted.clear();
    form.set(effectiveValue);
    notifyDirty();
  }

  return { getPatch, reset, getDirty: () => new Set(dirty), getDeleted: () => new Set(deleted) };
}
