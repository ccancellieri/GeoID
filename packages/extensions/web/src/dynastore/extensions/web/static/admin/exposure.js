import { apiUrl } from "../common/url.js";

(async () => {
  const $ = (sel) => document.querySelector(sel);
  const state = {
    scope: "platform",
    catalog: null,
    resolved: {},
    explicit: {},
    dirty: {}
  };

  // Utility: fetch JSON with error handling. Routes absolute paths through
  // apiUrl() so the proxy prefix (e.g. /geospatial/v2/api/catalog) is applied.
  async function getJSON(url) {
    const target = apiUrl(url);
    const r = await fetch(target);
    if (!r.ok) {
      const text = await r.text();
      throw new Error(`${target} -> ${r.status}: ${text}`);
    }
    return r.json();
  }

  async function patchJSON(url, body) {
    const target = apiUrl(url);
    const r = await fetch(target, {
      method: "PATCH",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body)
    });
    if (!r.ok) {
      const text = await r.text();
      throw new Error(`${target} -> ${r.status}: ${text}`);
    }
    return r.json();
  }

  // Load list of catalogs
  async function loadCatalogs() {
    try {
      const res = await getJSON("/stac/catalogs");
      const sel = $("#catalog-select");
      while (sel.firstChild) sel.removeChild(sel.firstChild);

      const opt0 = document.createElement("option");
      opt0.value = "";
      opt0.textContent = "-- select catalog --";
      sel.appendChild(opt0);

      const items = res.items || res || [];
      if (!Array.isArray(items)) return;

      for (const c of items) {
        const opt = document.createElement("option");
        const id = c.id || c.catalog_id || (typeof c === "string" ? c : null);
        if (!id) continue;
        opt.value = id;
        opt.textContent = id;
        sel.appendChild(opt);
      }
      if (items.length > 0) {
        const firstId = items[0].id || items[0].catalog_id || items[0];
        if (firstId) state.catalog = firstId;
      }
    } catch (e) {
      // Catalogs may not exist; safe to ignore
      console.debug("loadCatalogs:", e.message);
    }
  }

  // Load configuration matrix (resolved and explicit)
  async function loadMatrix() {
    const base = state.scope === "platform"
      ? "/configs"
      : `/configs/catalogs/${encodeURIComponent(state.catalog)}`;

    try {
      state.resolved = await loadIndex(`${base}?resolved=true`);
      state.explicit = await loadIndex(`${base}?resolved=false`);
    } catch (e) {
      setStatus(`Load error: ${e.message}`, "err");
      return;
    }
    state.dirty = {};
    render();
  }

  // Flatten the composed response's nested scope→topic→[sub→]ClassName→payload
  // tree into a plain { ClassName -> payload } map.  Class names are globally
  // unique so the flatten is lossless.
  async function loadIndex(url) {
    const page = await getJSON(url);
    const tree = page && page.configs && typeof page.configs === "object"
      ? page.configs
      : page;
    const out = {};
    function isClassNode(name, node) {
      return typeof name === "string" && /^[A-Z]/.test(name)
        && node && typeof node === "object" && !Array.isArray(node);
    }
    function walk(node) {
      if (!node || typeof node !== "object" || Array.isArray(node)) return;
      for (const [k, v] of Object.entries(node)) {
        if (isClassNode(k, v)) out[k] = v;
        else walk(v);
      }
    }
    walk(tree);
    return out;
  }

  // Create a table cell with text content (safe-DOM)
  function td(text) {
    const c = document.createElement("td");
    c.textContent = text;
    return c;
  }

  // Create a table cell with a DOM node (safe-DOM)
  function tdNode(node) {
    const c = document.createElement("td");
    c.appendChild(node);
    return c;
  }

  // Create a checkbox input (safe-DOM)
  function cbox(key, checked) {
    const i = document.createElement("input");
    i.type = "checkbox";
    i.dataset.key = key;
    i.checked = !!checked;
    return i;
  }

  // Create a three-way select (inherit/ON/OFF) (safe-DOM)
  function selectThreeWay(key, current) {
    const s = document.createElement("select");
    s.dataset.key = key;
    s.className = "threeway-select";

    const opts = [
      ["inherit", "inherit"],
      ["true", "ON"],
      ["false", "OFF"]
    ];

    for (const [val, label] of opts) {
      const o = document.createElement("option");
      o.value = val;
      o.textContent = label;

      if ((current === null && val === "inherit") ||
          (current === true && val === "true") ||
          (current === false && val === "false")) {
        o.selected = true;
      }

      s.appendChild(o);
    }

    return s;
  }

  // Render the grid
  function render() {
    const body = $("#exposure-grid tbody");
    while (body.firstChild) body.removeChild(body.firstChild);

    const meta = $("#exposure-meta");
    if (meta) {
      meta.textContent = state.scope === "platform"
        ? "PLATFORM"
        : `CATALOG · ${state.catalog || "—"}`;
    }

    const rows = Object.entries(state.resolved);
    if (!rows.length) {
      const tr = document.createElement("tr");
      tr.className = "empty-row";
      const td = document.createElement("td");
      td.colSpan = 4;
      td.textContent = state.scope === "catalog" && !state.catalog
        ? "Select a catalog to view extension exposure."
        : "No extensions registered at this scope.";
      tr.appendChild(td);
      body.appendChild(tr);
      return;
    }

    for (const [key, val] of rows) {
      if (!val || typeof val.enabled !== "boolean") continue;

      const platformEnabled = val.enabled;
      const explicit = state.explicit[key];
      const explicitEnabled = (explicit && typeof explicit.enabled === "boolean")
        ? explicit.enabled
        : null;

      const dirtyVal = state.dirty[key];
      const effective = (dirtyVal === undefined)
        ? (explicitEnabled !== null ? explicitEnabled : platformEnabled)
        : (dirtyVal === null ? platformEnabled : !!dirtyVal.enabled);

      const row = document.createElement("tr");

      // Extension name
      row.appendChild(td(key));

      // Platform column (shows current platform value, or edit if in platform scope)
      if (state.scope === "platform") {
        row.appendChild(tdNode(cbox(key, effective)));
      } else {
        row.appendChild(td(platformEnabled ? "ON" : "(platform off)"));
      }

      // Catalog override column (only in catalog scope)
      if (state.scope === "catalog") {
        row.appendChild(tdNode(selectThreeWay(key, explicitEnabled)));
      } else {
        row.appendChild(td("-"));
      }

      // Effective column — chip-styled to match the rest of the admin UI.
      const eff = document.createElement("td");
      const chip = document.createElement("span");
      chip.className = effective ? "chip effect-ALLOW" : "chip effect-DENY";
      chip.textContent = effective ? "on" : "off";
      eff.appendChild(chip);
      row.appendChild(eff);

      body.appendChild(row);
    }
  }

  // Handle input changes
  function onChange(e) {
    const key = e.target.dataset.key;
    if (!key) return;

    if (state.scope === "platform") {
      state.dirty[key] = { enabled: !!e.target.checked };
    } else {
      const v = e.target.value;
      state.dirty[key] = (v === "inherit") ? null : { enabled: v === "true" };
    }

    render();
  }

  // Set status message — uses the shared .status / .status.ok / .status.err
  // / .status.warn classes from admin.css.
  function setStatus(msg, cls = "") {
    const s = $("#status");
    s.textContent = msg || "";
    s.className = "status" + (cls ? ` ${cls}` : "");
  }

  // Apply changes
  async function apply() {
    if (!Object.keys(state.dirty).length) {
      setStatus("No changes.", "");
      return;
    }

    const url = state.scope === "platform"
      ? "/configs"
      : `/configs/catalogs/${encodeURIComponent(state.catalog)}`;

    setStatus("Applying…", "");
    try {
      await patchJSON(url, state.dirty);
      setStatus("Applied.", "ok");
      await loadMatrix();
    } catch (err) {
      setStatus(`Error: ${err.message}`, "err");
    }
  }

  // Revert changes
  function revert() {
    state.dirty = {};
    render();
  }

  // Event listeners
  document.querySelectorAll('input[name="scope"]').forEach(r => {
    r.addEventListener("change", e => {
      state.scope = e.target.value;
      $("#catalog-select").disabled = state.scope !== "catalog";
      if (state.scope === "catalog") {
        const sel = $("#catalog-select");
        if (sel.value) state.catalog = sel.value;
      }
      loadMatrix();
    });
  });

  $("#catalog-select").addEventListener("change", e => {
    state.catalog = e.target.value;
    if (state.catalog) loadMatrix();
  });

  $("#exposure-grid").addEventListener("change", onChange);
  $("#apply").addEventListener("click", apply);
  $("#revert").addEventListener("click", revert);

  // Initialize
  await loadCatalogs();
  await loadMatrix();
})();
