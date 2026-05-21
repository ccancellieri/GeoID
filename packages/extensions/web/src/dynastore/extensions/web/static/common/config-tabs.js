// Configuration Hub — right-pane teaching tabs (#948).
//
// Renders the Tier-stack + a tab strip (Effective | Wire | Diff | Docs) into
// a host element. Pure DOM construction (no innerHTML with dynamic content).
// Reuses the shared .tabs / .tab-btn / .tab-panel / .chip styles from
// admin.css; provenance/tier-stack styles are namespaced .ci-*.
//
// Public API:
//   const view = mountConfigTabs(host, { onKnobs });
//   view.update({
//     classKey, lineage, tiers,
//     resolved, explicit, inherited,
//     patch, curlPatch, curlGet, docs, registryUrl, knobs,
//   });

import { TIERS } from "./config-insight.js";

function clear(n) { while (n.firstChild) n.removeChild(n.firstChild); }

const SOURCE_LABEL = {
  default: "default",
  platform: "platform",
  catalog: "catalog",
  collection: "collection",
};

// A provenance badge: small chip coloured by tier-of-origin.
export function provenanceBadge(source) {
  const el = document.createElement("span");
  el.className = `ci-badge ci-from-${source || "default"}`;
  el.textContent = `from: ${SOURCE_LABEL[source] || source || "default"}`;
  el.title = `This value is supplied by the ${source} tier`;
  return el;
}

function jsonBlock(value) {
  const pre = document.createElement("pre");
  pre.className = "preview-json";
  pre.textContent = typeof value === "string"
    ? value
    : JSON.stringify(value ?? {}, null, 2);
  return pre;
}

function mkButton(label, onClick, cls = "btn btn-secondary") {
  const b = document.createElement("button");
  b.type = "button";
  b.className = cls;
  b.textContent = label;
  b.addEventListener("click", onClick);
  return b;
}

// Tier-stack: one row per field, columns = tiers, the cell filled when that
// tier sets the field; the effective source highlighted. Hovering a row
// shows the lineage via title.
function renderTierStack(host, { lineage, tiers }) {
  clear(host);
  if (!lineage || !lineage.fieldOrder.length) {
    const p = document.createElement("div");
    p.className = "empty-hint";
    p.textContent = "Select a plugin to see how its values stack across tiers.";
    host.appendChild(p);
    return;
  }

  const table = document.createElement("table");
  table.className = "ci-stack";

  const thead = document.createElement("thead");
  const htr = document.createElement("tr");
  const hf = document.createElement("th");
  hf.textContent = "field";
  htr.appendChild(hf);
  for (const tier of TIERS) {
    if (!tiers.includes(tier)) continue;
    const th = document.createElement("th");
    th.textContent = tier;
    th.className = `ci-col-${tier}`;
    htr.appendChild(th);
  }
  const hs = document.createElement("th");
  hs.textContent = "from";
  htr.appendChild(hs);
  thead.appendChild(htr);
  table.appendChild(thead);

  const tbody = document.createElement("tbody");
  for (const name of lineage.fieldOrder) {
    const f = lineage.fields[name];
    const tr = document.createElement("tr");
    tr.title = TIERS
      .filter((t) => tiers.includes(t) && (t in f.layers))
      .map((t) => `${t}=${JSON.stringify(f.layers[t])}`)
      .join("  →  ");

    const tdName = document.createElement("td");
    tdName.className = "ci-field-name";
    tdName.textContent = name;
    tr.appendChild(tdName);

    for (const tier of TIERS) {
      if (!tiers.includes(tier)) continue;
      const td = document.createElement("td");
      td.className = "ci-cell";
      if (tier in f.layers) {
        td.classList.add("ci-set");
        if (tier === f.source) td.classList.add("ci-effective");
        const v = f.layers[tier];
        td.textContent = typeof v === "object"
          ? JSON.stringify(v)
          : String(v);
      } else {
        td.textContent = "·";
        td.classList.add("ci-unset");
      }
      tr.appendChild(td);
    }

    const tdFrom = document.createElement("td");
    tdFrom.appendChild(provenanceBadge(f.source));
    tr.appendChild(tdFrom);

    tbody.appendChild(tr);
  }
  table.appendChild(tbody);
  host.appendChild(table);
}

export function mountConfigTabs(host, { onKnobs } = {}) {
  clear(host);

  // --- Tier-stack region (always visible above the tabs) ---
  const stackWrap = document.createElement("div");
  stackWrap.className = "ci-stack-wrap";
  const stackHdr = document.createElement("div");
  stackHdr.className = "ci-subhead";
  stackHdr.textContent = "Tier stack";
  const stackHost = document.createElement("div");
  stackHost.className = "ci-stack-host";
  stackWrap.appendChild(stackHdr);
  stackWrap.appendChild(stackHost);
  host.appendChild(stackWrap);

  // --- Tab strip ---
  const tabNames = ["Effective", "Wire", "Diff", "Docs"];
  const tabs = document.createElement("div");
  tabs.className = "tabs";
  const panels = {};
  const buttons = {};

  const panelHost = document.createElement("div");
  panelHost.className = "ci-panels";

  function activate(name) {
    for (const n of tabNames) {
      buttons[n].classList.toggle("active", n === name);
      panels[n].classList.toggle("active", n === name);
    }
  }

  for (const name of tabNames) {
    const b = document.createElement("button");
    b.type = "button";
    b.className = "tab-btn";
    b.textContent = name;
    b.addEventListener("click", () => activate(name));
    tabs.appendChild(b);
    buttons[name] = b;

    const p = document.createElement("div");
    p.className = "tab-panel";
    panels[name] = p;
    panelHost.appendChild(p);
  }
  host.appendChild(tabs);
  host.appendChild(panelHost);
  activate("Effective");

  // --- Wire-tab display knobs (GET) ---
  const knobState = { resolved: true, meta: "field", view: "effective" };

  function buildKnobBar(curlGetEl, currentScope) {
    const bar = document.createElement("div");
    bar.className = "ci-knobs";
    const mkSel = (label, key, opts) => {
      const wrap = document.createElement("label");
      wrap.className = "ci-knob";
      wrap.textContent = label;
      const sel = document.createElement("select");
      for (const o of opts) {
        const opt = document.createElement("option");
        opt.value = String(o);
        opt.textContent = String(o);
        if (String(knobState[key]) === String(o)) opt.selected = true;
        sel.appendChild(opt);
      }
      sel.addEventListener("change", () => {
        knobState[key] = key === "resolved"
          ? sel.value === "true"
          : sel.value;
        if (typeof onKnobs === "function") onKnobs({ ...knobState });
      });
      wrap.appendChild(sel);
      return wrap;
    };
    bar.appendChild(mkSel("resolved", "resolved", [true, false]));
    bar.appendChild(mkSel("meta", "meta", ["none", "field", "schema"]));
    bar.appendChild(mkSel("view", "view", ["effective", "delta", "inherited"]));
    return bar;
  }

  function update(data) {
    const {
      classKey, lineage, tiers,
      resolved, explicit, inherited,
      patch, diff, curlPatch, curlGet, docs, registryUrl,
    } = data || {};

    renderTierStack(stackHost, { lineage: lineage || null, tiers: tiers || [] });

    // Effective panel — the resolved (waterfall) JSON, kept from the old
    // single-blob preview so nothing regresses.
    const eff = panels.Effective;
    clear(eff);
    if (!classKey) {
      const h = document.createElement("div");
      h.className = "empty-hint";
      h.textContent = "Select a plugin to inspect its effective configuration.";
      eff.appendChild(h);
    } else {
      const cap = document.createElement("div");
      cap.className = "ci-cap";
      cap.textContent = "Waterfall-resolved value (default → platform → catalog → collection).";
      eff.appendChild(cap);
      eff.appendChild(jsonBlock(resolved || {}));
    }

    // Wire panel — knob bar + GET curl + PATCH curl.
    const wire = panels.Wire;
    clear(wire);
    const wireCap = document.createElement("div");
    wireCap.className = "ci-cap";
    wireCap.textContent =
      "Learn the HTTP surface. Toggle the read knobs to see how the composed GET changes; the PATCH is exactly what Save will send.";
    wire.appendChild(wireCap);

    const getHdr = document.createElement("div");
    getHdr.className = "ci-subhead";
    getHdr.textContent = "GET (read)";
    wire.appendChild(getHdr);
    wire.appendChild(buildKnobBar(null));
    const getBlock = jsonBlock(curlGet || "");
    wire.appendChild(getBlock);
    wire.appendChild(mkButton("Copy GET", () => copy(curlGet || "")));

    const patchHdr = document.createElement("div");
    patchHdr.className = "ci-subhead";
    patchHdr.textContent = "PATCH (the Save body)";
    wire.appendChild(patchHdr);
    const hasPatch = patch && Object.keys(patch).length;
    if (!hasPatch) {
      const note = document.createElement("div");
      note.className = "ci-cap";
      note.textContent = "Edit a field (or toggle inherit/override) to populate the PATCH body.";
      wire.appendChild(note);
    }
    wire.appendChild(jsonBlock(curlPatch || ""));
    wire.appendChild(mkButton("Copy PATCH", () => copy(curlPatch || "")));

    // Diff panel — override + resolved before/after.
    const diffPanel = panels.Diff;
    clear(diffPanel);
    if (!diff || !diff.changed || !diff.changed.length) {
      const h = document.createElement("div");
      h.className = "empty-hint";
      h.textContent = "No pending changes. Edit a field to preview the before/after.";
      diffPanel.appendChild(h);
    } else {
      const summary = document.createElement("ul");
      summary.className = "ci-diff-list";
      for (const c of diff.changed) {
        const li = document.createElement("li");
        const k = document.createElement("strong");
        k.textContent = c.field;
        li.appendChild(k);
        li.appendChild(document.createTextNode(
          `: ${JSON.stringify(c.before)} → ${JSON.stringify(c.after)}`,
        ));
        summary.appendChild(li);
      }
      diffPanel.appendChild(summary);

      const grid = document.createElement("div");
      grid.className = "ci-diff-grid";
      const col = (title, body) => {
        const c = document.createElement("div");
        const h = document.createElement("div");
        h.className = "ci-subhead";
        h.textContent = title;
        c.appendChild(h);
        c.appendChild(jsonBlock(body));
        return c;
      };
      grid.appendChild(col("Override — before", diff.overrideBefore));
      grid.appendChild(col("Override — after", diff.overrideAfter));
      grid.appendChild(col("Resolved — before", diff.resolvedBefore));
      grid.appendChild(col("Resolved — after", diff.resolvedAfter));
      diffPanel.appendChild(grid);
    }

    // Docs panel — field descriptions + registry link.
    const docsPanel = panels.Docs;
    clear(docsPanel);
    if (!classKey) {
      const h = document.createElement("div");
      h.className = "empty-hint";
      h.textContent = "Select a plugin to read its field documentation.";
      docsPanel.appendChild(h);
    } else {
      const entries = Object.entries(docs || {});
      if (!entries.length) {
        const h = document.createElement("div");
        h.className = "ci-cap";
        h.textContent = "No field-level documentation declared for this plugin.";
        docsPanel.appendChild(h);
      } else {
        const dl = document.createElement("dl");
        dl.className = "ci-docs";
        for (const [field, desc] of entries) {
          const dt = document.createElement("dt");
          dt.textContent = field;
          const dd = document.createElement("dd");
          dd.textContent = desc;
          dl.appendChild(dt);
          dl.appendChild(dd);
        }
        docsPanel.appendChild(dl);
      }
      if (registryUrl) {
        const link = document.createElement("a");
        link.className = "ci-reg-link";
        link.href = registryUrl;
        link.target = "_blank";
        link.rel = "noopener";
        link.textContent = "Open registry entry (JSON Schema for SDK generators) ↗";
        docsPanel.appendChild(link);
      }
    }
  }

  function copy(text) {
    if (navigator.clipboard && text) {
      navigator.clipboard.writeText(text).catch(() => {});
    }
  }

  return { update, getKnobs: () => ({ ...knobState }) };
}
