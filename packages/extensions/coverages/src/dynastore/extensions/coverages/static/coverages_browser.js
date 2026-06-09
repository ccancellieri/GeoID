// coverages_browser.js — mounts the shared OGC browser shell with the coverage adapter.
// Navigates catalogs → collections; delegates body rendering to makeCoverageAdapter.

import { mountOgcBrowser } from "../static/common/ogc-browser.js";
import { makeCoverageAdapter } from "./coverage-adapter.js";

const root = document.querySelector("[data-ogc-root]");
mountOgcBrowser({
  root,
  basePath: "/coverages",
  adapter: makeCoverageAdapter({ basePath: "/coverages" }),
});
