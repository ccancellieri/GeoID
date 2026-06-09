// edr/edr_browser.js — entry point for the EDR Browser page.
// Mounts the shared OGC catalog->collection shell with the EDR position-query adapter.
import { mountOgcBrowser } from "../static/common/ogc-browser.js";
import { makeEdrAdapter } from "./edr-adapter.js";

const root = document.querySelector("[data-ogc-root]");
mountOgcBrowser({ root, basePath: "/edr", adapter: makeEdrAdapter({ basePath: "/edr" }) });
