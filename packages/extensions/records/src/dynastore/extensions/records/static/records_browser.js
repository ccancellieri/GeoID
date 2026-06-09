import { mountOgcBrowser } from "../static/common/ogc-browser.js";
import { makeItemsAdapter } from "../static/common/ogc-items-adapter.js";

const root = document.querySelector("[data-ogc-root]");
mountOgcBrowser({ root, basePath: "/records", adapter: makeItemsAdapter({ basePath: "/records" }) });
