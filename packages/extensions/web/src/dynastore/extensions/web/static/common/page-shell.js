// Shared page-shell custom element — ds-page-shell
//
// Renders the standard dark-theme page header used across all AIP browser
// pages. Dependency-free; works as a plain <script> import (no module
// bundler required).
//
// Attributes (all optional):
//   data-title       — page title text (localized by server before render)
//   data-breadcrumb  — supplementary breadcrumb text
//   data-logo-src    — absolute or relative URL to the brand logo image;
//                      defaults to a path resolved from this script's own URL
//   data-back-label  — label for the "Back to Home" link; defaults to "Back to Home"
//   data-back-href   — override the back-link href; defaults to "../"
//
// Serving-context support:
//   Admin pages are served at /web/static/{page}.html and load this script
//   via ../static/common/page-shell.js (the script is at /web/static/common/).
//   Extension pages are served at /web/{prefix}/{page}.html and load it via
//   ../static/common/page-shell.js (same relative path from their prefix dir).
//   In both cases document.currentScript.src is an absolute URL, so we resolve
//   the logo asset relative to the script's own directory rather than the
//   page's URL — that gives the right path in both contexts.
//
// iframe-safe "Back to Home":
//   When embedded in the shell iframe the element calls
//   window.parent.switchTab('home') and suppresses the href navigation,
//   replicating the onclick pattern used in extension pages.
//
// Opt-in utility classes (defined in page-shell.css, applied on demand):
//   .ds-page-body   — full-viewport flex column (body replacement for browser pages)
//   .ds-sidebar     — fixed-width sidebar panel

(function () {
  "use strict";

  // Resolve the logo URL from this script's location so both serving contexts
  // (admin at /web/static/common/ and extension prefix at /web/{ext}/…) land
  // on the same physical file at /web/static/dynastore.png.
  function _defaultLogoSrc() {
    const src =
      (typeof document !== "undefined" &&
        document.currentScript &&
        document.currentScript.src) ||
      "";
    if (!src) return "../static/dynastore.png";
    // Strip filename, keep directory, go up one level from common/ to static/
    const dir = src.replace(/\/[^/]*$/, "");        // …/common
    const staticDir = dir.replace(/\/[^/]*$/, ""); // …/static
    return staticDir + "/dynastore.png";
  }

  class DsPageShell extends HTMLElement {
    connectedCallback() {
      const title = this.getAttribute("data-title") || "";
      const breadcrumb = this.getAttribute("data-breadcrumb") || "";
      const logoSrc = this.getAttribute("data-logo-src") || _defaultLogoSrc();
      const backLabel = this.getAttribute("data-back-label") || "Back to Home";
      const backHref = this.getAttribute("data-back-href") || "../";

      // Build header DOM imperatively — no innerHTML, all text via
      // textContent so caller-supplied strings are never executed.
      const header = document.createElement("header");
      header.className = "header";

      const titleDiv = document.createElement("div");
      titleDiv.className = "header-title";

      const img = document.createElement("img");
      img.src = logoSrc;
      img.alt = "AIP Logo";
      titleDiv.appendChild(img);

      if (breadcrumb) {
        const bc = document.createElement("span");
        bc.className = "header-breadcrumb";
        bc.textContent = breadcrumb;
        titleDiv.appendChild(bc);

        const sep = document.createElement("span");
        sep.className = "header-breadcrumb-sep";
        sep.setAttribute("aria-hidden", "true");
        sep.textContent = " / ";
        titleDiv.appendChild(sep);
      }

      const titleSpan = document.createElement("span");
      titleSpan.textContent = title;
      titleDiv.appendChild(titleSpan);

      const backLink = document.createElement("a");
      backLink.href = backHref;
      backLink.className = "back-link";
      // Replicate the iframe-safe onclick pattern used across extension pages.
      backLink.addEventListener("click", function (e) {
        if (
          window.parent !== window &&
          typeof window.parent.switchTab === "function"
        ) {
          e.preventDefault();
          window.parent.switchTab("home");
        }
      });

      const arrow = document.createElement("i");
      arrow.className = "fa-solid fa-arrow-left";
      arrow.setAttribute("aria-hidden", "true");
      backLink.appendChild(arrow);
      backLink.appendChild(document.createTextNode(" "));
      backLink.appendChild(document.createTextNode(backLabel));

      header.appendChild(titleDiv);
      header.appendChild(backLink);

      this.appendChild(header);
    }
  }

  if (typeof customElements !== "undefined" && !customElements.get("ds-page-shell")) {
    customElements.define("ds-page-shell", DsPageShell);
  }
})();
