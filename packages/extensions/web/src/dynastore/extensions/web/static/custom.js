/**
 * DynaStore Web Interface Logic
 * Handles Navigation, Documentation System, Dashboard, Extensions, and I18n
 */

// Compute API root from script URL so fetch calls work on any page regardless of depth.
// custom.js is always served at {root}/web/static/custom.js — strip that suffix to get root.
const _SCRIPT_ROOT = (function () {
    const s = document.currentScript;
    return s ? s.src.replace(/\/web\/static\/custom\.js(\?.*)?$/, '') : '';
}());

// Global State
let docsManifest = null;
let docsLoaded = false;
let dashboardPulse = null;
let dashboardLastUpdate = 0;
let currentLocale = 'en';
let currentUser = null;
let authToken = null;
let platformConfig = null;
let TOKEN_KEY = 'ds_token'; // Default fallback
const REFRESH_KEY = 'ds_refresh_token';
let _refreshTimer = null;

/**
 * Returns the API root (origin + proxy prefix), e.g. 'https://host/geospatial/v2/api/auth'
 * or 'http://localhost:8080' for root deployments.
 *
 * Falls back to _SCRIPT_ROOT (derived synchronously from this script's own URL) when
 * platformConfig is not yet loaded. This is critical: the OIDC code->token exchange runs
 * during bootstrap BEFORE platformConfig is available (and /configs/plugins/web_config is
 * itself auth-gated, so it 403s pre-login), which previously left apiRoot() returning ''
 * and POSTed the exchange to an unprefixed /auth/token -> 404 behind a proxy prefix.
 */
function apiRoot() {
    if (platformConfig && platformConfig.root_path) return platformConfig.root_path.replace(/\/$/, '');
    return _SCRIPT_ROOT;
}


// --- I18n & Interface Logic ---
function toggleLangDropdown() {
    const dd = document.getElementById('lang-dropdown');
    if (dd) dd.classList.toggle('show');
}

function setLanguage(lang) {
    if (!lang) return;
    currentLocale = lang;
    localStorage.setItem('ds_lang', lang);
    const label = document.getElementById('current-lang-label');
    if (label) label.innerText = lang.toUpperCase();
    
    // Refresh sidebar and content
    loadSidebar();
    const currentHash = window.location.hash.substring(1).split(':')[0] || 'home';
    switchTab(currentHash);
    
    const langDd = document.getElementById('lang-dropdown');
    if (langDd) langDd.classList.remove('show');
}

async function loadSidebar() {
    const container = document.getElementById('nav-container');
    if (!container) return;

    try {
        const token = authToken || localStorage.getItem(TOKEN_KEY) || sessionStorage.getItem(TOKEN_KEY);
        const headers = token ? { 'Authorization': `Bearer ${token}` } : {};
        const res = await fetch(`config/pages?language=${currentLocale}`, { headers });
        if (!res.ok) throw new Error("Failed to load nav config");
        const allPages = await res.json();

        // 1. Separate Top-level items from fragments
        const pages = allPages.filter(p => !p.is_embed);
        
        // 2. Find Top-level items.
        //    A page is surfaced as a top-level app when:
        //      - it has no section (classic top-level page), OR
        //      - it opts in via show_as_tile (first-class app that should be
        //        launchable directly, while still grouped under its section), OR
        //      - its declared section parent is not present in this deployment
        //        (orphan promotion). Some catalog scopes ship only a subset of
        //        pages — e.g. an api_open catalog has no Admin shell — so a
        //        sub-page whose parent is absent would otherwise be unreachable.
        const sectionParentIds = new Set(pages.filter(p => !p.section).map(p => p.id));
        const isTopLevel = p =>
            !p.section || p.show_as_tile || !sectionParentIds.has(p.section);

        const topLevelPages = pages.filter(isTopLevel).sort((a, b) => {
            if (a.id === 'home') return -1;
            if (b.id === 'home') return 1;
            if (b.priority !== a.priority) return b.priority - a.priority;
            return a.title.localeCompare(b.title);
        });

        container.innerHTML = topLevelPages.map(page => {
            // Find pages targeting this one as a section (never list a page as
            // its own child, in case a promoted app shares its id with a section).
            const subPages = pages.filter(p => p.section === page.id && p.id !== page.id).sort((a, b) => (b.priority || 0) - (a.priority || 0));
            const hasSubPages = subPages.length > 0;

            // Restore collapsed state from localStorage (default: expanded)
            const storageKey = `ds_nav_section_${page.id}`;
            const isCollapsed = hasSubPages && localStorage.getItem(storageKey) === 'collapsed';

            let subHtml = '';
            if (hasSubPages) {
                subHtml = `
                    <div class="nav-subsection mt-1 mb-3 ml-4 border-l border-white/5 pl-2 space-y-1 hidden lg:block${isCollapsed ? ' ds-nav-hidden' : ''}">
                        ${subPages.map(s => `
                            <button onclick="switchTab('${s.id}')" id="nav-${s.id}" class="nav-btn w-full flex items-center gap-2 px-3 py-1.5 rounded-lg text-slate-500 hover:text-blue-400 hover:bg-white/5 transition-all text-xs">
                                <i class="fa-solid ${s.icon} w-4 text-center text-[10px]"></i>
                                <span class="truncate font-medium">${s.title}</span>
                            </button>
                        `).join('')}
                    </div>
                `;
            }

            const chevronHtml = hasSubPages ? `
                <button class="nav-chevron ml-auto hidden lg:flex items-center justify-center w-5 h-5 text-slate-500 hover:text-slate-300 transition-all flex-shrink-0${isCollapsed ? ' ds-chevron-collapsed' : ''}"
                        aria-label="Toggle ${page.title} section"
                        onclick="event.stopPropagation(); toggleNavSection('${page.id}', this)">
                    <i class="fa-solid fa-chevron-down text-[10px] transition-transform duration-200"></i>
                </button>
            ` : '';

            return `
                <div class="nav-group${isCollapsed ? ' ds-nav-group-collapsed' : ''}" data-section-id="${page.id}">
                    <div class="flex items-center gap-1">
                        <button onclick="switchTab('${page.id}')" id="nav-${page.id}" class="nav-btn flex-1 flex items-center gap-3 px-3 py-2.5 rounded-lg text-slate-400 hover:text-white hover:bg-white/5 transition-all group">
                            <i class="fa-solid ${page.icon} text-lg w-6 text-center group-hover:text-blue-400 transition-colors"></i>
                            <span class="hidden lg:block text-sm font-medium">${page.title}</span>
                        </button>
                        ${chevronHtml}
                    </div>
                    ${subHtml}
                </div>
            `;
        }).join('');

        // Ensure current active tab is highlighted
        updateActiveNav();

    } catch (e) {
        console.error("Sidebar load error:", e);
    }
}

function toggleNavSection(sectionId, chevronBtn) {
    const storageKey = `ds_nav_section_${sectionId}`;
    const group = document.querySelector(`.nav-group[data-section-id="${sectionId}"]`);
    if (!group) return;
    const subSection = group.querySelector('.nav-subsection');
    if (!subSection) return;

    const isCurrentlyCollapsed = subSection.classList.contains('ds-nav-hidden');
    if (isCurrentlyCollapsed) {
        // Expand
        subSection.classList.remove('ds-nav-hidden');
        group.classList.remove('ds-nav-group-collapsed');
        if (chevronBtn) chevronBtn.classList.remove('ds-chevron-collapsed');
        localStorage.setItem(storageKey, 'expanded');
    } else {
        // Collapse
        subSection.classList.add('ds-nav-hidden');
        group.classList.add('ds-nav-group-collapsed');
        if (chevronBtn) chevronBtn.classList.add('ds-chevron-collapsed');
        localStorage.setItem(storageKey, 'collapsed');
    }
}

function updateActiveNav(activeTabId) {
    const tabId = activeTabId || window.location.hash.substring(1).split(':')[0] || 'home';
    document.querySelectorAll('.nav-btn').forEach(btn => btn.classList.remove('active-tab'));
    const activeBtn = document.getElementById(`nav-${tabId}`);
    if (activeBtn) activeBtn.classList.add('active-tab');
}

async function switchTab(tabId) {
    const contentArea = document.getElementById('tab-content');
    const wrapper = document.getElementById('content-area');
    if (!contentArea) return;

    // 1. Update Navigation UI immediately with the target tab
    updateActiveNav(tabId);

    // 2. Show Loader
    contentArea.innerHTML = '<div class="flex items-center justify-center h-full py-40"><i class="fa-solid fa-circle-notch fa-spin text-4xl text-blue-500"></i></div>';

    let isFullPage = false;
    const controller = new AbortController();
    const timeoutId = setTimeout(() => controller.abort(), 10000);
    
    try {
        // 3. Fetch Content Fragment
        const res = await fetch(`pages/${tabId}?language=${currentLocale}`, {
            signal: controller.signal
        });
        clearTimeout(timeoutId);
        if (!res.ok) throw new Error(`Failed to load ${tabId}`);
        const html = await res.text();

        // 4. Detect full-page document vs fragment.
        //
        // Two distinct rendering modes:
        //
        // (a) IFRAME mode — the response is a standalone HTML document that
        //     manages its own <style> tags and JS.  Triggered by either:
        //       • The response starting with <!DOCTYPE or <html (legacy heuristic,
        //         kept for backward compatibility with existing admin pages that
        //         return full documents), OR
        //       • The fragment's root element carrying data-full-page="true"
        //         (explicit opt-in — use this for new full-document pages).
        //
        // (b) FRAGMENT mode — the response is an HTML snippet injected directly
        //     into the shell.  A fragment whose root element carries
        //     data-fills-viewport will expand to fill the available height
        //     without the iframe isolation overhead (e.g. map viewer widget).
        //
        // Extension authors: prefer the explicit attributes over relying on
        // DOCTYPE detection.  Return a fragment with data-full-page on the root
        // element when you need iframe isolation; return a fragment with
        // data-fills-viewport on the root when you only need full-height layout.

        // Quick probe: inject the raw html into a temporary container to check
        // root-element attributes before committing to a rendering path.
        const _probe = document.createElement('div');
        _probe.innerHTML = html;
        const _rootEl = _probe.firstElementChild;
        const _hasFullPageAttr = _rootEl && _rootEl.hasAttribute('data-full-page');

        isFullPage = _hasFullPageAttr
            || /^\s*<!DOCTYPE/i.test(html)
            || /^\s*<html/i.test(html);

        if (isFullPage) {
            // Full-page app: render in iframe, expand wrapper to fill height
            if (wrapper) {
                wrapper.classList.add('!p-0');
                wrapper.classList.remove('overflow-y-auto');
                wrapper.style.overflow = 'hidden';
            }
            contentArea.classList.remove('max-w-7xl', 'mx-auto', 'fade-in');
            contentArea.style.cssText = 'height:100%;display:flex;flex-direction:column;';
            contentArea.innerHTML = `<iframe src="pages/${tabId}?language=${currentLocale}" style="width:100%;flex:1;border:none;display:block;" allowfullscreen></iframe>`;
        } else {
            // Fragment: inject HTML then check if it requests full-height treatment.
            // Check both the root element (explicit opt-in) and any descendant
            // (legacy pattern used by some existing map-viewer fragments).
            contentArea.classList.add('max-w-7xl', 'mx-auto');
            contentArea.style.cssText = '';
            contentArea.innerHTML = html;
            executeScripts(contentArea);

            const fillsViewport = contentArea.querySelector('[data-fills-viewport]');
            if (fillsViewport) {
                // Full-height fragment (e.g. map viewer): mirror full-page wrapper treatment
                if (wrapper) {
                    wrapper.classList.add('!p-0');
                    wrapper.classList.remove('overflow-y-auto');
                    // Set height explicitly so percentage heights inside the fragment resolve correctly
                    wrapper.style.cssText = 'overflow:hidden;height:100%;display:flex;flex-direction:column;';
                }
                contentArea.classList.remove('max-w-7xl', 'mx-auto');
                contentArea.style.cssText = 'flex:1;min-height:0;display:flex;flex-direction:column;';
                fillsViewport.style.cssText = 'flex:1;min-height:0;';
            } else {
                // Normal scrollable fragment: restore wrapper defaults
                if (wrapper) {
                    wrapper.classList.remove('!p-0');
                    wrapper.classList.add('overflow-y-auto');
                    wrapper.style.cssText = '';
                }
            }

            contentArea.classList.remove('fade-in');
            void contentArea.offsetWidth; // Force reflow
            contentArea.classList.add('fade-in');
        }

        // 5. Component-Specific Handlers
        if (tabId === 'docs') initDocs();
        if (tabId === 'dashboard') {
            initDashboard();
            if (!dashboardPulse) dashboardPulse = setInterval(updateDashboard, 5000);
        } else {
            if (dashboardPulse) { clearInterval(dashboardPulse); dashboardPulse = null; }
        }

    } catch (e) {
        console.error(`Error switching to tab ${tabId}:`, e);
        clearTimeout(timeoutId);
        
        if (e.name === 'AbortError') {
            contentArea.innerHTML = `
                <div class="p-20 text-center">
                    <i class="fa-solid fa-clock text-4xl mb-4 text-yellow-500"></i>
                    <p class="text-lg mb-2">Loading timed out</p>
                    <p class="text-sm text-gray-500 mb-4">The page took too long to load.</p>
                    <button onclick="window.switchTab('${tabId}')" class="px-4 py-2 bg-blue-500 text-white rounded hover:bg-blue-600">
                        <i class="fa-solid fa-redo mr-2"></i>Retry
                    </button>
                </div>`;
        } else {
            contentArea.innerHTML = `
                <div class="p-20 text-center text-red-400">
                    <i class="fa-solid fa-triangle-exclamation text-4xl mb-4"></i>
                    <p>Failed to load ${tabId}</p>
                    <p class="text-sm mt-2">${e.message}</p>
                    <button onclick="window.switchTab('${tabId}')" class="mt-4 px-4 py-2 bg-blue-500 text-white rounded hover:bg-blue-600">
                        <i class="fa-solid fa-redo mr-2"></i>Retry
                    </button>
                </div>`;
        }
    }

    // 6. Update Hash and Scroll
    if (window.location.hash !== `#${tabId}`) {
        window.history.pushState(null, null, `#${tabId}`);
    }
    if (!isFullPage) window.scrollTo({ top: 0, behavior: 'smooth' });
}

// --- Extensions Logic ---
// We keep loadExtensions for when the 'extensions' tab specifically needs to re-populate its grid
async function loadExtensions() {
    const container = document.getElementById('extensions-grid');
    if (!container) return;
    
    container.innerHTML = '<div class="col-span-full text-center py-20 text-slate-500"><i class="fa-solid fa-circle-notch fa-spin text-2xl"></i><p class="mt-2">Loading extensions...</p></div>';

    try {
        const res = await fetch(`config/pages?language=${currentLocale}`);
        if (!res.ok) throw new Error("Failed to load extensions config");
        const pages = await res.json();
        
        const corePageIds = ['home', 'docs', 'extensions', 'dashboard'];
        const appPages = pages.filter(p =>
            (p.show_as_tile || p.section === 'extensions' || !p.section) &&
            !corePageIds.includes(p.id) &&
            !p.is_embed
        );

        if (appPages.length === 0) {
            container.innerHTML = '<div class="col-span-full text-center py-20 text-slate-500">No additional applications available.</div>';
            return;
        }

        container.innerHTML = appPages.map(page => `
            <div class="glass-card p-6 rounded-xl hover:bg-white/5 transition-colors group cursor-pointer border border-white/5 hover:border-blue-500/30" onclick="openExtension('${page.id}')">
                <div class="flex items-start justify-between mb-4">
                    <div class="w-12 h-12 bg-blue-500/10 rounded-lg flex items-center justify-center text-blue-400 group-hover:scale-110 transition-transform">
                        <i class="fa-solid ${page.icon} text-xl"></i>
                    </div>
                </div>
                <h3 class="text-lg font-bold text-white mb-2">${page.title}</h3>
                <p class="text-sm text-slate-400 mb-6 h-10 overflow-hidden text-ellipsis line-clamp-2">${page.description || 'No description available.'}</p>
                <div class="flex items-center text-blue-400 text-xs font-mono font-bold group-hover:underline uppercase tracking-wider">
                    Launch App <i class="fa-solid fa-arrow-right ml-2 opacity-0 group-hover:opacity-100 transition-opacity"></i>
                </div>
            </div>
        `).join('');

    } catch (e) {
        console.error("Extensions load error:", e);
        container.innerHTML = '<div class="col-span-full text-center py-20 text-red-400 border border-red-500/20 bg-red-500/5 rounded-xl">Failed to load extensions.</div>';
    }
}

function openExtension(pageId) {
    let viewer = document.getElementById('section-app-viewer');
    
    if (!viewer) {
        console.error("Viewer element not found!");
        return;
    }
    
    const frame = document.getElementById('app-viewer-frame');
    // Use relative path for pages
    frame.src = `pages/${pageId}?language=${currentLocale}`;
    
    viewer.classList.remove('hidden-section');
    document.body.style.overflow = 'hidden'; // Lock body scroll

    // Update hash for back button support
    if (window.location.hash !== `#ext:${pageId}`) {
        window.history.pushState(null, null, `#ext:${pageId}`);
    }
}

function closeExtension(skipHistory = false) {
    const viewer = document.getElementById('section-app-viewer');
    if (viewer) {
        viewer.classList.add('hidden-section');
        const frame = document.getElementById('app-viewer-frame');
        frame.src = 'about:blank';
        document.body.style.overflow = ''; // Restore body scroll

        if (!skipHistory && window.location.hash.startsWith('#ext:')) {
            window.history.pushState(null, null, '#extensions');
        }
    }
}

// --- Documentation System Logic ---
async function initDocs() {
    docsLoaded = true;
    const contentDiv = document.getElementById('docs-content');
    const sidebarDiv = document.getElementById('docs-sidebar-content');
    
    if (sidebarDiv) sidebarDiv.innerHTML = '<div class="py-4 text-center text-slate-500"><i class="fa-solid fa-spinner fa-spin"></i> <span class="text-xs ml-2">Loading docs...</span></div>';
    
    try {
        // Relative path
        const response = await fetch('docs-manifest');
        if (!response.ok) throw new Error('Failed to load docs manifest');
        docsManifest = await response.json();
        renderSidebar(docsManifest);
        if (contentDiv) contentDiv.innerHTML = '<div class="py-12 text-center text-slate-600"><p>Select a document from the sidebar to view</p></div>';
        handleHashChange();
    } catch (e) {
        console.error(e);
        if (contentDiv) contentDiv.innerHTML = `<div class="p-6 border border-red-500/20 bg-red-500/10 rounded-lg text-red-400"><h3>Error</h3><p>${e.message}</p></div>`;
        if (sidebarDiv) sidebarDiv.innerHTML = '<div class="text-red-400 text-xs p-4">Error loading docs</div>';
    }
}

function renderSidebar(manifest) {
    const sidebar = document.getElementById('docs-sidebar-content');
    if (!sidebar) return;

    let html = '';
    const categoryMapping = {
        'platform':     { title: 'Platform',     icon: 'fa-book-open' },
        'architecture': { title: 'Architecture', icon: 'fa-sitemap' },
        'components':   { title: 'Components',   icon: 'fa-puzzle-piece' },
        'modules':      { title: 'Modules',      icon: 'fa-cubes' },
        'extensions':   { title: 'Extensions',   icon: 'fa-layer-group' },
        'tasks':        { title: 'Tasks',        icon: 'fa-gears' },
        'root':         { title: 'Overview',     icon: 'fa-circle-info' },
    };

    // Fixed display order for known categories; unknown ones go last alphabetically
    const categoryOrder = ['platform', 'architecture', 'components', 'modules', 'extensions', 'tasks', 'root'];
    const sortedKeys = Object.keys(manifest).sort((a, b) => {
        const ia = categoryOrder.indexOf(a), ib = categoryOrder.indexOf(b);
        if (ia !== -1 && ib !== -1) return ia - ib;
        if (ia !== -1) return -1;
        if (ib !== -1) return 1;
        return a.localeCompare(b);
    });

    sortedKeys.forEach(catKey => {
        const items = manifest[catKey];
        if (items && items.length > 0) {
            const info = categoryMapping[catKey] || { title: catKey.charAt(0).toUpperCase() + catKey.slice(1), icon: 'fa-circle-info' };
            html += `
                <div class="mb-6">
                    <h3 class="px-4 text-[10px] font-bold text-slate-500 uppercase tracking-widest mb-2 flex items-center gap-2">
                        <i class="fa-solid ${info.icon} text-slate-600"></i> ${info.title}
                    </h3>
                    <ul class="space-y-0.5">
            `;
            items.forEach(doc => {
                const displayName = doc.title || doc.id;
                html += `
                    <li>
                        <a href="#docs:${doc.id}" class="doc-sidebar-link group flex items-center px-4 py-2 text-sm text-slate-400 hover:text-white hover:bg-white/5 transition-all border-l-2 border-transparent" data-doc-id="${doc.id}">
                            <span class="truncate">${displayName}</span>
                        </a>
                    </li>
                `;
            });
            html += `</ul></div>`;
        }
    });

    sidebar.innerHTML = html;
    
    document.querySelectorAll('.doc-sidebar-link').forEach(link => {
        link.addEventListener('click', function(e) {
            e.preventDefault();
            const docId = this.getAttribute('data-doc-id');
            loadDocContent(docId);
            window.location.hash = `#docs:${docId}`;
        });
    });
}

async function loadDocContent(docId) {
    const contentDiv = document.getElementById('docs-content');
    if (!contentDiv) return;
    contentDiv.innerHTML = '<div class="py-20 text-center"><i class="fa-solid fa-circle-notch fa-spin text-2xl text-blue-500"></i><p class="text-slate-500 mt-2 text-sm">Loading...</p></div>';
    
    document.querySelectorAll('.doc-sidebar-link').forEach(l => {
        l.classList.remove('active', 'text-blue-400', 'bg-blue-500/10', 'border-blue-500', 'border-l-2');
        l.classList.add('border-transparent');
    });
    const activeLink = document.querySelector(`.doc-sidebar-link[data-doc-id="${docId}"]`);
    if (activeLink) {
        activeLink.classList.remove('border-transparent');
        activeLink.classList.add('active', 'text-blue-400', 'bg-blue-500/10', 'border-blue-500', 'border-l-2');
    }

    try {
        const res = await fetch(`docs-content/${docId}`);
        if (!res.ok) throw new Error(`HTTP ${res.status}`);
        const html = await res.text();
        contentDiv.innerHTML = `<div class="prose prose-invert max-w-3xl mx-auto">${html}</div>`;
        
        // Generate IDs for headers
        contentDiv.querySelectorAll('h1, h2, h3, h4, h5, h6').forEach(header => {
            if (!header.id) header.id = header.textContent.toLowerCase().trim().replace(/[^\w\s-]/g, '').replace(/\s/g, '-');
        });
    } catch (e) {
        contentDiv.innerHTML = `<div class="p-8 text-center text-red-400 border border-red-500/20 bg-red-500/5 rounded-xl">Document not found: ${docId}</div>`;
    }
}

function handleHashChange() {
    const hash = window.location.hash;

    if (hash.startsWith('#ext:')) {
        const pageId = hash.substring(5);
        openExtension(pageId);
        return;
    } else {
        // If we were in an extension and shifted to something else, close it
        const viewer = document.getElementById('section-app-viewer');
        if (viewer && !viewer.classList.contains('hidden-section')) {
            closeExtension(true);
        }
    }

    if (!docsLoaded && hash.startsWith('#docs')) return;

    if (hash.startsWith('#docs:')) {
        loadDocContent(hash.substring(6));
        return;
    }
    if (hash === '#docs' && docsManifest) {
        const firstCat = Object.keys(docsManifest)[0];
        if (firstCat && docsManifest[firstCat].length > 0) {
            loadDocContent(docsManifest[firstCat][0].id);
        }
        return;
    }

    // Generic page route: any other `#<page-id>` is fetched as a fragment.
    // Without this, browser-back / forward / a manual URL edit between page
    // hashes (e.g. `#admin` -> `#geoid`) updated the hash but left the
    // content area stuck on the previous page.  switchTab handles fetch
    // failures (404 / 403) internally.
    if (hash && hash !== '#' && hash !== '#home') {
        switchTab(hash.substring(1));
    } else if (!hash || hash === '#' || hash === '#home') {
        switchTab('home');
    }
}

// --- Dashboard Logic ---
async function initDashboard() { updateDashboard(); }
async function updateDashboard() {
    if (Date.now() - dashboardLastUpdate < 2000) return;
    dashboardLastUpdate = Date.now();
    await Promise.all([fetchDashboardStats(), fetchDashboardLogs(), fetchDashboardTasks()]);
}

async function fetchDashboardStats() {
    try {
        // Platform-tier overview — sysadmin-only via the
        // web_dashboard_platform_access policy. Non-sysadmin callers get 403
        // and the catch below swallows it so the UI tile stays empty.
        const res = await fetch('dashboard/stats', {
            headers: authToken ? { 'Authorization': `Bearer ${authToken}` } : {},
        });
        if (!res.ok) return;
        const stats = await res.json();
        if (document.getElementById('stat-total-requests')) {
            document.getElementById('stat-total-requests').innerText =
                (stats.total_requests || 0).toLocaleString();
        }
        if (document.getElementById('stat-avg-latency')) {
            document.getElementById('stat-avg-latency').innerText =
                `${Math.round(stats.average_latency_ms || 0)}ms`;
        }
        // Compute success rate from status_code_distribution (defect D fix).
        // Only display when the distribution has data; avoid a fake 100%.
        const rateEl = document.getElementById('stat-success-rate');
        if (rateEl) {
            const dist = stats.status_code_distribution || {};
            const total = Object.values(dist).reduce((s, n) => s + n, 0);
            if (total > 0) {
                const success = Object.entries(dist)
                    .filter(([code]) => code.startsWith('2') || code.startsWith('3'))
                    .reduce((s, [, n]) => s + n, 0);
                rateEl.innerText = `${Math.round((success / total) * 100)}%`;
            } else {
                rateEl.innerText = '—';
            }
        }
    } catch(e) {}
}

async function fetchDashboardLogs() {
    try {
        const level = document.getElementById('log-filter-level')?.value || 'INFO';
        // Must carry the proxy prefix: a bare /logs/system resolves against the
        // proxy root (e.g. https://host/logs/system) and 404s behind a base path
        // like /geospatial/dev/api/catalog. apiRoot() supplies the prefix, as it
        // does for every other absolute API call in this file.
        const res = await fetch(`${apiRoot()}/logs/system?limit=50&level=${level}`, {
            headers: authToken ? { 'Authorization': `Bearer ${authToken}` } : {},
        });
        if (!res.ok) return;
        const data = await res.json();
        const logs = data.logs || [];
        const container = document.getElementById('dashboard-logs');
        if (!container) return;
        while (container.firstChild) container.removeChild(container.firstChild);
        if (!logs.length) {
            const empty = document.createElement('div');
            empty.className = 'text-slate-500 text-center py-4';
            empty.textContent = 'No logs';
            container.appendChild(empty);
            return;
        }
        for (const l of logs) {
            const ts = l.timestamp || l.created_at;
            const date = ts ? new Date(ts) : new Date();
            const timeStr = isNaN(date.getTime()) ? new Date().toLocaleTimeString() : date.toLocaleTimeString();
            const row = document.createElement('div');
            row.className = 'text-xs font-mono p-1 hover:bg-white/5 flex gap-2';
            const tEl = document.createElement('span');
            tEl.className = 'text-slate-500 shrink-0';
            tEl.textContent = timeStr;
            const levelEl = document.createElement('span');
            const levelColor = l.level === 'ERROR' ? 'text-red-400'
                : l.level === 'WARN' ? 'text-yellow-400'
                : 'text-blue-400';
            levelEl.className = `font-bold ${levelColor} w-12 shrink-0`;
            levelEl.textContent = l.level || '';
            const msgEl = document.createElement('span');
            msgEl.className = 'break-all';
            msgEl.textContent = l.message || '';
            row.appendChild(tEl);
            row.appendChild(levelEl);
            row.appendChild(msgEl);
            container.appendChild(row);
        }
    } catch(e) { console.error('Failed to load dashboard logs', e); }
}

async function fetchDashboardTasks() {
    try {
        const res = await fetch('dashboard/tasks', {
            headers: authToken ? { 'Authorization': `Bearer ${authToken}` } : {},
        });
        if(!res.ok) return;
        const tasks = await res.json();
        const container = document.getElementById('dashboard-tasks');
        if(container) container.innerHTML = tasks.length ? tasks.map(t => `<div class="p-2 border-b border-white/5"><div class="flex justify-between"><div class="font-bold text-sm">${t.type}</div><div class="text-xs text-purple-300 bg-purple-500/10 px-1 rounded">${t.status}</div></div><div class="text-[10px] text-slate-500">${t.id}</div></div>`).join('') : '<div class="text-slate-500 text-center py-4">No active tasks</div>';
        if(document.getElementById('stat-active-tasks')) document.getElementById('stat-active-tasks').innerText = tasks.length;
    } catch(e){}
}

// --- Initialization ---
document.addEventListener('DOMContentLoaded', async () => {
    // Restore sidebar collapsed state
    if (localStorage.getItem('ds_sidebar_collapsed') === '1') {
        const sidebar = document.getElementById('main-sidebar');
        const icon = document.getElementById('sidebar-toggle-icon');
        if (sidebar) sidebar.classList.add('sidebar-collapsed');
        if (icon) icon.className = 'fa-solid fa-angles-right';
    }

    // 1. Load sidebar config and render
    await loadSidebar();

    window.addEventListener('hashchange', handleHashChange);

    // Resolve the initial tab from the URL hash.  Previously this routed only
    // a hard-coded set (docs/dashboard/extensions) and silently dropped every
    // other hash to `home`, so reloading a deep link like `#admin`, `#geoid`,
    // or `#stac_browser` always landed on the landing page.  Generalised:
    //   - `#ext:<id>`       -> openExtension(id)
    //   - `#docs[:...]`     -> docs tab (preserve docs-specific behaviour)
    //   - `#<page-id>`      -> switchTab('<page-id>') for any page id (admin,
    //                          geoid, stac_browser, dashboard, notebooks, …)
    //                          — switchTab handles fetch failures internally.
    //   - empty / `#home`   -> switchTab('home')
    const hash = window.location.hash;
    if (hash.startsWith('#ext:')) {
        openExtension(hash.substring(5));
    } else if (hash.startsWith('#docs')) {
        await switchTab('docs');
    } else if (hash && hash !== '#' && hash !== '#home') {
        await switchTab(hash.substring(1));
    } else {
        await switchTab('home');
    }

    // Language Selector Listeners
    document.querySelectorAll('.lang-btn').forEach(btn => {
        btn.addEventListener('click', (e) => {
            setLanguage(e.target.dataset.lang);
        });
    });

    // CI Status Badge Panel
    const badgeTrigger = document.getElementById('badge-trigger');
    const badgePanel = document.getElementById('badge-panel');

    if (badgeTrigger && badgePanel) {
        badgeTrigger.addEventListener('click', (e) => {
            e.stopPropagation();
            const wasHidden = badgePanel.classList.contains('hidden');
            badgePanel.classList.toggle('hidden');
            if (wasHidden) loadTestResults();
        });

        document.addEventListener('click', (e) => {
            if (!badgePanel.contains(e.target) && !badgeTrigger.contains(e.target)) {
                badgePanel.classList.add('hidden');
            }
        });
    }

    // Global click listener to close dropdowns
    document.addEventListener('click', (e) => {
        // Lang dropdown
        const langDd = document.getElementById('lang-dropdown');
        const langBtn = document.getElementById('lang-dropdown-btn');
        if (langDd && langBtn && !langDd.contains(e.target) && !langBtn.contains(e.target)) {
            langDd.classList.remove('show');
        }

        // User dropdown
        const userDd = document.getElementById('user-dropdown');
        const userBtn = document.getElementById('user-menu-btn');
        if (userDd && userBtn && !userDd.contains(e.target) && !userBtn.contains(e.target)) {
            hideUserDropdown();
        }
    });

    // --- Bootstrapping Platform & Auth ---
    bootstrap();

    // OGC compliance is rendered server-side into the home page fragment
    // (web.py home_page() reads get_conformance_summary() directly), so the
    // browser doesn't fetch /web/dashboard/ogc-compliance on first paint.
    // The loadOgcCompliance() function is preserved below for any future
    // page-specific call site (admin / dashboard) that wants the live
    // dashboard endpoint.

});

function executeScripts(container) {
    const scripts = container.querySelectorAll('script');
    scripts.forEach(oldScript => {
        const newScript = document.createElement('script');
        Array.from(oldScript.attributes).forEach(attr => newScript.setAttribute(attr.name, attr.value));
        newScript.appendChild(document.createTextNode(oldScript.innerHTML));
        oldScript.parentNode.replaceChild(newScript, oldScript);
    });
}

// --- CI Test Results Panel ---

async function loadTestResults() {
    const content = document.getElementById('ci-results-content');
    const meta = document.getElementById('ci-meta');
    const dot = document.getElementById('ci-status-dot');
    if (!content) return;

    try {
        const res = await fetch(`${_SCRIPT_ROOT}/web/static/test-results.json`);
        if (!res.ok) throw new Error(`HTTP ${res.status}`);
        const data = await res.json();

        const statusColor = {
            passed: 'bg-emerald-500',
            failed: 'bg-red-500',
            error: 'bg-red-500',
            pending: 'bg-slate-600',
        };

        const overallOk = data.unit.status === 'passed' && data.integration.status === 'passed';
        const overallPending = data.unit.status === 'pending' && data.integration.status === 'pending';

        if (dot) {
            dot.className = `w-2 h-2 rounded-full ${overallPending ? 'bg-slate-600' : overallOk ? 'bg-emerald-500 animate-pulse' : 'bg-red-500 animate-pulse'}`;
        }

        function suiteRow(label, suite) {
            const color = statusColor[suite.status] || 'bg-slate-600';
            const textColor = suite.status === 'passed' ? 'text-emerald-400' : suite.status === 'failed' || suite.status === 'error' ? 'text-red-400' : 'text-slate-500';
            return `
                <div class="flex items-center justify-between gap-2">
                    <span class="text-slate-400 text-[10px]">${label}</span>
                    <div class="flex items-center gap-1.5">
                        ${suite.total > 0 ? `<span class="text-[9px] ${textColor}">${suite.passed}/${suite.total}</span>` : ''}
                        <span class="w-1.5 h-1.5 rounded-full ${color}"></span>
                    </div>
                </div>`;
        }

        content.innerHTML = `
            <div class="space-y-2">
                ${suiteRow('Unit Tests', data.unit)}
                ${suiteRow('Integration', data.integration)}
            </div>`;

        if (meta) {
            const ts = data.generated_at ? new Date(data.generated_at).toLocaleDateString() : '';
            meta.textContent = `${data.branch} · ${data.commit}${ts ? ' · ' + ts : ''}`;
        }
    } catch (e) {
        if (content) content.innerHTML = '<div class="text-slate-600 text-[10px] text-center py-1">Not available locally</div>';
        if (meta) meta.textContent = 'Run CI to generate results';
    }
}

async function bootstrap() {
    try {
        const resp = await fetch(`${_SCRIPT_ROOT}/configs/plugins/web_config`);
        if (resp.ok) {
            platformConfig = await resp.json();
            if (platformConfig.token_key) {
                TOKEN_KEY = platformConfig.token_key;
            }
            
            // Sync Brand Name
            const brandEl = document.getElementById('platform-brand-name');
            if (brandEl && platformConfig.brand_name) {
                brandEl.innerText = platformConfig.brand_name;
            }
            const subEl = document.getElementById('platform-brand-subtitle');
            if (subEl && platformConfig.brand_subtitle) {
                subEl.innerText = platformConfig.brand_subtitle;
            }
        }
    } catch (e) {
        console.warn("Failed to fetch platform config, using defaults:", e);
    }
    
    // Now load token
    authToken = localStorage.getItem(TOKEN_KEY);
    
    // Initialize Auth Session
    await initAuthSession();
}

// --- Authentication & Session Management ---

// --- Token Refresh Logic ---

/** Parse the exp claim from a JWT without verifying the signature. */
function _jwtExp(token) {
    try {
        const payload = JSON.parse(atob(token.split('.')[1]));
        return payload.exp || 0;
    } catch { return 0; }
}

/**
 * Store access + refresh tokens and schedule a proactive refresh 60 s before expiry.
 */
function storeTokens(data) {
    authToken = data.access_token;
    localStorage.setItem(TOKEN_KEY, authToken);
    if (data.refresh_token) {
        localStorage.setItem(REFRESH_KEY, data.refresh_token);
    }
    _scheduleTokenRefresh();
}

function _scheduleTokenRefresh() {
    if (_refreshTimer) clearTimeout(_refreshTimer);
    const exp = _jwtExp(authToken);
    if (!exp) return;
    const msUntilRefresh = (exp * 1000) - Date.now() - 60_000; // 60 s early
    if (msUntilRefresh <= 0) return; // already expired or imminent — handled reactively
    _refreshTimer = setTimeout(async () => {
        const ok = await tryRefreshToken();
        if (!ok) handleLogout();
    }, msUntilRefresh);
}

/**
 * Exchange the stored refresh token for a new access token.
 * Returns true on success, false on failure.
 */
async function tryRefreshToken() {
    const refreshToken = localStorage.getItem(REFRESH_KEY);
    if (!refreshToken) return false;
    try {
        const form = new URLSearchParams();
        form.append('refresh_token', refreshToken);
        const res = await fetch(`${apiRoot()}/auth/refresh`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
            body: form,
        });
        if (!res.ok) {
            localStorage.removeItem(REFRESH_KEY);
            return false;
        }
        const data = await res.json();
        storeTokens(data);
        console.log('Access token silently refreshed.');
        return true;
    } catch (e) {
        console.warn('Token refresh failed:', e);
        return false;
    }
}

/**
 * Initializes the auth session by checking for a 'code' in URL or existing token.
 */
async function initAuthSession() {
    const urlParams = new URLSearchParams(window.location.search);
    const code = urlParams.get('code');

    if (code) {
        console.log("Detected auth code, exchanging for token...");
        // Clean URL immediately for better UX
        const cleanUrl = window.location.protocol + "//" + window.location.host + window.location.pathname + window.location.hash;
        window.history.replaceState({path: cleanUrl}, '', cleanUrl);
        
        await exchangeCodeForToken(code);
    }

    if (authToken) {
        // Proactively refresh if the stored token is already expired or expiring soon
        if (_jwtExp(authToken) * 1000 < Date.now() + 30_000) {
            const refreshed = await tryRefreshToken();
            if (!refreshed) {
                localStorage.removeItem(TOKEN_KEY);
                authToken = null;
            }
        }
        if (authToken) await refreshUserProfile();
        else updateUserWidget(null);
    } else {
        updateUserWidget(null);
    }
}

async function exchangeCodeForToken(code) {
    try {
        const formData = new URLSearchParams();
        formData.append('grant_type', 'authorization_code');
        formData.append('code', code);
        formData.append('redirect_uri', window.location.pathname);
        formData.append('client_id', 'dynastore');

        const response = await fetch(`${apiRoot()}/auth/token`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
            body: formData
        });

        if (response.ok) {
            const data = await response.json();
            storeTokens(data);
            console.log("Auth token acquired and stored under:", TOKEN_KEY);
            await refreshUserProfile();
        } else {
            console.error("Token exchange failed", await response.text());
        }
    } catch (e) {
        console.error("Error during token exchange", e);
    }
}

async function refreshUserProfile(alreadyRefreshed = false) {
    if (!authToken) return;

    try {
        const response = await fetch(`${apiRoot()}/auth/userinfo`, {
            headers: { 'Authorization': `Bearer ${authToken}` }
        });

        if (response.ok) {
            currentUser = await response.json();
            console.log("User profile loaded:", currentUser);
            updateUserWidget(currentUser);
            // Reload sidebar so role-gated pages (admin, configs) become visible
            await loadSidebar();
            _scheduleTokenRefresh();
        } else if (response.status === 401) {
            // /userinfo rejected the access token. Try a silent refresh ONCE;
            // if the refresh succeeds but the new token is also rejected, the
            // failure is persistent (disabled account, audience drift, clock
            // skew, …) — sign out so the user gets a clear signal instead of
            // an invisible /userinfo-401 ↔ /refresh-200 loop (issue #516).
            if (alreadyRefreshed) {
                console.warn(
                    "/userinfo still 401 after a successful refresh — signing out to break the loop."
                );
                handleLogout();
                return;
            }
            const refreshed = await tryRefreshToken();
            if (refreshed) {
                await refreshUserProfile(true);
            } else {
                handleLogout();
            }
        } else {
            // Any other non-OK (403 authorization denial, 5xx, …) must NOT be
            // swallowed: the login bootstrap gates the UI on this call, so a
            // silent fall-through leaves the page in a half-logged-in, grayed
            // and locked state with no way forward. A refresh would not help a
            // 403 (the token is valid but the principal is denied), so sign out
            // to a clean signed-out shell — a clear, recoverable signal instead
            // of a stuck page. Distinct from the 401 path, which retries once.
            console.warn(
                `/userinfo returned ${response.status}; signing out to avoid a locked UI.`
            );
            handleLogout();
        }
    } catch (e) {
        console.error("Failed to fetch user profile", e);
    }
}

function updateUserWidget(user) {
    const userWidget = document.getElementById('user-widget');
    const authButtons = document.getElementById('auth-buttons');
    
    if (!user) {
        if (userWidget) userWidget.classList.add('hidden');
        if (authButtons) authButtons.classList.remove('hidden');
        // Refresh sidebar so role-gated entries disappear after sign-out.
        loadSidebar();
        return;
    }

    if (userWidget) userWidget.classList.remove('hidden');
    if (authButtons) authButtons.classList.add('hidden');

    // Update UI elements
    const nameEl = document.getElementById('user-display-name');
    const emailEl = document.getElementById('user-email');
    const avatarEl = document.getElementById('user-avatar');

    if (nameEl) nameEl.innerText = user.preferred_username || user.name || user.sub || 'User';
    if (emailEl) emailEl.innerText = user.email || '';

    if (avatarEl) {
        const initial = (user.preferred_username || user.name || user.sub || 'U').charAt(0).toUpperCase();
        avatarEl.innerText = initial;
    }

    // Role-gated nav (admin hub, governance, etc.) is rendered dynamically by
    // loadSidebar() from /web/config/pages — no static admin link to toggle here.
    loadSidebar();
}

function toggleSidebar() {
    const sidebar = document.getElementById('main-sidebar');
    const icon = document.getElementById('sidebar-toggle-icon');
    if (!sidebar) return;
    const isCollapsed = sidebar.classList.toggle('sidebar-collapsed');
    if (icon) {
        icon.className = isCollapsed ? 'fa-solid fa-angles-right' : 'fa-solid fa-angles-left';
    }
    localStorage.setItem('ds_sidebar_collapsed', isCollapsed ? '1' : '0');
}

function toggleUserDropdown() {
    const dd = document.getElementById('user-dropdown');
    if (!dd) return;
    const isHidden = dd.classList.contains('opacity-0');
    if (isHidden) {
        dd.classList.remove('pointer-events-none', 'opacity-0', 'scale-95');
        dd.classList.add('opacity-100', 'scale-100');
    } else {
        hideUserDropdown();
    }
}

function hideUserDropdown() {
    const dd = document.getElementById('user-dropdown');
    if (dd) {
        dd.classList.add('pointer-events-none', 'opacity-0', 'scale-95');
        dd.classList.remove('opacity-100', 'scale-100');
    }
}

function handleLogout() {
    if (_refreshTimer) clearTimeout(_refreshTimer);
    localStorage.removeItem(TOKEN_KEY);
    localStorage.removeItem(REFRESH_KEY);
    authToken = null;
    currentUser = null;
    // Navigate to the backend logout endpoint which terminates the Keycloak SSO
    // session. A bare reload only cleared localStorage — Keycloak's session cookie
    // survived, so a subsequent Sign-In silently re-authenticated without a prompt.
    window.location.href = `${apiRoot()}/auth/logout?redirect_uri=${encodeURIComponent('/web/')}`;
}

// --- Profile Modal Logic ---

function showProfileModal() {
    hideUserDropdown();
    const modal = document.getElementById('profile-modal');
    if (!modal) return;

    // Populate modal
    if (currentUser) {
        const displayName = currentUser.preferred_username || currentUser.name || currentUser.sub || 'User';
        document.getElementById('modal-display-name').innerText = displayName;
        document.getElementById('modal-email').innerText = currentUser.email || '';
        document.getElementById('modal-avatar-init').innerText = displayName.charAt(0).toUpperCase();
        document.getElementById('profile-name-input').value = displayName;
        document.getElementById('profile-lang-input').value = currentLocale;

        const roles = currentUser.roles || [...(currentUser.realm_roles || []), ...(currentUser.client_roles || [])];
        const topRole = roles.find(r => ['sysadmin', 'admin'].includes(r)) || roles[0] || 'user';
        document.getElementById('user-role-badge').innerText = topRole.toUpperCase();
    }

    modal.classList.remove('hidden');
    // Trigger animations
    setTimeout(() => {
        modal.classList.remove('opacity-0');
        modal.querySelector('.glass-panel').classList.remove('scale-95');
        modal.querySelector('.glass-panel').classList.add('scale-100');
    }, 10);

    _startTokenCountdown();
}

function hideProfileModal() {
    const modal = document.getElementById('profile-modal');
    if (!modal) return;

    modal.classList.add('opacity-0');
    modal.querySelector('.glass-panel').classList.remove('scale-100');
    modal.querySelector('.glass-panel').classList.add('scale-95');

    _stopTokenCountdown();

    setTimeout(() => {
        modal.classList.add('hidden');
    }, 300);
}

let _tokenCountdownTimer = null;

function _formatRemaining(secs) {
    if (!Number.isFinite(secs) || secs <= 0) return 'expired';
    const h = Math.floor(secs / 3600);
    const m = Math.floor((secs % 3600) / 60);
    const s = Math.floor(secs % 60);
    if (h > 0) return `${h}h ${m}m ${s}s`;
    if (m > 0) return `${m}m ${s}s`;
    return `${s}s`;
}

function _renderTokenCountdown() {
    const el = document.getElementById('token-expiry-countdown');
    if (!el) return;
    const exp = _jwtExp(authToken);
    if (!authToken || !exp) {
        el.innerText = 'no token';
        el.className = 'text-sm font-mono text-slate-500';
        return;
    }
    const remaining = exp - Math.floor(Date.now() / 1000);
    el.innerText = _formatRemaining(remaining);
    if (remaining <= 0) {
        el.className = 'text-sm font-mono text-red-400';
    } else if (remaining < 60) {
        el.className = 'text-sm font-mono text-amber-400';
    } else {
        el.className = 'text-sm font-mono text-emerald-400';
    }
}

function _startTokenCountdown() {
    _stopTokenCountdown();
    _renderTokenCountdown();
    _tokenCountdownTimer = setInterval(_renderTokenCountdown, 1000);
}

function _stopTokenCountdown() {
    if (_tokenCountdownTimer) {
        clearInterval(_tokenCountdownTimer);
        _tokenCountdownTimer = null;
    }
}

async function copyAccessToken() {
    const label = document.getElementById('token-copy-label');
    if (!authToken) {
        if (label) label.innerText = 'No token';
        return;
    }
    try {
        await navigator.clipboard.writeText(authToken);
        if (label) {
            const original = label.innerText;
            label.innerText = 'Copied!';
            setTimeout(() => { label.innerText = original; }, 1500);
        }
    } catch (e) {
        console.warn('Clipboard write failed:', e);
        if (label) {
            label.innerText = 'Copy failed';
            setTimeout(() => { label.innerText = 'Copy token'; }, 2000);
        }
    }
}

async function renewAccessToken() {
    const label = document.getElementById('token-renew-label');
    const icon = document.getElementById('token-renew-icon');
    if (label) label.innerText = 'Renewing…';
    if (icon) icon.classList.add('fa-spin');
    const ok = await tryRefreshToken();
    if (icon) icon.classList.remove('fa-spin');
    if (label) {
        label.innerText = ok ? 'Renewed' : 'Failed';
        setTimeout(() => { label.innerText = 'Renew'; }, 1500);
    }
    _renderTokenCountdown();
}

// Intercept profile form submission
document.getElementById('profile-form')?.addEventListener('submit', (e) => {
    e.preventDefault();
    alert('Profile updates are coming soon! This is a preview of the interface.');
    hideProfileModal();
});

// Handle password updates
async function updatePassword() {
    const currentPassword = document.getElementById('profile-current-password').value;
    const newPassword = document.getElementById('profile-new-password').value;
    const confirmPassword = document.getElementById('profile-confirm-password').value;

    if (!currentPassword || !newPassword || !confirmPassword) {
        alert('All password fields are required.');
        return;
    }

    if (newPassword !== confirmPassword) {
        alert('New passwords do not match.');
        return;
    }

    if (newPassword.length < 8) {
        alert('New password must be at least 8 characters long.');
        return;
    }

    const token = localStorage.getItem(TOKEN_KEY) || sessionStorage.getItem(TOKEN_KEY);
    const formData = new URLSearchParams();
    formData.append('current_password', currentPassword);
    formData.append('new_password', newPassword);

    try {
        const res = await fetch(`${apiRoot()}/auth/password`, {
            method: 'PUT',
            headers: {
                'Authorization': `Bearer ${authToken || token}`,
                'Content-Type': 'application/x-www-form-urlencoded'
            },
            body: formData.toString()
        });

        if (!res.ok) {
            const err = await res.json().catch(() => ({}));
            throw new Error(err.detail || 'Failed to update password');
        }

        alert('Password updated successfully!');
        
        // Clear fields
        document.getElementById('profile-current-password').value = '';
        document.getElementById('profile-new-password').value = '';
        document.getElementById('profile-confirm-password').value = '';

    } catch (e) {
        console.error('Password update error:', e);
        alert(e.message);
    }
}

// --- OGC Compliance ---

function _ogcBadgeColor(name) {
    const map = {
        'OGC API Features': ['bg-blue-500/20', 'text-blue-400', 'border-blue-500/30'],
        'STAC API': ['bg-emerald-500/20', 'text-emerald-400', 'border-emerald-500/30'],
        'OGC API Processes': ['bg-purple-500/20', 'text-purple-400', 'border-purple-500/30'],
        'OGC API Records': ['bg-yellow-500/20', 'text-yellow-400', 'border-yellow-500/30'],
        'OGC API Tiles': ['bg-cyan-500/20', 'text-cyan-400', 'border-cyan-500/30'],
        'OGC API Maps': ['bg-orange-500/20', 'text-orange-400', 'border-orange-500/30'],
        'OGC Dimensions': ['bg-pink-500/20', 'text-pink-400', 'border-pink-500/30'],
        'OGC API Styles': ['bg-indigo-500/20', 'text-indigo-400', 'border-indigo-500/30'],
    };
    return map[name] || ['bg-slate-500/20', 'text-slate-400', 'border-slate-500/30'];
}

function _createBadge(text, colorClasses) {
    const span = document.createElement('span');
    span.className = `px-2 py-0.5 rounded-full text-[10px] font-bold uppercase tracking-wider border ${colorClasses.join(' ')}`;
    span.textContent = text;
    return span;
}

async function loadOgcCompliance() {
    try {
        const resp = await fetch(`${_SCRIPT_ROOT}/web/dashboard/ogc-compliance`);
        if (!resp.ok) return;
        const data = await resp.json();

        // Populate compact badges in Platform Capabilities card
        const badgesEl = document.getElementById('ogc-compliance-badges');
        if (badgesEl && data.standards) {
            badgesEl.replaceChildren();
            for (const s of data.standards) {
                badgesEl.appendChild(_createBadge(`${s.name} (${s.implemented})`, _ogcBadgeColor(s.name)));
            }
        }

        const countEl = document.getElementById('ogc-conformance-count');
        if (countEl) {
            countEl.textContent = `${data.total_conformance_classes} conformance classes registered across ${data.standards.length} standard families`;
        }

        // Populate detail panel
        const detailEl = document.getElementById('ogc-compliance-detail');
        if (detailEl) detailEl.classList.remove('hidden');

        const tsEl = document.getElementById('ogc-report-timestamp');
        if (tsEl && data.timestamp) {
            tsEl.textContent = 'Generated: ' + new Date(data.timestamp).toLocaleString();
        }

        const gridEl = document.getElementById('ogc-standards-grid');
        if (gridEl && data.standards) {
            gridEl.replaceChildren();
            for (const s of data.standards) {
                const colors = _ogcBadgeColor(s.name);
                const card = document.createElement('div');
                card.className = `glass-panel rounded-lg p-4 border-l-4 ${colors[0].replace('/20', '/40')}`;

                const header = document.createElement('div');
                header.className = 'flex items-center justify-between mb-2';
                const title = document.createElement('h4');
                title.className = 'text-sm font-bold text-white';
                title.textContent = s.name;
                const badge = document.createElement('span');
                badge.className = `px-2 py-0.5 rounded text-[10px] font-bold border ${colors.join(' ')}`;
                badge.textContent = s.implemented;
                header.appendChild(title);
                header.appendChild(badge);
                card.appendChild(header);

                const ul = document.createElement('ul');
                ul.className = 'space-y-1';
                for (const uri of s.uris) {
                    const li = document.createElement('li');
                    li.className = 'text-[10px] text-slate-500 truncate';
                    li.title = uri;
                    li.textContent = uri.replace(/^https?:\/\/[^/]+\/spec\//, '').replace(/\/conf\//, ' / ');
                    ul.appendChild(li);
                }
                card.appendChild(ul);
                gridEl.appendChild(card);
            }
        }

        const notImplEl = document.getElementById('ogc-not-impl-list');
        if (notImplEl && data.not_implemented) {
            notImplEl.replaceChildren();
            for (const name of data.not_implemented) {
                const span = document.createElement('span');
                span.className = 'px-2 py-0.5 rounded-full text-[10px] font-medium text-slate-600 border border-slate-700';
                span.textContent = name;
                notImplEl.appendChild(span);
            }
        }
    } catch (e) {
        console.warn('Failed to load OGC compliance data:', e);
    }
}