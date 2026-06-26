/**
 * Unified App Shell Component
 * 
 * Provides consistent navigation across all DynaStore web applications.
 * Features:
 * - Persistent sidebar with navigation sections
 * - Global search (Cmd+K)
 * - Quick access (recent items, bookmarks)
 * - Keyboard shortcuts
 * - Accessibility (WCAG 2.1 AA)
 * 
 * Usage:
 * <ds-app-shell>
 *   <ds-sidebar slot="sidebar"></ds-sidebar>
 *   <main slot="content">
 *     <!-- Page-specific content -->
 *   </main>
 * </ds-app-shell>
 */

(function() {
    'use strict';

    // === State Management ===
    const state = {
        sidebar: {
            expanded: localStorage.getItem('ds-sidebar-expanded') !== 'false',
            activeSection: localStorage.getItem('ds-sidebar-active-section') || 'platform'
        },
        search: {
            query: '',
            recent: JSON.parse(localStorage.getItem('ds-search-recent') || '[]'),
            isOpen: false
        },
        bookmarks: JSON.parse(localStorage.getItem('ds-bookmarks') || '[]'),
        recentItems: JSON.parse(localStorage.getItem('ds-recent-items') || '[]')
    };

    function persistState() {
        localStorage.setItem('ds-sidebar-expanded', state.sidebar.expanded);
        localStorage.setItem('ds-sidebar-active-section', state.sidebar.activeSection);
        localStorage.setItem('ds-search-recent', JSON.stringify(state.search.recent.slice(0, 10)));
        localStorage.setItem('ds-bookmarks', JSON.stringify(state.bookmarks));
        localStorage.setItem('ds-recent-items', JSON.stringify(state.recentItems.slice(0, 10)));
    }

    // === Sidebar Component ===
    class DsSidebar extends HTMLElement {
        connectedCallback() {
            this.render();
            this.attachEventListeners();
        }

        render() {
            const expanded = state.sidebar.expanded;
            this.innerHTML = `
                <aside class="ds-sidebar ${expanded ? 'expanded' : 'collapsed'}" 
                       role="navigation" 
                       aria-label="Main navigation">
                    <div class="ds-sidebar-header">
                        ${expanded ? `
                            <a href="/" class="ds-logo">
                                <img src="../static/dynastore.png" alt="DynaStore" />
                            </a>
                        ` : `
                            <button class="ds-sidebar-toggle" 
                                    aria-label="Expand sidebar"
                                    title="Expand sidebar">
                                <i class="fa-solid fa-bars"></i>
                            </button>
                        `}
                        ${expanded ? `
                            <button class="ds-sidebar-toggle" 
                                    aria-label="Collapse sidebar"
                                    title="Collapse sidebar">
                                <i class="fa-solid fa-chevron-left"></i>
                            </button>
                        ` : ''}
                    </div>
                    
                    ${expanded ? `
                        <div class="ds-search-trigger" 
                             role="button"
                             tabindex="0"
                             aria-label="Open global search (Cmd+K)">
                            <i class="fa-solid fa-search"></i>
                            <span>Search...</span>
                            <kbd>⌘K</kbd>
                        </div>
                    ` : ''}
                    
                    <nav class="ds-nav-sections">
                        ${this.renderSections()}
                    </nav>
                    
                    ${expanded && state.recentItems.length > 0 ? `
                        <div class="ds-recent-items">
                            <h4>Recent</h4>
                            <ul>
                                ${state.recentItems.map(item => `
                                    <li>
                                        <a href="${item.url}" title="${item.title}">
                                            <i class="fa-solid fa-clock"></i>
                                            ${this.truncate(item.title, 20)}
                                        </a>
                                    </li>
                                `).join('')}
                            </ul>
                        </div>
                    ` : ''}
                </aside>
            `;
        }

        renderSections() {
            const sections = [
                {
                    id: 'platform',
                    title: 'Platform',
                    items: [
                        { id: 'home', href: '/', icon: 'fa-home', label: 'Home' },
                        { id: 'stats', href: '/web/stats', icon: 'fa-chart-line', label: 'Stats' },
                        { id: 'dashboard', href: '/web/dashboard/', icon: 'fa-tachometer-alt', label: 'Dashboard' }
                    ]
                },
                {
                    id: 'catalogs',
                    title: 'Catalogs',
                    items: [], // Will be dynamically populated
                    dynamic: true
                },
                {
                    id: 'admin',
                    title: 'Admin',
                    items: [
                        { id: 'configs', href: '/web/configs', icon: 'fa-cog', label: 'Configuration' },
                        { id: 'access', href: '/web/admin/access-bindings', icon: 'fa-lock', label: 'Access' }
                    ]
                },
                {
                    id: 'help',
                    title: 'Help',
                    items: [
                        { id: 'docs', href: '/web/docs', icon: 'fa-book', label: 'Documentation' }
                    ]
                }
            ];

            return sections.map(section => `
                <div class="ds-nav-section ${state.sidebar.activeSection === section.id ? 'active' : ''}" 
                     data-section="${section.id}">
                    ${state.sidebar.expanded ? `
                        <button class="ds-section-toggle"
                                aria-expanded="${state.sidebar.activeSection === section.id}"
                                aria-controls="section-${section.id}">
                            <i class="fa-solid ${this.getSectionIcon(section.id)}"></i>
                            <span>${section.title}</span>
                            <i class="fa-solid fa-chevron-down ds-chevron"></i>
                        </button>
                    ` : `
                        <button class="ds-section-toggle ds-collapsed"
                                title="${section.title}"
                                aria-label="${section.title}">
                            <i class="fa-solid ${this.getSectionIcon(section.id)}"></i>
                        </button>
                    `}
                    <ul class="ds-section-items" 
                        id="section-${section.id}"
                        role="menu">
                        ${section.items.map(item => this.renderNavItem(item)).join('')}
                        ${section.dynamic ? '<li class="ds-loading">Loading...</li>' : ''}
                    </ul>
                </div>
            `).join('');
        }

        renderNavItem(item) {
            const isActive = window.location.pathname === item.href;
            return `
                <li role="none">
                    <a href="${item.href}"
                       class="ds-nav-item ${isActive ? 'active' : ''}"
                       role="menuitem"
                       aria-current="${isActive ? 'page' : 'false'}">
                        ${item.icon ? `<i class="fa-solid ${item.icon}"></i>` : ''}
                        <span>${item.label}</span>
                    </a>
                </li>
            `;
        }

        getSectionIcon(sectionId) {
            const icons = {
                'platform': 'fa-globe',
                'catalogs': 'fa-database',
                'admin': 'fa-tools',
                'help': 'fa-question-circle'
            };
            return icons[sectionId] || 'fa-folder';
        }

        truncate(text, maxLength) {
            return text.length > maxLength ? text.substring(0, maxLength) + '...' : text;
        }

        attachEventListeners() {
            // Toggle sidebar
            this.querySelectorAll('.ds-sidebar-toggle').forEach(btn => {
                btn.addEventListener('click', (e) => {
                    e.stopPropagation();
                    state.sidebar.expanded = !state.sidebar.expanded;
                    persistState();
                    this.render();
                    this.attachEventListeners();
                });
            });

            // Toggle sections
            this.querySelectorAll('.ds-section-toggle').forEach(btn => {
                btn.addEventListener('click', (e) => {
                    e.stopPropagation();
                    const section = btn.closest('.ds-nav-section');
                    const sectionId = section.dataset.section;
                    state.sidebar.activeSection = 
                        state.sidebar.activeSection === sectionId ? '' : sectionId;
                    persistState();
                    this.render();
                    this.attachEventListeners();
                });
            });

            // Open global search
            const searchTrigger = this.querySelector('.ds-search-trigger');
            if (searchTrigger) {
                searchTrigger.addEventListener('click', () => {
                    const shell = this.closest('ds-app-shell');
                    if (shell) shell.openSearch();
                });
                searchTrigger.addEventListener('keydown', (e) => {
                    if (e.key === 'Enter' || e.key === ' ') {
                        e.preventDefault();
                        const shell = this.closest('ds-app-shell');
                        if (shell) shell.openSearch();
                    }
                });
            }

            // Track navigation for recent items
            this.querySelectorAll('.ds-nav-item').forEach(link => {
                link.addEventListener('click', (e) => {
                    const href = e.currentTarget.getAttribute('href');
                    const label = e.currentTarget.querySelector('span')?.textContent || href;
                    this.addToRecentItems(href, label);
                });
            });
        }

        addToRecentItems(url, title) {
            const item = { url, title, timestamp: Date.now() };
            state.recentItems = [
                item,
                ...state.recentItems.filter(i => i.url !== url)
            ].slice(0, 10);
            persistState();
        }
    }

    // === Search Overlay Component ===
    class DsSearchOverlay extends HTMLElement {
        connectedCallback() {
            this.render();
            this.attachEventListeners();
        }

        render() {
            this.innerHTML = `
                <div class="ds-search-overlay" hidden>
                    <div class="ds-search-backdrop"></div>
                    <div class="ds-search-modal" role="dialog" aria-modal="true" aria-label="Global search">
                        <div class="ds-search-header">
                            <i class="fa-solid fa-search"></i>
                            <input type="search"
                                   class="ds-search-input"
                                   placeholder="Search catalogs, collections, docs..."
                                   aria-label="Search"
                                   autocomplete="off"
                                   autocapitalize="off"
                                   spellcheck="false" />
                            <kbd>ESC</kbd>
                        </div>
                        <div class="ds-search-results">
                            <div class="ds-search-recent">
                                <h4>Recent searches</h4>
                                <ul class="ds-recent-searches"></ul>
                            </div>
                            <div class="ds-search-live-results"></div>
                        </div>
                    </div>
                </div>
            `;
        }

        attachEventListeners() {
            const overlay = this.querySelector('.ds-search-overlay');
            const input = this.querySelector('.ds-search-input');
            const backdrop = this.querySelector('.ds-search-backdrop');

            // Close on backdrop click
            backdrop?.addEventListener('click', () => this.close());

            // Close on ESC
            document.addEventListener('keydown', (e) => {
                if (e.key === 'Escape' && !overlay.hidden) {
                    this.close();
                }
            });

            // Search input
            let debounceTimer;
            input?.addEventListener('input', (e) => {
                clearTimeout(debounceTimer);
                debounceTimer = setTimeout(() => {
                    this.performSearch(e.target.value);
                }, 200);
            });

            // Recent searches
            this.renderRecentSearches();
        }

        renderRecentSearches() {
            const recentList = this.querySelector('.ds-recent-searches');
            if (!recentList) return;

            recentList.innerHTML = state.search.recent.length > 0
                ? state.search.recent.map(query => `
                    <li>
                        <button class="ds-recent-search" data-query="${query}">
                            <i class="fa-solid fa-history"></i>
                            ${query}
                        </button>
                    </li>
                `).join('')
                : '<li class="ds-empty">No recent searches</li>';

            recentList.querySelectorAll('.ds-recent-search').forEach(btn => {
                btn.addEventListener('click', (e) => {
                    const query = e.currentTarget.dataset.query;
                    const input = this.querySelector('.ds-search-input');
                    if (input) {
                        input.value = query;
                        this.performSearch(query);
                    }
                });
            });
        }

        async performSearch(query) {
            if (!query || query.length < 2) {
                this.renderRecentSearches();
                return;
            }

            // Add to recent searches
            state.search.recent = [
                query,
                ...state.search.recent.filter(q => q !== query)
            ].slice(0, 10);
            persistState();

            const resultsContainer = this.querySelector('.ds-search-live-results');
            if (resultsContainer) {
                resultsContainer.innerHTML = `
                    <div class="ds-searching">
                        <i class="fa-solid fa-spinner fa-spin"></i>
                        Searching...
                    </div>
                `;

                try {
                    const response = await fetch(
                        `/web/search?q=${encodeURIComponent(query)}&limit=5`
                    );
                    
                    if (!response.ok) {
                        throw new Error(`Search failed: ${response.status}`);
                    }
                    
                    const data = await response.json();
                    this.renderSearchResults(data, query);
                } catch (error) {
                    console.error('Search error:', error);
                    resultsContainer.innerHTML = `
                        <div class="ds-search-error">
                            <i class="fa-solid fa-exclamation-triangle"></i>
                            <p>Search failed. Please try again.</p>
                        </div>
                    `;
                }
            }
        }

        renderSearchResults(data, query) {
            const resultsContainer = this.querySelector('.ds-search-live-results');
            if (!resultsContainer) return;

            const { results, total, query_time_ms } = data;
            
            if (total === 0) {
                resultsContainer.innerHTML = `
                    <div class="ds-search-empty">
                        <i class="fa-solid fa-search"></i>
                        <p>No results found for "${query}"</p>
                        <p class="ds-muted">Try different keywords or check your spelling</p>
                    </div>
                `;
                return;
            }

            let html = `<div class="ds-search-stats">
                ${total} results (${query_time_ms}ms)
            </div>`;

            // Catalogs
            if (results.catalogs && results.catalogs.length > 0) {
                html += `
                    <div class="ds-result-group">
                        <h4><i class="fa-solid fa-database"></i> Catalogs</h4>
                        <ul>
                            ${results.catalogs.map(c => `
                                <li>
                                    <a href="${c.url}" class="ds-result-item">
                                        <div class="ds-result-title">${this.highlightMatch(c.title, query)}</div>
                                        ${c.description ? `<div class="ds-result-desc">${this.truncate(c.description, 80)}</div>` : ''}
                                    </a>
                                </li>
                            `).join('')}
                        </ul>
                    </div>
                `;
            }

            // Collections
            if (results.collections && results.collections.length > 0) {
                html += `
                    <div class="ds-result-group">
                        <h4><i class="fa-solid fa-layer-group"></i> Collections</h4>
                        <ul>
                            ${results.collections.map(c => `
                                <li>
                                    <a href="${c.url}" class="ds-result-item">
                                        <div class="ds-result-title">${this.highlightMatch(c.title, query)}</div>
                                        <div class="ds-result-meta">${c.catalog_id}</div>
                                    </a>
                                </li>
                            `).join('')}
                        </ul>
                    </div>
                `;
            }

            // Items
            if (results.items && results.items.length > 0) {
                html += `
                    <div class="ds-result-group">
                        <h4><i class="fa-solid fa-cube"></i> Items</h4>
                        <ul>
                            ${results.items.map(i => `
                                <li>
                                    <a href="${i.url}" class="ds-result-item">
                                        <div class="ds-result-title">${this.highlightMatch(i.title, query)}</div>
                                    </a>
                                </li>
                            `).join('')}
                        </ul>
                    </div>
                `;
            }

            // Docs
            if (results.docs && results.docs.length > 0) {
                html += `
                    <div class="ds-result-group">
                        <h4><i class="fa-solid fa-book"></i> Documentation</h4>
                        <ul>
                            ${results.docs.map(d => `
                                <li>
                                    <a href="${d.url}" class="ds-result-item">
                                        <div class="ds-result-title">${this.highlightMatch(d.title, query)}</div>
                                        ${d.category ? `<div class="ds-result-meta">${d.category}</div>` : ''}
                                    </a>
                                </li>
                            `).join('')}
                        </ul>
                    </div>
                `;
            }

            // Tasks
            if (results.tasks && results.tasks.length > 0) {
                html += `
                    <div class="ds-result-group">
                        <h4><i class="fa-solid fa-tasks"></i> Tasks</h4>
                        <ul>
                            ${results.tasks.map(t => `
                                <li>
                                    <a href="${t.url}" class="ds-result-item">
                                        <div class="ds-result-title">${this.highlightMatch(t.title, query)}</div>
                                    </a>
                                </li>
                            `).join('')}
                        </ul>
                    </div>
                `;
            }

            resultsContainer.innerHTML = html;

            // Add click handlers to track recent items
            resultsContainer.querySelectorAll('.ds-result-item').forEach(link => {
                link.addEventListener('click', (e) => {
                    const href = e.currentTarget.getAttribute('href');
                    const title = e.currentTarget.querySelector('.ds-result-title')?.textContent || href;
                    this.addToRecentItems(href, title);
                });
            });
        }

        highlightMatch(text, query) {
            const regex = new RegExp(`(${query})`, 'gi');
            return text.replace(regex, '<mark>$1</mark>');
        }

        open() {
            const overlay = this.querySelector('.ds-search-overlay');
            const input = this.querySelector('.ds-search-input');
            if (overlay) {
                overlay.hidden = false;
                input?.focus();
            }
        }

        close() {
            const overlay = this.querySelector('.ds-search-overlay');
            const input = this.querySelector('.ds-search-input');
            if (overlay) {
                overlay.hidden = true;
                if (input) input.value = '';
            }
        }
    }

    // === App Shell Component ===
    class DsAppShell extends HTMLElement {
        connectedCallback() {
            this.render();
            this.setupKeyboardShortcuts();
        }

        render() {
            this.innerHTML = `
                <div class="ds-app-shell">
                    <slot name="sidebar"></slot>
                    <main class="ds-main-content" role="main">
                        <slot name="content"></slot>
                    </main>
                    <ds-search-overlay></ds-search-overlay>
                </div>
            `;
        }

        setupKeyboardShortcuts() {
            document.addEventListener('keydown', (e) => {
                // Cmd+K / Ctrl+K to open search
                if ((e.metaKey || e.ctrlKey) && e.key === 'k') {
                    e.preventDefault();
                    this.openSearch();
                }
            });
        }

        openSearch() {
            const searchOverlay = this.querySelector('ds-search-overlay');
            if (searchOverlay) searchOverlay.open();
        }
    }

    // Register custom elements
    if (typeof customElements !== 'undefined') {
        if (!customElements.get('ds-app-shell')) {
            customElements.define('ds-app-shell', DsAppShell);
        }
        if (!customElements.get('ds-sidebar')) {
            customElements.define('ds-sidebar', DsSidebar);
        }
        if (!customElements.get('ds-search-overlay')) {
            customElements.define('ds-search-overlay', DsSearchOverlay);
        }
    }
})();
