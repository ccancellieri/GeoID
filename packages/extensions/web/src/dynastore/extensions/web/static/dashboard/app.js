// Dashboard application logic.
//
// Operator-facing surface: stats, logs, tasks. Tabs (.tab-btn / .tab-panel)
// + context selector + manual / 30 s auto refresh. All server-derived
// strings are rendered via textContent / createElement — never innerHTML —
// since logs and task names carry untrusted text.

import { mountContextBar } from "../static/common/context-bar.js";
import { apiBase } from "../static/common/url.js";
import { authHeader as _authHeader } from "../static/common/api.js";

const app = {
    state: {
        activeTab: 'overview',
        catalogId: null,
        collectionId: null,
        // Cached full task list for client-side pill filtering (defect C).
        _allTasks: [],
        _activePillFilter: 'active',
    },

    // --- Initialization ---
    init() {
        const container = document.getElementById('context-selector-container');
        if (container) {
            this.contextHandle = mountContextBar(container, {
                mode: 'select',
                enableVirtualCollections: false,
                onChange: ({ catalogId, collectionId }) => {
                    this.state.catalogId = catalogId;
                    this.state.collectionId = collectionId;
                    this.refreshAll();
                },
            });
        }

        this.bindEvents();
        this.refreshAll();
        // Auto-refresh every 30 s.
        setInterval(() => this.refreshAll(), 30000);
    },

    bindEvents() {
        // Tab switching (atlas .tab-btn / .tab-panel idiom).
        document.querySelectorAll('.tab-btn').forEach((el) => {
            el.addEventListener('click', (e) => {
                const target = e.currentTarget.dataset.tab;
                if (target) {
                    e.preventDefault();
                    this.switchTab(target);
                }
            });
        });

        const refreshBtn = document.getElementById('refresh-btn');
        if (refreshBtn) {
            refreshBtn.addEventListener('click', () => this.refreshAll());
        }

        const logsFilterBtn = document.getElementById('logs-filter-btn');
        if (logsFilterBtn) {
            logsFilterBtn.addEventListener('click', () => this.loadLogs());
        }

        // Task-filter pill toolbar — client-side filter against cached tasks.
        document.querySelectorAll('.tasks-toolbar .pill').forEach((el) => {
            el.addEventListener('click', (e) => {
                document.querySelectorAll('.tasks-toolbar .pill').forEach(
                    (p) => p.classList.remove('active')
                );
                e.currentTarget.classList.add('active');
                const filter = e.currentTarget.dataset.filter || 'active';
                this.state._activePillFilter = filter;
                this._renderTasks(this.state._allTasks);
            });
        });
    },

    switchTab(tabId) {
        document.querySelectorAll('.tab-btn').forEach((el) => {
            const isActive = el.dataset.tab === tabId;
            el.classList.toggle('active', isActive);
            el.setAttribute('aria-selected', isActive ? 'true' : 'false');
        });

        document.querySelectorAll('.tab-panel').forEach((el) => {
            const isActive = el.id === `tab-${tabId}`;
            el.classList.toggle('active', isActive);
            if (isActive) { el.removeAttribute('hidden'); }
            else { el.setAttribute('hidden', ''); }
        });

        const titleEl = document.getElementById('page-title');
        if (titleEl) {
            const titles = {
                overview: 'System overview',
                logs: 'Logs explorer',
                tasks: 'Task monitor',
            };
            titleEl.textContent = titles[tabId] || tabId;
        }

        this.state.activeTab = tabId;
        this.refreshActiveView();
    },

    refreshAll() {
        const now = new Date().toLocaleTimeString();
        const updatedEl = document.getElementById('last-updated');
        if (updatedEl) { updatedEl.textContent = `Updated: ${now}`; }

        if (this.state.activeTab === 'overview') { this.loadOverview(); }
        if (this.state.activeTab === 'logs') { this.loadLogs(); }
        if (this.state.activeTab === 'tasks') { this.loadTasks(); }
    },

    refreshActiveView() {
        this.refreshAll();
    },

    // --- Data fetching ---

    async loadOverview() {
        try {
            const { catalogId, collectionId } = this.state;
            // Scope comes from the context-bar picker, not the page URL. Build an
            // absolute, proxy-prefix-aware URL to the picked catalog's stats so the
            // result does not depend on where the shell HTML is served from.
            const url = this._statsUrl(catalogId, collectionId);

            const res = await fetch(url, {
                credentials: "same-origin",
                headers: { ..._authHeader() },
            });
            if (!res.ok) { throw new Error(`${res.status} ${url}`); }
            const data = await res.json();

            const setText = (id, value) => {
                const el = document.getElementById(id);
                if (el) { el.textContent = value; }
            };

            // StatsSummary fields: total_requests, average_latency_ms,
            // unique_principals, status_code_distribution.
            setText(
                'stat-requests',
                data.total_requests != null
                    ? data.total_requests.toLocaleString()
                    : '—'
            );

            setText(
                'stat-latency',
                data.average_latency_ms != null
                    ? `${Math.round(data.average_latency_ms)} ms`
                    : '—'
            );

            setText(
                'stat-principals',
                data.unique_principals != null
                    ? data.unique_principals.toLocaleString()
                    : '—'
            );

            // Success rate from status_code_distribution: sum of 2xx+3xx / total.
            const dist = data.status_code_distribution || {};
            const total = Object.values(dist).reduce((s, n) => s + n, 0);
            const successEl = document.getElementById('stat-success-rate');
            if (successEl) {
                if (total > 0) {
                    const success = Object.entries(dist)
                        .filter(([code]) => code.startsWith('2') || code.startsWith('3'))
                        .reduce((s, [, n]) => s + n, 0);
                    successEl.textContent = `${Math.round((success / total) * 100)}%`;
                } else {
                    successEl.textContent = '—';
                }
            }
        } catch (e) {
            console.error('Failed to load stats', e);
        }
    },

    // Stats endpoint scoped to the picked catalog (and collection, if any).
    // With no catalog selected, fall back to the platform-tier summary.
    _statsUrl(catalogId, collectionId) {
        const prefix = apiBase();
        if (!catalogId) { return `${prefix}/web/dashboard/stats`; }
        const base = `${prefix}/web/dashboard/catalogs/${encodeURIComponent(catalogId)}`;
        return collectionId
            ? `${base}/collections/${encodeURIComponent(collectionId)}/stats`
            : `${base}/stats`;
    },

    // Tasks endpoint scoped to the picked catalog; platform-tier when unset.
    _tasksUrl(catalogId) {
        const prefix = apiBase();
        return catalogId
            ? `${prefix}/web/dashboard/catalogs/${encodeURIComponent(catalogId)}/tasks`
            : `${prefix}/web/dashboard/tasks`;
    },

    // Proxy-prefix-aware absolute URL to the canonical logs API, scoped to the
    // picked catalog/collection.
    _logsUrl(catalogId, collectionId) {
        const prefix = apiBase();
        if (collectionId) {
            return `${prefix}/logs/catalogs/${encodeURIComponent(catalogId)}/collections/${encodeURIComponent(collectionId)}/logs?limit=20`;
        }
        return `${prefix}/logs/catalogs/${encodeURIComponent(catalogId)}/logs?limit=20`;
    },

    async loadLogs() {
        try {
            const { catalogId, collectionId } = this.state;
            // /web/dashboard/catalogs/{cat}/logs was deleted; fetch from the
            // canonical logs extension surface instead (defect A fix).
            const url = catalogId
                ? this._logsUrl(catalogId, collectionId)
                : null;

            if (!url) { return; }

            const res = await fetch(url, {
                credentials: "same-origin",
                headers: { ..._authHeader() },
            });
            if (!res.ok) { throw new Error(`${res.status} ${url}`); }
            // Response is LogsListResponse: {logs: [...], kibana_dashboard_url?, total?}
            const logs = (await res.json()).logs || [];

            const tbody = document.querySelector('#logs-table tbody');
            if (!tbody) { return; }

            // Drop existing children safely (no innerHTML).
            while (tbody.firstChild) { tbody.removeChild(tbody.firstChild); }

            if (!Array.isArray(logs) || logs.length === 0) {
                const tr = document.createElement('tr');
                tr.className = 'empty-row';
                const td = document.createElement('td');
                td.colSpan = 4;
                td.textContent = 'No logs found.';
                tr.appendChild(td);
                tbody.appendChild(tr);
                return;
            }

            const queryEl = document.getElementById('logs-query');
            const levelEl = document.getElementById('logs-level');
            const queryFilter = (queryEl?.value || '').toLowerCase().trim();
            const levelFilter = levelEl?.value || 'ALL';

            const visible = logs.filter((log) => {
                if (levelFilter !== 'ALL' && log.level !== levelFilter) {
                    return false;
                }
                if (queryFilter && !(log.message || '').toLowerCase().includes(queryFilter)) {
                    return false;
                }
                return true;
            });

            if (visible.length === 0) {
                const tr = document.createElement('tr');
                tr.className = 'empty-row';
                const td = document.createElement('td');
                td.colSpan = 4;
                td.textContent = 'No logs match the current filter.';
                tr.appendChild(td);
                tbody.appendChild(tr);
                return;
            }

            visible.forEach((log) => {
                const tr = document.createElement('tr');

                const tsRaw = log.timestamp || log.created_at;
                const tsCell = document.createElement('td');
                tsCell.textContent = tsRaw
                    ? new Date(tsRaw).toLocaleString()
                    : 'Just now';
                tr.appendChild(tsCell);

                const levelCell = document.createElement('td');
                const span = document.createElement('span');
                span.className = `log-level level-${(log.level || 'info').toLowerCase()}`;
                span.textContent = log.level || 'INFO';
                levelCell.appendChild(span);
                tr.appendChild(levelCell);

                const svcCell = document.createElement('td');
                svcCell.textContent = log.service || 'system';
                tr.appendChild(svcCell);

                const msgCell = document.createElement('td');
                msgCell.textContent = log.message || '';
                tr.appendChild(msgCell);

                tbody.appendChild(tr);
            });
        } catch (e) {
            console.error('Failed to load logs', e);
        }
    },

    async loadTasks() {
        try {
            const res = await fetch(this._tasksUrl(this.state.catalogId), {
                credentials: "same-origin",
                headers: { ..._authHeader() },
            });
            if (!res.ok) { throw new Error(`${res.status} ${this._tasksUrl(this.state.catalogId)}`); }
            const tasks = await res.json();
            this.state._allTasks = Array.isArray(tasks) ? tasks : [];
            this._renderTasks(this.state._allTasks);
        } catch (e) {
            console.error('Failed to load tasks', e);
        }
    },

    // Render task cards filtered by the active pill (defect C fix).
    // filter values match task.status lowercased; 'active' matches
    // 'active', 'running', 'in_progress' as synonyms.
    _renderTasks(allTasks) {
        const grid = document.getElementById('tasks-grid');
        if (!grid) { return; }

        while (grid.firstChild) { grid.removeChild(grid.firstChild); }

        const filter = this.state._activePillFilter || 'active';
        const filtered = allTasks.filter((task) => {
            const s = (task.status || '').toLowerCase();
            if (filter === 'active') {
                return s === 'active' || s === 'running' || s === 'in_progress';
            }
            return s === filter;
        });

        if (filtered.length === 0) {
            const empty = document.createElement('p');
            empty.className = 'empty-cell';
            empty.textContent = allTasks.length === 0
                ? 'No background tasks.'
                : `No ${filter} tasks.`;
            grid.appendChild(empty);
            return;
        }

        filtered.forEach((task) => {
            const card = document.createElement('div');
            card.className = 'task-card';

            const header = document.createElement('div');
            header.className = 'task-header';

            const title = document.createElement('div');
            title.className = 'task-title';
            title.textContent = task.name || 'task';
            header.appendChild(title);

            const status = document.createElement('span');
            const statusLabel = (task.status || 'unknown').toUpperCase();
            const statusKind = (() => {
                const s = (task.status || '').toLowerCase();
                if (s === 'failed' || s === 'error') { return 'effect-DENY'; }
                if (s === 'completed' || s === 'success') { return 'effect-ALLOW'; }
                return '';
            })();
            status.className = `chip ${statusKind}`;
            status.textContent = statusLabel;
            header.appendChild(status);

            card.appendChild(header);

            const idLine = document.createElement('div');
            idLine.className = 'task-id';
            idLine.textContent = `ID ${task.id || '—'}`;
            card.appendChild(idLine);

            const progress = Math.max(0, Math.min(100, Number(task.progress) || 0));
            const bar = document.createElement('div');
            bar.className = 'progress-bar';
            const fill = document.createElement('div');
            fill.className = 'progress-fill';
            fill.style.width = `${progress}%`;
            bar.appendChild(fill);
            card.appendChild(bar);

            const pct = document.createElement('div');
            pct.className = 'task-progress-pct';
            pct.textContent = `${progress}%`;
            card.appendChild(pct);

            grid.appendChild(card);
        });
    },
};

// Start
document.addEventListener('DOMContentLoaded', () => app.init());
