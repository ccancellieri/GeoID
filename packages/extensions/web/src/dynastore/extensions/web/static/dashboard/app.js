// Dashboard application logic.
//
// Operator-facing surface: stats, logs, tasks. Tabs (.tab-btn / .tab-panel)
// + context selector + manual / 30 s auto refresh. All server-derived
// strings are rendered via textContent / createElement — never innerHTML —
// since logs and task names carry untrusted text.
//
// Fixes for #2503:
// - Added AbortController for race condition prevention
// - Added error handling with UI error states
// - Added loading states during async operations
// - Added auto-refresh countdown timer
// - Improved task filtering with server-side pagination

import { mountContextBar } from "../static/common/context-bar.js";
import { apiBase } from "../static/common/url.js";
import { authHeader as _authHeader } from "../static/common/api.js";

const app = {
    state: {
        activeTab: 'overview',
        catalogId: null,
        collectionId: null,
        // Task filter state (now server-side, no caching)
        _activePillFilter: 'active',
        _taskLimit: 100,
    },
    
    // AbortController for preventing race conditions
    _abortController: null,
    
    // Auto-refresh state
    _refreshInterval: 30000, // 30 seconds
    _countdown: 30,
    _countdownInterval: null,

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
        this.startAutoRefresh();
        this.refreshAll();
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
            refreshBtn.addEventListener('click', () => {
                this.refreshAll();
                this._countdown = 30; // Reset countdown on manual refresh
            });
        }

        const logsFilterBtn = document.getElementById('logs-filter-btn');
        if (logsFilterBtn) {
            logsFilterBtn.addEventListener('click', () => this.loadLogs());
        }

        // Task-filter pill toolbar — now triggers server-side fetch
        document.querySelectorAll('.tasks-toolbar .pill').forEach((el) => {
            el.addEventListener('click', (e) => {
                document.querySelectorAll('.tasks-toolbar .pill').forEach(
                    (p) => p.classList.remove('active')
                );
                e.currentTarget.classList.add('active');
                const filter = e.currentTarget.dataset.filter || 'active';
                this.state._activePillFilter = filter;
                this.loadTasks(); // Fetch from server with new filter
            });
        });
    },
    
    startAutoRefresh() {
        // Clear existing interval if any
        if (this._countdownInterval) {
            clearInterval(this._countdownInterval);
        }
        
        this._countdown = 30;
        
        // Update countdown every second
        this._countdownInterval = setInterval(() => {
            this._countdown--;
            
            const countdownEl = document.getElementById('refresh-countdown');
            if (countdownEl) {
                countdownEl.textContent = this._countdown;
            }
            
            if (this._countdown <= 0) {
                this._countdown = 30;
                this.refreshAll();
            }
        }, 1000);
    },

    switchTab(tabId) {
        // Cancel any pending requests
        this.cancelPendingRequests();
        
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
    
    cancelPendingRequests() {
        if (this._abortController) {
            this._abortController.abort();
            this._abortController = null;
        }
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
    
    showLoading(containerId) {
        const container = document.getElementById(containerId);
        if (!container) return;
        
        // Clear existing content
        while (container.firstChild) {
            container.removeChild(container.firstChild);
        }
        
        // Show loading state
        const loading = document.createElement('div');
        loading.className = 'loading-state';
        
        const spinner = document.createElement('i');
        spinner.className = 'fa-solid fa-spinner fa-spin';
        loading.appendChild(spinner);
        
        const text = document.createElement('p');
        text.textContent = 'Loading...';
        loading.appendChild(text);
        
        container.appendChild(loading);
    }
    ,
    
    showError(containerId, message, retryCallback) {
        const container = document.getElementById(containerId);
        if (!container) return;
        
        // Clear existing content
        while (container.firstChild) {
            container.removeChild(container.firstChild);
        }
        
        // Show error state
        const error = document.createElement('div');
        error.className = 'error-state';
        
        const icon = document.createElement('i');
        icon.className = 'fa-solid fa-exclamation-triangle';
        error.appendChild(icon);
        
        const text = document.createElement('p');
        text.textContent = message;
        error.appendChild(text);
        
        if (retryCallback) {
            const retryBtn = document.createElement('button');
            retryBtn.className = 'btn btn-secondary btn-xs';
            retryBtn.textContent = 'Retry';
            retryBtn.addEventListener('click', retryCallback);
            error.appendChild(retryBtn);
        }
        
        container.appendChild(error);
    },

    // --- Data fetching ---

    async loadOverview() {
        // Cancel previous request
        this.cancelPendingRequests();
        this._abortController = new AbortController();
        
        try {
            const { catalogId, collectionId } = this.state;
            const url = this._statsUrl(catalogId, collectionId);

            const res = await fetch(url, {
                credentials: "same-origin",
                headers: { ..._authHeader() },
                signal: this._abortController.signal,
            });
            if (!res.ok) { throw new Error(`HTTP ${res.status}`); }
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
            if (e.name === 'AbortError') return; // Ignore cancelled requests
            console.error('Failed to load stats', e);
            // Show error in stats grid
            const statsGrid = document.querySelector('.stats-grid');
            if (statsGrid) {
                this.showError('stat-requests', 'Failed to load stats', () => this.loadOverview());
            }
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
    // Now supports server-side filtering via query params (#2503).
    _tasksUrl(catalogId, status = null, limit = 100) {
        const prefix = apiBase();
        let url = catalogId
            ? `${prefix}/web/dashboard/catalogs/${encodeURIComponent(catalogId)}/tasks`
            : `${prefix}/web/dashboard/tasks`;
        
        const params = new URLSearchParams();
        if (status && status !== 'all') {
            // Map UI filter to backend status values
            const statusMap = {
                'active': 'running,pending,in_progress',
                'completed': 'completed,success',
                'failed': 'failed,error'
            };
            params.set('status', statusMap[status] || status);
        }
        params.set('limit', limit.toString());
        
        return `${url}?${params.toString()}`;
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
        // Cancel previous request
        this.cancelPendingRequests();
        this._abortController = new AbortController();
        
        // Show loading state
        const tbody = document.querySelector('#logs-table tbody');
        if (!tbody) return;
        this.showLoading('logs-table-tbody');
        
        try {
            const { catalogId, collectionId } = this.state;
            const url = catalogId
                ? this._logsUrl(catalogId, collectionId)
                : null;

            if (!url) {
                this.showError('logs-table-tbody', 'Select a catalog to view logs');
                return;
            }

            const res = await fetch(url, {
                credentials: "same-origin",
                headers: { ..._authHeader() },
                signal: this._abortController.signal,
            });
            if (!res.ok) { throw new Error(`HTTP ${res.status}`); }
            // Response is LogsListResponse: {logs: [...], kibana_dashboard_url?, total?}
            const logs = (await res.json()).logs || [];

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
            if (e.name === 'AbortError') return; // Ignore cancelled requests
            console.error('Failed to load logs', e);
            this.showError('logs-table-tbody', 'Failed to load logs: ' + e.message, () => this.loadLogs());
        }
    },

    async loadTasks() {
        // Cancel previous request
        this.cancelPendingRequests();
        this._abortController = new AbortController();
        
        // Show loading state
        const grid = document.getElementById('tasks-grid');
        if (!grid) return;
        this.showLoading('tasks-grid');
        
        try {
            // Use server-side filtering instead of client-side caching
            const url = this._tasksUrl(
                this.state.catalogId,
                this.state._activePillFilter,
                this.state._taskLimit
            );
            
            const res = await fetch(url, {
                credentials: "same-origin",
                headers: { ..._authHeader() },
                signal: this._abortController.signal,
            });
            if (!res.ok) { throw new Error(`HTTP ${res.status}`); }
            
            // Backend returns filtered tasks
            let tasks = await res.json();
            tasks = Array.isArray(tasks) ? tasks : [];
            
            // Render tasks (no client-side filtering needed)
            this._renderTasks(tasks);
        } catch (e) {
            if (e.name === 'AbortError') return; // Ignore cancelled requests
            console.error('Failed to load tasks', e);
            this.showError('tasks-grid', 'Failed to load tasks: ' + e.message, () => this.loadTasks());
        }
    },

    // Render task cards (no filtering - already done server-side)
    _renderTasks(tasks) {
        const grid = document.getElementById('tasks-grid');
        if (!grid) { return; }

        while (grid.firstChild) { grid.removeChild(grid.firstChild); }

        if (tasks.length === 0) {
            const empty = document.createElement('p');
            empty.className = 'empty-cell';
            empty.textContent = `No ${this.state._activePillFilter} tasks.`;
            grid.appendChild(empty);
            return;
        }

        tasks.forEach((task) => {
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
