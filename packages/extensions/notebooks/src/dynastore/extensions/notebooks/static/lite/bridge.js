// JupyterLite Bridge Script
// Bridges communication between the host DynaStore application
// and the JupyterLite environment running in an iframe.
//
// Message types:
//   INIT_CONTEXT   — Auth token + catalog context injection
//   LOAD_NOTEBOOK  — Load a notebook into the current session

(function() {
    console.log("[Bridge] Initializing DynaStore Bridge...");

    // --- In-memory Contents API shim ---
    // JupyterLite's ServiceWorker is not active in this sub-path deployment
    // (scope /web/lite/ cannot intercept the absolute /api/contents/* calls
    // JupyterLab emits). Intercept fetch early, before JupyterLab loads, so
    // autosave/read succeed against an in-memory store. The host-side Save
    // button (`saveNotebookToDynaStore`) still persists to DynaStore.
    const CONTENTS_STORE = Object.create(null);
    const CONTENTS_RE = /\/api\/contents(\/|$)([^?#]*)/;

    // Synchronously seed from parent before JupyterLab boots — same-origin
    // iframe (sandbox allow-same-origin) so window.parent is reachable. The
    // host populates window.preloadedNotebooks before opening any notebook.
    try {
        const parentCache = window.parent && window.parent.preloadedNotebooks;
        if (parentCache && typeof parentCache === 'object') {
            const now = new Date().toISOString();
            const dirs = new Set();
            for (const path of Object.keys(parentCache)) {
                const nb = parentCache[path];
                if (!nb || !nb.cells) continue;
                CONTENTS_STORE[path] = {
                    name: path.split('/').pop(),
                    path: path,
                    type: 'notebook',
                    format: 'json',
                    content: nb,
                    writable: true,
                    created: now,
                    last_modified: now,
                    mimetype: null,
                    size: null,
                };
                const slash = path.lastIndexOf('/');
                if (slash > 0) dirs.add(path.slice(0, slash));
            }
            for (const d of dirs) {
                if (!CONTENTS_STORE[d]) {
                    CONTENTS_STORE[d] = {
                        name: d.split('/').pop(),
                        path: d,
                        type: 'directory',
                        format: 'json',
                        content: null,
                        writable: true,
                        created: now,
                        last_modified: now,
                    };
                }
            }
            console.log("[Bridge] Pre-seeded " + Object.keys(parentCache).length + " notebook(s) from parent cache.");
        }
    } catch (e) {
        console.warn("[Bridge] parent cache read failed:", e);
    }

    const origFetch = window.fetch.bind(window);
    window.fetch = function(input, init) {
        try {
            const url = typeof input === 'string' ? input : input.url;
            const method = ((init && init.method) || (typeof input !== 'string' && input.method) || 'GET').toUpperCase();
            const m = url && url.match(CONTENTS_RE);
            if (m) {
                const path = decodeURIComponent(m[2] || '').replace(/^\/+/, '');
                const now = new Date().toISOString();
                if (method === 'PUT' || method === 'POST' || method === 'PATCH') {
                    let body = init && init.body;
                    if (typeof body === 'string') { try { body = JSON.parse(body); } catch (e) {} }
                    body = body || {};
                    const entry = {
                        name: path.split('/').pop() || 'untitled',
                        path: path,
                        type: body.type || (path.endsWith('.ipynb') ? 'notebook' : 'file'),
                        format: body.format || 'json',
                        content: body.content !== undefined ? body.content : null,
                        writable: true,
                        created: CONTENTS_STORE[path]?.created || now,
                        last_modified: now,
                        mimetype: body.mimetype || null,
                        size: null,
                    };
                    CONTENTS_STORE[path] = entry;
                    return Promise.resolve(new Response(JSON.stringify(entry), {
                        status: 201, headers: { 'Content-Type': 'application/json' }
                    }));
                }
                if (method === 'GET') {
                    // JupyterLite drives request <dir>/all.json to enumerate
                    // bundled files. Synthesize one from CONTENTS_STORE so the
                    // file browser shows everything we've seeded.
                    if (path.endsWith('all.json') || path === 'all.json') {
                        const dirPath = path === 'all.json' ? '' : path.slice(0, -('all.json'.length)).replace(/\/+$/, '');
                        const prefix = dirPath ? dirPath + '/' : '';
                        const seenAll = Object.create(null);
                        const entries = [];
                        for (const k of Object.keys(CONTENTS_STORE)) {
                            if (!k.startsWith(prefix)) continue;
                            const rest = k.slice(prefix.length);
                            if (!rest) continue;
                            const slash = rest.indexOf('/');
                            const childKey = slash === -1 ? k : (prefix + rest.slice(0, slash));
                            if (seenAll[childKey]) continue;
                            seenAll[childKey] = 1;
                            const e = CONTENTS_STORE[childKey] || {
                                name: rest.slice(0, slash === -1 ? rest.length : slash),
                                path: childKey,
                                type: slash === -1 ? 'notebook' : 'directory',
                                format: 'json', writable: true, content: null,
                                created: now, last_modified: now,
                            };
                            entries.push(Object.assign({}, e, { content: null }));
                        }
                        return Promise.resolve(new Response(JSON.stringify(entries), {
                            status: 200, headers: { 'Content-Type': 'application/json' }
                        }));
                    }
                    const entry = CONTENTS_STORE[path];
                    if (entry && entry.type !== 'directory') {
                        return Promise.resolve(new Response(JSON.stringify(entry), {
                            status: 200, headers: { 'Content-Type': 'application/json' }
                        }));
                    }
                    // Directory listing: root ('') or any stored directory path.
                    // JupyterLab's file browser fetches GET /api/contents/<dir> expecting
                    // type=directory with `content` populated by direct children only.
                    const normDir = path.replace(/\/+$/, '');
                    const isRoot = normDir === '';
                    if (isRoot || (entry && entry.type === 'directory')) {
                        const prefix = isRoot ? '' : normDir + '/';
                        const seen = Object.create(null);
                        const items = [];
                        for (const k of Object.keys(CONTENTS_STORE)) {
                            if (!k.startsWith(prefix)) continue;
                            const rest = k.slice(prefix.length);
                            if (!rest || rest === '') continue;
                            const slash = rest.indexOf('/');
                            if (slash === -1) {
                                const e = CONTENTS_STORE[k];
                                if (seen[e.path]) continue;
                                seen[e.path] = 1;
                                items.push(Object.assign({}, e, { content: null }));
                            } else {
                                const childDir = prefix + rest.slice(0, slash);
                                if (seen[childDir]) continue;
                                seen[childDir] = 1;
                                const existing = CONTENTS_STORE[childDir];
                                items.push(existing
                                    ? Object.assign({}, existing, { content: null })
                                    : {
                                        name: rest.slice(0, slash),
                                        path: childDir,
                                        type: 'directory',
                                        format: 'json',
                                        content: null,
                                        writable: true,
                                        created: now,
                                        last_modified: now,
                                    });
                            }
                        }
                        return Promise.resolve(new Response(JSON.stringify({
                            name: isRoot ? '' : (normDir.split('/').pop() || ''),
                            path: normDir,
                            type: 'directory',
                            format: 'json',
                            content: items,
                            writable: true,
                            created: (entry && entry.created) || now,
                            last_modified: (entry && entry.last_modified) || now,
                        }), { status: 200, headers: { 'Content-Type': 'application/json' } }));
                    }
                    return Promise.resolve(new Response(JSON.stringify({ message: 'Not found' }), {
                        status: 404, headers: { 'Content-Type': 'application/json' }
                    }));
                }
                if (method === 'DELETE') {
                    delete CONTENTS_STORE[path];
                    return Promise.resolve(new Response('', { status: 204 }));
                }
            }
        } catch (e) {
            console.warn('[Bridge] contents shim error', e);
        }
        return origFetch(input, init);
    };
    window.DYNASTORE_CONTENTS_STORE = CONTENTS_STORE;

    // Poll for the JupyterLab application instance. The /notebooks/ and
    // /tree/ single-page builds expose it as `window.jupyterapp`; the /lab/
    // build does not expose any global, so we stick with the classic builds.
    function waitForJupyterApp(timeoutMs) {
        return new Promise(function(resolve) {
            var started = Date.now();
            (function tick() {
                var app = window.jupyterapp || window.jupyterlab;
                if (app && app.commands && app.serviceManager) return resolve(app);
                if (Date.now() - started > timeoutMs) return resolve(null);
                setTimeout(tick, 100);
            })();
        });
    }

    // Seed notebooks into JupyterLite's real contents manager (indexedDB-
    // backed LocalForageDrive). The fetch shim above is a fallback — the
    // native drives bypass fetch entirely, so we must call
    // serviceManager.contents.save() for files to appear in the file
    // browser and be openable via docmanager:open.
    async function seedNotebooksViaContentsManager(app) {
        const parentCache = window.parent && window.parent.preloadedNotebooks;
        if (!parentCache || typeof parentCache !== 'object') return 0;
        const paths = Object.keys(parentCache);
        let saved = 0;
        for (const path of paths) {
            const nb = parentCache[path];
            if (!nb || !nb.cells) continue;
            try {
                // Ensure parent directories exist — Lite's drive requires it.
                const parts = path.split('/');
                let dir = '';
                for (let i = 0; i < parts.length - 1; i++) {
                    dir = dir ? dir + '/' + parts[i] : parts[i];
                    try {
                        await app.serviceManager.contents.get(dir, { content: false });
                    } catch (e) {
                        try {
                            await app.serviceManager.contents.save(dir, {
                                type: 'directory',
                                format: 'json',
                                content: null,
                            });
                        } catch (e2) {}
                    }
                }
                await app.serviceManager.contents.save(path, {
                    type: 'notebook',
                    format: 'json',
                    content: nb,
                });
                saved++;
            } catch (e) {
                console.warn('[Bridge] contents.save failed for', path, e);
            }
        }
        console.log('[Bridge] Seeded', saved, 'of', paths.length, 'notebooks via contents manager.');
        return saved;
    }

    // Store context
    window.DYNASTORE_CONTEXT = {
        token: null,
        catalogCode: null,
        baseUrl: null,
        stacItems: null
    };

    // --- Kernel context injection ---
    // window.pyodideReadyPromise is never set by current JupyterLite (Pyodide runs
    // in a per-kernel Worker, not the main thread). Use the JupyterLab kernel
    // manager API instead: connectTo() + requestExecute().
    // Maps kernel id → timestamp (ms) of last successful injection.
    // Timestamp-based rather than boolean so the cooldown naturally expires
    // after a kernel restart (Pyodide never emits 'restarting', so we cannot
    // clear the flag explicitly — we rely on time passing instead).
    const _injectedKernels = new Map();
    const _INJECT_COOLDOWN_MS = 2000; // prevents idle-loop from our own requestExecute
    // Persistent connections kept alive solely to watch statusChanged (restart detection).
    const _kernelWatchConns = new Map();
    // Guard: subscribe to kernels.runningChanged only once.
    let _runningChangedConnected = false;

    async function _injectContextIntoKernel(kernels, model) {
        const id = model && model.id;
        if (!id) return;
        const _lastTs = _injectedKernels.get(id);
        if (_lastTs !== undefined && Date.now() - _lastTs < _INJECT_COOLDOWN_MS) return;
        _injectedKernels.set(id, Date.now());
        const ctx = window.DYNASTORE_CONTEXT;
        if (!ctx || !ctx.baseUrl) return;
        const safeBase = (ctx.baseUrl || '').replace(/'/g, "\\'");
        const safeCatalog = (ctx.catalogCode || '').replace(/'/g, "\\'");
        const safeToken = (ctx.token || '').replace(/'/g, "\\'");
        const stacJson = JSON.stringify(ctx.stacItems || {}).replace(/'/g, "\\'");
        const code = [
            'import json, os',
            'try:',
            '    import pyodide_http; pyodide_http.patch_all()',
            'except ImportError: pass',
            "os.environ['DYNASTORE_BASE_URL'] = '" + safeBase + "'",
            "os.environ['DYNASTORE_CATALOG'] = '" + safeCatalog + "'",
            "os.environ['DYNASTORE_TOKEN'] = '" + safeToken + "'",
            'try:',
            "    STAC_CONTEXT = json.loads('" + stacJson + "')",
            "    print(f\"DynaStore: base={os.environ['DYNASTORE_BASE_URL']} token={'set' if os.environ['DYNASTORE_TOKEN'] else 'EMPTY'}\")",
            'except Exception as e:',
            '    STAC_CONTEXT = {}',
        ].join('\n');
        // Reuse the persistent watcher connection when available to avoid
        // creating an extra KernelConnection just for execution.
        let conn = _kernelWatchConns.get(id);
        let ownedConn = !conn;
        if (!conn) conn = kernels.connectTo({ model });
        try {
            const future = conn.requestExecute({ code, silent: true, store_history: false });
            await future.done;
            console.log('[Bridge] Context injected into kernel', id);
        } catch (e) {
            _injectedKernels.delete(id);
            console.warn('[Bridge] Failed to inject into kernel', id, e);
        } finally {
            if (ownedConn) try { conn.dispose(); } catch (_) {}
        }
    }

    function _watchKernels(app) {
        const kernels = app.serviceManager && app.serviceManager.kernels;
        if (!kernels) return;

        // Establish a persistent watcher connection for each kernel so we can
        // detect restarts via statusChanged and clear _injectedKernels.
        function _ensureWatcher(model) {
            const id = model && model.id;
            if (!id || _kernelWatchConns.has(id)) return;
            let watchConn;
            try {
                watchConn = kernels.connectTo({ model });
                _kernelWatchConns.set(id, watchConn);
                watchConn.statusChanged.connect(function(kernel, status) {
                    if (status === 'restarting' || status === 'autorestarting') {
                        // Explicit restart signal: clear timestamp so next idle injects immediately.
                        // Pyodide kernels never emit this, but standard WS kernels do.
                        _injectedKernels.delete(id);
                    } else if (status === 'idle') {
                        // Inject on every idle; the cooldown inside _injectContextIntoKernel
                        // prevents an infinite loop caused by our own requestExecute → idle cycle.
                        // Pyodide never emits 'restarting', so timestamp expiry is the only way
                        // to detect that the kernel restarted and lost its env vars.
                        _injectContextIntoKernel(kernels, model);
                    } else if (status === 'dead' || status === 'terminating') {
                        _injectedKernels.delete(id);
                        _kernelWatchConns.delete(id);
                        try { watchConn.dispose(); } catch (_) {}
                    }
                });
            } catch (e) {
                console.warn('[Bridge] Failed to set up watcher for kernel', id, e);
                _kernelWatchConns.delete(id);
                if (watchConn) try { watchConn.dispose(); } catch (_) {}
            }
        }

        try {
            for (const model of kernels.running()) {
                _ensureWatcher(model);
                _injectContextIntoKernel(kernels, model);
            }
        } catch (e) {}
        if (!_runningChangedConnected) {
            try {
                kernels.runningChanged.connect(function() {
                    try {
                        for (const model of kernels.running()) {
                            _ensureWatcher(model);
                            _injectContextIntoKernel(kernels, model);
                        }
                    } catch (e2) {}
                });
                _runningChangedConnected = true;
            } catch (e) {
                console.warn('[Bridge] kernels.runningChanged subscription failed:', e);
            }
        }
    }

    // Kick off seeding as soon as the app is ready. This is independent of
    // INIT_CONTEXT so notebooks appear even before host posts auth context.
    (async function bootSeed() {
        const app = await waitForJupyterApp(30000);
        if (!app) {
            console.warn('[Bridge] jupyterapp never appeared; contents-manager seeding skipped.');
            return;
        }
        window.DYNASTORE_JUPYTER_APP = app;
        await seedNotebooksViaContentsManager(app);
        _watchKernels(app);
        // Refresh file browser if present (/tree/ build) so seeded files show.
        try {
            if (app.commands.hasCommand('filebrowser:refresh')) {
                app.commands.execute('filebrowser:refresh').catch(() => {});
            }
        } catch (e) {}
        // If URL has ?path=, the app may have already failed to open it
        // before we seeded; retry via docmanager:open now that it exists.
        try {
            const q = new URLSearchParams(window.location.search);
            const wanted = q.get('path');
            if (wanted && app.commands.hasCommand('docmanager:open')) {
                await app.commands.execute('docmanager:open', { path: wanted, factory: 'Notebook' }).catch(() => {});
            }
        } catch (e) {}
        try { window.parent.postMessage({ type: 'LITE_SEED_COMPLETE' }, '*'); } catch (e) {}
    })();

    window.addEventListener('message', async (event) => {
        if (!event.data || !event.data.type) return;

        // --- INIT_CONTEXT: inject auth + catalog context ---
        if (event.data.type === 'INIT_CONTEXT') {
            console.log("[Bridge] Received INIT_CONTEXT", event.data);
            const { token, catalogCode, baseUrl, stacItems } = event.data;

            window.DYNASTORE_CONTEXT.token = token;
            window.DYNASTORE_CONTEXT.catalogCode = catalogCode;
            window.DYNASTORE_CONTEXT.baseUrl = baseUrl || '';
            window.DYNASTORE_CONTEXT.stacItems = stacItems;

            // Inject env vars into all running kernels with updated context.
            // Clears the injection cache so a second INIT_CONTEXT (e.g. auth
            // token refresh) re-propagates to every kernel.
            if (window.DYNASTORE_JUPYTER_APP) {
                _injectedKernels.clear();
                _watchKernels(window.DYNASTORE_JUPYTER_APP);
            }
            window.parent.postMessage({ type: 'NOTEBOOK_READY' }, '*');
        }

        // --- LOAD_NOTEBOOK: write one notebook into the shim + optionally open it ---
        if (event.data.type === 'LOAD_NOTEBOOK') {
            const { notebook, notebookId, subdir, autoOpen } = event.data;
            if (!notebook || !notebook.cells) {
                console.error("[Bridge] Invalid notebook content");
                return;
            }
            const path = _seedNotebook(subdir, notebookId, notebook);
            console.log("[Bridge] LOAD_NOTEBOOK seeded:", path);
            if (autoOpen !== false) _openInLab(path);
        }

        // --- LOAD_NOTEBOOK_BATCH: preload many notebooks at once, no auto-open ---
        if (event.data.type === 'LOAD_NOTEBOOK_BATCH') {
            const items = Array.isArray(event.data.items) ? event.data.items : [];
            let seeded = 0;
            for (const it of items) {
                if (!it || !it.notebook || !it.notebook.cells) continue;
                _seedNotebook(it.subdir, it.notebookId, it.notebook);
                seeded++;
            }
            console.log("[Bridge] LOAD_NOTEBOOK_BATCH seeded", seeded, "notebooks");
            // Refresh JupyterLab's file browser so newly seeded files show up
            try {
                const app = await waitForJupyterApp(20000);
                if (app && app.commands.hasCommand('filebrowser:refresh')) {
                    app.commands.execute('filebrowser:refresh').catch(function() {});
                }
            } catch (e) {}
        }

        // --- OPEN_NOTEBOOK: open an already-seeded notebook by path ---
        if (event.data.type === 'OPEN_NOTEBOOK') {
            const path = event.data.path;
            if (path) _openInLab(path);
        }
    });

    function _seedNotebook(subdir, notebookId, notebook) {
        const name = (notebookId || 'notebook') + '.ipynb';
        const path = (subdir ? subdir.replace(/^\/+|\/+$/g, '') + '/' : '') + name;
        const now = new Date().toISOString();
        CONTENTS_STORE[path] = {
            name: name,
            path: path,
            type: 'notebook',
            format: 'json',
            content: notebook,
            writable: true,
            created: CONTENTS_STORE[path]?.created || now,
            last_modified: now,
            mimetype: null,
            size: null,
        };
        // Ensure the parent directory entry exists so the file browser lists it
        if (subdir) {
            const dir = subdir.replace(/^\/+|\/+$/g, '');
            if (dir && !CONTENTS_STORE[dir]) {
                CONTENTS_STORE[dir] = {
                    name: dir.split('/').pop(),
                    path: dir,
                    type: 'directory',
                    format: 'json',
                    content: null,
                    writable: true,
                    created: now,
                    last_modified: now,
                };
            }
        }
        // Also push into the real contents manager if the app is ready.
        const app = window.DYNASTORE_JUPYTER_APP;
        if (app && app.serviceManager) {
            (async () => {
                try {
                    if (subdir) {
                        const dir = subdir.replace(/^\/+|\/+$/g, '');
                        try { await app.serviceManager.contents.get(dir, { content: false }); }
                        catch (e) {
                            try { await app.serviceManager.contents.save(dir, { type: 'directory', format: 'json', content: null }); }
                            catch (e2) {}
                        }
                    }
                    await app.serviceManager.contents.save(path, {
                        type: 'notebook', format: 'json', content: notebook,
                    });
                } catch (e) {
                    console.warn('[Bridge] live contents.save failed for', path, e);
                }
            })();
        }
        return path;
    }

    async function _openInLab(path) {
        const app = await waitForJupyterApp(30000);
        if (!app) {
            // Last-resort deep-link: ask the host to reload the iframe with ?path=
            // so JupyterLab opens the notebook from the contents shim on boot.
            console.warn("[Bridge] jupyterlab not ready; requesting deep-link open for " + path);
            try {
                window.parent.postMessage({ type: 'LITE_DEEP_LINK_OPEN', path: path }, '*');
            } catch (e) {}
            return;
        }
        try {
            await app.commands.execute('docmanager:open', { path: path, factory: 'Notebook' });
        } catch (e) {
            console.warn("[Bridge] docmanager:open failed for " + path, e);
        }
    }

    // Custom Save Handler: persist notebooks back to DynaStore
    window.saveNotebookToDynaStore = async function(name, content) {
        console.log(`[Bridge] Saving notebook ${name}...`);

        const { token, catalogCode, baseUrl } = window.DYNASTORE_CONTEXT;

        if (!catalogCode) {
            console.error("[Bridge] Cannot save: no catalog selected");
            return;
        }

        try {
            const notebookId = name.replace('.ipynb', '');
            const apiBase = baseUrl || '';
            const url = `${apiBase}/notebooks/${encodeURIComponent(catalogCode)}/${encodeURIComponent(notebookId)}`;

            const headers = { 'Content-Type': 'application/json' };
            if (token) {
                headers['Authorization'] = `Bearer ${token}`;
            }

            const response = await fetch(url, {
                method: 'PUT',
                headers: headers,
                body: JSON.stringify(content)
            });

            if (response.ok) {
                console.log("[Bridge] Notebook saved successfully");
            } else {
                console.error("[Bridge] Save failed", response.status, await response.text());
            }
        } catch (e) {
            console.error("[Bridge] Network error during save", e);
        }
    };

})();
