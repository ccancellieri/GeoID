/**
 * Reusable Context Selector for DynaStore Administrative Views.
 * Provides a cascaded select interface for Catalogs -> Collections -> Assets.
 */
class ContextSelector {
    /**
     * @param {Object} options Configuration options
     * @param {string} options.containerId ID of the DOM element to mount the selector in
     * @param {boolean} options.enableCollection Whether to show the Collection dropdown
     * @param {boolean} options.enableVirtualCollections Whether to include Virtual STAC Collections
     * @param {boolean} options.enableAssets Whether to show the Asset dropdown
     * @param {string} options.defaultCatalog Initial catalog to select (e.g., '_system_')
     * @param {Function} options.onChange Callback fired on context change (receives {catalogId, collectionId, assetId})
     */
    constructor(options = {}) {
        this.containerId = options.containerId || 'context-selector-container';
        this.enableCollection = options.enableCollection ?? true;
        this.enableVirtualCollections = options.enableVirtualCollections ?? false;
        this.enableAssets = options.enableAssets ?? false;
        this.enableSearch = options.enableSearch ?? false;
        this.multipleCatalogs = options.multipleCatalogs ?? false;
        this.multipleCollections = options.multipleCollections ?? false;
        this.defaultCatalog = options.defaultCatalog || '_system_';
        this.autoSelectFirst = options.autoSelectFirst ?? false;
        this.preferredCollection = options.preferredCollection || null;
        this.onChangeCallback = options.onChange || null;

        this.state = {
            catalogId: this.multipleCatalogs ? [this.defaultCatalog] : this.defaultCatalog,
            collectionId: this.multipleCollections ? [] : '',
            assetId: '',
            q: ''
        };

        this.container = document.getElementById(this.containerId);
        if (!this.container) {
            console.error(`ContextSelector: Container #${this.containerId} not found.`);
            return;
        }

        this.render();
        this.bindEvents();
        this.loadCatalogs();
    }

    render() {
        let html = '';
        if (this.enableSearch) {
            html += `
                <input type="text" id="cs-search-input" class="filter-input" placeholder="Search..." style="padding: 0.25rem 0.5rem;"/>
            `;
        }

        html += `
            <select id="cs-catalog-select" class="filter-select" ${this.multipleCatalogs ? 'multiple' : ''}>
                <option value="_system_">System (Global)</option>
            </select>
        `;

        if (this.enableCollection) {
            html += `
                <select id="cs-collection-select" class="filter-select" disabled ${this.multipleCollections ? 'multiple' : ''}>
                    ${this.multipleCollections ? '' : '<option value="">All Collections</option>'}
                </select>
            `;
        }
        
        if (this.enableAssets) {
            html += `
                <select id="cs-asset-select" class="filter-select" disabled>
                    <option value="">All Assets</option>
                </select>
            `;
        }

        // Apply a wrapper class for styling if needed
        this.container.innerHTML = `<div class="context-selector-wrapper" style="display: flex; gap: 0.5rem; align-items: center;">${html}</div>`;
        
        this.catalogSelect = document.getElementById('cs-catalog-select');
        if (this.enableCollection) this.collectionSelect = document.getElementById('cs-collection-select');
        if (this.enableAssets) this.assetSelect = document.getElementById('cs-asset-select');
    }

    bindEvents() {
        if (this.enableSearch) {
            this.searchInput = document.getElementById('cs-search-input');
            let debounceTimer;
            this.searchInput.addEventListener('input', (e) => {
                clearTimeout(debounceTimer);
                debounceTimer = setTimeout(() => {
                    this.state.q = e.target.value.trim();
                    this.refreshData();
                }, 300);
            });
        }

        this.catalogSelect.addEventListener('change', (e) => {
            if (this.multipleCatalogs) {
                this.state.catalogId = Array.from(e.target.selectedOptions).map(o => o.value);
            } else {
                this.state.catalogId = e.target.value;
            }
            this.state.collectionId = this.multipleCollections ? [] : '';
            this.state.assetId = '';
            
            if (this.enableCollection) {
                this.collectionSelect.innerHTML = this.multipleCollections ? '' : '<option value="">All Collections</option>';
                const isSystem = this.multipleCatalogs ? this.state.catalogId.includes('_system_') : (this.state.catalogId === '_system_');
                this.collectionSelect.disabled = isSystem;
                if (!isSystem) {
                    const catalogsToLoad = this.multipleCatalogs ? this.state.catalogId : [this.state.catalogId];
                    // Load collections for first catalog initially, could be extended later
                    if (catalogsToLoad.length > 0) {
                        this.loadCollections(catalogsToLoad[0]);
                    }
                }
            }
            if (this.enableAssets) {
                this.assetSelect.innerHTML = '<option value="">All Assets</option>';
                this.assetSelect.disabled = true;
            }
            this.triggerChange();
        });

        if (this.enableCollection) {
            this.collectionSelect.addEventListener('change', (e) => {
                if (this.multipleCollections) {
                    this.state.collectionId = Array.from(e.target.selectedOptions).map(o => o.value);
                } else {
                    this.state.collectionId = e.target.value;
                }
                this.state.assetId = '';
                
                if (this.enableAssets) {
                    this.assetSelect.innerHTML = '<option value="">All Assets</option>';
                    const hasCollection = this.multipleCollections ? this.state.collectionId.length > 0 : !!this.state.collectionId;
                    this.assetSelect.disabled = !hasCollection;
                    if (hasCollection) {
                        const firstCatalog = this.multipleCatalogs ? this.state.catalogId[0] : this.state.catalogId;
                        const firstCollection = this.multipleCollections ? this.state.collectionId[0] : this.state.collectionId;
                        this.loadAssets(firstCatalog, firstCollection);
                    }
                }
                this.triggerChange();
            });
        }
        
        if (this.enableAssets) {
            this.assetSelect.addEventListener('change', (e) => {
                this.state.assetId = e.target.value;
                this.triggerChange();
            });
        }
    }

    triggerChange() {
        // Expose array versions as well
        const detail = { 
            ...this.state,
            catalogIds: Array.isArray(this.state.catalogId) ? this.state.catalogId : [this.state.catalogId],
            collectionIds: Array.isArray(this.state.collectionId) ? this.state.collectionId : (this.state.collectionId ? [this.state.collectionId] : [])
        };
        const event = new CustomEvent('contextChanged', { detail });
        window.dispatchEvent(event);
        if (this.onChangeCallback) {
            this.onChangeCallback(detail);
        }
    }

    refreshData() {
        // Clear options except default
        this.catalogSelect.innerHTML = '<option value="_system_">System (Global)</option>';
        if (this.enableCollection) {
            this.collectionSelect.innerHTML = this.multipleCollections ? '' : '<option value="">All Collections</option>';
            this.collectionSelect.disabled = true;
        }
        if (this.enableAssets) {
            this.assetSelect.innerHTML = '<option value="">All Assets</option>';
            this.assetSelect.disabled = true;
        }
        
        // Reset selected IDs to default
        this.state.catalogId = this.multipleCatalogs ? [this.defaultCatalog] : this.defaultCatalog;
        this.state.collectionId = this.multipleCollections ? [] : '';
        this.state.assetId = '';
        
        this.loadCatalogs();
    }

    // Resolve absolute API path with optional reverse-proxy prefix.
    // Page is served at <prefix>/web/pages/<id> in iframes; relative URLs
    // resolve against /web/pages/, hitting the wrong endpoint (404).
    // Match the pattern in extensions/stac/static/stac_browser.html.
    _apiPath(path) {
        const p = window.location.pathname || '';
        const idx = p.indexOf('/web/');
        const prefix = idx !== -1 ? p.substring(0, idx) : '';
        return prefix + path;
    }

    async loadCatalogs() {
        try {
            let offset = 0;
            const limit = 100;
            let allCatalogs = [];

            while (true) {
                let url = this._apiPath(`/catalogs?limit=${limit}&offset=${offset}`);
                if (this.state.q) url += `&q=${encodeURIComponent(this.state.q)}`;

                const res = await fetch(url, { credentials: "same-origin" });
                if (!res.ok) throw new Error("Failed to fetch catalogs");
                const catalogs = await res.json();
                
                if (catalogs.length === 0) break;
                allCatalogs = allCatalogs.concat(catalogs);
                if (catalogs.length < limit) break;
                offset += limit;
            }
            
            allCatalogs.forEach(c => {
                const opt = document.createElement('option');
                const idValue = c.id || c.code;
                opt.value = idValue;
                opt.textContent = `${c.title || idValue}`;
                if (this.multipleCatalogs ? this.state.catalogId.includes(idValue) : idValue === this.defaultCatalog) opt.selected = true;
                this.catalogSelect.appendChild(opt);
            });

            // Auto-select the first non-system catalog when no explicit default is set
            const currentIsSystem = this.multipleCatalogs
                ? this.state.catalogId.every(id => id === '_system_')
                : this.state.catalogId === '_system_';
            if (this.autoSelectFirst && currentIsSystem && allCatalogs.length > 0) {
                const firstId = allCatalogs[0].id || allCatalogs[0].code;
                if (this.multipleCatalogs) {
                    this.state.catalogId = [firstId];
                    Array.from(this.catalogSelect.options).forEach(o => { o.selected = o.value === firstId; });
                } else {
                    this.state.catalogId = firstId;
                    this.catalogSelect.value = firstId;
                }
            }

            // Trigger initial load for collections if default isn't _system_
            const isSystem = this.multipleCatalogs ? this.state.catalogId.every(id => id === '_system_') : (this.state.catalogId === '_system_');
            if (!isSystem && this.enableCollection) {
                const catalogsToLoad = this.multipleCatalogs ? this.state.catalogId : [this.state.catalogId];
                if (catalogsToLoad.length > 0) {
                    this.loadCollections(catalogsToLoad[0]);
                }
            }
        } catch(e) { 
            console.error("ContextSelector: Failed to fetch catalogs", e); 
        }
    }

    async loadCollections(catalogId) {
        try {
            let offset = 0;
            const limit = 100;
            let allCollections = [];

            while (true) {
                let url = this._apiPath(`/catalogs/${catalogId}/collections?limit=${limit}&offset=${offset}`);
                if (this.enableVirtualCollections) {
                    url += '&include_virtual=true';
                }
                if (this.state.q) {
                    url += `&q=${encodeURIComponent(this.state.q)}`;
                }

                const res = await fetch(url, { credentials: "same-origin" });
                if (!res.ok) throw new Error("Failed to fetch collections");
                const cols = await res.json();
                
                if (cols.length === 0) break;
                allCollections = allCollections.concat(cols);
                if (cols.length < limit) break;
                offset += limit;
            }
            
            allCollections.forEach(c => {
                const opt = document.createElement('option');
                opt.value = c.id || c.code;
                opt.textContent = c.title || c.id || c.code;
                this.collectionSelect.appendChild(opt);
            });
            this.collectionSelect.disabled = false;

            // Auto-select preferred collection (e.g. "demo") or the first one
            if (this.autoSelectFirst && allCollections.length > 0) {
                const preferred = this.preferredCollection
                    ? allCollections.find(c => (c.id || c.code) === this.preferredCollection)
                    : null;
                const target = preferred || allCollections[0];
                const targetId = target.id || target.code;
                if (this.multipleCollections) {
                    this.state.collectionId = [targetId];
                    Array.from(this.collectionSelect.options).forEach(o => { o.selected = o.value === targetId; });
                } else {
                    this.state.collectionId = targetId;
                    this.collectionSelect.value = targetId;
                }
                this.triggerChange();
            }
        } catch(e) { 
            console.error("ContextSelector: Failed to load collections", e); 
        }
    }
    
    async loadAssets(catalogId, collectionId) {
        try {
            const res = await fetch(
                this._apiPath(`/catalogs/${catalogId}/collections/${collectionId}/assets`),
                { credentials: "same-origin" }
            );
            if (!res.ok) throw new Error("Failed to fetch assets");
            const assets = await res.json();
            
            assets.forEach(a => {
                const opt = document.createElement('option');
                opt.value = a.id || a.name;
                opt.textContent = a.title || a.id || a.name;
                this.assetSelect.appendChild(opt);
            });
            this.assetSelect.disabled = false;
        } catch(e) {
            console.warn("ContextSelector: Assets endpoints might not be implemented yet.", e);
        }
    }
}

// Make globally available
window.ContextSelector = ContextSelector;
