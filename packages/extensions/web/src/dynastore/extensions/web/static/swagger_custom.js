/**
 * DynaStore Swagger UI Customization
 * Features: FAO Branding, Dark/Light Toggle, Sticky Header, Header Restructuring
 */

window.addEventListener('load', function() {
    console.log("DynaStore UI Custom Logic Loaded");

    // --- CONFIGURATION ---
    const CSS_LAYOUT_URL = window.SWAGGER_LAYOUT_CSS_URL || 'swagger_common.css';
    const CSS_THEME_URL = window.SWAGGER_THEME_CSS_URL || 'swagger_theme.css'; 
    
    // Robust Path Resolution for Static Assets
    let staticBase = '';
    const scriptName = 'swagger_custom.js';
    const scripts = document.querySelectorAll('script');
    for (const script of scripts) {
        if (script.src && script.src.includes(scriptName)) {
            staticBase = script.src.substring(0, script.src.lastIndexOf('/') + 1);
            break;
        }
    }
    if (!staticBase) {
        if (CSS_LAYOUT_URL && CSS_LAYOUT_URL.indexOf('/') !== -1) {
            staticBase = CSS_LAYOUT_URL.substring(0, CSS_LAYOUT_URL.lastIndexOf('/') + 1);
        } else {
            staticBase = 'extension-static/'; 
        }
    }
    
    const LOGO_URL = staticBase + 'dynastore.png';
    const FAO_LOGO_URL = staticBase + 'fao.svg';
    
    const THEME_LINK_ID = 'dynastore-dark-mode';
    const LAYOUT_LINK_ID = 'dynastore-common-layout';
    const STORAGE_KEY = 'dynastore_theme';
    const STICKY_KEY = 'dynastore_sticky_header';

    // --- ICONS (Clean SVG Paths) ---
    const ICON_PIN = `<svg viewBox="0 0 24 24"><path d="M16 12V4h1v-2H7v2h1v8l-2 2v2h5v6h2v-6h5v-2z"/></svg>`;
    const ICON_MOON = `<svg viewBox="0 0 24 24"><path d="M12 3c-4.97 0-9 4.03-9 9s4.03 9 9 9 9-4.03 9-9c0-.46-.04-.92-.1-1.36-.98 1.37-2.58 2.26-4.4 2.26-3.03 0-5.5-2.47-5.5-5.5 0-1.82.89-3.42 2.26-4.4-.44-.06-.9-.1-1.36-.1z"/></svg>`;
    const ICON_SUN = `<svg viewBox="0 0 24 24"><path d="M6.76 4.84l-1.8-1.79-1.41 1.41 1.79 1.79 1.42-1.41zM4 10.5H1v2h3v-2zm9-9.95h-2V3.5h2V.55zm7.45 3.91l-1.41-1.41-1.79 1.79 1.41 1.41 1.79-1.79zm-3.21 13.7l1.79 1.8 1.41-1.41-1.8-1.79-1.4 1.4zM20 10.5v2h3v-2h-3zm-8-5c-3.31 0-6 2.69-6 6s2.69 6 6 6 6-2.69 6-6-2.69-6-6-6zm-1 16.95h2V19.5h-2v2.95zm-7.45-3.91l1.41 1.41 1.79-1.8-1.41-1.41-1.79 1.8z"/></svg>`;
    const ICON_HELP = `<svg viewBox="0 0 24 24"><path d="M12 2C6.48 2 2 6.48 2 12s4.48 10 10 10 10-4.48 10-10S17.52 2 12 2zm1 17h-2v-2h2v2zm2.07-7.75l-.9.92C13.45 12.9 13 13.5 13 15h-2v-.5c0-1.1.45-2.1 1.17-2.83l1.24-1.26c.37-.36.59-.86.59-1.41 0-1.1-.9-2-2-2s-2 .9-2 2H8c0-2.21 1.79-4 4-4s4 1.79 4 4c0 .88-.36 1.68-.93 2.25z"/></svg>`;

    // --- 1. INJECT COMMON LAYOUT ---
    if (!document.getElementById(LAYOUT_LINK_ID)) {
        const link = document.createElement('link');
        link.rel = 'stylesheet';
        link.id = LAYOUT_LINK_ID;
        link.href = CSS_LAYOUT_URL.includes('/') ? CSS_LAYOUT_URL : staticBase + CSS_LAYOUT_URL;
        document.head.appendChild(link);
    }

    // --- 2. HEADER CONSTRUCTION ---
    async function reconstructHeader() {
        const swaggerUiRoot = document.querySelector('.swagger-ui');
        if (!swaggerUiRoot) return false;

        let topbar = swaggerUiRoot.querySelector('.topbar');
        if (!topbar) {
            topbar = document.createElement('div');
            topbar.className = 'topbar';
            swaggerUiRoot.prepend(topbar);
        }

        let wrapper = topbar.querySelector('.wrapper');
        if (!wrapper) {
            wrapper = document.createElement('div');
            wrapper.className = 'wrapper';
            topbar.appendChild(wrapper);
        }

        if (wrapper.dataset.customized === 'true' || wrapper.dataset.processing === 'true') return true;
        wrapper.dataset.processing = 'true';

        try {
            wrapper.innerHTML = '';
            wrapper.style.display = 'flex';
            wrapper.style.justifyContent = 'space-between';
            wrapper.style.alignItems = 'center';
            wrapper.style.width = '100%';
            wrapper.style.maxWidth = '1400px';
            wrapper.style.margin = '0 auto';

            // --- LEFT SECTION ---
            const leftSection = document.createElement('div');
            leftSection.style.display = 'flex';
            leftSection.style.alignItems = 'center';
            leftSection.style.gap = '15px';

            // 1. FAO Logo
            const faoContainer = document.createElement('a');
            faoContainer.href = "https://www.fao.org";
            faoContainer.target = "_blank";
            faoContainer.style.display = "flex"; 
            faoContainer.style.alignItems = "center";
            
            try {
                const response = await fetch(FAO_LOGO_URL);
                if (response.ok) {
                    faoContainer.innerHTML = await response.text();
                    const svgEl = faoContainer.querySelector('svg');
                    if (svgEl) {
                        svgEl.style.height = "50px"; 
                        svgEl.style.width = "auto";
                        svgEl.style.display = "block";
                    }
                } else { throw new Error("Fetch failed"); }
            } catch (e) {
                const faoImg = document.createElement('img');
                faoImg.src = FAO_LOGO_URL;
                faoImg.alt = "FAO";
                faoImg.style.height = "50px";
                faoImg.style.display = "block";
                faoContainer.appendChild(faoImg);
            }
            leftSection.appendChild(faoContainer);

            // Divider
            const divider = document.createElement('div');
            divider.style.height = "40px";
            divider.style.width = "1px";
            divider.style.backgroundColor = "#ccc";
            leftSection.appendChild(divider);

            // Link to the /web page
            const webLink = document.createElement('a');
            webLink.href = staticBase + '../'; 
            webLink.target = "_blank";
            webLink.style.display = "flex";
            webLink.style.alignItems = "center";
            
            // 2. DynaStore Logo
            const logoContainer = document.createElement('div');
            logoContainer.id = 'dynastore-logo-container';
            logoContainer.style.height = "50px"; 
            logoContainer.style.display = "flex";
            logoContainer.style.alignItems = "center";

            const logoImg = document.createElement('img');
            logoImg.src = LOGO_URL;
            logoImg.alt = "DynaStore Logo";
            logoImg.style.height = "100%"; 
            logoImg.style.width = "auto";
            logoImg.style.display = "block";
            logoImg.id = "dynastore-logo-img";

            logoContainer.appendChild(logoImg);
            webLink.appendChild(logoContainer);
            leftSection.appendChild(webLink);
            
            // Alternative: make logo clickable to /web
            logoContainer.style.cursor = "pointer";
            logoContainer.onclick = () => {
                window.open(staticBase + '../', "_blank");
            };

            // 3. Title
            const infoDesc = document.querySelector('.swagger-ui .info .description');
            let appTitleText = "Dynamic Storage API server"; 
            if (infoDesc && infoDesc.textContent) {
                const text = infoDesc.textContent.split('\n')[0].trim(); 
                if (text) appTitleText = text;
            }

            const titleDiv = document.createElement('div');
            titleDiv.className = 'header-app-title';
            // appTitleText comes from the OpenAPI spec's info.description — a
            // plain title, never markup — so it must be set as text, not HTML.
            titleDiv.textContent = appTitleText;
            leftSection.appendChild(titleDiv);

            wrapper.appendChild(leftSection);

            // --- RIGHT SECTION ---
            const rightSection = document.createElement('div');
            rightSection.className = 'header-right';
            
            // Buttons
            rightSection.appendChild(createStickyToggle());
            rightSection.appendChild(createThemeToggle());
            rightSection.appendChild(createHelpButton());

            wrapper.appendChild(rightSection);

            wrapper.dataset.customized = 'true';
            
            // Init States
            const storedTheme = localStorage.getItem(STORAGE_KEY);
            const prefersDark = window.matchMedia && window.matchMedia('(prefers-color-scheme: dark)').matches;
            setTheme(storedTheme || (prefersDark ? 'dark' : 'light'));

            const storedSticky = localStorage.getItem(STICKY_KEY);
            if (storedSticky === 'true') {
                setSticky(true);
            }

        } catch (e) {
            console.error("Header Error:", e);
        } finally {
            wrapper.dataset.processing = 'false';
        }
        return true;
    }

    // --- 3. CONTROLS ---

    function createThemeToggle() {
        const btn = document.createElement('button');
        btn.id = 'theme-toggle-btn';
        btn.className = 'btn fao-btn'; 
        btn.onclick = () => {
            const current = localStorage.getItem(STORAGE_KEY);
            setTheme(current === 'dark' ? 'light' : 'dark');
        };
        return btn;
    }

    function createStickyToggle() {
        const btn = document.createElement('button');
        btn.id = 'sticky-toggle-btn';
        btn.className = 'btn fao-btn';
        btn.innerHTML = `${ICON_PIN} <span>Pin</span>`; 
        btn.title = "Toggle Sticky Header";
        btn.onclick = () => {
            const topbar = document.querySelector('.swagger-ui .topbar');
            const isSticky = topbar.classList.contains('sticky');
            setSticky(!isSticky);
        };
        return btn;
    }

    function createHelpButton() {
        const btn = document.createElement('button');
        btn.id = 'help-btn';
        btn.className = 'btn fao-btn';
        btn.innerHTML = `${ICON_HELP} <span>Help</span>`; 
        btn.title = "Open Help";
        btn.onclick = () => {
            const helpHeader = document.getElementById('operations-tag-Help');
            if (helpHeader) {
                helpHeader.scrollIntoView({ behavior: 'smooth', block: 'center' });
                const section = helpHeader.closest('.opblock-tag-section');
                if (section && !section.classList.contains('is-open')) helpHeader.click();
            } else {
                const docLink = document.querySelector('.swagger-ui .info .help-box a');
                if (docLink) window.open(docLink.href, '_blank');
            }
        };
        return btn;
    }

    function setSticky(enable) {
        const topbar = document.querySelector('.swagger-ui .topbar');
        const btn = document.getElementById('sticky-toggle-btn');
        if (!topbar) return;

        if (enable) {
            topbar.classList.add('sticky');
            if (btn) btn.classList.add('active');
            localStorage.setItem(STICKY_KEY, 'true');
        } else {
            topbar.classList.remove('sticky');
            if (btn) btn.classList.remove('active');
            localStorage.setItem(STICKY_KEY, 'false');
        }
    }

    function updateThemeBtn(mode) {
        const btn = document.getElementById('theme-toggle-btn');
        if (!btn) return;
        
        if (mode === 'dark') {
            btn.innerHTML = `${ICON_MOON} <span>Dark</span>`;
        } else {
            btn.innerHTML = `${ICON_SUN} <span>Light</span>`;
        }
    }

    function setTheme(mode) {
        let link = document.getElementById(THEME_LINK_ID);
        const headerTitle = document.querySelector('.header-app-title');
        const logoImg = document.getElementById('dynastore-logo-img');

        updateThemeBtn(mode);

        if (mode === 'dark') {
            if (!link) {
                link = document.createElement('link');
                link.rel = 'stylesheet';
                link.id = THEME_LINK_ID;
                link.href = CSS_THEME_URL.includes('/') ? CSS_THEME_URL : staticBase + CSS_THEME_URL;
                document.head.appendChild(link);
            }
            if (headerTitle) headerTitle.style.color = '#e0e0e0';
            if (logoImg) logoImg.style.filter = "invert(1) brightness(2)"; 
            localStorage.setItem(STORAGE_KEY, 'dark');
        } else {
            if (link) link.remove();
            if (headerTitle) headerTitle.style.color = '#555555';
            if (logoImg) logoImg.style.filter = "none";
            localStorage.setItem(STORAGE_KEY, 'light');
        }
    }

    // --- 4. MOVE & STYLE AUTHORIZE BUTTON ---
    function moveAuthorizeButton() {
        const headerRight = document.querySelector('.header-right');
        if (!headerRight) return; 

        const authWrapper = document.querySelector('.swagger-ui .auth-wrapper');
        
        if (authWrapper) {
            // Apply the shared class to the button itself
            const authBtn = authWrapper.querySelector('.btn.authorize');
            if (authBtn && !authBtn.classList.contains('fao-btn')) {
                authBtn.classList.add('fao-btn');
                // Optional: Force icon update or check it
            }

            if (authWrapper.parentElement !== headerRight) {
                headerRight.appendChild(authWrapper);
                const schemeContainer = document.querySelector('.swagger-ui .scheme-container');
                if (schemeContainer && !schemeContainer.hasChildNodes()) {
                    schemeContainer.style.display = 'none';
                }
            }
        }
    }

    // --- 5. FOOTER ---
    function injectFooter() {
        if (document.getElementById('dynastore-footer')) return;

        const footer = document.createElement('div');
        footer.id = 'dynastore-footer';

        const label = document.createElement('span');
        label.textContent = 'DynaStore API';
        footer.appendChild(label);

        const versionSpan = document.createElement('span');
        versionSpan.id = 'swagger-version';
        versionSpan.style.marginLeft = '4px';
        footer.appendChild(versionSpan);

        const copyright = document.createElement('span');
        copyright.textContent = ' \u00A9 2025 FAO';
        footer.appendChild(copyright);

        fetch('../health').then(function(r){return r.json()}).then(function(d){
            if(d.version) versionSpan.textContent = 'v' + d.version;
        }).catch(function(){});
        
        const author = document.createElement('span');
        author.innerText = ' • Architected by Carlo Cancellieri';
        Object.assign(author.style, {
            opacity: '0',
            transition: 'opacity 0.3s ease-in-out',
            cursor: 'pointer',
            marginLeft: '10px',
            fontSize: '0.9em',
            textDecoration: 'underline'
        });
        
        footer.addEventListener('mouseenter', () => { author.style.opacity = '1'; });
        footer.addEventListener('mouseleave', () => { author.style.opacity = '0'; });
        author.addEventListener('click', (e) => {
            e.stopPropagation();
            window.open('https://github.com/ccancellieri', '_blank');
        });

        footer.appendChild(author);
        document.body.appendChild(footer); 
    }

    // --- EXECUTION LOOP ---
    const checkInterval = setInterval(() => {
        reconstructHeader();
        moveAuthorizeButton();
        injectFooter(); 
    }, 100);
    
    setTimeout(() => clearInterval(checkInterval), 15000);
});