#    Copyright 2025 FAO
#
#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at
#
#        http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS,
#    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#    See the License for the specific language governing permissions and
#    limitations under the License.
#
#    Author: Carlo Cancellieri (ccancellieri@gmail.com)
#    Company: FAO, Viale delle Terme di Caracalla, 00100 Rome, Italy
#    Contact: copyright@fao.org - http://fao.org/contact-us/terms/en/

import logging
import inspect
import os
import re
from pathlib import Path
from typing import Dict, Any, Callable, List, Optional, Union
from contextlib import asynccontextmanager

from dynastore.tools.language_utils import resolve_localized_field
from dynastore.modules import ModuleProtocol, get_protocols
from dynastore.models.protocols.web import WebModuleProtocol, WebPageProtocol, StaticFilesProtocol, WebOverrideProtocol
from dynastore.models.protocols.web_ui import WebPageContributor, StaticAssetProvider
from dynastore.models.protocols.configs import ConfigsProtocol
from .models import WebPageConfig, WebPageSettingsConfig

logger = logging.getLogger(__name__)

class WebModule(WebModuleProtocol, ModuleProtocol):
    """
    The WebModule acts as the central registry and logic provider for
    Dynastore's web interface capabilities. It manages:
    - Registered Web Pages (from extensions)
    - Static Content Providers
    - Documentation Discovery
    - Localization resolution
    """

    def __init__(self):
        # Registry for pluggable web pages: { page_id: {config: WebPageConfig, providers: List[Tuple[int, Callable, bool]]} }
        self.web_pages: Dict[str, Dict[str, Any]] = {}

        # Registry for static content providers: { prefix: provider_callable }
        self.static_providers: Dict[str, Callable[[], List[str]]] = {}

        # Documentation registry
        self.docs_registry: Dict[str, Dict[str, Any]] = {}

        # Application directories for scanning
        self.app_dirs: List[str] = []
        self.project_root: Optional[str] = None

    @asynccontextmanager
    async def lifespan(self, app_state: object):

        """Module lifecycle management."""
        self._initialize_discovery()

        # WebPageSettingsConfig auto-registers via PluginConfig.__init_subclass__
        # at import time — no explicit registration needed.

        # Automatic discovery of protocol implementers
        self._discover_contributors()

        yield


    def _initialize_discovery(self):
        """Initialize project structure discovery."""
        self.project_root = self._find_project_root(
            __file__, ["setup.py", "pyproject.toml", "main.py"]
        )
        if self.project_root:
            self.app_dirs = self._discover_app_dirs(self.project_root)
            self.docs_registry = self._scan_for_documentation()
        else:
            logger.warning(
                "WebModule: Could not find project root. Docs scanning will be limited."
            )

    def register_web_page(self, config: Dict[str, Any], provider: Callable[..., Any]):
        """Registers a web page handler."""
        page_id = config["id"]
        priority = config.get("priority", 0)
        is_embed = config.get("is_embed", False)

        if page_id not in self.web_pages:
            # Initialise with a placeholder config; a non-embed provider will
            # set the real config below (or this embed is the only registrant).
            self.web_pages[page_id] = {
                "config": WebPageConfig(**config),
                "providers": [],
            }

        # Guard: same handler may arrive from decorator scan (configure_app)
        # AND global protocol scan (WebModule.lifespan) — skip duplicates.
        # Python creates a fresh bound-method object on every getattr, so `is`
        # alone is unreliable; compare by underlying function + instance instead.
        def _same_handler(h1: Any, h2: Any) -> bool:
            if h1 is h2:
                return True
            return (
                hasattr(h1, "__func__") and hasattr(h2, "__func__")
                and h1.__func__ is h2.__func__
                and getattr(h1, "__self__", None) is getattr(h2, "__self__", None)
            )

        existing_handlers = [p[1] for p in self.web_pages[page_id]["providers"]]
        if any(_same_handler(h, provider) for h in existing_handlers):
            logger.debug(f"WebModule: Skipping duplicate provider for '{page_id}'")
            return

        # Add provider tuple and keep list sorted by priority (lower = first)
        self.web_pages[page_id]["providers"].append((priority, provider, is_embed))
        self.web_pages[page_id]["providers"].sort(key=lambda x: x[0])

        # Only non-embed providers carry authoritative navigation metadata
        # (title, icon, section, required_roles).  An embed provider injecting
        # supplemental content must not overwrite the page's nav config.
        if not is_embed and priority < self.web_pages[page_id]["config"].priority:
            self.web_pages[page_id]["config"] = WebPageConfig(**config)

        logger.info(f"WebModule: Registered {'embed ' if is_embed else ''}provider for '{page_id}' (priority: {priority})")

        # If this page targets a section (e.g., 'home'), also register it as a provider for that section
        section = config.get("section")
        if section and section != page_id:
            logger.info(f"WebModule: Also registering provider for section '{section}' (from page '{page_id}')")
            section_config = config.copy()
            section_config["id"] = section
            section_config["is_embed"] = True # section targets are always embedded
            self.register_web_page(section_config, provider)

    def register_static_provider(self, prefix: str, provider: Any):
        """Registers a static file provider."""
        if prefix in self.static_providers:
            logger.warning(
                f"WebModule: Overwriting static provider for prefix '{prefix}'"
            )
        self.static_providers[prefix] = provider
        logger.info(f"WebModule: Registered static provider for prefix '{prefix}'")

    async def is_static_file_provided(self, prefix: str, path: str) -> bool:
        """Checks if a static file is provided by any registered provider."""
        provider = self.static_providers.get(prefix)
        if not provider:
            return False
            
        if isinstance(provider, StaticFilesProtocol):
            return await provider.is_file_provided(path)
        
        # Legacy: provider is a callable returning a list of files
        try:
            files = provider()
            return path in files
        except Exception:
            return False

    async def list_static_files(self, prefix: str, query: Optional[str] = None, limit: int = 100, offset: int = 0) -> List[str]:
        """Lists static files for a prefix with optional filtering and pagination."""
        provider = self.static_providers.get(prefix)
        if not provider:
            return []
            
        if isinstance(provider, StaticFilesProtocol):
            return await provider.list_static_files(query=query, limit=limit, offset=offset)
        
        # Legacy fallback
        try:
            files = provider()
            if query:
                files = [f for f in files if query.lower() in f.lower()]
            return files[offset:offset+limit]
        except Exception:
            return []


    def _discover_contributors(self) -> None:
        """Discover web-page / static-asset contributors via the protocol
        registry.  No reflective class-walking across foreign extensions —
        each contributor self-describes via ``get_web_pages()`` and
        ``get_static_assets()``.
        """
        # WebPageContributor: capability-protocol producers
        for contributor in get_protocols(WebPageContributor):
            try:
                for spec in contributor.get_web_pages():
                    self.register_web_page(spec.to_config(), spec.handler)
            except Exception as e:
                logger.error(
                    f"WebModule: Failed to register WebPageContributor from {contributor}: {e}"
                )

        # StaticAssetProvider: capability-protocol producers
        for provider in get_protocols(StaticAssetProvider):
            try:
                for asset in provider.get_static_assets():
                    self.register_static_provider(
                        asset.prefix.strip("/"), asset.files_provider
                    )
            except Exception as e:
                logger.error(
                    f"WebModule: Failed to register StaticAssetProvider from {provider}: {e}"
                )

        # Legacy WebPageProtocol (render_page + get_web_page_config) — still
        # supported for extensions that use the object-oriented style.
        for page_prov in get_protocols(WebPageProtocol):
            try:
                config = page_prov.get_web_page_config()
                handler = getattr(page_prov, "render_page", None)
                if handler:
                    self.register_web_page(config, handler)
            except Exception as e:
                logger.error(
                    f"WebModule: Failed to register WebPageProtocol from {page_prov}: {e}"
                )

        # Legacy StaticFilesProtocol — still supported for richer providers.
        for static_prov in get_protocols(StaticFilesProtocol):
            try:
                prefix = static_prov.get_static_prefix().strip("/")
                self.register_static_provider(prefix, static_prov)
            except Exception as e:
                logger.error(
                    f"WebModule: Failed to register StaticFilesProtocol from {static_prov}: {e}"
                )

    async def _get_overrides(self) -> Dict[str, WebPageConfig]:
        """Fetch persistent overrides from the config system."""
        configs = get_protocols(ConfigsProtocol)
        for c in configs:
            try:
                settings = await c.get_config(WebPageSettingsConfig)
                return settings.pages
            except Exception as e:
                logger.debug(f"WebModule: Could not fetch web_pages config: {e}")
        return {}

    async def get_web_pages_config(self, language: str = "en") -> List[Dict[str, Any]]:
        """
        Returns the list of registered web pages with localized titles and descriptions.
        Merges protocol/decorator metadata with persistent configuration overrides.
        """
        overrides = await self._get_overrides()
        
        results = []
        for p in self.web_pages.values():
            base_config: WebPageConfig = p["config"]
            
            # Apply persistent overrides if available
            override = overrides.get(base_config.id)
            if override:
                # Merge logic: overrides take precedence
                config_data = base_config.model_dump()
                override_data = override.model_dump(exclude_unset=True)
                config_data.update(override_data)
                config = WebPageConfig(**config_data)
            else:
                config = base_config

            if not config.enabled:
                continue

            # Embed-only providers inject content into a parent page; they are
            # not standalone navigation destinations and must not appear in the
            # left-side menu returned to the frontend.
            if config.is_embed:
                continue

            # Resolve translations
            title = resolve_localized_field(config.title, language)
            description = resolve_localized_field(config.description, language)
            section = resolve_localized_field(config.section, language) if config.section else None
            icon = config.icon

            results.append(
                {
                    "id": config.id,
                    "title": title,
                    "icon": icon,
                    "description": description,
                    "priority": config.priority,
                    "section": section,
                    "is_embed": config.is_embed,
                    "required_roles": config.required_roles,
                }
            )

        return results

    async def get_web_page_content(self, page_id: str, request: Any, language: str = "en") -> Any:
        """
        Retrieves and aggregates content for a specific web page by ID.
        Supports multiple registered providers which are joined together.
        """
        if page_id not in self.web_pages:
            return None
        
        providers = self.web_pages[page_id].get("providers", [])
        content_parts = []
        
        for priority, handler, is_embed in providers:
            try:
                sig = inspect.signature(handler)
                kwargs = {}
                if "request" in sig.parameters:
                    kwargs["request"] = request
                if "language" in sig.parameters:
                    kwargs["language"] = language
                
                if inspect.iscoroutinefunction(handler):
                    content = await handler(**kwargs)
                elif callable(handler):
                    content = handler(**kwargs)
                else:
                    content = handler
                
                from fastapi import Response
                if isinstance(content, Response):
                    # We might need to handle response objects by extracting content
                    if hasattr(content, "body"):
                         body = content.body
                         content_parts.append(body.tobytes().decode() if isinstance(body, memoryview) else body.decode())
                    else:
                         content_parts.append(str(content))
                else:
                    content_parts.append(str(content))
            except Exception as e:
                logger.error(f"WebModule: Error calling provider for '{page_id}': {e}", exc_info=True)
                continue

        if not content_parts:
            return ""
            
        return "\n".join(content_parts)

    def generate_etag(self, content: Any) -> str:
        """Generates an ETag for the given content."""
        import hashlib
        if isinstance(content, str):
            content = content.encode("utf-8")
        elif not isinstance(content, bytes):
            content = str(content).encode("utf-8")
        return hashlib.md5(content).hexdigest()

    def get_cache_headers(self, max_age: int = 3600) -> Dict[str, str]:
        """Returns standard cache-control headers. Callers set ETag separately."""
        return {
            "Cache-Control": f"public, max-age={max_age}, stale-while-revalidate=60",
            "Vary": "Accept-Encoding",
        }


    def get_style_overrides(self) -> List[str]:
        """
        Aggregates all style overrides from registered WebOverrideProtocol providers.
        """
        overrides = get_protocols(WebOverrideProtocol)
        all_styles = []
        for o in overrides:
            all_styles.extend(o.get_style_overrides())
        return all_styles

    def get_docs_manifest(self) -> Dict[str, List[Dict[str, str]]]:
        """Returns the organized documentation manifest."""
        buckets = {"root": [], "modules": [], "extensions": [], "tasks": []}

        for doc_item in self.docs_registry.values():
            cat = doc_item.get("category", "root")
            if cat not in buckets:
                buckets[cat] = []

            buckets[cat].append(
                {
                    "id": doc_item["id"],
                    "title": doc_item["title"],
                    "path": doc_item["path"],
                }
            )

        manifest = {}
        for cat, items in buckets.items():
            if items:
                items.sort(key=lambda x: x["title"])
                manifest[cat] = items

        return manifest

    def get_doc_path(self, doc_id: str) -> Optional[str]:
        """Resolves a doc_id to a file path."""
        item = self.docs_registry.get(doc_id)
        return item["path"] if item else None

    # --- Internal Logic (Moved from web extension) ---

    def _find_project_root(self, start_path: str, markers: List[str]) -> Optional[str]:
        """Walks up from start_path to find a directory containing one of the marker files."""
        try:
            start = Path(start_path).resolve()
            if start.is_file():
                start = start.parent

            for current_path in [start, *start.parents]:
                for marker in markers:
                    marker_path = current_path / marker
                    if marker_path.exists():
                        if marker.lower() in ("pyproject.toml", "setup.py"):
                            return str(current_path)
                        if self._has_app_structure(current_path):
                            return str(current_path)
            return None
        except Exception as e:
            logger.debug(f"Error resolving project root: {e}")
            return None

    def _has_app_structure(self, root_dir: Path) -> bool:
        """Helper to verify if a directory looks like a Dynastore app."""
        comps = ["modules", "extensions", "tasks"]
        # Check direct children
        for comp in comps:
            if (root_dir / comp).is_dir():
                return True
        # Check src directory
        src_dir = root_dir / "src"
        if src_dir.is_dir():
            for child in src_dir.iterdir():
                if child.is_dir():
                    for comp in comps:
                        if (child / comp).is_dir():
                            return True
        return False

    def _discover_app_dirs(self, project_root: str) -> List[str]:
        """Return a list of candidate application directories."""
        candidates: List[str] = []
        comps = ("modules", "extensions", "tasks")
        root = Path(project_root)

        # 1) Direct child directories under <project_root>/src
        src_dir = root / "src"
        if src_dir.is_dir():
            for child in src_dir.iterdir():
                if child.is_dir():
                    for comp in comps:
                        if (child / comp).is_dir():
                            candidates.append(str(child.resolve()))
                            break

        # 2) Direct child directories under project_root
        for child in root.iterdir():
            if child.is_dir():
                for comp in comps:
                    if (child / comp).is_dir():
                        candidates.append(str(child.resolve()))
                        break

        # 3) project_root itself
        for comp in comps:
            if (root / comp).is_dir():
                candidates.append(str(root.resolve()))
                break

        # Deduplicate
        seen = set()
        result: List[str] = []
        for p in candidates:
            if p not in seen:
                seen.add(p)
                result.append(p)
        return result

    def _scan_for_documentation(self) -> Dict[str, Dict[str, Any]]:
        """Scans for documentation recursively."""
        registry = {}

        def process_doc_file(
            fpath: Path,
            category: str,
            component_name: Optional[str],
            root_reference: Path,
        ):
            try:
                # ID Generation Logic
                id_parts = []
                if category != "root":
                    id_parts.append(category)

                if component_name:
                    id_parts.append(component_name)

                is_readme = fpath.stem.lower() == "readme"
                if not is_readme:
                    stem = fpath.stem.lower()
                    if component_name and stem.startswith(component_name):
                        clean_stem = stem[len(component_name) :].lstrip("_-")
                        if clean_stem:
                            id_parts.append(clean_stem)
                    else:
                        id_parts.append(stem)

                if not id_parts:
                    id_parts.append(fpath.stem.lower())

                doc_id = ":".join(id_parts)
                final_id = doc_id
                counter = 1
                while final_id in registry:
                    final_id = f"{doc_id}_{counter}"
                    counter += 1
                doc_id = final_id

                # Title Extraction Logic
                if is_readme:
                    title = fpath.parent.name.replace("_", " ").title()
                else:
                    title = fpath.stem.replace("_", " ").title()

                try:
                    with open(fpath, "r", encoding="utf-8", errors="ignore") as f:
                        for _ in range(50):
                            line = f.readline()
                            if not line:
                                break
                            stripped = line.strip()
                            if stripped.startswith("#"):
                                raw_title = stripped.lstrip("#").strip()
                                if raw_title:
                                    clean_title = re.sub(
                                        r"\[([^\]]+)\]\([^)]+\)", r"\1", raw_title
                                    )
                                    title = re.sub(r"[*_`]", "", clean_title).strip()
                                    break
                except Exception:
                    pass

                if component_name and category != "root":
                    comp_title = component_name.replace("_", " ").title()
                    if (
                        title.lower() != comp_title.lower()
                        and not title.lower().startswith(comp_title.lower())
                    ):
                        title = f"{comp_title}: {title}"

                registry[doc_id] = {
                    "id": doc_id,
                    "title": title,
                    "path": str(fpath.resolve()),
                    "category": category,
                }
            except Exception as e:
                logger.error(f"Error processing doc file {fpath}: {e}")

        # Scanning
        if self.project_root:
            root_path = Path(self.project_root)
            for item in root_path.glob("*.md"):
                process_doc_file(item, "root", None, root_path)

            docs_folder = root_path / "docs"
            if docs_folder.is_dir():
                for item in docs_folder.glob("*.md"):
                    process_doc_file(item, "root", None, docs_folder)

            categories = ["modules", "extensions", "tasks"]
            for app_dir_str in self.app_dirs:
                app_path = Path(app_dir_str)
                app_docs = app_path / "docs"
                if app_docs.is_dir():
                    for item in app_docs.glob("*.md"):
                        process_doc_file(item, "root", None, app_docs)

                for cat in categories:
                    cat_dir = app_path / cat
                    if not cat_dir.is_dir():
                        continue
                    for component_dir in cat_dir.iterdir():
                        if not component_dir.is_dir():
                            continue
                        for root, dirs, files in os.walk(component_dir):
                            dirs[:] = [
                                d
                                for d in dirs
                                if d != "node_modules" and not d.startswith(".")
                            ]
                            for filename in files:
                                if filename.lower().endswith((".md", ".markdown")):
                                    md_file = Path(root) / filename
                                    process_doc_file(
                                        md_file, cat, component_dir.name, component_dir
                                    )

        return registry
