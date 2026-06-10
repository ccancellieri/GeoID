#    Copyright 2026 FAO
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

from typing import List, Callable, Dict, Any, Optional, Protocol, runtime_checkable

@runtime_checkable
class WebPageProtocol(Protocol):

    """Protocol for components that provide a web page."""
    def get_web_page_config(self) -> Dict[str, Any]: ...
    async def render_page(self, request: Any, language: str = "en") -> Any: ...


@runtime_checkable
class WebOverrideProtocol(Protocol):
    """Protocol for components that override default web UI elements."""
    def get_landing_page_override(self) -> Optional[Dict[str, Any]]: ...
    def get_style_overrides(self) -> List[str]: ...
    def get_component_overrides(self) -> Dict[str, Any]: ...


@runtime_checkable
class StaticFilesProtocol(Protocol):
    """Protocol for components that provide static files."""
    def get_static_prefix(self) -> str: ...
    async def is_file_provided(self, path: str) -> bool: ...
    async def list_static_files(
        self, query: Optional[str] = None, limit: int = 100, offset: int = 0
    ) -> List[str]: ...


class WebModuleProtocol():
    web_pages: Dict[str, Dict[str, Any]]
    static_providers: Dict[str, Callable[..., Any]]
    static_prefix_meta: Dict[str, Dict[str, str]]

    def register_web_page(
        self, config: Dict[str, Any], provider: Callable[..., Any]
    ): ...
    def register_static_provider(
        self,
        prefix: str,
        provider: Callable[..., Any],
        owner: str = "",
        description: str = "",
        public: bool = True,
    ): ...
    async def get_web_pages_config(self, language: str = "en") -> List[Dict[str, Any]]: ...
    def get_web_page_content(self, page_id: str, request: Any, language: str = "en") -> Any: ...
    def get_docs_manifest(self) -> Dict[str, List[Dict[str, str]]]: ...
    def get_doc_path(self, doc_id: str) -> Optional[str]: ...
    def generate_etag(self, content_parts: List[bytes]) -> str: ...
    def get_cache_headers(self, max_age: int = 3600) -> Dict[str, str]: ...
    def list_static_prefix_info(self) -> List[Dict[str, str]]:
        """Return metadata (prefix, owner, description) for every registered
        static prefix.  Each entry is a plain dict with keys
        ``prefix``, ``owner``, and ``description``."""
        ...

    def get_static_prefix_meta(self) -> Dict[str, Any]:
        """Return the raw metadata dict keyed by prefix.

        Each value is a dict with at least ``owner``, ``description``, and
        ``public`` (bool).  Callers must treat unknown keys as forward-
        compatible additions and absent ``public`` as ``True``.  The web
        policy builder uses this to derive the anonymous ALLOW list for
        prefixes registered after the literal baseline was written.
        """
        ...

    def list_page_providers(self, page_id: str) -> List[Dict[str, Any]]:
        """Return introspection data for every handler registered for *page_id*.

        Each entry contains ``priority`` (int), ``is_embed`` (bool), and
        ``handler`` (qualified name string).
        """
        ...

