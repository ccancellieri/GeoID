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

"""Capability protocols for web UI contribution.

Producers implement `WebPageContributor.get_web_pages()` and/or
`StaticAssetProvider.get_static_assets()`. The WebModule iterates
`get_protocols(...)` during its own lifespan — no reflective class
walking across foreign extensions.
"""

from dataclasses import dataclass, field
from typing import Any, Callable, Dict, Iterable, List, Optional, Protocol, Tuple, Union, runtime_checkable


LocalizedText = Union[str, Dict[str, str]]


@dataclass(frozen=True)
class WebPageSpec:
    """Declarative specification for a pluggable web page.

    ``handler`` is an (optionally async) callable invoked to produce page
    content.  It may accept keyword arguments named ``request`` and/or
    ``language``; the WebModule introspects its signature and only passes
    the parameters it declares.
    """

    page_id: str
    title: LocalizedText
    handler: Callable[..., Any]
    icon: str = "fa-circle"
    description: LocalizedText = ""
    required_roles: Optional[Tuple[str, ...]] = None
    audience_policy_id: Optional[str] = None
    """ID of a registered ``Policy`` whose role bindings define this page's
    audience. Preferred over ``required_roles`` for new pages: operators
    rebind the policy to whatever roles they want via REST without
    touching decorator code. ``required_roles`` remains as a fallback for
    pages that haven't been migrated."""
    section: Optional[LocalizedText] = None
    priority: int = 0
    is_embed: bool = False
    enabled: bool = True

    def to_config(self) -> Dict[str, Any]:
        """Render the spec as the legacy config dict consumed by WebModule."""
        return {
            "id": self.page_id,
            "title": self.title,
            "icon": self.icon,
            "description": self.description,
            "required_roles": list(self.required_roles) if self.required_roles else None,
            "audience_policy_id": self.audience_policy_id,
            "priority": self.priority,
            "section": self.section,
            "is_embed": self.is_embed,
            "enabled": self.enabled,
        }


@dataclass(frozen=True)
class StaticAsset:
    """Declarative specification for a static-files prefix.

    ``files_provider`` is a callable returning the list of absolute file
    paths that should be served under ``prefix``.  ``base_path`` is an
    optional filesystem root for future ``StaticFiles`` mounts.
    """

    prefix: str
    files_provider: Callable[[], List[str]]
    base_path: Optional[str] = None


@runtime_checkable
class WebPageContributor(Protocol):
    """A producer that contributes one or more web pages."""

    def get_web_pages(self) -> Iterable[WebPageSpec]: ...


@runtime_checkable
class StaticAssetProvider(Protocol):
    """A producer that contributes one or more static-asset prefixes."""

    def get_static_assets(self) -> Iterable[StaticAsset]: ...


__all__ = [
    "WebPageSpec",
    "StaticAsset",
    "WebPageContributor",
    "StaticAssetProvider",
]
