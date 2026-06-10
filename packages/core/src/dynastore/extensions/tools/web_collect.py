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

"""Helpers that let a contributing class materialise its own
`WebPageSpec` / `StaticAsset` lists from ``@expose_web_page`` /
``@expose_static`` decorated bound methods.

Only the contributing instance inspects itself — the WebModule never
performs reflective class-walking across other extensions.
"""

from __future__ import annotations

from typing import Iterator, List, Tuple

from dynastore.models.protocols.web_ui import StaticAsset, WebPageSpec


def _iter_decorated_methods(instance: object, attr: str) -> Iterator[Tuple[str, object]]:
    """Yield ``(name, bound_method)`` pairs for every function on the
    instance's class MRO whose underlying function carries ``attr``.

    Uses class-dict inspection (not ``inspect.getmembers``) so we never
    trigger ``@property`` descriptors during discovery.
    """
    seen: set[str] = set()
    for klass in type(instance).__mro__:
        for name, value in klass.__dict__.items():
            if name in seen:
                continue
            if not callable(value):
                continue
            if not hasattr(value, attr):
                continue
            seen.add(name)
            yield name, getattr(instance, name)


def collect_web_pages(instance: object) -> List[WebPageSpec]:
    """Return `WebPageSpec` entries for every ``@expose_web_page``
    decorated method bound on ``instance``."""

    specs: List[WebPageSpec] = []
    for _, method in _iter_decorated_methods(instance, "_web_page_spec"):
        meta = getattr(method, "_web_page_spec", None)
        if meta is None:
            continue
        specs.append(WebPageSpec(handler=method, **meta))  # type: ignore[arg-type]
    return specs


def collect_static_assets(instance: object) -> List[StaticAsset]:
    """Return `StaticAsset` entries for every ``@expose_static``
    decorated method bound on ``instance``."""

    assets: List[StaticAsset] = []
    for _, method in _iter_decorated_methods(instance, "_web_static_prefix"):
        prefix = getattr(method, "_web_static_prefix", None)
        if prefix is None:
            continue
        owner = getattr(method, "_web_static_owner", "") or ""
        description = getattr(method, "_web_static_description", "") or ""
        public = getattr(method, "_web_static_public", True)
        assets.append(
            StaticAsset(
                prefix=prefix.strip("/"),
                files_provider=method,  # type: ignore[arg-type]
                owner=owner,
                description=description,
                public=public,
            )
        )
    return assets


__all__ = ["collect_web_pages", "collect_static_assets"]
