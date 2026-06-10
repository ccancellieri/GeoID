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

"""
Pluggable provider of the catalog list shown in UI pickers.

The web layer serves a single ``GET /web/catalogs`` endpoint that the
front-end picker reads. Instead of hard-coding which API supplies the
catalogs, every module that can enumerate catalogs registers a
``CatalogListProvider`` via ``register_plugin``; the endpoint resolves the
winner from ``get_protocols(CatalogListProvider)`` (priority-sorted,
``is_available()``-gated).

WINNER-TAKES-ALL, NOT FIRST-NON-EMPTY. The highest-priority *available*
provider is authoritative — its result is used even when empty. This is a
security invariant: an auth-aware provider (e.g. IAM) that legitimately
returns zero catalogs for a restricted principal must NOT be overridden by
a lower-priority full-list provider, or a tenant-scoped deployment would
leak catalogs the caller cannot see. Conversely, on a deployment where IAM
is not mounted there is no auth enforcement, so the lower-priority public
full-list provider is the one that wins and the picker is populated.

Priority convention (lower wins): auth-aware providers ~10; the public
full-list fallback ~100.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, List, Protocol, runtime_checkable

if TYPE_CHECKING:
    from starlette.requests import Request


@dataclass(frozen=True)
class CatalogOption:
    """One entry in a catalog picker: a stable id and a display label.

    ``title`` is always a resolved, single-language string (never a raw
    ``LocalizedText`` map) so the front-end can render it directly. Falls
    back to the id when the catalog has no localized title.
    """

    id: str
    title: str


@runtime_checkable
class CatalogListProvider(Protocol):
    """Producer of the catalog list for UI pickers.

    Optional: if no provider is registered, ``get_protocols`` yields nothing
    and ``/web/catalogs`` returns an empty list. See the module docstring for
    the winner-takes-all resolution and the security rationale.
    """

    priority: int

    # Deliberately NOT named ``list_catalogs``: this protocol is
    # runtime-checkable, so ``isinstance`` matches on attribute presence
    # alone. A storage-layer ``CatalogsProtocol`` implementation also has a
    # ``list_catalogs(limit, offset, ...)`` method and would satisfy the
    # check, win the priority sort, and then be called with a ``Request``
    # as ``limit``. The distinct name keeps structural matching unambiguous.
    async def list_catalog_options(
        self, request: "Request", language: str = "en"
    ) -> List[CatalogOption]: ...
