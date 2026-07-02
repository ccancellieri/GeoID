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

"""Shared catalog/collection lookup-or-404 helpers.

Every OGC-protocol extension (and several non-OGC ones) repeats the same two
shapes when it needs to turn a path-parameter id into a resource or a 404:

* **Model lookup** — call ``get_catalog``/``get_collection`` (or the
  cached ``get_catalog_model``) and raise 404 when the result is falsy.
* **Internal-id resolution** — call ``resolve_catalog_id``/
  ``resolve_collection_id``, translate a ``ValueError`` into 404, and raise
  404 again when the resolved id itself is falsy.

Free functions here are importable by any extension, OGC or not — mirrors
the shape of :mod:`dynastore.extensions.tools.query`. :class:`OGCServiceMixin`
subclasses additionally get thin ``_resolve_catalog_or_404`` /
``_resolve_collection_or_404`` wrapper methods (see ``extensions/ogc_base.py``)
that resolve ``self._get_catalogs_service()`` for them.
"""

from typing import TYPE_CHECKING, Any, Optional

from fastapi import HTTPException

if TYPE_CHECKING:
    from dynastore.models.protocols import CatalogsProtocol
    from dynastore.models.shared_models import Catalog, Collection


async def resolve_catalog_or_404(
    catalogs_svc: "CatalogsProtocol",
    catalog_id: str,
    *,
    detail: Optional[str] = None,
    use_model: bool = False,
    **kwargs: Any,
) -> "Catalog":
    """Fetch a catalog model or raise 404.

    Calls ``catalogs_svc.get_catalog(catalog_id, **kwargs)`` by default —
    ``**kwargs`` forwards whatever the call site passes (e.g. ``lang=``,
    ``hints=``, ``ctx=``; call sites use these inconsistently).  Pass
    ``use_model=True`` to call ``get_catalog_model`` instead (the raw/cached
    lookup used by config- and status-style endpoints that don't need
    localization).
    """
    getter = catalogs_svc.get_catalog_model if use_model else catalogs_svc.get_catalog
    catalog = await getter(catalog_id, **kwargs)
    if not catalog:
        raise HTTPException(
            status_code=404,
            detail=detail or f"Catalog '{catalog_id}' not found.",
        )
    return catalog


async def resolve_collection_or_404(
    catalogs_svc: Any,
    catalog_id: str,
    collection_id: str,
    *,
    detail: Optional[str] = None,
    **kwargs: Any,
) -> "Collection":
    """Fetch a collection model or raise 404.

    ``catalogs_svc`` accepts either a ``CatalogsProtocol`` or its
    ``.collections`` sub-resource — both expose a compatible
    ``get_collection(catalog_id, collection_id, **kwargs)``. ``**kwargs``
    forwards ``lang=``/``hints=``/``ctx=`` as the call site passes them.
    """
    collection = await catalogs_svc.get_collection(catalog_id, collection_id, **kwargs)
    if not collection:
        raise HTTPException(
            status_code=404,
            detail=detail or f"Collection '{collection_id}' not found.",
        )
    return collection


async def resolve_internal_catalog_id_or_404(
    catalogs_svc: "CatalogsProtocol",
    catalog_id: str,
    *,
    detail: Optional[str] = None,
) -> str:
    """Resolve a public catalog ``external_id`` to its immutable internal id.

    Wraps ``resolve_catalog_id(catalog_id, allow_missing=False)``: a
    ``ValueError`` (not found) and a falsy result are both mapped to 404.
    Returns the resolved internal id — never a model.
    """
    try:
        internal_id = await catalogs_svc.resolve_catalog_id(
            catalog_id, allow_missing=False
        )
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    if not internal_id:
        raise HTTPException(
            status_code=404,
            detail=detail or f"Catalog '{catalog_id}' not found.",
        )
    return internal_id


async def resolve_internal_collection_id_or_404(
    catalogs_svc: Any,
    internal_catalog_id: str,
    collection_id: str,
    *,
    detail: Optional[str] = None,
) -> str:
    """Resolve a public collection ``external_id`` to its immutable internal id.

    Wraps ``catalogs_svc.collections.resolve_collection_id(internal_catalog_id,
    collection_id, allow_missing=False)``: a ``ValueError`` (not found) and a
    falsy result are both mapped to 404. An ``AttributeError`` (a test stub
    without ``resolve_collection_id``) falls back to treating ``collection_id``
    as already-internal, matching the pre-existing call sites this replaces.
    ``catalogs_svc`` accepts either a ``CatalogsProtocol`` (whose
    ``.collections`` this reaches through) or the ``.collections``
    sub-resource directly.
    """
    collections = getattr(catalogs_svc, "collections", catalogs_svc)
    try:
        internal_id = await collections.resolve_collection_id(
            internal_catalog_id, collection_id, allow_missing=False
        )
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except AttributeError:
        internal_id = collection_id
    if not internal_id:
        raise HTTPException(
            status_code=404,
            detail=detail or f"Collection '{collection_id}' not found.",
        )
    return internal_id
