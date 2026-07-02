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

"""Declarative coverage -> Styles + Maps link builder.

Pass 2's Styles integration is declarative: coverages do not render.
The metadata response carries link references so clients can discover
the styles registry (Pass 1) and the Maps rendering endpoint.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional

from dynastore.models.protocols.link_contrib import AnchoredLink


def _link_dict(link: AnchoredLink) -> Dict[str, Any]:
    """Render an ``AnchoredLink`` as the coverage-metadata wire shape.

    ``title`` is omitted (rather than emitted empty) to keep the wire shape
    identical to the pre-``AnchoredLink`` raw-dict links, which never carried
    a title.
    """
    d: Dict[str, Any] = {"rel": link.rel, "type": link.media_type, "href": link.href}
    if link.title:
        d["title"] = link.title
    return d


def build_coverage_links(
    *,
    base_url: str,
    catalog_id: str,
    collection_id: str,
    default_style_id: Optional[str],
) -> List[Dict[str, Any]]:
    base = base_url.rstrip("/")
    cov_base = f"{base}/coverages/catalogs/{catalog_id}/collections/{collection_id}/coverage"
    styles_base = f"{base}/styles/catalogs/{catalog_id}/collections/{collection_id}/styles"

    def _root(rel: str, media_type: str, href: str) -> AnchoredLink:
        return AnchoredLink(
            anchor="resource_root", rel=rel, href=href, title="", media_type=media_type,
        )

    links: List[AnchoredLink] = [
        _root("self", "application/json", f"{cov_base}/metadata"),
        _root("data", "image/tiff;application=geotiff", cov_base),
        _root("describedby", "application/json", f"{cov_base}/domainset"),
        _root("describedby", "application/json", f"{cov_base}/rangetype"),
        _root("styles", "application/json", styles_base),
    ]
    if default_style_id:
        style_href = f"{styles_base}/{default_style_id}"
        links.append(_root("style", "application/json", style_href))
        links.append(_root("style", "application/vnd.ogc.sld+xml;version=1.1", style_href))
        links.append(
            _root(
                "http://www.opengis.net/def/rel/ogc/1.0/map",
                "image/png",
                f"{base}/maps/catalogs/{catalog_id}/collections/{collection_id}/map?style={default_style_id}",
            )
        )
    return [_link_dict(link) for link in links]
