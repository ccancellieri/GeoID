"""Declarative coverage -> Styles + Maps link builder.

Pass 2's Styles integration is declarative: coverages do not render.
The metadata response carries link references so clients can discover
the styles registry (Pass 1) and the Maps rendering endpoint.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional


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

    links: List[Dict[str, Any]] = [
        {"rel": "self", "type": "application/json", "href": f"{cov_base}/metadata"},
        {"rel": "data", "type": "image/tiff;application=geotiff", "href": cov_base},
        {"rel": "describedby", "type": "application/json", "href": f"{cov_base}/domainset"},
        {"rel": "describedby", "type": "application/json", "href": f"{cov_base}/rangetype"},
        {"rel": "styles", "type": "application/json", "href": styles_base},
    ]
    if default_style_id:
        style_href = f"{styles_base}/{default_style_id}"
        links.append({"rel": "style", "type": "application/json", "href": style_href})
        links.append({
            "rel": "style",
            "type": "application/vnd.ogc.sld+xml;version=1.1",
            "href": style_href,
        })
        links.append({
            "rel": "http://www.opengis.net/def/rel/ogc/1.0/map",
            "type": "image/png",
            "href": f"{base}/maps/catalogs/{catalog_id}/collections/{collection_id}/map?style={default_style_id}",
        })
    return links
