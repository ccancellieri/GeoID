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

# dynastore/extensions/wfs/wfs_generator.py
from datetime import datetime, timezone, UTC
import orjson
from fastapi import Request, Response

import logging
from typing import Iterable, List, Dict, Any, Optional

from xml.etree.ElementTree import (
    Element,
    SubElement,
    tostring,
    fromstring,
    register_namespace,
)
from xml.dom.minidom import parseString
from datetime import datetime
import html  # For unescaping GML fragments
from dynastore.tools.features import FeatureCollection
from dynastore.extensions.tools.formatters import OutputFormatEnum

logger = logging.getLogger(__name__)

# --- XML Namespace Constants ---
NS = {
    "wfs": "http://www.opengis.net/wfs/2.0",
    "ows": "http://www.opengis.net/ows/1.1",
    "gml": "http://www.opengis.net/gml/3.2",
    "xsi": "http://www.w3.org/2001/XMLSchema-instance",
    "xlink": "http://www.w3.org/1999/xlink",
    "fes": "http://www.opengis.net/fes/2.0",
    "xs": "http://www.w3.org/2001/XMLSchema",
}

register_namespace("gml", NS["gml"])
register_namespace("fes", NS["fes"])
register_namespace("wfs", NS["wfs"])
register_namespace("ows", NS["ows"])
register_namespace("xsi", NS["xsi"])

# --- XML Utilities ---


def _prettify_xml(elem: Element) -> str:
    """Returns a pretty-printed and properly encoded XML string for an Element."""
    rough_string = tostring(elem, "utf-8")
    reparsed = parseString(rough_string)
    # Use toprettyxml for readable, indented output, crucial for debugging and client compatibility.
    return reparsed.toprettyxml(indent="  ", encoding="utf-8").decode("utf-8")


# --- WFS Response Generators ---


def create_exception_report(code: str, locator: Optional[str], text_message: str) -> str:
    """
    Creates an OGC-compliant WFS Exception Report in XML format. This is the
    standard and required way to report errors for all WFS operations.
    """
    root = Element(
        "ows:ExceptionReport",
        {
            "xmlns:ows": NS["ows"],
            "xmlns:xsi": NS["xsi"],
            "xsi:schemaLocation": "http://www.opengis.net/ows/1.1 http://schemas.opengis.net/ows/1.1.0/owsExceptionReport.xsd",
            "version": "1.1.0",
            "xml:lang": "en",
        },
    )
    exception = SubElement(
        root, "ows:Exception", {"exceptionCode": code, "locator": locator or ""}
    )
    SubElement(exception, "ows:ExceptionText").text = text_message
    return _prettify_xml(root)


def create_capabilities_response(
    root_wfs_url: str,
    scoped_catalog_id: Optional[str],
    catalogs_with_collections: Dict[str, List[Dict[str, Any]]],
    service_metadata: Optional[Dict[str, Any]] = None,
    language: str = "en",
) -> str:
    """
    Generates the WFS GetCapabilities XML response. This document is the primary
    discovery mechanism for WFS clients, describing the service's metadata,
    supported operations, and available feature types (collections).
    """
    # Base namespaces
    ns_map = {
        "version": "2.0.0",
        "xmlns:wfs": NS["wfs"],
        "xmlns:ows": NS["ows"],
        "xmlns:gml": NS["gml"],
        "xmlns:fes": NS["fes"],
        "xmlns:xlink": NS["xlink"],
        f"{{{NS['xsi']}}}schemaLocation": f"{NS['wfs']} http://schemas.opengis.net/wfs/2.0/wfs.xsd {NS['gml']} http://schemas.opengis.net/gml/3.2.1/gml.xsd",
        "updateSequence": datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ"),
    }
    # Dynamically add a namespace for each catalog, similar to GeoServer workspaces
    for catalog_id in catalogs_with_collections.keys():
        # Ensure catalog_id is a valid NCName (XML prefix)
        safe_prefix = catalog_id.replace(":", "_").replace("-", "_")

        ns_map[f"xmlns:{safe_prefix}"] = f"{root_wfs_url}/{catalog_id}"

    root = Element("wfs:WFS_Capabilities", ns_map)

    # Service Identification block: high-level metadata about the service.
    service_id = SubElement(root, "ows:ServiceIdentification")
    if service_metadata and service_metadata.get("title"):
        SubElement(service_id, "ows:Title").text = service_metadata["title"]
    else:
        SubElement(service_id, "ows:Title").text = "DynaStore Web Feature Service"

    if service_metadata and service_metadata.get("description"):
        SubElement(service_id, "ows:Abstract").text = service_metadata["description"]
    else:
        SubElement(
            service_id, "ows:Abstract"
        ).text = "Provides access to vector features via the OGC WFS 2.0 standard."
    SubElement(service_id, "ows:ServiceType", {"codeSpace": "OGC"}).text = "WFS"
    SubElement(service_id, "ows:ServiceTypeVersion").text = "2.0.0"
    SubElement(service_id, "ows:Fees").text = "NONE"
    SubElement(service_id, "ows:AccessConstraints").text = "NONE"

    # Service Provider block
    service_provider = SubElement(root, "ows:ServiceProvider")
    SubElement(service_provider, "ows:ProviderName").text = "DynaStore"
    contact = SubElement(service_provider, "ows:ServiceContact")
    SubElement(contact, "ows:IndividualName").text = "Service Administrator"
    contact_info = SubElement(contact, "ows:ContactInfo")
    address = SubElement(contact_info, "ows:Address")
    SubElement(address, "ows:ElectronicMailAddress").text = "admin@example.com"

    # Operations Metadata block: lists the operations this server supports.
    operations_meta = SubElement(root, "ows:OperationsMetadata")

    # The KVP service URL must end with a '?' for clients to correctly
    # append parameters (e.g., ?service=WFS&request=GetFeature).
    # If this is a scoped request, the URL must include the catalog_id.
    if scoped_catalog_id:
        base_op_url = f"{root_wfs_url}/{scoped_catalog_id}"
    else:
        base_op_url = root_wfs_url

    kvp_service_url = f"{base_op_url}?"

    # --- GetCapabilities Operation ---
    op_get_cap = SubElement(
        operations_meta, "ows:Operation", {"name": "GetCapabilities"}
    )
    dcp_get_cap = SubElement(op_get_cap, "ows:DCP")
    http_get_cap = SubElement(dcp_get_cap, "ows:HTTP")
    SubElement(
        http_get_cap,
        "ows:Get",
        {f"{{{NS['xlink']}}}type": "simple", f"{{{NS['xlink']}}}href": kvp_service_url},
    )
    # POST requests should also go to the correct base URL
    SubElement(
        http_get_cap,
        "ows:Post",
        {f"{{{NS['xlink']}}}type": "simple", f"{{{NS['xlink']}}}href": base_op_url},
    )

    param_accept_versions = SubElement(
        op_get_cap, "ows:Parameter", {"name": "AcceptVersions"}
    )
    allowed_versions = SubElement(param_accept_versions, "ows:AllowedValues")
    SubElement(allowed_versions, "ows:Value").text = "2.0.0"

    # --- DescribeFeatureType Operation ---
    op_desc_ft = SubElement(
        operations_meta, "ows:Operation", {"name": "DescribeFeatureType"}
    )
    dcp_desc_ft = SubElement(op_desc_ft, "ows:DCP")
    http_desc_ft = SubElement(dcp_desc_ft, "ows:HTTP")
    SubElement(
        http_desc_ft,
        "ows:Get",
        {f"{{{NS['xlink']}}}type": "simple", f"{{{NS['xlink']}}}href": kvp_service_url},
    )
    SubElement(
        http_desc_ft,
        "ows:Post",
        {f"{{{NS['xlink']}}}type": "simple", f"{{{NS['xlink']}}}href": base_op_url},
    )

    param_output_format_desc = SubElement(
        op_desc_ft, "ows:Parameter", {"name": "outputFormat"}
    )
    allowed_formats_desc = SubElement(param_output_format_desc, "ows:AllowedValues")
    SubElement(allowed_formats_desc, "ows:Value").text = "application/xsd+xml"

    # --- GetFeature Operation ---
    op_get_feat = SubElement(operations_meta, "ows:Operation", {"name": "GetFeature"})
    dcp_get_feat = SubElement(op_get_feat, "ows:DCP")
    http_get_feat = SubElement(dcp_get_feat, "ows:HTTP")
    SubElement(
        http_get_feat,
        "ows:Get",
        {f"{{{NS['xlink']}}}type": "simple", f"{{{NS['xlink']}}}href": kvp_service_url},
    )
    SubElement(
        http_get_feat,
        "ows:Post",
        {f"{{{NS['xlink']}}}type": "simple", f"{{{NS['xlink']}}}href": base_op_url},
    )

    param_result_type = SubElement(op_get_feat, "ows:Parameter", {"name": "resultType"})

    SubElement(
        SubElement(param_result_type, "ows:AllowedValues"), "ows:Value"
    ).text = "results"
    SubElement(
        SubElement(param_result_type, "ows:AllowedValues"), "ows:Value"
    ).text = "hits"

    # As per WFS 2.0.0, startIndex is an integer, default 0.
    # We explicitly state 0 as an allowed value and the default.
    param_start_index = SubElement(op_get_feat, "ows:Parameter", {"name": "startIndex"})
    SubElement(
        SubElement(param_start_index, "ows:AllowedValues"), "ows:Value"
    ).text = "0"  # Indicate it's an integer, starting from 0
    SubElement(param_start_index, "ows:DefaultValue").text = "0"

    param_output_format_feat = SubElement(
        op_get_feat, "ows:Parameter", {"name": "outputFormat"}
    )
    allowed_formats_feat = SubElement(param_output_format_feat, "ows:AllowedValues")
    # Advertise all supported formats
    SubElement(
        allowed_formats_feat, "ows:Value"
    ).text = "application/gml+xml; version=3.2"  # Default
    SubElement(
        allowed_formats_feat, "ows:Value"
    ).text = "application/json"  # For GeoJSON
    SubElement(allowed_formats_feat, "ows:Value").text = "text/csv"
    SubElement(
        allowed_formats_feat, "ows:Value"
    ).text = "application/vnd.geopackage+sqlite3"  # Common MIME for GPKG
    SubElement(
        allowed_formats_feat, "ows:Value"
    ).text = "application/zip"  # For Shapefile

    # General WFS constraints
    SubElement(
        operations_meta, "ows:Constraint", {"name": "ImplementsBasicWFS"}
    ).text = "TRUE"
    SubElement(
        operations_meta, "ows:Constraint", {"name": "ImplementsTransactionalWFS"}
    ).text = "FALSE"
    SubElement(
        operations_meta, "ows:Constraint", {"name": "ImplementsLockingWFS"}
    ).text = "FALSE"
    SubElement(operations_meta, "ows:Constraint", {"name": "KVPEncoding"}).text = "TRUE"
    SubElement(operations_meta, "ows:Constraint", {"name": "XMLEncoding"}).text = "TRUE"
    # Constraints for Response Paging (WFS 2.0.0, Section 7.7.4.4)
    SubElement(
        operations_meta, "ows:Constraint", {"name": "ImplementsResultPaging"}
    ).text = "TRUE"
    SubElement(
        operations_meta, "ows:Constraint", {"name": "PagingIsTransactionSafe"}
    ).text = "FALSE"
    SubElement(
        operations_meta, "ows:Constraint", {"name": "ResponseCacheTimeout"}
    ).text = "PT0S"  # No caching

    # Feature Type List block: the most important part, advertises available layers.
    feature_list = SubElement(root, "wfs:FeatureTypeList")
    for catalog_id, collections in catalogs_with_collections.items():
        # Ensure catalog_id is a valid NCName (XML prefix)
        safe_prefix = catalog_id.replace(":", "_").replace("-", "_")
        for collection in collections:
            feature_type = SubElement(feature_list, "wfs:FeatureType")

            # The qualified name 'catalog:collection' is the unique identifier for a feature type.
            SubElement(
                feature_type, "wfs:Name"
            ).text = f"{safe_prefix}:{collection['id']}"
            SubElement(feature_type, "wfs:Title").text = collection.get("title")
            if collection.get("description"):
                SubElement(feature_type, "wfs:Abstract").text = collection.get(
                    "description"
                )

            keywords_el = SubElement(feature_type, "ows:Keywords")
            if collection.get("keywords"):
                for kw in collection["keywords"]:
                    SubElement(keywords_el, "ows:Keyword").text = kw

            SubElement(
                feature_type, "wfs:DefaultCRS"
            ).text = "urn:ogc:def:crs:EPSG::4326"
            # Advertise Web Mercator as another supported CRS
            SubElement(feature_type, "wfs:OtherCRS").text = "urn:ogc:def:crs:EPSG::3857"

            bbox_element = SubElement(feature_type, "ows:WGS84BoundingBox")

            # --- START: Robust BBOX Parsing (Fourth Attempt) ---
            spatial_extent = collection.get("extent", {}).get("spatial", {})
            bbox_data = spatial_extent.get("bbox")

            # The 'crs' is at the root of the collection, not in 'spatial'.
            # It is also typically a list.
            crs_list = collection.get("crs", [])

            bbox = None
            is_wgs84 = False

            if not crs_list:
                # If CRS is missing entirely, assume the BBOX is WGS84, as that's
                # the requirement for ows:WGS84BoundingBox.
                is_wgs84 = True
                logger.debug(
                    f"Collection {collection.get('id')} has no CRS list; assuming WGS84 for bbox."
                )
            else:
                # Check if any CRS in the list is a WGS84 variant
                for crs_string in crs_list:
                    if isinstance(crs_string, str):
                        if "CRS84" in crs_string or "4326" in crs_string:
                            is_wgs84 = True
                            break  # Found a valid CRS

            if bbox_data and is_wgs84:
                try:
                    if isinstance(bbox_data[0], (list, tuple)):  # Case 1: [[...]]
                        # Check inner list
                        if bbox_data[0] and len(bbox_data[0]) == 4:
                            bbox = bbox_data[0]
                    elif len(bbox_data) == 4:  # Case 2: [...]
                        # Check that first element is NOT a list, to avoid [[...], [...]]
                        if not isinstance(bbox_data[0], list):
                            bbox = bbox_data
                except (IndexError, TypeError, KeyError):
                    logger.warning(
                        f"Collection {safe_prefix}:{collection['id']} had WGS84 CRS but unparseable bbox data: {bbox_data}"
                    )
                    pass  # bbox remains None

            # --- Render the bbox or default ---
            if bbox and len(bbox) == 4 and None not in bbox:
                # We found a valid, 4-element, WGS84 bbox with no None values
                # OWS BoundingBox is (LowerCorner) lon lat (UpperCorner) lon lat
                SubElement(
                    bbox_element, "ows:LowerCorner"
                ).text = f"{bbox[0]} {bbox[1]}"
                SubElement(
                    bbox_element, "ows:UpperCorner"
                ).text = f"{bbox[2]} {bbox[3]}"
            else:
                # Bbox is None or invalid. Log the specific reason.
                collection_name = f"{safe_prefix}:{collection.get('id')}"
                if not bbox_data:
                    logger.warning(
                        f"Collection {collection_name} is missing spatial.bbox data. Defaulting to global."
                    )
                elif not is_wgs84:
                    logger.warning(
                        f"Collection {collection_name} has a non-WGS84 CRS (Found CRS list: {crs_list}). Defaulting to global."
                    )
                elif bbox is None:
                    logger.warning(
                        f"Collection {collection_name} has WGS84 CRS but unparseable/empty bbox: {bbox_data}. Defaulting to global."
                    )
                elif len(bbox) != 4:
                    logger.warning(
                        f"Collection {collection_name} has WGS84 CRS but bbox is not 4 elements: {bbox}. Defaulting to global."
                    )
                elif None in bbox:
                    logger.warning(
                        f"Collection {collection_name} has WGS84 CRS but bbox contains None values: {bbox}. Defaulting to global."
                    )

                # Add a default global extent
                SubElement(bbox_element, "ows:LowerCorner").text = "-180 -90"
                SubElement(bbox_element, "ows:UpperCorner").text = "180 90"
            # --- END: Robust BBOX Parsing ---

    # Filter Capabilities block
    filter_caps = SubElement(root, "fes:Filter_Capabilities")
    conformance = SubElement(filter_caps, "fes:Conformance")
    SubElement(
        SubElement(conformance, "fes:Constraint", {"name": "ImplementsQuery"}),
        "ows:DefaultValue",
    ).text = "TRUE"
    SubElement(
        SubElement(conformance, "fes:Constraint", {"name": "ImplementsAdHocQuery"}),
        "ows:DefaultValue",
    ).text = "TRUE"
    SubElement(
        SubElement(
            conformance, "fes:Constraint", {"name": "ImplementsMinSpatialFilter"}
        ),
        "ows:DefaultValue",
    ).text = "TRUE"
    SubElement(
        SubElement(conformance, "fes:Constraint", {"name": "ImplementsSpatialFilter"}),
        "ows:DefaultValue",
    ).text = "TRUE"
    SubElement(
        SubElement(
            conformance, "fes:Constraint", {"name": "ImplementsMinTemporalFilter"}
        ),
        "ows:DefaultValue",
    ).text = "TRUE"
    SubElement(
        SubElement(conformance, "fes:Constraint", {"name": "ImplementsTemporalFilter"}),
        "ows:DefaultValue",
    ).text = "TRUE"

    spatial_caps = SubElement(filter_caps, "fes:Spatial_Capabilities")
    geom_operands = SubElement(spatial_caps, "fes:GeometryOperands")
    SubElement(geom_operands, "fes:GeometryOperand", {"name": "gml:Envelope"})
    SubElement(geom_operands, "fes:GeometryOperand", {"name": "gml:Point"})
    SubElement(geom_operands, "fes:GeometryOperand", {"name": "gml:LineString"})
    SubElement(geom_operands, "fes:GeometryOperand", {"name": "gml:Polygon"})

    spatial_operators = SubElement(spatial_caps, "fes:SpatialOperators")
    SubElement(spatial_operators, "fes:SpatialOperator", {"name": "BBOX"})
    SubElement(spatial_operators, "fes:SpatialOperator", {"name": "Intersects"})
    SubElement(spatial_operators, "fes:SpatialOperator", {"name": "Contains"})
    SubElement(spatial_operators, "fes:SpatialOperator", {"name": "Within"})
    SubElement(spatial_operators, "fes:SpatialOperator", {"name": "Equals"})

    return _prettify_xml(root)


def create_describe_feature_type_response(
    schema_prefix: str,
    typename: str,
    target_namespace: str,
    feature_schema: Dict[str, str],
) -> str:
    """
    Generates the WFS DescribeFeatureType XML Schema (XSD) response. This
    provides a formal, machine-readable definition of a feature type's structure.
    """

    safe_prefix = schema_prefix.replace(":", "_").replace("-", "_")

    # The service_url is now the correct full namespace URL.

    root = Element(
        "xs:schema",
        {
            "targetNamespace": target_namespace,
            "xmlns:xs": NS["xs"],
            "xmlns:wfs": NS["wfs"],
            "xmlns:gml": NS["gml"],
            f"xmlns:{safe_prefix}": target_namespace,
            "elementFormDefault": "qualified",
        },
    )
    # Import the GML schema, which provides base types like AbstractFeatureType.
    SubElement(
        root,
        "xs:import",
        {
            "namespace": NS["gml"],
            "schemaLocation": "http://schemas.opengis.net/gml/3.2.1/gml.xsd",
        },
    )

    # Define the complex type for the feature.
    complex_type = SubElement(root, "xs:complexType", {"name": f"{typename}Type"})
    extension = SubElement(
        SubElement(complex_type, "xs:complexContent"),
        "xs:extension",
        {"base": "gml:AbstractFeatureType"},
    )
    sequence = SubElement(extension, "xs:sequence")

    # Add each field from the introspected schema as an element in the sequence.
    for field_name, xsd_type in feature_schema.items():
        if xsd_type == "gml:GeometryPropertyType":  # Geometry is a special case
            SubElement(
                sequence,
                "xs:element",
                {
                    "name": field_name,
                    "type": xsd_type,
                    "minOccurs": "0",
                    "nillable": "true",
                },
            )
        else:
            SubElement(
                sequence,
                "xs:element",
                {
                    "name": field_name,
                    "type": xsd_type,
                    "minOccurs": "0",
                    "nillable": "true",
                },
            )

    # Finally, declare the top-level element for this feature type.

    SubElement(
        root,
        "xs:element",
        {
            "name": typename,
            "type": f"{safe_prefix}:{typename}Type",
            "substitutionGroup": "gml:AbstractFeature",
        },
    )

    return _prettify_xml(root)


def create_hits_response(
    count: int, schema_prefix: str, typename: str, target_namespace: str
) -> str:
    """
    Generates a WFS FeatureCollection response for a RESULTTYPE=hits request.
    This response contains *only* the feature count and no feature members.
    """
    safe_prefix = schema_prefix.replace(":", "_").replace("-", "_")

    register_namespace(safe_prefix, target_namespace)

    # --- FIX: Use Clark notation and let register_namespace handle xmlns ---
    ns_map = {
        # "xmlns:wfs": NS["wfs"], <-- REMOVED
        "timeStamp": datetime.now(timezone.utc).isoformat() + "Z",
        "numberMatched": str(count),
        "numberReturned": "0",  # For 'hits', numberReturned is always 0
        f"{{{NS['xsi']}}}schemaLocation": f"{NS['wfs']} http://schemas.opengis.net/wfs/2.0/wfs.xsd {target_namespace} {target_namespace.rsplit('/', 1)[0]}?service=WFS&version=2.0.0&request=DescribeFeatureType&TYPENAME={safe_prefix}:{typename}",
    }
    # Use Clark notation {uri}tag for the root element
    root = Element(f"{{{NS['wfs']}}}FeatureCollection", ns_map)

    # For 'hits', we MUST NOT include any <wfs:member> elements.

    return _prettify_xml(root)


def create_feature_collection_response(
    features: Iterable[Any],
    schema_prefix: str,
    typename: str,
    target_namespace: str,
    number_matched: int,
    language: str = "en",
    previous_url: Optional[str] = None,
    next_url: Optional[str] = None,
    number_returned: int = 0,
) -> str:
    """
    Generates a GML FeatureCollection from a list of database features.
    This is the primary data response format for WFS GetFeature requests.
    """

    safe_prefix = schema_prefix.replace(":", "_").replace("-", "_")

    # Register the custom prefix for ElementTree to use
    register_namespace(safe_prefix, target_namespace)

    # --- FIX: Use Clark notation and let register_namespace handle xmlns ---
    ns_map = {
        # "xmlns:wfs": NS["wfs"], <-- REMOVED
        # f"xmlns:{safe_prefix}": target_namespace, <-- REMOVED
        "timeStamp": datetime.now(timezone.utc).isoformat() + "Z",
        "numberMatched": str(number_matched),
        "numberReturned": str(number_returned),
        # The DescribeFeatureType URL must be the *root* WFS URL, which is the namespace minus the schema_prefix
        f"{{{NS['xsi']}}}schemaLocation": f"{NS['wfs']} http://schemas.opengis.net/wfs/2.0/wfs.xsd "
        f"{target_namespace} {target_namespace.rsplit('/', 1)[0]}?service=WFS&version=2.0.0&request=DescribeFeatureType&TYPENAME={safe_prefix}:{typename}",
    }
    # Use Clark notation {uri}tag for the root element
    root = Element(f"{{{NS['wfs']}}}FeatureCollection", ns_map)

    # Create a single wfs:additionalObjects element to hold pagination links
    additional_objects_elem = None
    # Add pagination links if provided
    if previous_url:
        if additional_objects_elem is None:
            # Use Clark notation
            additional_objects_elem = SubElement(
                root, f"{{{NS['wfs']}}}additionalObjects"
            )
        # Use Clark notation
        SubElement(
            additional_objects_elem,
            f"{{{NS['wfs']}}}SimpleFeatureCollection",
            {f"{{{NS['wfs']}}}previous": previous_url},
        )
    if next_url:
        if additional_objects_elem is None:
            # Use Clark notation
            additional_objects_elem = SubElement(
                root, f"{{{NS['wfs']}}}additionalObjects"
            )
        # Use Clark notation
        SubElement(
            additional_objects_elem,
            f"{{{NS['wfs']}}}SimpleFeatureCollection",
            {f"{{{NS['wfs']}}}next": next_url},
        )

    for feature in features:
        # Use Clark notation
        member = SubElement(root, f"{{{NS['wfs']}}}member")

        # Use Clark notation for the feature type tag
        ft_tag = f"{{{target_namespace}}}{typename}"

        # Get the ID (fallback from properties if not on the object)
        feat_id = getattr(feature, "geoid", None)
        if feat_id is None and hasattr(feature, "properties"):
             feat_id = feature.properties.get("geoid")
        if feat_id is None and hasattr(feature, "id"):
             feat_id = feature.id

        # This tells ElementTree "this attribute is in the GML namespace"
        # without triggering a duplicate xmlns:gml definition.
        ft_element = SubElement(
            member, ft_tag, {f"{{{NS['gml']}}}id": f"{typename}.{feat_id or ''}"}
        )

        # Inject the raw GML geometry fragment retrieved from PostGIS (using ST_AsGML).
        gml_geom_str = getattr(feature, "geom_gml", None)
        if gml_geom_str:
            try:
                # 1. The string from the DB might be HTML-escaped. Unescape it.
                gml_geom_str = html.unescape(gml_geom_str)
                # 2. The fragment is wrapped to provide namespace context during parsing.
                #    We must register the 'gml' prefix for the parser here!
                gml_fragment = fromstring(
                    f'<root xmlns:gml="{NS["gml"]}">{gml_geom_str}</root>'
                )[0]

                # 3. Create the <...:geom> property element that will contain the geometry.

                # Use Clark notation
                geom_tag = f"{{{target_namespace}}}geom"
                geom_property_element = SubElement(ft_element, geom_tag)

                # 4. Append the parsed geometry fragment into our new property element.
                geom_property_element.append(gml_fragment)

            except Exception as e:
                logger.error(
                    f"Failed to parse GML geometry fragment for feature {feat_id}: {e}. Fragment: {gml_geom_str}"
                )

        # Combine fixed fields and dynamic attributes into a single dictionary for rendering.
        from ...tools.features import FeatureProperties

        attributes_to_render = feature.model_dump(
            exclude={"geom_gml", "geom", "properties"}, exclude_none=True
        )
        for field_name in FeatureProperties.model_fields:
            if hasattr(feature, field_name):
                val = getattr(feature, field_name)
                if val is not None:
                    # Format datetimes to ISO standard for XML
                    if isinstance(val, datetime):
                        attributes_to_render[field_name] = val.isoformat()
                    else:
                        attributes_to_render[field_name] = val

        if isinstance(feature.properties, dict):
            attributes_to_render.update(feature.properties)
        elif hasattr(feature.properties, "model_dump"):
            attributes_to_render.update(feature.properties.model_dump(exclude_none=True))

        for key, value in attributes_to_render.items():
            # Explicitly skip the raw GML field and the geometry property name to avoid duplicating it as a text property.
            if key not in ("geom_gml", "geom"):
                # Use Clark notation
                attr_tag = f"{{{target_namespace}}}{key}"
                # Ensure value is a string for XML serialization
                if value is not None:
                    SubElement(ft_element, attr_tag).text = str(value)
                else:
                    # Handle None values by creating an empty element,
                    # or you could add xsi:nil="true" if the schema supports it.
                    SubElement(ft_element, attr_tag)

    return _prettify_xml(root)
