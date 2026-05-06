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

import random
import uuid
import logging
from osgeo import gdal, ogr, osr, __version__ as gdal_version
import xml.etree.ElementTree as ET
from typing import List, Dict, Any, Optional
from collections import defaultdict

logger = logging.getLogger(__name__)

# --- Helper Functions ---


def _hex_to_rgba(hex_color: str, opacity: float = 1.0) -> list[int]:
    hex_color = hex_color.lstrip("#")
    if len(hex_color) == 6:
        r, g, b = (int(hex_color[i : i + 2], 16) for i in (0, 2, 4))
        return [r, g, b, int(opacity * 255)]
    return [0, 0, 0, 255]


def _export_dataset_to_png_bytes(dataset: gdal.Dataset) -> bytes:
    from dynastore.tools.identifiers import generate_uuidv7

    mem_path = f"/vsimem/{generate_uuidv7()}.png"
    png_driver = gdal.GetDriverByName("PNG")
    try:
        png_driver.CreateCopy(mem_path, dataset, 0)
        f = gdal.VSIFOpenL(mem_path, "rb")
        gdal.VSIFSeekL(f, 0, 2)
        size = gdal.VSIFTellL(f)
        gdal.VSIFSeekL(f, 0, 0)
        return bytes(gdal.VSIFReadL(1, size, f))
    finally:
        if "f" in locals() and f:
            gdal.VSIFCloseL(f)
        gdal.Unlink(mem_path)


# --- High-Performance Default Renderer ---


def _render_default_style(
    mem_dataset: gdal.Dataset, geoms: List[ogr.Geometry], srs: osr.SpatialReference
):
    """
    Renders geometries with random fills and a black stroke using the compatible
    RasterizeLayer method.
    """
    if not geoms:
        return

    mem_driver = ogr.GetDriverByName("MEM")
    all_boundaries = []

    # Pass 1: Render fills feature-by-feature to get random colors
    for geom in geoms:
        # Create a temporary source for this single feature
        temp_fill_source = mem_driver.CreateDataSource("")
        temp_fill_layer = temp_fill_source.CreateLayer(
            "feature", srs=srs, geom_type=geom.GetGeometryType()
        )

        feature = ogr.Feature(temp_fill_layer.GetLayerDefn())
        feature.SetGeometry(geom)
        temp_fill_layer.CreateFeature(feature)

        r, g, b = (random.randint(50, 200) for _ in range(3))
        fill_rgba = [r, g, b, int(0.5 * 255)]  # 50% opacity
        gdal.RasterizeLayer(
            mem_dataset, [1, 2, 3, 4], temp_fill_layer, burn_values=fill_rgba
        )

        temp_fill_source.Destroy()

        # Collect the boundary for the batched stroke pass
        boundary = geom.GetBoundary()
        if boundary and not boundary.IsEmpty():
            all_boundaries.append(boundary)

    # Pass 2: Render all strokes at once for performance
    if all_boundaries:
        stroke_source = mem_driver.CreateDataSource("")
        stroke_layer = stroke_source.CreateLayer(
            "boundaries", srs=srs, geom_type=ogr.wkbLineString
        )

        for boundary in all_boundaries:
            stroke_feature = ogr.Feature(stroke_layer.GetLayerDefn())
            stroke_feature.SetGeometry(boundary)
            stroke_layer.CreateFeature(stroke_feature)

        stroke_rgba = [0, 0, 0, 255]  # Opaque black
        gdal.RasterizeLayer(
            mem_dataset, [1, 2, 3, 4], stroke_layer, burn_values=stroke_rgba
        )
        stroke_source.Destroy()


# --- Custom Style Renderer ---


def _render_sld_style(
    mem_dataset: gdal.Dataset, ogr_layer: ogr.Layer, style_content: Dict[str, Any]
):
    """
    Parses and applies styling rules from an SLD 1.1 XML string.
    """
    sld_body = style_content.get("sld_body")
    if not isinstance(sld_body, str):
        raise ValueError("SLD content must be a string in the 'sld_body' key.")

    try:
        root = ET.fromstring(sld_body)
    except ET.ParseError as e:
        logger.error(f"Failed to parse SLD XML: {e}")
        raise ValueError("Invalid SLD XML content.")

    ns = {"sld": "http://www.opengis.net/sld", "ogc": "http://www.opengis.net/ogc"}

    # Find the style rules for the current layer
    named_layer = root.find(f".//sld:NamedLayer[sld:Name='{ogr_layer.GetName()}']", ns)
    if named_layer is None:
        logger.warning(
            f"No <NamedLayer> found for '{ogr_layer.GetName()}' in the provided SLD style."
        )
        return

    for rule in named_layer.findall(".//sld:Rule", ns):
        # This is a simplified filter translation. A full implementation
        # would require a complete OGC Filter to SQL WHERE clause translator.
        filter_element = rule.find("ogc:Filter", ns)
        if filter_element is not None:
            prop_equal = filter_element.find("ogc:PropertyIsEqualTo", ns)
            if prop_equal is not None:
                name_el = prop_equal.find("ogc:PropertyName", ns)
                literal_el = prop_equal.find("ogc:Literal", ns)
                prop_name = name_el.text if name_el is not None else None
                prop_literal = literal_el.text if literal_el is not None else None
                if prop_name and prop_literal:
                    ogr_layer.SetAttributeFilter(f"{prop_name} = '{prop_literal}'")
            else:
                ogr_layer.SetAttributeFilter(
                    None
                )  # Apply to all if filter is not PropertyIsEqualTo
        else:
            ogr_layer.SetAttributeFilter(None)  # Apply to all if no filter

        # Find and apply PolygonSymbolizer
        for poly_symbolizer in rule.findall(".//sld:PolygonSymbolizer", ns):
            fill = poly_symbolizer.find("sld:Fill", ns)
            if fill is not None:
                color_param = fill.find("sld:CssParameter[@name='fill']", ns)
                opacity_param = fill.find("sld:CssParameter[@name='fill-opacity']", ns)
                if color_param is not None and opacity_param is not None:
                    color_text = color_param.text
                    opacity_text = opacity_param.text
                    if color_text and opacity_text:
                        fill_rgba = _hex_to_rgba(color_text, float(opacity_text))
                        gdal.RasterizeLayer(
                            mem_dataset, [1, 2, 3, 4], ogr_layer, burn_values=fill_rgba
                        )

            stroke = poly_symbolizer.find("sld:Stroke", ns)
            if stroke is not None:
                color_param = stroke.find("sld:CssParameter[@name='stroke']", ns)
                opacity_param = stroke.find(
                    "sld:CssParameter[@name='stroke-opacity']", ns
                )
                if color_param is not None:
                    color_text = color_param.text
                    opacity_text = (
                        opacity_param.text if opacity_param is not None else None
                    )
                    opacity = float(opacity_text) if opacity_text else 1.0
                    if color_text:
                        stroke_rgba = _hex_to_rgba(color_text, opacity)
                        gdal.RasterizeLayer(
                            mem_dataset,
                            [1, 2, 3, 4],
                            ogr_layer,
                            burn_values=stroke_rgba,
                            options=["ALL_TOUCHED=TRUE"],
                        )


def _mapbox_gl_filter_to_ogr_sql(gl_filter: List[Any]) -> Optional[str]:
    """
    Converts a simplified Mapbox GL filter expression to an OGR SQL WHERE clause.
    Supports only simple '==' and '!=' comparisons for now.
    """
    if not gl_filter or not isinstance(gl_filter, list):
        return None

    operator = gl_filter[0]
    if operator in ["==", "!="]:
        if (
            len(gl_filter) != 3
            or not isinstance(gl_filter[1], list)
            or gl_filter[1][0] != "get"
        ):
            logger.warning(f"Unsupported Mapbox GL filter structure: {gl_filter}")
            return None

        property_name = gl_filter[1][1]
        value = gl_filter[2]

        sql_op = "=" if operator == "==" else "!="
        # OGR SQL expects single quotes for string literals
        return f"\"{property_name}\" {sql_op} '{value}'"

    logger.warning(
        f"Unsupported Mapbox GL filter operator: {operator}. Only '==' and '!=' are supported for now."
    )
    return None


def _render_mapbox_gl_style(
    mem_dataset: gdal.Dataset, ogr_layer: ogr.Layer, style_content: Dict[str, Any]
):
    """
    Parses and applies styling rules from a Mapbox GL JSON style.
    This is a simplified implementation supporting basic fill and stroke for polygons.
    """
    mapbox_gl_layers = style_content.get("layers", [])

    for gl_layer in mapbox_gl_layers:
        # Check if this Mapbox GL layer applies to the current OGR layer
        # We assume 'source-layer' in Mapbox GL corresponds to our OGR layer name.
        # A more robust solution might check 'source' and then 'source-layer'.
        if gl_layer.get("source-layer") != ogr_layer.GetName():
            continue

        # Apply filter if present
        ogr_layer.SetAttributeFilter(None)  # Reset filter for each GL layer
        gl_filter = gl_layer.get("filter")
        if gl_filter:
            ogr_sql_filter = _mapbox_gl_filter_to_ogr_sql(gl_filter)
            if ogr_sql_filter:
                ogr_layer.SetAttributeFilter(ogr_sql_filter)
            else:
                logger.warning(
                    f"Skipping filter for Mapbox GL layer '{gl_layer.get('id')}' due to unsupported filter format."
                )

        layer_type = gl_layer.get("type")
        paint_props = gl_layer.get("paint", {})

        if layer_type == "fill":
            fill_color = paint_props.get("fill-color", "#808080")
            fill_opacity = paint_props.get("fill-opacity", 0.5)
            fill_rgba = _hex_to_rgba(fill_color, fill_opacity)
            gdal.RasterizeLayer(
                mem_dataset, [1, 2, 3, 4], ogr_layer, burn_values=fill_rgba
            )

            # Mapbox GL also has 'fill-outline-color' for polygon outlines
            outline_color = paint_props.get("fill-outline-color")
            if outline_color:
                line_opacity = paint_props.get(
                    "fill-opacity", 1.0
                )  # Often same as fill opacity for outline
                line_rgba = _hex_to_rgba(outline_color, line_opacity)

                # Extract boundaries and rasterize as lines
                stroke_source = ogr.GetDriverByName("MEM").CreateDataSource("")
                stroke_layer = stroke_source.CreateLayer(
                    "boundaries",
                    srs=ogr_layer.GetSpatialRef(),
                    geom_type=ogr.wkbLineString,
                )
                ogr_layer.ResetReading()  # Reset to iterate all features (even filtered ones if filter was not applied)
                for feature in ogr_layer:
                    boundary = feature.GetGeometryRef().GetBoundary()
                    if boundary:
                        stroke_feature = ogr.Feature(stroke_layer.GetLayerDefn())
                        stroke_feature.SetGeometry(boundary)
                        stroke_layer.CreateFeature(stroke_feature)

                gdal.RasterizeLayer(
                    mem_dataset, [1, 2, 3, 4], stroke_layer, burn_values=line_rgba
                )
                stroke_source.Destroy()

        elif layer_type == "line":
            line_color = paint_props.get("line-color", "#000000")
            line_opacity = paint_props.get("line-opacity", 1.0)
            line_rgba = _hex_to_rgba(line_color, line_opacity)

            # For line layers, we rasterize the layer directly
            gdal.RasterizeLayer(
                mem_dataset,
                [1, 2, 3, 4],
                ogr_layer,
                burn_values=line_rgba,
                options=["ALL_TOUCHED=TRUE"],
            )

        # Reset attribute filter for the next Mapbox GL layer or next OGR layer
        ogr_layer.SetAttributeFilter(None)


def _render_custom_style(
    mem_dataset: gdal.Dataset, ogr_layer: ogr.Layer, style_content: Dict[str, Any]
):
    """Applies styling rules from a style object to a given OGR layer."""
    layer_style_config = style_content.get(
        ogr_layer.GetName(), style_content.get("default", {})
    )
    for rule in layer_style_config.get("rules", []):
        attr_filter = rule.get("filter")
        if attr_filter and attr_filter != "default":
            ogr_layer.SetAttributeFilter(attr_filter)
        else:
            ogr_layer.SetAttributeFilter(None)

        fill_symbolizer = next(
            (s for s in rule.get("symbolizers", []) if s.get("type") == "fill"), None
        )
        if fill_symbolizer:
            fc, fo = (
                fill_symbolizer.get("color", "#808080"),
                fill_symbolizer.get("opacity", 0.5),
            )
            fill_rgba = _hex_to_rgba(fc, fo)
            gdal.RasterizeLayer(
                mem_dataset, [1, 2, 3, 4], ogr_layer, burn_values=fill_rgba
            )

        stroke_symbolizer = next(
            (s for s in rule.get("symbolizers", []) if s.get("type") == "stroke"), None
        )
        if stroke_symbolizer:
            sc, so = (
                stroke_symbolizer.get("color", "#000000"),
                stroke_symbolizer.get("opacity", 1.0),
            )
            stroke_rgba = _hex_to_rgba(sc, so)

            stroke_source = ogr.GetDriverByName("MEM").CreateDataSource("")
            stroke_layer = stroke_source.CreateLayer(
                "boundaries", srs=ogr_layer.GetSpatialRef(), geom_type=ogr.wkbLineString
            )
            ogr_layer.ResetReading()
            for feature in ogr_layer:
                boundary = feature.GetGeometryRef().GetBoundary()
                if boundary:
                    stroke_feature = ogr.Feature(stroke_layer.GetLayerDefn())
                    stroke_feature.SetGeometry(boundary)
                    stroke_layer.CreateFeature(stroke_feature)

            gdal.RasterizeLayer(
                mem_dataset, [1, 2, 3, 4], stroke_layer, burn_values=stroke_rgba
            )
            stroke_source.Destroy()

    ogr_layer.SetAttributeFilter(None)


# --- Main Entry Point ---


def render_map_image(
    width: int,
    height: int,
    bbox: List[float],
    crs: str,
    source_srid: int,
    layers_data: List[Dict[str, Any]],
    style_record: Optional[Any],
    transparent: bool = True,
    bgcolor: Optional[str] = None,
) -> bytes:
    gdal_major_version = int(gdal_version.split(".")[0])

    mem_dataset = gdal.GetDriverByName("MEM").Create(
        "", width, height, 4, gdal.GDT_Byte
    )

    target_srs = osr.SpatialReference()
    target_srs.SetFromUserInput(crs)
    if gdal_major_version >= 3:
        target_srs.SetAxisMappingStrategy(osr.OAMS_TRADITIONAL_GIS_ORDER)
    mem_dataset.SetProjection(target_srs.ExportToWkt())

    geotransform = (
        bbox[0],
        (bbox[2] - bbox[0]) / width,
        0,
        bbox[3],
        0,
        (bbox[1] - bbox[3]) / height,
    )
    mem_dataset.SetGeoTransform(geotransform)

    if not transparent:
        rgba = _hex_to_rgba(bgcolor) if bgcolor else [255, 255, 255, 255]
        for i, val in enumerate(rgba, 1):
            mem_dataset.GetRasterBand(i).Fill(val)

    source_srs = osr.SpatialReference()
    source_srs.ImportFromEPSG(int(source_srid))
    if gdal_major_version >= 3:
        source_srs.SetAxisMappingStrategy(osr.OAMS_TRADITIONAL_GIS_ORDER)

    coord_transform = (
        osr.CoordinateTransformation(source_srs, target_srs)
        if not target_srs.IsSame(source_srs)
        else None
    )

    if style_record is None:
        all_geoms = []
        for item in layers_data:
            geom = ogr.CreateGeometryFromWkb(item["geom"])
            if geom:
                if coord_transform:
                    geom.Transform(coord_transform)
                all_geoms.append(geom)
        _render_default_style(mem_dataset, all_geoms, target_srs)
    else:
        # --- Style Dispatch Logic ---
        style_content = (
            style_record.content.model_dump()
        )  # Get the content dict (SLDContent or MapboxContent)
        style_format = style_record.content.format.value  # Get the format enum value

        grouped_features = defaultdict(list)
        for feature in layers_data:
            grouped_features[feature["layer"]].append(feature)

        for layer_name, features in grouped_features.items():
            if not features:
                continue

            layer_source = ogr.GetDriverByName("MEM").CreateDataSource("")
            ogr_layer = layer_source.CreateLayer(
                layer_name, srs=target_srs, geom_type=ogr.wkbUnknown
            )

            # Add fields to the layer from the first feature's attributes
            if features and "attributes" in features[0] and features[0]["attributes"]:
                for key in features[0]["attributes"].keys():
                    ogr_layer.CreateField(ogr.FieldDefn(key, ogr.OFTString))

            for item in features:
                geom = ogr.CreateGeometryFromWkb(item["geom"])
                if geom:
                    if coord_transform:
                        geom.Transform(coord_transform)
                    feature = ogr.Feature(ogr_layer.GetLayerDefn())
                    feature.SetGeometry(geom)

                    # Set attribute values for the feature
                    if "attributes" in item and item["attributes"]:
                        for key, value in item["attributes"].items():
                            feature.SetField(key, str(value))

                    ogr_layer.CreateFeature(feature)

            # Dispatch based on style format
            if style_format == "SLD_1.1":
                _render_sld_style(mem_dataset, ogr_layer, style_content)
            elif style_format == "MapboxGL":  # Use the enum value directly
                _render_mapbox_gl_style(mem_dataset, ogr_layer, style_content)
            else:
                logger.warning(
                    f"Unsupported style format '{style_format}'. Falling back to default renderer."
                )
                all_geoms_from_layer = []
                ogr_layer.ResetReading()
                for feature in ogr_layer:
                    all_geoms_from_layer.append(feature.GetGeometryRef())
                _render_default_style(mem_dataset, all_geoms_from_layer, target_srs)
            layer_source.Destroy()

    return _export_dataset_to_png_bytes(mem_dataset)
