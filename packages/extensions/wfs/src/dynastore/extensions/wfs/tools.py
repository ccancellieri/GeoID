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
from typing import Dict, List, Any, Tuple
from sqlalchemy.dialects.postgresql import UUID, JSONB
from sqlalchemy import (
    String, Text, BigInteger, Integer, Boolean, TIMESTAMP, Float, Numeric, Date
)
from geoalchemy2 import Geometry
import json # Import json for GeoJSON processing

logger = logging.getLogger(__name__)


def get_xsd_type_from_sa_type(sa_type: Any) -> str:
    """
    Maps SQLAlchemy types to their corresponding XML Schema (XSD) type.
    This is used for generating the DescribeFeatureType response for fixed table columns.
    """
    if isinstance(sa_type, (UUID, String, Text)):
        return "xs:string"
    if isinstance(sa_type, (BigInteger, Integer)):
        return "xs:long"
    if isinstance(sa_type, Boolean):
        return "xs:boolean"
    if isinstance(sa_type, TIMESTAMP):
        return "xs:dateTime"
    if isinstance(sa_type, (Float, Numeric)):
        return "xs:double"
    if isinstance(sa_type, Date):
        return "xs:date"
    if isinstance(sa_type, Geometry):
        return "gml:GeometryPropertyType"
    if isinstance(sa_type, JSONB):
         # The contents of JSONB are handled dynamically, not as a single column.
        return "xs:string"

    logger.warning(f"No direct XSD mapping for SQLAlchemy type '{type(sa_type)}'. Using xs:string.")
    return "xs:string"


def get_xsd_type_from_python_type(py_type: type) -> str:
    """
    Maps Python types (from JSONB introspection) to their corresponding XSD types.
    This is used for generating the DescribeFeatureType response for dynamic attributes.
    """
    if py_type is str:
        return "xs:string"
    if py_type is int:
        return "xs:integer"
    if py_type is float:
        return "xs:double"
    if py_type is bool:
        return "xs:boolean"
    if py_type is list:
        return "xs:string" 
    if py_type is dict:
        return "xs:string" 

    logger.warning(f"No direct XSD mapping for Python type '{py_type}'. Using xs:string.")
    return "xs:string"


def get_xsd_type_from_attribute_schema_type(schema_type: str) -> str:
    """Maps our internal schema type strings to XSD types."""
    type_map = {
        "string": "xs:string",
        "number": "xs:double", # Use double for generic numbers
        "integer": "xs:int",
        "boolean": "xs:boolean",
        "date": "xs:date",
        "datetime": "xs:dateTime",
        "array": "xs:anyType", # Generic for arrays, could be more specific if needed
        "object": "xs:anyType", # Generic for objects, could be more specific if needed
    }
    return type_map.get(schema_type, "xs:string") # Default to string if unknown
