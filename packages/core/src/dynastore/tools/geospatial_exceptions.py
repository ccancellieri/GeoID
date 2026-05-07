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

class GeometryProcessingError(Exception):
    """Custom exception for geometry processing failures."""

    pass


class InvalidWKBError(GeometryProcessingError):
    """Raised when a WKB string cannot be parsed."""

    pass


class InvalidGeometryError(GeometryProcessingError):
    """Raised when a geometry fails OGC validation rules."""

    pass


class UnfixableGeometryError(GeometryProcessingError):
    """Raised when a geometry is still invalid after a fix attempt."""

    pass


class DisallowedGeometryTypeError(GeometryProcessingError):
    """Raised when a geometry's type is not in the allowed list."""

    pass


class SridMismatchError(GeometryProcessingError):
    """Raised when a geometry's SRID does not match the expected SRID."""

    pass
