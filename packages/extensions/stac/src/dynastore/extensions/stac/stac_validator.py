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

# dynastore/extensions/stac/stac_validator.py

"""
Driver-agnostic STAC validation at write time.

Uses pystac's ``JsonSchemaSTACValidator`` for core + extension schema validation.
Falls back to stac-pydantic's ``validate_extensions()`` when available.
This module is called from the service layer before persisting to any storage driver
(PostgreSQL sidecar pipeline or Elasticsearch/SFEOS).
"""

import json
import logging
from typing import Any, Dict, List, Optional

import pystac
from pydantic.networks import _BaseUrl, _BaseMultiHostUrl

from dynastore.models.localization import _is_valid_lang_key

logger = logging.getLogger(__name__)


def _coerce_for_stac_validation(value: Any, lang: str = "en") -> Any:
    """Recursively coerce an internal payload into the shape STAC validators expect.

    Two transforms:

    * **Internationalized dicts** — values like ``{"en": "ESA", "fr": "ASE"}``
      (where every key is a valid language code) are flattened to a single
      string using *lang* with an ``en`` / first-available fallback.  STAC
      core fields (``links[].title``, ``providers[].name``, ``license``, …)
      are scalars, so the i18n shape trips both pystac and stac-pydantic.
    * **datetime / date** — pystac calls ``len()`` on temporal extents and
      assumes ISO strings.  Internal payloads coming from ``model_dump()``
      still hold ``datetime`` objects.

    Containers are walked recursively; everything else is returned as-is.
    """
    if isinstance(value, dict):
        if value and all(
            isinstance(k, str) and _is_valid_lang_key(k) for k in value.keys()
        ):
            picked = (
                value.get(lang)
                or value.get("en")
                or next(iter(value.values()), None)
            )
            return _coerce_for_stac_validation(picked, lang)
        return {k: _coerce_for_stac_validation(v, lang) for k, v in value.items()}
    if isinstance(value, list):
        return [_coerce_for_stac_validation(v, lang) for v in value]
    if hasattr(value, "isoformat"):
        return value.isoformat()
    # pydantic v2 Url types (HttpUrl, AnyUrl, …) aren't JSON-serialisable.
    if isinstance(value, (_BaseUrl, _BaseMultiHostUrl)):
        return str(value)
    return value

# --- Capability detection ---

try:
    from pystac.validation import JsonSchemaSTACValidator

    _PYSTAC_VALIDATOR = JsonSchemaSTACValidator()
    PYSTAC_VALIDATION_AVAILABLE = True
except ImportError:
    _PYSTAC_VALIDATOR = None
    PYSTAC_VALIDATION_AVAILABLE = False

try:
    from stac_pydantic.item import Item as StacPydanticItem
    from stac_pydantic.collection import Collection as StacPydanticCollection
    from stac_pydantic.extensions import validate_extensions as _sp_validate_extensions

    STAC_PYDANTIC_AVAILABLE = True
except ImportError:
    STAC_PYDANTIC_AVAILABLE = False


class STACValidationError(Exception):
    """Raised when a STAC document fails validation."""

    def __init__(self, message: str, errors: Optional[List[str]] = None):
        self.errors = errors or []
        super().__init__(message)


def validate_stac_item(
    item_dict: Dict[str, Any],
    *,
    strict: bool = False,
) -> List[str]:
    """
    Validate a STAC Item dictionary against core spec and declared extensions.

    Parameters
    ----------
    item_dict : dict
        A complete STAC Item as a JSON-serialisable dict.
    strict : bool
        If ``True``, raise :class:`STACValidationError` on failure.
        If ``False`` (default), return a list of warning strings.

    Returns
    -------
    list[str]
        Empty list on success; list of validation messages otherwise.
    """
    warnings: List[str] = []
    coerced = _coerce_for_stac_validation(item_dict)

    # 1. pystac core + extension JSON-Schema validation
    if PYSTAC_VALIDATION_AVAILABLE and _PYSTAC_VALIDATOR is not None:
        try:
            stac_item = pystac.Item.from_dict(coerced)
            stac_item.validate()
        except Exception as exc:
            msg = f"pystac validation: {exc}"
            if strict:
                raise STACValidationError(msg, errors=[msg]) from exc
            warnings.append(msg)
            logger.warning(msg)

    # 2. stac-pydantic model + extension-schema validation (when available)
    if STAC_PYDANTIC_AVAILABLE:
        try:
            from dynastore.tools.json import CustomJSONEncoder
            json_safe = json.loads(json.dumps(coerced, cls=CustomJSONEncoder))
            sp_item = StacPydanticItem(**json_safe)
            _sp_validate_extensions(sp_item)
        except Exception as exc:
            msg = f"stac-pydantic validation: {exc}"
            if strict:
                raise STACValidationError(msg, errors=[msg]) from exc
            warnings.append(msg)
            logger.warning(msg)

    return warnings


def validate_stac_collection(
    collection_dict: Dict[str, Any],
    *,
    strict: bool = False,
) -> List[str]:
    """
    Validate a STAC Collection dictionary against core spec and declared extensions.

    Parameters
    ----------
    collection_dict : dict
        A complete STAC Collection as a JSON-serialisable dict.
    strict : bool
        If ``True``, raise :class:`STACValidationError` on failure.
        If ``False`` (default), return a list of warning strings.

    Returns
    -------
    list[str]
        Empty list on success; list of validation messages otherwise.
    """
    warnings: List[str] = []
    coerced = _coerce_for_stac_validation(collection_dict)

    # 1. pystac core + extension JSON-Schema validation
    if PYSTAC_VALIDATION_AVAILABLE and _PYSTAC_VALIDATOR is not None:
        try:
            stac_coll = pystac.Collection.from_dict(coerced)
            stac_coll.validate()
        except Exception as exc:
            msg = f"pystac validation: {exc}"
            if strict:
                raise STACValidationError(msg, errors=[msg]) from exc
            warnings.append(msg)
            logger.warning(msg)

    # 2. stac-pydantic model + extension-schema validation (when available)
    if STAC_PYDANTIC_AVAILABLE:
        try:
            from dynastore.tools.json import CustomJSONEncoder
            json_safe = json.loads(json.dumps(coerced, cls=CustomJSONEncoder))
            sp_coll = StacPydanticCollection(**json_safe)
            _sp_validate_extensions(sp_coll)
        except Exception as exc:
            msg = f"stac-pydantic validation: {exc}"
            if strict:
                raise STACValidationError(msg, errors=[msg]) from exc
            warnings.append(msg)
            logger.warning(msg)

    return warnings


# --- Exception handler (registered by STACService at construction) ---

from fastapi import HTTPException, status  # noqa: E402

from dynastore.extensions.tools.exception_handlers import ExceptionHandler  # noqa: E402


class STACValidationExceptionHandler(ExceptionHandler):
    """Maps STACValidationError → 422 with a structured detail."""

    def can_handle(self, exception: Exception) -> bool:
        return isinstance(exception, STACValidationError)

    def handle(
        self, exception: Exception, context: Optional[Dict[str, Any]] = None
    ) -> Optional[HTTPException]:
        context = context or {}
        resource_id = context.get("resource_id", "")
        operation = context.get("operation", "Operation")

        errors = getattr(exception, "errors", None) or [str(exception)]
        head = (
            f"{operation} failed STAC validation for '{resource_id}'"
            if resource_id
            else f"{operation} failed STAC validation"
        )
        detail = head + ": " + "; ".join(errors)
        logger.warning(detail)
        return HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail=detail
        )
