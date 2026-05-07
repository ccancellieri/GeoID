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

from enum import Enum
from datetime import datetime, timezone
from typing import (
    Any,
    Dict,
    List,
    Optional,
    Tuple,
    TypeVar,
    Generic,
    Union,
    Set,
    TYPE_CHECKING,
    ClassVar,
    Protocol,
    runtime_checkable,
)
from pydantic import BaseModel, Field, HttpUrl, ConfigDict, model_validator
from typing_extensions import Self


# STAC Language Extension Schema URI
STAC_LANGUAGE_EXTENSION_URI = "https://stac-extensions.github.io/language/v1.0.0/schema.json"

class Language(str, Enum):
    """ISO 639-1 language codes for supported languages."""
    EN = "en"
    FR = "fr"
    ES = "es"
    ZH = "zh"
    RU = "ru"
    AR = "ar"
    IT = "it"
    DE = "de"


class LanguageObject(BaseModel):
    """
    STAC Language Object structure.
    """
    code: str = Field(..., description="The RFC 5646 language tag.")
    name: str = Field(..., description="The name of the language in the language itself.")
    alternate: Optional[str] = Field(None, description="The name of the language in English.")
    dir: Optional[str] = Field("ltr", description="The direction of the text, 'ltr' or 'rtl'.")

    model_config = ConfigDict(extra='allow')


# Static registry of language metadata for auto-generation
_LANGUAGE_METADATA: Dict[str, Dict[str, str]] = {
    "en": {"name": "English", "alternate": "English", "dir": "ltr"},
    "fr": {"name": "Français", "alternate": "French", "dir": "ltr"},
    "es": {"name": "Español", "alternate": "Spanish", "dir": "ltr"},
    "zh": {"name": "中文", "alternate": "Chinese", "dir": "ltr"},
    "ru": {"name": "Русский", "alternate": "Russian", "dir": "ltr"},
    "ar": {"name": "عربي", "alternate": "Arabic", "dir": "rtl"},
    "it": {"name": "Italiano", "alternate": "Italian", "dir": "ltr"},
    "de": {"name": "Deutsch", "alternate": "German", "dir": "ltr"},
}

def get_language_object(code: str) -> LanguageObject:
    """Factory to create a LanguageObject from a code with defaults."""
    # Normalize code if needed (e.g. en-US -> en lookup if strict missing)
    base_code = code.split('-')[0]
    meta = _LANGUAGE_METADATA.get(code) or _LANGUAGE_METADATA.get(base_code) or {"name": code, "dir": "ltr"}
    
    return LanguageObject(
        code=code,
        name=meta["name"],
        alternate=meta.get("alternate"),
        dir=meta.get("dir", "ltr")
    )


T = TypeVar("T")

# Pre-declare a lightweight language-key check used by the model_validator
# below.  The public ``is_valid_language_key`` (defined at the bottom of this
# module) delegates to the same regex.
import re as _re
_LANGUAGE_KEY_RE = _re.compile(r"^[a-z]{2}(-[a-zA-Z]{2,8})?$")

def _is_valid_lang_key(key: str) -> bool:
    return bool(_LANGUAGE_KEY_RE.match(key))


class LocalizedDTO(BaseModel, Generic[T]):
    """
    A generic model for localized content.
    Provides a standardized structure for multi-language support.
    """
    model_config = ConfigDict(extra='allow') # Allow extra keys for dynamic languages

    # Define standard keys for type hints, but allow others via extra='allow'
    en: Optional[T] = None
    fr: Optional[T] = None
    es: Optional[T] = None
    zh: Optional[T] = None
    ru: Optional[T] = None
    ar: Optional[T] = None
    it: Optional[T] = None
    de: Optional[T] = None

    @model_validator(mode="after")
    def _validate_language_keys(self) -> Self:
        """Reject extra keys that do not look like ISO 639-1 language codes."""
        if self.model_extra:
            _known = {"en", "fr", "es", "zh", "ru", "ar", "it", "de"}
            bad = [
                k for k in self.model_extra
                if k not in _known and not _is_valid_lang_key(k)
            ]
            if bad:
                raise ValueError(
                    f"Invalid language key(s): {bad}. "
                    "Keys must be ISO 639-1 codes (e.g. 'en', 'pt-BR')."
                )
        return self

    def merge_updates(self, updates: Union[T, Dict[str, T]], lang: str) -> 'LocalizedDTO[T]':
        """
        Merges new language values into this DTO, returning a new instance.
        
        If lang is '*', updates MUST be a dict. This dict replaces the entire content 
        of the localized object, effectively allowing removal of languages not present 
        in the update.
        
        If lang is a specific code, the value is updated/added for that code only, 
        preserving other languages.
        """
        if lang == "*" and isinstance(updates, dict):
            # Full replacement logic for '*'
            # We create a new instance strictly from the updates, discarding current state.
            return self.__class__.model_validate(updates)
        
        # Partial update logic for specific language
        merged_data = self.model_dump(exclude_none=True)
        
        if isinstance(updates, dict):
            # If updates is a dict but lang is specific (e.g. lang='en'), 
            # we assume the user meant to update 'en' with the value inside the dict 
            # if the dict has no language keys, OR if the dict is the value itself.
            # But normally if lang='en', updates should be T.
            # If updates is passed as dict but matches structure of T (e.g. LicenseContent), treat as T.
            
            # Simple case: updates IS the value T
            update_payload = {lang: updates}
        else:
            # Scalar value update
            update_payload = {lang: updates}
            
        merged_data.update(update_payload)
        return self.__class__.model_validate(merged_data)

    def get_available_languages(self) -> Set[str]:
        """Returns a set of language codes that have values."""
        return {k for k, v in self.model_dump(exclude_none=True).items() if v is not None}

    def resolve(self, lang: str, default: str = "en", include_language_keys: bool = False) -> Union[Optional[T], Dict[str, T]]:
        """
        Resolves the value for the given language.
        
        If lang is '*', returns the full dictionary of all available translations.
        Otherwise, returns the specific value for the requested language, 
        falling back to default or first available.
        
        If include_language_keys is True and lang is not '*', the value is returned 
        wrapped in a dictionary with its language code as the key.
        """
        data = self.model_dump(exclude_none=True)
        if not data:
            return None
        
        # 0. Wildcard: return full dictionary
        if lang == '*':
            return data

        # Select the best matching language and value
        resolved_key = None
        resolved_val = None

        # 1. Try exact match
        if lang in data:
            resolved_key = lang
            resolved_val = data[lang]
        else:
            # 2. Try base language (e.g. 'en' for 'en-US')
            base_lang = lang.split('-')[0]
            if base_lang in data:
                resolved_key = base_lang
                resolved_val = data[base_lang]
            # 3. Try default
            elif default in data:
                resolved_key = default
                resolved_val = data[default]
            # 4. Return first available
            else:
                resolved_key = next(iter(data.keys()))
                resolved_val = data[resolved_key]
        
        if include_language_keys:
            return {resolved_key: resolved_val}
        return resolved_val


class LocalizedText(LocalizedDTO[str]):
    """Localized string content."""
    model_config = ConfigDict(json_schema_extra={"example": {Language.EN.value: ""}})

    @classmethod
    def delocalize_input(cls, value: Any, lang: str) -> Dict[str, Any]:
        """Wraps a simple string in a language-keyed dict."""
        if isinstance(value, str):
            if lang == '*':
                 # If lang is *, input MUST be a dict. If string provided, assume default 'en'?
                 # Or raise error. Let's assume 'en' fallback for robustness.
                 return {"en": value}
            return {lang: value}
        return value


class LocalizedKeywords(LocalizedDTO[List[str]]):
    """Localized keywords list."""
    model_config = ConfigDict(json_schema_extra={"example": {Language.EN.value: ["keyword1", "keyword2"]}})

    @classmethod
    def delocalize_input(cls, value: Any, lang: str) -> Dict[str, Any]:
        """Wraps a simple list of strings in a language-keyed dict."""
        if isinstance(value, list):
            if lang == '*':
                 return {"en": value}
            return {lang: value}
        return value


class LicenseContent(BaseModel):
    """Properties for a localized license description."""
    name: str = Field(..., description="Localized name of the license")
    url: Optional[HttpUrl] = Field(None, description="Link to the license text")


class LocalizedLicense(LocalizedDTO[LicenseContent]):
    """Localized license information."""
    model_config = ConfigDict(json_schema_extra={
        "example": {
            Language.EN.value: {
                "name": "Creative Commons Attribution 4.0 International",
                "url": "https://creativecommons.org/licenses/by/4.0/"
            }
        }
    })


class LocalizedExtraMetadata(LocalizedDTO[Dict[str, Any]]):
    """Localized extra metadata dictionary for custom fields/extensions."""
    model_config = ConfigDict(json_schema_extra={
        "example": {
            Language.EN.value: {"custom_extension:field": "value"}
        }
    })

    @classmethod
    def delocalize_input(cls, value: Any, lang: str) -> Dict[str, Any]:
        """Wraps a dictionary in a language-keyed dict if it's not already one."""
        if isinstance(value, dict):
            # Check if keys are languages
            if any(k in _LANGUAGE_METADATA for k in value.keys()):
                return value
            
            # If lang is *, and input is NOT a lang-keyed dict (checked above), 
            # we can't safely convert. But we must return dict.
            if lang == '*':
                # Assume raw dict is 'en' content? Or return as is (risky)?
                return {"en": value}
                
            return {lang: value}
        return value


def localize_dict(data: Dict[str, Any], lang: str) -> Tuple[Dict[str, Any], Set[str]]:
    """
    Helper to localize a dictionary where values can be localized objects.
    Detects if values are localized dictionaries and flattens them, 
    returning the localized dictionary and a set of available languages.
    """
    if not data:
        return {}, set()

    result = data.copy()
    available_languages: Set[str] = set()

    for key, value in list(result.items()):
        if isinstance(value, dict) and any(k in _LANGUAGE_METADATA for k in value.keys()):
            available_languages.update(value.keys())
            
            if lang == '*':
                resolved = value
            else:
                resolved = value.get(lang)
                if resolved is None:
                    base = lang.split('-')[0]
                    resolved = value.get(base) or value.get("en") or next(iter(value.values()), None)
            
            result[key] = resolved
    
    return result, available_languages


# === Validation Helpers ===


def is_valid_language_key(key: str) -> bool:
    """Check whether *key* looks like an ISO 639-1 language code.

    Accepts two-letter codes (``en``, ``fr``) and optionally a region or
    script subtag (``en-US``, ``zh-Hans``).  Rejects arbitrary strings
    like ``"hello"`` or ``"custom_field"``.
    """
    return _is_valid_lang_key(key)


def is_multilanguage_input(value: Any) -> bool:
    """
    Detects if a value is a multilanguage dictionary.
    
    Returns True if value is a dict with at least one language code key.
    
    Args:
        value: The value to check
        
    Returns:
        True if value is a multilanguage dictionary, False otherwise
        
    Examples:
        >>> is_multilanguage_input({"en": "Hello", "fr": "Bonjour"})
        True
        >>> is_multilanguage_input("Hello")
        False
        >>> is_multilanguage_input({"custom_field": "value"})
        False
    """
    if not isinstance(value, dict):
        return False
    
    # If ANY key is a known language code, treat as multilanguage
    return any(k in _LANGUAGE_METADATA for k in value.keys())


def validate_language_consistency(data: Dict[str, Any], lang: str) -> None:
    """
    Validates that multilanguage input is consistent with lang parameter.
    
    This function prevents conflicting scenarios where a user provides
    multilanguage dictionaries (e.g., {"it": "..."}) with a specific
    language parameter (e.g., lang="en").
    
    Args:
        data: The input data dictionary to validate
        lang: The language parameter provided
        
    Raises:
        ValueError: If multilanguage dict is provided with specific lang (not '*')
        
    Examples:
        >>> # This is OK - multilanguage input with lang='*'
        >>> validate_language_consistency(
        ...     {"title": {"en": "Title", "fr": "Titre"}},
        ...     lang="*"
        ... )
        
        >>> # This is OK - single-language input with specific lang
        >>> validate_language_consistency(
        ...     {"title": "My Title"},
        ...     lang="en"
        ... )
        
        >>> # This raises ValueError - multilanguage input with specific lang
        >>> validate_language_consistency(
        ...     {"title": {"en": "Title", "fr": "Titre"}},
        ...     lang="en"
        ... )
        ValueError: Conflicting language parameters...
    """
    # Fields that support localization
    localizable_fields = ['title', 'description', 'keywords', 'license', 'extra_metadata']
    
    for field in localizable_fields:
        if field not in data:
            continue
            
        value = data[field]
        
        # Check if this field contains multilanguage input
        if is_multilanguage_input(value) and lang != '*':
            lang_keys = [k for k in value.keys() if k in _LANGUAGE_METADATA]
            raise ValueError(
                f"Conflicting language parameters: field '{field}' contains "
                f"multilanguage dictionary with keys {lang_keys}, but lang='{lang}' "
                f"was specified. Use lang='*' when providing multilanguage input, "
                f"or provide single-language content with a specific lang code."
            )


# =============================================================================
# Protocols
# =============================================================================


@runtime_checkable
class InternalColumnProtocol(Protocol):
    """Protocol for objects that can declare their internal columns."""

    def get_internal_columns(self) -> Set[str]:
        """Returns a set of internal column names to be excluded from public output."""
        ...


# =============================================================================
# LocalizableModelMixin — base mixin for all localizable Pydantic models.
# Defined here (in localization.py) so it can be imported before any of the
# models in shared_models.py that use it (Link, Provider, …).
# =============================================================================


class LocalizableModelMixin:
    """A mixin to provide localization and merge methods to Pydantic models."""

    if TYPE_CHECKING:
        # Host-class (Pydantic BaseModel) attributes used by mixin methods.
        model_fields: ClassVar[Dict[str, Any]]

        def model_dump(self, **kwargs: Any) -> Dict[str, Any]: ...

        @classmethod
        def model_validate(cls, obj: Any, **kwargs: Any) -> Self: ...

    def localize(
        self, lang: str, include_language_keys: bool = False
    ) -> Tuple[Dict[str, Any], Set[str]]:
        """
        Converts this Pydantic model to a dict with localized fields resolved,
        recursively localising fields that are localization-aware.
        """
        if not self:
            return {}, set()

        data = self.model_dump(by_alias=True, exclude_none=True)
        
        # Manually exclude internal columns if the model defines them
        # (This is a safety measure because AppJSONResponse/json.dumps ignores Field(exclude=True))
        if isinstance(self, InternalColumnProtocol):
            internal_cols = self.get_internal_columns()
            for col in internal_cols:
                if col in data:
                    del data[col]
        
        available_languages: Set[str] = set()

        for field_name, field_info in self.__class__.model_fields.items():
            original_value = getattr(self, field_name, None)
            if original_value is None:
                continue

            serialization_alias = field_info.serialization_alias or field_name

            if isinstance(original_value, LocalizedDTO):
                available_languages.update(original_value.get_available_languages())
                resolved = original_value.resolve(lang, include_language_keys=include_language_keys)
                if resolved is not None:
                    data[serialization_alias] = resolved
                elif serialization_alias in data:
                    del data[serialization_alias]

            elif hasattr(original_value, "localize") and callable(getattr(original_value, "localize")):
                localized_data, sub_langs = original_value.localize(lang, include_language_keys=include_language_keys)
                data[serialization_alias] = localized_data
                available_languages.update(sub_langs)

            elif isinstance(original_value, list) and original_value:
                if hasattr(original_value[0], "localize") and callable(getattr(original_value[0], "localize")):
                    localized_list = []
                    for item in original_value:
                        item_data, item_langs = item.localize(lang, include_language_keys=include_language_keys)
                        localized_list.append(item_data)
                        available_languages.update(item_langs)
                    data[serialization_alias] = localized_list

        return data, available_languages

    def get_available_languages(self, field_name: str) -> List[str]:
        """Returns a list of available language codes for a given localized field."""
        field_value = getattr(self, field_name, None)
        if isinstance(field_value, LocalizedDTO):
            return list(field_value.get_available_languages())
        return []

    def merge_localized_updates(
        self, updates: Union[Dict[str, Any], "BaseModel"], lang: str
    ) -> "Self":
        """
        Merges updates into this model, delegating to localized fields.
        Returns a new updated model instance.
        """
        if isinstance(updates, BaseModel):
            updates = updates.model_dump(by_alias=True, exclude_none=True)
            lang = "*"

        updated_data = self.model_dump(by_alias=True)

        for field_name, new_value in updates.items():
            if new_value is None:
                updated_data[field_name] = None
                continue

            current_field_value = getattr(self, field_name, None)

            if current_field_value is not None and hasattr(current_field_value, "merge_updates"):
                updated_field_obj = current_field_value.merge_updates(new_value, lang)
                if isinstance(updated_field_obj, BaseModel):
                    updated_data[field_name] = updated_field_obj.model_dump(by_alias=True, exclude_none=True)
                else:
                    updated_data[field_name] = updated_field_obj
            elif current_field_value is None:
                field_info = type(self).model_fields.get(field_name)
                if field_info:
                    from typing import get_args, Union as TypingUnion
                    field_type = field_info.annotation
                    if hasattr(field_type, "__origin__") and field_type.__origin__ is TypingUnion:
                        for arg in get_args(field_type):
                            if arg is not type(None):
                                field_type = arg
                                break

                    if isinstance(field_type, type) and hasattr(field_type, "merge_updates"):
                        try:
                            empty_instance = field_type()
                        except Exception:
                            updated_data[field_name] = new_value
                            continue
                        updated_field_obj = empty_instance.merge_updates(new_value, lang)
                        if isinstance(updated_field_obj, BaseModel):
                            updated_data[field_name] = updated_field_obj.model_dump(by_alias=True, exclude_none=True)
                        else:
                            updated_data[field_name] = updated_field_obj
                    else:
                        updated_data[field_name] = new_value
                else:
                    updated_data[field_name] = new_value
            else:
                updated_data[field_name] = new_value

        return self.__class__.model_validate(updated_data)

    @classmethod
    def create_from_localized_input(cls, data: Dict[str, Any], lang: str) -> "Self":
        """
        Factory method to create a model from a dict that may contain
        single-language values for its localized fields.
        """
        if lang == "*" or not data:
            return cls.model_validate(data)

        from typing import get_args, Union as TypingUnion
        processed_data = dict(data)

        for field_name, field_info in cls.model_fields.items():
            if field_name not in processed_data or processed_data[field_name] is None:
                continue

            value = processed_data[field_name]
            field_type = field_info.annotation
            if hasattr(field_type, "__origin__") and field_type.__origin__ is TypingUnion:
                field_type = next(
                    (arg for arg in get_args(field_type) if arg is not type(None)), None
                )

            if (
                field_type
                and hasattr(field_type, "delocalize_input")
                and callable(getattr(field_type, "delocalize_input"))
            ):
                processed_data[field_name] = field_type.delocalize_input(value, lang)

        return cls.model_validate(processed_data)