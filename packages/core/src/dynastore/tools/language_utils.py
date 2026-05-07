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

from typing import Optional, Dict, Any, Union, List
from dynastore.models.localization import LocalizedDTO, LocalizedText, LocalizedKeywords, Language, LocalizedLicense


def resolve_localized_field(
    field: Optional[Union[LocalizedText, LocalizedKeywords, Dict]],
    lang: str
) -> Optional[Union[str, List[str], Dict]]:
    """
    Resolve a localized field to a single language value or return all languages.
    
    Args:
        field: Localized field (LocalizedText, LocalizedKeywords, or Dict)
        lang: Language code or '*' for all languages
        
    Returns:
        - If lang='*': Returns the full dict with all languages
        - Otherwise: Returns the value for the requested language, falling back to 'en', then first available
        - Returns None if field is None or empty
    """
    if not field:
        return None
    
    # Convert Pydantic models to dict and filter out None values
    from pydantic import BaseModel
    if isinstance(field, BaseModel):
        # Get all fields, then manually filter None values
        all_fields = field.model_dump()
        field_dict = {k: v for k, v in all_fields.items() if v is not None}
    elif isinstance(field, dict):
        field_dict = {k: v for k, v in field.items() if v is not None}
    elif isinstance(field, (str, list)):
        return field
    else:
        return None
    
    if not field_dict:
        return None
    
    # Return all languages if requested
    if lang == "*":
        return field_dict
    
    # Try requested language, fallback to 'en', then first available
    return (
        field_dict.get(lang) or 
        field_dict.get("en") or 
        next(iter(field_dict.values()), None)
    )


def inject_localized_field(
    current: Optional[Dict],
    value: Union[str, List[str], Dict],
    lang: str
) -> Dict:
    """
    Inject a single language value into a localized field, preserving other languages.
    
    Args:
        current: Current localized field dict (or None)
        value: Value to inject (string, list, or dict if lang='*')
        lang: Language code or '*' to replace all languages
        
    Returns:
        Updated localized field dict
    """
    result = current.copy() if current else {}
    
    # If lang='*', replace entire dict
    if lang == "*":
        if isinstance(value, dict):
            return value
        else:
            # Invalid: lang='*' requires dict value
            return result
    
    # Inject single language
    result[lang] = value
    return result
