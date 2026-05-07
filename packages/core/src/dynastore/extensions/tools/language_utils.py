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

from typing import Optional
from fastapi import Header, Query, HTTPException
from dynastore.models.localization import Language

def get_language(
    accept_language: Optional[str] = Header(None, alias="Accept-Language"),
    lang: Optional[str] = Query(None, description="Language code (ISO 639-1) or '*' for all languages. Overrides Accept-Language header.")
) -> str:
    """
    Extract preferred language from Accept-Language header or lang query parameter.
    
    Priority:
    1. 'lang' query parameter (if provided).
    2. 'Accept-Language' header (first valid language).
    3. Default to 'en'.
    
    Raises:
        HTTPException(400): If both `lang` and `Accept-Language` are provided but specify different languages.
    """
    header_lang = None
    if accept_language:
        languages = accept_language.split(',')
        if languages:
            first_lang = languages[0].split(';')[0].strip()
            # Extract just the language part (e.g. 'fr' from 'fr-FR')
            lang_code = first_lang.split('-')[0].lower()
            try:
                Language(lang_code)
                header_lang = lang_code
            except ValueError:
                pass  # Invalid language in header, ignore it

    # Check for conflict if both are present and valid
    if lang and header_lang:
        # Normalize lang query param (e.g. ensure lower case)
        query_lang = lang.lower()
        if query_lang != header_lang and query_lang != "*":
             raise HTTPException(
                status_code=400, 
                detail=f"Language conflict: Query parameter 'lang={lang}' conflicts with 'Accept-Language: {accept_language}' header."
            )

    # Priority 1: 'lang' query parameter
    if lang:
        return lang
    
    # Priority 2: Validated header language
    if header_lang:
        return header_lang
    
    return Language.EN.value  # Default fallback
