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

import re

# A set of common reserved SQL keywords to prevent identifier collision.
POSTGRES_RESERVED_WORDS = {
    'all', 'analyse', 'analyze', 'and', 'any', 'array', 'as', 'asc',
    'asymmetric', 'both', 'case', 'cast', 'check', 'collate', 'column',
    'constraint', 'create', 'current_catalog', 'current_date',
    'current_role', 'current_time', 'current_timestamp', 'current_user',
    'default', 'deferrable', 'desc', 'distinct', 'do', 'else', 'end',
    'except', 'false', 'fetch', 'for', 'foreign', 'from', 'grant', 'group',
    'having', 'in', 'initially', 'intersect', 'into', 'leading', 'limit',
    'localtime', 'localtimestamp', 'not', 'null', 'offset', 'on', 'only',
    'or', 'order', 'placing', 'primary', 'references', 'returning',
    'select', 'session_user', 'some', 'symmetric', 'table', 'then', 'to',
    'trailing', 'true', 'union', 'unique', 'user', 'using', 'variadic',
    'when', 'where', 'window', 'with'
}

def sanitize_for_sql_identifier(value: str) -> str:
    """
    Sanitizes a string to make it a safe PostgreSQL identifier by replacing
    all non-alphanumeric characters (except underscore) with an underscore.
    This is used for creating safe names from arbitrary values (e.g., partition keys).
    """
    return re.sub(r'[^a-zA-Z0-9_]', '_', str(value))

class InvalidIdentifierError(ValueError):
    """Raised when an identifier fails validation."""
    pass


def validate_sql_identifier(identifier: str) -> str:
    """
    Validates a string to ensure it is a safe identifier.
    
    Raises:
        InvalidIdentifierError: If the identifier does not meet constraints.
        
    Returns:
        str: The validated, lowercased identifier.
    """
    if not isinstance(identifier, str):
        raise TypeError("Identifier must be a string.")
    
    if not identifier:
        raise InvalidIdentifierError("Identifier cannot be empty.")
    
    identifier_lower = identifier.lower()
    
    # 1. Check length constraint (max 63 characters).
    if len(identifier_lower) > 63:
        raise InvalidIdentifierError("Identifier must be 63 characters or less.")
        
    # 2. Check for reserved keywords.
    if identifier_lower in POSTGRES_RESERVED_WORDS:
        raise InvalidIdentifierError(f"Identifier '{identifier_lower}' is a reserved keyword.")
        
    # 3. Check character constraints
    #    Authorized chars: a-z, 0-9, _, ., -, > (for JSON paths like 'data->key' or 'schema.table')
    if not re.match(r"^[a-z_][a-z0-9_.>-]*$", identifier_lower):
        raise InvalidIdentifierError(
            "Identifier must start with a letter or underscore, and contain only "
            "lowercase letters, numbers, underscores, dots, or JSON operators (->)."
        )
        
    return identifier_lower