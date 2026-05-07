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

"""
Centralized conflict/constraint violation handling for REST APIs.

This module provides utilities to convert low-level database constraint violations
(unique constraint, foreign key, duplicate object) into HTTP 409 Conflict responses.
"""

import logging
from typing import Optional, Union
from fastapi import HTTPException, status
from sqlalchemy.exc import IntegrityError

from dynastore.modules.db_config.exceptions import (
    UniqueViolationError,
    ForeignKeyViolationError,
    DuplicateObjectError,
    DatabaseError,
    PGCODE_EXCEPTION_MAP,
)

logger = logging.getLogger(__name__)


def conflict_to_409(
    error: Union[DatabaseError, IntegrityError],
    resource_name: str = "Resource",
    resource_id: Optional[str] = None
) -> HTTPException:
    """
    Convert database constraint violation to HTTP 409 Conflict response.
    
    Handles both raw SQLAlchemy IntegrityError and custom DatabaseError exceptions.
    Automatically generates appropriate error messages based on exception type.
    
    Args:
        error: The database exception (IntegrityError or DatabaseError subclass)
        resource_name: Name of the resource (e.g., "Catalog", "Collection", "Asset")
        resource_id: ID/code of the resource (optional, improves error message)
    
    Returns:
        HTTPException with 409 Conflict status code
    
    """
    # Convert raw IntegrityError if needed.
    #
    # Pre-fix: any unmapped pgcode silently downcast to DuplicateObjectError →
    # bogus 409 (closed #200). Now we only map pgcodes that genuinely warrant
    # 409. Other IntegrityErrors should never reach this function — they're
    # filtered out upstream by ``is_conflict_error``. If we somehow do get one
    # (a caller invoked us directly with a non-conflict IntegrityError), the
    # raise below makes the misuse loud rather than masquerading as 409.
    if isinstance(error, IntegrityError):
        pgcode = getattr(error.orig, 'pgcode', None) if error.orig else None
        if pgcode and pgcode in PGCODE_EXCEPTION_MAP:
            error = PGCODE_EXCEPTION_MAP[pgcode](str(error.orig), original_exception=error)
        else:
            raise ValueError(
                f"conflict_to_409 called with IntegrityError carrying pgcode "
                f"{pgcode!r} which is not a conflict-class pgcode. The caller "
                f"should funnel this through the ExceptionHandlerRegistry "
                f"instead so the right 4xx/5xx mapping can fire. "
                f"Original: {error.orig}"
            )

    # Generate message based on exception type. Pre-fix the catch-all `else`
    # downcast every other DatabaseError subclass to 409 (e.g. NotNull /
    # Check violations the new typed map produces). Now strict: any
    # non-conflict-class typed exception raises so the misuse is loud.
    if isinstance(error, (UniqueViolationError, DuplicateObjectError)):
        detail = f"{resource_name} '{resource_id}' already exists." if resource_id else f"{resource_name} already exists."
    elif isinstance(error, ForeignKeyViolationError):
        detail = f"Cannot create {resource_name} '{resource_id}': referenced resource does not exist." if resource_id else "Cannot create resource: referenced resource does not exist."
    else:
        raise ValueError(
            f"conflict_to_409 called with non-conflict-class exception "
            f"{type(error).__name__}: {error}. Conflict status (409) is only "
            f"appropriate for UniqueViolationError, ForeignKeyViolationError, "
            f"and DuplicateObjectError. Route this through the "
            f"ExceptionHandlerRegistry for the correct status mapping."
        )

    logger.warning(f"Conflict (409): {detail} [Original: {error}]")
    return HTTPException(status_code=status.HTTP_409_CONFLICT, detail=detail)
