#    Copyright 2026 FAO
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
import traceback
from typing import Optional, Dict, Any
from fastapi import HTTPException
from dynastore.extensions.tools.exception_handlers import ExceptionHandler
from dynastore.models.protocols.logs import LogsProtocol
from dynastore.tools.discovery import get_protocol
from dynastore.modules.concurrency import run_in_background

logger = logging.getLogger(__name__)

class CatalogLoggingExceptionHandler(ExceptionHandler):
    """
    Handler that logs server errors (5xx) to the catalog/system log service.
    
    This handler DOES NOT produce an HTTP response (returns None).
    It is intended to be registered FIRST (or early) in the chain, effectively acting
    as middleware to side-effect (log) the error before a subsequent handler
    converts it to a response.
    
    Only logs 5xx errors (server errors), not 4xx (user errors).
    """
    
    def can_handle(self, exception: Exception) -> bool:
        # Only handle exceptions that will result in 5xx errors
        # Skip HTTPException with 4xx status codes (user errors)
        if isinstance(exception, HTTPException):
            return exception.status_code >= 500
        
        # All other exceptions are assumed to be server errors
        return True
    
    def handle(self, exception: Exception, context: Optional[Dict[str, Any]] = None) -> Optional[HTTPException]:
        context = context or {}
        
        # Extract context
        catalog_id = context.get('catalog_id') or context.get('resource_id')
        collection_id = context.get('collection_id')
        operation = context.get('operation', 'Unknown Operation')
        resource_name = context.get('resource_name', 'System')
        
        # Determine target catalog
        target_catalog = catalog_id
        if not target_catalog:
             # Try to guess based on context signals or fallback to system
             if resource_name == "Catalog" and context.get('resource_id'):
                  target_catalog = context.get('resource_id')
             else:
                  target_catalog = "_system_"
        
        # Capture full stacktrace
        stack_trace = ''.join(traceback.format_exception(type(exception), exception, exception.__traceback__))
        logger.debug(f"EXCEPTION: {stack_trace}")
        
        message = f"Error during {operation}: {str(exception)}"
        
        # Prepare details with stacktrace and request context
        details = {
            "error_type": exception.__class__.__name__,
            "resource_name": resource_name,
            "operation": operation,
            "original_exception_args": [str(arg) for arg in exception.args],
            "stacktrace": stack_trace,  # Will be extracted to separate column
        }
        
        # Add request context if available
        if 'request_context' in context:
            details['request_context'] = context['request_context']
        
        if collection_id:
            details['collection_id'] = collection_id
            
        # Fire-and-forget async logging via concurrency module to ensure task tracking
        # and prevent garbage collection.
        
        async def _do_log():
            try:
                logs_service = get_protocol(LogsProtocol)
                if logs_service:
                    # immediate=True: dispatch straight to the backend now
                    # (#2749 — no PG connection, no caller transaction to
                    # join). This is already a detached background task, so
                    # the resulting log_id/context mutation below cannot
                    # reach the response by the time this runs — kept only
                    # for parity with the synchronous exception_handlers.py
                    # path and any future caller that awaits this handler.
                    log_id = await logs_service.log_event(
                        catalog_id=target_catalog,  # type: ignore[arg-type]
                        event_type="exception",
                        level="ERROR",
                        message=message,
                        collection_id=collection_id,
                        details=details,
                        immediate=True,
                    )
                    if log_id:
                        context['log_id'] = log_id
                        context['log_catalog'] = target_catalog
                        context['log_collection'] = collection_id
            except Exception as e:
                logger.error(f"Failed to send exception to LogService: {e}")
                
        # Use centralized background runner
        try:
             run_in_background(_do_log(), name=f"log_exception_{operation}")
        except Exception as e:
            logger.error(f"Failed to schedule async logging: {e}. Original error: {message}", exc_info=True)

        # RETURN NONE to allow other handlers to process the exception into a response
        return None
