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

"""GCP module domain exceptions.

These are framework-free exceptions raised by the GCP module's business logic.
The extension boundary in ``extensions/tools/exception_handlers.py`` maps them
to the appropriate HTTP status codes via registered handlers.
"""


class GcpServiceUnavailableError(Exception):
    """A required GCP or upstream service is temporarily unavailable.

    Maps to HTTP 503. Optionally carries a ``retry_after`` hint (seconds)
    that the handler forwards as a ``Retry-After`` response header.
    """

    def __init__(self, message: str, *, retry_after: int | None = None) -> None:
        super().__init__(message)
        self.retry_after = retry_after


class GcpFailedDependencyError(Exception):
    """A prerequisite for the requested operation has failed in a way that
    cannot be retried without operator intervention.

    Maps to HTTP 424 (Failed Dependency).  Typical use: catalog storage
    provisioning reached a terminal ``failed`` state — the client must
    diagnose and re-provision the catalog before retrying the upload.
    """


class GcpInternalError(Exception):
    """An unexpected internal consistency failure within the GCP module.

    Maps to HTTP 500.  Use for assertions that should never fire under
    normal operation (e.g. GCS returns no session URI despite a successful
    API call, or storage metadata is inconsistent with readiness state).
    """
