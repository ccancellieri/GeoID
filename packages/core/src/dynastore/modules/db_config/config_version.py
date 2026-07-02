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

"""Optimistic-concurrency (CAS) version-token codec for config writes (#2707).

Every config row (``configs.platform_configs``, ``<tenant>.catalog_configs``,
``<tenant>.collection_configs``) already carries a ``updated_at TIMESTAMPTZ
NOT NULL`` column, bumped to ``NOW()`` on every write (see
``typed_store/ddl.py``). That column doubles as a compare-and-set token
without any schema change: this module is the single place that encodes it
to the opaque string exposed at the ``ConfigsProtocol`` boundary
(``get_config_versioned`` / ``set_config(..., expected_version=...)``) and
decodes it back for the CAS ``UPDATE ... WHERE updated_at = :expected_version``
predicate.
"""

from datetime import datetime

__all__ = ["encode_config_version", "decode_config_version"]


def encode_config_version(updated_at: datetime) -> str:
    """Derive the opaque CAS token a caller round-trips through ``set_config``."""
    return updated_at.isoformat()


def decode_config_version(version: str) -> datetime:
    """Parse a token back to the ``updated_at`` value it encodes.

    Raises ``ValueError`` on a malformed token. A caller should only ever
    pass a token obtained from ``get_config_versioned`` — a corrupt or
    foreign token is a caller bug, not a legitimate conflict, so this is
    allowed to propagate rather than being downgraded to a conflict.
    """
    return datetime.fromisoformat(version)
