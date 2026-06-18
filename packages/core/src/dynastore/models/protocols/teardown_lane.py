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

"""TeardownLane — driver-declared teardown routing capability."""

from enum import Enum


class TeardownLane(str, Enum):
    """Declares how a driver's storage is torn down during a hard-delete cascade.

    Each concrete driver class sets a ``teardown_lane: ClassVar[TeardownLane]``
    attribute (defaulting to ``ASYNC_CASCADE`` on the protocol base classes) to
    tell the routing-driven cascade owner whether it should enqueue the driver
    for async ``drop_storage``, skip it because teardown already happened
    inline, delegate to a dedicated owner, or skip because no teardown is
    possible.

    Lane semantics
    ~~~~~~~~~~~~~~
    INLINE_TXN
        The driver's storage is dropped synchronously inside the delete
        transaction (PostgreSQL: the items-table DROP and the asset-row DELETE
        are atomic with the registry row drop).  The async cascade must NOT
        re-drop PG storage: a second ``DROP TABLE`` on the same physical table
        races the inline drop for the table lock (LockNotAvailableError /
        "lock timeout") and is pure redundancy.

    ASYNC_CASCADE
        Standard async path.  The routing-driven cascade owner enqueues one
        ``CleanupRef`` for this driver and calls ``driver.drop_storage()``
        post-commit.  Elasticsearch, DuckDB, and Iceberg drivers use this lane.
        This is the default on all four storage-driver protocol base classes.

    ASYNC_DEDICATED
        Teardown is handled by a dedicated cascade owner with its own
        credentials and lifecycle (e.g. ``GcsCatalogPrefixOwner`` /
        ``GcsCollectionPrefixOwner`` for GCS binary storage).  The
        routing-driven owner skips drivers in this lane to avoid conflicting
        with the dedicated owner.  The enum value is reserved for a registered
        GCS/GCP routing driver class if one ever exists.

    NONE
        No teardown is needed or possible.  The driver is read-only, has no
        physical storage it owns, or delegates full cleanup to an external
        system (e.g. ``ItemsBigQueryDriver`` which operates on a pre-existing
        customer-owned dataset and has no ``drop_storage``).
    """

    INLINE_TXN = "inline_txn"
    ASYNC_CASCADE = "async_cascade"
    ASYNC_DEDICATED = "async_dedicated"
    NONE = "none"
