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

"""Table-driven meta-contract test for Items-tier storage drivers.

Every ``Items*Driver`` class declares the same four structural facts —
its own class name, a routing ``priority``, a ``capabilities`` set, and
the read-flavour ``Hint``s it serves via ``supported_hints``. Each
driver's own test file used to repeat four near-identical test
functions to pin these facts down. This module replaces that
duplication with a single parametrized table: one row per driver, one
set of assertions shared by all rows.

Driver-specific behaviour (write semantics, `is_available()` wiring,
registry visibility, etc.) is intentionally NOT covered here — that
stays in each driver's own test file.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Tuple, Type

import pytest

from dynastore.models.protocols.storage_driver import Capability
from dynastore.modules.storage.drivers.bigquery import ItemsBigQueryDriver
from dynastore.modules.storage.drivers.duckdb import ItemsDuckdbDriver
from dynastore.modules.storage.drivers.elasticsearch import ItemsElasticsearchDriver
from dynastore.modules.storage.drivers.elasticsearch_private import (
    ItemsElasticsearchPrivateDriver,
)
from dynastore.modules.storage.drivers.postgresql import ItemsPostgresqlDriver
from dynastore.modules.storage.hints import Hint


@dataclass(frozen=True)
class DriverMetaSpec:
    """One row of the meta-contract table: a driver class plus the
    structural facts its own test file used to assert individually."""

    driver_cls: Type[object]
    expected_priority: int
    required_capabilities: Tuple[str, ...] = field(default_factory=tuple)
    excluded_capabilities: Tuple[str, ...] = field(default_factory=tuple)
    required_hints: Tuple[Hint, ...] = field(default_factory=tuple)


DRIVER_META_SPECS = [
    DriverMetaSpec(
        driver_cls=ItemsPostgresqlDriver,
        expected_priority=10,
        required_capabilities=(Capability.STREAMING, Capability.SOFT_DELETE, Capability.EXPORT),
        excluded_capabilities=(Capability.READ_ONLY,),
        required_hints=(Hint.SPATIAL_FILTER, Hint.AGGREGATION, Hint.GEOMETRY_EXACT),
    ),
    DriverMetaSpec(
        driver_cls=ItemsElasticsearchDriver,
        expected_priority=50,
        required_capabilities=(Capability.STREAMING, Capability.SOFT_DELETE),
        required_hints=(Hint.FULLTEXT, Hint.SPATIAL_FILTER, Hint.AGGREGATION),
    ),
    DriverMetaSpec(
        driver_cls=ItemsElasticsearchPrivateDriver,
        expected_priority=51,
        required_capabilities=(Capability.STREAMING,),
        excluded_capabilities=(Capability.SOFT_DELETE,),
        # SEARCH-flavour hints are pinned in this driver's own
        # ``test_has_search_hints`` (it also asserts opt-in-only
        # routing behaviour, which is out of scope for this table).
    ),
    DriverMetaSpec(
        driver_cls=ItemsDuckdbDriver,
        expected_priority=30,
        required_capabilities=(Capability.READ, Capability.STREAMING, Capability.EXPORT),
        required_hints=(Hint.SPATIAL_FILTER, Hint.SORT, Hint.GROUP_BY),
    ),
    DriverMetaSpec(
        driver_cls=ItemsBigQueryDriver,
        expected_priority=50,
        # WRITE is declared at the class level so the driver can
        # participate in routing-config WRITE fan-outs; actual write
        # behaviour is gated by ``reporter_mode`` on the per-collection
        # config — default "off" means WRITE is a no-op.
        required_capabilities=("READ", "WRITE", "STREAMING", "INTROSPECTION"),
        required_hints=(Hint.COUNT, Hint.AGGREGATION),
    ),
]

try:
    import pyiceberg  # noqa: F401
except ImportError:
    pass  # Iceberg driver module itself imports fine without pyiceberg (it
    # lazy-loads pyiceberg internally); skip this row since there is
    # nothing meaningful to exercise without the real dependency.
else:
    from dynastore.modules.storage.drivers.iceberg import ItemsIcebergDriver

    DRIVER_META_SPECS.append(
        DriverMetaSpec(
            driver_cls=ItemsIcebergDriver,
            expected_priority=20,
            required_capabilities=(
                Capability.STREAMING,
                Capability.EXPORT,
                Capability.TIME_TRAVEL,
                Capability.VERSIONING,
                Capability.SNAPSHOTS,
                Capability.SCHEMA_EVOLUTION,
                Capability.SOFT_DELETE,
            ),
            excluded_capabilities=(Capability.READ_ONLY,),
            required_hints=(Hint.SPATIAL_FILTER, Hint.STATISTICS, Hint.SORT),
        )
    )


def _spec_id(spec: DriverMetaSpec) -> str:
    return spec.driver_cls.__name__


@pytest.mark.parametrize("spec", DRIVER_META_SPECS, ids=_spec_id)
def test_driver_class_name(spec: DriverMetaSpec) -> None:
    driver = spec.driver_cls()
    assert type(driver).__name__ == spec.driver_cls.__name__


@pytest.mark.parametrize("spec", DRIVER_META_SPECS, ids=_spec_id)
def test_priority(spec: DriverMetaSpec) -> None:
    driver = spec.driver_cls()
    assert driver.priority == spec.expected_priority


@pytest.mark.parametrize("spec", DRIVER_META_SPECS, ids=_spec_id)
def test_capabilities(spec: DriverMetaSpec) -> None:
    driver = spec.driver_cls()
    for capability in spec.required_capabilities:
        assert capability in driver.capabilities
    for capability in spec.excluded_capabilities:
        assert capability not in driver.capabilities


@pytest.mark.parametrize("spec", DRIVER_META_SPECS, ids=_spec_id)
def test_read_flavour_hints(spec: DriverMetaSpec) -> None:
    driver = spec.driver_cls()
    for hint in spec.required_hints:
        assert hint in driver.supported_hints
