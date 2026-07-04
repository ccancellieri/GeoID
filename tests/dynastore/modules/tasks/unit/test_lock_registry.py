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

"""Uniqueness tests for the central lock-key registry (epic #2830, B4).

Two collision domains are checked here, each against real values imported
live from their owning modules (not hand-copied literals), so a rename or
edit at the source is caught immediately rather than silently drifting
from this test:

* Domain A hand-picked ``configs.leader_lease`` int constants — the ones a
  human chooses by encoding a mnemonic as ASCII hex.
* The static (non-``{service}``-parameterized) string namespaces used
  across Domain A/B/C — collisions there matter because two identical
  strings fold to the identical lock id regardless of domain.
"""

from __future__ import annotations

from dynastore.modules.tasks.durable.lock_registry import LEADER_LEASE_INT_KEYS


def test_leader_lease_int_keys_are_unique() -> None:
    values = list(LEADER_LEASE_INT_KEYS.values())
    assert len(values) == len(set(values)), (
        f"Duplicate Domain-A advisory lock key detected: {LEADER_LEASE_INT_KEYS!r}"
    )


def test_static_lock_namespace_strings_are_unique() -> None:
    from dynastore.modules.db_config.config_seeder import _SEED_LOCK_KEY
    from dynastore.modules.scaling.monitoring_signal_provider import (
        _MONITORING_SIGNAL_LOCK_KEY,
    )
    from dynastore.modules.tasks.dispatcher import _REAPER_LOCK_NAMESPACE
    from dynastore.modules.tasks.tasks_module import _MANDATORY_BACKSTOP_LOCK_NAME

    namespaces = {
        "config_seeder._SEED_LOCK_KEY": _SEED_LOCK_KEY,
        "monitoring_signal_provider._MONITORING_SIGNAL_LOCK_KEY": (
            _MONITORING_SIGNAL_LOCK_KEY
        ),
        "dispatcher._REAPER_LOCK_NAMESPACE": _REAPER_LOCK_NAMESPACE,
        "tasks_module._MANDATORY_BACKSTOP_LOCK_NAME": _MANDATORY_BACKSTOP_LOCK_NAME,
    }
    values = list(namespaces.values())
    assert len(values) == len(set(values)), (
        f"Duplicate static lock namespace string detected: {namespaces!r}"
    )
