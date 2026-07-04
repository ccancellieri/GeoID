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

"""Central registry of statically-assigned advisory-lock / lease keys.

Before this module existed, every leader-elected loop picked its own
hand-crafted ASCII-hex bigint and warned about collisions purely via a
comment referencing the *other* loops' raw values (e.g. "must not collide
with SoftDeleteReaper (0x5D3A7E1F_C2B84961)") — so avoiding a collision
meant grepping the whole tree for every sibling constant. This module is
the one place to check (and extend) before picking a new key.

Three separate collision domains exist in this codebase; only the first
is centralized here as literal constants, because it is the only one where
a human hand-picks the raw numeric value:

Domain A — ``configs.leader_lease.lock_key`` (a DB row, not a real
    Postgres lock): populated by :func:`dynastore.modules.db_config.
    locking_tools.lease_leadership` / ``lease_leadership_with_heartbeat``,
    used by every ``PeriodicService.lock_key``. An ``int`` key is stored
    AS-IS; a ``str`` key is folded to an int via ``_get_stable_lock_id``
    (SHA-256, see ``locking_tools.py``). The hand-picked ASCII-hex bigint
    constants below are the ones that must be kept pairwise distinct by a
    human — see ``LEADER_LEASE_INT_KEYS`` and the uniqueness test in
    ``tests/dynastore/modules/tasks/unit/test_lock_registry.py``.

Domain B — a genuine transaction-scoped ``pg_try_advisory_xact_lock`` /
    ``pg_advisory_xact_lock``, SHA-256-folded via
    :func:`~dynastore.modules.tasks.durable.locks.stable_lock_id_sha256`
    and entered through ``acquire_startup_lock`` (``locking_tools.py``).
    Used for one-shot startup/seed serialization. Static namespace keys
    in this domain: ``config_seeder._SEED_LOCK_KEY``. There are also
    per-call dynamic keys here (``db_config/query_executor.py``'s
    ``f"ddl.{stmt_hash}"``, ``modules/presets/bootstrap.py``'s
    ``f"iam_seed:{preset_name}:{scope_key}"``) — each embeds a
    caller-supplied discriminator, so a static collision-avoidance
    registry does not apply to them.

Domain C — a genuine transaction-scoped ``pg_try_advisory_xact_lock``,
    BLAKE2b-folded via :func:`~dynastore.modules.tasks.durable.locks.
    stable_lock_id_blake2b` (aliased as ``_stable_advisory_lock_key`` in
    ``dispatcher.py``). Static namespace keys in this domain:
    ``dispatcher._REAPER_LOCK_NAMESPACE`` and
    ``tasks_module._MANDATORY_BACKSTOP_LOCK_NAME``.

Domain A also has a handful of *string* keys that fold into the same
lease table as the int constants below (``monitoring_signal_provider.
_MONITORING_SIGNAL_LOCK_KEY``, ``gcp/scaling_reconciler.py``'s
``"gcp-scaling-reconciler"``, ``gcp/liveness_reconciler.py``'s
``f"gcp-liveness-reconciler:{service}"``, ``tasks/registry/publisher.py``'s
``f"task-registry-heartbeat:{service}"``). The two static (non-dynamic)
ones are covered by the string-uniqueness test alongside the Domain B/C
namespaces; the ``{service}``-parameterized ones are left in place since
their collision-avoidance already comes from the interpolated value.

Picking a new Domain-A int key: choose an 8-byte mnemonic (ASCII where
possible), hex-encode it as a signed 64-bit literal, and confirm it is not
already present in ``LEADER_LEASE_INT_KEYS`` below.
"""

# stdlib only — this module must stay a leaf so anything can import the
# int constants without pulling in the task dispatcher, catalog services,
# or DB config machinery. Mirrors the same rule in ``durable/locks.py``.

# ---------------------------------------------------------------------------
# Domain A — configs.leader_lease.lock_key int constants (canonical home;
# the original modules import these rather than redefining the literal).
# ---------------------------------------------------------------------------

SUPERVISOR_ADVISORY_LOCK_KEY = 0x4D41494E_54454E41
"""``modules/catalog/maintenance_supervisor.py`` leader election. ASCII "MAINTENA"."""

LOG_DRAINER_ADVISORY_LOCK_KEY = 0x4C4F4744_52414E31
"""``modules/catalog/log_drainer.py`` leader election. ASCII "LOGDRAN1"."""

SOFT_DELETE_REAPER_ADVISORY_LOCK_KEY = 0x5D3A7E1F_C2B84961
"""``modules/catalog/soft_delete_reaper.py`` leader election. Deterministic constant (non-ASCII)."""

LIFECYCLE_REAPER_ADVISORY_LOCK_KEY = 0x4C494643_52454150
"""``modules/catalog/lifecycle_reaper.py`` leader election. ASCII "LIFCREAP"."""

ZOMBIE_REAPER_ADVISORY_LOCK_KEY = 0x5A4F4D42_49455031
"""``modules/db/zombie_session_reaper.py`` leader election. ASCII "ZOMBIEP1"."""

CONTENTION_MONITOR_LOCK_KEY = 0x4C4F434B_4D4F4E49
"""``modules/db/db_contention_monitor.py`` leader election. ASCII "LOCKMONI"."""

LEADER_LEASE_INT_KEYS = {
    "maintenance_supervisor": SUPERVISOR_ADVISORY_LOCK_KEY,
    "log_drainer": LOG_DRAINER_ADVISORY_LOCK_KEY,
    "soft_delete_reaper": SOFT_DELETE_REAPER_ADVISORY_LOCK_KEY,
    "lifecycle_reaper": LIFECYCLE_REAPER_ADVISORY_LOCK_KEY,
    "zombie_session_reaper": ZOMBIE_REAPER_ADVISORY_LOCK_KEY,
    "db_contention_monitor": CONTENTION_MONITOR_LOCK_KEY,
}
"""Owner-name -> literal lock key, for the uniqueness test. Extend this dict
(and the constant above it) when adding a new hand-picked Domain-A int key."""
