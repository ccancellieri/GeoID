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

"""Pin tests for stable advisory-lock ID derivations.

These tests assert EXACT integer outputs.  Any change to the derivation
logic will cause these tests to fail — that is intentional.  The values
are frozen: changing the hash algorithm or byte interpretation re-keys
advisory locks across a rolling deploy, causing a split-brain condition
where two pods both believe they are the sole leader.

Computed from:
    python3 -c "
    import hashlib
    def sha256(k): h=hashlib.sha256(k.encode()).digest(); return int.from_bytes(h[:8],'big',signed=True)
    def blake2b(*p): h=hashlib.blake2b(b'\x00'.join(x.encode() for x in p),digest_size=8); return int.from_bytes(h.digest(),'big')&0x7FFFFFFFFFFFFFFF
    print(sha256('events_backlog_monitor'))    # 2667144991034832408
    print(sha256('maintenance_supervisor'))    # -8447579458234736832
    print(blake2b('dynastore.idx_reaper','gdal'))  # 8324191321573474088
    print(blake2b('dynastore.idx_reaper'))         # 1265204377436225853
    "
"""

import pytest

from dynastore.durable.locks import stable_lock_id_sha256, stable_lock_id_blake2b


# ---------------------------------------------------------------------------
# Pinned exact-value assertions (FROZEN — do not change without a coordinated
# rolling-deploy migration; see module docstring for the split-brain risk).
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("key,expected", [
    ("events_backlog_monitor", 2667144991034832408),
    ("maintenance_supervisor", -8447579458234736832),
])
def test_stable_lock_id_sha256_pinned(key, expected):
    assert stable_lock_id_sha256(key) == expected, (
        f"stable_lock_id_sha256({key!r}) changed — advisory lock re-keyed; "
        "split-brain risk during rolling deploy."
    )


@pytest.mark.parametrize("parts,expected", [
    (("dynastore.idx_reaper", "gdal"), 8324191321573474088),
    (("dynastore.idx_reaper",), 1265204377436225853),
])
def test_stable_lock_id_blake2b_pinned(parts, expected):
    assert stable_lock_id_blake2b(*parts) == expected, (
        f"stable_lock_id_blake2b{parts!r} changed — advisory xact lock re-keyed; "
        "split-brain risk during rolling deploy."
    )


# ---------------------------------------------------------------------------
# Sign/range invariants
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("key", ["events_backlog_monitor", "maintenance_supervisor"])
def test_sha256_can_be_negative(key):
    """SHA-256 variant is a signed bigint — negative values are valid."""
    result = stable_lock_id_sha256(key)
    assert isinstance(result, int)
    assert -(2 ** 63) <= result < 2 ** 63


def test_sha256_maintenance_supervisor_is_negative():
    """maintenance_supervisor produces a negative lock ID (regression guard)."""
    assert stable_lock_id_sha256("maintenance_supervisor") < 0


@pytest.mark.parametrize("parts", [
    ("dynastore.idx_reaper", "gdal"),
    ("dynastore.idx_reaper",),
])
def test_blake2b_is_non_negative_and_fits_63_bits(parts):
    """BLAKE2b variant is masked to 63 bits — always non-negative."""
    result = stable_lock_id_blake2b(*parts)
    assert result >= 0
    assert result < 2 ** 63


# ---------------------------------------------------------------------------
# The two derivations are intentionally distinct for the same input string.
# ---------------------------------------------------------------------------


def test_sha256_and_blake2b_differ_for_same_key():
    """SHA-256 and BLAKE2b derivations must produce different outputs.

    They serve different lock primitives (session vs transaction) and must
    not collide in the advisory-lock namespace.
    """
    key = "events_backlog_monitor"
    assert stable_lock_id_sha256(key) != stable_lock_id_blake2b(key), (
        "sha256 and blake2b derivations must differ for the same key; "
        "colliding values would mean two different lock primitives share "
        "a PostgreSQL advisory-lock slot."
    )


# ---------------------------------------------------------------------------
# Alias back-compat: locking_tools and dispatcher must still export the same values.
# ---------------------------------------------------------------------------


def test_locking_tools_alias_matches_durable():
    """locking_tools._get_stable_lock_id is an alias of stable_lock_id_sha256."""
    from dynastore.modules.db_config.locking_tools import _get_stable_lock_id

    key = "events_backlog_monitor"
    assert _get_stable_lock_id(key) == stable_lock_id_sha256(key)


def test_dispatcher_alias_matches_durable():
    """dispatcher._stable_advisory_lock_key is an alias of stable_lock_id_blake2b."""
    from dynastore.modules.tasks.dispatcher import _stable_advisory_lock_key

    parts = ("dynastore.idx_reaper", "gdal")
    assert _stable_advisory_lock_key(*parts) == stable_lock_id_blake2b(*parts)
