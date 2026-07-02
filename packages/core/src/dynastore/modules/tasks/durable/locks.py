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

"""Stable advisory-lock ID derivations for PostgreSQL.

This module provides exactly two derivation functions.  Both currently
key transaction-scoped locks (``pg_try_advisory_xact_lock`` /
``pg_advisory_xact_lock``, released automatically when the surrounding
transaction commits or rolls back) — there are no session-scoped
advisory-lock call sites (``pg_advisory_lock`` / ``pg_try_advisory_lock``)
in this codebase today. The two derivations are kept distinct because
their call sites are independent and must not collide:

``stable_lock_id_sha256``
    Leadership-election / config-lock callers (e.g. ``db_config``
    locking).  SHA-256 is used to produce a signed 64-bit integer from
    the first 8 bytes of the digest.

``stable_lock_id_blake2b``
    Task-dispatcher serialization guards.  BLAKE2b with digest_size=8
    is used and the result is masked to a non-negative 63-bit integer
    that fits PostgreSQL's signed ``bigint`` type.

**Both output spaces are FROZEN.**  Changing either derivation
re-keys advisory locks so that, during a rolling deploy, old and new
pods derive different integers for the same logical lock name.  When
two pods hold different xact advisory locks they both believe they
are the sole leader — a split-brain double-drain scenario where two
processes independently drain the same queue simultaneously.  Update
either function only with a coordinated, flag-gated, zero-downtime
migration that ensures all pods move together.
"""

# stdlib only — no dynastore imports allowed here.
import hashlib


def stable_lock_id_sha256(key: str) -> int:
    """Stable signed 64-bit integer from ``key`` for ``pg_try_advisory_xact_lock``.

    Uses SHA-256: takes the first 8 bytes of the digest and interprets
    them as a big-endian *signed* integer so the output can be negative.
    This matches the PostgreSQL ``bigint`` range ``[-2^63, 2^63-1]``.

    Used for transaction-scoped (``pg_try_advisory_xact_lock``) leadership
    election.  The output is FROZEN — see module docstring for the
    split-brain risk of any change.
    """
    hashed = hashlib.sha256(key.encode("utf-8")).digest()
    return int.from_bytes(hashed[:8], byteorder="big", signed=True)


def stable_lock_id_blake2b(*parts: str) -> int:
    """Stable non-negative 63-bit integer for ``pg_try_advisory_xact_lock``.

    Uses BLAKE2b with ``digest_size=8`` over the null-byte-joined UTF-8
    encoding of ``parts``.  The raw unsigned 64-bit value is masked to
    63 bits (``& 0x7FFFFFFFFFFFFFFF``) so the result is always
    non-negative and fits PostgreSQL's signed ``bigint``.

    Python's built-in ``hash()`` is salted per-process (PEP 456) unless
    ``PYTHONHASHSEED`` is fixed — two pods hashing the same string will
    produce different values, breaking any cross-pod serialization
    guarantee.  BLAKE2b is deterministic across pods, processes, and
    Python versions.

    Used for transaction-scoped serialization guards.  The output is
    FROZEN — see module docstring for the split-brain risk of any change.
    """
    h = hashlib.blake2b(
        b"\x00".join(p.encode("utf-8") for p in parts),
        digest_size=8,
    )
    return int.from_bytes(h.digest(), "big") & 0x7FFFFFFFFFFFFFFF
