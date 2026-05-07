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

import uuid
import time
import os
import threading


# UUIDV7_GENERATOR_DDL = """
# CREATE OR REPLACE FUNCTION uuidv7_generate()
# RETURNS uuid AS $$
# DECLARE
#   v_time_us bigint;
#   v_unix_t  bigint;
#   v_rand_a  bigint;
#   v_rand_b  bigint;
# BEGIN
#   -- 1. Microsecond-precision timestamp (clock_timestamp() has µs resolution in PG).
#   v_time_us := floor(extract(epoch from clock_timestamp()) * 1000000)::bigint;
#
#   -- 2. 48-bit ms timestamp.
#   v_unix_t := v_time_us / 1000;
#
#   -- 3. 12-bit sub-ms counter: scale µs-within-ms (0–999) to 12 bits (0–4091).
#   --    clock_timestamp() advances in µs steps, so rand_a is naturally monotonic
#   --    within each ms without any shared state across sessions or replicas.
#   --    Mirrors RFC 9562 §6.2 Method 3 (higher-resolution sub-ms timestamp).
#   v_rand_a := (v_time_us % 1000) * 4096 / 1000;
#
#   -- 4. 62 bits of randomness for rand_b.
#   --    Ensures uniqueness when multiple rows are inserted within the same µs
#   --    (e.g. bulk inserts from concurrent Cloud Run replicas).
#   v_rand_b := floor(random() * 4611686018427387904)::bigint;
#
#   -- 5. Construct UUID: [unix_ts_ms:48][ver=7:4][rand_a:12][var=0b10:2][rand_b:62]
#   RETURN lpad(to_hex(v_unix_t), 12, '0') ||
#          to_hex((7 << 12) | v_rand_a) ||
#          to_hex((2 << 14) | (v_rand_b >> 48)) ||
#          lpad(to_hex(v_rand_b & x'ffffffffffff'::bigint), 12, '0');
# END;
# $$ LANGUAGE plpgsql VOLATILE;
# """


# --- Monotonic state for generate_uuidv7 ---
# RFC 9562 §6.2 Method 1: per-ms monotonic counter seeded randomly each new ms.
# The lock is held only for the counter read/increment; os.urandom() for rand_b
# is intentionally outside the lock to minimise contention during bulk ingestion.
_uuidv7_lock = threading.Lock()
_uuidv7_last_ms: int = 0
_uuidv7_counter: int = 0
_UUIDV7_COUNTER_MAX: int = 0xFFF  # 12 bits — 4095 UUIDs per ms per process


def generate_uuidv7() -> uuid.UUID:
    """
    Generates a UUID v7 following RFC 9562 with a monotonic counter (§6.2 Method 1).

    Layout (128 bits):
      [unix_ts_ms: 48][ver=7: 4][counter: 12][var=0b10: 2][rand_b: 62]

    Guarantees:
    - Strictly monotonic within a process (counter increments within the same ms).
    - Counter seeded randomly on each new millisecond to avoid cross-process collisions.
    - Clock rollback (NTP, suspend/resume) handled: timestamp is held at last known ms.
    - Counter overflow (>4095 UUIDs/ms): synthetic ms advancement preserves ordering.
    - rand_b (62 bits) remains fully random — uniqueness unaffected.
    """
    global _uuidv7_last_ms, _uuidv7_counter

    with _uuidv7_lock:
        ms = int(time.time() * 1000)

        if ms > _uuidv7_last_ms:
            # New millisecond: advance clock, seed counter randomly.
            _uuidv7_last_ms = ms
            _uuidv7_counter = int.from_bytes(os.urandom(2), "big") & _UUIDV7_COUNTER_MAX
        else:
            # Same ms OR clock went backwards: hold at last known ms, increment counter.
            _uuidv7_counter += 1
            if _uuidv7_counter > _UUIDV7_COUNTER_MAX:
                # Counter overflow: advance synthetic timestamp, re-seed counter.
                _uuidv7_last_ms += 1
                _uuidv7_counter = int.from_bytes(os.urandom(2), "big") & _UUIDV7_COUNTER_MAX

        ms_snapshot = _uuidv7_last_ms
        counter_snapshot = _uuidv7_counter

    # rand_b: 8 bytes (64 bits); variant bits applied to the first byte below.
    # Allocated outside the lock — os.urandom() is thread-safe and slow under contention.
    rand_b = os.urandom(8)

    uuid_bytes = bytearray(ms_snapshot.to_bytes(6, "big"))
    uuid_bytes.append(0x70 | (counter_snapshot >> 8))   # ver=7 | counter[11:8]
    uuid_bytes.append(counter_snapshot & 0xFF)           # counter[7:0]
    uuid_bytes.append((rand_b[0] & 0x3F) | 0x80)        # var=0b10 | rand_b[61:56]
    uuid_bytes.extend(rand_b[1:])                        # rand_b[55:0]

    return uuid.UUID(bytes=bytes(uuid_bytes))


def generate_geoid() -> str:
    """Generates a new unique identifier (UUIDv7) for a geospatial feature."""
    return str(generate_uuidv7())


def generate_transaction_id() -> str:
    """Generates a unique transaction identifier (UUIDv7)."""
    return str(generate_uuidv7())


def generate_task_id() -> uuid.UUID:
    """Generates a unique task identifier (UUIDv7)."""
    return generate_uuidv7()


def generate_id_hex() -> str:
    """Generates a UUIDv7-based hex string (no dashes) for use as an ID suffix or slug."""
    return generate_uuidv7().hex
