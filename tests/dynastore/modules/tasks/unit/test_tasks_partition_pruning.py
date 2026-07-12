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

"""Source-shape pins for #3218 — terminal task writes skip partition pruning.

``tasks`` is RANGE-partitioned on its ``timestamp`` column (the row's
creation time). Every terminal write (``complete_task`` / ``fail_task`` /
``dead_letter_task`` / ``heartbeat_task_if_active`` / ``heartbeat_tasks``)
used to key its UPDATE on ``task_id`` alone, so Postgres could not prune
partitions and probed every live monthly partition's ``idx_tasks_task_id``
on every call. These pins prove each write now also carries an equality
predicate on the row's creation timestamp (``created_at`` — distinct from
``finished_at``, the completion time), and that a 0-row result caused by a
wrong ``created_at`` is diagnosed as a distinguishable failure rather than a
silent, benign-looking owner_id race loss.

Real-Postgres partition placement, the DEFAULT-partition no-regression case
and the live wrong-partition-key diagnostic log are covered by the
integration counterpart in
``integration/test_tasks_partition_pruning.py`` — these are fast, no-DB pins
of the SQL/signature shape only.
"""

from __future__ import annotations

import inspect


def _tasks_module():
    import dynastore.modules.tasks.tasks_module as tm
    return tm


# --- (a) every terminal write carries the created_at equality predicate ----


def test_complete_task_accepts_created_at_kwarg():
    fn = _tasks_module().complete_task
    sig = inspect.signature(fn)
    assert "created_at" in sig.parameters
    assert sig.parameters["created_at"].kind == inspect.Parameter.KEYWORD_ONLY


def test_complete_task_where_clause_carries_created_at_guard():
    src = inspect.getsource(_tasks_module().complete_task)
    assert (
        'created_at_guard = " AND timestamp = :created_at" if created_at is not None else ""'
        in src
    )
    assert "WHERE task_id = :task_id{created_at_guard}{owner_guard};" in src


def test_fail_task_accepts_created_at_kwarg():
    fn = _tasks_module().fail_task
    sig = inspect.signature(fn)
    assert "created_at" in sig.parameters
    assert sig.parameters["created_at"].kind == inspect.Parameter.KEYWORD_ONLY


def test_fail_task_where_clause_carries_created_at_guard_on_both_branches():
    """Both the retry (PENDING/DEAD_LETTER) and non-retry (FAILED) UPDATE
    branches must carry the guard — a task can be failed via either path."""
    src = inspect.getsource(_tasks_module().fail_task)
    assert (
        'created_at_guard = " AND timestamp = :created_at" if created_at is not None else ""'
        in src
    )
    assert src.count("WHERE task_id = :task_id{created_at_guard}{owner_guard}") == 2


def test_dead_letter_task_accepts_created_at_kwarg():
    fn = _tasks_module().dead_letter_task
    sig = inspect.signature(fn)
    assert "created_at" in sig.parameters
    assert sig.parameters["created_at"].kind == inspect.Parameter.KEYWORD_ONLY


def test_dead_letter_task_where_clause_carries_created_at_guard():
    src = inspect.getsource(_tasks_module().dead_letter_task)
    assert (
        'created_at_guard = " AND timestamp = :created_at" if created_at is not None else ""'
        in src
    )
    assert "WHERE task_id = :task_id{created_at_guard}{owner_guard}" in src


def test_heartbeat_task_if_active_accepts_created_at_kwarg():
    fn = _tasks_module().heartbeat_task_if_active
    sig = inspect.signature(fn)
    assert "created_at" in sig.parameters
    assert sig.parameters["created_at"].kind == inspect.Parameter.KEYWORD_ONLY


def test_heartbeat_task_if_active_where_clause_carries_created_at_guard():
    src = inspect.getsource(_tasks_module().heartbeat_task_if_active)
    assert (
        'created_at_guard = " AND timestamp = :created_at" if created_at is not None else ""'
        in src
    )
    assert "WHERE task_id = :task_id{created_at_guard}" in src
    # Unaffected: the ACTIVE-status gate that makes the rowcount signal
    # meaningful must still be present alongside the new guard.
    assert "status = 'ACTIVE'" in src


def test_heartbeat_tasks_takes_task_id_created_at_pairs_not_bare_ids():
    """The batch heartbeat no longer accepts a bare list of task_ids — every
    entry must carry its own row's creation timestamp so the UPDATE can
    match each row on (task_id, timestamp), mirroring claim_batch's existing
    (timestamp, task_id) join shape for the same partitioned table."""
    fn = _tasks_module().heartbeat_tasks
    sig = inspect.signature(fn)
    params = list(sig.parameters)
    assert params[1] == "tasks", "batch heartbeat's second param must be the (task_id, created_at) pairs"
    src = inspect.getsource(fn)
    normalised = " ".join(src.split()).upper()
    assert "UNNEST(CAST(:TASK_IDS AS UUID[]), CAST(:CREATED_ATS AS TIMESTAMPTZ[]))" in normalised
    assert "T.TASK_ID = BATCH.TASK_ID" in normalised
    assert "T.TIMESTAMP = BATCH.CREATED_AT" in normalised
    assert "T.STATUS = 'ACTIVE'" in normalised


# --- claim_for_execution / GCP select helpers surface the creation timestamp


def test_claim_for_execution_returns_timestamp_for_downstream_threading():
    """main_task.py's in-job heartbeat + terminal writes need the claimed
    row's creation timestamp; claim_for_execution's RETURNING clause must
    surface it so no second round-trip is required to get it."""
    src = inspect.getsource(_tasks_module().claim_for_execution)
    normalised = " ".join(src.split()).upper()
    assert "RETURNING TASK_ID, STATUS, OWNER_ID, TIMESTAMP;" in normalised


def test_select_lapsed_gcp_tasks_surfaces_timestamp():
    src = inspect.getsource(_tasks_module().select_lapsed_gcp_tasks)
    normalised = " ".join(src.split()).upper()
    assert "COLLECTION_ID, TIMESTAMP" in normalised


def test_select_stale_gcp_tasks_surfaces_timestamp():
    src = inspect.getsource(_tasks_module().select_stale_gcp_tasks)
    normalised = " ".join(src.split()).upper()
    assert "COLLECTION_ID, TIMESTAMP" in normalised


# --- (d) a wrong created_at must surface loudly, not as a benign race loss -


def test_diagnose_created_at_miss_helper_exists_and_is_async():
    fn = _tasks_module()._diagnose_created_at_miss
    assert inspect.iscoroutinefunction(fn)


def test_diagnose_created_at_miss_logs_at_error_on_mismatch():
    src = inspect.getsource(_tasks_module()._diagnose_created_at_miss)
    assert "logger.error(" in src
    assert "WRONG partition key" in src


def test_diagnose_created_at_miss_does_not_log_when_timestamps_match():
    """A genuine race loss (row exists under the SAME created_at, e.g. the
    owner_id guard tripped) must not be misreported as a wrong partition
    key — only an actual mismatch logs the ERROR."""
    src = inspect.getsource(_tasks_module()._diagnose_created_at_miss)
    assert "actual_ts != created_at" in src


def test_complete_task_diagnoses_a_created_at_guarded_miss():
    src = inspect.getsource(_tasks_module().complete_task)
    assert "_diagnose_created_at_miss(engine, task_id, created_at, \"complete_task\")" in src


def test_fail_task_diagnoses_a_created_at_guarded_miss():
    src = inspect.getsource(_tasks_module().fail_task)
    assert "_diagnose_created_at_miss(engine, task_id, created_at, \"fail_task\")" in src


def test_dead_letter_task_diagnoses_a_created_at_guarded_miss():
    src = inspect.getsource(_tasks_module().dead_letter_task)
    assert "_diagnose_created_at_miss(engine, task_id, created_at, \"dead_letter_task\")" in src


def test_heartbeat_task_if_active_diagnoses_a_created_at_guarded_miss():
    src = inspect.getsource(_tasks_module().heartbeat_task_if_active)
    assert (
        '_diagnose_created_at_miss(engine, task_id, created_at, "heartbeat_task_if_active")'
        in src
    )
