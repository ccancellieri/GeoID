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

"""Unit tests for the ``in_task_run`` contextvar / ``task_run_scope``."""

from dynastore.tools.execution_context import (
    current_task_catalog,
    in_task_run,
    task_run_scope,
)


def test_default_is_not_in_task_run():
    assert in_task_run() is False


def test_task_run_scope_sets_and_resets():
    assert in_task_run() is False
    with task_run_scope():
        assert in_task_run() is True
    assert in_task_run() is False


def test_nested_scopes_restore_prior_value():
    assert in_task_run() is False
    with task_run_scope():
        assert in_task_run() is True
        with task_run_scope():
            assert in_task_run() is True
        # still inside the outer scope
        assert in_task_run() is True
    assert in_task_run() is False


# ---------------------------------------------------------------------------
# catalog scoping (#2716)
# ---------------------------------------------------------------------------


def test_default_task_catalog_is_none():
    assert current_task_catalog() is None


def test_task_run_scope_without_catalog_stays_none():
    with task_run_scope():
        assert current_task_catalog() is None


def test_task_run_scope_records_declared_catalog():
    assert current_task_catalog() is None
    with task_run_scope(catalog="cat-x"):
        assert current_task_catalog() == "cat-x"
    assert current_task_catalog() is None


def test_nested_scope_restores_prior_catalog():
    with task_run_scope(catalog="outer-cat"):
        assert current_task_catalog() == "outer-cat"
        with task_run_scope(catalog="inner-cat"):
            assert current_task_catalog() == "inner-cat"
        assert current_task_catalog() == "outer-cat"
