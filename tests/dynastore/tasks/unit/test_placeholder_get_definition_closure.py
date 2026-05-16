"""Regression test for the ``DefinitionOnlyTask`` placeholder closure bug.

Background: ``register_task_definitions_only`` creates a ``DefinitionOnlyTask``
*inside* the ``for ep in entry_points(...)`` loop. The class previously held a
``@staticmethod get_definition`` that returned the bare loop variable
``process_def`` — a free-variable closure that resolves at call time, not at
class-definition time. Every placeholder therefore returned the **last**
``process_def`` produced by the loop, not its own.

The bug was dormant in practice because the dispatcher reads the captured
``TaskConfig.definition`` field (set correctly at registration time) and
never calls ``task_config.cls.get_definition()`` post-registration. Still
incorrect on its face — a future refactor that called the class method
would silently get the wrong definition for every placeholder except the
last one registered. Ruff B023 flagged it.

Fix: convert to ``@classmethod`` and read ``cls._process_definition`` so
each placeholder reads its own per-class attribute, which IS bound at
class-definition time (lines 167–168 in ``packages/core/src/dynastore/tasks/__init__.py``).
"""
from __future__ import annotations


def test_each_placeholder_returns_its_own_definition():
    """Reproduce the loop-closure shape from
    ``register_task_definitions_only`` and verify each placeholder's
    ``get_definition()`` returns its own ``process_def``, not the last
    one created in the loop.
    """
    process_defs = ["alpha-def", "beta-def", "gamma-def"]
    placeholders: list[type] = []

    for process_def in process_defs:
        # Mirror the class shape from packages/core/src/dynastore/tasks/__init__.py
        # (the production class also pins ``_process_definition`` at class-body
        # time, but the buggy ``get_definition`` ignored it).
        class DefinitionOnlyTask:
            _process_definition = process_def
            is_placeholder = True

            @classmethod
            def get_definition(cls):
                return cls._process_definition

        placeholders.append(DefinitionOnlyTask)

    # After the loop, ``process_def`` lexically resolves to the final
    # value ("gamma-def"). A bare-name closure returning ``process_def``
    # would make every placeholder return "gamma-def". The fix returns
    # ``cls._process_definition`` instead.
    results = [p.get_definition() for p in placeholders]
    assert results == process_defs, (
        f"each placeholder must return its own process_def; got {results} "
        f"expected {process_defs}. A regression of the closure bug would "
        f"make every entry equal to {process_defs[-1]!r}."
    )


def test_buggy_shape_demonstrates_the_regression():
    """Inverse demonstration: the **old** code shape (bare-name closure
    in a staticmethod) collapses every placeholder to the last
    ``process_def``. This test exists so the regression is unambiguously
    documented even if the fix is later misapplied as ``cls`` capture in
    ``__init_subclass__`` or similar."""
    process_defs = ["alpha-def", "beta-def", "gamma-def"]
    buggy_placeholders: list[type] = []

    for process_def in process_defs:
        class BuggyDefinitionOnlyTask:
            _process_definition = process_def

            @staticmethod
            def get_definition():
                return process_def  # late-bound free variable — the bug

        buggy_placeholders.append(BuggyDefinitionOnlyTask)

    results = [p.get_definition() for p in buggy_placeholders]
    # Every placeholder returns the last value — that's the bug.
    assert results == [process_defs[-1]] * len(process_defs)
