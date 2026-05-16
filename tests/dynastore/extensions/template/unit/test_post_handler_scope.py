"""Regression test for the template extension's POST /interpolate handler.

Background: ``interpolate_template_post`` and ``interpolate_template_get``
both live in ``TemplatingExtension``'s class body (each decorated with
``@router.post`` / ``@router.get`` on the class-level ``router`` attr).
The POST handler delegated to the GET handler by bare name::

    return await interpolate_template_get(request=request, ...)

Python nested-function name resolution cannot see class-body names, so
every POST to ``/template/interpolate`` raised ``NameError`` at request
time. A ``# type: ignore[name-defined]`` comment marked the suspect call
but the runtime impact was missed for months. The fix qualifies the call
through the class (``TemplatingExtension.interpolate_template_get(...)``)
so name resolution succeeds at runtime.

This test pins both halves:

* The qualified call site survives — bare-name fallback would mean a
  regression is back.
* The POST handler can be invoked without raising ``NameError``.
"""
from __future__ import annotations

import inspect

import pytest


def test_post_handler_uses_class_qualified_call_to_get_handler():
    """The POST body must not contain a bare ``interpolate_template_get(``
    call — that pattern broke at request time because nested functions
    cannot see class-body names."""
    pytest.importorskip("jinja2")
    from dynastore.extensions.template import templating

    src = inspect.getsource(templating.TemplatingExtension.interpolate_template_post)
    assert "TemplatingExtension.interpolate_template_get(" in src, (
        "POST handler must call the GET handler via the class — bare-name "
        "delegation NameErrors at runtime (see PR fixing this)."
    )
    assert "await interpolate_template_get(" not in src, (
        "Bare-name call regressed; nested-function name resolution cannot "
        "see class-body names."
    )


def test_post_handler_does_not_nameerror_on_get_handler():
    """Empirical guard: invoking the POST handler must not raise
    ``NameError`` for the GET-handler symbol. Any later failure is fine
    here — we are pinning only the scope-resolution bug."""
    pytest.importorskip("jinja2")
    import asyncio

    from dynastore.extensions.template.templating import TemplatingExtension

    fake_req = type(
        "FakeInterpolationRequest",
        (),
        {
            "t": "{{x}}",
            "t_url": None,
            "t_url_h": None,
            "m_urls": None,
            "m_urls_h": None,
            "m": None,
            "r_mt": "text/html",
            "ru": None,
            "ru_h": None,
            "et": False,
            "ae": False,
            "mm": False,
            "stream": False,
        },
    )()

    try:
        asyncio.run(TemplatingExtension.interpolate_template_post(fake_req))
    except NameError as exc:  # pragma: no cover - the regression we are pinning
        pytest.fail(
            "NameError surfaced from POST handler — class-body scope bug is "
            f"back: {exc}"
        )
    except Exception:
        # Any non-NameError is acceptable here — the bug was scope, not
        # the downstream template/IO machinery.
        pass
