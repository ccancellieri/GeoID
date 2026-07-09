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

"""Regression tests for geoid#3132: credential-bearing config must never
reach Cloud Run logs, including inside a formatted traceback.

``RedactingLogFilter`` only rewrites ``record.msg``/``record.args`` before
formatting — it never sees ``record.exc_info``, whose rendered traceback
text (via ``Formatter.formatException``) can independently carry a raw
DSN on the exception's own message line. ``_JsonFormatter`` scrubs that
text directly. The DSN below is a fake, obviously-not-real placeholder.
"""
from __future__ import annotations

import json
import logging

from dynastore.main import _JsonFormatter

_FAKE_DSN = "postgresql://dbuser:FAKEPASS@db-host:5432/gis"


def test_traceback_with_dsn_in_exception_message_is_redacted():
    import sys

    try:
        raise RuntimeError(f"could not parse SQLAlchemy URL: {_FAKE_DSN}")
    except RuntimeError:
        exc_info = sys.exc_info()
        record = logging.LogRecord(
            name="test",
            level=logging.CRITICAL,
            pathname=__file__,
            lineno=1,
            msg="DBService: FATAL: Failed to create database connection pool",
            args=(),
            exc_info=exc_info,
        )

    line = _JsonFormatter().format(record)
    assert "FAKEPASS" not in line
    payload = json.loads(line)
    assert "FAKEPASS" not in payload["exception"]
    assert "db-host:5432/gis" in payload["exception"]


def test_no_exc_info_omits_exception_key():
    record = logging.LogRecord(
        name="test", level=logging.INFO, pathname=__file__, lineno=1,
        msg="plain message", args=(), exc_info=None,
    )
    line = _JsonFormatter().format(record)
    payload = json.loads(line)
    assert "exception" not in payload
