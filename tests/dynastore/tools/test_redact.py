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

"""Unit tests for ``dynastore.tools.redact`` (geoid#3132).

Every credential used below is a fake, obviously-not-real placeholder
(``FAKEPASS``, ``FAKETOKEN``) — never a real secret. Tests assert the
fake value never appears in the redacted / formatted output.
"""
from __future__ import annotations

import dataclasses
import datetime
import decimal
import logging
import uuid
from typing import Optional

import pytest
from pydantic import BaseModel

from dynastore.tools.redact import RedactingLogFilter, redact_for_log
from dynastore.tools.secrets import Secret

_FAKE_DSN = "postgresql://dbuser:FAKEPASS@db-host:5432/gis"
_FAKE_TOKEN = "FAKETOKEN-abc123"


def test_bare_secret_string_under_sensitive_key_is_fully_masked():
    out = redact_for_log(_FAKE_TOKEN, key="api_key")
    assert out == "***"
    assert "FAKETOKEN" not in out


def test_uri_userinfo_is_redacted_but_shape_preserved():
    out = redact_for_log(_FAKE_DSN, key="database_url")
    assert "FAKEPASS" not in out
    assert "dbuser" not in out
    assert out.startswith("postgresql://***:***@")
    assert "db-host:5432/gis" in out


def test_plain_url_without_userinfo_is_untouched():
    """A key literally named 'url' that holds no credentials (no
    userinfo) must not be destructively masked — only userinfo is ever
    stripped, so diagnostic URLs without credentials survive intact."""
    out = redact_for_log("https://example.org/webhook", key="callback_url")
    assert out == "https://example.org/webhook"


def test_uri_embedded_in_free_text_is_scrubbed_regardless_of_key():
    """A DSN embedded in an exception/log message (no key context) must
    still be caught by the URI-userinfo scan."""
    text = f"connection failed: {_FAKE_DSN}"
    out = redact_for_log(text)
    assert "FAKEPASS" not in out
    assert "db-host:5432/gis" in out


def test_dict_recurses_and_masks_sensitive_keys():
    payload = {
        "host": "db-host",
        "password": "FAKEPASS",
        "nested": {"token": _FAKE_TOKEN, "note": "ok"},
    }
    out = redact_for_log(payload)
    assert out["host"] == "db-host"
    assert out["password"] == "***"
    assert out["nested"]["token"] == "***"
    assert out["nested"]["note"] == "ok"
    assert "FAKEPASS" not in str(out)
    assert "FAKETOKEN" not in str(out)


def test_list_of_dicts_recurses():
    payload = [{"dsn": _FAKE_DSN}, {"dsn": "postgresql://a@b/c"}]
    out = redact_for_log(payload)
    assert "FAKEPASS" not in str(out)
    assert out[0]["dsn"].startswith("postgresql://***")


def test_dataclass_fields_are_redacted():
    @dataclasses.dataclass
    class ConnSettings:
        host: str
        password: str

    out = redact_for_log(ConnSettings(host="db-host", password="FAKEPASS"))
    assert out == {"host": "db-host", "password": "***"}


def test_pydantic_model_field_named_password_is_masked():
    class DriverOptions(BaseModel):
        host: str
        password: str

    out = redact_for_log(DriverOptions(host="db-host", password="FAKEPASS"))
    assert out["host"] == "db-host"
    assert out["password"] == "***"
    assert "FAKEPASS" not in str(out)


def test_pydantic_model_with_secret_field_stays_masked():
    class DriverOptions(BaseModel):
        model_config = {"arbitrary_types_allowed": True}
        connection_url: Optional[Secret] = None

    model = DriverOptions(connection_url=Secret(_FAKE_DSN))
    out = redact_for_log(model)
    assert "FAKEPASS" not in str(out)


def test_sqlalchemy_url_hides_password():
    from sqlalchemy.engine import make_url

    url = make_url("postgresql://dbuser:FAKEPASS@db-host:5432/gis")
    out = redact_for_log(url)
    assert "FAKEPASS" not in out
    assert "db-host" in out


def test_exception_message_is_redacted():
    exc = RuntimeError(f"could not connect: {_FAKE_DSN}")
    out = redact_for_log(exc)
    assert "FAKEPASS" not in out
    assert "db-host:5432/gis" in out


def test_non_sensitive_values_pass_through_unchanged():
    assert redact_for_log(42) == 42
    assert redact_for_log(True) is True
    assert redact_for_log(None) is None
    assert redact_for_log("hello world") == "hello world"


def _capture_log(record_kwargs: dict) -> logging.LogRecord:
    record = logging.LogRecord(
        name="test", level=logging.INFO, pathname=__file__, lineno=1,
        **record_kwargs,
    )
    RedactingLogFilter().filter(record)
    return record


def test_filter_redacts_percent_style_args():
    record = _capture_log(
        {"msg": "DBService: Using DB configuration: %s", "args": (_FAKE_DSN,), "exc_info": None}
    )
    formatted = record.getMessage()
    assert "FAKEPASS" not in formatted
    assert "db-host:5432/gis" in formatted


def test_filter_redacts_already_formatted_fstring_message():
    """A call site that f-string-interpolated the DSN before ever calling
    the logger — the second-layer safety net the filter exists for."""
    record = _capture_log(
        {"msg": f"DBService: Using DB configuration: {_FAKE_DSN}", "args": (), "exc_info": None}
    )
    formatted = record.getMessage()
    assert "FAKEPASS" not in formatted
    assert "db-host:5432/gis" in formatted


def test_filter_redacts_mapping_style_args():
    # Mapping-style logging args reach LogRecord as a one-tuple holding the
    # mapping (``logger.info("conn=%(dsn)s", {...})``); LogRecord unwraps it.
    record = _capture_log(
        {"msg": "conn=%(dsn)s", "args": ({"dsn": _FAKE_DSN},), "exc_info": None}
    )
    formatted = record.getMessage()
    assert "FAKEPASS" not in formatted


def test_filter_never_drops_the_record_on_redaction_error():
    """The filter must return True (keep the record) even if something
    inside redaction misbehaves — a logging filter must never be the
    reason a log line silently vanishes."""
    record = logging.LogRecord(
        name="test", level=logging.INFO, pathname=__file__, lineno=1,
        msg="plain message", args=(), exc_info=None,
    )
    assert RedactingLogFilter().filter(record) is True


@pytest.mark.parametrize(
    "key",
    ["password", "passwd", "pwd", "secret", "token", "api_key", "apikey",
     "authorization", "dsn", "database_url"],
)
def test_all_suggested_key_classes_mask_bare_secrets(key):
    out = redact_for_log(_FAKE_TOKEN, key=key)
    assert out == "***"


def test_credential_free_scalars_pass_through_with_type_preserved():
    for value in (
        decimal.Decimal("1203519.860405"),
        uuid.UUID("019f1326-645d-72bf-9c17-7451e825ca34"),
        datetime.datetime(2026, 7, 13, 9, 50, 30),
        datetime.date(2026, 7, 13),
        datetime.timedelta(seconds=90),
    ):
        assert redact_for_log(value) is value


def test_filter_keeps_numeric_format_args_working():
    """Regression: the filter repr()'d a Decimal ``%.0f`` arg into a str,
    so ``record.getMessage()`` raised ``TypeError: must be real number,
    not str`` and the record was destroyed (stuck-pending warner WARN,
    dev 2026-07-13)."""
    record = _capture_log({
        "msg": "stuck-pending: task '%s' has been PENDING for %.0fs",
        "args": (
            uuid.UUID("019f1326-645d-72bf-9c17-7451e825ca34"),
            decimal.Decimal("1203519.860405"),
        ),
        "exc_info": None,
    })
    formatted = record.getMessage()
    assert "task '019f1326-645d-72bf-9c17-7451e825ca34'" in formatted
    assert "PENDING for 1203520s" in formatted
