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

"""Log-safe redaction of credential-bearing values (geoid#3132).

A connection-URI-class secret (DSN, API key, bearer token, ...) is only
ever one accidental ``f"{config}"`` or ``logger.info(..., config)`` away
from a Cloud Logging entry — Cloud Logging is replicated and searchable,
so a single leaked line is a real exposure even in dev. This module is
the call-site helper: wrap a settings object, driver-options mapping, or
connection error before it reaches a logger call.

``redact_for_log`` walks the value recursively:

* Mappings and sequences are rebuilt with every leaf redacted. Sequences
  (list/tuple/set/frozenset) are normalized to a ``list`` — this is a
  log-safe *rendering*, not a round-trip serializer, so exact container
  type is not preserved.
* Dataclasses and Pydantic v2 models are expanded field-by-field (a
  Pydantic model goes through ``model_dump(mode="python")`` first, which
  also invokes any field-level masking such as
  :class:`dynastore.tools.secrets.Secret`).
* A ``sqlalchemy.engine.URL`` renders via its own
  ``render_as_string(hide_password=True)``.
* A ``BaseException`` is redacted via its ``str()`` message — the same
  text a bare ``logger.warning("...: %s", exc)`` would have emitted.
* Every string leaf is scanned for an embedded ``scheme://user:pass@host``
  fragment and has only the userinfo replaced, regardless of key —
  scheme, host, port, and path (e.g. the database name) survive so the
  redacted line stays useful for diagnosis. A leaf reached through a
  key matching one of the sensitive classes below (``password``,
  ``token``, ``dsn``, ...) is additionally fully masked when it does not
  look like a URI (a bare secret / token / password has no shape worth
  keeping).

:class:`dynastore.tools.secrets.Secret` instances need no special case
here — their own ``__str__``/``__repr__`` already return the ``***``
mask, and the fallback branch below renders unknown objects through
``repr()``.

Second layer: :class:`RedactingLogFilter` wraps this helper as a
``logging.Filter`` so a future call site that stringifies a credential
directly into the log message (bypassing this helper entirely) is still
scrubbed for the URI-userinfo case before the record is emitted. The
filter cannot recover key-name context once a message has already been
formatted into plain prose, so it is a safety net for the URI class of
leak specifically — call sites remain responsible for using
``redact_for_log`` on structured values before logging them.
"""

from __future__ import annotations

import dataclasses
import logging
import re
from collections.abc import Mapping, Sequence
from typing import Any, Optional

from pydantic import BaseModel
from sqlalchemy.engine import URL as SQLAlchemyURL

_MASK = "***"

# Key classes called out by geoid#3132's suggested fix. Matched against a
# normalized (lowercased, non-alnum -> "_") key, both as a whole and as an
# underscore-split token — so "DATABASE_URL", "connection_url", and
# "apiKey" all match via the "url" / "apikey" tokens below.
_SENSITIVE_KEY_TOKENS = frozenset(
    {
        "password",
        "passwd",
        "pwd",
        "secret",
        "token",
        "api_key",
        "apikey",
        "authorization",
        "dsn",
        "database_url",
        "url",
        "uri",
    }
)

_NON_ALNUM_RE = re.compile(r"[^a-z0-9]+")

# Matches "scheme://userinfo@" and captures the userinfo so it (and only
# it) can be replaced. Deliberately does not touch host/port/path — those
# are the diagnostic shape we want to keep.
_URI_USERINFO_RE = re.compile(r"([A-Za-z][A-Za-z0-9+.\-]*://)([^/@\s]+)@")


def _is_sensitive_key(key: str) -> bool:
    normalized = _NON_ALNUM_RE.sub("_", key.lower()).strip("_")
    if normalized in _SENSITIVE_KEY_TOKENS:
        return True
    return bool(set(normalized.split("_")) & _SENSITIVE_KEY_TOKENS)


def _redact_uri_userinfo(text: str) -> str:
    def _sub(match: "re.Match[str]") -> str:
        scheme, userinfo = match.group(1), match.group(2)
        return f"{scheme}{_MASK}:{_MASK}@" if ":" in userinfo else f"{scheme}{_MASK}@"

    return _URI_USERINFO_RE.sub(_sub, text)


def _redact_string(value: str, key: Optional[str]) -> str:
    if key is not None and _is_sensitive_key(key) and "://" not in value:
        return _MASK
    return _redact_uri_userinfo(value)


def redact_for_log(value: Any, *, key: Optional[str] = None) -> Any:
    """Return a log-safe copy of ``value`` with credential-shaped data masked.

    ``key`` is the field/parameter name ``value`` was reached under, if
    any — used to decide whether a bare (non-URI) string should be fully
    masked. Pass it explicitly when redacting a single named value, e.g.
    ``redact_for_log(raw_dsn, key="database_url")``.
    """
    if value is None or isinstance(value, (bool, int, float)):
        return value
    if isinstance(value, SQLAlchemyURL):
        return value.render_as_string(hide_password=True)
    if isinstance(value, str):
        return _redact_string(value, key)
    if isinstance(value, BaseException):
        return _redact_string(str(value), key)
    if isinstance(value, BaseModel):
        try:
            dumped = value.model_dump(mode="python")
        except Exception:
            return f"<unrenderable {type(value).__name__}>"
        return redact_for_log(dumped)
    if dataclasses.is_dataclass(value) and not isinstance(value, type):
        return {
            f.name: redact_for_log(getattr(value, f.name, None), key=f.name)
            for f in dataclasses.fields(value)
        }
    if isinstance(value, Mapping):
        return {k: redact_for_log(v, key=str(k)) for k, v in value.items()}
    if isinstance(value, (list, tuple, set, frozenset)):
        return [redact_for_log(v, key=key) for v in value]
    if isinstance(value, (bytes, bytearray)):
        return _redact_string(repr(value), key)
    # Unknown object (exception subclass already handled above, plain
    # class instance, etc.) — render through repr() and scrub that text.
    try:
        return _redact_string(repr(value), key)
    except Exception:
        return f"<unrenderable {type(value).__name__}>"


class RedactingLogFilter(logging.Filter):
    """Second-layer safety net: scrub log records before they are emitted.

    Rewrites ``record.msg`` and every positional/mapping arg via
    :func:`redact_for_log`. Covers the case a call site forgot to redact a
    structured value passed as a ``%s`` arg (args are redacted with their
    original type preserved where possible, so ``%d``/``%s`` formatting
    still works) as well as an already-formatted f-string message that
    embeds a ``scheme://user:pass@host`` fragment.

    Best-effort: any exception while redacting a given record is
    swallowed and that piece left as-is rather than dropping the log
    line entirely — a filter must never be the reason a record disappears.
    """

    def filter(self, record: logging.LogRecord) -> bool:
        try:
            if isinstance(record.msg, str):
                record.msg = redact_for_log(record.msg)
        except Exception:
            pass
        if record.args:
            try:
                if isinstance(record.args, Mapping):
                    record.args = {
                        k: redact_for_log(v, key=str(k))
                        for k, v in record.args.items()
                    }
                elif isinstance(record.args, Sequence) and not isinstance(
                    record.args, str
                ):
                    record.args = tuple(redact_for_log(a) for a in record.args)
            except Exception:
                pass
        return True


__all__ = ["redact_for_log", "RedactingLogFilter"]
