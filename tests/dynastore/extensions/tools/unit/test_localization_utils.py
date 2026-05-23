#    Copyright 2025 FAO
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

"""Unit tests for the shared use_lang detection helper."""

from dynastore.extensions.tools.localization_utils import (
    _I18N_FIELDS,
    detect_use_lang,
)


def test_multilanguage_field_returns_star():
    data = {"title": {"en": "Hello", "fr": "Bonjour"}}
    assert detect_use_lang(data, "en") == "*"


def test_plain_value_returns_default():
    data = {"title": "Hello", "description": "World"}
    assert detect_use_lang(data, "en") == "en"


def test_empty_dict_returns_default():
    assert detect_use_lang({}, "fr") == "fr"


def test_non_i18n_dict_is_not_multilanguage():
    # A dict whose keys are not language codes must not trigger "*".
    data = {"extra_metadata": {"custom_field": "value"}}
    assert detect_use_lang(data, "en") == "en"


def test_any_field_triggers_star():
    # Only the last inspected field carries i18n input.
    data = {
        "title": "Hello",
        "extra_metadata": {"en": "meta", "it": "meta-it"},
    }
    assert detect_use_lang(data, "en") == "*"


def test_default_field_set_matches_callers():
    assert _I18N_FIELDS == (
        "title",
        "description",
        "keywords",
        "license",
        "extra_metadata",
    )


def test_custom_fields_argument_is_honored():
    data = {"label": {"en": "x", "de": "y"}}
    # Default field set ignores "label"; explicit fields picks it up.
    assert detect_use_lang(data, "en") == "en"
    assert detect_use_lang(data, "en", fields=("label",)) == "*"
