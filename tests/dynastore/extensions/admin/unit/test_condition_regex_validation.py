#    Copyright 2026 FAO
#    Licensed under the Apache License, Version 2.0 (the "License").

"""Write-time regex validation (Layer B) for policy conditions and grant quotas.

These are pure-unit tests: no DB, no app lifespan, no HTTP stack.
Pydantic validation is triggered by constructing DTOs directly; the
422-on-invalid-pattern guarantee is enforced by Pydantic raising
``ValidationError`` which FastAPI maps to HTTP 422.
"""
from __future__ import annotations

import pytest
from pydantic import ValidationError

from dynastore.models.protocols.policies import (
    CreateBindingRequest,
    PolicyCreate,
    PolicyUpdate,
    _validate_condition_regex_patterns,
)
from dynastore.models.auth import Condition


# ---------------------------------------------------------------------------
# _validate_condition_regex_patterns helper
# ---------------------------------------------------------------------------


class TestValidateConditionRegexPatterns:
    def test_valid_rate_limit_path_pattern_passes(self):
        conditions = [Condition(type="rate_limit", config={"path_pattern": "^/tiles/"})]
        _validate_condition_regex_patterns(conditions)  # must not raise

    def test_invalid_rate_limit_path_pattern_raises(self):
        conditions = [Condition(type="rate_limit", config={"path_pattern": "*bad("})]
        with pytest.raises(ValueError, match="path_pattern"):
            _validate_condition_regex_patterns(conditions)

    def test_invalid_max_count_path_pattern_raises(self):
        conditions = [Condition(type="max_count", config={"path_pattern": "["})]
        with pytest.raises(ValueError, match="path_pattern"):
            _validate_condition_regex_patterns(conditions)

    def test_invalid_query_match_pattern_raises(self):
        conditions = [Condition(type="query_match", config={"pattern": "(?P<unclosed"})]
        with pytest.raises(ValueError, match="query_match"):
            _validate_condition_regex_patterns(conditions)

    def test_invalid_match_regex_value_raises(self):
        conditions = [
            Condition(type="match", config={"attribute": "path", "operator": "regex", "value": "*bad("})
        ]
        with pytest.raises(ValueError, match="match.*regex"):
            _validate_condition_regex_patterns(conditions)

    def test_match_non_regex_operator_not_validated(self):
        # "eq" operator carries a literal string value — must NOT be regex-compiled.
        conditions = [
            Condition(type="match", config={"attribute": "path", "operator": "eq", "value": "*bad("})
        ]
        _validate_condition_regex_patterns(conditions)  # must not raise

    def test_empty_conditions_passes(self):
        _validate_condition_regex_patterns([])  # must not raise

    def test_no_pattern_field_passes(self):
        conditions = [Condition(type="rate_limit", config={"limit": 10, "window_seconds": 60})]
        _validate_condition_regex_patterns(conditions)  # must not raise


# ---------------------------------------------------------------------------
# PolicyCreate — invalid condition raises ValidationError (→ HTTP 422)
# ---------------------------------------------------------------------------


class TestPolicyCreateValidation:
    def _base(self, **kwargs) -> dict:
        return {"id": "p1", "actions": ["GET"], "resources": [".*"], **kwargs}

    def test_invalid_rate_limit_path_pattern_raises_422(self):
        with pytest.raises(ValidationError) as exc_info:
            PolicyCreate(
                **self._base(conditions=[
                    {"type": "rate_limit", "config": {"limit": 10, "window_seconds": 60, "path_pattern": "*bad("}}
                ])
            )
        assert "path_pattern" in str(exc_info.value)

    def test_invalid_query_match_pattern_raises_422(self):
        with pytest.raises(ValidationError) as exc_info:
            PolicyCreate(
                **self._base(conditions=[
                    {"type": "query_match", "config": {"param": "format", "pattern": "["}}
                ])
            )
        assert "query_match" in str(exc_info.value)

    def test_valid_conditions_pass(self):
        pc = PolicyCreate(
            **self._base(conditions=[
                {"type": "rate_limit", "config": {"limit": 10, "window_seconds": 60, "path_pattern": "^/tiles/"}}
            ])
        )
        assert pc.conditions[0].type == "rate_limit"

    def test_no_conditions_passes(self):
        pc = PolicyCreate(**self._base())
        assert pc.conditions == []


# ---------------------------------------------------------------------------
# PolicyUpdate — invalid condition raises ValidationError (→ HTTP 422)
# ---------------------------------------------------------------------------


class TestPolicyUpdateValidation:
    def test_invalid_max_count_path_pattern_raises_422(self):
        with pytest.raises(ValidationError) as exc_info:
            PolicyUpdate(conditions=[
                {"type": "max_count", "config": {"limit": 100, "path_pattern": "(?P<unclosed"}}
            ])
        assert "path_pattern" in str(exc_info.value)

    def test_none_conditions_pass(self):
        pu = PolicyUpdate(conditions=None)
        assert pu.conditions is None

    def test_valid_conditions_pass(self):
        pu = PolicyUpdate(conditions=[
            {"type": "max_count", "config": {"limit": 100}}
        ])
        assert pu.conditions[0].type == "max_count"


# ---------------------------------------------------------------------------
# CreateBindingRequest.quota — invalid path_pattern raises ValidationError
# ---------------------------------------------------------------------------


class TestCreateBindingRequestQuotaValidation:
    def _base(self, quota: dict) -> dict:
        from uuid import uuid4
        return {
            "principal_id": str(uuid4()),
            "object_kind": "role",
            "object_ref": "viewer",
            "quota": quota,
        }

    def test_invalid_rate_limit_path_pattern_raises_422(self):
        with pytest.raises(ValidationError) as exc_info:
            CreateBindingRequest(**self._base({
                "rate_limit": {"limit": 10, "window_seconds": 60, "path_pattern": "*bad("}
            }))
        assert "path_pattern" in str(exc_info.value)

    def test_invalid_max_count_path_pattern_raises_422(self):
        with pytest.raises(ValidationError) as exc_info:
            CreateBindingRequest(**self._base({
                "max_count": {"limit": 100, "path_pattern": "["}
            }))
        assert "path_pattern" in str(exc_info.value)

    def test_valid_quota_passes(self):
        from uuid import uuid4
        cbr = CreateBindingRequest(
            principal_id=str(uuid4()),
            object_kind="role",
            object_ref="viewer",
            quota={"rate_limit": {"limit": 10, "window_seconds": 60, "path_pattern": "^/tiles/"}},
        )
        assert cbr.quota["rate_limit"]["path_pattern"] == "^/tiles/"

    def test_no_quota_passes(self):
        from uuid import uuid4
        cbr = CreateBindingRequest(
            principal_id=str(uuid4()),
            object_kind="role",
            object_ref="viewer",
        )
        assert cbr.quota is None
