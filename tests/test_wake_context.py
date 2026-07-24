from __future__ import annotations

from datetime import UTC, datetime

import pytest

from project.research.wake_context import (
    GranularApprovalPolicy,
    WakeContext,
    WakeContextValidationError,
    normalize_approval_policy,
)

CAPTURED_AT = datetime(2026, 7, 23, 12, 0, tzinfo=UTC)


def test_standard_wake_context_round_trips_and_builds_resume_params() -> None:
    context = WakeContext(
        thread_id="thread-a",
        permission_profile="restricted",
        approval_policy="on-request",
        captured_at=CAPTURED_AT,
    )

    assert WakeContext.from_dict(context.to_dict()) == context
    assert context.resume_params() == {
        "threadId": "thread-a",
        "permissions": "restricted",
        "approvalPolicy": "on-request",
    }


def test_unnamed_permission_profile_round_trips_without_resume_override() -> None:
    context = WakeContext(
        thread_id="thread-a",
        permission_profile=None,
        approval_policy="never",
        captured_at=CAPTURED_AT,
    )

    assert WakeContext.from_dict(context.to_dict()) == context
    assert context.to_dict()["permission_profile"] is None
    assert context.resume_params() == {
        "threadId": "thread-a",
        "approvalPolicy": "never",
    }


def test_granular_approval_policy_is_normalized_and_round_trips() -> None:
    context = WakeContext(
        thread_id="thread-a",
        permission_profile="custom",
        approval_policy={
            "granular": {
                "mcp_elicitations": True,
                "request_permissions": True,
                "rules": False,
                "sandbox_approval": True,
                "skill_approval": True,
            }
        },
        captured_at=CAPTURED_AT,
    )

    assert isinstance(context.approval_policy, GranularApprovalPolicy)
    assert normalize_approval_policy(context.approval_policy) is context.approval_policy
    assert WakeContext.from_dict(context.to_dict()) == context
    assert context.resume_params()["approvalPolicy"] == {
        "granular": {
            "mcp_elicitations": True,
            "request_permissions": True,
            "rules": False,
            "sandbox_approval": True,
            "skill_approval": True,
        }
    }


@pytest.mark.parametrize(
    ("value", "message"),
    (
        (
            {
                "thread_id": "",
                "permission_profile": "restricted",
                "approval_policy": "never",
                "captured_at": CAPTURED_AT.isoformat(),
            },
            "thread_id must be a non-empty string",
        ),
        (
            {
                "thread_id": "thread-a",
                "permission_profile": "bad\nprofile",
                "approval_policy": "never",
                "captured_at": CAPTURED_AT.isoformat(),
            },
            "permission_profile must not contain control characters",
        ),
        (
            {
                "thread_id": "thread-a",
                "permission_profile": "restricted",
                "approval_policy": "always",
                "captured_at": CAPTURED_AT.isoformat(),
            },
            "unsupported approval policy",
        ),
        (
            {
                "thread_id": "thread-a",
                "permission_profile": "restricted",
                "approval_policy": {"granular": "invalid"},
                "captured_at": CAPTURED_AT.isoformat(),
            },
            "approval_policy.granular must be an object",
        ),
        (
            {
                "thread_id": "thread-a",
                "permission_profile": "restricted",
                "approval_policy": {"granular": {"rules": True}},
                "captured_at": CAPTURED_AT.isoformat(),
            },
            "approval_policy.granular has invalid fields",
        ),
        (
            {
                "thread_id": "thread-a",
                "permission_profile": "restricted",
                "approval_policy": {
                    "granular": {
                        "mcp_elicitations": True,
                        "rules": "yes",
                        "sandbox_approval": True,
                    }
                },
                "captured_at": CAPTURED_AT.isoformat(),
            },
            "approval_policy.granular fields must be booleans",
        ),
        (
            {
                "thread_id": "thread-a",
                "permission_profile": "restricted",
                "approval_policy": "never",
                "captured_at": "not-a-time",
            },
            "captured_at must be an ISO 8601 string",
        ),
    ),
)
def test_invalid_serialized_wake_context_is_rejected(
    value: dict[str, object],
    message: str,
) -> None:
    with pytest.raises(WakeContextValidationError, match=message):
        WakeContext.from_dict(value)


@pytest.mark.parametrize(
    "value",
    (
        None,
        {},
        {
            "thread_id": "thread-a",
            "permission_profile": "restricted",
            "approval_policy": "never",
            "captured_at": CAPTURED_AT.isoformat(),
            "unexpected": True,
        },
    ),
)
def test_wake_context_requires_exact_fields(value: object) -> None:
    with pytest.raises(WakeContextValidationError, match="invalid fields"):
        WakeContext.from_dict(value)


def test_wake_context_requires_timezone_aware_capture_time() -> None:
    with pytest.raises(WakeContextValidationError, match="UTC offset"):
        WakeContext(
            thread_id="thread-a",
            permission_profile="restricted",
            approval_policy="never",
            captured_at=datetime(2026, 7, 23),
        )


def test_granular_policy_requires_the_outer_granular_field() -> None:
    with pytest.raises(WakeContextValidationError, match="supported app-server policy"):
        normalize_approval_policy({"invalid": {}})
