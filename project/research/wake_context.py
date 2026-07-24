"""Validated Codex permission context for managed lifecycle wakeups."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, cast

WAKE_CONTEXT_FILENAME = "wake-context.json"
CODEX_THREAD_ENVIRONMENT_VARIABLE = "CODEX_THREAD_ID"
CODEX_PERMISSION_PROFILE_ENVIRONMENT_VARIABLE = "CODEX_PERMISSION_PROFILE"
STANDARD_APPROVAL_POLICIES = frozenset({"untrusted", "on-request", "never"})
REQUIRED_GRANULAR_FIELDS = frozenset({"mcp_elicitations", "rules", "sandbox_approval"})
OPTIONAL_GRANULAR_FIELDS = frozenset({"request_permissions", "skill_approval"})
WAKE_CONTEXT_FIELDS = frozenset(
    {"thread_id", "permission_profile", "approval_policy", "captured_at"}
)


class WakeContextValidationError(ValueError):
    """A wake context does not match the persisted schema."""


def _validated_text(value: object, field_name: str) -> str:
    if not isinstance(value, str) or not value or len(value) > 512:
        raise WakeContextValidationError(f"{field_name} must be a non-empty string")
    if any(ord(character) < 32 or ord(character) == 127 for character in value):
        raise WakeContextValidationError(f"{field_name} must not contain control characters")
    return value


def _validated_permission_profile(value: object) -> str | None:
    if value is None:
        return None
    return _validated_text(value, "permission_profile")


def _normalized_datetime(value: datetime, field_name: str) -> datetime:
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
        raise WakeContextValidationError(f"{field_name} must include a UTC offset")
    return value.astimezone(UTC)


@dataclass(frozen=True, slots=True)
class GranularApprovalPolicy:
    """Immutable form of app-server's granular approval policy."""

    mcp_elicitations: bool
    rules: bool
    sandbox_approval: bool
    request_permissions: bool = False
    skill_approval: bool = False

    @classmethod
    def from_value(cls, value: object) -> GranularApprovalPolicy:
        if not isinstance(value, Mapping) or set(value) != {"granular"}:
            raise WakeContextValidationError(
                "approval_policy must be a supported app-server policy"
            )
        granular = value["granular"]
        if not isinstance(granular, Mapping):
            raise WakeContextValidationError("approval_policy.granular must be an object")
        fields = frozenset(granular)
        allowed_fields = REQUIRED_GRANULAR_FIELDS | OPTIONAL_GRANULAR_FIELDS
        if not fields >= REQUIRED_GRANULAR_FIELDS or not fields <= allowed_fields:
            raise WakeContextValidationError("approval_policy.granular has invalid fields")
        if not all(isinstance(granular[field], bool) for field in fields):
            raise WakeContextValidationError("approval_policy.granular fields must be booleans")
        return cls(
            mcp_elicitations=granular["mcp_elicitations"],
            rules=granular["rules"],
            sandbox_approval=granular["sandbox_approval"],
            request_permissions=granular.get("request_permissions", False),
            skill_approval=granular.get("skill_approval", False),
        )

    def to_dict(self) -> dict[str, object]:
        return {
            "granular": {
                "mcp_elicitations": self.mcp_elicitations,
                "request_permissions": self.request_permissions,
                "rules": self.rules,
                "sandbox_approval": self.sandbox_approval,
                "skill_approval": self.skill_approval,
            }
        }


ApprovalPolicy = str | GranularApprovalPolicy
ApprovalPolicyInput = ApprovalPolicy | Mapping[str, object]


def normalize_approval_policy(value: object) -> ApprovalPolicy:
    if isinstance(value, str):
        if value not in STANDARD_APPROVAL_POLICIES:
            raise WakeContextValidationError(f"unsupported approval policy: {value!r}")
        return value
    if isinstance(value, GranularApprovalPolicy):
        return value
    return GranularApprovalPolicy.from_value(value)


def approval_policy_payload(value: ApprovalPolicy) -> str | dict[str, object]:
    return value if isinstance(value, str) else value.to_dict()


@dataclass(frozen=True, slots=True)
class WakeContext:
    """The effective app-server authority captured before a managed run starts."""

    thread_id: str
    permission_profile: str | None
    approval_policy: ApprovalPolicyInput
    captured_at: datetime

    def __post_init__(self) -> None:
        object.__setattr__(self, "thread_id", _validated_text(self.thread_id, "thread_id"))
        object.__setattr__(
            self,
            "permission_profile",
            _validated_permission_profile(self.permission_profile),
        )
        object.__setattr__(
            self,
            "approval_policy",
            normalize_approval_policy(self.approval_policy),
        )
        object.__setattr__(
            self,
            "captured_at",
            _normalized_datetime(self.captured_at, "captured_at"),
        )

    @classmethod
    def from_dict(cls, value: object) -> WakeContext:
        if not isinstance(value, Mapping) or frozenset(value) != WAKE_CONTEXT_FIELDS:
            raise WakeContextValidationError("wake context has invalid fields")
        captured_at = value["captured_at"]
        if not isinstance(captured_at, str):
            raise WakeContextValidationError("captured_at must be an ISO 8601 string")
        try:
            parsed_at = datetime.fromisoformat(captured_at.replace("Z", "+00:00"))
        except ValueError as error:
            raise WakeContextValidationError("captured_at must be an ISO 8601 string") from error
        return cls(
            thread_id=_validated_text(value["thread_id"], "thread_id"),
            permission_profile=_validated_permission_profile(value["permission_profile"]),
            approval_policy=normalize_approval_policy(value["approval_policy"]),
            captured_at=parsed_at,
        )

    def to_dict(self) -> dict[str, Any]:
        approval_policy = cast(ApprovalPolicy, self.approval_policy)
        return {
            "thread_id": self.thread_id,
            "permission_profile": self.permission_profile,
            "approval_policy": approval_policy_payload(approval_policy),
            "captured_at": self.captured_at.isoformat(),
        }

    def resume_params(self) -> dict[str, Any]:
        approval_policy = cast(ApprovalPolicy, self.approval_policy)
        params = {
            "threadId": self.thread_id,
            "approvalPolicy": approval_policy_payload(approval_policy),
        }
        if self.permission_profile is not None:
            params["permissions"] = self.permission_profile
        return params
