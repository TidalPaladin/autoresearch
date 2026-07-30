"""Shared notify-wake authority model used by research event producers."""

from __future__ import annotations

from notify_wake.models import ModelError, WakeContext, normalize_approval_policy

WAKE_CONTEXT_FILENAME = "wake-context.json"
CODEX_THREAD_ENVIRONMENT_VARIABLE = "CODEX_THREAD_ID"
CODEX_PERMISSION_PROFILE_ENVIRONMENT_VARIABLE = "CODEX_PERMISSION_PROFILE"
WakeContextValidationError = ModelError

__all__ = [
    "CODEX_PERMISSION_PROFILE_ENVIRONMENT_VARIABLE",
    "CODEX_THREAD_ENVIRONMENT_VARIABLE",
    "WAKE_CONTEXT_FILENAME",
    "WakeContext",
    "WakeContextValidationError",
    "normalize_approval_policy",
]
