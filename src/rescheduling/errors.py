"""统一错误类型，携带 HTTP 状态码与稳定错误码。"""
from __future__ import annotations

from typing import Any


class ApiError(Exception):
    """所有可预期业务错误的基类。"""

    http_status = 400
    code = "BAD_REQUEST"

    def __init__(self, message: str, details: dict[str, Any] | None = None):
        super().__init__(message)
        self.message = message
        self.details = details or {}

    def to_dict(self) -> dict[str, Any]:
        return {"code": self.code, "message": self.message, "details": self.details}


class ValidationError(ApiError):
    http_status = 400
    code = "VALIDATION_ERROR"


class NotFoundError(ApiError):
    http_status = 404
    code = "NOT_FOUND"


class ConflictError(ApiError):
    http_status = 409
    code = "CONFLICT"


class UnresolvedConflictError(ApiError):
    http_status = 409
    code = "UNRESOLVED_CONFLICT"


class BlockedByEarlierChangeError(ApiError):
    http_status = 409
    code = "BLOCKED_BY_EARLIER_CHANGE"


class ExecutionStalledError(ApiError):
    http_status = 409
    code = "EXECUTION_STALLED"


class RollbackStalledError(ApiError):
    http_status = 409
    code = "ROLLBACK_STALLED"
