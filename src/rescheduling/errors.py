"""领域错误：统一错误码与 HTTP 状态映射。"""
from __future__ import annotations


class DomainError(Exception):
    """所有可预期业务错误的基类。"""

    code = "DOMAIN_ERROR"
    http_status = 400

    def __init__(self, message: str, details: dict | None = None) -> None:
        super().__init__(message)
        self.message = message
        self.details = details or {}

    def to_dict(self) -> dict:
        return {"code": self.code, "message": self.message, "details": self.details}


class ValidationError(DomainError):
    code = "VALIDATION"
    http_status = 400


class NotFoundError(DomainError):
    code = "NOT_FOUND"
    http_status = 404


class StateConflictError(DomainError):
    """状态机不允许的操作，或资源冲突。"""

    code = "STATE_CONFLICT"
    http_status = 409


class VersionConflictError(DomainError):
    """乐观锁版本不一致：存在并发变更，需刷新后重试。"""

    code = "VERSION_CONFLICT"
    http_status = 409


class IdempotencyMismatchError(DomainError):
    """同一幂等键提交了不同内容。"""

    code = "IDEMPOTENCY_MISMATCH"
    http_status = 409
