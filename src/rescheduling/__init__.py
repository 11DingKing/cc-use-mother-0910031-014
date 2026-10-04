"""临时变更联动重排后端：变更请求、影响场次、资源冲突、候选方案与通知事件的统一流程。"""
from .errors import (
    DomainError,
    IdempotencyMismatchError,
    NotFoundError,
    StateConflictError,
    ValidationError,
    VersionConflictError,
)
from .models import ChangeType, ImpactStatus, RequestState, SessionState
from .seed import seed_if_empty
from .service import ChangeService
from .store import Store

__version__ = "1.0.0"

__all__ = [
    "ChangeService",
    "ChangeType",
    "DomainError",
    "IdempotencyMismatchError",
    "ImpactStatus",
    "NotFoundError",
    "RequestState",
    "SessionState",
    "StateConflictError",
    "Store",
    "ValidationError",
    "VersionConflictError",
    "seed_if_empty",
]
