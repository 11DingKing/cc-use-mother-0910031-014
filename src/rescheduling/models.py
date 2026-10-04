"""领域模型：状态机、类型与确定性常量。

状态机对应 domain/contract.json 中的契约状态（筹备/待确认/已排定/执行中/已结算），
并补充变更流程自身的请求状态、影响状态与方案状态。
"""
from __future__ import annotations

from enum import Enum


class SessionState(str, Enum):
    """场次状态（前五个来自领域契约，已取消为变更流程引入）。"""

    PREPARING = "筹备"
    PENDING_CONFIRM = "待确认"
    SCHEDULED = "已排定"
    IN_PROGRESS = "执行中"
    SETTLED = "已结算"
    CANCELLED = "已取消"


#: 占用资源（场馆/讲解员）的场次状态，冲突检测以之为准。
OCCUPYING_SESSION_STATES = (
    SessionState.PREPARING.value,
    SessionState.PENDING_CONFIRM.value,
    SessionState.SCHEDULED.value,
    SessionState.IN_PROGRESS.value,
)

#: 可被变更请求影响的场次状态（执行中与已结算不再改排）。
IMPACTABLE_SESSION_STATES = (
    SessionState.PREPARING.value,
    SessionState.PENDING_CONFIRM.value,
    SessionState.SCHEDULED.value,
)


class ChangeType(str, Enum):
    """变更类型：场馆临时闭馆 / 讲解员请假。"""

    VENUE_CLOSURE = "venue_closure"
    GUIDE_LEAVE = "guide_leave"


class RequestState(str, Enum):
    """变更请求状态机。

    pending_approval → approved → executing → executed / partially_executed / failed
                    ↘ rejected                ↘（失败/中断后）resume 回到 executing
    executed / partially_executed / failed → rolled_back
    """

    PENDING_APPROVAL = "pending_approval"
    APPROVED = "approved"
    EXECUTING = "executing"
    EXECUTED = "executed"
    PARTIALLY_EXECUTED = "partially_executed"
    FAILED = "failed"
    REJECTED = "rejected"
    ROLLED_BACK = "rolled_back"


#: 仍持有“场次锁”的请求状态：同一场次同一时间只允许一个活跃变更。
LOCK_REQUEST_STATES = (
    RequestState.PENDING_APPROVAL.value,
    RequestState.APPROVED.value,
    RequestState.EXECUTING.value,
    RequestState.FAILED.value,
    RequestState.PARTIALLY_EXECUTED.value,
)

#: 资源不可用窗口生效的请求状态（已批准的闭馆/请假即视为资源不可用）。
UNAVAILABLE_RESOURCE_STATES = (
    RequestState.APPROVED.value,
    RequestState.EXECUTING.value,
    RequestState.FAILED.value,
    RequestState.EXECUTED.value,
    RequestState.PARTIALLY_EXECUTED.value,
)


class ImpactType(str, Enum):
    VENUE_UNAVAILABLE = "venue_unavailable"
    GUIDE_UNAVAILABLE = "guide_unavailable"


class ImpactStatus(str, Enum):
    """影响场次的处理状态。"""

    PENDING = "pending"  # 已识别，待决策
    PLANNED = "planned"  # 已选定候选方案
    APPLIED = "applied"  # 已生效
    SKIPPED = "skipped"  # 未接受，保持未解决
    FAILED = "failed"  # 执行失败，可续办
    ROLLED_BACK = "rolled_back"  # 已回滚


#: 仍占用场次锁的影响状态。
OPEN_IMPACT_STATUSES = (
    ImpactStatus.PENDING.value,
    ImpactStatus.PLANNED.value,
    ImpactStatus.FAILED.value,
)


class OptionKind(str, Enum):
    """候选方案类型：改派资源（换场馆/换讲解员）或取消场次。"""

    REASSIGN = "reassign"
    CANCEL = "cancel"


class DecisionAction(str, Enum):
    ACCEPT = "accept"
    REJECT = "reject"


class OutboxStatus(str, Enum):
    PENDING = "pending"
    DELIVERED = "delivered"
