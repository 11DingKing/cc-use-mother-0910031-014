"""领域常量与时间工具。

场次状态取自 domain/contract.json（筹备/待确认/已排定/执行中/已结算），
契约文件缺失时回退到内置常量，保证服务独立可用。
"""
from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path

# 变更类型
VENUE_CLOSURE = "VENUE_CLOSURE"  # 场馆临时闭馆
GUIDE_LEAVE = "GUIDE_LEAVE"      # 讲解员请假
CHANGE_TYPES = (VENUE_CLOSURE, GUIDE_LEAVE)

RESOURCE_VENUE = "VENUE"
RESOURCE_GUIDE = "GUIDE"

# 变更请求状态机
DRAFT = "DRAFT"
PREVIEWED = "PREVIEWED"
SUBMITTED = "SUBMITTED"
APPROVED = "APPROVED"                      # 方案已冻结，等待/可执行
EXECUTING = "EXECUTING"                    # Saga 执行中（单写者锁）
PARTIALLY_APPLIED = "PARTIALLY_APPLIED"    # 部分步骤已完成，停在失败点
APPLIED = "APPLIED"                        # 全部步骤完成
FAILED = "FAILED"                          # 首步即失败，停在失败点
ROLLING_BACK = "ROLLING_BACK"              # 补偿 Saga 执行中
ROLLED_BACK = "ROLLED_BACK"                # 补偿完成
VOID = "VOID"                              # 申请作废（未生效，终态）

STATUS_LABELS = {
    DRAFT: "草稿",
    PREVIEWED: "已预演",
    SUBMITTED: "待审批",
    APPROVED: "已批准",
    EXECUTING: "执行中",
    PARTIALLY_APPLIED: "部分生效",
    APPLIED: "已生效",
    FAILED: "执行中断",
    ROLLING_BACK: "回滚中",
    ROLLED_BACK: "已回滚",
    VOID: "已作废",
}

# 尚未冻结方案、可能在之后插队审批的状态
UNFROZEN = (DRAFT, PREVIEWED, SUBMITTED)
# 可以作废申请的状态（方案尚未产生现实效果）
VOIDABLE = (DRAFT, PREVIEWED, SUBMITTED)
# 方案已冻结、可能影响后续变更预演与执行顺序的状态
FROZEN_ACTIVE = (APPROVED, EXECUTING, PARTIALLY_APPLIED, FAILED, ROLLING_BACK)
# 现实不可用窗口仍在生效的状态（含已全部执行）
WINDOW_ACTIVE = FROZEN_ACTIVE + (APPLIED,)
# 可以（重新）驱动执行的状态
EXECUTABLE = (APPROVED, PARTIALLY_APPLIED, FAILED)
# 可以发起补偿回滚的状态（APPROVED 尚未生效时撤销等价于零步补偿）
ROLLBACKABLE = (APPLIED, PARTIALLY_APPLIED, FAILED, ROLLING_BACK, APPROVED)
# 终态
TERMINAL = (APPLIED, ROLLED_BACK, VOID)

# 场次决策
ACT_RESCHEDULE = "RESCHEDULE"
ACT_CANCEL = "CANCEL"
DECISION_ACTIONS = (ACT_RESCHEDULE, ACT_CANCEL)

# 计划步骤
STEP_MARK = "MARK_UNAVAILABLE"
STEP_RESCHEDULE = "RESCHEDULE_SESSION"
STEP_CANCEL = "CANCEL_SESSION"

STEP_PENDING = "PENDING"
STEP_DONE = "DONE"
STEP_FAILED = "FAILED"

MSG_PENDING = "PENDING"
MSG_DELIVERED = "DELIVERED"

_FALLBACK_STATES = ("筹备", "待确认", "已排定", "执行中", "已结算")


def contract_states() -> tuple[str, ...]:
    """读取契约中的场次状态；读取失败时回退内置值。"""
    try:  # pragma: no cover - 与部署路径相关
        from domain_contract.validator import load_contract

        root = Path(__file__).resolve().parents[2]
        contract = load_contract(root / "domain" / "contract.json")
        return tuple(contract["states"])
    except Exception:
        return _FALLBACK_STATES


SESSION_STATES = contract_states()
# 只有未进入执行/结算的场次允许联动调整
MOVABLE_STATES = tuple(s for s in SESSION_STATES if s not in ("执行中", "已结算"))


def parse_ts(value: str) -> datetime:
    return datetime.fromisoformat(value)


def to_iso(dt: datetime) -> str:
    return dt.isoformat(timespec="minutes")


def shift_ts(value: str, days: int) -> str:
    return to_iso(parse_ts(value) + timedelta(days=days))


def overlaps(start_a: str, end_a: str, start_b: str, end_b: str) -> bool:
    """半开区间 [start, end) 是否重叠。"""
    return start_a < end_b and start_b < end_a
