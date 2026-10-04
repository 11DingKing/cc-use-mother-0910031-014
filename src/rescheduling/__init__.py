"""临时变更联动重排后端。

统一流程：变更请求 -> 影响预演 -> 冲突/候选方案 -> 审批冻结 ->
有序 Saga 执行（排班与待投递消息同事务）-> 投递 / 补偿回滚。
"""
from __future__ import annotations

from .clock import Clock, SystemClock
from .service import Service

__all__ = ["Service", "Clock", "SystemClock", "version"]

version = "0.2.0"
