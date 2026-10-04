# 临时变更联动重排

本项目维护临时变更联动重排的领域约定、角色边界与样例数据，并提供完整 Python 后端：
将**变更请求、影响场次、资源冲突、候选方案、通知事件**纳入统一流程，审批后在同一事务
更新排班并写入待投递消息，解决“学生已收到通知但资源尚未调整”的一致性问题。

## 目录

- `domain/contract.json`：领域角色、状态、约束和样例。
- `src/domain_contract/`：契约读取与确定性校验。
- `src/rescheduling/`：后端服务（模型 / 持久化 / 影响分析 / 统一流程 / HTTP API）。
- `tools/check_contract.py`：命令行契约摘要检查。
- `tools/run_server.py`：启动后端服务。
- `tests/`：契约回归 + 服务、API、并发测试。

## 架构

```
src/rescheduling/
├── models.py    状态机与常量（请求/影响/方案/消息状态）
├── errors.py    领域错误 → HTTP 状态映射
├── store.py     SQLite 持久化；BEGIN IMMEDIATE 串行化并发写
├── planner.py   影响分析、冲突检测、候选方案生成（纯读取，预演与执行复用）
├── service.py   统一流程：创建→预演→审批→执行→续办/回滚，同事务写排班+通知
├── api.py       标准库 HTTP API（无第三方依赖）
└── seed.py      演示数据（V1-V3 场馆 / G1-G3 讲解员 / SE1-SE4 场次）
```

### 请求状态机

```
pending_approval ──approve(部分接受)──▶ approved ──execute──▶ executing ─┬─▶ executed
      │                                  │                              ├─▶ partially_executed
      ├─reject──▶ rejected               │                              └─▶ failed ──resume──▶ executing
      └─preview（可反复预演）             │         executed/partially_executed/failed ──rollback──▶ rolled_back
```

### 关键保证（对应契约约束）

| 契约约束 | 实现 |
| --- | --- |
| 变更影响图 | 创建即预演：`planner.analyze` 输出受影响场次 + 候选方案 + 冲突标注；`POST /change-requests/dry-run` 支持无状态预演 |
| 方案预演比较 | 每个影响场次生成改派/取消候选，确定性排序（无冲突优先、分数降序、编号升序），冲突逐条标注 |
| 排班通知一致性 | 每个场次的改排与其待投递消息（outbox）在**同一事务**提交；消息只有排班生效后才可投递 |
| 失败续办恢复 | 执行步骤逐条落库（`execution_steps`），失败请求可从断点 `resume`，可同时为失败项改选方案 |

其余工程保证：

- **部分接受**：审批按影响场次逐项接受/驳回；未解决项保持锁定并在 `GET /change-requests/{id}/conflicts` 可见。
- **幂等**：创建支持 `Idempotency-Key`（重放返回原结果，内容不同报 409）；执行步骤与通知消息均有唯一去重键，重复执行/续办不产生重复改排或重复通知。
- **并发确定序**：写事务 `BEGIN IMMEDIATE` 串行化；乐观版本号拒绝过期决策；同一场次由**最早创建**的活跃请求持锁，后到者预演即见 `session_under_change` 冲突；执行前对所选方案做实时冲突复检（容量/占用/闭馆/请假）。
- **回滚**：按相反顺序恢复场次快照，并在同事务写入撤销通知；回滚幂等。

## API 摘要

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| GET | `/health` `/venues` `/guides` `/groups` `/sessions` | 基础数据查询 |
| POST | `/sessions` | 新建场次（容量/占用/闭馆/请假冲突校验） |
| POST | `/change-requests` | 创建变更（头 `Idempotency-Key` 幂等），自动预演影响图 |
| POST | `/change-requests/dry-run` | 无状态预演，不落库 |
| GET | `/change-requests` `/change-requests/{id}` | 列表 / 详情（影响、方案、决策、步骤） |
| POST | `/change-requests/{id}/preview` | 审批前重新预演 |
| POST | `/change-requests/{id}/approve` | 审批（`decisions` 逐项接受/驳回，`expected_version` 乐观锁） |
| POST | `/change-requests/{id}/reject` | 驳回 |
| POST | `/change-requests/{id}/execute` | 执行（幂等；`simulate_failure_at` 故障演练） |
| POST | `/change-requests/{id}/resume` | 从失败/中断点续办，可改选方案 |
| POST | `/change-requests/{id}/rollback` | 回滚已生效步骤并写撤销通知 |
| GET | `/change-requests/{id}/conflicts` | 未解决冲突视图 |
| GET | `/change-requests/{id}/audit` | 审计轨迹 |
| GET | `/outbox` `POST /outbox/dispatch` | 待投递消息查询 / FIFO 投递 |

错误格式统一：`{"error": {"code", "message", "details"}}`（400 校验 / 404 不存在 / 409 状态·版本·幂等冲突）。

## 运行

```bash
# 启动服务（内存库 + 演示数据）
python3 tools/run_server.py --port 8080
# 持久化到文件
python3 tools/run_server.py --db var/rescheduling.db --port 8080
```

典型流程（闭馆 V1 上午时段）：

```bash
curl -X POST localhost:8080/change-requests -H 'Idempotency-Key: demo-1' \
  -d '{"type":"venue_closure","resource_id":"V1","window_start":"2026-10-10T08:00:00",
       "window_end":"2026-10-10T12:00:00","reason":"消防演练"}'
curl -X POST localhost:8080/change-requests/<id>/approve -d '{"decisions":[...]}'
curl -X POST localhost:8080/change-requests/<id>/execute -d '{}'
curl localhost:8080/change-requests/<id>/conflicts
curl -X POST localhost:8080/outbox/dispatch -d '{}'
```

## 验证

测试命令：`python3 -m unittest discover -s tests -v`

编译命令：`python3 -m compileall -q src tools tests`

命令行检查：`python3 tools/check_contract.py domain/contract.json`
