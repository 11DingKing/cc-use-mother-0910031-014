# 临时变更联动重排

场馆临时闭馆、讲解员请假后，把「变更请求 → 影响场次 → 资源冲突 → 候选方案 → 审批 → 排班更新 → 通知」
放进**同一套有序流程**，杜绝「学生已收到通知、资源却尚未调整」。

纯 Python 标准库 + SQLite，零第三方依赖。

## 领域模型与核心保证

| 契约不变量 | 实现位置 |
| --- | --- |
| 变更影响图 | `planning.py` 影响识别、候选方案、冲突检测 |
| 方案预演比较 | `planning.py` 多候选（顺延天数 × 同/换馆 × 同/换讲解员，确定性 rank） |
| 排班通知一致性 | `executor.py` 每步「排班更新 + 待投递消息」**同一事务**（Transactional Outbox） |
| 失败续办恢复 | `executor.py` 有序 Saga：前向/补偿幂等，停在失败点，可安全续办 |

### 流程与状态机

```
草稿 DRAFT → 已预演 PREVIEWED → 待审批 SUBMITTED
          → 已批准 APPROVED → 执行中 EXECUTING → 已生效 APPLIED
                                   ↓ 失败停点
                             FAILED / PARTIALLY_APPLIED ──续办 execute──► APPLIED
                                   └──────────────── 补偿回滚 rollback ──► ROLLED_BACK
```

- **预演（可重复）**：基于「**投影排班**」——现存场次叠加所有前序已冻结变更的待执行步骤，
  并计入闭馆/请假的现实不可用窗口；产出影响场次、候选方案、未解决硬冲突。
- **审批（部分接受）**：逐场次选择候选 `option_id` 重排或 `CANCEL` 取消；存在未解决硬冲突
  （执行中/已结算场次不可动、无可用槽位且未决定取消）时拒绝审批。
- **冻结与指纹**：决策冻结为确定性步骤序列（`MARK → 按场次时间排序的改期/取消`），
  执行前逐场比对前态 `version`，排班被外部改动则**停在该失败点**，不覆盖他人修改。
- **确定顺序**：并发变更按 `changes.seq` 定序——审批闸、执行闸（前序未完后序不跑）、
  回滚逆序闸；`exec_lock` 全局单写者 + 每变更互斥 + `BEGIN IMMEDIATE` 串行化写事务。
- **Outbox**：通知在步骤事务内写 `outbox`（去重键含方案指纹），由投递器至少一次发送；
  因此**绝不可能**出现排班未动而通知先发，反之亦然。
- **重复执行/续办**：DONE 步骤重放跳过、消息 `INSERT OR IGNORE` 幂等；
  失败步骤重新校验前态后可重试。最后一步提交后崩溃也正确识别为成功。
- **回滚**：逆序补偿，恢复排班并写「排班恢复/闭馆解除」消息；ROLLBACK 后可重新预演审批。
- **重复提交**：`Idempotency-Key` 相同的创建请求返回原变更。

## 目录

- `domain/contract.json`：领域角色、状态、约束和样例。
- `src/domain_contract/`：契约读取与确定性校验。
- `src/rescheduling/`：后端实现。
  - `domain.py` 常量/状态机；`errors.py` 稳定错误码（含 HTTP 状态）；`clock.py` 可重放时钟。
  - `storage.py` SQLite 层（schema、`BEGIN IMMEDIATE`、进程内写锁）。
  - `planning.py` 投影排班、影响图、候选方案、冲突、冻结步骤与指纹。
  - `executor.py` 有序 Saga、Outbox 消息、单写者锁、顺序闸、失败点/补偿。
  - `service.py` 应用门面；`api.py` 标准库 HTTP 服务；`seed.py` 演示数据。
- `tools/check_contract.py`：命令行摘要检查。
- `tests/`：契约与业务回归测试（17 个用例，含并发、漂移、失败注入与 HTTP）。

## 快速开始

```bash
# 初始化演示数据并启动
PYTHONPATH=src python3 -m rescheduling --db data/app.db --port 8080
```

### API 总览

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/admin/venues` `/admin/guides` | 场馆/讲解员登记 |
| POST/GET | `/sessions` | 场次维护 |
| POST/GET | `/changes` | 创建变更（支持 `Idempotency-Key`） |
| GET | `/changes/{id}` | 详情：冻结步骤、未解决冲突 |
| POST/GET | `/changes/{id}/preview` | **预演影响/候选/冲突** / 查看预演 |
| POST | `/changes/{id}/submit` | 提交审批 |
| POST | `/changes/{id}/approve` | 部分接受并冻结：`{"approver","decisions":{场次:{action,option_id}}}` |
| POST | `/changes/{id}/reject` `/void` | 驳回 / 作废未生效申请 |
| POST | `/changes/{id}/execute` | 执行或**从失败点续办**（幂等） |
| POST | `/changes/{id}/rollback` | 补偿回滚（幂等） |
| GET | `/changes/{id}/messages` | 待投递/已投递通知事件 |
| POST | `/messages/deliver` | 投递待发送消息 |
| GET | `/changes/{id}/events` `/events` | 有序事件流 |

### 示例

```bash
curl -X POST localhost:8080/changes -H 'Idempotency-Key: k-1' -d '{
  "change_type":"VENUE_CLOSURE","resource_id":"V1",
  "unavailable_start":"2026-10-10T00:00","unavailable_end":"2026-10-10T23:59",
  "reason":"场馆检修"}'
curl -X POST localhost:8080/changes/C-xxxx/preview      # 看影响与未解决冲突
curl -X POST localhost:8080/changes/C-xxxx/submit
curl -X POST localhost:8080/changes/C-xxxx/approve -d '{"approver":"甲","decisions":{}}'
curl -X POST localhost:8080/changes/C-xxxx/execute      # 失败后重发同一请求即续办
curl -X POST localhost:8080/messages/deliver
```

## 验证

```bash
python3 -m unittest discover -s tests -v
python3 -m compileall -q src tools tests
python3 tools/check_contract.py domain/contract.json
```
