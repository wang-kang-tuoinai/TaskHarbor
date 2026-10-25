# 操作契约：complete

> 初版仅使用 Redis。一次调用只提交一个任务的一轮成功结果，成功终态统一为 completed。

## 1. 操作目的

Server 在 handler 成功返回、且本地未将该轮裁定为超时或失去执行权时，提交执行结果并结束该轮执行权。

## 2. 调用方与输入

- 调用方：Server 的结果上报流程。
- 输入：单个 task_id、该轮 attempt_token、序列化后的 result。
- task_id、attempt_token 必填且非空；无业务结果时 result 可以为空，但须采用统一空值约定并遵守平台大小限制。
- 不接收任务列表，不由调用方指定目标状态、当前 attempts 或完成时间。
- 相同上报的网络重试复用相同 task_id、attempt_token、result。

## 3. 执行前置条件

在 Lua 内检查：

1. 任务详情存在，status = running。
2. attempt_token 与当前执行身份一致。
3. active 中存在任务，lease 中存在该 task_id。
4. Redis 当前时间严格小于 lease ZSET 的 score；相等视为过期。

不设置或读取取消标记，任务为 canceled 或其他非 running 状态时直接拒绝。

execution_timeout 由 Server 本地控制。handler 成功和超时处理只选一个本地分支；裁定超时后不再发起 complete。Redis 不保存 execution_deadline_at，也不按执行时长额外拒绝成功上报。

## 4. 成功后的变化与返回值

状态变化：running → completed。

同一 Lua 中完成：

- 保存 result，设置 status = completed、finish_at = Redis 当前 Unix 毫秒。
- 从 active、lease 移除 task_id。
- 撤销 owner_id、attempt_token，清除 next_retry_at（如有）。
- 不加入 pending 或 retry，不增加 attempts，不改原始配置，不清空历史失败信息。

返回 applied、error：

| 情况 | applied | error |
| --- | --- | --- |
| 本次完成生效 | true | nil |
| 不存在、状态不符、token 不符、lease 缺失或已到期 | false | nil |
| 参数或一致性错误 | false | 可识别错误 |
| 存储/网络错误、响应丢失 | 无确定生效结论 | 对应错误 |

applied 仅在 error 为 nil 时有确定含义。收到拒绝不等于 handler 没成功执行，也不证明前一次 complete 没有成功。

## 5. 原子性与不变量

所有前置检查、结果和终态写入、索引移除及执行身份撤销在同一 Lua 中执行。输入、数据类型及可预见的计算错误应在修改前校验。

- completed 不再拥有当前执行身份或等待、执行索引。
- 旧 token 不得修改新一轮任务的状态、结果或租约。
- complete 不增加 attempts，不主动重试业务执行。
- 取消后不补报成功或错误，不覆盖 canceled。

## 6. 失败及结果未知时的行为

- 参数错误或已识别的一致性错误：返回错误并记录，不盲目原样重试或假定已经修复。
- applied = false 且 error = nil：不重试本次 complete，停止该任务续租，结束上报流程。
- 网络/存储异常或结果未知：最多额外重试两次，含首次最多三次；重试使用相同参数。
- 任何一次收到确定成功或前置条件拒绝后，立即停止后续重试。
- 重试耗尽后停止续租，从续租集合移除并结束本地上报管理；若 Redis 仍为完整的 running、active、lease 状态，由 Reclaim 到期处理。
- 正常完成上报期间如仍保留续租，只能有界地服务于本地明确管理的该轮；上报流程结束后不能继续续租。
- 执行槽只能在 handler 真正退出后归还一次。停止续租和清理上报信息不构成提前归还仍存活 handler 执行槽的理由。

本地已经完成业务副作用但结果未提交成功，回收后仍可能再次执行，业务需要自行处理副作用幂等。

## 7. 并发竞争规则

以 Redis 原子生效顺序及当前前置条件决定结果：

- complete 先成功：后续 Cancel 返回 already_terminal，ReportFailure、Renew 被拒绝。
- Cancel 先成功：complete 因状态为 canceled 被拒绝。
- ReportFailure 先迁移：complete 因非 running 被拒绝。
- Reclaim 先回收：complete 因状态或 token 不符被拒绝。
- 租约已过期但尚未回收：即使 complete 先于回收执行，也必须拒绝。
- Renew 在有效期内先延长租约：complete 按更新后的 score 判断，不是两个操作只能成功一个。

## 8. 重复请求与幂等规则

首次成功后，重复请求因非 running 被拒绝，不重复改结果、finish_at 或执行次数。

若任务已重试并被重新领取，旧请求还会因 token 不匹配被拒绝。这里保证重复请求不会再次产生状态修改，不保证返回与首次相同的结果，也不新增完成请求去重记录。

首次成功但响应丢失后，再次收到 applied = false 不能证明首次失败；可查询当前状态，但不自动重入队。

## 9. 崩溃与恢复行为及已知限制

- Server 在提交生效前崩溃：完整 running 记录由租约到期回收。
- 已生效但响应前崩溃：Redis 保留 completed，重复上报不再修改。
- Redis 自身故障的持久性依赖部署配置，不能由 Lua 推导无条件不丢结果。
- Lua 运行时错误可能保留此前写入；初版不自动检测、对账或修复部分写入状态。只有仍满足完整 running、active、lease 关系的任务，才能依赖正常回收流程。
- 初版没有本地租约有效期维护；网络异常时旧 handler 可能继续运行，Redis 仍通过状态、token 和 score 拒绝失效上报。
- Redis 不按 execution_timeout 强制拒绝所有迟到成功，本地暂停、处理延迟和在途请求的限制与 Submit、Claim 一致。

## 10. 验收场景

- 正常单任务完成：completed、result、finish_at 正确，active、lease 及当前身份清除，attempts 不变。
- canceled 或其他非 running：拒绝，不覆盖终态及错误信息。
- token 不符、租约到期或缺失：拒绝，不修改新一轮数据。
- 完成成功但响应丢失：重复请求不重复写入；允许返回前置条件不满足。
- 前置条件拒绝：不继续重试；网络错误：最多额外重试两次。
- 上报始终失败：停止续租，完整的 running 任务到期进入回收路径。
- complete 与 Renew、Cancel、ReportFailure、Reclaim 交错：符合第 7 节。
- 本地超时先被裁定：不再发起该轮 complete；不把 Redis 存储端硬截止校验列为已实现能力。
