# 操作契约：Renew

> 初版仅使用 Redis。每次只续租一个任务；租约仅以 lease ZSET 的 score 为准。

## 1. 操作目的

Server 定期对本地明确接收并管理的执行尝试续租。续租表示该执行尝试仍由本地管理，不证明 handler 一定在推进。

Server 遍历本地续租集合，对每个任务分别调用 Renew。不能仅因进程在线就为该 owner 名下所有任务续租。

## 2. 调用方与输入

- 调用方：Server 的续租模块。
- 单任务输入：task_id、对应的 attempt_token、lease_duration（本次租约时长）。
- task_id、attempt_token 必填且非空；lease_duration 为正值，来自平台运行配置，不由业务提交方随意指定。
- attempt_token 属于每次任务执行尝试，不是每个 Server 共用一个。
- 不传入 Server 的绝对到期时间戳；Lua 使用 Redis TIME 计算。
- 时长换算和时间相加应检查溢出。

## 3. 执行前置条件

在 Lua 内检查：

1. 任务详情存在且 status = running。
2. 输入 attempt_token 与当前记录一致。
3. active 中存在该任务；异常索引按一致性错误处理。
4. lease ZSET 存在该 task_id，且当前 Redis 时间严格小于其 score。

仅存在于 lease ZSET 不代表租约有效。now 等于 score 时已到期，不能复活。

本地只为已接收并管理、尚未停止续租的任务发起请求；本地超时、已知取消、明确失去执行权或上报流程最终结束后，应从续租集合移除。

## 4. 成功后的变化与返回值

status 保持 running，Lua 设置：

    lease ZSET 的 member = task_id
    新 score = Redis 当前 Unix 毫秒 + lease_duration（毫秒）

不改变 owner_id、attempt_token、attempts、原始 execution_timeout、retry_delay 或本地超时定时器。不在任务详情写入 lease_expires_at，不设置 execution_deadline_at。

返回 applied、error，针对本次单任务操作：

| 情况 | applied | error |
| --- | --- | --- |
| 续租生效且收到确认 | true | nil |
| 任务不存在、非 running、token 不匹配、lease 缺失或已到期 | false | nil |
| 参数或一致性错误 | false | 可识别错误 |
| 存储、网络错误或响应丢失 | 无确定生效结论 | 对应错误 |

只有 error 为 nil 时，才能用 applied 判定本次结果；error 不为 nil 不能解释为确定未续租。

## 5. 原子性与不变量

输入、数据类型、当前状态、token、active、当前 lease score 的检查与新 score 写入，在同一 Lua 中完成，使用同一轮 Redis 时间判断。

- 仅当前有效执行身份可以续租。
- 已到期租约不能复活；非 running 任务不能续租。
- 续租不增加 attempts，不切换任务状态。
- 续租不重置 Server 本地 execution_timeout。
- 初版租约不按 execution_timeout 截断，Redis 不执行存储端硬截止时间检查。

## 6. 失败及结果未知时的行为

- 参数错误：不原样重试，应记录并修正调用。
- 明确返回 applied = false 且 error = nil：停止该任务续租，向本地 handler 发出 context 取消；不再以该身份发起结果上报。
- 执行槽只能在 handler 真正退出后归还一次，不能在发出取消信号或收到续租拒绝时提前归还。
- 存储/网络错误或响应丢失：本次不立即重试，也不单凭该错误取消 handler 或归还槽；等待下一次续租定时器。
- 下一次请求仍须重新检查当前状态、token、score，不承接上次可能存在的权限。
- 本地 execution_timeout 到期时，仍按 Claim、ReportFailure 的规则停止续租并上报超时，不因等待下一次 Renew 而延长本地定时器。

## 7. 并发竞争规则

- Reclaim 先回收并重新 Claim：新 token 生效，旧 token Renew 被拒绝。
- Renew 在租约有效期内先生效：Reclaim 在脚本中看到新的未到期 score，不回收。
- 已到期但尚未回收：即使 token 相同，也必须拒绝 Renew。
- Cancel 先进入 canceled：Renew 因状态不符被拒绝。
- complete、ReportFailure 先迁移状态并移除 lease：Renew 被拒绝。
- Renew 先成功不阻止之后的 Cancel、complete、ReportFailure；它们仍各自检查当前前置条件。

## 8. 重复请求与幂等规则

每次调用均重新校验权限。重复有效续租按各次 Redis 当前时间计算 score，不保证重复请求返回完全相同的 score。

不建立额外请求去重记录，不增加 attempts，不新增重复 lease 成员。首次响应丢失时等待下一次定时续租，不能据此声称首次未生效。

## 9. 崩溃与恢复行为及待开发功能

- Server 崩溃后停止续租，已授予的任务仍保存在 Redis，由 Reclaim 到期处理；仅本地管理信息随进程丢失。
- 重启使用新的 owner_id，不恢复旧轮次的续租。
- 授予执行权消耗尝试预算，未完成的尝试按预算恢复，不保证 handler 必定启动。
- Redis 重启的持久性取决于部署配置；Lua 错误不自动回滚此前写入。脚本部分写入状态的自动检测、对账及修复不属于第一版。

待开发：Server 本地维护租约有效时间，并在无法确认续租、已超过有效期时主动停止 handler。第一版不实现这项保护。

因此网络持续异常期间，旧 handler 可能在 Redis 租约失效、被回收后继续执行，直到收到明确拒绝、本地超时或自行退出。Redis 的 token、状态和 score 检查保护平台数据，不能消除重复执行的业务副作用。

## 10. 验收场景

- 正常单任务续租：只有对应 task_id 的 score 更新，状态、token、attempts 不变。
- 租约到期但仍留在 ZSET：拒绝续租，不能因成员存在而续租成功。
- now 等于 score：按到期处理。
- 回收并重新领取后以旧 token 续租：拒绝，不改变新 score。
- Cancel 或结果上报先成功：续租被拒绝。
- 续租响应丢失：不立即归还槽，下一次定时调用重新判断。
- 明确续租拒绝且 handler 尚未退出：停止续租、发出取消，实际退出后才归还槽。
- 本地超时触发：停止续租，不重置执行定时器。
- 本地租约有效期判断仅列为待开发，不把它当作第一版已实现的验收能力。
