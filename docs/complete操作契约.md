# 操作契约：complete

> 初版仅使用 Redis。一次调用只提交一个任务的一轮成功结果，成功终态统一为 completed。

> 2026-11-07 已确认设计，依据 [ADR-0006](adr/0006-result-reporting-lifecycle.md)。本轮仅实现真实 complete/ReportFailure 及监督衔接，不包含 Reclaim、Cancel、Shutdown；本文不是实现完成或测试通过的证明。

## 1. 操作目的

Server 在 handler 成功返回、且本地未将该轮裁定为超时或失去执行权时，提交执行结果并结束该轮执行权。

## 2. 调用方与输入

- 调用方：Server 的结果上报流程。
- 输入：单个 task_id、该轮 attempt_token、序列化后的 result。
- task_id、attempt_token 必填且非空；result 沿用 1000 字节上限，使用 Task.SetResult 已序列化的字节，不再次 JSON 编码。
- 不接收任务列表，不由调用方指定目标状态、当前 attempts 或完成时间。
- 相同上报的网络重试复用相同 task_id、attempt_token、result。

complete 始终写入 result 字段。未调用 SetResult 时 nil 与空字节切片统一为零长度值，不省略字段；SetResult(nil) 保存四字节 JSON 字面量 null，SetResult("") 保存包含两个引号的 JSON 空字符串。零长度表示未提供业务结果，不能直接当作合法 JSON 解码；字段缺失与零长度不同。

## 3. 执行前置条件

在 Lua 内检查：

1. 任务详情存在，status = running。
2. attempt_token 与当前执行身份一致。
3. active 中存在任务，lease 中存在该 task_id。
4. Redis 当前时间严格小于 lease ZSET 的 score；相等视为过期。

不设置或读取取消标记，任务为 canceled 或其他非 running 状态时直接拒绝。

先判断任务存在、合法状态与 token，再检查当前执行所需索引；已结束任务正常清理后的索引缺失不报异常。仍 running 且 token 匹配但 active/lease 缺失、必要字段缺失、非法数值或相关 key 类型错误，返回一致性错误且不写入；索引完整但租约到期正常拒绝。只检查本操作需要的数据，不做全量修复。

execution_timeout_ms 由 Server 本地控制。handler 成功和超时处理只选一个本地分支；裁定超时后不再发起 complete。Redis 不保存 execution_deadline_at，也不按执行时长额外拒绝成功上报。

## 4. 成功后的变化与返回值

状态变化：running → completed。

同一 Lua 中完成：

- 保存 result，设置 status = completed、finished_at = Redis 当前 Unix 毫秒。
- 从 active、lease 移除 task_id。
- 撤销 owner_id、attempt_token。
- 不加入 pending 或 retry，不增加 attempts，不改原始配置，不清空历史失败信息。

返回 applied、error：

| 情况 | applied | error |
| --- | --- | --- |
| 本次完成生效 | true | nil |
| 不存在、合法状态非 running、token 不符或索引完整但租约已到期 | false | nil |
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

- 参数、一致性、明确权限或脚本错误、按本平台策略不重试的 DNS/地址错误及非法响应：不重试，结束该轮上报与续租，记录错误并调用 reportFatal 停止新领取；其他任务继续续租和上报。非法响应可能对应已生效请求，不回滚或认定未执行。
- applied = false 且 error = nil：不重试本次 complete，停止该任务续租，结束上报流程。
- 暂时性网络错误、请求超时或可重试的结果未知：最多额外重试两次，含首次最多三次；重试使用相同参数。明确非暂时性错误和非法响应不能笼统归入此类。
- 任何一次收到确定成功或前置条件拒绝后，立即停止后续重试。
- 重试耗尽后停止续租，从续租集合移除并结束本地上报管理；若 Redis 仍为完整的 running、active、lease 状态，由 Reclaim 到期处理。
- 正常完成上报期间保留该轮续租资格，Renew 模块正常时继续续租；上报流程结束后移除记录、停止续租。
- 执行槽只能在 handler 真正退出后归还一次。停止续租和清理上报信息不构成提前归还仍存活 handler 执行槽的理由。

本地已经完成业务副作用但结果未提交成功，回收后仍可能再次执行，业务需要自行处理副作用幂等。

### 6.1 两个上报操作共用的执行规则

以下规则同时适用于 complete 和 ReportFailure：

- 内部存储包执行单次操作并返回 applied、error，负责 Lua、解析和错误分类；Server 负责参数固定、预算、重试、取消、日志及致命错误；监督负责唯一分支裁定与最终清理。不得叠加多层重试，不新增公开上报 API 或通用框架。
- 每次请求最多 3 秒，保留调用方更短网络超时；重试前分别等待 100ms、200ms。数值为内部常量，不新增公开配置。最多三次请求的预算约 9.3 秒，加正常调度开销，不承诺严格墙钟返回或必定在租约内完成。
- 沿用 ADR-0005 已验证的真实网络 deadline 与共享池方式，不修改调用方配置、不新建连接池。使用独立请求预算，不直接继承 Handler 已超时取消的 context，也不绑定 Claim/Renew 循环。
- 每次请求前及重试等待期间检查已知失权、已生效取消、一致性撤销及 abort，中止后续请求；不能仅依靠外层监督 select。已在途请求不能保证撤回，最终由 Redis 校验执行权。不能通过 executions 存在性推断上报资格。
- 已知撤销先同步停止上报，再取消 Handler，即使 Handler 的首个原因已经是本地超时。父 context 的禁止原因传播到 Handler 后，由调用边界同步停止上报，异步回调可唤醒重试等待。若 Handler 已先因本地超时取消，后来只取消父 context 无法覆盖首个原因，此时须显式撤销该执行尝试。
- 启动前观察到 abort 时跳过 Handler，由接收路径清理续租资格并归还槽。已启动的 Handler 仍在实际退出时归还槽。
- DNS/地址错误沿用 [Claim 契约的共享分类](Claim操作契约.md#存储或网络异常)。暂时性 DNS 错误继续使用固定参数及既有重试预算。
- 单纯全局 Renew 故障不禁止上报。正常结果上报保留续租资格；超时上报前已删除的资格不恢复。确定成功或拒绝立即停止，耗尽后清理，不仅凭一次网络耗尽停止全实例领取。
- 结果分支选定后，原 execution_timeout_ms 不再中断上报或改判超时。超时分支选定则忽略迟到结果；同时就绪允许选其中之一，不承诺真实先后顺序。已知失权与 abort 的检查仍生效。
- Handler 实际退出即归还槽，不等上报结束；仍存活 Handler 不提前归还。上报中的任务数可能超过 Handler 并发数，本轮不增加独立上报并发限制。有限预算不是固定的上报数量上限。
- complete 失败不得转为 ReportFailure，不能把存储故障写成业务失败；不自动查询找回首次结果，不自行重新入队或退回尝试预算。

### 6.2 两个上报操作共用的日志规则

使用标准库 log/slog 的默认 logger，由应用统一配置输出，不新增依赖、WithLogger 或错误回调。Errors 仍只通知实例首个导致停止领取的后台致命错误，上报也复用该机制；后续通知被抑制仍执行处理，无人读取不能阻塞。

| 最终情况 | 记录方式 |
| --- | --- |
| 确认成功 | 不强制输出逐任务成功日志 |
| 首次请求正常拒绝 | Debug：上报未被接受，不称为业务失败 |
| 结果未知后又收到拒绝 | Warn：本轮未确认最终结果，前次可能已生效 |
| 网络重试耗尽 | Warn：结果未获确认，包含操作、taskID、请求次数及底层错误 |
| 参数、一致性、明确权限或脚本错误、非法响应 | Error，并按故障规则停止新领取 |

不为每次短暂超时重复输出 Warn；不输出 payload、业务 result 或完整业务错误正文。Handler 超时是通过 ReportFailure 保存的执行失败原因；上报网络超时是平台通信异常，两者不得混淆。

## 7. 并发竞争规则

以 Redis 原子生效顺序及当前前置条件决定结果：

- complete 先成功：后续 Cancel 返回 already_terminal，ReportFailure、Renew 被拒绝。
- Cancel 先成功：complete 因状态为 canceled 被拒绝。
- ReportFailure 先迁移：complete 因非 running 被拒绝。
- Reclaim 先回收：complete 因状态或 token 不符被拒绝。
- 租约已过期但尚未回收：即使 complete 先于回收执行，也必须拒绝。
- Renew 在有效期内先延长租约：complete 按更新后的 score 判断，不是两个操作只能成功一个。

## 8. 重复请求与幂等规则

首次成功后，重复请求因非 running 被拒绝，不重复改结果、finished_at 或执行次数。

若任务已重试并被重新领取，旧请求还会因 token 不匹配被拒绝。这里保证重复请求不会再次产生状态修改，不保证返回与首次相同的结果，也不新增完成请求去重记录。

首次成功但响应丢失后，再次收到 applied = false 不能证明首次失败；可查询当前状态，但不自动重入队。

## 9. 崩溃与恢复行为及已知限制

- Server 在提交生效前崩溃：完整 running 记录由租约到期回收。
- 已生效但响应前崩溃：Redis 保留 completed，重复上报不再修改。
- Redis 自身故障的持久性依赖部署配置，不能由 Lua 推导无条件不丢结果。
- Lua 运行时错误可能保留此前写入；初版不自动检测、对账或修复部分写入状态。只有仍满足完整 running、active、lease 关系的任务，才能依赖正常回收流程。
- 初版没有本地租约有效期维护；网络异常时旧 handler 可能继续运行，Redis 仍通过状态、token 和 score 拒绝失效上报。
- Redis 不按 execution_timeout_ms 强制拒绝所有迟到成功，本地暂停、处理延迟和在途请求的限制与 Submit、Claim 一致。

上报耗尽后仍有完整执行索引的 running 记录，由正常运行的回收循环在租约到期后处理。上报并发可能超过 Handler 并发，目前没有额外限流。[Reclaim 契约](Reclaim操作契约.md)

## 10. 验收场景

- 正常单任务完成：completed、result、finished_at 正确，active、lease 及当前身份清除，attempts 不变。
- canceled 或其他非 running：拒绝，不覆盖终态及错误信息。
- token 不符、索引完整但租约到期：正常拒绝，不修改新一轮数据；当前身份 running 缺 active/lease 为一致性错误且零写入。
- 完成成功但响应丢失：重复请求不重复写入；允许返回前置条件不满足。
- 前置条件拒绝或按本平台策略不重试的 DNS/地址错误结束本轮上报。暂时性网络错误最多额外重试两次。
- 上报始终失败：停止续租，完整的 running 任务到期进入回收路径。
- complete 与 Renew、Cancel、ReportFailure、Reclaim 交错：符合第 7 节。
- 本地超时先被裁定：不再发起该轮 complete；不把 Redis 存储端硬截止校验列为已实现能力。

- 未设置结果、JSON null、JSON 空字符串及 1000 字节边界分别验证，不重复编码、不省略零长度字段。
- 两操作均验证真实网络 deadline、更短超时保留、最多三次固定参数请求、100ms/200ms 可取消等待；明确错误不重试，响应丢失后的拒绝不误报首次失败。
- 上报期间发生失权、已生效取消、abort 或一致性撤销：不再发起下一次请求；超时失败可在 Handler context 已取消后发起独立预算请求。
- 结果分支选定后越过执行超时时点，不切换为超时失败；Handler 已退出时槽释放而监督和续租资格仍可保留，最终清理。
- 不可恢复错误停止领取但不停止其他任务续租；网络耗尽不停止全实例；日志级别、必要字段和敏感正文排除符合第 6.2 节。
- Cancel 竞争使用受控状态迁移验证。已实现的回收链路使用真实 Redis 验证旧 token 的迟到上报会被拒绝。[Reclaim 集成测试](../reclaim_server_integration_test.go)
