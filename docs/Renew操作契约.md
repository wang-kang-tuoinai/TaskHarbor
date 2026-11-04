# 操作契约：Renew

> 初版仅使用 Redis。每次只续租一个任务；租约仅以 lease ZSET 的 score 为准。

> 2026-11-04 确认的设计，依据 [ADR-0004](adr/0004-renew-failure-and-eligibility.md)。本轮只实现 Renew 及必要的启动、监督集成；complete、ReportFailure 继续占位，Reclaim、客户端 Cancel、公开 Shutdown 和本地租约截止保护延期。本文是实施与验收依据，不表示已实现。

## 1. 操作目的

Server 定期对本地明确接收并管理的执行尝试续租。续租表示该执行尝试仍由本地管理，不证明 handler 一定在推进。

Server 遍历本地续租集合，对每个任务分别调用 Renew。不能仅因进程在线就为该 owner 名下所有任务续租。

executions 表保存本地具备续租资格的执行尝试，不证明当前租约有效，也不保证正在续租；实际请求由 Renew 模块的运行状态决定。全局续租故障停用循环时保留记录，由原监督路径清理。术语见 [GLOSSARY](../GLOSSARY.md)。

## 2. 调用方与输入

- 调用方：Server 的续租模块。
- 单任务输入：task_id、对应的 attempt_token、lease_duration（本次租约时长）。
- task_id、attempt_token 必填且非空；lease_duration 为正值，来自平台运行配置，不由业务提交方随意指定。
- attempt_token 属于每次任务执行尝试，不是每个 Server 共用一个。
- 不传入 Server 的绝对到期时间戳；Lua 使用 Redis TIME 计算。
- 时长换算和时间相加应检查溢出。

## 3. 执行前置条件

在同一 Lua 内按当前状态和身份判断是否需要检查执行索引；写入前完成必要的参数、字段、类型与数值校验：

1. 任务详情存在且 status = running。
2. 输入 attempt_token 与当前记录一致。
3. active 中存在该任务；异常索引按一致性错误处理。
4. lease ZSET 存在该 task_id，且当前 Redis 时间严格小于其 score。

任务不存在、合法状态已非 running 或 token 不匹配时正常拒绝，不继续把已完成任务清理后的索引缺失当成异常。必要字段缺失、非法状态或相关 key 类型错误属于一致性错误；仍为 running 且 token 匹配时，active 或 lease 成员缺失、lease score 非法也属于一致性错误。只检查本操作所需数据，不进行全量对账。索引完整但租约到期属于正常拒绝。

仅存在于 lease ZSET 不代表租约有效。now 等于 score 时已到期，不能复活。

本地只为已接收并管理、尚未停止续租的任务发起请求；本地超时、已知取消、明确失去执行权或上报流程最终结束后，应从续租集合移除。

## 4. 成功后的变化与返回值

status 保持 running，Lua 设置：

    lease ZSET 的 member = task_id
    新 score = Redis 当前 Unix 毫秒 + lease_duration（毫秒）

不改变 owner_id、attempt_token、attempts、原始 execution_timeout_ms、retry_delay_ms 或本地超时定时器。不在任务详情写入 lease_expires_at，不设置 execution_deadline_at。

返回 applied、error，针对本次单任务操作：

| 情况 | applied | error |
| --- | --- | --- |
| 续租生效且收到确认 | true | nil |
| 任务不存在、非 running、token 不匹配，或索引完整但租约已到期 | false | nil |
| 参数或一致性错误 | false | 可识别错误 |
| 存储、网络错误或响应丢失 | 无确定生效结论 | 对应错误 |

只有 error 为 nil 时，才能用 applied 判定本次结果；error 不为 nil 不能解释为确定未续租。

## 5. 原子性与不变量

输入、数据类型、当前状态、token、active、当前 lease score 的检查与新 score 写入，在同一 Lua 中完成，使用同一轮 Redis 时间判断。

- 仅当前有效执行身份可以续租。
- 已到期租约不能复活；非 running 任务不能续租。
- 续租不增加 attempts，不切换任务状态。
- 续租不重置 Server 本地 execution_timeout_ms。
- 初版租约不按 execution_timeout_ms 截断，Redis 不执行存储端硬截止时间检查。

## 6. 失败及结果未知时的行为

- 参数错误：不原样重试，应记录并修正调用。
- 明确返回 applied = false 且 error = nil：停止该任务续租，向本地 handler 发出 context 取消；不再以该身份发起结果上报。
- 执行槽只能在 handler 真正退出后归还一次，不能在发出取消信号或收到续租拒绝时提前归还。
- 暂时性网络错误或响应丢失：本次不立即重试，也不单凭该错误取消 handler 或归还槽；继续扫描其他任务，该任务等待下一次续租定时器。不能把明确权限或脚本错误当成网络波动。
- 下一次请求仍须重新检查当前状态、token、score，不承接上次可能存在的权限。
- 本地 execution_timeout_ms 到期时，仍按 Claim、ReportFailure 的规则停止续租并上报超时，不因等待下一次 Renew 而延长本地定时器。

### 6.1 故障影响范围

- 正常拒绝仅影响对应 taskID、attemptToken：移除续租资格、取消 Handler、结束监督，不上报结果，不进入 Errors，不停止 Claim 或其他任务续租。
- 单任务一致性异常：取消并移除该轮次，不调用 ReportFailure；停止新 Claim，通过 Errors 报告包含任务身份的错误。其他健康任务继续执行、续租和处理结果。不自动修复 Redis 数据，也不将异常取消描述为业务失败。
- 全局致命续租故障（如权限、脚本错误）及非法响应：停止新 Claim 和 Renew，通过 Errors 报告。不清空 executions，不因此取消 Handler；监督继续处理结果、超时及其他取消并正常清理。非法响应可能对应已生效请求，不回滚租约，不声称未续租。
- 全局故障后在途 Claim 明确成功，仍进入正常 acceptExecution 流程；登记不意味着续租恢复，不增加专用接管分支。实例不恢复已停用循环，修复后创建新实例。
- Errors 容量为 1、单消费者，只通知实例首个导致停止领取的后台致命错误，Claim 与 Renew 共用。后续错误不再发送，但仍执行对应故障处理；无人接收不得阻塞。通知不代表所有 Handler 或后台协程结束，不关闭 channel 来表达服务结束。

### 6.2 取消原因与结果竞争

使用 context.WithCancelCause 表达明确失权、已生效的主动取消及一致性异常等取消原因，与本地超时和 abort 区分。不能用 executions 中有无记录推断执行权：超时会先移除再尝试失败上报，全局续租停止则可以保留记录但不再续租。

仅增加 ctx.Done 分支不够：结果和取消同时就绪时 select 可能选中结果。结果分支在开始上报前也应检查已知的禁止上报原因；超时或未知 Handler 等失败路径同样不能绕过已知失权。明确失权、已生效主动取消或单任务一致性异常后不再发起 complete/ReportFailure；abort 只取消清理，不伪造失败。单纯全局续租故障不禁止结果上报。

检查之后才到达的取消、已经发出的上报无法由本地检查撤回，最终由 Redis 校验状态、token 和租约；不承诺消除此窗口。只选择一个监督分支，迟到结果不触发第二次上报，槽等 Handler 实际退出才归还。主动取消的请求发出或网络错误不等于取消已生效；Cancel 功能本轮不实现。

### 6.3 调度与请求时间边界

- Start 启动独立 Renew 循环，不绑定 Claim 的取消 context；停止领取不能连带停止健康任务续租。
- 按 renewInterval 固定周期取快照，锁外串行请求；空集合不请求 Redis，不持有执行表锁等待网络或调用取消函数。
- 每个请求具有独立超时，上限取 renewInterval；底层客户端更短的超时可提前生效，不修改共享客户端配置。请求等待必须受该边界约束，不能只设置一个底层不遵守的 context 截止时间。
- 一轮扫描未结束时不启动下一轮，错过的周期不逐个补跑。网络错误继续扫描其他任务，不在本轮立即重试该任务；保留禁用透明重试的客户端前置条件，明确 NOSCRIPT 才可缓存回退。
- 请求前确认对应执行尝试仍在集合内，跳过已移除条目。检查后移除与在途请求仍有竞争，接受 Renew 成功延后回收的窗口，不做复杂串行化。
- 串行扫描有容量限制：renewInterval 是触发周期，不保证每个任务在该时长内完成续租；即使 renewInterval <= leaseDuration / 3，请求数量和延迟仍可能使后续任务过期。有界并发留待需要时另行设计。

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

全局续租循环停止后同样存在上述窗口，且不再有下一次 Renew 拒绝来通知失权。已有及迟到 Claim 的任务仍可执行并尝试上报，是否被接受取决于上报时的 Redis 执行权。当前上报为空方法，不能宣称已有真实成功机会；占位返回后记录清理、停止续租，但 Redis 仍可能为 running。没有 Reclaim 时租约到期不会自动迁移任务。

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

- 同 token 的 running 任务缺 active/lease、非法 score、必要字段缺失及错误 key 类型：可识别异常且不写入；已非 running 或旧 token 的请求不因正常索引清理误报。
- Renew 与状态迁移竞争：原子决定是否更新，不复活到期租约，不修改新 token 的 score。未实现的 Cancel/Reclaim/上报用构造状态验证前置条件，不宣称端到端功能完成。
- 真实丢弃已成功 Renew 的响应：结果未知，不立即重试、不取消、不释放槽；下一轮重新鉴权。
- 失权与 Handler 结果、超时同时就绪，以及未知 Handler 路径：已知禁止上报原因不被绕过；迟到结果不上报；不响应取消的 Handler 保持占槽。
- 单任务一致性异常：停止领取并报告，异常任务取消，其他任务继续续租；普通拒绝不停止领取、不发送致命错误。
- 全局脚本/权限错误与非法响应：停止领取和续租、保留执行记录及已有 Handler；迟到成功 Claim 正常登记执行但不恢复续租，结果和超时仍走正常监督。
- Claim 与 Renew 同时致命失败且无人读取 Errors：只通知首个错误，两个故障处理均不阻塞；Claim 独立停止不影响健康续租。
- 空集合、慢请求、请求超时和长扫描：锁外请求、轮次不重叠、不逐个补跑、单次等待受限；网络错误不阻止后续条目扫描。验证请求前删除跳过，以及旧 token 清理不影响新轮次。
- 保留阶段限制验证：上报占位不改变 Redis 状态；不把本地租约截止、真实回收或公开退出当成已实现能力。
