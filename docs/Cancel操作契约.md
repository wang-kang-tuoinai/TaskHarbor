# 操作契约：Cancel

> 初版仅使用 Redis。取消是直接进入 canceled 终态，不设置取消标记。

2026-11-17 已确认的实施依据。订阅与故障取舍见 [ADR-0009](adr/0009-cancel-notification-and-subscription.md)，2026-11-19 确认的通知 Unicode 边界见 [ADR-0010](adr/0010-cancel-notification-unicode.md)。设计确认不表示功能已经实现。

## 1. 操作目的

取消 pending、retry_wait 或 running 任务，立即撤销平台执行权，并尽力通知执行实例停止 handler。

canceled 表示平台不再接受该任务的执行和上报，不表示 handler 已实际退出，也不撤销已经发生的业务副作用。

## 2. 调用方与输入

- 调用方：客户端。
- 公开入口为 Client.Cancel(ctx, taskID)，返回结构化 CancelResult 与 error；输入 task_id 必填且非空，不自动裁剪或改写。内部 redisstore 包负责 Lua 调用和响应解析。
- 客户端不指定 owner_id、attempt_token 或目标状态。
- 透传调用方 context，不新增单独 timeout 参数、Cancel 超时配置或平台重试循环，不修改共享客户端配置。调用方配置的 go-redis 底层重试可能发生，结果描述最终确认的观察，不承诺复现第一次请求的分类。
- 若需要 context 截止时间约束实际网络读写，调用方须启用 ContextTimeoutEnabled；本轮不新增 NewClient 构造限制。超时不撤回 Redis 已生效的取消，返回错误仍可能表示结果未知。

## 3. 执行前置条件

在 Lua 内先检查任务详情存在，再检查当前状态：

- 不存在：返回 not_found，不创建、不入队、不推送通知。
- pending、running、retry_wait：允许取消。
- completed、failed、canceled：返回 already_terminal 及当前状态，说明已是终态，无法再次取消，不修改记录。
- 未知状态、记录或索引类型异常：返回可识别的一致性错误，不将其当作可正常取消任务。

取消不要求租约有效，也不要求调用方具有本轮执行身份；running 任务即使租约已过期但尚未回收，也可取消。

写前按当前状态检查必要关系，不扩展为全量对账：

- pending：pending 必须为 List 且存在目标成员；执行身份不得残留，可直接查询的 active、lease、retry 成员冲突按一致性错误处理。无需统计目标在 List 中出现次数。
- running：active、lease 的类型及成员关系完整，owner_id、attempt_token 非空；retry 成员冲突按异常处理，不额外扫描 pending 查残留。
- retry_wait：retry 的类型及目标成员完整，执行身份字段不存在（空值也属残留），active、lease 不含目标；不额外扫描 pending。
- 所访问 key 类型、所需记录及通知编码在首次写入前检查或准备。类型正确不等于目标成员存在；必要成员缺失同样报错。
- 不因 payload、执行时长或重试额度等与取消无关的配置阻止取消。终态直接返回，不顺手检查或修复全部残留索引。

pending 成员查找与删除的成本随 List 长度增长，不保证固定耗时；不新增辅助成员 Set，具体查找方式需兼容实际支持的 Redis 版本。

## 4. 成功后的变化与返回值

在同一个 Lua 原子边界内，根据当前状态完成：

| 原状态 | 索引处理 |
| --- | --- |
| pending | 从当前逻辑队列的 pending 移除所有等于 task_id 的列表项 |
| retry_wait | 从 retry ZSET 移除 task_id |
| running | 从 active、lease 移除 task_id，撤销 owner_id、attempt_token |

随后设置 status = canceled、finished_at = Redis 当前 Unix 毫秒；不增加 attempts，不修改原始提交配置。正常终态不保留 pending、retry、active、lease 成员或当前执行身份。

仅对原状态为 running 的任务：先在脚本内保存原 owner_id 和 attempt_token，再撤销身份，并向 cancel:{owner_id} 推送包含 task_id 和原 attempt_token 的取消通知。通知不在脚本内等待消费确认或重试。

通知使用 JSON，字段为 task_id、attempt_token，不依赖分隔符拼接。编码在写前完成；状态迁移后使用受保护的发布调用（如 redis.pcall）捕获发布错误。发布失败时保留取消成功，内部响应携带通知失败诊断，由 Client 使用现有 slog 记录，不新增公开通知成功字段，不回滚、不重发。其他脚本错误与网络结果未知不能因此泛化为成功。

Submit 接受任意非空任务 ID 字节。原 running 任务的 ID 若含非法 UTF-8，仍按原始字节取消任务，但跳过 JSON 发布并记录诊断，避免替换字符导致通知命中另一个任务。

正常 pending 只含目标一次；即使损坏数据中重复出现，删除多个也不报一致性错误。若成员缺失，应在写前拒绝，不能先删除再以删除数量报“零写入校验异常”。此清理仅针对取消目标，不代表通用修复能力。

返回结果：

| 情况 | 结果 | error |
| --- | --- | --- |
| 本次取消生效 | canceled，当前状态 canceled | nil |
| 任务不存在 | not_found | nil |
| 已处于终态 | already_terminal，携带当前状态 | nil |
| 参数或一致性错误 | 无成功结果 | 可识别错误 |
| 存储或网络错误 | 无确定结果，可能已经取消 | 对应错误 |

Redis 取消生效，不以 Server 是否收到通知或 handler 是否退出为成功前提。

CancelResult 区分 canceled、not_found、already_terminal；already_terminal 携带实际终态，not_found 与 already_terminal 两种正常未迁移情况不作为异常。发布时没有订阅者也不改变取消成功语义。

## 5. 原子性与不变量

任务存在性、状态及数据类型检查，按状态移除索引，撤销身份，设置 canceled、finished_at，以及发出运行中任务的通知，在同一 Lua 中执行。写入前完成可预见的校验。

- 终态不得被其他操作改回 running、retry_wait 或 pending。
- 取消不消耗新的尝试次数。
- 不设置取消标记；其他操作以 status 是否为 running 等前置条件判断能否继续。
- 通知只是尽力加速本地停止，Redis 状态才是权威结果。
- Server 处理匹配本地 task_id、attempt_token 的通知后，停止续租并发出 context 取消。执行槽必须等 handler 真正退出才归还，且只归还一次。

本地取消原因应表达已生效的主动取消，监督结束且不调用 complete 或 ReportFailure。Handler 因取消返回错误不构成新的业务失败；结果与取消同时就绪时也须检查已知取消原因。在途上报由 Redis 拒绝；仅发起 Cancel 请求或收到网络错误不证明取消生效。具体监督竞争边界见 [Renew 契约](Renew操作契约.md)第 6.2 节，本轮 Renew 开发不包含 Cancel 实现。

### Server 通知处理与订阅生命周期

1. Start 持锁检查条件、封存注册、生成 ownerID 并进入 starting，然后释放锁。锁外创建订阅并等待 Redis 明确确认，使用 3 秒预算；仅取得订阅对象不等于确认成功。
2. 确认成功后重新持锁，检查启动期间是否发生 Shutdown；仍允许启动时保存订阅资源，并统一启动 Claim、Renew、Reclaim 和通知消费循环。不得持锁等待订阅网络操作，不能依赖 goroutine 启动顺序代替订阅确认。
3. 确认失败清理订阅资源，Start 返回错误，不启动上述循环；实例不再次启动，调用方创建新实例。Start 因此增加网络等待。实际网络等待需验证，不能仅凭 context 到期声称底层调用已返回。
4. Shutdown 覆盖 starting：请求取消启动等待，协调启动收尾；迟到订阅成功不得再启动循环。区分启动流程结束与已启动循环结束，不能无条件等待从未启动的 Claim 的 done。底层操作未及时返回时，Shutdown 仍按自身期限返回并保持收尾，不提前标记 stopped。
5. 使用 PubSub.Channel() 接收，不与 Receive* 混用（首次订阅确认在创建 Channel 前完成）。连接恢复及接收错误主要交由当前 go-redis 实现处理，不自建接收、心跳或重连框架。
6. 消费协程解析 JSON、校验字段，按 task_id 与 attempt_token 精确查找本地执行尝试。找到时复用撤销路径，在表锁外取消对应 Handler 和上报、移除续租资格，原因使用 errExecutionCanceled。旧 token 不影响新执行尝试。
7. 无匹配记录时忽略该消息并继续消费，不缓存早到通知。格式错误、非法 UTF-8、未配对的 UTF-16 代理项转义或缺字段的消息，记录诊断后丢弃并继续消费。有效代理项对和合法的 U+FFFD 按解码后的字符精确匹配。不等待 Handler 退出、不在消费路径同步做网络上报。
8. 通知渠道仅供可信平台组件发布，正常消息不额外查询 Redis 重复确认。消息不可信的访问控制设计不由此解析逻辑替代。
9. 正常排空期间继续订阅；强制阶段或正常排空后的清理请求关闭 Server 自己持有的 PubSub。实例完全停止前确认消费协程结束，不关闭调用方 Redis 客户端或共享池。
10. 消息 channel 关闭时区分是否已由生命周期管理明确请求关闭订阅，不能仅凭 stopping 判断正常，因为排空时仍需通知。主动关闭属正常退出；非预期关闭停止通知消费与 Claim，通过现有 Errors 报告，保留健康 Renew、Reclaim、Handler 和上报，不自动另建订阅流程。

## 6. 失败及结果未知时的行为

- 参数非法：返回参数错误，不修改。
- not_found、already_terminal：不需要为本次取消目的继续重试；若返回 canceled，任务已经处于取消终态。
- 网络超时或响应丢失：不能推断未取消；调用方可用同一 task_id 重试或查询。
- 不因通知丢失、Server 宕机或 handler 未退出而撤销 canceled，也不在 Lua 中重试通知。
- 后续 Renew、complete、ReportFailure 读取到非 running 后应拒绝，不再补报错误或覆盖终态。
- Pub/Sub 不保证送达，断线期间遗漏消息不因重连补回。Channel 不直接提供每次接收错误，不承诺每次断线都经 Errors 通知；持续重连错误也不能依靠消息 channel 关闭来识别。消费积压时库可能丢弃消息。
- 消息 channel 通常在 PubSub 被关闭后结束；普通网络抖动、暂时不可达或消费缓慢不等同于 channel 关闭。Errors 仍只通知首个致命错误，通知被抑制不能跳过必要故障处理。

## 7. 并发竞争规则

以 Redis 中原子生效顺序决定结果，不声称不存在竞争：

- Cancel 先于 Claim：任务变为 canceled 并移除 pending，Claim 不得领取。
- Claim 先于 Cancel：任务先变为 running，Cancel 再直接进入 canceled 并移除执行索引。
- complete 先于 Cancel：已为 completed，Cancel 返回 already_terminal。
- Cancel 先于 complete 或 ReportFailure：上报因非 running 被拒绝，不修改结果或错误。
- ReportFailure 或 Reclaim 先迁移到 retry_wait：Cancel 按 retry_wait 取消。
- Reclaim 已迁移到 failed：Cancel 返回 already_terminal。
- Renew 先续租不阻止之后取消；Cancel 先执行则后续 Renew 被拒绝。

## 8. 重复请求与幂等规则

首次取消只生效一次；重复请求看到 canceled 后返回 already_terminal，不重复修改 finished_at 或推送通知。

首次取消成功但响应丢失时，重复结果只能说明当前任务已处于终态，不承诺复现第一次请求的返回值。不新增取消请求去重记录。

底层透明重试也可能使调用方最终得到 already_terminal/canceled：它表示当前已取消，不是取消失败，也不能确认哪次请求完成取消。不会为补发通知刷新 finished_at 或再次迁移。

## 9. 崩溃与恢复行为及已知缺陷

- Server 崩溃或未收到通知：Redis 的 canceled 仍有效，后续上报被拒绝；不保证 handler 被强制终止。
- Redis 自身重启是否保留近期变化取决于待明确的持久化和复制配置，Lua 不能推导无条件不丢数据。
- Lua 运行时错误不自动回滚此前写入。部分写入留下的不完整状态是已知限制；初版不实现自动检测、对账和修复，也不声称普通回收器可以兜底。

已知取消与本地登记、启动的衔接缺陷，第一版暂不解决：

1. Claim 在 Redis 成功，Server 尚未收到响应或完成本地登记。
2. Cancel 进入 canceled 并推送通知。
3. Server 因本地尚无记录而忽略通知。
4. Claim 后续流程仍可能启动 handler。

此时平台状态仍是 canceled，后续续租和上报会被拒绝，但 handler 可能占用执行槽并产生业务副作用。该缺陷必须保留在后续开发清单，不能描述为已被状态检查完全解决。

先订阅后领取只消除主动留下的未订阅启动窗口，不消除上述本地登记窗口。表中缺记录也可能是超时、失权或监督清理的正常结果，不应当成实例故障。running、retry_wait 分支未扫描 pending 的异常残留不在检测保证内；正常终态不变量不等于 Cancel 会修复所有历史损坏。

## 10. 验收场景

- 分别取消 pending、retry_wait、running：检查 canceled、finished_at、对应索引及当前身份清理，attempts 不变。
- 任务不存在：返回 not_found，不创建任务。
- 对三个终态取消：返回 already_terminal 和实际状态，不修改结果、错误或 finished_at。
- 响应丢失后重试：无重复迁移、通知和 finished_at 更新。
- 分别安排取消与领取、成功上报、失败上报、回收的先后顺序：符合第 7 节。
- handler 延迟退出或忽略 context：取消后不提前归还执行槽。
- 制造通知先于本地登记的窗口：记录 handler 可能启动的已知缺陷，验证 Redis 仍拒绝后续操作，不把不启动列为初版保证。
- pending 成员缺失、错误类型、执行身份或可直接查询的索引冲突：写前拒绝且零写入；重复 pending 项取消后全部移除且返回成功。
- running 租约已过期但关系完整时仍可取消；取消不受重试额度耗尽影响。
- 通知发布失败：状态和索引迁移保留、公开结果仍成功、诊断可见；无订阅者不报取消失败。验证响应丢失与底层重试可能改变结果分类但不重复通知。
- JSON 格式错误、非法 UTF-8、未配对的 UTF-16 代理项转义、字段缺失、未知执行和旧 token：不取消其他任务，消费继续处理后续合法消息。
- 订阅确认前不领取；确认失败/超时清理且对象不可重启；启动期间 Shutdown、迟到确认和从未启动的 done 不造成死锁或退出后启动。
- 正常排空仍可收到取消，强制阶段关闭订阅；意外 channel 关闭停止 Claim/消费但不停止健康续租及上报；主动关闭不报致命错误，Redis 客户端仍可使用。
- 网络中断与恢复验证消息可能丢失、Redis 取消仍有效；不得以 channel 未关闭声称连接健康。真实网络验证启动预算和 Cancel context 边界，区分等待超时与操作未生效。
