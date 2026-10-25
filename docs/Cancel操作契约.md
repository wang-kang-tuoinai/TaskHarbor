# 操作契约：Cancel

> 初版仅使用 Redis。取消是直接进入 canceled 终态，不设置取消标记。

## 1. 操作目的

取消 pending、retry_wait 或 running 任务，立即撤销平台执行权，并尽力通知执行实例停止 handler。

canceled 表示平台不再接受该任务的执行和上报，不表示 handler 已实际退出，也不撤销已经发生的业务副作用。

## 2. 调用方与输入

- 调用方：客户端。
- 输入：单个 task_id，必填且非空。
- 客户端不指定 owner_id、attempt_token 或目标状态。
- 客户端是否重试由业务调用方决定，初版平台不实现客户端自动重试。

## 3. 执行前置条件

在 Lua 内先检查任务详情存在，再检查当前状态：

- 不存在：返回 not_found，不创建、不入队、不推送通知。
- pending、running、retry_wait：允许取消。
- completed、failed、canceled：返回 already_terminal 及当前状态，说明已是终态，无法再次取消，不修改记录。
- 未知状态、记录或索引类型异常：返回可识别的一致性错误，不将其当作可正常取消任务。

取消不要求租约有效，也不要求调用方具有本轮执行身份；running 任务即使租约已过期但尚未回收，也可取消。

## 4. 成功后的变化与返回值

在同一个 Lua 原子边界内，根据当前状态完成：

| 原状态 | 索引处理 |
| --- | --- |
| pending | 从当前逻辑队列的 pending 移除 task_id |
| retry_wait | 从 retry ZSET 移除 task_id，清除详情中的 next_retry_at |
| running | 从 active、lease 移除 task_id，撤销 owner_id、attempt_token |

随后设置 status = canceled、finish_at = Redis 当前 Unix 毫秒；清除残留的 next_retry_at，不增加 attempts，不修改原始提交配置。正常终态不保留 pending、retry、active、lease 成员或当前执行身份。

仅对原状态为 running 的任务：先在脚本内保存原 owner_id 和 attempt_token，再撤销身份，并向 cancel:{owner_id} 推送包含 task_id 和原 attempt_token 的取消通知。通知不在脚本内等待消费确认或重试。

返回结果：

| 情况 | 结果 | error |
| --- | --- | --- |
| 本次取消生效 | canceled，当前状态 canceled | nil |
| 任务不存在 | not_found | nil |
| 已处于终态 | already_terminal，携带当前状态 | nil |
| 参数或一致性错误 | 无成功结果 | 可识别错误 |
| 存储或网络错误 | 无确定结果，可能已经取消 | 对应错误 |

Redis 取消生效，不以 Server 是否收到通知或 handler 是否退出为成功前提。

## 5. 原子性与不变量

任务存在性、状态及数据类型检查，按状态移除索引，撤销身份，设置 canceled、finish_at，以及发出运行中任务的通知，在同一 Lua 中执行。写入前完成可预见的校验。

- 终态不得被其他操作改回 running、retry_wait 或 pending。
- 取消不消耗新的尝试次数。
- 不设置取消标记；其他操作以 status 是否为 running 等前置条件判断能否继续。
- 通知只是尽力加速本地停止，Redis 状态才是权威结果。
- Server 处理匹配本地 task_id、attempt_token 的通知后，停止续租并发出 context 取消。执行槽必须等 handler 真正退出才归还，且只归还一次。

## 6. 失败及结果未知时的行为

- 参数非法：返回参数错误，不修改。
- not_found、already_terminal：不需要为本次取消目的继续重试；若返回 canceled，任务已经处于取消终态。
- 网络超时或响应丢失：不能推断未取消；调用方可用同一 task_id 重试或查询。
- 不因通知丢失、Server 宕机或 handler 未退出而撤销 canceled，也不在 Lua 中重试通知。
- 后续 Renew、complete、ReportFailure 读取到非 running 后应拒绝，不再补报错误或覆盖终态。

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

首次取消只生效一次；重复请求看到 canceled 后返回 already_terminal，不重复修改 finish_at 或推送通知。

首次取消成功但响应丢失时，重复结果只能说明当前任务已处于终态，不承诺复现第一次请求的返回值。不新增取消请求去重记录。

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

## 10. 验收场景

- 分别取消 pending、retry_wait、running：检查 canceled、finish_at、对应索引及当前身份清理，attempts 不变。
- 任务不存在：返回 not_found，不创建任务。
- 对三个终态取消：返回 already_terminal 和实际状态，不修改结果、错误或 finish_at。
- 响应丢失后重试：无重复迁移、通知和 finish_at 更新。
- 分别安排取消与领取、成功上报、失败上报、回收的先后顺序：符合第 7 节。
- handler 延迟退出或忽略 context：取消后不提前归还执行槽。
- 制造通知先于本地登记的窗口：记录 handler 可能启动的已知缺陷，验证 Redis 仍拒绝后续操作，不把不启动列为初版保证。
