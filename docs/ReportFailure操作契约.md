# 操作契约：ReportFailure

> 初版仅使用 Redis。一次调用处理单个任务的一轮失败；取消直接由 Cancel 进入 canceled，失败上报不补写非 running 任务。

## 1. 操作目的

Server 在 handler 返回错误，或者本地 execution_timeout_ms 到期时，报告本轮失败并释放 Redis 执行权，根据已消耗尝试次数进入 retry_wait 或 failed。

超时是触发失败上报的事件，不是新的任务状态。超时上报可以在 handler 尚未退出时发起，但不允许提前归还本地执行槽。

## 2. 调用方与输入

- 调用方：Server 的失败/超时上报流程。
- 输入：单个 task_id、该轮 attempt_token、error_message。
- task_id、attempt_token、error_message 必填；标识非空，错误内容遵守平台大小限制。
- 超时由 Server 生成明确的超时错误信息，业务调用方不指定当前 attempts 或目标状态。
- retry_delay_ms 不随本次上报传入，读取 Submit 保存的不可变配置。
- 网络重试复用相同 task_id、attempt_token 和错误内容。

## 3. 执行前置条件

在 Lua 内检查：

1. 任务详情存在，status = running。
2. attempt_token 与当前执行身份一致。
3. active 中存在 task_id，lease 中存在该 task_id。
4. Redis 当前时间严格小于 lease score；相等即到期。
5. attempts、max_retry 及本次可能使用的 retry_delay_ms 等记录合法，异常数据按一致性错误处理。

不设置或读取取消标记。只要任务不是 running，包括已经 canceled，就拒绝本次上报，不补写错误和错误时间。

## 4. 成功后的变化与返回值

在同一 Lua 中：

1. 保存 last_error = error_message、last_failed_at = Redis 当前 Unix 毫秒。
2. 移除 active、lease，撤销 owner_id、attempt_token。
3. attempts 保持不变，按已授予尝试的数量决定目标。

| 当前额度 | 目标状态及数据 |
| --- | --- |
| attempts < max_retry + 1 | status = retry_wait；读取任务 Hash 中 retry_delay_ms，计算 next_retry_at 并仅作为 score 加入 retry ZSET |
| attempts = max_retry + 1 | status = failed；设置 finished_at，不加入等待索引 |

正常数据应满足 attempts ≤ max_retry + 1；大于上限属于一致性异常，不作为正常路径吞掉。

重试时间统一为：

    next_retry_at = Redis 当前 Unix 毫秒 + retry_delay_ms
    retry ZSET 的 member = task_id
    retry ZSET 的 score = next_retry_at

retry_delay_ms 省略时由 Submit 规范化为 30 秒，最小 15 秒；不得在上报时覆盖。retry_wait 的 finished_at 应为空；next_retry_at 只是重试到期时间的名称，仅作为 retry ZSET 的 score 保存，不单独写入任务详情 Hash，也不存在详情时间字段的同步写入或清理。

返回 applied、error：

- true、nil：本次迁移成功。
- false、nil：任务不存在、状态不符、token 不符、lease 缺失或已到期，没有修改。
- 可识别错误：参数或一致性异常。
- 存储/网络错误或响应丢失：结果可能未知，不能把 applied 当作确定未执行。

## 5. 原子性与不变量

前置条件、数据类型、额度及时间计算校验，与错误写入、索引移除、身份撤销、目标状态、重试索引或终态信息写入，在同一 Lua 中完成。

- ReportFailure 不增加 attempts，也不退回已消耗次数。
- retry_wait 有且只有对应的 retry 成员，不保留 active、lease 或当前执行身份。
- failed 为终态，不保留等待、执行索引及当前执行身份。
- 旧 token 不覆盖新一轮错误、状态或租约。
- canceled 等非 running 任务不会因为补报错误再次变化。

## 6. 失败及结果未知时的行为

- 参数错误或已识别的一致性错误：记录并返回，不盲目原样重试，不声称已自动修复。
- applied = false、error = nil：不重试本次上报，停止续租并结束本地上报管理。
- 网络/存储异常或结果未知：最多额外重试两次，含首次最多三次；收到确定成功或拒绝即停止。
- 重试耗尽后，从续租集合移除该任务并结束本地上报流程；若 Redis 仍保有完整 running、active、lease，则由 Reclaim 到期处理。
- handler 正常返回错误后的有界上报期间，可继续对该轮续租；上报最终结束后必须停止。
- 本地超时已裁定时，应立即停止续租并取消 context，再调用本操作，不为等待上报而延长 execution_timeout_ms。
- 结束上报管理后，不再接受该轮迟到 handler 返回作为新的上报结果。
- handler 尚未退出时，仍保留退出等待和槽归还责任。执行槽只在 handler 实际退出后归还一次，不能靠删除续租记录提前归还。

如果超时上报失败，Redis 可能暂时仍是 running；只能等待租约回收，不能宣称本地超时已经同步终结存储状态。

## 7. 并发竞争规则

- Cancel 先成功：ReportFailure 因非 running 拒绝，不补写 last_error。
- ReportFailure 先进入 retry_wait：Cancel 随后移除 retry 并直接进入 canceled。
- ReportFailure 先进入 failed：Cancel 看到终态，不再修改。
- complete 先成功：失败上报拒绝；失败上报先迁移，后续 complete 拒绝。
- Renew 先成功：本操作按更新后的 score 判断，仍可报告失败。
- 本操作先成功：Renew 因非 running 或 lease 缺失拒绝。
- Reclaim 先回收，或者任务已重新领取：旧上报因状态、token 不符拒绝。
- 租约已到期但尚未回收：仍然拒绝上报，不能只看任务是否还在 lease 中。

## 8. 重复请求与幂等规则

首次上报成功后已离开 running，重复上报不再次写错误、不重复加入 retry、不增加 attempts。

即使任务已回到 pending 并重新领取，旧 token 仍不能影响新一轮。保证的是重复请求不重复修改，不保证首次和重试返回相同结果。

首次成功但响应丢失，再次返回前置条件不满足是合法结果；不能因此再次安排业务重试或主动重复入队。

## 9. 崩溃与恢复行为及限制

- Server 在上报生效前崩溃：停止续租，完整执行记录到期回收。
- 上报已生效但响应丢失：retry_wait 由 Reclaim 到期转 pending，failed 保持终态。
- 授予执行权消耗尝试预算，未完成的尝试按预算恢复，不保证 handler 必定启动。
- Redis 故障后的持久性依赖部署配置，不引入 MySQL 补写或对账。
- Lua 错误不自动回滚此前写入。部分写入后的自动检测、对账和修复不属于第一版；移除了 lease 却未完成迁移等异常不能承诺由普通回收器发现。
- execution_timeout_ms 仅在 Server 本地控制，不保存 execution_deadline_at。初版也不在本地维护租约有效期，相关保护明确留待后续。

## 10. 验收场景

- 有剩余额度：进入 retry_wait，错误字段正确，score = Redis now + 保存的 retry_delay_ms，任务详情不含 next_retry_at 字段。
- 额度耗尽：进入 failed 并写 finished_at，无 retry 成员，attempts 不变。
- canceled、completed 或其他非 running：拒绝且不补写错误。
- 旧 token、已过期或缺失租约：拒绝，不影响新一轮。
- 首次成功但响应丢失：重复上报不重复迁移、入队或更新时间。
- 网络错误重试耗尽：停止续租，到期回收完整执行记录。
- 超时且 handler 尚未退出：允许上报失败，但实际退出前不归还执行槽。
- 并发成功、失败、取消、续租、回收：符合第 7 节。
