# 操作契约：Reclaim

> 初版仅使用 Redis。负责租约到期恢复与重试到期调度；候选查找、当前条件检查及迁移都在同一 Lua 中完成。

## 1. 操作目的

1. 将 lease ZSET 中已到期的 running 任务，按剩余尝试额度迁移到 retry_wait 或 failed。
2. 将 retry ZSET 中已到期的 retry_wait 任务迁移到 pending。

Reclaim 不强制终止旧 handler，不增加 attempts，不通过取消标记处理取消；Cancel 直接进入 canceled 并清理索引。

## 2. 调用方与输入

- 调用方：订阅/绑定逻辑队列的 Server 定时回收模块。
- 无业务调用参数；队列、每轮处理上限及调度间隔来自平台运行配置。
- 不传入 Server 当前时间，不传入脚本外扫描后认定“已过期”的 task_id 列表。
- 至少一个负责该队列的 Server 正常运行时，周期性发起回收；无负责该队列的在线 Server 时，初版不承诺自动推进。
- 可按逻辑队列分别调用脚本；每次脚本有界处理，避免无界循环阻塞 Redis。

## 3. 执行前置条件

在 Lua 内使用 Redis TIME 得到当前 Unix 毫秒。

租约回收候选：

- 从当前 lease ZSET 内选取 score ≤ now 的成员。
- 任务详情存在、status = running，且当前 owner_id、attempt_token、active、lease 关系完整。
- 重新读取当前 score 并判断到期，不凭成员存在性判断租约失效。
- attempts、max_retry、可能使用的 retry_delay 合法。

重试调度候选：

- 从当前 retry ZSET 内选取 score ≤ now 的成员。
- 详情存在且 status = retry_wait，详情 next_retry_at 与当前 score 一致。
- 没有当前执行身份、active 或 lease 成员，且仍有尝试额度。
- 不已存在于 pending。

任务记录缺失、状态与索引不匹配、额度异常或数据类型错误，按可识别的一致性异常处理，不静默执行或声称已经修复。终态残留索引不属于正常回收路径。

## 4. 成功后的变化与返回值

### 租约到期：running → retry_wait 或 failed

对每个满足当前条件的任务原子完成：

1. 移除 active 和 lease 中的 task_id。
2. 撤销 owner_id、attempt_token。
3. 保存 last_error（租约到期原因）和 last_failed_at = now。仅凭租约到期不能断言 handler 是超时、崩溃还是失联。
4. attempts 不变，按额度更新状态及目标索引。

| 条件 | 修改 |
| --- | --- |
| attempts < max_retry + 1 | status = retry_wait；读取 Hash 中 retry_delay，计算 next_retry_at = now + retry_delay × 1000，并以相同 score 加入 retry；finish_at 为空 |
| attempts = max_retry + 1 | status = failed；设置 finish_at = now，清除 next_retry_at，不加入等待索引 |

retry_delay 为 Submit 保存的不可变配置，默认 30 秒，最小 15 秒。生成重试时间不得使用 Server 墙上时钟或临时覆盖值。

### 重试到期：retry_wait → pending

原子完成：

1. 检查当前 status、retry score 及其他前置条件。
2. 从 retry 移除 task_id。
3. 设置 status = pending，清除 next_retry_at。
4. 将 task_id 加入对应逻辑队列 pending 一次。

不增加 attempts，不创建执行身份或 lease，不清空最近失败信息。只有后续 Claim 成功授予执行权时才增加一次 attempts。

返回 error：nil 表示本次调用正常处理完成，允许没有到期任务；不表示队列没有剩余到期任务或所有任务已经恢复。

## 5. 原子性与不变量

候选查找、当前记录读取、状态和到期判断、次数判断、时间计算及完整迁移，在同一 Lua 脚本内完成；不拆成脚本外扫描再直接修改。

- Lua 使用当前数据判断，不信任旧扫描结果。
- running 正常对应 active、lease 和当前执行身份。
- retry_wait 正常对应 retry 及一致的 next_retry_at，不保留当前执行身份、active 或 lease。
- pending 正常对应 pending 索引，不同时位于 retry、active、lease。
- failed、completed、canceled 为终态，不被 Reclaim 再次调度。
- 每次迁移从源索引移除并只加入一次目标索引，回收和重试调度都不增加 attempts。
- now 等于 lease score 时可回收；now 等于 retry score 时可进入 pending。

修改前校验输入配置、数据类型、相关记录和时间计算，不能依赖 Lua 出错后自动回滚。

## 6. 失败及结果未知时的行为

- 没有到期任务：正常空操作，不报业务错误。
- 存储/网络错误或响应丢失：不立即重试，等待下一次回收定时器，重新从当前数据选候选。
- error 不等于本次所有任务都未迁移；脚本批次或前一个脚本可能已经完成部分任务，下一轮不能重复盲目入队。
- 一致性异常：记录或返回可识别错误，第一版不自动修复。
- 不通过重复执行绕过状态、score 和执行身份检查。
- Lua 部分写入破坏源、目标索引关系时，不保证下一轮普通回收能够兜底。

## 7. 并发竞争规则

- 两个回收器处理同一队列：Lua 串行生效；先完成的已移除源成员，后者不得重复迁移。
- Renew 在租约有效时先续租：本脚本看到新的未到期 score，不回收。
- 租约已经到期：Renew、complete、ReportFailure 必须拒绝；本操作按当前额度回收。
- Cancel 先成功：状态为 canceled，相关源索引已移除，不再回收或重入队。
- 本操作先进入 retry_wait 或 pending：Cancel 再按当前状态直接进入 canceled。
- complete、ReportFailure 在有效租约下先成功：源 lease 已移除，回收器不再处理该执行尝试。
- Reclaim 完成重试调度后被 Claim：新一轮产生新 token 和 lease；后续回收只依据该轮当前 score，不能撤销新一轮未过期执行权。

Redis 内一个当前有效身份，不代表物理上不存在仍运行的旧 handler。旧 token 的续租和上报必须由对应操作拒绝。

## 8. 重复请求与幂等规则

已成功迁移的任务不再满足原源状态及索引条件，因此重复调用不重复迁移，也不增加 attempts。

不能仅以“使用 Lua”宣称幂等；幂等依赖当前状态、源索引、到期时间与原子迁移共同成立。源索引类型为 List 时，也不能依靠 ZSET 成员唯一性替代 pending 去重保证。

## 9. 崩溃与恢复行为及已知限制

- 某个 Server 崩溃：只要还有负责该队列的 Server 正常运行，就继续定时回收。
- Server 在脚本完成后、收到响应前崩溃：迁移已经生效，其他 Server 按当前源索引处理，不重复重建队列。
- Server 在请求生效前退出：下一轮仍可选取原到期成员。
- Redis 自身故障后的数据保留取决于尚待明确的持久化、复制和恢复配置；重启不清空或盲目重建索引。
- Lua 错误不自动回滚此前写入。部分写入不完整状态的自动检测、对账和修复不在第一版范围，不声称重启或周期回收必然解决。
- execution_timeout 只由 Server 本地控制，本操作只按 lease score 判断租约到期，不读取 execution_deadline_at。
- 本地超时 ReportFailure 未成功时，状态可能延迟到 lease 到期才推进。
- 本地租约到期保护暂未开发，旧 handler 可能仍在执行；本操作不保证强制终止或撤销业务副作用。
- 授予执行权消耗尝试预算，未完成的尝试按预算恢复，不保证 handler 必定启动。

## 10. 验收场景

- 过期 running、有剩余额度：移除 active、lease，撤销身份，进入 retry_wait；错误时间、next_retry_at、retry score 正确，attempts 不变。
- 过期 running、额度耗尽：进入 failed，finish_at 正确，没有等待、执行索引或当前身份。
- 未到期租约：不修改；边界 now = score 时允许回收。
- retry 到期：移除 retry、改 pending、清除 next_retry_at、入 pending 一次；未到期不迁移。
- 两 Server 同时回收同一批任务：不重复入 retry 或 pending，不重复消耗尝试次数。
- Renew 先延长、Cancel 先取消、上报先成功：依据当前状态和 score 处理。
- 回收后重新领取：旧 token 续租和上报失败，新一轮未到期租约不受影响。
- 一台 Server 崩溃：另一台负责同队列的 Server 可以继续回收。
- 丢失回收响应：下一轮按当前数据处理，无重复入队。
- 注入不一致索引：返回或记录异常，不把自动修复列为初版能力。
