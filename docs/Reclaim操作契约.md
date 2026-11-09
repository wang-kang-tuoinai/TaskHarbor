# 操作契约：Reclaim

> 初版仅使用 Redis 和 default 队列。租约回收与重试到期调度使用两个 Lua，各自完成候选查找、整批检查及迁移。本契约为已确认的实施依据，不表示功能已经实现；关键取舍见 [ADR-0007](adr/0007-reclaim-batches-and-failure-scope.md)。

## 1. 操作目的

1. 将 lease ZSET 中已到期的 running 任务，按剩余尝试额度迁移到 retry_wait 或 failed。
2. 将 retry ZSET 中已到期的 retry_wait 任务迁移到 pending。

Reclaim 不强制终止旧 handler，不增加 attempts，不通过取消标记处理取消；Cancel 直接进入 canceled 并清理索引。

## 2. 调用方与输入

- 调用方：订阅/绑定逻辑队列的 Server 定时回收模块。
- 无业务调用参数；队列固定为 default，每个脚本每轮最多处理 100 个候选（内部常量，不新增公开配置）。调度间隔沿用 WithReclaimInterval，默认 10 秒、最小 2 秒。
- 不传入 Server 当前时间，不传入脚本外扫描后认定“已过期”的 task_id 列表。
- 至少一个负责该队列且回收循环正常运行的 Server 才能周期性推进；仅有在线 Server 不代表回收仍在运行。
- 每轮先调用一次租约回收脚本，正常返回（包括空操作）后再调用一次重试调度脚本。第一阶段报错则结束本轮，不执行第二阶段；两个脚本之间允许其他操作执行，不要求整体原子性。
- 每轮各脚本至多执行一次；取满 100 个也不连续清空积压，等待下一轮。候选数量有界不构成固定执行时长保证。

### Server 调度与请求预算

- Start 接入独立串行 Reclaim 协程，生命周期不绑定 Claim 或 Renew 的 context；不引入公开退出接口。
- 启动后先等待一次随机间隔，每轮结束后再等待下一次。每次独立取配置间隔的 80%～120%，不重叠、不补跑错过的周期。抖动降低同频概率，不保证实例绝不同步。
- 每个脚本独立使用 3 秒请求预算；沿用已有超时包装，共享调用方连接池并保留更短的 I/O 超时，不修改共享客户端配置或新建连接池。
- 沿用 Server 客户端禁用透明重试、启用 ContextTimeoutEnabled 的前置条件。请求超时不能中止 Redis 中已执行的 Lua；脚本成本依靠限批及避免全表扫描控制。

## 3. 执行前置条件

每个 Lua 各调用一次 Redis TIME 得到当前 Unix 毫秒，脚本内复用。候选查询同时返回 score（WITHSCORES），校验它是非负且不超过 9007199254740991 的整数毫秒。同一脚本内候选选择后没有其他请求插入，不为防并发重复读取 score。

写前检查所访问 key 的类型、任务详情为 Hash、task_id 与候选一致、queue 为 default，以及下列状态和索引条件。不扩大为所有历史字段的全量校验。

租约回收候选：

- 从当前 lease ZSET 内选取 score ≤ now 的成员。
- 任务详情存在、status = running，且当前 owner_id、attempt_token、active、lease 关系完整。
- 使用候选查询返回的当前 score 判断到期，不凭成员存在性判断租约失效；owner_id、attempt_token 必须非空。
- attempts、max_retry、可能使用的 retry_delay_ms 合法。

重试调度候选：

- 从当前 retry ZSET 内选取 score ≤ now 的成员。
- 详情存在且 status = retry_wait；重试到期时间仅从 retry ZSET 的当前 score 读取，不与任务详情中的时间字段比较。
- 没有当前执行身份、active 或 lease 成员，且仍有尝试额度。
- 正常状态下不应已存在于 pending，但初版不扫描 pending List 验证成员是否存在，也不新增辅助成员索引。仅依靠合法源状态和原子迁移保证正常并发不重复入队；已损坏的 retry/pending 双重成员不在本操作的检测保证内。

attempts 必须为正整数且不超过 max_retry + 1；重试调度还要求严格小于该上限。数值范围沿用 Submit 与结果上报约束；仅在租约回收需要进入 retry_wait 时检查 retry_delay_ms 及时间加法的精确整数范围。retry_wait 的 owner_id、attempt_token 必须不存在，空字符串残留也不合法。

在上述检查范围内，任务记录缺失、状态与索引不匹配、额度异常或数据类型错误，按可识别的一致性异常处理，不静默执行或声称已经修复。终态残留源索引不属于正常回收路径。

## 4. 成功后的变化与返回值

### 租约到期：running → retry_wait 或 failed

对每个满足当前条件的任务原子完成：

1. 移除 active 和 lease 中的 task_id。
2. 撤销 owner_id、attempt_token。
3. 保存固定 last_error = "lease expired" 和 last_failed_at = now，允许覆盖上一轮失败原因。仅凭租约到期不能断言 handler 是超时、崩溃、失联或 Server 未正确上报。
4. attempts 不变，按额度更新状态及目标索引。

| 条件 | 修改 |
| --- | --- |
| attempts < max_retry + 1 | status = retry_wait；读取 Hash 中 retry_delay_ms，计算 next_retry_at = now + retry_delay_ms，并将该值仅作为 score 加入 retry ZSET；finished_at 为空 |
| attempts = max_retry + 1 | status = failed；设置 finished_at = now，不加入等待索引 |

retry_delay_ms 为 Submit 保存的不可变配置，默认 30 秒，最小 15 秒。生成重试时间不得使用 Server 墙上时钟或临时覆盖值。next_retry_at 仅表示 retry ZSET 的 score，不作为字段单独存入任务详情 Hash；移除 retry 成员即移除对应重试到期时间，不额外读写或清理详情时间字段。

### 重试到期：retry_wait → pending

原子完成：

1. 检查当前 status、retry score 及其他前置条件。
2. 从 retry 移除 task_id。
3. 设置 status = pending。
4. 将 task_id 加入对应逻辑队列 pending 一次。

不增加 attempts，不创建执行身份或 lease，不清空最近失败信息。只有后续 Claim 成功授予执行权时才增加一次 attempts。

返回 error：nil 表示本次调用正常处理完成，允许没有到期任务；不表示队列没有剩余到期任务或所有任务已经恢复。

## 5. 原子性与不变量

候选查找、当前记录读取、状态和到期判断、次数判断、时间计算及完整迁移，在同一 Lua 脚本内完成；不拆成脚本外扫描再直接修改。

- Lua 使用当前数据判断，不信任旧扫描结果。
- running 正常对应 active、lease 和当前执行身份。
- retry_wait 正常对应 retry ZSET 成员，其 score 为 next_retry_at；任务详情不保存该时间，也不保留当前执行身份、active 或 lease。
- pending 正常对应 pending 索引，不同时位于 retry、active、lease。
- failed、completed、canceled 为终态，不被 Reclaim 再次调度。
- 每次迁移从源索引移除并只加入一次目标索引，回收和重试调度都不增加 attempts。
- now 等于 lease score 时可回收；now 等于 retry score 时可进入 pending。

每个脚本分为整批校验和整批迁移两个阶段。先完成本批全部候选、相关 key 类型、额度和时间计算的校验，全部通过后才开始写入。可预见的校验异常保证当前脚本整批零写入；一个坏候选会阻塞本批健康任务，不自动跳过或隔离。此保证不覆盖写入阶段的运行时故障，不能依赖 Lua 出错后自动回滚，也不承诺两个脚本整体回滚。

## 6. 失败及结果未知时的行为

- 没有到期任务：正常空操作，不报业务错误。
- 暂时性网络错误或响应丢失：结束本轮，不立即重试，等待下一次定时调度，从当前数据重新选候选；沿用 Claim、Renew、结果上报共用的网络错误分类。
- 一致性异常、明确不可恢复的存储错误及非法响应：停止本实例 Claim 和 Reclaim，通过现有 Errors 机制报告；保留 Renew、已有 Handler 和结果上报，不直接据此取消本地执行。不自动修复或自动恢复已停止循环。
- Errors 仍只通知实例首个致命错误，不是完整故障明细；后续通知即使被抑制，也必须执行停止对应循环的处理。
- error 不等于本轮没有迁移。当前脚本写前校验异常保证该脚本零写入，但前一个脚本可能已成功；运行时故障可能部分写入，网络错误或非法响应可能对应已生效操作，不回滚或盲目重放。
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

不能仅以“使用 Lua”宣称幂等；幂等依赖合法的当前状态、源索引、到期时间与原子迁移共同成立。retry 移除、状态修改和 pending 入队在同一脚本中完成，使后续回收器不能再次选择已迁移成员；不是依靠 pending List 自身去重。若数据已异常地同时存在于 retry 和 pending，本操作可能追加重复项，不承诺检测或修复。

不立即重试是为了控制故障期间的请求压力、保持调度简单，不是因为重新选择当前候选必然重复入队；下一轮仍须完整校验当前状态。

## 9. 崩溃与恢复行为及已知限制

- 某个 Server 崩溃：只要还有负责该队列且回收循环正常运行的 Server，就继续定时回收。
- 坏候选可能使多个实例的回收都停止，增加实例或仅重启进程不能替代数据修复。没有健康回收循环时，不承诺自动恢复。
- 第一阶段持续失败会延迟第二阶段的重试调度；初版不将两个阶段拆成独立调度循环。
- 限批、随机等待和请求耗时共同影响恢复延迟；积压较大时推进较慢，不承诺固定恢复时限或吞吐量。多实例增加处理能力的同时也增加 Redis 请求量。
- Server 在脚本完成后、收到响应前崩溃：迁移已经生效，其他 Server 按当前源索引处理，不重复重建队列。
- Server 在请求生效前退出：下一轮仍可选取原到期成员。
- Redis 自身故障后的数据保留取决于尚待明确的持久化、复制和恢复配置；重启不清空或盲目重建索引。
- Lua 错误不自动回滚此前写入。部分写入不完整状态的自动检测、对账和修复不在第一版范围，不声称重启或周期回收必然解决。
- execution_timeout_ms 只由 Server 本地控制，本操作只按 lease score 判断租约到期，不读取 execution_deadline_at。
- 本地超时 ReportFailure 未成功时，状态可能延迟到 lease 到期才推进。
- 本地租约到期保护暂未开发，旧 handler 可能仍在执行；本操作不保证强制终止或撤销业务副作用。
- 授予执行权消耗尝试预算，未完成的尝试按预算恢复，不保证 handler 必定启动。

## 10. 验收场景

- 过期 running、有剩余额度：移除 active、lease，撤销身份，进入 retry_wait；错误时间、retry score 正确，任务详情不含 next_retry_at 字段，attempts 不变。
- 过期 running、额度耗尽：进入 failed，finished_at 正确，没有等待、执行索引或当前身份。
- 未到期租约：不修改；边界 now = score 时允许回收。
- retry 到期：移除 retry、改 pending、入 pending 一次；未到期不迁移。
- 两 Server 同时回收同一批任务：不重复入 retry 或 pending，不重复消耗尝试次数。
- Renew 先延长、Cancel 先取消、上报先成功：依据当前状态和 score 处理。
- 回收后重新领取：旧 token 续租和上报失败，新一轮未到期租约不受影响。
- 一台 Server 崩溃：另一台负责同队列的 Server 可以继续回收。
- 丢失回收响应：下一轮按当前数据处理，无重复入队。
- 注入不一致索引：返回或记录异常，不把自动修复列为初版能力。
- 在批次末尾放入异常候选：当前脚本整批零写入，前面的健康候选也不迁移；覆盖字段缺失、身份残留、非法额度、key 类型和时间计算异常。
- 超过 100 个到期候选：单脚本只处理至多 100 个，不在同轮继续清空；后续轮次继续推进。
- 第一阶段成功、第二阶段校验失败：保留第一阶段迁移，第二阶段整批零写入；不将整体 error 解释为零迁移。
- 第一阶段网络错误或致命错误：均跳过第二阶段；前者下一轮恢复，后者停止 Claim/Reclaim，健康任务仍可 Renew 和上报。
- 非法响应作为结果未知且致命处理，不回滚已生效操作；Errors 已通知其他故障时仍执行回收停止逻辑。
- 受控计时验证首轮及后续 80%～120% 等待、串行不重叠、不补跑、每脚本独立预算及保留更短 I/O 超时。真实 TCP 测试验证网络等待边界，不能仅以 context 已取消证明请求已返回。
- pending 检查边界：不要求全表成员扫描或损坏数据自动去重；并发无重复入队测试从合法状态开始，不将此结论扩展到 retry/pending 双重成员。
