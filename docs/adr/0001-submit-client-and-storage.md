# ADR-0001：Client Submit 接口与 Redis 存储边界

- 状态：已接受
- 日期：2026-10-31

## 背景

项目按 Client Submit → Server Claim → Server Renew 推进。Submit 尚未实现，需要明确连接归属、提交配置和 Lua 的写前检查。原契约使用整数秒且要求 max_retry 必填；本决策更新这些规则。

## 决策

1. Client 使用结构体，NewClient 接收调用方创建的 *redis.Client；构造只初始化本地状态。调用方配置并关闭 Redis 客户端。Submit 是接收 context 的方法。
2. 每次提交创建独立配置，应用 SubmitOption 后统一校验。零个选项合法，选项元素为 nil 时返回参数错误。payload 的 nil 与空切片统一为零长度字节内容。
3. max_retry 可选，默认 0，与显式 0 等价；仍参与幂等比较。
4. 输入支持整数毫秒。存储字段为 execution_timeout_ms、retry_delay_ms；默认分别为 60000、30000，后者最小为 15000。Go 使用 time.Duration，拒绝非整数毫秒，不截断。created_at 保留原名，使用 Redis Unix 毫秒时间戳。
5. 第一版 Client 与 Server 均固定使用 default 队列，不提供 queue 选项。Hash 保存 queue，并与其余五项初始配置共同参与幂等比较。task_id 继续在平台范围唯一。
6. internal/redisstore 使用包函数封装 keys、args、脚本调用和响应解析，不新建连接池。scripts.go 嵌入 scripts/submit_task.lua。内部包不反向导入 taskq；Client 将内部结果与错误转换为公开类型。
7. Lua 已有记录分支循环比较六项配置，任一字段缺失返回 incomplete，优先于 conflict；空字符串是合法值。新建分支在 HSET 前检查 pending 类型只能为 none 或 list，再读取 TIME、写 Hash、RPUSH。不修复异常记录，不重复入队。
8. 平台不增加 Submit 自动重试循环。go-redis 的连接、超时及底层重试配置由调用方管理，平台不修改共享客户端配置。当前依赖默认 MaxRetries 为 3，-1 禁用重试；接入时应显式选择并在依赖升级时复核。底层重试可能在首次响应丢失后返回 duplicate，Created=false 仍是成功确认。Claim 不具有同样的幂等性，实施时必须单独落实禁止不安全透明重试的要求。

## 后果

- Submit 幂等比较使用规范化后的配置；省略默认值与显式默认值相同。
- 默认 max_retry=0 时，一次授予执行权即消耗全部尝试预算，即使 handler 未启动，租约回收也可能直接进入 failed。
- Claim、Renew、结果上报及 Reclaim 文档统一引用毫秒字段；重试到期时间直接相加，不再乘 1000。
- incomplete 是存储数据异常，不是普通配置冲突；网络错误和异常响应不证明未创建。
- Lua 写前类型检查避免可预见的 WRONGTYPE 部分写入，但不扩大原契约的持久性、运行时错误或自动修复保证。
- 此决策不代表代码已实现，也不提供旧数据的自动迁移；若已有试验数据采用旧字段，应单独决定处理方式。

## 替代方案

- 每次 Submit 传入 Redis 客户端：不采用，连接依赖由 Client 持有。
- 第一版开放自定义队列：不采用，当前只实现默认队列。
- 仅支持整数秒：不采用，允许毫秒级时长输入。
- 缺失字段按空字符串比较：不采用，避免把异常记录误判为重复提交。

## 关联

- [Submit 操作契约](../Submit操作契约.md)
- [Submit 实施计划](../Submit实施计划.md)
