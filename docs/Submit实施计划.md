# Client Submit 实施计划

状态：Client Submit 已于 2026-10-31 实现并验证。本文保留实施要求；实际完成内容、测试命令及验证边界见[当前进度](当前进度.md)。Claim、Renew 未在本次实施。

依据：[ADR-0001](adr/0001-submit-client-and-storage.md)、[Submit 契约](Submit操作契约.md)。顺序保持 Submit → Claim → Renew。

## 1. 参数与边界

- NewClient 校验 rdb 非 nil，持有调用方的客户端；不 Ping、不启动 goroutine、不负责 Close。
- Submit(ctx, taskID, taskType, payload, opts...) 返回 *SubmitResult 和 error。
- taskID 非空，不裁剪或改写；taskType 非空并遵守最终公布的格式限制。
- payload 最大 1 MiB（1048576 字节），按原始字节比较；nil 和空切片等价，不强制 JSON。
- max_retry 默认 0；拒绝负数，须保证 max_retry + 1 在 Go 和 Lua 中可安全表示。
- execution_timeout_ms 默认 60000，最小 1；retry_delay_ms 默认 30000，最小 15000。time.Duration 必须整除 time.Millisecond，拒绝小数毫秒，不截断。
- 不开放 queue 选项，统一使用 default。每次创建独立配置，依次应用选项后校验；拒绝 nil 选项元素。
- 已确认：task_type 为 1～128 字节，只允许 ASCII 字母、数字、下划线、连字符、点和冒号，大小写敏感；execution_timeout_ms 最大 86400000（24 小时），retry_delay_ms 最大 604800000（7 天），均包含上限。实施时进一步确认 max_retry 上限为 2147483646，保证 max_retry + 1 在 32/64 位 Go int 和 Lua 中均可精确表示；先验证本机 Redis 5.0.14.1，其他版本兼容性及统一支持版本范围待定。

## 2. 文件与依赖方向

- client.go：Client、构造函数、提交选项、默认值、公开参数错误及结果转换。
- internal/redisstore/submit.go：包函数接收 rdb 和规范化参数，生成 keys/args，调用脚本并解析响应。
- internal/redisstore/scripts.go：使用 go:embed 嵌入脚本，集中构造 redis.Script。
- internal/redisstore/scripts/submit_task.lua：原子判断、创建和入队。
- taskq 导入 redisstore，后者不导入 taskq；不引入生产依赖或通用存储框架。

## 3. Lua 流程

1. 对输入执行必要的写前检查，key 由内部调用代码按固定规则生成：taskq:meta:<task_id> 与 taskq:default:pending。
2. meta 已存在时通过 HMGET 读取 task_type、payload、max_retry、execution_timeout_ms、retry_delay_ms、queue。错误 key 类型由 HMGET 在写入前拒绝。
3. 循环逐项比较：false 表示缺失，记录 any_missing；不同值记录 mismatch。先返回 incomplete，再判断 duplicate 或 conflict。不扫描或修改 pending，也不检查所有运行字段。
4. 新建分支在 HSET 之前用 TYPE 确认 pending 为 none 或 list，否则报错且不写入。
5. 调用一次 Redis TIME 取得 created_at，初始化配置、status=pending、attempts=0、queue=default，然后 RPUSH。运行身份、结果和租约字段不初始化。
6. 返回 {status, task_id}。不因重复请求重置状态，不自动补字段或修复记录。

## 4. 响应与重试

校验响应数组长度、元素类型和状态。created/duplicate 分别返回 Created=true/false；conflict 转为可识别的 ID 内容冲突；incomplete 转为可识别的存储数据异常。底层错误保留错误链；redis.Nil、异常结构和未知状态均不能作为成功或任务不存在的证据。

连接归属与重试配置以 ADR-0001 为统一说明，不添加平台重试循环。调用方保留 ID、原始 payload 和有效配置。Claim 阶段单独检查 Redis 自动重试，不擅自修改共享 rdb。

## 5. 验证与完成条件

- 参数：空 ID/type、payload 超限、nil 选项、负重试次数、非整数毫秒、默认值、时长边界；非法输入不发 Redis 写请求。
- 正常创建：字段名、毫秒值、created_at、queue、attempts 与 pending 正确，二进制 payload 无损。
- 幂等：nil/空 payload、max_retry 省略/0、默认时长省略/显式一致；同 ID 同配置并发只创建入队一次；不同配置冲突；已运行或终态记录不重置。
- 异常：pending 错误类型不留下 Hash；配置字段缺失返回 incomplete，包含同时存在 mismatch 的场景；queue 不同返回 conflict；异常响应不误报成功。
- 故障：创建响应丢失后同 ID 同配置重试；脚本缓存缺失；底层自动重试导致 duplicate 的返回语义。
- 先运行定向测试，再运行 go test ./...；Redis 原子性和 Lua 错误路径用真实 Redis 集成测试验证。按实现内容决定 go vet 与并发检测；未运行项如实记录。
- 完成后更新当前进度，只报告实际实现和验证结果。
