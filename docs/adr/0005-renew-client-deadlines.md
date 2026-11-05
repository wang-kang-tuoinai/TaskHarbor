# ADR-0005：续租客户端截止时间前置条件

- 状态：已接受
- 日期：2026-11-05
- 关系：部分替代 ADR-0002 的 Server 客户端构造条件；补充 ADR-0004 的网络等待边界。原有重试及故障影响范围不变。

## 背景

Renew 要求单次请求的网络等待受 renewInterval 截止时间约束。当前 go-redis 默认不把请求 context 用于已建立连接的读写；仅使用 context.WithTimeout 无法满足该要求。真实 TCP 实验在 50ms context、250ms socket 超时下，默认配置约 251ms 返回，启用 ContextTimeoutEnabled 后约 50ms 返回。

Options() 返回只读配置，不能由平台修改。WithTimeout 支持复制客户端配置并共享原连接池，无需另建池。用户已确认增加构造前置条件，并要求验证真实网络等待、较短超时、原配置与池共享。

## 决策

1. 调用方构造 Server 的 Redis 客户端时，必须显式设置 MaxRetries=-1 和 ContextTimeoutEnabled=true；NewServer 检查并拒绝不满足条件的客户端。平台不修改调用方配置、不关闭其客户端或池。
2. Renew 使用 WithTimeout 包装，继续共享调用方原连接池。包装的读写预算取 renewInterval 与调用方正值 ReadTimeout、WriteTimeout 的最小值，不延长已有较短超时；原来禁用 I/O 超时的配置也不会使 Renew 跳过 socket deadline。不得关闭该包装而关闭共享池。
3. 每次 Renew 仍创建独立的 context 截止时间，覆盖获取连接及脚本缓存回退等本次请求过程。允许正常调度误差，验收网络 deadline 实际生效，不承诺函数严格在墙钟期限返回。
4. 串行扫描、不补跑错过周期、错误分类和执行监督沿用 ADR-0004，不增加公开配置或 Shutdown。

## 后果

- 已有 Server 调用方需要显式启用 ContextTimeoutEnabled；Submit 单独使用的客户端不因此新增前置条件。
- 两个 I/O 阶段使用同一最短预算，可能比调用方较长的另一个阶段更早超时；只影响包装，不改变原客户端。
- 当前依赖的 WithTimeout 不复制命令 Hook。测试对包装显式安装 Hook，真实网络和池共享以 Dialer、TCP 及 Redis 验证，不修改依赖或增加转发框架。
- 网络持续异常、本地租约保护、容量限制及占位上报等边界仍见 Renew 契约；该决策不提供持久化或完整退出保证。

## 替代方案

- 修改共享 Options：拒绝，违背连接归属且依赖明确声明其只读。
- 只在 goroutine 外等待 context：拒绝，底层请求可能继续等待并与后续扫描重叠。
- 新建专用连接池：不采用，当前已确认复用调用方池。

## 关联

- [ADR-0002](0002-claim-failure-and-retry-policy.md)
- [ADR-0004](0004-renew-failure-and-eligibility.md)
- [Renew 操作契约及配置示例](../Renew操作契约.md)
- [真实网络与共享池测试](../../renew_network_integration_test.go)
