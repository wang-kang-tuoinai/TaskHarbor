# 架构决策记录

ADR 记录已讨论的背景、决策、替代方案及后果。状态使用“提议”“已接受”“已废弃”或“已替代”；后续改变已接受决策时新增 ADR 并注明替代关系。

| 编号 | 标题 | 状态 |
| --- | --- | --- |
| [0001](0001-submit-client-and-storage.md) | Client Submit 接口与 Redis 存储边界 | 已接受 |
| [0002](0002-claim-failure-and-retry-policy.md) | Claim 的重试边界与异常处理 | 已接受 |
| [0003](0003-execution-supervision-and-lifecycle.md) | 执行监督、资源归属与分阶段生命周期 | 已接受 |

0002、0003 补充已讨论的 Server 设计，不替代 0001。0003 区分本轮实施边界与已确认但延期实现的退出、续租语义；“已接受”不表示代码已经实现。
