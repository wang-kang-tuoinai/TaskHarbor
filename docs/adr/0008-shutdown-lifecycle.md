# ADR-0008：Shutdown 的排空、强制取消与实际停止

- 状态：已接受
- 日期：2026-11-15
- 关系：细化并部分替代 ADR-0003 的“已确认、延期实现的完整退出语义”；补充 ADR-0006 的 Server 级上报取消来源。其余任务执行权、资源归属和故障范围保持不变。

## 背景

Handler 实际退出、监督结束、结果被 Redis 接受及 Server 停止是不同事件。只等执行槽会漏掉上报，只等监督会漏掉不响应取消的 Handler；续租资格表也不能证明本地工作全部结束。退出调用必须有等待上限，同时不能在期限到达时把仍存活的工作标成已停止。

多个 Shutdown 调用各自携带期限，会使退出控制权不明确。当前 abort 的启动前检查与 Handler 启动之间还有竞争窗口；独立于 Handler 的上报 context 需要统一的强制取消来源，且不能只遍历续租资格表寻找仍在上报的任务。

## 决策

1. 公开 Shutdown(ctx) error，本轮不同时增加 Close。首次运行态调用拥有退出控制权，context 约束整个等待过程，包括等待 Claim 和后台循环结束，不仅是任务排空。
2. 正常阶段停止 Claim，维持已有 Renew/Reclaim、Handler 与结果上报。在途 Claim 明确成功后纳入排空；不退回 attempts，不自行重入队。
3. 一个执行生命周期 WaitGroup 分别登记监督与 Handler。监督启动前登记，实际清理完成后结束计数；Handler 启动前登记，实际退出并归还槽后结束计数。等待 Claim 接管结束后才等待计数归零。监督登记 Handler 时自身仍占有计数，不能出现零计数等待后又新增工作的窗口。
4. 正常排空要求 Handler 和监督都结束，再停止并等待 Renew/Reclaim 等后台循环。执行表及槽不充当排空判据。nil 只表示本实例完全停止，不表示任务全部 completed 或全部上报成功；不等待整个 Redis 队列清空。
5. context 取消或到期且尚未完全停止时，关闭 abort、取消 Server 级上报 context 及后台循环，请求进入强制阶段，返回对应 context 错误。不无限等待网络或不合作 Handler 后才返回；唯一退出协调流程继续收尾，实际全部退出后才置为 stopped。
6. 所有任务的 reportCtx 从 Server 级上报 context 派生，仍与 Handler 超时、Claim/Renew 取消独立。单任务撤销只取消自身；强制阶段统一取消，包括已移出续租资格表的超时上报。取消不保证底层请求立即返回，不撤回已生效的 Redis 操作。
7. 复用 reportingStopped，在短临界区内完成最终启动检查、Handler 计数登记及启动；关闭 abort 使用同一把锁，不新增 forcing 布尔状态。清理、上报及 Handler 执行均在锁外。保证强制阶段开始后不再批准新 Handler；此前已获准的 goroutine 可能稍后才执行，不承诺业务代码一行都不再运行，也不扩大为消除全部单任务撤销竞争。
8. 尚未 Start 时 Shutdown 直接关闭生命周期并返回 nil，之后不能 Start；stopping 时再次调用立即返回可识别的 ErrShutdownInProgress，不影响首次调用的期限；stopped 时返回 nil。Start 与 Shutdown 同步决定先后，不能退出后又启动循环，不持锁等待完成信号。
9. 已停止或尚未启动的关闭不因传入 context 已取消而报错；运行态收到已取消 context 则请求强制阶段并返回其错误。等待时完成通知与 context 同时就绪，可复查是否完全停止：已停止返回 nil，否则请求强制阶段并返回 context 错误，不按实际发生时间做复杂排序。
10. 首次超时后保持 stopping 直至真实退出，期间重复调用仍报进行中；真实 stopped 后重复调用返回 nil，不追溯上次调用错误。不关闭调用方的 Redis 客户端或共享池，不伪造业务失败/取消，不提前归还存活 Handler 的槽。

## 后果与限制

- 一个不响应取消的 Handler 可使状态长期停留 stopping，唯一协调等待也可能持续存在。超时返回不表示全部 goroutine 已退出。
- 正常排空保留结果提交机会；强制阶段可能留下 running 等状态供其他健康回收器推进，不保证任务已完成或实例退出后立即回收。
- 在途 Renew 或上报仍可能生效，context 取消不能撤回业务副作用。调用方不能仅因 Shutdown 超时返回就认为共享资源已无人使用。
- 短临界区提供明确启动边界，但不持锁等待业务或网络。无需第二套执行表或保存全部单任务 done channel。
- 本 ADR 记录已确认设计，不表示 Shutdown 已实现或验证。

## 替代方案

- 只等监督、执行表或槽：不采用，均不能同时证明 Handler 和上报生命周期结束。
- 每个并发调用的 context 都能强制停止：不采用，让首次调用独占退出控制权，避免后续短期限改变正在进行的退出。
- 超时后继续无期限等完再返回：不采用，破坏调用等待预算；立即标记 stopped 也不采用，会虚报实际停止。
- 扫描 executions 取消上报：不采用，表中不包含所有仍在上报的任务。统一父 context 能覆盖这些请求。
- 新增 forcing 与 abort 重复表示强制状态：不采用，复用既有停止检查并建立共同同步边界。

## 关联

- [ADR-0003](0003-execution-supervision-and-lifecycle.md)
- [ADR-0006](0006-result-reporting-lifecycle.md)
- [领域术语](../../GLOSSARY.md)
