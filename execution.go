package taskq

import (
	"context"
	"fmt"
	"time"
)

type executionResult struct {
	result []byte
	err    error
}

// acceptExecution 接收明确领取的任务和独立身份；调用方已占用一个执行槽。
// task 必须非 nil；接收后调用方不再访问它，Handler 对业务视图的修改不改变 key。
// 无论接收是否成功，该槽均交由此路径管理。done 仅表示监督结束，不表示 Handler 已退出。
// 此处不接入 Redis Claim，不改变任务状态或尝试预算。
func (s *server) acceptExecution(task *Task, key executionKey, timeout time.Duration) (<-chan struct{}, error) {
	ctx, cancel := context.WithCancel(context.Background())
	if err := s.addExecution(key, &execution{executionTimeout: timeout, cancel: cancel}); err != nil {
		cancel()
		<-s.slots
		return nil, err
	}
	s.mu.Lock()
	handler := s.handlers[task.Type]
	s.mu.Unlock()
	done := make(chan struct{})
	go func() {
		defer close(done)
		if handler == nil {
			// 未启动 Handler 时，槽仍由接收/监督路径归还，不创建虚假的执行协程。
			<-s.slots
			s.onFailure(key, fmt.Errorf("no handler registered for task type %q", task.Type))
			s.deleteExecution(key)
			cancel()
			return
		}
		// Handler 只发送一次；监督先退出后，这一个缓冲仍能接纳迟到结果。
		results := make(chan executionResult, 1)
		// 计时紧邻 Handler 启动；登记准备只创建可取消 context，不提前消耗执行时长。
		timer := time.NewTimer(timeout)
		defer timer.Stop()
		go func() {
			// 槽属于实际运行的 Handler；监督超时不能代替其归还。
			defer func() { <-s.slots }()
			var result executionResult
			defer func() {
				if value := recover(); value != nil {
					result.err = fmt.Errorf("handler panic: %v", value)
				}
				result.result = task.result
				results <- result
			}()
			result.err = handler(ctx, task)
		}()
		select {
		case result := <-results:
			if result.err == nil {
				s.onComplete(key, result.result)
			} else {
				s.onFailure(key, result.err)
			}
			s.deleteExecution(key)
			cancel()
		case <-timer.C:
			// 先移出未来的续租集合并取消，再处理超时；迟到结果只留在缓冲 channel。
			s.deleteExecution(key)
			cancel()
			s.onFailure(key, context.DeadlineExceeded)
		case <-s.abort:
			// abort 只撤销本地管理，不等同于已确认的业务失败。
			s.deleteExecution(key)
			cancel()
		}
	}()
	return done, nil
}

// complete 暂为空方法；真实 Redis 上报、错误和重试接口留待后续实现。
func (*server) complete(executionKey, []byte) {}

// reportFailure 暂为空方法；调用它不表示 Redis 已接受失败或推进了状态。
func (*server) reportFailure(executionKey, error) {}
