package taskq

import (
	"context"
	"errors"
	"fmt"
	"time"
)

var (
	errExecutionLost         = errors.New("execution authority lost")
	errExecutionCanceled     = errors.New("execution cancellation confirmed")
	errExecutionInconsistent = errors.New("execution data inconsistent")
)

// executionKey 独立于可变的 Task 字段来标识一次执行尝试。
type executionKey struct {
	taskID       string
	attemptToken string
}

// execution 保存续租资格，不证明租约有效或 Renew 正在运行。登记后字段不可变。
// 请在表锁之外调用 cancel。
type execution struct {
	executionTimeout       time.Duration
	cancel                 context.CancelCauseFunc
	reportCtx              context.Context // 继承实例级上报取消，独立于 Handler 超时及 Claim/Renew。
	cancelExecutionReports context.CancelFunc
}

func (s *server) addExecution(key executionKey, e *execution) error {
	if key.taskID == "" || key.attemptToken == "" || e == nil || e.cancel == nil || e.executionTimeout <= 0 {
		return errors.New("invalid execution")
	}
	s.executionsMu.Lock()
	defer s.executionsMu.Unlock()
	if _, exists := s.executions[key]; exists {
		return errors.New("execution already exists")
	}
	s.executions[key] = e
	return nil
}

func (s *server) getExecution(key executionKey) (*execution, bool) {
	s.executionsMu.RLock()
	defer s.executionsMu.RUnlock()
	e, ok := s.executions[key]
	return e, ok
}

func (s *server) deleteExecution(key executionKey) {
	s.executionsMu.Lock()
	defer s.executionsMu.Unlock()
	delete(s.executions, key)
}

// snapshotExecutions 复制表条目，共享不可变的 execution 记录。
// 请在本方法返回后再发起 Redis 请求和取消操作。
func (s *server) snapshotExecutions() map[executionKey]*execution {
	s.executionsMu.RLock()
	defer s.executionsMu.RUnlock()
	snapshot := make(map[executionKey]*execution, len(s.executions))
	for key, e := range s.executions {
		snapshot[key] = e
	}
	return snapshot
}

func (s *server) revokeExecution(key executionKey, record *execution, cause error) {
	record.cancel(cause) // 取消回调必须在表锁外执行。
	s.deleteExecution(key)
}

type executionResult struct {
	result []byte
	err    error
}

// acceptExecution 接收明确领取的任务和独立身份；调用方已占用一个执行槽。
// task 必须非 nil；接收后调用方不再访问它，Handler 对业务视图的修改不改变 key。
// 无论接收是否成功，该槽均交由此路径管理。done 仅表示监督结束，不表示 Handler 已退出。
// 此处不重复领取或增加 attempts；结果状态交由上报操作原子迁移。
// parent 属于本轮任务，不能使用领取循环的取消 context。
func (s *server) acceptExecution(parent context.Context, task *Task, key executionKey, timeout time.Duration) (<-chan struct{}, error) {
	ctx, cancel := context.WithCancelCause(parent)
	reportCtx, cancelExecutionReports := context.WithCancel(s.reportCtx)
	record := &execution{executionTimeout: timeout, reportCtx: reportCtx, cancelExecutionReports: cancelExecutionReports}
	record.cancel = func(cause error) {
		// 使用本次撤销的原因，Handler context 可能已保留了更早的超时原因。
		if forbidsReporting(cause) {
			record.cancelExecutionReports()
		}
		cancel(cause)
	}
	if err := s.addExecution(key, record); err != nil {
		cancel(nil)
		cancelExecutionReports()
		<-s.slots
		return nil, err
	}
	s.mu.Lock()
	handler := s.handlers[task.Type]
	s.mu.Unlock()
	done := make(chan struct{})
	s.executionWG.Add(1)
	go func() {
		defer s.executionWG.Done()
		defer close(done)
		defer cancelExecutionReports()
		if handler == nil {
			// 未启动 Handler 时，槽仍由接收/监督路径归还，不创建虚假的执行协程。
			<-s.slots
			s.reportExecutionResult(ctx, record, key, executionResult{err: fmt.Errorf("no handler registered for task type %q", task.Type)})
			s.deleteExecution(key)
			cancel(nil)
			return
		}
		// Handler 只发送一次；监督先退出后，这一个缓冲仍能接纳迟到结果。
		results := make(chan executionResult, 1)
		// 强制阶段的关闭与最终启动批准共用短临界区。清理和上报必须在锁外。
		s.launchMu.Lock()
		if s.reportingStopped(ctx, record) {
			s.launchMu.Unlock()
			s.deleteExecution(key)
			cancel(nil)
			<-s.slots
			return
		}
		// 计时紧邻 Handler 启动；登记准备只创建可取消 context，不提前消耗执行时长。
		timer := time.NewTimer(timeout)
		defer timer.Stop()
		// 监督仍占有计数，排空等待期间新增 Handler 不会遇到零计数 Add。
		s.executionWG.Add(1)
		go func() {
			defer s.executionWG.Done()
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
		s.launchMu.Unlock()
		select {
		case result := <-results:
			s.reportExecutionResult(ctx, record, key, result)
			s.deleteExecution(key)
			cancel(nil)
		case <-timer.C:
			// 先移出未来的续租集合并取消，再处理超时；迟到结果只留在缓冲 channel。
			s.deleteExecution(key)
			cancel(context.DeadlineExceeded)
			s.reportExecutionResult(ctx, record, key, executionResult{err: context.DeadlineExceeded})
		case <-ctx.Done():
			s.deleteExecution(key)
		case <-s.abort:
			// abort 只撤销本地管理，不等同于已确认的业务失败。
			s.deleteExecution(key)
			cancel(context.Canceled)
		}
	}()
	return done, nil
}

// 已选定的分支可能与取消同时就绪。调用上报前再次检查停止状态，不能查表代替。
func (s *server) reportExecutionResult(ctx context.Context, record *execution, key executionKey, result executionResult) {
	if s.reportingStopped(ctx, record) {
		return
	}
	if result.err == nil {
		s.onComplete(ctx, record, key, result.result)
	} else {
		s.onFailure(ctx, record, key, result.err)
	}
}
