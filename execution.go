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

func reportingForbidden(ctx context.Context) bool {
	cause := context.Cause(ctx)
	return errors.Is(cause, errExecutionLost) || errors.Is(cause, errExecutionCanceled) || errors.Is(cause, errExecutionInconsistent)
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
	reportCtx, cancelReports := context.WithCancel(context.Background())
	record := &execution{executionTimeout: timeout, reportCtx: reportCtx, cancelReports: cancelReports}
	record.cancel = func(cause error) {
		if errors.Is(cause, errExecutionLost) || errors.Is(cause, errExecutionCanceled) || errors.Is(cause, errExecutionInconsistent) {
			cancelReports()
		}
		cancel(cause)
	}
	if err := s.addExecution(key, record); err != nil {
		cancel(nil)
		cancelReports()
		<-s.slots
		return nil, err
	}
	s.mu.Lock()
	handler := s.handlers[task.Type]
	s.mu.Unlock()
	done := make(chan struct{})
	go func() {
		defer close(done)
		defer cancelReports()
		if record.reportForbidden.Load() || reportingForbidden(ctx) {
			s.deleteExecution(key)
			cancel(nil)
			<-s.slots
			return
		}
		if handler == nil {
			// 未启动 Handler 时，槽仍由接收/监督路径归还，不创建虚假的执行协程。
			<-s.slots
			if !record.reportForbidden.Load() && !reportingForbidden(ctx) {
				s.onFailure(ctx, record, key, fmt.Errorf("no handler registered for task type %q", task.Type))
			}
			s.deleteExecution(key)
			cancel(nil)
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
			// select 可在取消已知时选中结果；开始上报前必须再次检查原因，不能查表代替。
			if !record.reportForbidden.Load() && !reportingForbidden(ctx) {
				if result.err == nil {
					s.onComplete(ctx, record, key, result.result)
				} else {
					s.onFailure(ctx, record, key, result.err)
				}
			}
			s.deleteExecution(key)
			cancel(nil)
		case <-timer.C:
			// 先移出未来的续租集合并取消，再处理超时；迟到结果只留在缓冲 channel。
			s.deleteExecution(key)
			cancel(context.DeadlineExceeded)
			if !record.reportForbidden.Load() && !reportingForbidden(ctx) {
				s.onFailure(ctx, record, key, context.DeadlineExceeded)
			}
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
