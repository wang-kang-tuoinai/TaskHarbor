package taskq

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/wang-kang-tuoinai/TaskHarbor/internal/redisstore"
)

// CancelOutcome 区分本次取消生效、任务不存在和任务已经终态。
type CancelOutcome string

const (
	// CancelCanceled 表示本次取消使任务进入 canceled。
	CancelCanceled CancelOutcome = "canceled"
	// CancelNotFound 表示任务不存在。
	CancelNotFound CancelOutcome = "not_found"
	// CancelAlreadyTerminal 表示任务已处于终态。
	CancelAlreadyTerminal CancelOutcome = "already_terminal"
)

// CancelResult 表示 Redis 确认的取消结果，仅在 error 为 nil 时有效。
type CancelResult struct {
	Outcome CancelOutcome
	Status  string // 当前任务状态，任务不存在时为空。
}

// Cancel 在 default 队列中原子取消非终态任务。任务不存在或已经终态会返回正常结果。
// Redis 已确认的取消会撤销执行权，本地 Handler 的退出取决于尽力通知和 Handler 自身。
// 网络错误可能出现在取消生效后。调用方若启用 Redis 透明重试，可能最终收到
// already_terminal/canceled。ContextTimeoutEnabled 用于将 ctx 截止时间应用到网络读写。
func (c *Client) Cancel(ctx context.Context, taskID string) (CancelResult, error) {
	if taskID == "" {
		return CancelResult{}, fmt.Errorf("%w: task ID must not be empty", ErrInvalidArgument)
	}
	result, err := redisstore.Cancel(ctx, c.rdb, taskID)
	if err != nil {
		if errors.Is(err, redisstore.ErrCancelInconsistent) || errors.Is(err, redisstore.ErrCancelResponse) {
			return CancelResult{}, fmt.Errorf("%w: cancel task %q: %w", ErrStorageData, taskID, err)
		}
		return CancelResult{}, fmt.Errorf("cancel task %q: %w", taskID, err)
	}
	if result.NotificationError != "" {
		slog.WarnContext(ctx, "task cancellation notification skipped or failed", "task_id", taskID, "error", result.NotificationError)
	}
	return CancelResult{Outcome: CancelOutcome(result.Outcome), Status: result.Status}, nil
}
