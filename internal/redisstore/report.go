package redisstore

import (
	"context"
	"errors"
	"fmt"

	"github.com/redis/go-redis/v9"
)

var (
	// ErrReportArgument 表示上报输入非法，未发送命令。
	ErrReportArgument = errors.New("invalid report argument")
	// ErrReportInconsistent 表示当前执行所需存储数据不一致。
	ErrReportInconsistent = errors.New("inconsistent report task")
	// ErrReportResponse 表示响应非法，上报可能已生效。
	ErrReportResponse = errors.New("invalid report response")
)

// CompleteParams 保存已经序列化的成功结果，空值也须写入 result。
type CompleteParams struct {
	TaskID       string
	AttemptToken string
	Result       []byte
}

// Complete 执行一次原子成功上报；仅 err=nil 时 applied 有确定含义。
// 调用方禁用透明重试；仅明确 NOSCRIPT 允许脚本缓存回退。
func Complete(ctx context.Context, rdb *redis.Client, p CompleteParams) (bool, error) {
	if len(p.Result) > 1000 {
		return false, fmt.Errorf("%w: result exceeds 1000 bytes", ErrReportArgument)
	}
	return runReport(ctx, rdb, completeScript, "complete", p.TaskID, p.AttemptToken, p.Result,
		[]string{"taskq:meta:" + p.TaskID, "taskq:default:active", "taskq:default:lease"})
}

// FailureParams 保存首次请求前已经固定的失败诊断。
type FailureParams struct {
	TaskID       string
	AttemptToken string
	ErrorMessage string
}

// ReportFailure 执行一次原子失败上报，由已保存的尝试预算决定目标状态。
// 仅 err=nil 时 applied 有确定含义；Server 负责有界网络重试。
func ReportFailure(ctx context.Context, rdb *redis.Client, p FailureParams) (bool, error) {
	if p.ErrorMessage == "" || len(p.ErrorMessage) > 4096 {
		return false, fmt.Errorf("%w: error message must contain 1..4096 bytes", ErrReportArgument)
	}
	return runReport(ctx, rdb, failureScript, "ReportFailure", p.TaskID, p.AttemptToken, p.ErrorMessage,
		[]string{"taskq:meta:" + p.TaskID, "taskq:default:active", "taskq:default:lease", "taskq:default:retry"})
}

func runReport(ctx context.Context, rdb *redis.Client, script *redis.Script, operation, id, token string, content any, keys []string) (bool, error) {
	if rdb == nil || id == "" || token == "" {
		return false, fmt.Errorf("%w: client, task and token required", ErrReportArgument)
	}
	response, err := script.Run(ctx, rdb, keys, id, token, content).Result()
	if err != nil {
		if redis.HasErrorPrefix(err, "TASKQ_REPORT_INCONSISTENT") {
			err = fmt.Errorf("%w: %w", ErrReportInconsistent, err)
		}
		return false, fmt.Errorf("%s task %q token %q: %w", operation, id, token, err)
	}
	value, ok := response.(int64)
	if !ok || value != 0 && value != 1 {
		return false, fmt.Errorf("%w: %s task %q token %q: expected integer 0 or 1", ErrReportResponse, operation, id, token)
	}
	return value == 1, nil
}
