package redisstore

import (
	"context"
	"errors"
	"fmt"
	"unicode/utf8"

	"github.com/redis/go-redis/v9"
)

var (
	// ErrCancelInconsistent 表示写入前发现任务详情或索引不一致。
	ErrCancelInconsistent = errors.New("inconsistent cancel task")
	// ErrCancelResponse 表示响应不符协议，取消结果未知。
	ErrCancelResponse = errors.New("invalid cancel response")
)

// CancelResult 是原子迁移返回的业务结果。
type CancelResult struct {
	Outcome           string
	Status            string
	NotificationError string
}

// Cancel 使用公开 Client 已校验的任务 ID 执行取消。
func Cancel(ctx context.Context, rdb *redis.Client, taskID string) (CancelResult, error) {
	utf8ID := 0
	if utf8.ValidString(taskID) {
		utf8ID = 1
	}
	response, err := cancelScript.Run(ctx, rdb, []string{"taskq:meta:" + taskID}, taskID, utf8ID).Result()
	if err != nil {
		if redis.HasErrorPrefix(err, "TASKQ_CANCEL_INCONSISTENT") {
			return CancelResult{}, fmt.Errorf("%w: %w", ErrCancelInconsistent, err)
		}
		if errors.Is(err, redis.Nil) {
			return CancelResult{}, fmt.Errorf("%w: %w", ErrCancelResponse, err)
		}
		return CancelResult{}, err
	}
	return parseCancelResponse(response)
}

func parseCancelResponse(response any) (CancelResult, error) {
	values, ok := response.([]any)
	if !ok || len(values) != 3 {
		return CancelResult{}, fmt.Errorf("%w: expected three-element array", ErrCancelResponse)
	}
	outcome, outcomeOK := values[0].(string)
	status, statusOK := values[1].(string)
	diagnostic, diagnosticOK := values[2].(string)
	if !outcomeOK || !statusOK || !diagnosticOK {
		return CancelResult{}, fmt.Errorf("%w: expected string fields", ErrCancelResponse)
	}
	switch {
	case outcome == "canceled" && status == "canceled":
		return CancelResult{Outcome: outcome, Status: status, NotificationError: diagnostic}, nil
	case diagnostic == "" && outcome == "not_found" && status == "",
		diagnostic == "" && outcome == "already_terminal" && (status == "completed" || status == "failed" || status == "canceled"):
		return CancelResult{Outcome: outcome, Status: status}, nil
	default:
		return CancelResult{}, fmt.Errorf("%w: unexpected outcome %q with status %q", ErrCancelResponse, outcome, status)
	}
}
