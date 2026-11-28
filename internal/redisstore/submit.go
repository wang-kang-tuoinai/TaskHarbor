// Package redisstore 封装 Redis 原子操作，不持有连接生命周期。
package redisstore

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

// 提交和读取任务详情使用相同的初始配置边界。
const (
	MaxPayloadSize            = 1 << 20
	MaxTaskTypeSize           = 128
	MaxSubmitRetries          = 2147483646
	MaxSubmitExecutionTimeout = 24 * time.Hour
	MinSubmitRetryDelay       = 15 * time.Second
	MaxSubmitRetryDelay       = 7 * 24 * time.Hour
)

var (
	// ErrConflict 表示已存初始配置与本次提交不同。
	ErrConflict = errors.New("submit configuration conflict")
	// ErrIncomplete 表示已存初始配置缺失，优先于冲突返回。
	ErrIncomplete = errors.New("incomplete submit configuration")
	// ErrInvalidResponse 表示脚本响应不符合协议，不能据此确认提交结果。
	ErrInvalidResponse = errors.New("invalid submit response")
)

// SubmitParams 是 Client 已校验、时长已规范化为毫秒的提交参数。
type SubmitParams struct {
	TaskID             string
	TaskType           string
	Queue              string
	Payload            []byte
	MaxRetry           int
	ExecutionTimeoutMS int64
	RetryDelayMS       int64
}

// SubmitResult 是脚本返回的提交确认。
type SubmitResult struct {
	TaskID  string
	Created bool
}

// ValidateTaskType 检查提交参数和已保存任务详情使用的任务类型。
func ValidateTaskType(taskType string) error {
	if len(taskType) == 0 || len(taskType) > MaxTaskTypeSize {
		return fmt.Errorf("task type must contain 1..%d ASCII bytes", MaxTaskTypeSize)
	}
	for i := 0; i < len(taskType); i++ {
		b := taskType[i]
		if !(b >= 'a' && b <= 'z' || b >= 'A' && b <= 'Z' || b >= '0' && b <= '9' || b == '_' || b == '-' || b == '.' || b == ':') {
			return fmt.Errorf("invalid task type byte at %d", i)
		}
	}
	return nil
}

// Submit 只负责序列化和调用；参数校验由公开 Client 边界完成。
func Submit(ctx context.Context, rdb *redis.Client, p SubmitParams) (*SubmitResult, error) {
	response, err := submitScript.Run(ctx, rdb,
		[]string{"taskq:meta:" + p.TaskID, "taskq:" + p.Queue + ":pending"},
		p.TaskID, p.TaskType, p.Payload, p.MaxRetry, p.ExecutionTimeoutMS, p.RetryDelayMS, p.Queue,
	).Result()
	if err != nil {
		return nil, fmt.Errorf("run submit script: %w", err)
	}
	return parseSubmitResponse(response, p.TaskID)
}

func parseSubmitResponse(response any, taskID string) (*SubmitResult, error) {
	values, ok := response.([]any)
	if !ok || len(values) != 2 {
		return nil, fmt.Errorf("%w: expected two-element array", ErrInvalidResponse)
	}
	status, statusOK := values[0].(string)
	id, idOK := values[1].(string)
	if !statusOK || !idOK || id != taskID {
		return nil, fmt.Errorf("%w: expected string status and matching task ID", ErrInvalidResponse)
	}
	switch status {
	case "created", "duplicate":
		return &SubmitResult{TaskID: id, Created: status == "created"}, nil
	case "conflict":
		return nil, ErrConflict
	case "incomplete":
		return nil, ErrIncomplete
	default:
		return nil, fmt.Errorf("%w: unknown status %q", ErrInvalidResponse, status)
	}
}
