package redisstore

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

var (
	// ErrTaskNotFound 表示成功读取到空的任务详情 Hash。
	ErrTaskNotFound = errors.New("missing task hash")
	// ErrTaskData 表示任务详情 Hash 或读取响应不符合约定。
	ErrTaskData = errors.New("invalid task hash")
)

// TaskInfo 保存任务详情 Hash 中可公开的字段。
type TaskInfo struct {
	TaskID           string
	TaskType         string
	Queue            string
	Payload          []byte
	MaxRetry         int
	ExecutionTimeout time.Duration
	RetryDelay       time.Duration
	Status           string
	Attempts         int
	Result           []byte
	LastError        string
	CreatedAt        time.Time
	LastClaimedAt    *time.Time
	LastFailedAt     *time.Time
	FinishedAt       *time.Time
}

// GetTask 用一条 HGETALL 读取快照，使所有字段来自同一次读取。
func GetTask(ctx context.Context, rdb *redis.Client, taskID string) (*TaskInfo, error) {
	fields, err := rdb.HGetAll(ctx, "taskq:meta:"+taskID).Result()
	if err != nil {
		if redis.HasErrorPrefix(err, "WRONGTYPE") || errors.Is(err, redis.Nil) {
			return nil, fmt.Errorf("%w: %w", ErrTaskData, err)
		}
		return nil, err
	}
	if len(fields) == 0 {
		return nil, ErrTaskNotFound
	}
	return parseTaskInfo(fields, taskID)
}

func parseTaskInfo(fields map[string]string, taskID string) (*TaskInfo, error) {
	if fields["task_id"] != taskID {
		return nil, fmt.Errorf("%w: missing or mismatched task_id", ErrTaskData)
	}
	taskType, queue := fields["task_type"], fields["queue"]
	if err := ValidateTaskType(taskType); err != nil {
		return nil, fmt.Errorf("%w: task_type: %v", ErrTaskData, err)
	}
	if queue != "default" {
		return nil, fmt.Errorf("%w: missing or invalid queue", ErrTaskData)
	}
	payload, hasPayload := fields["payload"]
	if !hasPayload || len(payload) > MaxPayloadSize {
		return nil, fmt.Errorf("%w: missing or oversized payload", ErrTaskData)
	}
	maxRetry, err := parseTaskUint(fields, "max_retry", 0, MaxSubmitRetries)
	if err != nil {
		return nil, err
	}
	attempts, err := parseTaskUint(fields, "attempts", 0, maxRetry+1)
	if err != nil {
		return nil, err
	}
	// 提交时长上限远低于 time.Duration 溢出边界，先校验毫秒再转换。
	timeoutMS, err := parseTaskUint(fields, "execution_timeout_ms", 1, uint64(MaxSubmitExecutionTimeout.Milliseconds()))
	if err != nil {
		return nil, err
	}
	delayMS, err := parseTaskUint(fields, "retry_delay_ms", uint64(MinSubmitRetryDelay.Milliseconds()), uint64(MaxSubmitRetryDelay.Milliseconds()))
	if err != nil {
		return nil, err
	}
	createdAt, err := parseTaskTime("created_at", fields["created_at"])
	if err != nil {
		return nil, err
	}
	task := TaskInfo{
		TaskID: taskID, TaskType: taskType, Queue: queue,
		MaxRetry: int(maxRetry), ExecutionTimeout: time.Duration(timeoutMS) * time.Millisecond, RetryDelay: time.Duration(delayMS) * time.Millisecond,
		Status: fields["status"], Attempts: int(attempts), CreatedAt: createdAt,
	}
	for _, field := range []struct {
		key string
		at  **time.Time
	}{
		{"last_claimed_at", &task.LastClaimedAt},
		{"last_failed_at", &task.LastFailedAt},
		{"finished_at", &task.FinishedAt},
	} {
		if raw, exists := fields[field.key]; exists {
			at, err := parseTaskTime(field.key, raw)
			if err != nil {
				return nil, err
			}
			*field.at = &at
		}
	}
	if err := validateTaskState(fields, &task); err != nil {
		return nil, err
	}
	task.Payload = []byte(payload)
	if result, exists := fields["result"]; exists {
		task.Result = []byte(result)
	}
	task.LastError = fields["last_error"]
	return &task, nil
}

func validateTaskState(fields map[string]string, task *TaskInfo) error {
	status, attempts, limit := task.Status, task.Attempts, task.MaxRetry+1
	finished := false
	switch status {
	case "pending", "retry_wait":
		if attempts == limit {
			return fmt.Errorf("%w: attempt budget exhausted in %s", ErrTaskData, status)
		}
	case "running":
	case "completed", "failed", "canceled":
		finished = true
	default:
		return fmt.Errorf("%w: missing or invalid status", ErrTaskData)
	}
	if (attempts == 0 && status != "pending" && status != "canceled") || (status == "failed" && attempts != limit) {
		return fmt.Errorf("%w: attempts incompatible with %s", ErrTaskData, status)
	}
	if (task.LastClaimedAt != nil) != (attempts > 0) || (task.FinishedAt != nil) != finished {
		return fmt.Errorf("%w: missing or unexpected last_claimed_at or finished_at", ErrTaskData)
	}
	owner, hasOwner := fields["owner_id"]
	token, hasToken := fields["attempt_token"]
	if status == "running" && (owner == "" || token == "") || status != "running" && (hasOwner || hasToken) {
		return fmt.Errorf("%w: execution identity incompatible with %s", ErrTaskData, status)
	}
	result, hasResult := fields["result"]
	if hasResult != (status == "completed") || len(result) > MaxResultSize {
		return fmt.Errorf("%w: missing, unexpected or oversized result", ErrTaskData)
	}
	lastError, hasError := fields["last_error"]
	if hasError != (task.LastFailedAt != nil) || (hasError && (lastError == "" || len(lastError) > MaxErrorMessageSize)) {
		return fmt.Errorf("%w: invalid last_error or last_failed_at", ErrTaskData)
	}
	// 重领、重新入队和最终失败都需要已记录的失败。首次执行可以在失败前取消，
	// 也可以消耗本轮后在等待下一轮时取消，取决于是否还有剩余尝试额度。
	needsFailure := attempts > 1 || status == "failed" || status == "retry_wait" || (status == "pending" && attempts > 0)
	canHaveFailure := needsFailure || (status == "canceled" && attempts == 1 && attempts < limit)
	if hasError && !canHaveFailure || !hasError && needsFailure {
		return fmt.Errorf("%w: failure history incompatible with %s and attempts", ErrTaskData, status)
	}
	// ReportFailure 和 Reclaim 进入 failed 时使用同一次 Redis 取时写入两个字段。
	if status == "failed" && !task.LastFailedAt.Equal(*task.FinishedAt) {
		return fmt.Errorf("%w: last_failed_at differs from finished_at in failed", ErrTaskData)
	}
	return nil
}

func parseTaskUint(fields map[string]string, field string, min, max uint64) (uint64, error) {
	value, err := strconv.ParseUint(fields[field], 10, 64)
	if err != nil || value < min || value > max {
		return 0, fmt.Errorf("%w: %s must be an integer in %d..%d", ErrTaskData, field, min, max)
	}
	return value, nil
}

func parseTaskTime(field, value string) (time.Time, error) {
	ms, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		return time.Time{}, fmt.Errorf("%w: %s must be a Unix millisecond timestamp", ErrTaskData, field)
	}
	return time.UnixMilli(ms).UTC(), nil
}
