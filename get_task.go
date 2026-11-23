package taskq

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/wang-kang-tuoinai/TaskHarbor/internal/redisstore"
)

// ErrTaskNotFound 表示查询时任务详情不存在。
var ErrTaskNotFound = errors.New("task not found")

// TaskInfo 是 GetTask 读取的任务详情快照。时间均为 UTC，未发生的事件时间为 nil。
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
	// Result 保留原始字节。nil 表示字段缺失，空切片表示成功但未设置结果，JSON null 保留为四个字节。
	Result []byte
	// LastError 记录最近一次失败，任务随后成功时仍会保留。
	LastError     string
	CreatedAt     time.Time
	LastClaimedAt *time.Time
	LastFailedAt  *time.Time
	FinishedAt    *time.Time
}

// GetTask 按全局任务 ID 查询 Redis 中的任务详情，保留 ID 的原始字节。
// ErrTaskNotFound 表示成功读取时任务不存在。running 表示已保存的状态，租约到期独立处理。
// 数据不一致返回 ErrStorageData，请求失败保留底层原因。所有错误都返回 nil 详情。
// 客户端重试成功时，以该次成功读取为准。调用方负责 Redis 客户端配置。
func (c *Client) GetTask(ctx context.Context, taskID string) (*TaskInfo, error) {
	if taskID == "" {
		return nil, fmt.Errorf("%w: task ID must not be empty", ErrInvalidArgument)
	}
	task, err := redisstore.GetTask(ctx, c.rdb, taskID)
	if err != nil {
		switch {
		case errors.Is(err, redisstore.ErrTaskNotFound):
			return nil, fmt.Errorf("%w: %q", ErrTaskNotFound, taskID)
		case errors.Is(err, redisstore.ErrTaskData):
			return nil, fmt.Errorf("%w: get task %q: %w", ErrStorageData, taskID, err)
		default:
			return nil, fmt.Errorf("get task %q: %w", taskID, err)
		}
	}
	return &TaskInfo{
		TaskID: task.TaskID, TaskType: task.TaskType, Queue: task.Queue, Payload: task.Payload,
		MaxRetry: task.MaxRetry, ExecutionTimeout: task.ExecutionTimeout, RetryDelay: task.RetryDelay,
		Status: task.Status, Attempts: task.Attempts, Result: task.Result, LastError: task.LastError,
		CreatedAt: task.CreatedAt, LastClaimedAt: task.LastClaimedAt, LastFailedAt: task.LastFailedAt, FinishedAt: task.FinishedAt,
	}, nil
}
