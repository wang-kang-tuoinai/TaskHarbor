package taskq

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/wang-kang-tuoinai/TaskHarbor/internal/redisstore"
)

const (
	// MaxPayloadSize 是提交参数的最大字节数，不限制参数编码。
	MaxPayloadSize = 1 << 20
	// MaxTaskTypeSize 是 ASCII 任务类型的最大字节数。
	MaxTaskTypeSize = 128
	// MaxSubmitRetries 保证最大尝试次数在 32/64 位 Go int 和 Lua 中均可精确表示。
	MaxSubmitRetries = 2147483646
	// MaxSubmitExecutionTimeout 是单次执行时长的上限。
	MaxSubmitExecutionTimeout = 24 * time.Hour
	// MaxSubmitRetryDelay 是重试等待时长的上限。
	MaxSubmitRetryDelay = 7 * 24 * time.Hour
)

var (
	// ErrInvalidArgument 表示 Client 构造或提交参数非法。
	ErrInvalidArgument = errors.New("invalid argument")
	// ErrTaskIDConflict 表示同一 ID 已绑定不同的初始配置。
	ErrTaskIDConflict = errors.New("task ID content conflict")
	// ErrStorageData 表示已存配置缺失或存储响应不符合协议；不会自动修复。
	ErrStorageData = errors.New("invalid storage data")
)

// Client 提交任务，持有调用方的 Redis 客户端，可并发使用。
// 必须通过 NewClient 构造；调用方负责配置和关闭 Redis 客户端。
type Client struct {
	rdb *redis.Client
}

// NewClient 只保存 rdb，不发起请求或启动 goroutine。
func NewClient(rdb *redis.Client) (*Client, error) {
	if rdb == nil {
		return nil, fmt.Errorf("%w: rdb is required", ErrInvalidArgument)
	}
	return &Client{rdb: rdb}, nil
}

// SubmitResult 确认任务已被接受，不承诺响应时任务仍为 pending。
type SubmitResult struct {
	TaskID  string
	Created bool // false 表示已有任务且初始配置一致，包括底层重试后确认的情况。
}

type submitConfig struct {
	maxRetry         int
	executionTimeout time.Duration
	retryDelay       time.Duration
}

// SubmitOption 配置一次提交；按传入顺序应用，最后统一校验。
type SubmitOption func(*submitConfig)

func defaultSubmitConfig() *submitConfig {
	return &submitConfig{executionTimeout: 60 * time.Second, retryDelay: 30 * time.Second}
}

// WithSubmitMaxRetry 设置首次尝试之外允许的重试次数，默认 0。
func WithSubmitMaxRetry(n int) SubmitOption {
	return func(c *submitConfig) { c.maxRetry = n }
}

// WithSubmitExecutionTimeout 设置整数毫秒的单次执行时长，默认 60 秒。
func WithSubmitExecutionTimeout(d time.Duration) SubmitOption {
	return func(c *submitConfig) { c.executionTimeout = d }
}

// WithSubmitRetryDelay 设置整数毫秒的重试等待时长，默认 30 秒，最小 15 秒。
func WithSubmitRetryDelay(d time.Duration) SubmitOption {
	return func(c *submitConfig) { c.retryDelay = d }
}

// Submit 原子创建任务并入队，或确认同 ID、同初始配置的任务已存在。
// taskType 为 1～128 字节的 ASCII 字母、数字及 _-.:，大小写敏感。
// 网络或存储错误不证明任务未创建；安全重试必须保留 ID、原始 payload 和有效配置。
// 平台不增加重试循环，也不修改调用方的 Redis 重试配置。
func (c *Client) Submit(ctx context.Context, taskID, taskType string, payload []byte, opts ...SubmitOption) (*SubmitResult, error) {
	cfg := defaultSubmitConfig()
	for _, opt := range opts {
		if opt == nil {
			return nil, fmt.Errorf("%w: submit option must not be nil", ErrInvalidArgument)
		}
		opt(cfg)
	}
	if taskID == "" {
		return nil, fmt.Errorf("%w: task ID must not be empty", ErrInvalidArgument)
	}
	if len(taskType) == 0 || len(taskType) > MaxTaskTypeSize {
		return nil, fmt.Errorf("%w: task type must contain 1..%d ASCII bytes", ErrInvalidArgument, MaxTaskTypeSize)
	}
	for i := 0; i < len(taskType); i++ {
		b := taskType[i]
		if !(b >= 'a' && b <= 'z' || b >= 'A' && b <= 'Z' || b >= '0' && b <= '9' || b == '_' || b == '-' || b == '.' || b == ':') {
			return nil, fmt.Errorf("%w: invalid task type byte at %d", ErrInvalidArgument, i)
		}
	}
	if len(payload) > MaxPayloadSize {
		return nil, fmt.Errorf("%w: payload exceeds %d bytes", ErrInvalidArgument, MaxPayloadSize)
	}
	if cfg.maxRetry < 0 || cfg.maxRetry > MaxSubmitRetries {
		return nil, fmt.Errorf("%w: max retry must be in 0..%d", ErrInvalidArgument, MaxSubmitRetries)
	}
	if cfg.executionTimeout < time.Millisecond || cfg.executionTimeout > MaxSubmitExecutionTimeout || cfg.executionTimeout%time.Millisecond != 0 {
		return nil, fmt.Errorf("%w: execution timeout must be whole milliseconds in 1ms..%v", ErrInvalidArgument, MaxSubmitExecutionTimeout)
	}
	if cfg.retryDelay < 15*time.Second || cfg.retryDelay > MaxSubmitRetryDelay || cfg.retryDelay%time.Millisecond != 0 {
		return nil, fmt.Errorf("%w: retry delay must be whole milliseconds in 15s..%v", ErrInvalidArgument, MaxSubmitRetryDelay)
	}
	result, err := redisstore.Submit(ctx, c.rdb, redisstore.SubmitParams{
		TaskID: taskID, TaskType: taskType, Payload: payload, MaxRetry: cfg.maxRetry,
		ExecutionTimeoutMS: cfg.executionTimeout.Milliseconds(), RetryDelayMS: cfg.retryDelay.Milliseconds(),
	})
	if err != nil {
		switch {
		case errors.Is(err, redisstore.ErrConflict):
			return nil, fmt.Errorf("%w: %q", ErrTaskIDConflict, taskID)
		case errors.Is(err, redisstore.ErrIncomplete), errors.Is(err, redisstore.ErrInvalidResponse):
			return nil, fmt.Errorf("%w: task %q: %w", ErrStorageData, taskID, err)
		default:
			return nil, fmt.Errorf("submit task %q: %w", taskID, err)
		}
	}
	return &SubmitResult{TaskID: result.TaskID, Created: result.Created}, nil
}
