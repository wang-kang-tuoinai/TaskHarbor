package redisstore

import (
	"context"
	"errors"
	"fmt"

	"github.com/redis/go-redis/v9"
)

const reclaimBatchLimit = 100

var (
	// ErrReclaimInconsistent 表示当前批次写前校验失败，脚本未写入。
	ErrReclaimInconsistent = errors.New("inconsistent reclaim batch")
	// ErrReclaimResponse 表示响应非法，本次迁移结果未知。
	ErrReclaimResponse = errors.New("invalid reclaim response")
)

// ReclaimLeases 原子回收 default 队列的一批到期租约；nil 也可能表示空批次。
// 调用方禁用透明重试；仅明确 NOSCRIPT 允许脚本缓存回退。
func ReclaimLeases(ctx context.Context, rdb *redis.Client) error {
	return runReclaim(ctx, rdb, reclaimLeasesScript, "reclaim leases",
		[]string{"taskq:default:lease", "taskq:default:active", "taskq:default:retry"})
}

// ScheduleRetries 原子调度 default 队列的一批到期重试；nil 也可能表示空批次。
// 调用方禁用透明重试；仅明确 NOSCRIPT 允许脚本缓存回退。
func ScheduleRetries(ctx context.Context, rdb *redis.Client) error {
	return runReclaim(ctx, rdb, scheduleRetriesScript, "schedule retries",
		[]string{"taskq:default:retry", "taskq:default:active", "taskq:default:lease", "taskq:default:pending"})
}

func runReclaim(ctx context.Context, rdb *redis.Client, script *redis.Script, phase string, keys []string) error {
	if rdb == nil {
		return fmt.Errorf("%s: Redis client is required", phase)
	}
	response, err := script.Run(ctx, rdb, keys, reclaimBatchLimit).Result()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			err = fmt.Errorf("%w: %w", ErrReclaimResponse, err)
		} else if redis.HasErrorPrefix(err, "TASKQ_RECLAIM_INCONSISTENT") {
			err = fmt.Errorf("%w: %w", ErrReclaimInconsistent, err)
		}
		return fmt.Errorf("%s: %w", phase, err)
	}
	count, ok := response.(int64)
	if !ok || count < 0 || count > reclaimBatchLimit {
		return fmt.Errorf("%s: %w: expected integer 0..%d", phase, ErrReclaimResponse, reclaimBatchLimit)
	}
	return nil
}
