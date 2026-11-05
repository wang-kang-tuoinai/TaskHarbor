package redisstore

import (
	"context"
	"errors"
	"fmt"

	"github.com/redis/go-redis/v9"
)

var (
	// ErrRenewArgument 表示续租输入非法，未发送存储命令。
	ErrRenewArgument = errors.New("invalid renew argument")
	// ErrRenewInconsistent 表示续租所需数据不一致；不自动修复。
	ErrRenewInconsistent = errors.New("inconsistent renew task")
	// ErrRenewResponse 表示响应非法；租约可能已更新，不能回滚或重放。
	ErrRenewResponse = errors.New("invalid renew response")
)

// RenewParams 是单次续租输入，时长单位为整数毫秒。
type RenewParams struct {
	TaskID          string
	AttemptToken    string
	LeaseDurationMS int64
}

// Renew 原子检查当前执行权并设置租约。仅 err=nil 时 applied 才有确定含义。
// 客户端由调用方禁用自动重试；仅明确 NOSCRIPT 时允许脚本缓存回退。
func Renew(ctx context.Context, rdb *redis.Client, p RenewParams) (applied bool, err error) {
	if rdb == nil || p.TaskID == "" || p.AttemptToken == "" || p.LeaseDurationMS <= 0 || p.LeaseDurationMS > maxExactInteger {
		return false, fmt.Errorf("%w: client, task, token and positive exact lease milliseconds required", ErrRenewArgument)
	}
	response, err := renewScript.Run(ctx, rdb, []string{"taskq:meta:" + p.TaskID, "taskq:default:active", "taskq:default:lease"}, p.TaskID, p.AttemptToken, p.LeaseDurationMS).Result()
	if err != nil {
		if redis.HasErrorPrefix(err, "TASKQ_RENEW_INCONSISTENT") {
			err = fmt.Errorf("%w: %w", ErrRenewInconsistent, err)
		}
		return false, fmt.Errorf("renew task %q token %q: %w", p.TaskID, p.AttemptToken, err)
	}
	value, ok := response.(int64)
	if !ok || value != 0 && value != 1 {
		return false, fmt.Errorf("%w: task %q token %q: expected integer 0 or 1", ErrRenewResponse, p.TaskID, p.AttemptToken)
	}
	return value == 1, nil
}
