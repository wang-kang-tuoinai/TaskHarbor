package redisstore

import (
	"context"
	"errors"
	"fmt"
	"strconv"

	"github.com/redis/go-redis/v9"
)

var (
	// ErrClaimArgument 表示 Claim 请求参数非法，未发送存储命令。
	ErrClaimArgument = errors.New("invalid claim argument")
	// ErrClaimInconsistent 表示候选数据不一致；异常队首保留，需人工处理。
	ErrClaimInconsistent = errors.New("inconsistent claim candidate")
	// ErrClaimResponse 表示响应不符合协议；执行权可能已授予，不能回滚或重放。
	ErrClaimResponse = errors.New("invalid claim response")
)

const maxExactInteger int64 = 1<<53 - 1 // Redis Lua 数值和 ZSET score 的精确整数上限。

// ClaimParams 是一次独立领取的参数，时长单位为整数毫秒。
// 调用方为每次调用提供唯一 AttemptToken；它不是请求去重 ID。
type ClaimParams struct {
	OwnerID         string
	AttemptToken    string
	LeaseDurationMS int64
}

// ClaimResult 包含业务内容及本次调用持有的执行身份。
type ClaimResult struct {
	TaskID             string
	TaskType           string
	Payload            []byte
	Queue              string
	Attempts           int
	ExecutionTimeoutMS int64
	OwnerID            string
	AttemptToken       string
}

// Claim 从 default 原子领取一次；空队列返回 nil, nil。
// rdb 必须由调用方禁用自动重试（构造时 MaxRetries=-1）。这里不修改共享配置，
// 也不重放结果未知的请求；只有明确 NOSCRIPT 才由 Script.Run 回退到 EVAL。
// 存储和网络错误保留底层错误链，供领取循环分类。
func Claim(ctx context.Context, rdb *redis.Client, p ClaimParams) (*ClaimResult, error) {
	if rdb == nil || p.OwnerID == "" || p.AttemptToken == "" || p.LeaseDurationMS <= 0 || p.LeaseDurationMS > maxExactInteger {
		return nil, fmt.Errorf("%w: client, owner, token and positive exact lease milliseconds required", ErrClaimArgument)
	}
	response, err := claimScript.Run(ctx, rdb,
		[]string{"taskq:default:pending", "taskq:default:active", "taskq:default:lease"},
		p.OwnerID, p.AttemptToken, p.LeaseDurationMS,
	).Result()
	if err != nil {
		if redis.HasErrorPrefix(err, "TASKQ_CLAIM_INCONSISTENT") {
			return nil, fmt.Errorf("%w: %w", ErrClaimInconsistent, err)
		}
		return nil, fmt.Errorf("run claim script: %w", err)
	}
	return parseClaimResponse(response, p)
}

func parseClaimResponse(response any, p ClaimParams) (*ClaimResult, error) {
	values, ok := response.([]any)
	if !ok {
		return nil, fmt.Errorf("%w: expected array", ErrClaimResponse)
	}
	if len(values) == 0 {
		return nil, nil
	}
	if len(values) != 6 {
		return nil, fmt.Errorf("%w: expected six business fields", ErrClaimResponse)
	}
	var fields [6]string
	for i, value := range values {
		var ok bool
		fields[i], ok = value.(string)
		if !ok {
			return nil, fmt.Errorf("%w: field %d must be a string", ErrClaimResponse, i)
		}
	}
	attempts, attemptsErr := strconv.ParseUint(fields[4], 10, 31)
	timeout, timeoutErr := strconv.ParseUint(fields[5], 10, 64)
	// 响应是新的信任边界：拒绝无法安全交给执行端的内容，不根据请求猜测任务。
	if fields[0] == "" || fields[1] == "" || fields[3] != "default" ||
		attemptsErr != nil || attempts == 0 || timeoutErr != nil || timeout == 0 || timeout > 86400000 {
		return nil, fmt.Errorf("%w: invalid task, queue, attempts or execution timeout", ErrClaimResponse)
	}
	return &ClaimResult{
		TaskID: fields[0], TaskType: fields[1], Payload: []byte(fields[2]), Queue: fields[3],
		Attempts: int(attempts), ExecutionTimeoutMS: int64(timeout),
		OwnerID: p.OwnerID, AttemptToken: p.AttemptToken,
	}, nil
}
