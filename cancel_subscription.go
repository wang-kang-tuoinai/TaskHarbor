package taskq

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"
	"unicode/utf8"

	"github.com/redis/go-redis/v9"
)

const cancelSubscriptionTimeout = 3 * time.Second

func (s *server) subscribeToCancellation(ctx context.Context) (*redis.PubSub, error) {
	channel := "cancel:" + s.ownerID
	// 禁用 I/O 截止时间的共享客户端也必须约束初始化及 SUBSCRIBE 写入。
	client := boundedRedisClient(s.redisClient, cancelSubscriptionTimeout)
	sub := client.Subscribe(ctx)
	err := sub.Subscribe(ctx, channel)
	if err == nil {
		var reply any
		reply, err = sub.ReceiveTimeout(ctx, client.Options().ReadTimeout)
		if err == nil {
			ack, ok := reply.(*redis.Subscription)
			if !ok || ack.Kind != "subscribe" || ack.Channel != channel || ack.Count != 1 {
				err = fmt.Errorf("unexpected confirmation %T", reply)
			} else {
				err = ctx.Err()
			}
		}
	}
	if err != nil {
		_ = sub.Close()
		return nil, fmt.Errorf("subscribe to task cancellation: %w", err)
	}
	return sub, nil
}

func (s *server) consumeCancellation() {
	sub := s.subscription // 先于 goroutine 启动写入，仅在消费协程退出后清除。
	defer close(s.subscriptionDone)
	closed := make(chan struct{})
	stopClose := context.AfterFunc(s.subscriptionCtx, func() {
		_ = sub.Close()
		close(closed)
	})

	for message := range sub.Channel() {
		var notice struct {
			TaskID       string `json:"task_id"`
			AttemptToken string `json:"attempt_token"`
		}
		if !utf8.ValidString(message.Payload) || json.Unmarshal([]byte(message.Payload), &notice) != nil ||
			notice.TaskID == "" || notice.AttemptToken == "" {
			slog.Warn("invalid task cancellation notification", "channel", message.Channel)
			continue
		}
		key := executionKey{taskID: notice.TaskID, attemptToken: notice.AttemptToken}
		if record, ok := s.getExecution(key); ok {
			s.revokeExecution(key, record, errExecutionCanceled)
		}
	}
	if s.subscriptionCtx.Err() == nil {
		s.reportFatal(errors.New("task cancellation subscription closed unexpectedly"))
	}
	if stopClose() {
		_ = sub.Close()
	} else {
		<-closed
	}
}
