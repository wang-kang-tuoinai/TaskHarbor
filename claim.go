package taskq

import (
	"context"
	"crypto/rand"
	"fmt"
	"time"

	"github.com/wang-kang-tuoinai/TaskHarbor/internal/redisstore"
)

const (
	claimRequestTimeout = 3 * time.Second
	claimPollInterval   = 100 * time.Millisecond
	claimBackoffInitial = 100 * time.Millisecond
	claimBackoffMax     = 2 * time.Second
)

func (s *server) claimLoop() {
	defer close(s.claimDone)
	backoff := claimBackoffInitial
	nextQueue := 0
claiming:
	for {
		select {
		case <-s.claimCtx.Done():
			return
		case s.slots <- struct{}{}:
		}
		for range s.queues {
			// 持槽时只有明确的空响应允许继续探测，停领检查也覆盖空响应后的下一次请求。
			if s.claimCtx.Err() != nil {
				<-s.slots
				return
			}
			queue := s.queues[nextQueue]
			nextQueue = (nextQueue + 1) % len(s.queues)
			token, err := s.newAttemptToken()
			if err != nil {
				<-s.slots
				s.reportFatal(fmt.Errorf("generate claim attempt token for queue %q: %w", queue, err))
				return
			}
			// 每次探测独立计时；连接等待、读写和 NOSCRIPT 回退共用这次期限。
			request, cancel := context.WithTimeout(s.claimCtx, claimRequestTimeout)
			claimed, err := redisstore.Claim(request, s.claimClient, redisstore.ClaimParams{
				Queue: queue, OwnerID: s.ownerID, AttemptToken: token, LeaseDurationMS: s.leaseDuration.Milliseconds(),
			})
			cancel()
			if err != nil {
				<-s.slots
				if s.claimCtx.Err() != nil {
					return
				}
				if transientRedisError(err) {
					if !waitContext(s.claimCtx, backoff) {
						return
					}
					backoff = min(backoff*2, claimBackoffMax)
					continue claiming
				}
				s.reportFatal(fmt.Errorf("claim queue %q: %w", queue, err))
				return
			}
			backoff = claimBackoffInitial
			if claimed == nil {
				continue
			}
			// 明确成功的在途请求仍然接管。acceptExecution 负责归还槽，包括登记失败时。
			_, err = s.acceptExecution(context.Background(), &Task{
				ID: claimed.TaskID, Type: claimed.TaskType, Payload: claimed.Payload, Attempts: claimed.Attempts,
			}, executionKey{taskID: claimed.TaskID, attemptToken: token}, claimed.Queue, time.Duration(claimed.ExecutionTimeoutMS)*time.Millisecond)
			if err != nil {
				s.reportFatal(fmt.Errorf("accept claimed task %q from queue %q: %w", claimed.TaskID, queue, err))
				return
			}
			continue claiming
		}
		<-s.slots
		if !waitContext(s.claimCtx, claimPollInterval) {
			return
		}
	}
}

func waitContext(ctx context.Context, delay time.Duration) bool {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return ctx.Err() == nil
	}
}

func newAttemptToken() (string, error) {
	var token [16]byte
	if _, err := rand.Read(token[:]); err != nil {
		return "", err
	}
	return fmt.Sprintf("%x", token), nil
}
