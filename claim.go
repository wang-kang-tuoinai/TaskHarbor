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
	for {
		select {
		case <-s.claimCtx.Done():
			return
		case s.slots <- struct{}{}:
		}
		if s.claimCtx.Err() != nil {
			<-s.slots
			return
		}
		token, err := s.newAttemptToken()
		if err != nil {
			<-s.slots
			s.reportFatal(fmt.Errorf("generate claim attempt token: %w", err))
			return
		}
		// 先取得槽并生成身份；连接等待、读写和 NOSCRIPT 回退共用这次期限。
		request, cancel := context.WithTimeout(s.claimCtx, claimRequestTimeout)
		claimed, err := redisstore.Claim(request, s.claimClient, redisstore.ClaimParams{
			OwnerID: s.ownerID, AttemptToken: token, LeaseDurationMS: s.leaseDuration.Milliseconds(),
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
				continue
			}
			s.reportFatal(err)
			return
		}
		backoff = claimBackoffInitial
		if claimed == nil {
			<-s.slots
			if !waitContext(s.claimCtx, claimPollInterval) {
				return
			}
			continue
		}
		// 响应已限制执行时长在 24 小时内，转换为 time.Duration 不会溢出。
		// acceptExecution 接管槽，包括它自身拒绝登记的情况，循环不能再次归还。
		_, err = s.acceptExecution(context.Background(), &Task{
			ID: claimed.TaskID, Type: claimed.TaskType, Payload: claimed.Payload, Attempts: claimed.Attempts,
		}, executionKey{taskID: claimed.TaskID, attemptToken: token}, time.Duration(claimed.ExecutionTimeoutMS)*time.Millisecond)
		if err != nil {
			s.reportFatal(fmt.Errorf("accept claimed task %q: %w", claimed.TaskID, err))
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
