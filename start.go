package taskq

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"net"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/wang-kang-tuoinai/TaskHarbor/internal/redisstore"
)

const (
	claimPollInterval   = 100 * time.Millisecond
	claimBackoffInitial = 100 * time.Millisecond
	claimBackoffMax     = 2 * time.Second
)

func (s *server) Start() error {
	if err := s.beginStart(); err != nil {
		return err
	}
	s.mu.Lock()
	s.status = serverRunning
	s.mu.Unlock()
	go s.claimLoop()
	go s.renewLoop()
	return nil
}

func (s *server) Errors() <-chan error { return s.errors }

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
		claimed, err := redisstore.Claim(s.claimCtx, s.redisClient, redisstore.ClaimParams{
			OwnerID: s.ownerID, AttemptToken: token, LeaseDurationMS: s.leaseDuration.Milliseconds(),
		})
		if err != nil {
			<-s.slots
			if s.claimCtx.Err() != nil {
				return
			}
			if transientRedisError(err) {
				if !waitClaim(s.claimCtx, backoff) {
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
			if !waitClaim(s.claimCtx, claimPollInterval) {
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

func (s *server) reportFatal(err error) {
	// 故障处理不能放入 Once：后续 Renew 故障即使不通知，也仍须停用对应循环。
	s.cancelClaim()
	s.fatalOnce.Do(func() { s.errors <- err })
}

func transientRedisError(err error) bool {
	var redisErr redis.Error
	if errors.As(err, &redisErr) {
		return false
	}
	var networkErr net.Error
	return errors.As(err, &networkErr) || errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) ||
		errors.Is(err, net.ErrClosed) || errors.Is(err, context.DeadlineExceeded) || errors.Is(err, redis.ErrPoolTimeout)
}

func waitClaim(ctx context.Context, delay time.Duration) bool {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

func newAttemptToken() (string, error) {
	var token [16]byte
	if _, err := rand.Read(token[:]); err != nil {
		return "", err
	}
	return fmt.Sprintf("%x", token), nil
}
