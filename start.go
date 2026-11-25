package taskq

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
)

func (s *server) Start() error {
	s.mu.Lock()
	if err := s.beginStartLocked(); err != nil {
		s.mu.Unlock()
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), cancelSubscriptionTimeout)
	s.cancelStart = cancel
	s.mu.Unlock()
	// Redis 的订阅确认必须在锁外等待，Shutdown 在此期间仍可撤销启动许可。
	sub, err := s.subscribeToCancellation(ctx)
	cancel()
	s.mu.Lock()
	s.cancelStart = nil
	if s.status != serverStarting {
		s.mu.Unlock()
		if sub != nil {
			_ = sub.Close()
		}
		close(s.startDone)
		return errors.New("server shutdown during startup")
	}
	close(s.startDone)
	if err != nil {
		s.forceStop()
		s.markStoppedLocked()
		s.mu.Unlock()
		return err
	}
	s.status = serverRunning
	s.subscription = sub
	go s.consumeCancellation()
	go s.claimLoop()
	go s.renewLoop()
	go s.reclaimLoop()
	s.mu.Unlock()
	return nil
}

// 调用方持有 mu，只有封存注册并生成身份后才能等待网络。
func (s *server) beginStartLocked() error {
	if s.status != serverCreated {
		return errors.New("server has already started")
	}
	if len(s.handlers) == 0 {
		return errors.New("at least one handler is required")
	}
	var id [16]byte
	if _, err := rand.Read(id[:]); err != nil {
		return fmt.Errorf("generate owner ID: %w", err)
	}
	s.ownerID = fmt.Sprintf("%x", id)
	s.status = serverStarting
	return nil
}
