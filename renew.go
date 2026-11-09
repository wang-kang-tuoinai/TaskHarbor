package taskq

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/wang-kang-tuoinai/TaskHarbor/internal/redisstore"
)

func (s *server) renewLoop() {
	defer close(s.renewDone)
	ticker := time.NewTicker(s.renewInterval)
	defer ticker.Stop()
	for {
		select {
		case <-s.renewCtx.Done():
			return
		case <-ticker.C:
		}
		for key := range s.snapshotExecutions() {
			if s.renewCtx.Err() != nil {
				return
			}
			record, ok := s.getExecution(key)
			if !ok {
				continue
			}
			ctx, cancel := context.WithTimeout(s.renewCtx, s.renewInterval)
			applied, err := redisstore.Renew(ctx, s.renewClient, redisstore.RenewParams{
				TaskID: key.taskID, AttemptToken: key.attemptToken, LeaseDurationMS: s.leaseDuration.Milliseconds(),
			})
			cancel()
			if s.renewCtx.Err() != nil {
				return
			}
			if err != nil {
				if errors.Is(err, redisstore.ErrRenewInconsistent) {
					s.revokeExecution(key, record, fmt.Errorf("%w: %w", errExecutionInconsistent, err))
					s.reportFatal(err)
					continue
				}
				if transientRedisError(err) {
					continue
				}
				s.reportFatal(err)
				return // 不清表、不取消 Handler，迟到的明确 Claim 仍走正常接管。
			}
			if !applied {
				s.revokeExecution(key, record, errExecutionLost)
			}
		}
		// Ticker 本身丢弃多数错过的周期；排掉剩余一个，不在长扫描后补跑。
		select {
		case <-ticker.C:
		default:
		}
	}
}

func (s *server) revokeExecution(key executionKey, record *execution, cause error) {
	record.cancel(cause) // 取消回调必须在表锁外执行。
	s.deleteExecution(key)
}
