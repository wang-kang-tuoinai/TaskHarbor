package taskq

import (
	"context"
	"errors"
)

// ErrShutdownInProgress 表示首次退出流程尚未完成，后续调用不改变其期限。
var ErrShutdownInProgress = errors.New("server shutdown in progress")

func (s *server) Shutdown(ctx context.Context) error {
	s.mu.Lock()
	switch s.status {
	case serverCreated:
		s.forceStop()
		s.status = serverStopped
		close(s.shutdownDone)
		s.mu.Unlock()
		return nil
	case serverStopped:
		s.mu.Unlock()
		return nil
	case serverStopping:
		s.mu.Unlock()
		return ErrShutdownInProgress
	}
	s.status = serverStopping
	s.cancelClaim()
	// 运行态首次调用的预取消不能被空闲实例的快速排空掩盖。
	if err := ctx.Err(); err != nil {
		s.forceStop()
		go s.finishShutdown()
		s.mu.Unlock()
		return err
	}
	// 每实例只有首次调用创建协调流程；调用超时后它仍负责实际收尾。
	go s.finishShutdown()
	s.mu.Unlock()
	select {
	case <-s.shutdownDone:
		return nil
	case <-ctx.Done():
		s.mu.Lock()
		defer s.mu.Unlock()
		if s.status == serverStopped {
			return nil
		}
		s.forceStop()
		return ctx.Err()
	}
}

func (s *server) forceStop() {
	s.launchMu.Lock()
	select {
	case <-s.abort:
	default:
		close(s.abort)
	}
	s.launchMu.Unlock()
	// 父级取消覆盖已经移出续租表的超时上报，不能用扫描 executions 替代。
	s.cancelAllReports()
	s.cancelClaim()
	s.cancelRenew()
	s.cancelReclaim()
}

func (s *server) finishShutdown() {
	<-s.claimDone
	// Claim 不再接管监督后才能 Wait；监督在登记 Handler 时仍占有计数。
	s.executionWG.Wait()
	s.cancelRenew()
	s.cancelReclaim()
	<-s.renewDone
	<-s.reclaimDone
	s.cancelAllReports()
	s.mu.Lock()
	s.status = serverStopped
	close(s.shutdownDone)
	s.mu.Unlock()
}
