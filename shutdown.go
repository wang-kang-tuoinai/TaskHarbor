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
		s.markStoppedLocked()
		s.mu.Unlock()
		return nil
	case serverStopped:
		s.mu.Unlock()
		return nil
	case serverStopping:
		s.mu.Unlock()
		return ErrShutdownInProgress
	}
	starting := s.status == serverStarting
	s.status = serverStopping
	s.cancelClaim()
	if starting {
		s.cancelStart()
	}
	// 运行态首次调用的预取消不能被空闲实例的快速排空掩盖。
	if err := ctx.Err(); err != nil {
		s.forceStop()
		go s.finishShutdown(starting)
		s.mu.Unlock()
		return err
	}
	// 每实例只有首次调用创建协调流程；调用超时后它仍负责实际收尾。
	go s.finishShutdown(starting)
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
	// PubSub.Close 可能等待库内重连持有的锁；强制请求仅发出关闭信号。
	s.closeSubscription()
}

func (s *server) finishShutdown(starting bool) {
	if starting {
		// 启动流程负责释放迟到的订阅资源；后台循环未启动，没有 done 的生产者。
		<-s.startDone
		s.forceStop()
	} else {
		<-s.claimDone
		// Claim 不再接管监督后才能 Wait；监督在登记 Handler 时仍占有计数。
		s.executionWG.Wait()
		s.cancelRenew()
		s.cancelReclaim()
		s.closeSubscription()
		<-s.renewDone
		<-s.reclaimDone
		<-s.subscriptionDone
		s.cancelAllReports()
	}
	s.mu.Lock()
	s.markStoppedLocked()
	s.mu.Unlock()
}

func (s *server) markStoppedLocked() {
	s.subscription = nil
	s.status = serverStopped
	close(s.shutdownDone)
}
