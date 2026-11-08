package taskq

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/wang-kang-tuoinai/TaskHarbor/internal/redisstore"
)

const (
	reportRequestTimeout  = 3 * time.Second
	reportMaxAttempts     = 3
	reportRetryDelay      = 100 * time.Millisecond
	maxErrorMessageSize   = 4096
	errorTruncationMarker = "...[truncated]"
)

func (s *server) complete(ctx context.Context, record *execution, key executionKey, result []byte) {
	p := redisstore.CompleteParams{TaskID: key.taskID, AttemptToken: key.attemptToken, Result: result}
	s.report(ctx, record, key, "complete", func(request context.Context) (bool, error) { return redisstore.Complete(request, s.reportClient, p) })
}

func (s *server) reportFailure(ctx context.Context, record *execution, key executionKey, cause error) {
	// 首次请求前只求值一次，网络重试不重新读取可能变化的业务错误。
	message := strings.ToValidUTF8(cause.Error(), "�")
	if message == "" {
		message = "Handler 返回了空错误信息"
	}
	if len(message) > maxErrorMessageSize {
		end := maxErrorMessageSize - len(errorTruncationMarker)
		for !utf8.RuneStart(message[end]) {
			end--
		}
		message = message[:end] + errorTruncationMarker
	}
	p := redisstore.FailureParams{TaskID: key.taskID, AttemptToken: key.attemptToken, ErrorMessage: message}
	s.report(ctx, record, key, "ReportFailure", func(request context.Context) (bool, error) {
		return redisstore.ReportFailure(request, s.reportClient, p)
	})
}

func (s *server) report(ctx context.Context, record *execution, key executionKey, operation string, call func(context.Context) (bool, error)) {
	// 任务 context 只传播已确认禁止上报的原因，本地执行超时不能取消失败上报。
	stop := context.AfterFunc(ctx, func() {
		if reportingForbidden(ctx) {
			record.cancelReports()
		}
	})
	defer stop()
	unknown := false
	for attempt := 1; attempt <= reportMaxAttempts; attempt++ {
		if s.reportingStopped(ctx, record) {
			return
		}
		request, cancel := context.WithTimeout(record.reportCtx, reportRequestTimeout)
		applied, err := call(request)
		cancel()
		if err == nil {
			if !applied {
				if unknown {
					slog.Warn("task report outcome unconfirmed after rejection", "operation", operation, "task_id", key.taskID, "attempts", attempt)
				} else {
					slog.Debug("task report rejected", "operation", operation, "task_id", key.taskID, "attempts", attempt)
				}
			}
			return
		}
		if errors.Is(err, context.Canceled) && s.reportingStopped(ctx, record) {
			return
		}
		if !retryableReportError(err) {
			slog.Error("task report failed", "operation", operation, "task_id", key.taskID, "attempts", attempt, "error", err)
			s.reportFatal(err)
			return
		}
		if s.reportingStopped(ctx, record) {
			return
		}
		unknown = true
		if attempt == reportMaxAttempts {
			slog.Warn("task report outcome unconfirmed after retry exhaustion", "operation", operation, "task_id", key.taskID, "attempts", attempt, "error", err)
			return
		}
		timer := time.NewTimer(time.Duration(attempt) * reportRetryDelay)
		select {
		case <-record.reportCtx.Done():
			timer.Stop()
			return
		case <-s.abort:
			timer.Stop()
			return
		case <-timer.C:
		}
	}
}

func retryableReportError(err error) bool {
	// 永久解析或地址配置错误在请求发出前就已明确；不能当作结果未知耗尽预算。
	var dns *net.DNSError
	if errors.As(err, &dns) && (dns.IsNotFound || !dns.IsTimeout && !dns.IsTemporary) {
		return false
	}
	var address *net.AddrError
	var invalid net.InvalidAddrError
	if errors.As(err, &address) || errors.As(err, &invalid) {
		return false
	}
	return transientRedisError(err)
}

func (s *server) reportingStopped(ctx context.Context, record *execution) bool {
	if record.reportForbidden.Load() || reportingForbidden(ctx) || record.reportCtx.Err() != nil {
		return true
	}
	select {
	case <-s.abort:
		return true
	default:
		return false
	}
}
