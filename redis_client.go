package taskq

import (
	"context"
	"errors"
	"io"
	"net"
	"time"

	"github.com/redis/go-redis/v9"
)

// WithTimeout 复制配置且共享原池，取最短正 I/O 预算以保留更短超时，
// 并使调用方禁用 I/O 超时时仍会设置 socket deadline。包装不得 Close。
func boundedRedisClient(rdb *redis.Client, budget time.Duration) *redis.Client {
	for _, limit := range []time.Duration{rdb.Options().ReadTimeout, rdb.Options().WriteTimeout} {
		if limit > 0 && limit < budget {
			budget = limit
		}
	}
	return rdb.WithTimeout(budget)
}

func transientRedisError(err error) bool {
	var redisErr redis.Error
	if errors.As(err, &redisErr) {
		return false
	}
	// 平台不重试不存在的域名、非暂时性解析错误和非法地址。
	var dns *net.DNSError
	if errors.As(err, &dns) && (dns.IsNotFound || !dns.IsTimeout && !dns.IsTemporary) {
		return false
	}
	var address *net.AddrError
	var invalid net.InvalidAddrError
	if errors.As(err, &address) || errors.As(err, &invalid) {
		return false
	}
	var networkErr net.Error
	return errors.As(err, &networkErr) || errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) ||
		errors.Is(err, net.ErrClosed) || errors.Is(err, context.DeadlineExceeded) || errors.Is(err, redis.ErrPoolTimeout)
}
