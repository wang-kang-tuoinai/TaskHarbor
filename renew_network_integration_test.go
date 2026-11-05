//go:build integration

package taskq

import (
	"bufio"
	"context"
	"errors"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/wang-kang-tuoinai/TaskHarbor/internal/redisstore"
)

type renewDeadlineConn struct {
	net.Conn
	armed     *atomic.Bool
	deadlines chan time.Time
}

func (c *renewDeadlineConn) SetReadDeadline(deadline time.Time) error {
	if c.armed.Load() {
		select {
		case c.deadlines <- deadline:
		default:
		}
	}
	return c.Conn.SetReadDeadline(deadline)
}

func readRenewTCPCommand(reader *bufio.Reader) ([]string, error) {
	line, err := reader.ReadString('\n')
	if err != nil {
		return nil, err
	}
	count, err := strconv.Atoi(strings.TrimSpace(line[1:]))
	if err != nil {
		return nil, err
	}
	parts := make([]string, 0, count)
	for range count {
		line, err = reader.ReadString('\n')
		if err != nil {
			return nil, err
		}
		n, err := strconv.Atoi(strings.TrimSpace(line[1:]))
		if err != nil {
			return nil, err
		}
		data := make([]byte, n+2)
		if _, err := io.ReadFull(reader, data); err != nil {
			return nil, err
		}
		parts = append(parts, string(data[:n]))
	}
	return parts, nil
}

func TestRenewTCPDeadlineOptionsAndSharedPool(t *testing.T) {
	for _, tc := range []struct {
		name                string
		read, write, budget time.Duration
	}{{"deadline", 2 * time.Second, 2 * time.Second, 80 * time.Millisecond}, {"shorter-io", 100 * time.Millisecond, 2 * time.Second, 400 * time.Millisecond}, {"disabled-io", -2, -2, 80 * time.Millisecond}} {
		t.Run(tc.name, func(t *testing.T) {
			listener, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			var accepted atomic.Int32
			var mu sync.Mutex
			var connections []net.Conn
			var workers sync.WaitGroup
			acceptDone := make(chan struct{})
			go func() {
				defer close(acceptDone)
				for {
					conn, err := listener.Accept()
					if err != nil {
						return
					}
					accepted.Add(1)
					mu.Lock()
					connections = append(connections, conn)
					mu.Unlock()
					workers.Go(func() {
						defer conn.Close()
						reader := bufio.NewReader(conn)
						for {
							parts, err := readRenewTCPCommand(reader)
							if err != nil {
								return
							}
							switch strings.ToLower(parts[0]) {
							case "hello":
								_, _ = io.WriteString(conn, "-ERR unknown command 'hello'\r\n")
							case "evalsha", "eval": // 真正不回 TCP 响应，等待 socket deadline，而不是模拟超时错误。
							default:
								_, _ = io.WriteString(conn, "+PONG\r\n")
							}
						}
					})
				}
			}()
			t.Cleanup(func() {
				_ = listener.Close()
				<-acceptDone
				mu.Lock()
				for _, conn := range connections {
					_ = conn.Close()
				}
				mu.Unlock()
				workers.Wait()
			})
			var armed atomic.Bool
			deadlines := make(chan time.Time, 8)
			rdb := redis.NewClient(&redis.Options{Addr: listener.Addr().String(), MaxRetries: -1, ContextTimeoutEnabled: true, ReadTimeout: tc.read, WriteTimeout: tc.write, PoolSize: 1, Protocol: 2, DisableIdentity: true, Dialer: func(ctx context.Context, network, addr string) (net.Conn, error) {
				conn, err := (&net.Dialer{}).DialContext(ctx, network, addr)
				if err != nil {
					return nil, err
				}
				return &renewDeadlineConn{Conn: conn, armed: &armed, deadlines: deadlines}, nil
			}})
			t.Cleanup(func() { _ = rdb.Close() })
			original := *rdb.Options()
			unchanged := func() bool {
				got := rdb.Options()
				return got.Addr == original.Addr && got.MaxRetries == original.MaxRetries && got.ContextTimeoutEnabled == original.ContextTimeoutEnabled &&
					got.ReadTimeout == original.ReadTimeout && got.WriteTimeout == original.WriteTimeout && got.DialTimeout == original.DialTimeout &&
					got.PoolSize == original.PoolSize && got.PoolTimeout == original.PoolTimeout
			}
			public, err := NewServer(rdb)
			if err != nil {
				t.Fatal(err)
			}
			s := public.(*server)
			t.Cleanup(func() { s.cancelClaim(); s.cancelRenew() })
			if !unchanged() {
				t.Fatal("WithTimeout changed caller configuration")
			}
			if err := rdb.Ping(context.Background()).Err(); err != nil {
				t.Fatal(err)
			}
			if err := s.renewClient.Ping(context.Background()).Err(); err != nil {
				t.Fatal(err)
			}
			if accepted.Load() != 1 || rdb.PoolStats().TotalConns != 1 || s.renewClient.PoolStats().TotalConns != 1 {
				t.Fatal("renew wrapper created another pool or connection")
			}
			if tc.read > 0 && s.renewClient.Options().ReadTimeout > tc.read {
				t.Fatal("wrapper lengthened shorter I/O timeout")
			}
			ctx, cancel := context.WithTimeout(context.Background(), tc.budget)
			defer cancel()
			deadline, _ := ctx.Deadline()
			armed.Store(true)
			began := time.Now()
			applied, err := redisstore.Renew(ctx, s.renewClient, redisstore.RenewParams{TaskID: "task", AttemptToken: "token", LeaseDurationMS: 30000})
			elapsed := time.Since(began)
			var networkErr net.Error
			if applied || !errors.As(err, &networkErr) || !networkErr.Timeout() {
				t.Fatalf("applied=%v err=%v", applied, err)
			}
			select {
			case actual := <-deadlines:
				if actual.IsZero() || actual.After(deadline) {
					t.Fatalf("network deadline=%v request deadline=%v", actual, deadline)
				}
			default:
				t.Fatal("request did not install a real socket deadline")
			}
			// 校验截止时间和宽松的停滞上限；不承诺墙钟上严格按期限返回。
			if elapsed > time.Second {
				t.Fatalf("request stalled beyond scheduling tolerance: %v", elapsed)
			}
			armed.Store(false)
			if !unchanged() {
				t.Fatal("request changed caller options")
			}
			if err := rdb.Ping(context.Background()).Err(); err != nil {
				t.Fatalf("caller client closed: %v", err)
			}
			t.Logf("budget=%v elapsed=%v shared connections before timeout=1", tc.budget, elapsed)
		})
	}
}
