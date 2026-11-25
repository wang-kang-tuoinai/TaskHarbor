//go:build integration

package taskq

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/wang-kang-tuoinai/TaskHarbor/internal/redisstore"
)

type claimTCPRequest struct {
	connection int
	command    string
	token      string
	received   time.Time
}

func claimTCPCommand(connection int, args []string) claimTCPRequest {
	return claimTCPRequest{connection: connection, command: strings.ToLower(args[0]), token: args[7], received: time.Now()}
}

func isClaimTCPCommand(args []string) bool {
	return len(args) == 9 && args[3] == claimPending && args[4] == claimActive && args[5] == claimLease
}

func serveClaimTCP(t *testing.T, reply func(int, []string) string) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
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
			connections = append(connections, conn)
			connection := len(connections)
			workers.Go(func() {
				defer conn.Close()
				reader := bufio.NewReader(conn)
				for {
					args, err := readRedisTCPCommand(reader)
					if err != nil {
						return
					}
					response := reply(connection, args)
					if response != "" {
						if _, err := io.WriteString(conn, response); err != nil {
							return
						}
					}
				}
			})
		}
	}()
	t.Cleanup(func() {
		_ = listener.Close()
		<-acceptDone
		for _, conn := range connections {
			_ = conn.Close()
		}
		workers.Wait()
	})
	return listener.Addr().String()
}

func claimTCPDefaultReply(args []string) string {
	switch strings.ToLower(args[0]) {
	case "hello":
		return "-ERR unknown command 'hello'\r\n"
	case "subscribe":
		return cancelSubscribeAck(args[1])
	case "ping":
		return "+PONG\r\n"
	default:
		return ":1\r\n"
	}
}

func claimTCPOptions(addr string, read, write time.Duration) *redis.Options {
	return &redis.Options{
		Addr: addr, MaxRetries: -1, ContextTimeoutEnabled: true, Protocol: 2, DisableIdentity: true,
		PoolSize: 1, PoolTimeout: 8 * time.Second, ReadTimeout: read, WriteTimeout: write,
	}
}

func claimTCPClient(t *testing.T, addr string, read, write time.Duration) *redis.Client {
	t.Helper()
	rdb := redis.NewClient(claimTCPOptions(addr, read, write))
	t.Cleanup(func() { _ = rdb.Close() })
	return rdb
}

func startClaimServer(t *testing.T, rdb *redis.Client, handler HandlerFunc) Server {
	t.Helper()
	s, err := NewServer(rdb, WithConcurrency(1))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { shutdownClaimServer(t, s) })
	if err := s.HandleFunc("job", handler); err != nil {
		t.Fatal(err)
	}
	if err := s.Start(); err != nil {
		t.Fatal(err)
	}
	return s
}

func shutdownClaimServer(t *testing.T, s Server) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
	defer cancel()
	if err := s.Shutdown(ctx); err != nil {
		t.Error(err)
	}
}

func awaitClaimSignal[T any](t *testing.T, signal <-chan T, within time.Duration) T {
	t.Helper()
	select {
	case value := <-signal:
		return value
	case <-time.After(within):
		t.Fatalf("no Claim test signal within %v", within)
		var zero T
		return zero
	}
}

func TestClaimTCPRequestBudget(t *testing.T) {
	for _, tc := range []struct {
		name           string
		read, write    time.Duration
		minimum, limit time.Duration
	}{
		{"deadline", 8 * time.Second, 8 * time.Second, 2700 * time.Millisecond, 4 * time.Second},
		{"shorter-io", 8 * time.Second, 200 * time.Millisecond, 150 * time.Millisecond, time.Second},
		{"disabled-io", -2, -2, 2700 * time.Millisecond, 4 * time.Second},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var calls, pingConnection atomic.Int32
			requests := make(chan claimTCPRequest, 4)
			addr := serveClaimTCP(t, func(connection int, args []string) string {
				if isClaimTCPCommand(args) {
					requests <- claimTCPCommand(connection, args)
					if calls.Add(1) == 1 {
						return "" // 真实 TCP 读阻塞，由客户端截止时间结束。
					}
					return "-NOPERM test stops the next Claim\r\n"
				}
				if strings.EqualFold(args[0], "ping") {
					pingConnection.Store(int32(connection))
				}
				return claimTCPDefaultReply(args)
			})
			rdb := claimTCPClient(t, addr, tc.read, tc.write)
			original := *rdb.Options()
			if err := rdb.Ping(context.Background()).Err(); err != nil {
				t.Fatal(err)
			}
			prewarmed := int(pingConnection.Load())
			s := startClaimServer(t, rdb, unexpectedClaimHandler(t))
			first := awaitClaimSignal(t, requests, time.Second)
			if first.connection != prewarmed {
				t.Fatalf("Claim connection %d differs from caller's prewarmed connection %d", first.connection, prewarmed)
			}
			next := awaitClaimSignal(t, requests, tc.limit)
			if elapsed := next.received.Sub(first.received); elapsed < tc.minimum || elapsed > tc.limit {
				t.Errorf("first response timeout and independent Claim took %v, want %v..%v", elapsed, tc.minimum, tc.limit)
			}
			if first.token == "" || next.token == "" || first.token == next.token {
				t.Errorf("independent Claim tokens = %q, %q", first.token, next.token)
			}
			select {
			case err := <-s.Errors():
				if !strings.Contains(err.Error(), "test stops the next Claim") {
					t.Fatalf("unexpected fatal error: %v", err)
				}
			case <-time.After(time.Second):
				t.Fatal("second request did not report the sentinel")
			}
			shutdownClaimServer(t, s)
			current := rdb.Options()
			if current.ReadTimeout != original.ReadTimeout || current.WriteTimeout != original.WriteTimeout ||
				current.PoolSize != original.PoolSize || current.PoolTimeout != original.PoolTimeout ||
				current.MaxRetries != original.MaxRetries || current.ContextTimeoutEnabled != original.ContextTimeoutEnabled {
				t.Fatal("Claim changed the caller's client options")
			}
			if err := rdb.Ping(context.Background()).Err(); err != nil {
				t.Fatalf("Shutdown closed the caller's pool: %v", err)
			}
		})
	}
}

func TestClaimTCPNOSCRIPTSharesDeadline(t *testing.T) {
	t.Parallel()
	requests := make(chan claimTCPRequest, 4)
	addr := serveClaimTCP(t, func(connection int, args []string) string {
		if !isClaimTCPCommand(args) {
			return claimTCPDefaultReply(args)
		}
		request := claimTCPCommand(connection, args)
		requests <- request
		if request.command == "evalsha" {
			time.Sleep(300 * time.Millisecond)
			return "-NOSCRIPT No matching script. Please use EVAL.\r\n"
		}
		return "-NOPERM fallback reached\r\n"
	})
	var armed, commandConn atomic.Bool
	deadlines := make(chan time.Time, 4)
	opts := claimTCPOptions(addr, 8*time.Second, 8*time.Second)
	opts.Dialer = func(ctx context.Context, network, addr string) (net.Conn, error) {
		conn, err := (&net.Dialer{}).DialContext(ctx, network, addr)
		if err == nil && commandConn.CompareAndSwap(false, true) {
			conn = &renewDeadlineConn{Conn: conn, armed: &armed, deadlines: deadlines}
		}
		return conn, err
	}
	rdb := redis.NewClient(opts)
	t.Cleanup(func() { _ = rdb.Close() })
	if err := rdb.Ping(context.Background()).Err(); err != nil {
		t.Fatal(err)
	}
	armed.Store(true)
	s := startClaimServer(t, rdb, unexpectedClaimHandler(t))
	sha := awaitClaimSignal(t, requests, time.Second)
	fallback := awaitClaimSignal(t, requests, time.Second)
	if sha.command != "evalsha" || fallback.command != "eval" || sha.token == "" || sha.token != fallback.token {
		t.Fatalf("expected same-token NOSCRIPT fallback, got %+v and %+v", sha, fallback)
	}
	select {
	case err := <-s.Errors():
		if !strings.Contains(err.Error(), "fallback reached") {
			t.Fatalf("unexpected fatal error: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("fallback did not return its test sentinel")
	}
	first, second := awaitClaimSignal(t, deadlines, time.Second), awaitClaimSignal(t, deadlines, time.Second)
	// go-redis 读取缓存时钟设置 I/O 上限，起始命令可能略早于 context 截止时间。
	if first.IsZero() || second.IsZero() || second.Sub(first).Abs() > 50*time.Millisecond {
		t.Fatalf("NOSCRIPT reset the socket deadline: EVALSHA=%v EVAL=%v", first, second)
	}
}

func unexpectedClaimHandler(t *testing.T) HandlerFunc {
	t.Helper()
	return func(context.Context, *Task) error {
		t.Error("Claim without a successful reply started a Handler")
		return nil
	}
}

func claimSuccessReply(id string) string {
	return fmt.Sprintf("*6\r\n$%d\r\n%s\r\n$3\r\njob\r\n$0\r\n\r\n$7\r\ndefault\r\n$1\r\n1\r\n$5\r\n60000\r\n", len(id), id)
}

func TestClaimTCPSlotWaitPrecedesRequestBudget(t *testing.T) {
	t.Parallel()
	var calls atomic.Int32
	requests := make(chan claimTCPRequest, 8)
	addr := serveClaimTCP(t, func(connection int, args []string) string {
		if !isClaimTCPCommand(args) {
			return claimTCPDefaultReply(args)
		}
		requests <- claimTCPCommand(connection, args)
		switch calls.Add(1) {
		case 1:
			return claimSuccessReply("first")
		case 2:
			return claimSuccessReply("second")
		default:
			return "*0\r\n"
		}
	})
	rdb := claimTCPClient(t, addr, 8*time.Second, 8*time.Second)
	started, release := make(chan string, 4), make(chan struct{})
	defer closeShutdownGate(release)
	s := startClaimServer(t, rdb, func(_ context.Context, task *Task) error {
		started <- task.ID
		if task.ID == "first" {
			<-release
		}
		return nil
	})
	awaitClaimSignal(t, requests, time.Second)
	if id := awaitClaimSignal(t, started, time.Second); id != "first" {
		t.Fatalf("first Handler received %q", id)
	}
	select {
	case request := <-requests:
		t.Fatalf("Claim sent while the only slot was occupied: %+v", request)
	case <-time.After(3200 * time.Millisecond):
	}
	close(release)
	awaitClaimSignal(t, requests, time.Second)
	if id := awaitClaimSignal(t, started, time.Second); id != "second" {
		t.Fatalf("second Handler received %q", id)
	}
	select {
	case err := <-s.Errors():
		t.Fatalf("unexpected fatal error: %v", err)
	default:
	}
}

// 通过调用方的限流器观察连接池等待结果，命令仍使用真实 TCP 连接。
type claimPoolObserver struct {
	armed   atomic.Bool
	started chan time.Time
	results chan error
}

func (o *claimPoolObserver) Allow() error {
	if o.armed.Load() {
		o.started <- time.Now()
	}
	return nil
}

func (o *claimPoolObserver) ReportResult(err error) {
	if o.armed.Load() {
		o.results <- err
	}
}

func TestClaimTCPPoolWaitBudgetAndCancellation(t *testing.T) {
	for _, shutdown := range []bool{false, true} {
		name := "deadline"
		if shutdown {
			name = "shutdown"
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			observer := &claimPoolObserver{started: make(chan time.Time, 8), results: make(chan error, 8)}
			requests := make(chan claimTCPRequest, 4)
			addr := serveClaimTCP(t, func(connection int, args []string) string {
				if isClaimTCPCommand(args) {
					requests <- claimTCPCommand(connection, args)
					return "-NOPERM request escaped the occupied pool\r\n"
				}
				// SUBSCRIBE 在握手完成后发送。确认前开始观察，避免混入 HELLO 的结果。
				if strings.EqualFold(args[0], "subscribe") {
					observer.armed.Store(true)
				}
				return claimTCPDefaultReply(args)
			})
			opts := claimTCPOptions(addr, 8*time.Second, 8*time.Second)
			opts.Limiter = observer
			rdb := redis.NewClient(opts)
			t.Cleanup(func() { _ = rdb.Close() })
			held := rdb.Conn()
			t.Cleanup(func() { _ = held.Close() })
			if err := held.Ping(context.Background()).Err(); err != nil {
				t.Fatal(err)
			}
			// Conn 固定持有唯一池连接；订阅走独立的连接，领取会等待原池。
			s := startClaimServer(t, rdb, unexpectedClaimHandler(t))
			began := awaitClaimSignal(t, observer.started, time.Second)
			want := context.DeadlineExceeded
			if shutdown {
				ctx, cancel := context.WithTimeout(context.Background(), time.Second)
				defer cancel()
				if err := s.Shutdown(ctx); err != nil {
					t.Fatalf("Shutdown did not cancel Claim's pool wait: %v", err)
				}
				want = context.Canceled
			}
			if err := awaitClaimSignal(t, observer.results, 4*time.Second); !errors.Is(err, want) {
				t.Fatalf("Claim result from occupied pool = %v, want %v", err, want)
			}
			if elapsed := time.Since(began); !shutdown && (elapsed < 2700*time.Millisecond || elapsed > 4*time.Second) {
				t.Fatalf("Claim waited %v for a connection, want 3s with scheduling tolerance", elapsed)
			}
			select {
			case request := <-requests:
				t.Fatalf("Claim unexpectedly reached TCP endpoint: %+v", request)
			case err := <-s.Errors():
				t.Fatalf("unexpected fatal error: %v", err)
			default:
			}
			if shutdown && len(observer.started) != 0 {
				t.Fatal("Claim restarted after Shutdown")
			}
		})
	}
}

type observeClaimWrite struct {
	net.Conn
	observe func([]string)
}

func (c *observeClaimWrite) Write(data []byte) (int, error) {
	if args, err := readRedisTCPCommand(bufio.NewReader(bytes.NewReader(data))); err == nil && isClaimTCPCommand(args) {
		c.observe(args)
	}
	return c.Conn.Write(data)
}

func TestClaimRedisLostResponseThroughServer(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	admin := startSubmitRedis(t)
	if _, err := redisstore.Claim(ctx, admin, claimParams("warm")); err != nil {
		t.Fatal(err)
	}
	client := integrationClient(t, admin)
	mustSubmit(t, client, "first", "job", nil)
	mustSubmit(t, client, "second", "job", nil)

	var armed, dropped, commandConn atomic.Bool
	var calls atomic.Int32
	requests := make(chan claimTCPRequest, 8)
	allowNext := make(chan struct{})
	started, releaseHandler := make(chan string, 4), make(chan struct{})
	defer closeShutdownGate(allowNext)
	defer closeShutdownGate(releaseHandler)
	opts := claimTCPOptions(admin.Options().Addr, time.Second, time.Second)
	opts.Dialer = func(ctx context.Context, network, addr string) (net.Conn, error) {
		conn, err := (&net.Dialer{}).DialContext(ctx, network, addr)
		if err != nil {
			return nil, err
		}
		// Ping 预热的命令连接丢弃真实 Redis 的第一条成功响应。订阅连接不受影响。
		if commandConn.CompareAndSwap(false, true) {
			conn = &dropSubmitReplyConn{Conn: conn, armed: &armed, dropped: &dropped, expected: []byte(claimSuccessReply("first"))}
		}
		return &observeClaimWrite{Conn: conn, observe: func(args []string) {
			requests <- claimTCPCommand(0, args)
			switch calls.Add(1) {
			case 1:
				armed.Store(true)
			case 2:
				// 观察第二条命令的 token，并暂停送往 Redis，直到 GetTask 读到故障后的状态。
				<-allowNext
			}
		}}, nil
	}
	rdb := redis.NewClient(opts)
	t.Cleanup(func() { _ = rdb.Close() })
	if err := rdb.Ping(ctx).Err(); err != nil {
		t.Fatal(err)
	}
	s := startClaimServer(t, rdb, func(_ context.Context, task *Task) error {
		started <- task.ID
		<-releaseHandler
		return nil
	})
	first := awaitClaimSignal(t, requests, time.Second)
	second := awaitClaimSignal(t, requests, 2*time.Second)
	if !dropped.Load() || first.token == "" || second.token == "" || first.token == second.token {
		t.Fatalf("expected an independent Claim after a lost response: dropped=%v first=%+v second=%+v", dropped.Load(), first, second)
	}
	checkTask := func(id, status string, attempts int) {
		t.Helper()
		info, err := client.GetTask(ctx, id)
		if err != nil || info.Status != status || info.Attempts != attempts {
			t.Fatalf("GetTask(%q) = %+v, %v, want status=%s attempts=%d", id, info, err, status, attempts)
		}
	}
	checkTask("first", "running", 1)
	checkTask("second", "pending", 0)
	select {
	case id := <-started:
		t.Fatalf("Handler started for unknown Claim: %q", id)
	default:
	}

	close(allowNext)
	if id := awaitClaimSignal(t, started, time.Second); id != "second" {
		t.Fatalf("next independent Claim started Handler for %q", id)
	}
	checkTask("second", "running", 1)
	close(releaseHandler)
	shutdownClaimServer(t, s)
	checkTask("first", "running", 1)
	checkTask("second", "completed", 1)
	select {
	case err := <-s.Errors():
		t.Fatalf("unknown Claim was fatal: %v", err)
	default:
	}
}
