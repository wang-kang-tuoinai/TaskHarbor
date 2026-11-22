//go:build integration

package taskq

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

// 让真实 TCP 连接停在指定请求的响应阶段；除 HELLO 外不伪造命令响应。
func silentRedisReply(t *testing.T, command string) (addr string, entered, disconnected <-chan struct{}) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	waiting, closed := make(chan struct{}), make(chan struct{})
	accepted := make(chan net.Conn, 1)
	go func() {
		defer close(closed)
		defer close(accepted)
		conn, err := listener.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		accepted <- conn
		reader := bufio.NewReader(conn)
		for {
			args, err := readRedisTCPCommand(reader)
			if err != nil {
				return
			}
			switch strings.ToLower(args[0]) {
			case command:
				closeShutdownGate(waiting)
			case "hello":
				if _, err := io.WriteString(conn, "-ERR unknown command 'hello'\r\n"); err != nil {
					return
				}
			default:
				return
			}
		}
	}()
	t.Cleanup(func() {
		_ = listener.Close()
		if conn := <-accepted; conn != nil {
			_ = conn.Close()
		}
		<-closed
	})
	return listener.Addr().String(), waiting, closed
}

func TestStartCancellationSubscriptionTCPBudget(t *testing.T) {
	for _, command := range []string{"hello", "subscribe"} {
		t.Run(command, func(t *testing.T) {
			addr, blocked, disconnected := silentRedisReply(t, command)
			rdb := redis.NewClient(&redis.Options{
				Addr: addr, MaxRetries: -1, ContextTimeoutEnabled: true, Protocol: 2, DisableIdentity: true,
				ReadTimeout: -2, WriteTimeout: -2,
			})
			t.Cleanup(func() { _ = rdb.Close() })
			readTimeout, writeTimeout := rdb.Options().ReadTimeout, rdb.Options().WriteTimeout
			s, err := NewServer(rdb)
			if err != nil {
				t.Fatal(err)
			}
			if err := s.HandleFunc("job", func(context.Context, *Task) error { return nil }); err != nil {
				t.Fatal(err)
			}
			started := make(chan error, 1)
			began := time.Now()
			go func() { started <- s.Start() }()
			awaitReclaimSignal(t, blocked)
			var timeout net.Error
			if err := awaitReclaimSignal(t, started); !errors.As(err, &timeout) || !timeout.Timeout() {
				t.Fatalf("silent %s: Start=%v", command, err)
			}
			if elapsed := time.Since(began); elapsed < 2*time.Second || elapsed > 4*time.Second {
				t.Fatalf("startup exceeded 3-second budget tolerance: %v", elapsed)
			}
			awaitReclaimSignal(t, disconnected)
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			if err := s.Shutdown(ctx); err != nil {
				t.Fatalf("failed Start left unfinished work: %v", err)
			}
			if err := s.Start(); err == nil {
				t.Fatal("restarted server after failed subscription")
			}
			if rdb.Options().ReadTimeout != readTimeout || rdb.Options().WriteTimeout != writeTimeout {
				t.Fatal("bounded subscription modified Redis client's options")
			}
		})
	}
}

func TestCancelTCPContextBudget(t *testing.T) {
	addr, blocked, disconnected := silentRedisReply(t, "evalsha")
	rdb := redis.NewClient(&redis.Options{
		Addr: addr, MaxRetries: -1, ContextTimeoutEnabled: true, Protocol: 2, DisableIdentity: true,
		ReadTimeout: 6 * time.Second, WriteTimeout: 6 * time.Second,
	})
	t.Cleanup(func() { _ = rdb.Close() })
	client := integrationClient(t, rdb)
	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()
	began := time.Now()
	result, err := client.Cancel(ctx, "unknown")
	var timeout net.Error
	if result != (CancelResult{}) || !errors.As(err, &timeout) || !timeout.Timeout() {
		t.Fatalf("Cancel result = %+v, %v", result, err)
	}
	awaitReclaimSignal(t, blocked)
	awaitReclaimSignal(t, disconnected)
	if elapsed := time.Since(began); elapsed > time.Second {
		t.Fatalf("Cancel used the client I/O timeout instead of ctx: %v", elapsed)
	}
}

func TestCancelRedisLostReply(t *testing.T) {
	admin := startSubmitRedis(t)
	ctx := context.Background()
	// 预热脚本，使断连发生在已生效的 EVALSHA 返回之后。
	requireCancel(t, integrationClient(t, admin), "warm", CancelNotFound, "")
	const owner = "worker"
	sub := admin.Subscribe(ctx, "cancel:"+owner)
	t.Cleanup(func() { _ = sub.Close() })
	if _, err := sub.Receive(ctx); err != nil {
		t.Fatal(err)
	}
	for _, retries := range []int{-1, 1} {
		t.Run(fmt.Sprintf("max-retries=%d", retries), func(t *testing.T) {
			id := t.Name()
			mustSubmit(t, integrationClient(t, admin), id, "job", nil)
			mustClaimForCancel(t, admin, id, owner, "original-attempt")
			var armed, dropped atomic.Bool
			rdb := redis.NewClient(&redis.Options{
				Addr: admin.Options().Addr, MaxRetries: retries, PoolSize: 1,
				ContextTimeoutEnabled: true, MinRetryBackoff: time.Millisecond, MaxRetryBackoff: time.Millisecond,
				Dialer: func(ctx context.Context, network, addr string) (net.Conn, error) {
					conn, err := (&net.Dialer{}).DialContext(ctx, network, addr)
					if err != nil {
						return nil, err
					}
					return &dropSubmitReplyConn{
						Conn: conn, armed: &armed, dropped: &dropped,
						expected: []byte("*3\r\n$8\r\ncanceled\r\n$8\r\ncanceled\r\n$0\r\n\r\n"),
					}, nil
				},
			})
			t.Cleanup(func() { _ = rdb.Close() })
			if err := rdb.Ping(ctx).Err(); err != nil {
				t.Fatal(err)
			}
			client := integrationClient(t, rdb)
			armed.Store(true)
			result, err := client.Cancel(ctx, id)
			if !dropped.Load() {
				t.Fatalf("did not lose committed Redis reply: %+v, %v", result, err)
			}
			if retries < 0 {
				if result != (CancelResult{}) || !transientRedisError(err) {
					t.Fatalf("ambiguous cancellation: %+v, %v", result, err)
				}
			} else if result != (CancelResult{Outcome: CancelAlreadyTerminal, Status: "canceled"}) || err != nil {
				t.Fatalf("transparent retry: %+v, %v", result, err)
			}
			before := submitHash(t, admin, id)
			requireCancel(t, client, id, CancelAlreadyTerminal, "canceled")
			if before["finished_at"] == "" || !reflect.DeepEqual(before, submitHash(t, admin, id)) {
				t.Fatalf("repeat changed terminal state: %q", before)
			}
			msg, err := sub.ReceiveTimeout(ctx, time.Second)
			if message, ok := msg.(*redis.Message); err != nil || !ok || !strings.Contains(message.Payload, "original-attempt") {
				t.Fatalf("missing initial cancellation message: %v, %v", msg, err)
			}
			if err := admin.Publish(ctx, "cancel:"+owner, "fence").Err(); err != nil {
				t.Fatal(err)
			}
			msg, err = sub.ReceiveTimeout(ctx, time.Second)
			if message, ok := msg.(*redis.Message); err != nil || !ok || message.Payload != "fence" {
				t.Fatalf("repeat published a cancellation message: %v, %v", msg, err)
			}
		})
	}
}
