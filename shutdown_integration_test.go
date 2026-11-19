//go:build integration

package taskq

import (
	"bufio"
	"context"
	"errors"
	"io"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

func TestShutdownRedisDrainsReportAndPreservesClient(t *testing.T) {
	for _, failure := range []bool{false, true} {
		name := "completed"
		if failure {
			name = "retry_wait"
		}
		t.Run(name, func(t *testing.T) {
			rdb := startSubmitRedis(t)
			mustSubmit(t, integrationClient(t, rdb), "task", "job", nil, WithSubmitMaxRetry(1), WithSubmitExecutionTimeout(time.Minute))
			s := integrationServer(t, rdb, WithConcurrency(1), WithLeaseDuration(6*time.Second), WithRenewInterval(2*time.Second))
			releaseHandler, releaseReport := make(chan struct{}), make(chan struct{})
			defer closeShutdownGate(releaseHandler)
			defer closeShutdownGate(releaseReport)
			started, reporting, renewed := make(chan struct{}), make(chan struct{}), make(chan struct{}, 4)
			s.reportClient.AddHook(observeRenewHook{process: func(next redis.ProcessHook) redis.ProcessHook {
				return func(ctx context.Context, cmd redis.Cmder) error {
					if cmd.Name() == "evalsha" {
						close(reporting)
						<-releaseReport
					}
					return next(ctx, cmd)
				}
			}})
			s.renewClient.AddHook(observeRenewHook{process: func(next redis.ProcessHook) redis.ProcessHook {
				return func(ctx context.Context, cmd redis.Cmder) error {
					err := next(ctx, cmd)
					if err == nil && isRenewCommand(cmd) {
						renewed <- struct{}{}
					}
					return err
				}
			}})
			if err := s.HandleFunc("job", func(_ context.Context, task *Task) error {
				close(started)
				<-releaseHandler
				if failure {
					return errors.New("business failure")
				}
				return task.SetResult("ok")
			}); err != nil {
				t.Fatal(err)
			}
			if err := s.Start(); err != nil {
				t.Fatal(err)
			}
			select {
			case <-started:
			case <-time.After(3 * time.Second):
				t.Fatal("Handler did not start")
			}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			result := make(chan error, 1)
			go func() { result <- s.Shutdown(ctx) }()
			select {
			case <-s.claimDone:
			case <-time.After(time.Second):
				t.Fatal("Shutdown did not stop Claim")
			}
			mustSubmit(t, integrationClient(t, rdb), "pending", "job", nil)
			close(releaseHandler)
			select {
			case <-reporting:
			case <-time.After(time.Second):
				t.Fatal("report did not start")
			}
			select {
			case err := <-result:
				t.Fatalf("returned before report exit: %v", err)
			case <-renewed:
			case <-time.After(3 * time.Second):
				t.Fatal("drain did not renew")
			}
			close(releaseReport)
			if err := <-result; err != nil {
				t.Fatal(err)
			}
			fields := submitHash(t, rdb, "task")
			if fields["status"] != name || fields["attempts"] != "1" {
				t.Fatalf("task=%q", fields)
			}
			if !failure && fields["result"] != `"ok"` {
				t.Fatalf("result=%q", fields["result"])
			}
			if pending := submitHash(t, rdb, "pending"); pending["status"] != "pending" || pending["attempts"] != "0" {
				t.Fatalf("Shutdown drained global queue: %q", pending)
			}
			if err := rdb.Ping(context.Background()).Err(); err != nil {
				t.Fatalf("Shutdown closed caller's client: %v", err)
			}
			select {
			case _, ok := <-s.Errors():
				if !ok {
					t.Fatal("Shutdown closed Errors")
				}
				t.Fatal("unexpected fatal error")
			default:
			}
		})
	}
}

func TestShutdownTCPBudgetDoesNotWaitForSocket(t *testing.T) {
	for _, operation := range []string{"claim", "report"} {
		t.Run(operation, func(t *testing.T) {
			listener, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			blocked, release := make(chan struct{}), make(chan struct{})
			defer closeShutdownGate(release)
			acceptDone := make(chan struct{})
			var mu sync.Mutex
			var conns []net.Conn
			var workers sync.WaitGroup
			var blockOnce sync.Once
			var claimed bool
			go func() {
				defer close(acceptDone)
				for {
					conn, err := listener.Accept()
					if err != nil {
						return
					}
					mu.Lock()
					conns = append(conns, conn)
					mu.Unlock()
					workers.Go(func() {
						defer conn.Close()
						reader := bufio.NewReader(conn)
						for {
							parts, err := readRedisTCPCommand(reader)
							if err != nil {
								return
							}
							switch strings.ToLower(parts[0]) {
							case "hello":
								_, _ = io.WriteString(conn, "-ERR unknown command 'hello'\r\n")
							case "subscribe":
								_, _ = io.WriteString(conn, cancelSubscribeAck(parts[1]))
							case "evalsha", "eval":
								isClaim := parts[3] == "taskq:default:pending"
								if operation == "claim" || !isClaim {
									blockOnce.Do(func() { close(blocked) })
									<-release
								}
								reply := ":1\r\n"
								if isClaim {
									mu.Lock()
									first := !claimed
									claimed = true
									mu.Unlock()
									reply = "*0\r\n"
									if operation == "report" && first {
										reply = "*6\r\n$4\r\ntask\r\n$3\r\njob\r\n$0\r\n\r\n$7\r\ndefault\r\n$1\r\n1\r\n$5\r\n60000\r\n"
									}
								}
								_, _ = io.WriteString(conn, reply)
							default:
								_, _ = io.WriteString(conn, "+PONG\r\n")
							}
						}
					})
				}
			}()
			t.Cleanup(func() {
				closeShutdownGate(release)
				_ = listener.Close()
				<-acceptDone
				mu.Lock()
				for _, conn := range conns {
					_ = conn.Close()
				}
				mu.Unlock()
				workers.Wait()
			})
			rdb := redis.NewClient(&redis.Options{Addr: listener.Addr().String(), MaxRetries: -1, ContextTimeoutEnabled: true, ReadTimeout: time.Second, WriteTimeout: time.Second, Protocol: 2, DisableIdentity: true})
			t.Cleanup(func() { _ = rdb.Close() })
			s := integrationServer(t, rdb, WithConcurrency(1))
			if err := s.HandleFunc("job", func(context.Context, *Task) error { return nil }); err != nil {
				t.Fatal(err)
			}
			if err := s.Start(); err != nil {
				t.Fatal(err)
			}
			select {
			case <-blocked:
			case <-time.After(3 * time.Second):
				t.Fatal("socket request did not block")
			}
			ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
			defer cancel()
			started := time.Now()
			if err := s.Shutdown(ctx); !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("Shutdown=%v", err)
			}
			if elapsed := time.Since(started); elapsed > 500*time.Millisecond {
				t.Fatalf("Shutdown waited for socket: %v", elapsed)
			}
			if err := s.Shutdown(context.Background()); !errors.Is(err, ErrShutdownInProgress) {
				t.Fatalf("live socket marked stopped: %v", err)
			}
			close(release)
			select {
			case <-s.shutdownDone:
			case <-time.After(2 * time.Second):
				t.Fatal("socket release did not finish cleanup")
			}
			if err := s.Shutdown(context.Background()); err != nil {
				t.Fatal(err)
			}
			if err := rdb.Ping(context.Background()).Err(); err != nil {
				t.Fatalf("caller's Redis pool unusable: %v", err)
			}
		})
	}
}
