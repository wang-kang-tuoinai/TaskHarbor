//go:build integration

package taskq

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"net"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

func awaitReportState(t *testing.T, rdb *redis.Client, id, status string) map[string]string {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for {
		fields := submitHash(t, rdb, id)
		if fields["status"] == status {
			return fields
		}
		if time.Now().After(deadline) {
			t.Fatalf("task %s expected %s, got %q", id, status, fields)
		}
		time.Sleep(time.Millisecond)
	}
}

func TestReportingServerFailureSources(t *testing.T) {
	for _, mode := range []string{"error", "panic", "unknown", "timeout", "retry"} {
		t.Run(mode, func(t *testing.T) {
			rdb := startSubmitRedis(t)
			ctx := context.Background()
			taskType := "job"
			if mode == "unknown" {
				taskType = "unknown"
			}
			opts := []SubmitOption{WithSubmitExecutionTimeout(50 * time.Millisecond)}
			if mode == "retry" {
				opts = append(opts, WithSubmitMaxRetry(1))
			}
			mustSubmit(t, integrationClient(t, rdb), "task", taskType, nil, opts...)
			s := integrationServer(t, rdb, WithConcurrency(1))
			release := make(chan struct{})
			defer close(release)
			if err := s.HandleFunc("job", func(context.Context, *Task) error {
				switch mode {
				case "panic":
					panic("private panic")
				case "timeout":
					<-release
					return nil
				default:
					return errors.New("private business error")
				}
			}); err != nil {
				t.Fatal(err)
			}
			if err := s.Start(); err != nil {
				t.Fatal(err)
			}
			want := "failed"
			if mode == "retry" {
				want = "retry_wait"
			}
			fields := awaitReportState(t, rdb, "task", want)
			if fields["attempts"] != "1" || fields["last_error"] == "" || fields["last_failed_at"] == "" || fields["owner_id"] != "" || fields["attempt_token"] != "" || fields["result"] != "" {
				t.Fatalf("hash=%q", fields)
			}
			if n, err := rdb.Exists(ctx, claimActive, claimLease).Result(); err != nil || n != 0 {
				t.Fatalf("active/lease=%d err=%v", n, err)
			}
			if mode == "timeout" {
				if fields["last_error"] != context.DeadlineExceeded.Error() || len(s.slots) != 1 {
					t.Fatal("timeout did not report independently while handler retained slot")
				}
			}
			if mode == "retry" {
				if fields["finished_at"] != "" {
					t.Fatal("retry_wait has finished_at")
				}
				time.Sleep(25 * time.Millisecond)
				if fields := submitHash(t, rdb, "task"); fields["status"] != "retry_wait" {
					t.Fatal("unexpected automatic retry")
				}
			}
			if len(s.Errors()) != 0 {
				t.Fatal("business failure became fatal")
			}
			stopStartLoop(t, s)
		})
	}
}

func TestReportingRedisLostSuccessfulReply(t *testing.T) {
	for _, failure := range []bool{false, true} {
		t.Run(map[bool]string{false: "complete", true: "failure"}[failure], func(t *testing.T) {
			admin := startSubmitRedis(t)
			ctx := context.Background()
			// 预热实际脚本，故障仅丢弃一次已生效的标量成功响应。
			if _, err := callStoreReport(ctx, admin, failure); err != nil {
				t.Fatal(err)
			}
			mustSubmit(t, integrationClient(t, admin), "task", "job", nil, WithSubmitMaxRetry(1))
			var armed, dropped atomic.Bool
			rdb := redis.NewClient(&redis.Options{Addr: admin.Options().Addr, MaxRetries: -1, ContextTimeoutEnabled: true, PoolSize: 1, ReadTimeout: time.Second, WriteTimeout: time.Second, Dialer: func(ctx context.Context, network, addr string) (net.Conn, error) {
				conn, err := (&net.Dialer{Timeout: time.Second}).DialContext(ctx, network, addr)
				if err != nil {
					return nil, err
				}
				return &dropSubmitReplyConn{Conn: conn, armed: &armed, dropped: &dropped, expected: []byte(":1\r\n")}, nil
			}})
			t.Cleanup(func() { _ = rdb.Close() })
			s := integrationServer(t, rdb, WithConcurrency(1))
			started := make(chan struct{})
			release := make(chan struct{})
			second := make(chan struct{})
			allow := make(chan struct{})
			var calls atomic.Int32
			t.Cleanup(func() {
				for _, gate := range []chan struct{}{release, allow} {
					select {
					case <-gate:
					default:
						close(gate)
					}
				}
			})
			var logs bytes.Buffer
			previous := slog.Default()
			slog.SetDefault(slog.New(slog.NewJSONHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug})))
			defer slog.SetDefault(previous)
			s.reportClient.AddHook(observeRenewHook{process: func(next redis.ProcessHook) redis.ProcessHook {
				return func(ctx context.Context, cmd redis.Cmder) error {
					if cmd.Name() != "evalsha" && cmd.Name() != "eval" {
						return next(ctx, cmd)
					}
					n := calls.Add(1)
					if n == 1 {
						armed.Store(true)
					}
					if n == 2 {
						close(second)
						<-allow
					}
					return next(ctx, cmd)
				}
			}})
			if err := s.HandleFunc("job", func(_ context.Context, task *Task) error {
				close(started)
				<-release
				if failure {
					return errors.New("private-business-error")
				}
				return task.SetResult("private-result")
			}); err != nil {
				t.Fatal(err)
			}
			if err := s.Start(); err != nil {
				t.Fatal(err)
			}
			select {
			case <-started:
			case <-time.After(3 * time.Second):
				t.Fatal("handler did not start")
			}
			stopStartLoop(t, s)
			close(release)
			select {
			case <-second:
			case <-time.After(3 * time.Second):
				t.Fatal("lost response did not receive bounded retry")
			}
			if !dropped.Load() {
				t.Fatal("actual success response was not dropped")
			}
			before := claimSnapshot(t, admin)
			fields := submitHash(t, admin, "task")
			want := "completed"
			if failure {
				want = "retry_wait"
			}
			if fields["status"] != want {
				t.Fatalf("first report did not take effect %q", fields)
			}
			close(allow)
			awaitStartExecutions(t, s)
			if calls.Load() != 2 || len(s.Errors()) != 0 || !reflect.DeepEqual(before, claimSnapshot(t, admin)) {
				t.Fatalf("duplicate transition or wrong outcome calls=%d fatal=%d", calls.Load(), len(s.Errors()))
			}
			text := logs.String()
			if strings.Count(text, `"level":"WARN"`) != 1 || !strings.Contains(text, "unconfirmed after rejection") || strings.Contains(text, "private-business-error") || strings.Contains(text, "private-result") {
				t.Fatalf("wrong outcome log %s", text)
			}
		})
	}
}

func TestReportingServerCompletes(t *testing.T) {
	rdb := startSubmitRedis(t)
	mustSubmit(t, integrationClient(t, rdb), "task", "job", nil)
	s := integrationServer(t, rdb, WithConcurrency(1))
	if err := s.HandleFunc("job", func(_ context.Context, task *Task) error { return task.SetResult(map[string]bool{"ok": true}) }); err != nil {
		t.Fatal(err)
	}
	if err := s.Start(); err != nil {
		t.Fatal(err)
	}
	fields := awaitReportState(t, rdb, "task", "completed")
	if fields["result"] != `{"ok":true}` || fields["attempts"] != "1" || fields["finished_at"] == "" {
		t.Fatalf("hash=%q", fields)
	}
	stopStartLoop(t, s)
	awaitStartExecutions(t, s)
}

func TestReportingServerRenewsWhileResultPending(t *testing.T) {
	rdb := startSubmitRedis(t)
	ctx := context.Background()
	mustSubmit(t, integrationClient(t, rdb), "task", "job", nil, WithSubmitExecutionTimeout(100*time.Millisecond))
	s := integrationServer(t, rdb, WithConcurrency(1), WithLeaseDuration(6*time.Second), WithRenewInterval(2*time.Second))
	entered := make(chan struct{})
	allow := make(chan struct{})
	renewed := make(chan struct{}, 4)
	t.Cleanup(func() {
		select {
		case <-allow:
		default:
			close(allow)
		}
	})
	var requestOnce atomic.Bool
	s.reportClient.AddHook(observeRenewHook{process: func(next redis.ProcessHook) redis.ProcessHook {
		return func(ctx context.Context, cmd redis.Cmder) error {
			if (cmd.Name() == "evalsha" || cmd.Name() == "eval") && requestOnce.CompareAndSwap(false, true) {
				close(entered)
				<-allow
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
	if err := s.HandleFunc("job", func(_ context.Context, task *Task) error { return task.SetResult("success") }); err != nil {
		t.Fatal(err)
	}
	if err := s.Start(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		t.Fatal("report did not start")
	}
	stopStartLoop(t, s)
	initial, err := rdb.ZScore(ctx, claimLease, "task").Result()
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-renewed:
	case <-time.After(3 * time.Second):
		t.Fatal("report pending without continued renewal")
	}
	if len(s.slots) != 0 || len(s.snapshotExecutions()) != 1 {
		t.Fatal("slot release and reporting eligibility were coupled")
	}
	if score, err := rdb.ZScore(ctx, claimLease, "task").Result(); err != nil || score <= initial {
		t.Fatalf("lease=%v initial=%v err=%v", score, initial, err)
	}
	close(allow)
	fields := awaitReportState(t, rdb, "task", "completed")
	awaitStartExecutions(t, s)
	if fields["result"] != `"success"` || fields["last_error"] != "" {
		t.Fatalf("result reclassified after execution timeout %q", fields)
	}
}

func TestReportingServerFatalKeepsHealthyRenewal(t *testing.T) {
	rdb := startSubmitRedis(t)
	ctx := context.Background()
	for _, id := range []string{"bad", "healthy"} {
		mustSubmit(t, integrationClient(t, rdb), id, "job", nil)
	}
	s := integrationServer(t, rdb, WithConcurrency(2), WithLeaseDuration(6*time.Second), WithRenewInterval(2*time.Second))
	started := make(chan string, 2)
	badRelease := make(chan struct{})
	healthyRelease := make(chan struct{})
	renewed := make(chan struct{}, 4)
	t.Cleanup(func() {
		for _, gate := range []chan struct{}{badRelease, healthyRelease} {
			select {
			case <-gate:
			default:
				close(gate)
			}
		}
	})
	s.reportClient.AddHook(observeRenewHook{process: func(next redis.ProcessHook) redis.ProcessHook {
		return func(ctx context.Context, cmd redis.Cmder) error {
			args := cmd.Args()
			if (cmd.Name() == "evalsha" || cmd.Name() == "eval") && args[len(args)-3] == "bad" {
				return startRedisError("NOPERM report denied")
			}
			return next(ctx, cmd)
		}
	}})
	s.renewClient.AddHook(observeRenewHook{process: func(next redis.ProcessHook) redis.ProcessHook {
		return func(ctx context.Context, cmd redis.Cmder) error {
			err := next(ctx, cmd)
			if err == nil && isRenewCommand(cmd) && cmd.Args()[3] == "taskq:meta:healthy" {
				renewed <- struct{}{}
			}
			return err
		}
	}})
	if err := s.HandleFunc("job", func(ctx context.Context, task *Task) error {
		started <- task.ID
		if task.ID == "bad" {
			<-badRelease
		} else {
			<-healthyRelease
			if ctx.Err() != nil {
				t.Error("report fatal canceled healthy handler")
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := s.Start(); err != nil {
		t.Fatal(err)
	}
	for range 2 {
		select {
		case <-started:
		case <-time.After(3 * time.Second):
			t.Fatal("handlers did not start")
		}
	}
	initial, err := rdb.ZScore(ctx, claimLease, "healthy").Result()
	if err != nil {
		t.Fatal(err)
	}
	close(badRelease)
	select {
	case err := <-s.Errors():
		if !strings.Contains(err.Error(), "NOPERM") {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("report fatal not surfaced")
	}
	stopStartLoop(t, s)
	select {
	case <-renewed:
	case <-time.After(3 * time.Second):
		t.Fatal("healthy renewal stopped with report failure")
	}
	if score, err := rdb.ZScore(ctx, claimLease, "healthy").Result(); err != nil || score <= initial {
		t.Fatalf("healthy score=%v err=%v", score, err)
	}
	if fields := submitHash(t, rdb, "bad"); fields["status"] != "running" || fields["last_error"] != "" {
		t.Fatalf("communication failure became business failure %q", fields)
	}
	close(healthyRelease)
	awaitReportState(t, rdb, "healthy", "completed")
	awaitStartExecutions(t, s)
	if len(s.Errors()) != 0 {
		t.Fatal("additional fatal notification")
	}
}
