//go:build integration

package taskq

import (
	"context"
	"errors"
	"io"
	"net"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/wang-kang-tuoinai/TaskHarbor/internal/redisstore"
)

func awaitReclaimSignal[T any](t *testing.T, ch <-chan T) T {
	t.Helper()
	select {
	case value := <-ch:
		return value
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for reclaim test signal")
		var zero T
		return zero
	}
}

func TestReclaimServersCompeteOnCurrentRedisState(t *testing.T) {
	rdb := startSubmitRedis(t)
	ctx := context.Background()
	for _, id := range []string{"renewed", "completed", "canceled"} {
		seedReclaimCandidate(t, rdb, id, false, float64(time.Now().Add(time.Minute).UnixMilli()))
	}
	if applied, err := redisstore.Renew(ctx, rdb, redisstore.RenewParams{TaskID: "renewed", AttemptToken: "old-renewed", LeaseDurationMS: 120000}); !applied || err != nil {
		t.Fatalf("renew applied=%v err=%v", applied, err)
	}
	if applied, err := redisstore.Complete(ctx, rdb, redisstore.CompleteParams{TaskID: "completed", AttemptToken: "old-completed"}); !applied || err != nil {
		t.Fatalf("completion applied=%v err=%v", applied, err)
	}
	// Cancel 尚未实现，夹具只建立取消成功后的合法终态与索引关系。
	if _, err := rdb.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
		pipe.ZRem(ctx, claimLease, "canceled")
		pipe.SRem(ctx, claimActive, "canceled")
		pipe.HDel(ctx, "taskq:meta:canceled", "owner_id", "attempt_token")
		pipe.HSet(ctx, "taskq:meta:canceled", "status", "canceled", "finished_at", time.Now().UnixMilli())
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	seedReclaimCandidate(t, rdb, "expired", false, 1)
	seedReclaimCandidate(t, rdb, "due", true, 1)
	entered := make(chan struct{}, 2)
	release := make(chan struct{})
	finished := make(chan error, 2)
	for range 2 {
		s := integrationServer(t, rdb, WithReclaimInterval(2*time.Second))
		s.cancelClaim() // 保留入队结果，并验证回收独立于领取生命周期。
		firstLease := true
		s.reclaimClient.AddHook(observeRenewHook{process: func(next redis.ProcessHook) redis.ProcessHook {
			return func(ctx context.Context, cmd redis.Cmder) error {
				if cmd.Name() != "evalsha" && cmd.Name() != "eval" {
					return next(ctx, cmd)
				}
				source := cmd.Args()[3].(string)
				if firstLease && source == claimLease {
					firstLease = false
					entered <- struct{}{}
					select {
					case <-release:
					case <-ctx.Done():
						return ctx.Err()
					}
				}
				err := next(ctx, cmd)
				if source == reclaimRetry && !redis.HasErrorPrefix(err, "NOSCRIPT") {
					finished <- err
				}
				return err
			}
		}})
		if err := s.HandleFunc("job", func(context.Context, *Task) error { return nil }); err != nil {
			t.Fatal(err)
		}
		if err := s.Start(); err != nil {
			t.Fatal(err)
		}
	}
	for range 2 {
		awaitReclaimSignal(t, entered)
	}
	close(release)
	for range 2 {
		if err := awaitReclaimSignal(t, finished); err != nil {
			t.Fatal(err)
		}
	}
	requirePending(t, rdb, "due")
	for id, want := range map[string]string{"renewed": "running", "completed": "completed", "canceled": "canceled", "expired": "retry_wait", "due": "pending"} {
		if fields := submitHash(t, rdb, id); fields["status"] != want || fields["attempts"] != "1" {
			t.Fatalf("%s: %v", id, fields)
		}
	}
	if ids, err := rdb.ZRange(ctx, reclaimRetry, 0, -1).Result(); err != nil || len(ids) != 1 || ids[0] != "expired" {
		t.Fatalf("retry=%q err=%v", ids, err)
	}
}

func TestReclaimServerRecoversTasksAfterAnotherServerStops(t *testing.T) {
	rdb := startSubmitRedis(t)
	ctx := context.Background()
	c := integrationClient(t, rdb)
	for _, id := range []string{"reported", "abandoned"} {
		mustSubmit(t, c, id, "job", nil, WithSubmitMaxRetry(1), WithSubmitExecutionTimeout(time.Minute))
	}
	if !t.Run("stop-original-instance", func(t *testing.T) {
		s := integrationServer(t, rdb, WithConcurrency(1))
		abandoned := make(chan struct{})
		if err := s.HandleFunc("job", func(ctx context.Context, task *Task) error {
			if task.ID == "reported" {
				return errors.New("first failure")
			}
			close(abandoned)
			<-ctx.Done()
			return ctx.Err()
		}); err != nil {
			t.Fatal(err)
		}
		if err := s.Start(); err != nil {
			t.Fatal(err)
		}
		awaitReclaimSignal(t, abandoned)
		awaitReportState(t, rdb, "reported", "retry_wait")
	}) {
		return
	}
	old := submitHash(t, rdb, "abandoned")
	if old["status"] != "running" || old["attempt_token"] == "" {
		t.Fatalf("stopping the original instance changed storage: %v", old)
	}
	if err := rdb.ZAdd(ctx, claimLease, redis.Z{Member: "abandoned", Score: 1}).Err(); err != nil {
		t.Fatal(err)
	}
	s := integrationServer(t, rdb, WithConcurrency(2), WithReclaimInterval(2*time.Second))
	started := make(chan Task, 2)
	release := make(chan struct{})
	t.Cleanup(func() {
		select {
		case <-release:
		default:
			close(release)
		}
	})
	if err := s.HandleFunc("job", func(_ context.Context, task *Task) error { started <- *task; <-release; return nil }); err != nil {
		t.Fatal(err)
	}
	if err := s.Start(); err != nil {
		t.Fatal(err)
	}
	awaitReportState(t, rdb, "abandoned", "retry_wait")
	// 只推进等待时间，两个 retry_wait 的来源仍分别是真实上报和回收。
	if err := rdb.ZAdd(ctx, reclaimRetry, redis.Z{Member: "reported", Score: 1}, redis.Z{Member: "abandoned", Score: 2}).Err(); err != nil {
		t.Fatal(err)
	}
	seen := make(map[string]int)
	for range 2 {
		task := awaitReclaimSignal(t, started)
		seen[task.ID] = task.Attempts
	}
	if seen["reported"] != 2 || seen["abandoned"] != 2 {
		t.Fatalf("new executions: %v", seen)
	}
	current := submitHash(t, rdb, "abandoned")
	oldToken := old["attempt_token"]
	if current["attempt_token"] == "" || current["attempt_token"] == oldToken || current["owner_id"] == old["owner_id"] {
		t.Fatalf("new execution reused old identity: %v", current)
	}
	if applied, err := redisstore.Renew(ctx, rdb, redisstore.RenewParams{TaskID: "abandoned", AttemptToken: oldToken, LeaseDurationMS: 30000}); applied || err != nil {
		t.Fatalf("old renew applied=%v err=%v", applied, err)
	}
	if applied, err := redisstore.Complete(ctx, rdb, redisstore.CompleteParams{TaskID: "abandoned", AttemptToken: oldToken}); applied || err != nil {
		t.Fatalf("old completion applied=%v err=%v", applied, err)
	}
	if applied, err := redisstore.ReportFailure(ctx, rdb, redisstore.FailureParams{TaskID: "abandoned", AttemptToken: oldToken, ErrorMessage: "late failure"}); applied || err != nil {
		t.Fatalf("old failure applied=%v err=%v", applied, err)
	}
	if err := redisstore.ReclaimLeases(ctx, rdb); err != nil {
		t.Fatal(err)
	}
	if fields := submitHash(t, rdb, "abandoned"); fields["status"] != "running" || fields["attempt_token"] != current["attempt_token"] {
		t.Fatalf("reclaim changed new execution: %v", fields)
	}
	close(release)
	if fields := awaitReportState(t, rdb, "reported", "completed"); fields["last_error"] != "first failure" || fields["attempts"] != "2" {
		t.Fatalf("reported task=%v", fields)
	}
	if fields := awaitReportState(t, rdb, "abandoned", "completed"); fields["last_error"] != "lease expired" || fields["attempts"] != "2" {
		t.Fatalf("reclaimed task=%v", fields)
	}
	if len(s.Errors()) != 0 {
		t.Fatalf("unexpected background error: %v", <-s.Errors())
	}
}

func TestReclaimServerSecondPhaseFailureKeepsLeaseMigration(t *testing.T) {
	rdb := startSubmitRedis(t)
	ctx := context.Background()
	seedReclaimCandidate(t, rdb, "expired", false, 1)
	seedReclaimCandidate(t, rdb, "good-retry", true, 1)
	seedReclaimCandidate(t, rdb, "bad-retry", true, 2)
	if err := rdb.HSet(ctx, "taskq:meta:bad-retry", "owner_id", "").Err(); err != nil {
		t.Fatal(err)
	}
	s := integrationServer(t, rdb, WithReclaimInterval(2*time.Second))
	if err := s.HandleFunc("job", func(context.Context, *Task) error { t.Error("unexpected claim"); return nil }); err != nil {
		t.Fatal(err)
	}
	if err := s.Start(); err != nil {
		t.Fatal(err)
	}
	awaitReclaimSignal(t, s.reclaimDone)
	awaitReclaimSignal(t, s.claimDone)
	err := awaitReclaimSignal(t, s.Errors())
	if !errors.Is(err, redisstore.ErrReclaimInconsistent) || !strings.Contains(err.Error(), "schedule retries") || !strings.Contains(err.Error(), "bad-retry") {
		t.Fatalf("wrong stage/candidate error: %v", err)
	}
	if fields := submitHash(t, rdb, "expired"); fields["status"] != "retry_wait" || fields["last_error"] != "lease expired" {
		t.Fatalf("first phase was not retained: %v", fields)
	}
	for _, id := range []string{"good-retry", "bad-retry"} {
		if fields := submitHash(t, rdb, id); fields["status"] != "retry_wait" {
			t.Fatalf("second phase wrote to %s: %v", id, fields)
		}
	}
	if score, err := rdb.ZScore(ctx, reclaimRetry, "good-retry").Result(); err != nil || score != 1 {
		t.Fatalf("second phase removed a healthy retry: score=%v err=%v", score, err)
	}
	requirePending(t, rdb)
}

func TestReclaimServerUnknownRetryResponse(t *testing.T) {
	for _, mode := range []string{"lost", "malformed", "null"} {
		t.Run(mode, func(t *testing.T) {
			admin := startSubmitRedis(t)
			ctx := context.Background()
			// 缓存两个脚本，使故障命中真正的调度响应。
			if err := redisstore.ReclaimLeases(ctx, admin); err != nil {
				t.Fatal(err)
			}
			if err := redisstore.ScheduleRetries(ctx, admin); err != nil {
				t.Fatal(err)
			}
			seedReclaimCandidate(t, admin, "due", true, 1)
			var armed, dropped atomic.Bool
			opts := *admin.Options()
			opts.PoolSize = 1
			opts.MaxRetries = -1
			opts.Dialer = func(ctx context.Context, network, addr string) (net.Conn, error) {
				conn, err := (&net.Dialer{}).DialContext(ctx, network, addr)
				if err != nil {
					return nil, err
				}
				return &dropSubmitReplyConn{Conn: conn, armed: &armed, dropped: &dropped, expected: []byte(":1\r\n")}, nil
			}
			rdb := redis.NewClient(&opts)
			t.Cleanup(func() { _ = rdb.Close() })
			if err := rdb.Ping(ctx).Err(); err != nil {
				t.Fatal(err)
			}
			// 保留 Claim 循环供故障隔离断言，避免它消费这项待观察的入队结果。
			rdb.AddHook(submitHook{process: func(_ context.Context, cmd redis.Cmder) error {
				cmd.(*redis.Cmd).SetVal([]any{})
				return nil
			}})
			s := integrationServer(t, rdb, WithReclaimInterval(2*time.Second))
			type response struct {
				source       string
				began, ended time.Time
				value        any
				err          error
			}
			responses := make(chan response, 12)
			firstRetry := true
			s.reclaimClient.AddHook(observeRenewHook{process: func(next redis.ProcessHook) redis.ProcessHook {
				return func(ctx context.Context, cmd redis.Cmder) error {
					if cmd.Name() != "evalsha" && cmd.Name() != "eval" {
						return next(ctx, cmd)
					}
					got := response{source: cmd.Args()[3].(string), began: time.Now()}
					inject := firstRetry && got.source == reclaimRetry
					if inject {
						firstRetry = false
						if mode == "lost" {
							armed.Store(true)
						}
					}
					got.err = next(ctx, cmd)
					got.value = cmd.(*redis.Cmd).Val()
					if inject && mode == "malformed" && got.err == nil {
						cmd.(*redis.Cmd).SetVal("malformed")
					}
					got.ended = time.Now()
					responses <- got
					if inject && mode == "null" && got.err == nil {
						return redis.Nil
					}
					return got.err
				}
			}})
			if err := s.HandleFunc("job", func(context.Context, *Task) error { return nil }); err != nil {
				t.Fatal(err)
			}
			if err := s.Start(); err != nil {
				t.Fatal(err)
			}
			if got := awaitReclaimSignal(t, responses); got.source != claimLease || got.err != nil {
				t.Fatalf("first phase: %+v", got)
			}
			first := awaitReclaimSignal(t, responses)
			if first.source != reclaimRetry {
				t.Fatalf("second phase: %+v", first)
			}
			requirePending(t, admin, "due")
			if mode != "lost" {
				awaitReclaimSignal(t, s.reclaimDone)
				awaitReclaimSignal(t, s.claimDone)
				if first.err != nil || first.value != int64(1) || !errors.Is(awaitReclaimSignal(t, s.Errors()), redisstore.ErrReclaimResponse) {
					t.Fatalf("applied response was not handled as fatal and unknown: %+v", first)
				}
				requirePending(t, admin, "due")
				return
			}
			if !dropped.Load() || !errors.Is(first.err, io.EOF) && !errors.Is(first.err, net.ErrClosed) {
				t.Fatalf("successful response was not lost on TCP: %+v", first)
			}
			if got := awaitReclaimSignal(t, responses); got.source != claimLease || got.err != nil || got.began.Sub(first.ended) < 1600*time.Millisecond {
				t.Fatalf("next lease phase replayed or started too soon: %+v", got)
			}
			if got := awaitReclaimSignal(t, responses); got.source != reclaimRetry || got.err != nil || got.value != int64(0) {
				t.Fatalf("retry round did not use current source index: %+v", got)
			}
			requirePending(t, admin, "due")
			if len(s.Errors()) != 0 {
				t.Fatalf("temporary network error became fatal: %v", <-s.Errors())
			}
		})
	}
}
