package taskq

import (
	"context"
	"errors"
	"io"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/wang-kang-tuoinai/TaskHarbor/internal/redisstore"
)

func TestReclaimStartWaitsBetweenSerialRounds(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		type request struct {
			source string
			at     time.Time
			budget time.Duration
		}
		requests := make(chan request, 10)
		releaseLease := make(chan struct{})
		releaseRetry := make(chan struct{})
		requestCount := 0
		s := newStartServerWithReclaim(t, func(_ context.Context, cmd redis.Cmder) error {
			cmd.(*redis.Cmd).SetVal([]any{})
			return nil
		}, func(ctx context.Context, cmd redis.Cmder) error {
			at := time.Now()
			deadline, _ := ctx.Deadline()
			select {
			case requests <- request{cmd.Args()[3].(string), at, deadline.Sub(at)}:
			case <-ctx.Done():
				return ctx.Err()
			}
			requestCount++
			count := int64(0)
			switch requestCount {
			case 1, 2:
				release := releaseLease
				if requestCount == 2 {
					release = releaseRetry
				}
				select {
				case <-release:
				case <-ctx.Done():
					return ctx.Err()
				}
			case 3, 4:
				count = 100 // 满批也只执行一次，下一轮仍须等待。
			}
			cmd.(*redis.Cmd).SetVal(count)
			return nil
		}, WithReclaimInterval(2*time.Second))
		if err := s.HandleFunc("job", func(context.Context, *Task) error { return nil }); err != nil {
			t.Fatal(err)
		}
		started := time.Now()
		if err := s.Start(); err != nil {
			t.Fatal(err)
		}
		time.Sleep(1599 * time.Millisecond)
		synctest.Wait()
		if len(requests) != 0 {
			t.Fatal("first scan did not wait at least 80% of the interval")
		}
		await := func() request {
			t.Helper()
			select {
			case got := <-requests:
				return got
			case <-time.After(2401 * time.Millisecond):
				t.Fatal("reclaim request did not arrive")
				return request{}
			}
		}
		checkLease := func(req request, from time.Time) {
			t.Helper()
			delay := req.at.Sub(from)
			if req.source != "taskq:default:lease" || req.budget != 3*time.Second || delay < 1600*time.Millisecond || delay > 2400*time.Millisecond {
				t.Fatalf("lease request=%+v delay=%v", req, delay)
			}
		}
		checkLease(await(), started)
		time.Sleep(2500 * time.Millisecond)
		synctest.Wait()
		if len(requests) != 0 {
			t.Fatal("reclaim overlapped an in-flight script")
		}
		leaseFinished := time.Now()
		close(releaseLease)
		if got := await(); got.source != "taskq:default:retry" || got.budget != 3*time.Second || !got.at.Equal(leaseFinished) {
			t.Fatalf("retry did not get its own request budget: %+v", got)
		}
		time.Sleep(2500 * time.Millisecond)
		synctest.Wait()
		if len(requests) != 0 {
			t.Fatal("reclaim overlapped an in-flight retry phase")
		}
		finished := time.Now()
		close(releaseRetry)
		time.Sleep(1599 * time.Millisecond)
		synctest.Wait()
		if len(requests) != 0 {
			t.Fatal("reclaim replayed a missed cycle")
		}
		checkLease(await(), finished)
		if got := await(); got.source != "taskq:default:retry" {
			t.Fatalf("unexpected second stage: %+v", got)
		}
		time.Sleep(1599 * time.Millisecond)
		synctest.Wait()
		if len(requests) != 0 {
			t.Fatal("full batches caused an immediate replay")
		}
	})
}

func TestReclaimTransientLeaseErrorDefersBothPhases(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		requests := make(chan string, 8)
		fail := true
		s := newStartServerWithReclaim(t, func(_ context.Context, cmd redis.Cmder) error {
			cmd.(*redis.Cmd).SetVal([]any{})
			return nil
		}, func(ctx context.Context, cmd redis.Cmder) error {
			select {
			case requests <- cmd.Args()[3].(string):
			case <-ctx.Done():
				return ctx.Err()
			}
			if fail {
				fail = false
				return io.EOF
			}
			cmd.(*redis.Cmd).SetVal(int64(0))
			return nil
		}, WithReclaimInterval(2*time.Second))
		if err := s.HandleFunc("job", func(context.Context, *Task) error { return nil }); err != nil {
			t.Fatal(err)
		}
		if err := s.Start(); err != nil {
			t.Fatal(err)
		}
		for i, source := range []string{"taskq:default:lease", "taskq:default:lease", "taskq:default:retry"} {
			select {
			case got := <-requests:
				if got != source {
					t.Fatalf("phase %d requested %s, want %s", i, got, source)
				}
			case <-time.After(2500 * time.Millisecond):
				t.Fatalf("phase %d not attempted", i)
			}
			if i == 0 {
				time.Sleep(1599 * time.Millisecond)
				synctest.Wait()
				if len(requests) != 0 || len(s.Errors()) != 0 {
					t.Fatal("temporary failure triggered a request or fatal error before the next round")
				}
			}
		}
	})
}

func TestReclaimFatalStopsClaimButKeepsHealthyExecution(t *testing.T) {
	for _, priorFatal := range []bool{false, true} {
		name := "first-fatal"
		if priorFatal {
			name = "notification-already-pending"
		}
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				started := make(chan context.Context, 1)
				release := make(chan struct{})
				renewed := make(chan struct{}, 1)
				reported := make(chan struct{}, 1)
				prior := startRedisError("NOPERM earlier claim failure")
				claims, reclaims := 0, 0
				s := newStartServerWithReclaim(t, func(_ context.Context, cmd redis.Cmder) error {
					if isRenewCommand(cmd) {
						cmd.(*redis.Cmd).SetVal(int64(1))
						renewed <- struct{}{}
						return nil
					}
					claims++
					if claims == 1 {
						cmd.(*redis.Cmd).SetVal([]any{"healthy", "job", "", "default", "1", "60000"})
					} else if priorFatal {
						return prior
					} else {
						cmd.(*redis.Cmd).SetVal([]any{})
					}
					return nil
				}, func(_ context.Context, cmd redis.Cmder) error {
					reclaims++
					if cmd.Args()[3] != "taskq:default:lease" {
						t.Error("retry ran after the lease phase failed")
					}
					return startRedisError("TASKQ_RECLAIM_INCONSISTENT task broken: invalid task identity")
				}, WithConcurrency(2), WithReclaimInterval(2*time.Second))
				s.onComplete = s.complete
				s.reportClient.AddHook(submitHook{process: func(_ context.Context, cmd redis.Cmder) error {
					cmd.(*redis.Cmd).SetVal(int64(1))
					reported <- struct{}{}
					return nil
				}})
				t.Cleanup(func() {
					select {
					case <-release:
					default:
						close(release)
					}
				})
				if err := s.HandleFunc("job", func(ctx context.Context, _ *Task) error { started <- ctx; <-release; return nil }); err != nil {
					t.Fatal(err)
				}
				if err := s.Start(); err != nil {
					t.Fatal(err)
				}
				handlerCtx := <-started
				time.Sleep(2500 * time.Millisecond)
				synctest.Wait()
				for name, done := range map[string]<-chan struct{}{"Claim": s.claimDone, "Reclaim": s.reclaimDone} {
					select {
					case <-done:
					default:
						t.Fatalf("%s still running after fatal error", name)
					}
				}
				if reclaims != 1 || len(s.Errors()) != 1 || handlerCtx.Err() != nil {
					t.Fatalf("reclaims=%d errors=%d handler=%v", reclaims, len(s.Errors()), handlerCtx.Err())
				}
				err := <-s.Errors()
				if priorFatal && !errors.Is(err, prior) || !priorFatal && (!errors.Is(err, redisstore.ErrReclaimInconsistent) || !strings.Contains(err.Error(), "reclaim leases")) {
					t.Fatalf("unexpected first notification: %v", err)
				}
				time.Sleep(3500 * time.Millisecond)
				synctest.Wait()
				if len(renewed) != 1 || handlerCtx.Err() != nil {
					t.Fatal("fatal reclaim interrupted healthy renewal or handler")
				}
				close(release)
				synctest.Wait()
				if len(reported) != 1 || len(s.Errors()) != 0 {
					t.Fatal("healthy completion did not report, or extra fatal error was sent")
				}
			})
		})
	}
}
