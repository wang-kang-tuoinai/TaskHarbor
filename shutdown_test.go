package taskq

import (
	"context"
	"errors"
	"fmt"
	"github.com/redis/go-redis/v9"
	"io"
	"testing"
	"testing/synctest"
	"time"
)

func TestShutdownBeforeStart(t *testing.T) {
	s := newTestServer(t)
	var public Server = s
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := public.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	if err := public.Start(); err == nil {
		t.Fatal("Start accepted a stopped server")
	}
	if err := public.HandleFunc("job", func(context.Context, *Task) error { return nil }); err == nil {
		t.Fatal("registration accepted a stopped server")
	}
	if err := public.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestShutdownIdleServer(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s := newStartServer(t, func(_ context.Context, cmd redis.Cmder) error { cmd.(*redis.Cmd).SetVal([]any{}); return nil })
		if err := s.HandleFunc("job", func(context.Context, *Task) error { return nil }); err != nil {
			t.Fatal(err)
		}
		if err := s.Start(); err != nil {
			t.Fatal(err)
		}
		if err := s.Shutdown(context.Background()); err != nil {
			t.Fatal(err)
		}
		for _, done := range []chan struct{}{s.claimDone, s.renewDone, s.reclaimDone} {
			select {
			case <-done:
			default:
				t.Fatal("Shutdown returned before a background loop stopped")
			}
		}
		if err := s.Start(); err == nil {
			t.Fatal("stopped server restarted")
		}
	})
}

func TestShutdownDrainsHandlerAndReportWhileRenewing(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		claims, renews, reclaims := 0, 0, 0
		s := newStartServerWithReclaim(t, func(_ context.Context, cmd redis.Cmder) error {
			if isRenewCommand(cmd) {
				renews++
				cmd.(*redis.Cmd).SetVal(int64(1))
				return nil
			}
			claims++
			if claims == 1 {
				cmd.(*redis.Cmd).SetVal([]any{"task", "job", "", "default", "1", "60000"})
			} else {
				cmd.(*redis.Cmd).SetVal([]any{})
			}
			return nil
		}, func(_ context.Context, cmd redis.Cmder) error {
			reclaims++
			cmd.(*redis.Cmd).SetVal(int64(0))
			return nil
		}, WithConcurrency(1), WithLeaseDuration(6*time.Second), WithRenewInterval(2*time.Second), WithReclaimInterval(2*time.Second))
		releaseHandler, releaseReport := make(chan struct{}), make(chan struct{})
		defer closeShutdownGate(releaseHandler)
		defer closeShutdownGate(releaseReport)
		started, reporting := make(chan struct{}), make(chan struct{})
		if err := s.HandleFunc("job", func(ctx context.Context, task *Task) error {
			close(started)
			<-releaseHandler
			if ctx.Err() != nil {
				t.Error("normal drain canceled Handler")
			}
			return task.SetResult("ok")
		}); err != nil {
			t.Fatal(err)
		}
		s.onComplete = s.complete
		s.reportClient.AddHook(submitHook{process: func(_ context.Context, cmd redis.Cmder) error {
			close(reporting)
			<-releaseReport
			cmd.(*redis.Cmd).SetVal(int64(1))
			return nil
		}})
		if err := s.Start(); err != nil {
			t.Fatal(err)
		}
		<-started
		result := make(chan error, 1)
		go func() { result <- s.Shutdown(context.Background()) }()
		synctest.Wait()
		select {
		case err := <-result:
			t.Fatalf("returned with Handler alive: %v", err)
		default:
		}
		close(releaseHandler)
		<-reporting
		synctest.Wait()
		if len(s.slots) != 0 {
			t.Fatal("Handler did not release its slot")
		}
		time.Sleep(2400 * time.Millisecond)
		synctest.Wait()
		select {
		case err := <-result:
			t.Fatalf("returned with report pending: %v", err)
		default:
		}
		if renews == 0 || reclaims == 0 || s.renewCtx.Err() != nil || s.reclaimCtx.Err() != nil {
			t.Fatalf("drain stopped background work: renews=%d reclaims=%d", renews, reclaims)
		}
		close(releaseReport)
		if err := <-result; err != nil {
			t.Fatal(err)
		}
		if err := s.Shutdown(context.Background()); err != nil {
			t.Fatal(err)
		}
	})
}

func closeShutdownGate(gate chan struct{}) {
	select {
	case <-gate:
	default:
		close(gate)
	}
}

func TestShutdownForceCancelsAllReportsAndWaitsForActualExit(t *testing.T) {
	for _, mode := range []string{"normal", "timeout"} {
		t.Run(mode, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				claims := 0
				timeout := "60000"
				if mode == "timeout" {
					timeout = "1"
				}
				s := newStartServer(t, func(_ context.Context, cmd redis.Cmder) error {
					if isRenewCommand(cmd) {
						cmd.(*redis.Cmd).SetVal(int64(1))
						return nil
					}
					claims++
					if claims == 1 {
						cmd.(*redis.Cmd).SetVal([]any{"task", "job", "", "default", "1", timeout})
					} else {
						cmd.(*redis.Cmd).SetVal([]any{})
					}
					return nil
				}, WithConcurrency(1))
				releaseHandler, releaseReport := make(chan struct{}), make(chan struct{})
				defer closeShutdownGate(releaseHandler)
				defer closeShutdownGate(releaseReport)
				handlerCtx := make(chan context.Context, 1)
				requests := make(chan context.Context, 2)
				if err := s.HandleFunc("job", func(ctx context.Context, _ *Task) error {
					handlerCtx <- ctx
					if mode == "timeout" {
						<-releaseHandler
					}
					return nil
				}); err != nil {
					t.Fatal(err)
				}
				s.onComplete, s.onFailure = s.complete, s.reportFailure
				s.reportClient.AddHook(submitHook{process: func(ctx context.Context, cmd redis.Cmder) error { requests <- ctx; <-releaseReport; return ctx.Err() }})
				if err := s.Start(); err != nil {
					t.Fatal(err)
				}
				hctx := <-handlerCtx
				request := <-requests
				synctest.Wait()
				if mode == "timeout" && len(s.snapshotExecutions()) != 0 {
					t.Fatal("timed-out execution still eligible to renew")
				}
				ctx, cancel := context.WithTimeout(context.Background(), time.Second)
				defer cancel()
				result := make(chan error, 1)
				go func() { result <- s.Shutdown(ctx) }()
				synctest.Wait()
				time.Sleep(time.Second)
				if err := <-result; !errors.Is(err, context.DeadlineExceeded) {
					t.Fatalf("Shutdown=%v", err)
				}
				if request.Err() == nil {
					t.Fatal("force did not cancel in-flight report context")
				}
				if err := s.Shutdown(context.Background()); !errors.Is(err, ErrShutdownInProgress) {
					t.Fatalf("still-live work marked stopped: %v", err)
				}
				if mode == "timeout" && len(s.slots) != 1 {
					t.Fatal("force released a living Handler's slot")
				}
				close(releaseReport)
				synctest.Wait()
				if hctx.Err() == nil {
					t.Fatal("execution context not cleaned up after report exit")
				}
				if len(requests) != 0 {
					t.Fatal("forced report retried")
				}
				if mode == "timeout" {
					if err := s.Shutdown(context.Background()); !errors.Is(err, ErrShutdownInProgress) {
						t.Fatalf("Handler still alive: %v", err)
					}
				}
				close(releaseHandler)
				synctest.Wait()
				canceled, stop := context.WithCancel(context.Background())
				stop()
				if err := s.Shutdown(canceled); err != nil {
					t.Fatal(err)
				}
				if len(s.slots) != 0 {
					t.Fatal("actual Handler exit did not return slot")
				}
			})
		})
	}
}

func TestShutdownLateClaimFollowsDrainOrForce(t *testing.T) {
	for _, mode := range []string{"drain", "deadline", "cancel", "pre-canceled"} {
		t.Run(mode, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				claimStarted, releaseClaim := make(chan struct{}), make(chan struct{})
				releaseHandler := make(chan struct{})
				defer closeShutdownGate(releaseClaim)
				defer closeShutdownGate(releaseHandler)
				s := newStartServer(t, func(_ context.Context, cmd redis.Cmder) error {
					close(claimStarted)
					<-releaseClaim // 模拟取消后仍需等待 socket 的明确成功响应。
					cmd.(*redis.Cmd).SetVal([]any{"task", "job", "", "default", "2", "60000"})
					return nil
				}, WithConcurrency(1))
				started := make(chan context.Context, 1)
				if err := s.HandleFunc("job", func(ctx context.Context, task *Task) error {
					if task.Attempts != 2 {
						t.Error("late Claim changed attempts")
					}
					started <- ctx
					<-releaseHandler
					return nil
				}); err != nil {
					t.Fatal(err)
				}
				if err := s.Start(); err != nil {
					t.Fatal(err)
				}
				<-claimStarted
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				if mode == "deadline" {
					var stop context.CancelFunc
					ctx, stop = context.WithTimeout(ctx, time.Second)
					defer stop()
				}
				if mode == "pre-canceled" {
					cancel()
				}
				result := make(chan error, 1)
				go func() { result <- s.Shutdown(ctx) }()
				synctest.Wait()
				if mode == "drain" {
					canceled, stop := context.WithCancel(context.Background())
					stop()
					if err := s.Shutdown(canceled); !errors.Is(err, ErrShutdownInProgress) {
						t.Fatalf("second call=%v", err)
					}
					select {
					case <-s.abort:
						t.Fatal("second caller forced the first caller's drain")
					default:
					}
					close(releaseClaim)
					hctx := <-started
					if hctx.Err() != nil {
						t.Fatal("late accepted Handler canceled during drain")
					}
					synctest.Wait()
					select {
					case err := <-result:
						t.Fatalf("drain returned with Handler alive: %v", err)
					default:
					}
					close(releaseHandler)
					if err := <-result; err != nil {
						t.Fatal(err)
					}
				} else {
					if mode == "deadline" {
						time.Sleep(time.Second)
					} else {
						cancel()
					}
					want := context.Canceled
					if mode == "deadline" {
						want = context.DeadlineExceeded
					}
					if err := <-result; !errors.Is(err, want) {
						t.Fatalf("Shutdown=%v, want %v", err, want)
					}
					if err := s.Shutdown(context.Background()); !errors.Is(err, ErrShutdownInProgress) {
						t.Fatalf("blocked Claim marked stopped: %v", err)
					}
					select {
					case <-s.claimDone:
						t.Fatal("test did not retain in-flight Claim")
					default:
					}
					close(releaseClaim)
					synctest.Wait()
					select {
					case <-started:
						t.Fatal("forced late Claim started Handler")
					default:
					}
					if err := s.Shutdown(context.Background()); err != nil {
						t.Fatal(err)
					}
				}
				if len(s.slots) != 0 || len(s.snapshotExecutions()) != 0 || len(s.Errors()) != 0 {
					t.Fatal("late Claim leaked local work or reported abort as failure")
				}
			})
		})
	}
}

func TestShutdownCancelsRunningHandlerWithoutFabricatingFailure(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s := newStartServer(t, func(_ context.Context, cmd redis.Cmder) error {
			cmd.(*redis.Cmd).SetVal([]any{"task", "job", "", "default", "1", "60000"})
			return nil
		}, WithConcurrency(1))
		started, exited := make(chan context.Context, 1), make(chan struct{})
		release := make(chan struct{})
		defer closeShutdownGate(release)
		if err := s.HandleFunc("job", func(ctx context.Context, _ *Task) error { started <- ctx; <-release; close(exited); return nil }); err != nil {
			t.Fatal(err)
		}
		s.onComplete = func(context.Context, *execution, executionKey, []byte) { t.Error("abort fabricated complete") }
		s.onFailure = func(context.Context, *execution, executionKey, error) { t.Error("abort fabricated failure") }
		if err := s.Start(); err != nil {
			t.Fatal(err)
		}
		hctx := <-started
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		if err := s.Shutdown(ctx); !errors.Is(err, context.Canceled) {
			t.Fatalf("pre-canceled running Shutdown=%v", err)
		}
		synctest.Wait()
		if hctx.Err() == nil || len(s.slots) != 1 || len(s.snapshotExecutions()) != 0 {
			t.Fatal("abort did not cancel supervision while preserving Handler slot")
		}
		select {
		case <-exited:
			t.Fatal("noncooperative Handler exited prematurely")
		default:
		}
		if err := s.Shutdown(context.Background()); !errors.Is(err, ErrShutdownInProgress) {
			t.Fatalf("living Handler marked stopped: %v", err)
		}
		close(release)
		synctest.Wait()
		if err := s.Shutdown(ctx); err != nil {
			t.Fatal(err)
		}
	})
}

func TestShutdownBudgetIncludesBackgroundLoopExit(t *testing.T) {
	for _, mode := range []string{"renew", "reclaim"} {
		t.Run(mode, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				blocked, release := make(chan struct{}), make(chan struct{})
				releaseHandler := make(chan struct{})
				defer closeShutdownGate(release)
				defer closeShutdownGate(releaseHandler)
				claimed := false
				process := func(_ context.Context, cmd redis.Cmder) error {
					if isRenewCommand(cmd) {
						close(blocked)
						<-release
						cmd.(*redis.Cmd).SetVal(int64(1))
						return nil
					}
					if mode == "renew" && !claimed {
						claimed = true
						cmd.(*redis.Cmd).SetVal([]any{"task", "job", "", "default", "1", "60000"})
					} else {
						cmd.(*redis.Cmd).SetVal([]any{})
					}
					return nil
				}
				var reclaim func(context.Context, redis.Cmder) error
				if mode == "reclaim" {
					reclaim = func(_ context.Context, cmd redis.Cmder) error {
						close(blocked)
						<-release
						cmd.(*redis.Cmd).SetVal(int64(0))
						return nil
					}
				}
				s := newStartServerWithReclaim(t, process, reclaim, WithConcurrency(1), WithLeaseDuration(6*time.Second), WithRenewInterval(2*time.Second), WithReclaimInterval(2*time.Second))
				if err := s.HandleFunc("job", func(context.Context, *Task) error { <-releaseHandler; return nil }); err != nil {
					t.Fatal(err)
				}
				if err := s.Start(); err != nil {
					t.Fatal(err)
				}
				<-blocked
				close(releaseHandler)
				synctest.Wait()
				ctx, cancel := context.WithTimeout(context.Background(), time.Second)
				defer cancel()
				result := make(chan error, 1)
				go func() { result <- s.Shutdown(ctx) }()
				synctest.Wait()
				time.Sleep(time.Second)
				if err := <-result; !errors.Is(err, context.DeadlineExceeded) {
					t.Fatalf("Shutdown=%v", err)
				}
				if err := s.Shutdown(context.Background()); !errors.Is(err, ErrShutdownInProgress) {
					t.Fatalf("blocked background loop marked stopped: %v", err)
				}
				close(release)
				synctest.Wait()
				if err := s.Shutdown(context.Background()); err != nil {
					t.Fatal(err)
				}
			})
		})
	}
}

func TestShutdownStartRaceAndFailedChecks(t *testing.T) {
	for _, register := range []bool{false, true} {
		t.Run(fmt.Sprint("registered=", register), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				for range 32 {
					s := newStartServer(t, func(_ context.Context, cmd redis.Cmder) error { cmd.(*redis.Cmd).SetVal([]any{}); return nil })
					if register {
						if err := s.HandleFunc("job", func(context.Context, *Task) error { return nil }); err != nil {
							t.Fatal(err)
						}
					}
					start, stop := make(chan error, 1), make(chan error, 1)
					go func() { start <- s.Start() }()
					go func() { stop <- s.Shutdown(context.Background()) }()
					startErr := <-start
					if err := <-stop; err != nil {
						t.Fatal(err)
					}
					if !register && startErr == nil {
						t.Fatal("Start without handlers succeeded")
					}
					synctest.Wait()
					if err := s.Start(); err == nil {
						t.Fatal("Start restarted a stopped server")
					}
					if err := s.Shutdown(context.Background()); err != nil {
						t.Fatal(err)
					}
					s.mu.Lock()
					status := s.status
					s.mu.Unlock()
					if status != serverStopped {
						t.Fatal("concurrent Start resurrected server")
					}
					if startErr == nil {
						for _, done := range []chan struct{}{s.claimDone, s.renewDone, s.reclaimDone} {
							select {
							case <-done:
							default:
								t.Fatal("Shutdown returned before started loop exited")
							}
						}
					}
				}
			})
		})
	}
}

func TestShutdownPreCanceledIdleServerAlwaysForces(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		for range 32 {
			s := newStartServer(t, func(_ context.Context, cmd redis.Cmder) error { cmd.(*redis.Cmd).SetVal([]any{}); return nil })
			if err := s.HandleFunc("job", func(context.Context, *Task) error { return nil }); err != nil {
				t.Fatal(err)
			}
			if err := s.Start(); err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			if err := s.Shutdown(ctx); !errors.Is(err, context.Canceled) {
				t.Fatalf("running pre-canceled Shutdown=%v", err)
			}
			select {
			case <-s.abort:
			default:
				t.Fatal("pre-canceled first caller did not request force")
			}
			synctest.Wait()
			if err := s.Shutdown(ctx); err != nil {
				t.Fatal(err)
			}
		}
	})
}

func TestShutdownWaitsForHandlerAfterAuthorityLost(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s := newStartServer(t, func(_ context.Context, cmd redis.Cmder) error {
			if isRenewCommand(cmd) {
				cmd.(*redis.Cmd).SetVal(int64(0))
			} else {
				cmd.(*redis.Cmd).SetVal([]any{"task", "job", "", "default", "1", "60000"})
			}
			return nil
		}, WithConcurrency(1), WithLeaseDuration(6*time.Second), WithRenewInterval(2*time.Second))
		started := make(chan context.Context, 1)
		release := make(chan struct{})
		defer closeShutdownGate(release)
		if err := s.HandleFunc("job", func(ctx context.Context, _ *Task) error { started <- ctx; <-release; return nil }); err != nil {
			t.Fatal(err)
		}
		if err := s.Start(); err != nil {
			t.Fatal(err)
		}
		hctx := <-started
		time.Sleep(2 * time.Second)
		synctest.Wait()
		if !errors.Is(context.Cause(hctx), errExecutionLost) || len(s.snapshotExecutions()) != 0 {
			t.Fatal("test did not revoke authority")
		}
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := s.Shutdown(ctx); !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("living Handler omitted from drain: %v", err)
		}
		if len(s.slots) != 1 {
			t.Fatal("authority loss returned living Handler's slot")
		}
		close(release)
		synctest.Wait()
		if err := s.Shutdown(context.Background()); err != nil {
			t.Fatal(err)
		}
	})
}

func TestShutdownCompletesAfterFatalLoopOrReportExhaustion(t *testing.T) {
	for _, mode := range []string{"claim-fatal", "renew-fatal", "reclaim-fatal", "report-exhaustion", "unknown-handler"} {
		t.Run(mode, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				claimed := false
				typ := "job"
				if mode == "unknown-handler" {
					typ = "missing"
				}
				process := func(_ context.Context, cmd redis.Cmder) error {
					if isRenewCommand(cmd) {
						return startRedisError("NOPERM renew stopped")
					}
					if mode == "claim-fatal" {
						return startRedisError("NOPERM claim stopped")
					}
					if !claimed {
						claimed = true
						cmd.(*redis.Cmd).SetVal([]any{"task", typ, "", "default", "1", "60000"})
					} else {
						cmd.(*redis.Cmd).SetVal([]any{})
					}
					return nil
				}
				var reclaim func(context.Context, redis.Cmder) error
				if mode == "reclaim-fatal" {
					reclaim = func(context.Context, redis.Cmder) error { return startRedisError("NOPERM reclaim stopped") }
				}
				s := newStartServerWithReclaim(t, process, reclaim, WithConcurrency(1), WithLeaseDuration(6*time.Second), WithRenewInterval(2*time.Second), WithReclaimInterval(2*time.Second))
				release := make(chan struct{})
				defer closeShutdownGate(release)
				if err := s.HandleFunc("job", func(context.Context, *Task) error {
					if mode == "renew-fatal" || mode == "reclaim-fatal" {
						<-release
					}
					return nil
				}); err != nil {
					t.Fatal(err)
				}
				if mode == "report-exhaustion" {
					s.onComplete = s.complete
				}
				processed := make(chan struct{})
				reportCalls := 0
				if mode == "report-exhaustion" {
					s.reportClient.AddHook(submitHook{process: func(context.Context, redis.Cmder) error {
						if reportCalls == 0 {
							close(processed)
						}
						reportCalls++
						return io.EOF
					}})
				} else if mode == "unknown-handler" {
					s.onFailure = func(context.Context, *execution, executionKey, error) { close(processed) }
				}
				if err := s.Start(); err != nil {
					t.Fatal(err)
				}
				if mode == "renew-fatal" || mode == "reclaim-fatal" {
					<-s.Errors()
					close(release)
				}
				if mode == "claim-fatal" {
					<-s.Errors()
				}
				if mode == "report-exhaustion" || mode == "unknown-handler" {
					<-processed
				}
				if err := s.Shutdown(context.Background()); err != nil {
					t.Fatal(err)
				}
				if len(s.slots) != 0 || len(s.snapshotExecutions()) != 0 {
					t.Fatal("Shutdown omitted final local cleanup")
				}
				if mode == "report-exhaustion" && reportCalls != 3 {
					t.Fatalf("report requests=%d, want exhausted budget 3", reportCalls)
				}
			})
		})
	}
}

func TestShutdownCompletionAndDeadlineCanBecomeReadyTogether(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		for range 32 {
			s := newStartServer(t, func(_ context.Context, cmd redis.Cmder) error {
				cmd.(*redis.Cmd).SetVal([]any{"task", "job", "", "default", "1", "60000"})
				return nil
			}, WithConcurrency(1))
			started := make(chan struct{})
			if err := s.HandleFunc("job", func(context.Context, *Task) error { close(started); time.Sleep(time.Second); return nil }); err != nil {
				t.Fatal(err)
			}
			if err := s.Start(); err != nil {
				t.Fatal(err)
			}
			<-started
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			err := s.Shutdown(ctx)
			cancel()
			if err != nil && !errors.Is(err, context.DeadlineExceeded) {
				t.Fatal(err)
			}
			if err == nil {
				select {
				case <-s.shutdownDone:
				default:
					t.Fatal("nil returned without complete shutdown")
				}
			}
			synctest.Wait()
			if err := s.Shutdown(ctx); err != nil {
				t.Fatal(err)
			}
		}
	})
}
