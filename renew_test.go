package taskq

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/redis/go-redis/v9"
)

func TestNewServerRequiresContextDeadlines(t *testing.T) {
	rdb := redis.NewClient(&redis.Options{Addr: "127.0.0.1:1", MaxRetries: -1})
	t.Cleanup(func() { _ = rdb.Close() })
	if s, err := NewServer(rdb); s != nil || err == nil {
		t.Fatalf("server=%v err=%v", s, err)
	}
	if rdb.Options().ContextTimeoutEnabled {
		t.Fatal("constructor mutated caller options")
	}
}

func isRenewCommand(cmd redis.Cmder) bool {
	if cmd.Name() != "evalsha" && cmd.Name() != "eval" {
		return false
	}
	args := cmd.Args()
	return len(args) > 3 && strings.HasPrefix(args[3].(string), "taskq:meta:")
}

func TestRenewContinuesAfterClaimStops(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		renewed := make(chan executionKey, 8)
		started := make(chan context.Context, 2)
		release := make(chan struct{})
		claims := 0
		s := newStartServer(t, func(_ context.Context, cmd redis.Cmder) error {
			if isRenewCommand(cmd) {
				renewed <- executionKey{taskID: cmd.Args()[6].(string), attemptToken: cmd.Args()[7].(string)}
				cmd.(*redis.Cmd).SetVal(int64(1))
				return nil
			}
			claims++
			if claims > 2 {
				return startRedisError("NOPERM stop Claim only")
			}
			cmd.(*redis.Cmd).SetVal([]any{fmt.Sprintf("task-%d", claims), "job", "", "default", "1", "10000"})
			return nil
		}, WithConcurrency(3), WithLeaseDuration(6*time.Second), WithRenewInterval(2*time.Second))
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
		for range 2 {
			<-started
		}
		synctest.Wait()
		select {
		case <-s.claimDone:
		default:
			t.Fatal("claim did not stop")
		}
		time.Sleep(1999 * time.Millisecond)
		synctest.Wait()
		if len(renewed) != 0 {
			t.Fatal("renewed before interval")
		}
		time.Sleep(time.Millisecond)
		synctest.Wait()
		if len(renewed) != 2 {
			t.Fatalf("renew requests=%d", len(renewed))
		}
		seen := map[string]bool{}
		for range 2 {
			key := <-renewed
			if key.attemptToken == "" || seen[key.taskID] {
				t.Fatal("bad renewal identity")
			}
			seen[key.taskID] = true
		}
		time.Sleep(2 * time.Second)
		synctest.Wait()
		if len(renewed) != 2 || len(s.snapshotExecutions()) != 2 || len(s.slots) != 2 {
			t.Fatal("Claim stop affected healthy Renew or executions")
		}
		close(release)
		synctest.Wait()
		time.Sleep(2 * time.Second)
		synctest.Wait()
		if len(renewed) != 2 || len(s.slots) != 0 || len(s.snapshotExecutions()) != 0 {
			t.Fatal("cleared executions were renewed")
		}
	})
}

func TestExecutionKnownLossSuppressesAllReporting(t *testing.T) {
	for _, cause := range []error{errExecutionLost, errExecutionCanceled, errExecutionInconsistent} {
		for _, mode := range []string{"success", "error", "panic", "timeout", "unknown"} {
			t.Run(cause.Error()+"/"+mode, func(t *testing.T) {
				synctest.Test(t, func(t *testing.T) {
					s := newTestServer(t, WithConcurrency(1))
					parent, revoke := context.WithCancelCause(context.Background())
					defer revoke(nil)
					release := make(chan struct{})
					started := make(chan context.Context, 1)
					t.Cleanup(func() {
						select {
						case <-release:
						default:
							close(release)
						}
					})
					if mode != "unknown" {
						if err := s.HandleFunc("job", func(ctx context.Context, _ *Task) error {
							started <- ctx
							<-release
							if mode == "panic" {
								panic("late panic")
							}
							if mode == "error" {
								return errors.New("late business error")
							}
							return nil
						}); err != nil {
							t.Fatal(err)
						}
					} else {
						revoke(cause)
					}
					reports := make(chan struct{}, 4)
					s.onComplete = func(ctx context.Context, record *execution, _ executionKey, _ []byte) { reports <- struct{}{} }
					s.onFailure = func(ctx context.Context, record *execution, _ executionKey, _ error) { reports <- struct{}{} }
					s.slots <- struct{}{}
					done, err := s.acceptExecution(parent, &Task{ID: "task", Type: "job", Attempts: 1}, executionKey{taskID: "task", attemptToken: "token"}, time.Second)
					if err != nil {
						t.Fatal(err)
					}
					if mode != "unknown" {
						ctx := <-started
						revoke(cause)
						if !errors.Is(context.Cause(ctx), cause) {
							t.Fatal("cancellation cause lost")
						}
					}
					if mode != "timeout" {
						close(release)
					} else {
						time.Sleep(time.Second)
					}
					synctest.Wait()
					select {
					case <-done:
					default:
						t.Fatal("known cancellation did not finish supervision")
					}
					if len(reports) != 0 || len(s.snapshotExecutions()) != 0 {
						t.Fatal("known loss reported or retained renewal eligibility")
					}
					if mode == "timeout" {
						if len(s.slots) != 1 {
							t.Fatal("live handler lost its slot")
						}
						close(release)
						synctest.Wait()
					}
					if len(s.slots) != 0 || len(reports) != 0 {
						t.Fatal("late result blocked, reported or leaked slot")
					}
				})
			})
		}
	}
}

func TestExecutionKnownLossAtReportingBoundary(t *testing.T) {
	for _, mode := range []string{"success", "error", "panic", "timeout"} {
		t.Run(mode, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				s := newTestServer(t, WithConcurrency(1))
				key := executionKey{taskID: "task", attemptToken: "token"}
				started := make(chan context.Context, 1)
				release := make(chan struct{})
				allowCancel := make(chan struct{})
				known := make(chan struct{})
				revoked := make(chan struct{})
				t.Cleanup(func() {
					for _, gate := range []chan struct{}{release, allowCancel} {
						select {
						case <-gate:
						default:
							close(gate)
						}
					}
				})
				if err := s.HandleFunc("job", func(ctx context.Context, _ *Task) error {
					started <- ctx
					<-release
					switch mode {
					case "error":
						return errors.New("late failure")
					case "panic":
						panic("late panic")
					}
					return nil
				}); err != nil {
					t.Fatal(err)
				}
				reports := make(chan struct{}, 4)
				s.onComplete = func(ctx context.Context, record *execution, _ executionKey, _ []byte) { reports <- struct{}{} }
				s.onFailure = func(ctx context.Context, record *execution, _ executionKey, _ error) { reports <- struct{}{} }
				s.slots <- struct{}{}
				done, err := s.acceptExecution(context.Background(), &Task{ID: "task", Type: "job"}, key, time.Second)
				if err != nil {
					t.Fatal(err)
				}
				ctx := <-started
				record, ok := s.getExecution(key)
				if !ok {
					t.Fatal("execution not registered")
				}
				cancelReports := record.cancelReports
				// 暂停真实撤销在停止上报之后、取消 Handler 之前。结果或超时分支必须
				// 检查 reportCtx，因为此时 Handler 的 ctx.Done 尚未就绪。
				record.cancelReports = func() { cancelReports(); close(known); <-allowCancel }
				go func() { defer close(revoked); s.revokeExecution(key, record, errExecutionLost) }()
				<-known
				if record.reportCtx.Err() != context.Canceled || ctx.Err() != nil {
					t.Fatal("reporting must stop before handler cancellation")
				}
				if mode == "timeout" {
					time.Sleep(time.Second)
				} else {
					close(release)
				}
				synctest.Wait()
				select {
				case <-done:
				default:
					t.Fatal("supervisor did not finish selected branch")
				}
				if len(reports) != 0 {
					t.Fatal("known loss bypassed reporting guard")
				}
				if mode == "timeout" {
					if !errors.Is(context.Cause(ctx), context.DeadlineExceeded) || len(s.slots) != 1 {
						t.Fatal("timeout was not first cause or live slot released")
					}
				}
				close(allowCancel)
				<-revoked
				if mode == "timeout" {
					close(release)
				}
				synctest.Wait()
				if len(s.slots) != 0 || len(reports) != 0 {
					t.Fatal("late result reported or leaked slot")
				}
			})
		})
	}
}
