package taskq

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/wang-kang-tuoinai/TaskHarbor/internal/redisstore"
)

func TestRenewFailureScopes(t *testing.T) {
	for _, mode := range []string{"rejected", "inconsistent", "global", "invalid", "network"} {
		t.Run(mode, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				requests := make(chan string, 32)
				started := make(chan struct {
					id  string
					ctx context.Context
				}, 2)
				reports := make(chan string, 4)
				release := make(chan struct{})
				var claims atomic.Int32
				s := newStartServer(t, func(_ context.Context, cmd redis.Cmder) error {
					if isRenewCommand(cmd) {
						id := cmd.Args()[6].(string)
						requests <- id
						if mode == "global" {
							return startRedisError("NOPERM renew forbidden")
						}
						if mode == "invalid" {
							cmd.(*redis.Cmd).SetVal("malformed")
							return nil
						}
						if id == "bad" {
							switch mode {
							case "rejected":
								cmd.(*redis.Cmd).SetVal(int64(0))
								return nil
							case "inconsistent":
								return startRedisError("TASKQ_RENEW_INCONSISTENT task bad: missing lease")
							case "network":
								return io.EOF
							}
						}
						cmd.(*redis.Cmd).SetVal(int64(1))
						return nil
					}
					n := claims.Add(1)
					if n <= 2 {
						id := "healthy"
						if n == 1 {
							id = "bad"
						}
						cmd.(*redis.Cmd).SetVal([]any{id, "job", "", "default", "1", "10000"})
					} else {
						cmd.(*redis.Cmd).SetVal([]any{})
					}
					return nil
				}, WithConcurrency(3), WithLeaseDuration(6*time.Second), WithRenewInterval(2*time.Second))
				t.Cleanup(func() {
					select {
					case <-release:
					default:
						close(release)
					}
				})
				if err := s.HandleFunc("job", func(ctx context.Context, task *Task) error {
					started <- struct {
						id  string
						ctx context.Context
					}{task.ID, ctx}
					<-release
					return nil
				}); err != nil {
					t.Fatal(err)
				}
				s.onComplete = func(ctx context.Context, record *execution, key executionKey, _ []byte) { reports <- key.taskID }
				s.onFailure = func(ctx context.Context, record *execution, _ executionKey, _ error) {
					t.Error("renew cancellation became business failure")
				}
				if err := s.Start(); err != nil {
					t.Fatal(err)
				}
				contexts := map[string]context.Context{}
				for range 2 {
					item := <-started
					contexts[item.id] = item.ctx
				}
				synctest.Wait()
				time.Sleep(2 * time.Second)
				synctest.Wait()
				global := mode == "global" || mode == "invalid"
				fatal := global || mode == "inconsistent"
				wantRecords := 2
				if mode == "rejected" || mode == "inconsistent" {
					wantRecords = 1
					if contexts["bad"].Err() == nil || contexts["healthy"].Err() != nil {
						t.Fatal("per-task cancellation scope incorrect")
					}
				}
				if global || mode == "network" {
					for _, ctx := range contexts {
						if ctx.Err() != nil {
							t.Fatal("global/network failure canceled handler")
						}
					}
				}
				if len(s.snapshotExecutions()) != wantRecords || len(s.slots) != 2 {
					t.Fatal("renew failure changed eligibility or live slots incorrectly")
				}
				if fatal {
					select {
					case <-s.claimDone:
					default:
						t.Fatal("fatal renew did not stop Claim")
					}
					if len(s.Errors()) != 1 {
						t.Fatal("fatal renew not notified")
					}
					err := <-s.Errors()
					if mode == "inconsistent" && (!errors.Is(err, redisstore.ErrRenewInconsistent) || !strings.Contains(err.Error(), "bad")) {
						t.Fatal(err)
					}
				} else if len(s.Errors()) != 0 {
					t.Fatal("normal rejection/network error reported fatal")
				}
				counts := map[string]int{}
				for len(requests) > 0 {
					counts[<-requests]++
				}
				if global {
					if counts["bad"]+counts["healthy"] != 1 {
						t.Fatal("global fault did not stop scan")
					}
				} else if counts["bad"] != 1 || counts["healthy"] != 1 {
					t.Fatalf("scan counts=%v", counts)
				}
				beforeClaims := claims.Load()
				time.Sleep(2 * time.Second)
				synctest.Wait()
				if global {
					select {
					case <-s.renewDone:
					default:
						t.Fatal("global fault did not stop Renew")
					}
					if len(requests) != 0 {
						t.Fatal("global renew resumed")
					}
				} else {
					for len(requests) > 0 {
						counts[<-requests]++
					}
					wantBad := 1
					if mode == "network" {
						wantBad = 2
					}
					if counts["bad"] != wantBad || counts["healthy"] != 2 {
						t.Fatalf("later scan counts=%v", counts)
					}
				}
				if !fatal && claims.Load() <= beforeClaims {
					t.Fatal("normal rejection/network failure stopped Claim")
				}
				close(release)
				synctest.Wait()
				wantReports := 2
				if mode == "rejected" || mode == "inconsistent" {
					wantReports = 1
				}
				if len(reports) != wantReports || len(s.slots) != 0 || len(s.snapshotExecutions()) != 0 {
					t.Fatal("supervision or slot ownership incorrect after renew failure")
				}
			})
		})
	}
}

func TestClaimAndRenewFatalErrorsDoNotBlockEachOther(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		started := make(chan context.Context, 2)
		release := make(chan struct{})
		reports := make(chan struct{}, 2)
		claims := 0
		s := newStartServer(t, func(_ context.Context, cmd redis.Cmder) error {
			if isRenewCommand(cmd) {
				return startRedisError("ERR renew script failure")
			}
			claims++
			if claims <= 2 {
				cmd.(*redis.Cmd).SetVal([]any{fmt.Sprintf("task-%d", claims), "job", "", "default", "1", "10000"})
				return nil
			}
			time.Sleep(2 * time.Second)
			return startRedisError("NOPERM claim forbidden")
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
		s.onComplete = func(ctx context.Context, record *execution, _ executionKey, _ []byte) { reports <- struct{}{} }
		s.onFailure = func(ctx context.Context, record *execution, _ executionKey, _ error) {
			t.Error("global failure reported business failure")
		}
		if err := s.Start(); err != nil {
			t.Fatal(err)
		}
		contexts := []context.Context{<-started, <-started}
		synctest.Wait()
		time.Sleep(2 * time.Second)
		synctest.Wait()
		select {
		case <-s.claimDone:
		default:
			t.Fatal("Claim blocked on fatal notification")
		}
		select {
		case <-s.renewDone:
		default:
			t.Fatal("Renew blocked on second fatal notification")
		}
		if len(s.Errors()) != 1 || len(s.snapshotExecutions()) != 2 {
			t.Fatal("fatal notifications or retained eligibility incorrect")
		}
		for _, ctx := range contexts {
			if ctx.Err() != nil {
				t.Fatal("fatal background error canceled handler")
			}
		}
		<-s.Errors()
		select {
		case <-s.Errors():
			t.Fatal("extra notification or closed Errors")
		default:
		}
		close(release)
		synctest.Wait()
		if len(reports) != 2 || len(s.slots) != 0 {
			t.Fatal("existing results did not complete after global failure")
		}
	})
}

func TestRenewGlobalFaultAcceptsLateClaimNormally(t *testing.T) {
	for _, mode := range []string{"result", "timeout"} {
		t.Run(mode, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				lateRequest := make(chan struct{})
				deliver := make(chan struct{})
				release := make(chan struct{})
				started := make(chan string, 2)
				completed := make(chan string, 2)
				failed := make(chan string, 2)
				claims, renews := 0, 0
				s := newStartServer(t, func(_ context.Context, cmd redis.Cmder) error {
					if isRenewCommand(cmd) {
						renews++
						return startRedisError("ERR global renew fault")
					}
					claims++
					id, timeout := "existing", "10000"
					if claims == 2 {
						close(lateRequest)
						<-deliver
						id = "late"
						if mode == "timeout" {
							timeout = "1000"
						}
					}
					cmd.(*redis.Cmd).SetVal([]any{id, "job", "", "default", "1", timeout})
					return nil
				}, WithConcurrency(2), WithLeaseDuration(6*time.Second), WithRenewInterval(2*time.Second))
				t.Cleanup(func() {
					for _, gate := range []chan struct{}{deliver, release} {
						select {
						case <-gate:
						default:
							close(gate)
						}
					}
				})
				if err := s.HandleFunc("job", func(_ context.Context, task *Task) error { started <- task.ID; <-release; return nil }); err != nil {
					t.Fatal(err)
				}
				s.onComplete = func(ctx context.Context, record *execution, key executionKey, _ []byte) { completed <- key.taskID }
				s.onFailure = func(ctx context.Context, record *execution, key executionKey, err error) {
					if !errors.Is(err, context.DeadlineExceeded) {
						t.Error(err)
					}
					failed <- key.taskID
				}
				if err := s.Start(); err != nil {
					t.Fatal(err)
				}
				<-started
				<-lateRequest
				time.Sleep(2 * time.Second)
				synctest.Wait()
				select {
				case <-s.renewDone:
				default:
					t.Fatal("global fault did not stop Renew")
				}
				if len(s.snapshotExecutions()) != 1 {
					t.Fatal("global fault cleared eligibility")
				}
				close(deliver)
				if id := <-started; id != "late" {
					t.Fatal(id)
				}
				synctest.Wait()
				select {
				case <-s.claimDone:
				default:
					t.Fatal("Claim did not stop after late handoff")
				}
				if len(s.snapshotExecutions()) != 2 || len(s.slots) != 2 {
					t.Fatal("late Claim did not use normal handoff")
				}
				time.Sleep(2 * time.Second)
				synctest.Wait()
				if renews != 1 || claims != 2 {
					t.Fatal("stopped loops resumed")
				}
				if mode == "timeout" {
					if len(failed) != 1 || <-failed != "late" || len(s.slots) != 2 {
						t.Fatal("global fault suppressed timeout or released live slot")
					}
				}
				close(release)
				synctest.Wait()
				want := 2
				if mode == "timeout" {
					want = 1
				}
				if len(completed) != want || len(s.slots) != 0 || len(s.snapshotExecutions()) != 0 {
					t.Fatal("global fault suppressed normal supervision")
				}
			})
		})
	}
}
