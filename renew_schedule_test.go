package taskq

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/redis/go-redis/v9"
)

func TestRenewLongScanIsSerialAndDropsMissedTicks(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		requests := make(chan string, 16)
		started := make(chan struct{}, 3)
		release := make(chan struct{})
		claims := 0
		var active, maximum atomic.Int32
		s := newStartServer(t, func(ctx context.Context, cmd redis.Cmder) error {
			if isRenewCommand(cmd) {
				n := active.Add(1)
				defer active.Add(-1)
				for old := maximum.Load(); n > old; old = maximum.Load() {
					if maximum.CompareAndSwap(old, n) {
						break
					}
				}
				deadline, ok := ctx.Deadline()
				if !ok || deadline.Sub(time.Now()) > 2*time.Second {
					t.Error("request has no independent bounded deadline")
				}
				requests <- cmd.Args()[6].(string)
				<-ctx.Done()
				return ctx.Err()
			}
			claims++
			if claims <= 3 {
				cmd.(*redis.Cmd).SetVal([]any{fmt.Sprintf("task-%d", claims), "job", "", "default", "1", "60000"})
			} else {
				cmd.(*redis.Cmd).SetVal([]any{})
			}
			return nil
		}, WithConcurrency(4), WithLeaseDuration(6*time.Second), WithRenewInterval(2*time.Second))
		t.Cleanup(func() {
			select {
			case <-release:
			default:
				close(release)
			}
		})
		if err := s.HandleFunc("job", func(context.Context, *Task) error { started <- struct{}{}; <-release; return nil }); err != nil {
			t.Fatal(err)
		}
		if err := s.Start(); err != nil {
			t.Fatal(err)
		}
		for range 3 {
			<-started
		}
		synctest.Wait()
		time.Sleep(8 * time.Second)
		synctest.Wait()
		if len(requests) != 3 || maximum.Load() != 1 || active.Load() != 0 || len(s.Errors()) != 0 || len(s.snapshotExecutions()) != 3 {
			t.Fatalf("requests=%d max=%d active=%d errors=%d", len(requests), maximum.Load(), active.Load(), len(s.Errors()))
		}
		time.Sleep(1999 * time.Millisecond)
		synctest.Wait()
		if len(requests) != 3 {
			t.Fatal("missed ticks were replayed")
		}
		time.Sleep(time.Millisecond)
		synctest.Wait()
		if len(requests) != 4 {
			t.Fatal("next fixed-period scan did not start")
		}
		s.cancelRenew()
		synctest.Wait()
		close(release)
		synctest.Wait()
	})
}

func TestRenewSkipsRemovedSnapshotAndPreservesNewAttempt(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		requests := make(chan executionKey, 16)
		first := make(chan executionKey, 1)
		resume := make(chan struct{})
		release := make(chan struct{})
		started := make(chan context.Context, 4)
		var calls atomic.Int32
		claims := 0
		s := newStartServer(t, func(_ context.Context, cmd redis.Cmder) error {
			if isRenewCommand(cmd) {
				key := executionKey{taskID: cmd.Args()[6].(string), attemptToken: cmd.Args()[7].(string)}
				requests <- key
				if calls.Add(1) == 1 {
					first <- key
					<-resume
				}
				cmd.(*redis.Cmd).SetVal(int64(1))
				return nil
			}
			claims++
			if claims <= 3 {
				cmd.(*redis.Cmd).SetVal([]any{fmt.Sprintf("task-%d", claims), "job", "", "default", "1", "60000"})
			} else {
				cmd.(*redis.Cmd).SetVal([]any{})
			}
			return nil
		}, WithConcurrency(4), WithLeaseDuration(6*time.Second), WithRenewInterval(2*time.Second))
		t.Cleanup(func() {
			for _, gate := range []chan struct{}{resume, release} {
				select {
				case <-gate:
				default:
					close(gate)
				}
			}
		})
		if err := s.HandleFunc("job", func(ctx context.Context, _ *Task) error { started <- ctx; <-release; return nil }); err != nil {
			t.Fatal(err)
		}
		if err := s.Start(); err != nil {
			t.Fatal(err)
		}
		for range 3 {
			<-started
		}
		time.Sleep(2 * time.Second)
		firstKey := <-first
		snapshot := s.snapshotExecutions() // 网络请求阻塞时仍能读取和清理本地表。
		var oldKey executionKey
		var oldRecord *execution
		for key, record := range snapshot {
			if key != firstKey {
				oldKey, oldRecord = key, record
				break
			}
		}
		s.revokeExecution(oldKey, oldRecord, errExecutionLost)
		newKey := executionKey{taskID: oldKey.taskID, attemptToken: "new-attempt"}
		s.slots <- struct{}{}
		done, err := s.acceptExecution(context.Background(), &Task{ID: newKey.taskID, Type: "job", Attempts: 2}, newKey, time.Minute)
		if err != nil {
			t.Fatal(err)
		}
		newCtx := <-started
		s.revokeExecution(oldKey, oldRecord, errExecutionLost)
		if newCtx.Err() != nil {
			t.Fatal("old round cleanup canceled new round")
		}
		close(resume)
		synctest.Wait()
		if len(requests) != 2 {
			t.Fatalf("removed snapshot entry not skipped: %d", len(requests))
		}
		for len(requests) > 0 {
			if key := <-requests; key == oldKey || key == newKey {
				t.Fatalf("unexpected current-snapshot request %+v", key)
			}
		}
		time.Sleep(2 * time.Second)
		synctest.Wait()
		found := false
		for len(requests) > 0 {
			key := <-requests
			if key == oldKey {
				t.Fatal("old identity renewed")
			}
			if key == newKey {
				found = true
			}
		}
		if !found {
			t.Fatal("new round did not retain renewal eligibility")
		}
		close(release)
		<-done
		synctest.Wait()
	})
}

func TestRenewNeverResetsHandlerTimeout(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		started := make(chan struct{}, 1)
		release := make(chan struct{})
		failed := make(chan error, 1)
		var renews atomic.Int32
		claims := 0
		s := newStartServer(t, func(_ context.Context, cmd redis.Cmder) error {
			if isRenewCommand(cmd) {
				renews.Add(1)
				cmd.(*redis.Cmd).SetVal(int64(1))
				return nil
			}
			claims++
			cmd.(*redis.Cmd).SetVal([]any{"task", "job", "", "default", "1", "5000"})
			return nil
		}, WithConcurrency(1), WithLeaseDuration(6*time.Second), WithRenewInterval(2*time.Second))
		t.Cleanup(func() {
			select {
			case <-release:
			default:
				close(release)
			}
		})
		if err := s.HandleFunc("job", func(context.Context, *Task) error { started <- struct{}{}; <-release; return nil }); err != nil {
			t.Fatal(err)
		}
		s.onComplete = func(executionKey, []byte) { t.Error("late result completed") }
		s.onFailure = func(_ executionKey, err error) { failed <- err }
		if err := s.Start(); err != nil {
			t.Fatal(err)
		}
		<-started
		time.Sleep(5 * time.Second)
		synctest.Wait()
		if renews.Load() != 2 || len(failed) != 1 || len(s.slots) != 1 || len(s.snapshotExecutions()) != 0 {
			t.Fatal("renew changed local timeout or slot lifecycle")
		}
		s.cancelClaim()
		close(release)
		synctest.Wait()
	})
}
