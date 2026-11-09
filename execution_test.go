package taskq

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"
)

func TestExecutionStoppedBeforeHandlerStart(t *testing.T) {
	for _, mode := range []string{"abort", "parent"} {
		t.Run(mode, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				s := newTestServer(t, WithConcurrency(1))
				if err := s.HandleFunc("job", func(context.Context, *Task) error {
					t.Error("handler started after execution was stopped")
					return nil
				}); err != nil {
					t.Fatal(err)
				}
				s.onComplete = func(context.Context, *execution, executionKey, []byte) { t.Error("completion after stop") }
				s.onFailure = func(context.Context, *execution, executionKey, error) { t.Error("failure after stop") }
				parent, cancelParent := context.WithCancelCause(context.Background())
				defer cancelParent(nil)
				if mode == "abort" {
					close(s.abort)
				} else {
					cancelParent(errExecutionCanceled)
				}
				s.slots <- struct{}{}
				done, err := s.acceptExecution(parent, &Task{Type: "job"}, executionKey{"task", "token"}, time.Second)
				if err != nil {
					t.Fatal(err)
				}
				<-done
				synctest.Wait()
				if len(s.slots) != 0 || len(s.snapshotExecutions()) != 0 {
					t.Fatal("stopped handoff leaked a slot or renewal eligibility")
				}
			})
		})
	}
}

func TestExecutionSuccess(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s := newTestServer(t, WithConcurrency(1))
		key := executionKey{taskID: "task", attemptToken: "attempt"}
		var handlerContext context.Context
		if err := s.HandleFunc("job", func(ctx context.Context, task *Task) error {
			handlerContext = ctx
			if _, ok := s.getExecution(key); !ok {
				t.Error("handler started before execution registration")
			}
			if task.ID != "task" || task.Attempts != 1 || string(task.Payload) != "payload" {
				t.Errorf("wrong handler input: %+v", task)
			}
			// Handler 修改业务视图也不能改变本轮清理和上报使用的身份。
			task.ID = "changed-by-handler"
			return task.SetResult(map[string]bool{"ok": true})
		}); err != nil {
			t.Fatal(err)
		}
		calls := 0
		s.onComplete = func(ctx context.Context, record *execution, got executionKey, result []byte) {
			calls++
			if got != key || string(result) != `{"ok":true}` {
				t.Errorf("complete key=%+v result=%s", got, result)
			}
			if _, ok := s.getExecution(key); !ok || handlerContext.Err() != nil {
				t.Error("normal execution cleaned up before reporting placeholder")
			}
		}
		s.onFailure = func(ctx context.Context, record *execution, _ executionKey, _ error) {
			t.Error("success entered failure branch")
		}
		s.slots <- struct{}{}
		done, err := s.acceptExecution(context.Background(), &Task{ID: "task", Type: "job", Attempts: 1, Payload: []byte("payload")}, key, time.Second)
		if err != nil {
			t.Fatal(err)
		}
		<-done
		synctest.Wait()
		if calls != 1 || len(s.slots) != 0 || len(s.snapshotExecutions()) != 0 || handlerContext.Err() == nil {
			t.Fatalf("calls=%d occupied=%d records=%d context=%v", calls, len(s.slots), len(s.snapshotExecutions()), handlerContext.Err())
		}
	})
}

func TestExecutionHandlerPanic(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s := newTestServer(t, WithConcurrency(1))
		key := executionKey{taskID: "task", attemptToken: "panic-attempt"}
		if err := s.HandleFunc("job", func(context.Context, *Task) error { panic("broken handler") }); err != nil {
			t.Fatal(err)
		}
		failures := 0
		s.onComplete = func(ctx context.Context, record *execution, _ executionKey, _ []byte) {
			t.Error("panic entered complete branch")
		}
		s.onFailure = func(ctx context.Context, record *execution, got executionKey, err error) {
			failures++
			if got != key || err == nil || !strings.Contains(err.Error(), "broken handler") {
				t.Errorf("panic failure key=%+v err=%v", got, err)
			}
		}
		s.slots <- struct{}{}
		done, err := s.acceptExecution(context.Background(), &Task{ID: "task", Type: "job", Attempts: 1}, key, time.Second)
		if err != nil {
			t.Fatal(err)
		}
		<-done
		synctest.Wait()
		if failures != 1 || len(s.slots) != 0 || len(s.snapshotExecutions()) != 0 {
			t.Fatalf("failures=%d occupied=%d records=%d", failures, len(s.slots), len(s.snapshotExecutions()))
		}
	})
}

func TestExecutionHandlerError(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s := newTestServer(t, WithConcurrency(1))
		key := executionKey{taskID: "task", attemptToken: "attempt"}
		cause := errors.New("business failure")
		if err := s.HandleFunc("job", func(context.Context, *Task) error { return cause }); err != nil {
			t.Fatal(err)
		}
		failures := 0
		s.onComplete = func(ctx context.Context, record *execution, _ executionKey, _ []byte) {
			t.Error("error entered complete branch")
		}
		s.onFailure = func(ctx context.Context, record *execution, got executionKey, err error) {
			failures++
			if got != key || !errors.Is(err, cause) {
				t.Errorf("failure key=%+v err=%v", got, err)
			}
			if _, ok := s.getExecution(key); !ok {
				t.Error("normal result record removed before failure placeholder")
			}
		}
		s.slots <- struct{}{}
		done, err := s.acceptExecution(context.Background(), &Task{ID: "task", Type: "job", Attempts: 1}, key, time.Second)
		if err != nil {
			t.Fatal(err)
		}
		<-done
		synctest.Wait()
		if failures != 1 || len(s.slots) != 0 || len(s.snapshotExecutions()) != 0 {
			t.Fatalf("failures=%d occupied=%d records=%d", failures, len(s.slots), len(s.snapshotExecutions()))
		}
	})
}

func TestExecutionUnknownHandler(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s := newTestServer(t, WithConcurrency(1))
		key := executionKey{taskID: "task", attemptToken: "unknown-attempt"}
		task := &Task{ID: "task", Type: "unregistered", Attempts: 2}
		failures := 0
		s.onComplete = func(ctx context.Context, record *execution, _ executionKey, _ []byte) {
			t.Error("unknown handler entered complete branch")
		}
		s.onFailure = func(ctx context.Context, record *execution, got executionKey, err error) {
			failures++
			if got != key || err == nil || !strings.Contains(err.Error(), "unregistered") {
				t.Errorf("unknown handler failure key=%+v err=%v", got, err)
			}
		}
		s.slots <- struct{}{}
		done, err := s.acceptExecution(context.Background(), task, key, time.Second)
		if err != nil {
			t.Fatal(err)
		}
		<-done
		synctest.Wait()
		if failures != 1 || len(s.slots) != 0 || len(s.snapshotExecutions()) != 0 || task.Attempts != 2 {
			t.Fatalf("failures=%d occupied=%d records=%d attempts=%d", failures, len(s.slots), len(s.snapshotExecutions()), task.Attempts)
		}
	})
}

func TestExecutionTimeoutRetainsSlotUntilHandlerExit(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s := newTestServer(t, WithConcurrency(2))
		key := executionKey{taskID: "task", attemptToken: "timeout-attempt"}
		started := make(chan context.Context, 1)
		failureContext := make(chan context.Context, 1)
		release := make(chan struct{})
		t.Cleanup(func() {
			select {
			case <-release:
			default:
				close(release)
			}
		})
		if err := s.HandleFunc("job", func(ctx context.Context, task *Task) error {
			started <- ctx
			failureContext <- ctx
			<-release // 故意忽略取消，验证监督结束不能代替 Handler 退出。
			return task.SetResult("late success")
		}); err != nil {
			t.Fatal(err)
		}
		completions, failures := 0, 0
		s.onComplete = func(ctx context.Context, record *execution, _ executionKey, _ []byte) { completions++ }
		s.onFailure = func(ctx context.Context, record *execution, got executionKey, err error) {
			failures++
			if got != key || !errors.Is(err, context.DeadlineExceeded) {
				t.Errorf("timeout key=%+v err=%v", got, err)
			}
			if len(s.snapshotExecutions()) != 0 {
				t.Error("timeout reported before removing execution record")
			}
			if ctx := <-failureContext; ctx.Err() == nil {
				t.Error("timeout reported before canceling handler")
			}
		}
		task := &Task{ID: "task", Type: "job", Attempts: 1}
		s.slots <- struct{}{}
		s.slots <- struct{}{}        // 另一占用用于检查不会重复归还别的执行槽。
		time.Sleep(10 * time.Second) // 虚拟时间：领取和接收前的准备不消耗执行超时。
		done, err := s.acceptExecution(context.Background(), task, key, time.Second)
		if err != nil {
			t.Fatal(err)
		}
		ctx := <-started
		synctest.Wait()
		time.Sleep(999 * time.Millisecond)
		synctest.Wait()
		if ctx.Err() != nil || len(s.snapshotExecutions()) != 1 {
			t.Fatal("execution timeout consumed before full handler duration")
		}
		time.Sleep(time.Millisecond)
		synctest.Wait()
		select {
		case <-done:
		default:
			t.Fatal("supervision did not finish independently of blocked handler")
		}
		if completions != 0 || failures != 1 || ctx.Err() == nil || len(s.slots) != 2 || len(s.snapshotExecutions()) != 0 {
			t.Fatalf("complete=%d failure=%d context=%v occupied=%d records=%d", completions, failures, ctx.Err(), len(s.slots), len(s.snapshotExecutions()))
		}
		close(release)
		synctest.Wait()
		if len(s.slots) != 1 || completions != 0 || failures != 1 || task.Attempts != 1 {
			t.Fatalf("late result: occupied=%d complete=%d failure=%d attempts=%d", len(s.slots), completions, failures, task.Attempts)
		}
		<-s.slots
	})
}

func TestExecutionAbortBroadcast(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s := newTestServer(t, WithConcurrency(3))
		started := make(chan context.Context, 3)
		release := make(chan struct{})
		t.Cleanup(func() {
			select {
			case <-release:
			default:
				close(release)
			}
		})
		if err := s.HandleFunc("job", func(ctx context.Context, task *Task) error {
			started <- ctx
			<-release
			switch task.Payload[0] {
			case 0:
				return nil
			case 1:
				return errors.New("late failure")
			default:
				panic("late panic")
			}
		}); err != nil {
			t.Fatal(err)
		}
		var reports atomic.Int32
		s.onComplete = func(ctx context.Context, record *execution, _ executionKey, _ []byte) { reports.Add(1) }
		s.onFailure = func(ctx context.Context, record *execution, _ executionKey, _ error) { reports.Add(1) }
		var done []<-chan struct{}
		for i := range 3 {
			s.slots <- struct{}{}
			key := executionKey{taskID: "shared-task", attemptToken: fmt.Sprintf("attempt-%d", i)}
			finished, err := s.acceptExecution(context.Background(), &Task{ID: key.taskID, Type: "job", Attempts: 1, Payload: []byte{byte(i)}}, key, time.Hour)
			if err != nil {
				t.Fatal(err)
			}
			done = append(done, finished)
		}
		var contexts []context.Context
		for range 3 {
			contexts = append(contexts, <-started)
		}
		close(s.abort) // 关闭一次广播给全部监督协程，不发送单条通知。
		synctest.Wait()
		for _, finished := range done {
			select {
			case <-finished:
			default:
				t.Fatal("abort did not finish all supervisors")
			}
		}
		for _, ctx := range contexts {
			if ctx.Err() == nil {
				t.Error("abort did not cancel handler")
			}
		}
		if reports.Load() != 0 || len(s.snapshotExecutions()) != 0 || len(s.slots) != 3 {
			t.Fatalf("reports=%d records=%d occupied=%d", reports.Load(), len(s.snapshotExecutions()), len(s.slots))
		}
		close(release)
		synctest.Wait()
		if reports.Load() != 0 || len(s.slots) != 0 {
			t.Fatalf("late results: reports=%d occupied=%d", reports.Load(), len(s.slots))
		}
	})
}

func TestExecutionOldCleanupPreservesNewAttempt(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s := newTestServer(t, WithConcurrency(1))
		oldKey := executionKey{taskID: "same-task", attemptToken: "old"}
		newKey := executionKey{taskID: "same-task", attemptToken: "new"}
		reportStarted := make(chan struct{})
		reportRelease := make(chan struct{})
		newStarted := make(chan context.Context, 1)
		newRelease := make(chan struct{})
		t.Cleanup(func() {
			for _, gate := range []chan struct{}{reportRelease, newRelease} {
				select {
				case <-gate:
				default:
					close(gate)
				}
			}
		})
		if err := s.HandleFunc("job", func(ctx context.Context, task *Task) error {
			if task.Attempts == 2 {
				newStarted <- ctx
				<-newRelease
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		var completed atomic.Int32
		s.onComplete = func(ctx context.Context, record *execution, key executionKey, _ []byte) {
			completed.Add(1)
			if key == oldKey {
				close(reportStarted)
				<-reportRelease
			}
		}
		s.onFailure = func(ctx context.Context, record *execution, _ executionKey, _ error) { t.Error("unexpected failure") }
		s.slots <- struct{}{}
		oldDone, err := s.acceptExecution(context.Background(), &Task{ID: oldKey.taskID, Type: "job", Attempts: 1}, oldKey, time.Hour)
		if err != nil {
			t.Fatal(err)
		}
		<-reportStarted
		synctest.Wait()
		if len(s.slots) != 0 || len(s.snapshotExecutions()) != 1 {
			t.Fatal("handler slot and ongoing result handling have different lifetimes")
		}
		s.slots <- struct{}{}
		newDone, err := s.acceptExecution(context.Background(), &Task{ID: newKey.taskID, Type: "job", Attempts: 2}, newKey, time.Hour)
		if err != nil {
			t.Fatal(err)
		}
		newCtx := <-newStarted
		close(reportRelease)
		<-oldDone
		synctest.Wait()
		if _, ok := s.getExecution(newKey); !ok || newCtx.Err() != nil || len(s.snapshotExecutions()) != 1 || len(s.slots) != 1 {
			t.Fatal("old completion removed or canceled new attempt")
		}
		close(newRelease)
		<-newDone
		synctest.Wait()
		if completed.Load() != 2 || len(s.snapshotExecutions()) != 0 || len(s.slots) != 0 {
			t.Fatalf("complete=%d records=%d occupied=%d", completed.Load(), len(s.snapshotExecutions()), len(s.slots))
		}
	})
}

func TestExecutionCompetingSignalsChooseOneBranch(t *testing.T) {
	for round := range 32 {
		t.Run(fmt.Sprintf("round-%d", round), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				s := newTestServer(t, WithConcurrency(1))
				key := executionKey{taskID: "task", attemptToken: "racing-attempt"}
				started := make(chan context.Context, 1)
				if err := s.HandleFunc("job", func(ctx context.Context, task *Task) error {
					started <- ctx
					time.Sleep(time.Second)
					return task.SetResult("winner")
				}); err != nil {
					t.Fatal(err)
				}
				var completed, failed atomic.Int32
				s.onComplete = func(ctx context.Context, record *execution, got executionKey, result []byte) {
					completed.Add(1)
					if got != key || string(result) != `"winner"` {
						t.Errorf("complete key=%+v result=%s", got, result)
					}
				}
				s.onFailure = func(ctx context.Context, record *execution, got executionKey, err error) {
					failed.Add(1)
					if got != key || !errors.Is(err, context.DeadlineExceeded) {
						t.Errorf("failure key=%+v err=%v", got, err)
					}
				}
				s.slots <- struct{}{}
				done, err := s.acceptExecution(context.Background(), &Task{ID: "task", Type: "job", Attempts: 1}, key, time.Second)
				if err != nil {
					t.Fatal(err)
				}
				ctx := <-started
				go func() {
					time.Sleep(time.Second)
					close(s.abort)
				}()
				synctest.Wait()
				// 三种信号在同一虚拟时刻触发，不要求固定赢家。
				time.Sleep(time.Second)
				synctest.Wait()
				select {
				case <-done:
				default:
					t.Fatal("competing signals did not finish supervision")
				}
				if completed.Load()+failed.Load() > 1 || len(s.slots) != 0 || len(s.snapshotExecutions()) != 0 || ctx.Err() == nil {
					t.Fatalf("complete=%d failure=%d occupied=%d records=%d context=%v", completed.Load(), failed.Load(), len(s.slots), len(s.snapshotExecutions()), ctx.Err())
				}
			})
		})
	}
}
