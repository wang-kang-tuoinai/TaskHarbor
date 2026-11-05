package taskq

import (
	"context"
	"errors"
	"io"
	"net"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/wang-kang-tuoinai/TaskHarbor/internal/redisstore"
)

func TestNewServerRequiresDisabledRedisRetries(t *testing.T) {
	for _, retries := range []int{0, 1, -2, -1} {
		rdb := redis.NewClient(&redis.Options{Addr: "127.0.0.1:1", MaxRetries: retries, ContextTimeoutEnabled: true})
		t.Cleanup(func() { _ = rdb.Close() })
		commands := 0
		rdb.AddHook(submitHook{process: func(context.Context, redis.Cmder) error {
			commands++
			return nil
		}})
		effective := rdb.Options().MaxRetries
		s, err := NewServer(rdb)
		if retries == -1 {
			if err != nil || s == nil || s.(*server).redisClient != rdb {
				t.Fatalf("disabled retries: server=%v err=%v", s, err)
			}
		} else if s != nil || err == nil {
			t.Fatalf("max retries=%d accepted: server=%v err=%v", retries, s, err)
		}
		if rdb.Options().MaxRetries != effective || commands != 0 {
			t.Fatalf("constructor mutated client or sent commands: retries=%d commands=%d", rdb.Options().MaxRetries, commands)
		}
	}
}

type startRedisError string

func (e startRedisError) Error() string { return string(e) }
func (e startRedisError) RedisError()   {}

func newStartServer(t *testing.T, process func(context.Context, redis.Cmder) error, opts ...Option) *server {
	t.Helper()
	rdb := redis.NewClient(&redis.Options{Addr: "127.0.0.1:1", MaxRetries: -1, ContextTimeoutEnabled: true})
	t.Cleanup(func() { _ = rdb.Close() })
	rdb.AddHook(submitHook{process: process})
	public, err := NewServer(rdb, opts...)
	if err != nil {
		t.Fatal(err)
	}
	s := public.(*server)
	// 当前 go-redis 的 WithTimeout 共享池但不复制命令 Hook；两个测试调用处分别安装。
	s.renewClient.AddHook(submitHook{process: process})
	t.Cleanup(func() {
		s.cancelClaim()
		s.cancelRenew()
		s.mu.Lock()
		started := s.ownerID != ""
		s.mu.Unlock()
		if started {
			<-s.claimDone
			<-s.renewDone
		}
		close(s.abort)
	})
	return s
}

func TestStartLifecycle(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		calls := 0
		s := newStartServer(t, func(context.Context, redis.Cmder) error {
			calls++
			return startRedisError("NOPERM test stop")
		})
		var public Server = s
		if err := public.Start(); err == nil || calls != 0 {
			t.Fatal("startup without handlers accepted or sent commands")
		}
		if err := public.HandleFunc("job", func(context.Context, *Task) error { return nil }); err != nil {
			t.Fatal(err)
		}
		if err := public.Start(); err != nil {
			t.Fatal(err)
		}
		s.mu.Lock()
		owner, status := s.ownerID, s.status
		s.mu.Unlock()
		if owner == "" || status != serverRunning || cap(public.Errors()) != 1 || public.Errors() != public.Errors() {
			t.Fatal("invalid running state, identity or error channel")
		}
		if err := public.Start(); err == nil {
			t.Fatal("duplicate Start accepted")
		}
		if err := public.HandleFunc("late", func(context.Context, *Task) error { return nil }); err == nil {
			t.Fatal("registration after Start accepted")
		}
	})
}

func TestStartHandsClaimToHandler(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var request []any
		s := newStartServer(t, func(_ context.Context, cmd redis.Cmder) error {
			if request != nil {
				return startRedisError("NOPERM test stop")
			}
			request = append([]any(nil), cmd.Args()...)
			cmd.(*redis.Cmd).SetVal([]any{"id", "job", "\x00\xff", "default", "2", "1500"})
			return nil
		}, WithConcurrency(1))
		tasks := make(chan Task, 2)
		completed := make(chan executionKey, 2)
		if err := s.HandleFunc("job", func(_ context.Context, task *Task) error {
			tasks <- *task
			return task.SetResult("ok")
		}); err != nil {
			t.Fatal(err)
		}
		s.onComplete = func(key executionKey, result []byte) {
			if string(result) != `"ok"` {
				t.Errorf("result=%s", result)
			}
			completed <- key
		}
		s.onFailure = func(executionKey, error) { t.Error("unexpected execution failure") }
		if err := s.Start(); err != nil {
			t.Fatal(err)
		}
		synctest.Wait()
		select {
		case task := <-tasks:
			if task.ID != "id" || task.Type != "job" || task.Attempts != 2 || string(task.Payload) != "\x00\xff" {
				t.Fatalf("task=%+v", task)
			}
		default:
			t.Fatal("Start did not claim and execute task")
		}
		select {
		case key := <-completed:
			if len(request) != 9 || request[6] != s.ownerID || request[7] == "" || request[8] != int64(30000) || key.taskID != "id" || key.attemptToken != request[7] {
				t.Fatalf("request=%#v key=%+v owner=%q", request, key, s.ownerID)
			}
		default:
			t.Fatal("task not handed to completion placeholder")
		}
		if len(tasks) != 0 || len(completed) != 0 || len(s.slots) != 0 || len(s.snapshotExecutions()) != 0 {
			t.Fatal("duplicate handoff or leaked resources")
		}
	})
}

func TestStartEmptyQueuePollsAndCancels(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		tokens := make(chan string, 4)
		s := newStartServer(t, func(_ context.Context, cmd redis.Cmder) error {
			tokens <- cmd.Args()[7].(string)
			cmd.(*redis.Cmd).SetVal([]any{})
			return nil
		}, WithConcurrency(1))
		if err := s.HandleFunc("job", func(context.Context, *Task) error { t.Error("empty queue executed task"); return nil }); err != nil {
			t.Fatal(err)
		}
		if err := s.Start(); err != nil {
			t.Fatal(err)
		}
		synctest.Wait()
		if len(tokens) != 1 || len(s.slots) != 0 || len(s.Errors()) != 0 {
			t.Fatalf("requests=%d occupied=%d errors=%d", len(tokens), len(s.slots), len(s.Errors()))
		}
		time.Sleep(99 * time.Millisecond)
		synctest.Wait()
		if len(tokens) != 1 {
			t.Fatal("empty queue polled without waiting")
		}
		time.Sleep(time.Millisecond)
		synctest.Wait()
		if len(tokens) != 2 {
			t.Fatalf("independent claims count=%d", len(tokens))
		}
		s.cancelClaim()
		synctest.Wait()
		select {
		case <-s.claimDone:
		default:
			t.Fatal("poll wait did not cancel")
		}
		if len(tokens) != 2 || len(s.slots) != 0 || len(s.Errors()) != 0 {
			t.Fatal("canceled wait sent command, leaked slot or reported fatal error")
		}
		first, second := <-tokens, <-tokens
		if first == "" || first == second {
			t.Fatalf("independent claim tokens=%q %q", first, second)
		}
	})
}

func TestStartNetworkBackoffIsBoundedAndCancelable(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		tokens := make(chan string, 16)
		s := newStartServer(t, func(_ context.Context, cmd redis.Cmder) error {
			tokens <- cmd.Args()[7].(string)
			return io.EOF
		}, WithConcurrency(1))
		if err := s.HandleFunc("job", func(context.Context, *Task) error { t.Error("network error executed task"); return nil }); err != nil {
			t.Fatal(err)
		}
		if err := s.Start(); err != nil {
			t.Fatal(err)
		}
		synctest.Wait()
		if len(tokens) != 1 || len(s.Errors()) != 0 || len(s.slots) != 0 {
			t.Fatal("network error was fatal, replayed or leaked slot")
		}
		for i, delay := range []time.Duration{100 * time.Millisecond, 200 * time.Millisecond, 400 * time.Millisecond, 800 * time.Millisecond, 1600 * time.Millisecond, 2 * time.Second, 2 * time.Second} {
			time.Sleep(delay - time.Nanosecond)
			synctest.Wait()
			if len(tokens) != i+1 {
				t.Fatalf("retried before delay %v: calls=%d", delay, len(tokens))
			}
			time.Sleep(time.Nanosecond)
			synctest.Wait()
			if len(tokens) != i+2 || len(s.Errors()) != 0 || len(s.slots) != 0 {
				t.Fatalf("delay=%v calls=%d errors=%d occupied=%d", delay, len(tokens), len(s.Errors()), len(s.slots))
			}
		}
		s.cancelClaim()
		synctest.Wait()
		select {
		case <-s.claimDone:
		default:
			t.Fatal("backoff did not cancel")
		}
		if len(tokens) != 8 || len(s.Errors()) != 0 || len(s.snapshotExecutions()) != 0 {
			t.Fatal("cancel caused request, fatal notification or registration")
		}
		seen := map[string]bool{}
		for range 8 {
			token := <-tokens
			if token == "" || seen[token] {
				t.Fatalf("replayed token %q", token)
			}
			seen[token] = true
		}
	})
}

func TestStartPreflightDoesNotLeakSlots(t *testing.T) {
	for _, mode := range []string{"no-slot", "token-error"} {
		t.Run(mode, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				calls, tokens := 0, 0
				cause := errors.New("entropy unavailable")
				s := newStartServer(t, func(context.Context, redis.Cmder) error { calls++; return nil }, WithConcurrency(1))
				s.newAttemptToken = func() (string, error) { tokens++; return "", cause }
				if err := s.HandleFunc("job", func(context.Context, *Task) error { t.Error("preflight failure executed task"); return nil }); err != nil {
					t.Fatal(err)
				}
				if mode == "no-slot" {
					s.slots <- struct{}{}
				}
				if err := s.Start(); err != nil {
					t.Fatal(err)
				}
				synctest.Wait()
				if mode == "no-slot" {
					if calls != 0 || tokens != 0 || len(s.slots) != 1 {
						t.Fatal("claim or token generation bypassed occupied slot")
					}
					s.cancelClaim()
					synctest.Wait()
					if len(s.slots) != 1 || len(s.Errors()) != 0 {
						t.Fatal("canceled waiter released another task's slot or reported error")
					}
					<-s.slots
				} else {
					select {
					case <-s.claimDone:
					default:
						t.Fatal("token failure did not finish without error consumer")
					}
					select {
					case err := <-s.Errors():
						if !errors.Is(err, cause) {
							t.Fatalf("error chain: %v", err)
						}
					default:
						t.Fatal("token failure not reported")
					}
					if tokens != 1 {
						t.Fatalf("token generations=%d", tokens)
					}
				}
				if calls != 0 || len(s.slots) != 0 || len(s.snapshotExecutions()) != 0 {
					t.Fatal("preflight leaked resources or sent Redis command")
				}
			})
		})
	}
}

func TestStartFatalClaimErrorsStopAndNotify(t *testing.T) {
	for _, tc := range []struct {
		name     string
		cause    error
		response any
		want     error
	}{
		{"inconsistent", startRedisError("TASKQ_CLAIM_INCONSISTENT task bad-id: missing hash"), nil, redisstore.ErrClaimInconsistent},
		{"permission", startRedisError("NOPERM forbidden"), nil, nil},
		{"wrong-type", startRedisError("WRONGTYPE invalid index"), nil, nil},
		{"script", startRedisError("ERR script failure"), nil, nil},
		{"invalid-response", nil, []any{"untrusted-id"}, redisstore.ErrClaimResponse},
	} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				calls := 0
				s := newStartServer(t, func(_ context.Context, cmd redis.Cmder) error {
					calls++
					if tc.cause != nil {
						return tc.cause
					}
					cmd.(*redis.Cmd).SetVal(tc.response)
					return nil
				}, WithConcurrency(1))
				if err := s.HandleFunc("job", func(context.Context, *Task) error { t.Error("fatal claim executed task"); return nil }); err != nil {
					t.Fatal(err)
				}
				if err := s.Start(); err != nil {
					t.Fatal(err)
				}
				synctest.Wait()
				// 先确认退出，再读取 Errors，证明无人消费不会阻塞领取停止。
				select {
				case <-s.claimDone:
				default:
					t.Fatal("fatal error notification blocked")
				}
				select {
				case err := <-s.Errors():
					if err == nil || tc.want != nil && !errors.Is(err, tc.want) || tc.cause != nil && !errors.Is(err, tc.cause) {
						t.Fatalf("error chain: %v", err)
					}
				default:
					t.Fatal("fatal claim error not reported")
				}
				time.Sleep(5 * time.Second)
				synctest.Wait()
				if calls != 1 || len(s.Errors()) != 0 || len(s.slots) != 0 || len(s.snapshotExecutions()) != 0 {
					t.Fatal("fatal claim retried, repeated notification or leaked resources")
				}
				if err := s.Start(); err == nil {
					t.Fatal("failed server object restarted")
				}
			})
		})
	}
}

func TestStartReturnsWhileRedisIsBlocked(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		entered := make(chan struct{})
		s := newStartServer(t, func(ctx context.Context, _ redis.Cmder) error {
			close(entered)
			<-ctx.Done()
			return ctx.Err()
		}, WithConcurrency(1))
		if err := s.HandleFunc("job", func(context.Context, *Task) error { return nil }); err != nil {
			t.Fatal(err)
		}
		returned := make(chan error, 1)
		go func() { returned <- s.Start() }()
		<-entered
		synctest.Wait()
		select {
		case err := <-returned:
			if err != nil {
				t.Fatal(err)
			}
		default:
			t.Fatal("Start waited for Redis")
		}
		s.cancelClaim()
		synctest.Wait()
		if len(s.slots) != 0 || len(s.Errors()) != 0 {
			t.Fatal("canceled in-flight request leaked slot or reported fatal error")
		}
	})
}

func TestStartFatalClaimLeavesExistingHandlerRunning(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		calls := 0
		cause := startRedisError("NOPERM stop new claims")
		s := newStartServer(t, func(_ context.Context, cmd redis.Cmder) error {
			calls++
			if calls != 1 {
				return cause
			}
			cmd.(*redis.Cmd).SetVal([]any{"running-task", "job", "", "default", "1", "86400000"})
			return nil
		}, WithConcurrency(2))
		started := make(chan context.Context, 1)
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
			return task.SetResult("finished")
		}); err != nil {
			t.Fatal(err)
		}
		completed := 0
		s.onComplete = func(executionKey, []byte) { completed++ }
		s.onFailure = func(executionKey, error) { t.Error("claim failure became execution failure") }
		if err := s.Start(); err != nil {
			t.Fatal(err)
		}
		ctx := <-started
		synctest.Wait()
		select {
		case <-s.claimDone:
		default:
			t.Fatal("claim loop did not stop")
		}
		if err := <-s.Errors(); !errors.Is(err, cause) {
			t.Fatal(err)
		}
		time.Sleep(5 * time.Second)
		synctest.Wait()
		if ctx.Err() != nil || len(s.snapshotExecutions()) != 1 || len(s.slots) != 1 || calls != 2 {
			t.Fatal("fatal claim affected existing handler")
		}
		close(release)
		synctest.Wait()
		if completed != 1 || len(s.slots) != 0 || len(s.snapshotExecutions()) != 0 {
			t.Fatal("existing execution did not finish normally")
		}
	})
}

func TestStartUnknownHandlerUsesFailurePlaceholder(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		calls, failures := 0, 0
		var token string
		s := newStartServer(t, func(_ context.Context, cmd redis.Cmder) error {
			calls++
			if calls > 1 {
				return startRedisError("NOPERM test stop")
			}
			token = cmd.Args()[7].(string)
			cmd.(*redis.Cmd).SetVal([]any{"id", "unregistered", "", "default", "3", "60000"})
			return nil
		}, WithConcurrency(1))
		if err := s.HandleFunc("known", func(context.Context, *Task) error { t.Error("selected wrong handler"); return nil }); err != nil {
			t.Fatal(err)
		}
		s.onComplete = func(executionKey, []byte) { t.Error("unknown handler completed") }
		s.onFailure = func(key executionKey, err error) {
			failures++
			if key.taskID != "id" || key.attemptToken != token || !strings.Contains(err.Error(), "unregistered") {
				t.Errorf("key=%+v err=%v", key, err)
			}
		}
		if err := s.Start(); err != nil {
			t.Fatal(err)
		}
		synctest.Wait()
		if failures != 1 || calls != 2 || len(s.slots) != 0 || len(s.snapshotExecutions()) != 0 {
			t.Fatal("unknown handler did not follow failure/slot ownership path")
		}
	})
}

func TestStartOtherTransientErrorsRetry(t *testing.T) {
	for _, cause := range []error{io.ErrUnexpectedEOF, net.ErrClosed, context.DeadlineExceeded, redis.ErrPoolTimeout, &net.OpError{Op: "read", Net: "tcp", Err: errors.New("connection reset")}} {
		t.Run(cause.Error(), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				calls := 0
				s := newStartServer(t, func(context.Context, redis.Cmder) error {
					calls++
					if calls == 1 {
						return cause
					}
					return startRedisError("NOPERM test stop")
				})
				if err := s.HandleFunc("job", func(context.Context, *Task) error { return nil }); err != nil {
					t.Fatal(err)
				}
				if err := s.Start(); err != nil {
					t.Fatal(err)
				}
				synctest.Wait()
				if len(s.Errors()) != 0 || len(s.slots) != 0 {
					t.Fatal("transient error treated as fatal or leaked slot")
				}
				time.Sleep(100 * time.Millisecond)
				synctest.Wait()
				if calls != 2 {
					t.Fatalf("requests=%d", calls)
				}
			})
		})
	}
}
