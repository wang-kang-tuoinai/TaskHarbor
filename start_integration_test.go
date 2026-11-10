//go:build integration

package taskq

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/wang-kang-tuoinai/TaskHarbor/internal/redisstore"
)

func stopStartLoop(t *testing.T, s *server) {
	t.Helper()
	s.cancelClaim()
	select {
	case <-s.claimDone:
	case <-time.After(3 * time.Second):
		t.Error("claim loop did not stop during test cleanup")
	}
}

func awaitStartExecutions(t *testing.T, s *server) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for len(s.slots) != 0 || len(s.snapshotExecutions()) != 0 {
		if time.Now().After(deadline) {
			t.Error("local executions did not finish during test cleanup")
			return
		}
		time.Sleep(time.Millisecond)
	}
}

func integrationServer(t *testing.T, rdb *redis.Client, opts ...Option) *server {
	t.Helper()
	public, err := NewServer(rdb, opts...)
	if err != nil {
		t.Fatal(err)
	}
	s := public.(*server)
	t.Cleanup(func() {
		s.cancelRenew()
		s.cancelReclaim()
		s.mu.Lock()
		started := s.ownerID != ""
		s.mu.Unlock()
		if started {
			stopStartLoop(t, s)
			select {
			case <-s.renewDone:
			case <-time.After(3 * time.Second):
				t.Error("renew loop did not stop during cleanup")
			}
			select {
			case <-s.reclaimDone:
			case <-time.After(4 * time.Second):
				t.Error("reclaim loop did not stop during cleanup")
			}
		} else {
			s.cancelClaim()
		}
		close(s.abort)
		awaitStartExecutions(t, s)
	})
	return s
}

func TestStartRedisPipelineAndConcurrency(t *testing.T) {
	rdb := startSubmitRedis(t)
	c := integrationClient(t, rdb)
	ctx := context.Background()
	for i := range 5 {
		mustSubmit(t, c, fmt.Sprintf("task-%d", i), "job", []byte{0, 255})
	}
	s := integrationServer(t, rdb, WithConcurrency(2))
	started := make(chan string, 5)
	completed := make(chan string, 5)
	release := make(chan struct{})
	t.Cleanup(func() {
		select {
		case <-release:
		default:
			close(release)
		}
	})
	var running, maximum atomic.Int32
	if err := s.HandleFunc("job", func(_ context.Context, task *Task) error {
		current := running.Add(1)
		defer running.Add(-1)
		for old := maximum.Load(); current > old; old = maximum.Load() {
			if maximum.CompareAndSwap(old, current) {
				break
			}
		}
		if task.Attempts != 1 || string(task.Payload) != "\x00\xff" {
			t.Errorf("task=%+v", task)
		}
		started <- task.ID
		<-release
		return task.SetResult(task.ID)
	}); err != nil {
		t.Fatal(err)
	}
	s.onComplete = func(ctx context.Context, record *execution, key executionKey, result []byte) {
		s.complete(ctx, record, key, result)
		completed <- key.taskID
	}
	if err := s.Start(); err != nil {
		t.Fatal(err)
	}
	for range 2 {
		select {
		case <-started:
		case <-time.After(3 * time.Second):
			t.Fatal("initial handlers did not start")
		}
	}
	// 两个 Handler 保持阻塞时，后三个任务必须仍在 pending，不能越过本地槽上限。
	requirePending(t, rdb, "task-2", "task-3", "task-4")
	if count, err := rdb.SCard(ctx, claimActive).Result(); err != nil || count != 2 {
		t.Fatalf("active=%d err=%v", count, err)
	}
	close(release)
	seen := map[string]bool{}
	for range 5 {
		select {
		case id := <-completed:
			if seen[id] {
				t.Fatalf("task executed twice: %s", id)
			}
			seen[id] = true
		case <-time.After(3 * time.Second):
			t.Fatal("pipeline did not execute all tasks")
		}
	}
	stopStartLoop(t, s)
	awaitStartExecutions(t, s)
	if maximum.Load() != 2 || running.Load() != 0 || len(s.Errors()) != 0 {
		t.Fatalf("maximum=%d running=%d errors=%d", maximum.Load(), running.Load(), len(s.Errors()))
	}
	for i := range 5 {
		fields := submitHash(t, rdb, fmt.Sprintf("task-%d", i))
		if fields["status"] != "completed" || fields["attempts"] != "1" || fields["owner_id"] != "" || fields["attempt_token"] != "" || fields["result"] != fmt.Sprintf("\"task-%d\"", i) || fields["finished_at"] == "" {
			t.Fatalf("completed task has wrong Redis state: %q", fields)
		}
	}
	requirePending(t, rdb)
	if err := rdb.Ping(ctx).Err(); err != nil {
		t.Fatalf("caller client closed: %v", err)
	}
}

func TestStartRedisLostClaimReply(t *testing.T) {
	admin := startSubmitRedis(t)
	ctx := context.Background()
	if _, err := redisstore.Claim(ctx, admin, claimParams("warm")); err != nil {
		t.Fatal(err)
	}
	c := integrationClient(t, admin)
	mustSubmit(t, c, "first", "job", nil)
	mustSubmit(t, c, "second", "job", nil)
	var armed, dropped atomic.Bool
	expected := []byte("*6\r\n$5\r\nfirst\r\n$3\r\njob\r\n$0\r\n\r\n$7\r\ndefault\r\n$1\r\n1\r\n$5\r\n60000\r\n")
	rdb := redis.NewClient(&redis.Options{
		Addr: admin.Options().Addr, MaxRetries: -1, ContextTimeoutEnabled: true, PoolSize: 1, ReadTimeout: time.Second, WriteTimeout: time.Second,
		Dialer: func(ctx context.Context, network, addr string) (net.Conn, error) {
			conn, err := (&net.Dialer{Timeout: time.Second}).DialContext(ctx, network, addr)
			if err != nil {
				return nil, err
			}
			return &dropSubmitReplyConn{Conn: conn, armed: &armed, dropped: &dropped, expected: expected}, nil
		},
	})
	t.Cleanup(func() { _ = rdb.Close() })
	if err := rdb.Ping(ctx).Err(); err != nil {
		t.Fatal(err)
	}
	s := integrationServer(t, rdb, WithConcurrency(1))
	nextAttempt := make(chan struct{})
	allowNext := make(chan struct{})
	release := make(chan struct{})
	t.Cleanup(func() {
		for _, gate := range []chan struct{}{allowNext, release} {
			select {
			case <-gate:
			default:
				close(gate)
			}
		}
	})
	generated := 0
	s.newAttemptToken = func() (string, error) {
		generated++
		switch generated {
		case 1:
			return "lost-token", nil
		case 2:
			close(nextAttempt)
			<-allowNext
			return "new-token", nil
		default:
			return "", errors.New("unexpected extra claim")
		}
	}
	started := make(chan string, 2)
	completed := make(chan struct{}, 1)
	if err := s.HandleFunc("job", func(_ context.Context, task *Task) error { started <- task.ID; <-release; return nil }); err != nil {
		t.Fatal(err)
	}
	s.onComplete = func(ctx context.Context, record *execution, key executionKey, result []byte) {
		s.complete(ctx, record, key, result)
		completed <- struct{}{}
	}
	armed.Store(true)
	if err := s.Start(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-nextAttempt:
	case <-time.After(3 * time.Second):
		t.Fatal("lost reply did not lead to a new independent attempt")
	}
	if !dropped.Load() || len(started) != 0 || len(s.snapshotExecutions()) != 0 || len(s.Errors()) != 0 {
		t.Fatal("unknown claim was executed, registered or treated as fatal")
	}
	requireClaimState(t, admin, &redisstore.ClaimResult{TaskID: "first", OwnerID: s.ownerID, AttemptToken: "lost-token", Attempts: 1})
	requirePending(t, admin, "second")
	// 后续轮询是新领取；在第二次 token 生成处暂停，证明第一响应丢失后未透明多领。
	close(allowNext)
	select {
	case id := <-started:
		if id != "second" {
			t.Fatalf("executed unknown task %q", id)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("new independent claim did not execute second task")
	}
	requireClaimState(t, admin, &redisstore.ClaimResult{TaskID: "second", OwnerID: s.ownerID, AttemptToken: "new-token", Attempts: 1})
	stopStartLoop(t, s)
	close(release)
	select {
	case <-completed:
	case <-time.After(3 * time.Second):
		t.Fatal("known task did not finish locally")
	}
	awaitStartExecutions(t, s)
	if generated != 2 || len(started) != 0 || len(s.Errors()) != 0 {
		t.Fatal("unexpected extra attempt, handler or fatal error")
	}
	requireClaimState(t, admin, &redisstore.ClaimResult{TaskID: "first", OwnerID: s.ownerID, AttemptToken: "lost-token", Attempts: 1})
}
