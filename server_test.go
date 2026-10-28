package taskq

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

func newTestServer(t *testing.T, opts ...Option) *server {
	t.Helper()
	rdb := redis.NewClient(&redis.Options{Addr: "127.0.0.1:1"})
	t.Cleanup(func() { _ = rdb.Close() })
	s, err := NewServer(rdb, opts...)
	if err != nil {
		t.Fatal(err)
	}
	return s.(*server)
}

func TestNewServer(t *testing.T) {
	s := newTestServer(t)
	if cap(s.slots) != 10 || s.leaseDuration != 30*time.Second || s.renewInterval != 6*time.Second || s.reclaimInterval != 10*time.Second {
		t.Fatal("defaults changed")
	}
	if s.handlers == nil || s.executions == nil || s.status != serverCreated || s.ownerID != "" {
		t.Fatal("invalid initial state")
	}
	if _, err := NewServer(nil); err == nil {
		t.Fatal("nil Redis client accepted")
	}
	cases := [][]Option{
		{nil}, {WithConcurrency(0)}, {WithConcurrency(-1)},
		{WithLeaseDuration(time.Second)}, {WithRenewInterval(0)}, {WithReclaimInterval(-time.Second)},
		{WithLeaseDuration(6 * time.Second)},
	}
	for i, opts := range cases {
		if _, err := NewServer(s.redisClient, opts...); err == nil {
			t.Errorf("invalid configuration %d accepted", i)
		}
	}
	for _, opts := range [][]Option{
		{WithLeaseDuration(6 * time.Second), WithRenewInterval(2 * time.Second)},
		{WithRenewInterval(2 * time.Second), WithLeaseDuration(6 * time.Second)},
	} {
		if _, err := NewServer(s.redisClient, opts...); err != nil {
			t.Fatal(err)
		}
	}
}

func TestRegistrationAndStartup(t *testing.T) {
	s := newTestServer(t)
	if err := s.beginStart(); err == nil {
		t.Fatal("started without handlers")
	}
	if s.status != serverCreated || s.ownerID != "" {
		t.Fatal("failed precheck changed state")
	}
	originalErr := errors.New("original")
	fn := func(context.Context, *Task) error { return originalErr }
	if err := s.HandleFunc("", fn); err == nil {
		t.Fatal("empty type accepted")
	}
	if err := s.HandleFunc("job", nil); err == nil {
		t.Fatal("nil handler accepted")
	}
	if err := s.HandleFunc("job", fn); err != nil {
		t.Fatal(err)
	}
	if err := s.HandleFunc("job", func(context.Context, *Task) error { return nil }); err == nil {
		t.Fatal("duplicate accepted")
	}
	if err := s.handlers["job"](context.Background(), &Task{}); err != originalErr {
		t.Fatal("original overwritten")
	}
	if err := s.beginStart(); err != nil {
		t.Fatal(err)
	}
	if s.ownerID == "" || s.status != serverStarting {
		t.Fatal("startup did not assign identity and state")
	}
	owner := s.ownerID
	if err := s.beginStart(); err == nil || s.ownerID != owner {
		t.Fatal("duplicate startup accepted or identity changed")
	}
	for _, status := range []serverStatus{serverStarting, serverRunning, serverStopping, serverStopped} {
		s.status = status
		if err := s.HandleFunc("later", fn); err == nil {
			t.Fatalf("registration accepted in state %d", status)
		}
		if err := s.beginStart(); err == nil {
			t.Fatalf("startup accepted in state %d", status)
		}
	}
}

func TestConcurrentStartupAndRegistration(t *testing.T) {
	s := newTestServer(t)
	fn := func(context.Context, *Task) error { return nil }
	if err := s.HandleFunc("initial", fn); err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	var starts atomic.Int32
	for i := 0; i < 64; i++ {
		wg.Add(2)
		go func() {
			defer wg.Done()
			if s.beginStart() == nil {
				starts.Add(1)
			}
		}()
		go func(i int) { defer wg.Done(); _ = s.HandleFunc(fmt.Sprintf("type-%d", i), fn) }(i)
	}
	wg.Wait()
	if starts.Load() != 1 {
		t.Fatalf("successful startups: %d", starts.Load())
	}
	if err := s.HandleFunc("late", fn); err == nil {
		t.Fatal("registration accepted after startup")
	}
}

func TestExecutionAttemptsAndSnapshots(t *testing.T) {
	s := newTestServer(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	e := &execution{executionTimeout: time.Second, cancel: cancel}
	oldKey := executionKey{taskID: "task", attemptToken: "old"}
	newKey := executionKey{taskID: "task", attemptToken: "new"}
	if err := s.addExecution(oldKey, e); err != nil {
		t.Fatal(err)
	}
	if err := s.addExecution(newKey, e); err != nil {
		t.Fatal(err)
	}
	if err := s.addExecution(oldKey, e); err == nil {
		t.Fatal("duplicate attempt accepted")
	}
	snapshot := s.snapshotExecutions()
	delete(snapshot, newKey)
	if _, ok := s.getExecution(newKey); !ok {
		t.Fatal("snapshot modified live map")
	}
	s.deleteExecution(oldKey)
	if _, ok := s.getExecution(oldKey); ok {
		t.Fatal("old attempt not deleted")
	}
	if _, ok := s.getExecution(newKey); !ok {
		t.Fatal("new attempt deleted with old attempt")
	}
	// A callback may access the table: cancellation must occur outside its lock.
	callbackKey := executionKey{taskID: "callback", attemptToken: "token"}
	if err := s.addExecution(callbackKey, &execution{executionTimeout: time.Second, cancel: func() {
		s.deleteExecution(callbackKey)
		cancel()
	}}); err != nil {
		t.Fatal(err)
	}
	for _, record := range s.snapshotExecutions() {
		record.cancel()
	}
	if ctx.Err() == nil {
		t.Fatal("cancel not invoked")
	}
}

func TestConcurrentExecutionTable(t *testing.T) {
	s := newTestServer(t)
	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for j := 0; j < 50; j++ {
				key := executionKey{taskID: "shared-task", attemptToken: fmt.Sprintf("%d-%d", i, j)}
				e := &execution{executionTimeout: time.Second, cancel: func() {}}
				if err := s.addExecution(key, e); err != nil {
					t.Error(err)
					return
				}
				if got, ok := s.getExecution(key); !ok || got != e {
					t.Error("execution missing")
				}
				_ = s.snapshotExecutions()
				s.deleteExecution(key)
			}
		}(i)
	}
	wg.Wait()
	if len(s.snapshotExecutions()) != 0 {
		t.Fatal("execution leak")
	}
}

func TestSetResultOwnsJSON(t *testing.T) {
	task := &Task{}
	original := json.RawMessage(`{"value":1}`)
	if err := task.SetResult(original); err != nil {
		t.Fatal(err)
	}
	original[9] = '2'
	if string(task.result) != `{"value":1}` {
		t.Fatalf("result aliases input: %s", task.result)
	}
	saved := string(task.result)
	if err := task.SetResult(make(chan int)); err == nil {
		t.Fatal("unencodable result accepted")
	}
	if err := task.SetResult(strings.Repeat("x", MaxResultSize)); err == nil {
		t.Fatal("oversized result accepted")
	}
	if string(task.result) != saved {
		t.Fatal("failed SetResult overwrote result")
	}
	if err := task.SetResult("replacement"); err != nil {
		t.Fatal(err)
	}
	if string(task.result) != `"replacement"` {
		t.Fatal("result not replaced")
	}
}
