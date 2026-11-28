//go:build integration

package taskq

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/wang-kang-tuoinai/TaskHarbor/internal/redisstore"
)

func TestClaimServerConfiguredQueuesShareSlots(t *testing.T) {
	rdb := startSubmitRedis(t)
	c := integrationClient(t, rdb)
	ctx := context.Background()
	queues := []string{"empty", "mail", "sms"}
	s := integrationServer(t, rdb, WithQueues(queues...), WithConcurrency(1))
	queues[0], queues[1] = "default", "outside"
	mustSubmit(t, c, "default-task", "job", nil)
	mustSubmit(t, c, "mail-task", "job", nil, WithSubmitQueue("mail"))
	mustSubmit(t, c, "sms-task", "job", nil, WithSubmitQueue("sms"))
	started := make(chan string, 3)
	if err := s.HandleFunc("job", func(ctx context.Context, task *Task) error {
		started <- task.ID
		<-ctx.Done()
		return ctx.Err()
	}); err != nil {
		t.Fatal(err)
	}
	if err := s.Start(); err != nil {
		t.Fatal(err)
	}
	if id := awaitClaimSignal(t, started, 2*time.Second); id != "mail-task" {
		t.Fatalf("first task = %q, want mail-task after empty queue", id)
	}
	if got := requireTaskSnapshot(t, c, "mail-task", "running", 1); got.Queue != "mail" {
		t.Fatalf("claimed task stored queue = %q", got.Queue)
	}
	if active, err := rdb.SIsMember(ctx, "taskq:mail:active", "mail-task").Result(); err != nil || !active {
		t.Fatalf("mail task missing from own active index: active=%t, err=%v", active, err)
	}
	if _, err := rdb.ZScore(ctx, "taskq:mail:lease", "mail-task").Result(); err != nil {
		t.Fatalf("mail task missing from own lease index: %v", err)
	}
	select {
	case id := <-started:
		t.Fatalf("started %q while the shared slot was occupied", id)
	case <-time.After(2 * claimPollInterval):
	}
	requireTaskSnapshot(t, c, "sms-task", "pending", 0)
	requireTaskSnapshot(t, c, "default-task", "pending", 0)
}

func TestClaimServersCompeteForSameCustomQueue(t *testing.T) {
	rdb := startSubmitRedis(t)
	c := integrationClient(t, rdb)
	servers := []Server{
		integrationServer(t, rdb, WithQueues("mail"), WithConcurrency(1)),
		integrationServer(t, rdb, WithQueues("mail"), WithConcurrency(1)),
	}
	started := make(chan string, 4)
	release := make(chan struct{})
	for _, s := range servers {
		if err := s.HandleFunc("job", func(ctx context.Context, task *Task) error {
			started <- task.ID
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-release:
				return nil
			}
		}); err != nil {
			t.Fatal(err)
		}
		if err := s.Start(); err != nil {
			t.Fatal(err)
		}
	}
	for _, id := range []string{"first", "second"} {
		mustSubmit(t, c, id, "job", nil, WithSubmitQueue("mail"))
	}
	seen := make(map[string]bool)
	for range servers {
		id := awaitClaimSignal(t, started, 2*time.Second)
		if (id != "first" && id != "second") || seen[id] {
			t.Fatalf("competing Servers started unexpected or repeated task %q", id)
		}
		seen[id] = true
		requireTaskSnapshot(t, c, id, "running", 1)
	}
	first, second := submitHash(t, rdb, "first"), submitHash(t, rdb, "second")
	if first["owner_id"] == "" || second["owner_id"] == "" || first["owner_id"] == second["owner_id"] {
		t.Fatal("both Servers did not acquire separate execution attempts")
	}
	close(release)
	for _, s := range servers {
		shutdownClaimServer(t, s)
	}
	for _, id := range []string{"first", "second"} {
		requireTaskSnapshot(t, c, id, "completed", 1)
	}
	select {
	case id := <-started:
		t.Fatalf("continued Handler execution after both Servers shut down: %q", id)
	default:
	}
}

func TestRedisExecutionQueueCheckFollowsTokenCheck(t *testing.T) {
	rdb := startSubmitRedis(t)
	ctx := context.Background()
	const id, queue, token = "job", "mail", "current"
	const meta = "taskq:meta:" + id
	mustSubmit(t, integrationClient(t, rdb), id, "job", nil, WithSubmitQueue(queue), WithSubmitMaxRetry(1))
	params := claimParams(token)
	params.Queue = queue
	if claimed, err := redisstore.Claim(ctx, rdb, params); err != nil || claimed == nil {
		t.Fatalf("Claim = %+v, %v", claimed, err)
	}
	if _, err := rdb.Pipelined(ctx, func(pipe redis.Pipeliner) error {
		pipe.SAdd(ctx, "taskq:sms:active", id)
		pipe.ZAdd(ctx, "taskq:sms:lease", redis.Z{Score: 4_000_000_000_000, Member: id})
		pipe.ZAdd(ctx, "taskq:sms:retry", redis.Z{Score: 4_000_000_000_001, Member: id})
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	operations := []struct {
		name string
		call func(string) (bool, error)
		want error
	}{
		{"Renew", func(token string) (bool, error) {
			return redisstore.Renew(ctx, rdb, redisstore.RenewParams{ExpectedQueue: queue, TaskID: id, AttemptToken: token, LeaseDurationMS: 60000})
		}, redisstore.ErrRenewInconsistent},
		{"Complete", func(token string) (bool, error) {
			return redisstore.Complete(ctx, rdb, redisstore.CompleteParams{ExpectedQueue: queue, TaskID: id, AttemptToken: token})
		}, redisstore.ErrReportInconsistent},
		{"ReportFailure", func(token string) (bool, error) {
			return redisstore.ReportFailure(ctx, rdb, redisstore.FailureParams{ExpectedQueue: queue, TaskID: id, AttemptToken: token, ErrorMessage: "failure"})
		}, redisstore.ErrReportInconsistent},
	}
	for _, tc := range []struct {
		name string
		edit func() error
	}{
		{"missing", func() error { return rdb.HDel(ctx, meta, "queue").Err() }},
		{"mismatched", func() error { return rdb.HSet(ctx, meta, "queue", "sms").Err() }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if err := tc.edit(); err != nil {
				t.Fatal(err)
			}
			for _, op := range operations {
				t.Run(op.name, func(t *testing.T) {
					before := claimSnapshot(t, rdb)
					if applied, err := op.call("stale"); applied || err != nil {
						t.Fatalf("old token = %t, %v, want ordinary rejection", applied, err)
					}
					if applied, err := op.call(token); applied || !errors.Is(err, op.want) {
						t.Fatalf("current token = %t, %v, want %v", applied, err, op.want)
					}
					if !reflect.DeepEqual(claimSnapshot(t, rdb), before) {
						t.Fatal("queue validation changed task state or indexes")
					}
				})
			}
		})
	}
}

func TestClaimServerMalformedHeadStopsClaimAndKeepsHealthyWork(t *testing.T) {
	rdb := startSubmitRedis(t)
	c := integrationClient(t, rdb)
	ctx := context.Background()
	mustSubmit(t, c, "in-flight", "job", nil, WithSubmitQueue("mail"))
	mustSubmit(t, c, "later", "job", nil, WithSubmitQueue("mail"))
	const badID = "foreign-head"
	mustSubmit(t, c, badID, "job", nil, WithSubmitQueue("foreign"))
	if err := rdb.RPush(ctx, "taskq:sms:pending", badID).Err(); err != nil {
		t.Fatal(err)
	}
	before := claimSnapshot(t, rdb)
	s := integrationServer(t, rdb, WithQueues("mail", "sms"), WithConcurrency(2))
	started := make(chan string, 3)
	release := make(chan struct{})
	if err := s.HandleFunc("job", func(ctx context.Context, task *Task) error {
		started <- task.ID
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-release:
			return task.SetResult("finished")
		}
	}); err != nil {
		t.Fatal(err)
	}
	if err := s.Start(); err != nil {
		t.Fatal(err)
	}
	if id := awaitClaimSignal(t, started, 2*time.Second); id != "in-flight" {
		t.Fatalf("started %q, want in-flight", id)
	}
	err := awaitClaimSignal(t, s.Errors(), 2*time.Second)
	if !errors.Is(err, redisstore.ErrClaimInconsistent) || !strings.Contains(err.Error(), `queue "sms"`) || !strings.Contains(err.Error(), badID) {
		t.Fatalf("fatal claim error = %v", err)
	}
	after := claimSnapshot(t, rdb)
	for _, key := range []string{"taskq:meta:" + badID, "taskq:sms:pending", "taskq:sms:active", "taskq:sms:lease", "taskq:foreign:pending", "taskq:foreign:active", "taskq:foreign:lease"} {
		if after[key] != before[key] {
			t.Fatalf("inconsistent Claim changed %q", key)
		}
	}
	close(release)
	awaitReportState(t, rdb, "in-flight", "completed")
	shutdownClaimServer(t, s)
	requireTaskSnapshot(t, c, "later", "pending", 0)
	select {
	case id := <-started:
		t.Fatalf("stopped claim loop executed %q", id)
	default:
	}
}

func TestServerRotatesRenewsAndReportsByQueue(t *testing.T) {
	rdb := startSubmitRedis(t)
	c := integrationClient(t, rdb)
	ctx := context.Background()
	for _, job := range []struct{ id, queue string }{
		{"mail-first", "mail"}, {"mail-second", "mail"}, {"sms-first", "sms"}, {"sms-second", "sms"},
	} {
		mustSubmit(t, c, job.id, job.queue, nil, WithSubmitQueue(job.queue), WithSubmitMaxRetry(1))
	}
	// 残留在另一队列的同 ID 成员可暴露误用 default 索引的续租和上报。
	const future = 4_000_000_000_000
	if _, err := rdb.Pipelined(ctx, func(pipe redis.Pipeliner) error {
		for _, id := range []string{"mail-first", "sms-first"} {
			pipe.SAdd(ctx, claimActive, id)
			pipe.ZAdd(ctx, claimLease, redis.Z{Score: future, Member: id})
			pipe.ZAdd(ctx, "taskq:default:retry", redis.Z{Score: future, Member: id})
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	before := claimSnapshot(t, rdb)
	s := integrationServer(t, rdb, WithQueues("mail", "sms"), WithConcurrency(1), WithRenewInterval(2*time.Second))
	started := make(chan string, 4)
	release := make(chan struct{})
	for _, taskType := range []string{"mail", "sms"} {
		if err := s.HandleFunc(taskType, func(ctx context.Context, task *Task) error {
			started <- task.ID
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-release:
			}
			if task.ID == "sms-first" {
				return errors.New("phone unavailable")
			}
			return task.SetResult(task.ID)
		}); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.Start(); err != nil {
		t.Fatal(err)
	}
	if id := awaitClaimSignal(t, started, time.Second); id != "mail-first" {
		t.Fatalf("first handler = %q, want mail-first", id)
	}
	lease, err := rdb.ZScore(ctx, "taskq:mail:lease", "mail-first").Result()
	if err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(3 * time.Second)
	for {
		got, err := rdb.ZScore(ctx, "taskq:mail:lease", "mail-first").Result()
		if err != nil {
			t.Fatal(err)
		}
		if got > lease {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("mail lease did not advance while Handler was running")
		}
		time.Sleep(20 * time.Millisecond)
	}
	for _, id := range []string{"sms-first", "mail-second", "sms-second"} {
		select {
		case release <- struct{}{}:
		case <-time.After(time.Second):
			t.Fatal("running Handler did not receive release")
		}
		if got := awaitClaimSignal(t, started, time.Second); got != id {
			t.Fatalf("next handler = %q, want %q", got, id)
		}
	}
	close(release)
	for _, id := range []string{"mail-first", "mail-second", "sms-second"} {
		awaitReportState(t, rdb, id, "completed")
		if got := requireTaskSnapshot(t, c, id, "completed", 1); string(got.Result) != `"`+id+`"` {
			t.Fatalf("result for %q = %s", id, got.Result)
		}
	}
	if got := awaitReportState(t, rdb, "sms-first", "retry_wait"); got["last_error"] != "phone unavailable" {
		t.Fatalf("saved failure = %q", got["last_error"])
	}
	if got, err := rdb.ZRange(ctx, "taskq:sms:retry", 0, -1).Result(); err != nil || !reflect.DeepEqual(got, []string{"sms-first"}) {
		t.Fatalf("sms retry members = %q, %v", got, err)
	}
	if n, err := rdb.Exists(ctx, "taskq:mail:active", "taskq:mail:lease", "taskq:mail:retry", "taskq:sms:active", "taskq:sms:lease").Result(); err != nil || n != 0 {
		t.Fatalf("remaining execution or unexpected retry indexes = %d, %v", n, err)
	}
	after := claimSnapshot(t, rdb)
	for _, key := range []string{claimActive, claimLease, "taskq:default:retry"} {
		if after[key] != before[key] {
			t.Fatalf("execution changed unrelated index %q", key)
		}
	}
}
