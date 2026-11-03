//go:build integration

package taskq

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/wang-kang-tuoinai/TaskHarbor/internal/redisstore"
)

const (
	claimPending = "taskq:default:pending"
	claimActive  = "taskq:default:active"
	claimLease   = "taskq:default:lease"
)

func claimParams(token string) redisstore.ClaimParams {
	return redisstore.ClaimParams{OwnerID: "owner", AttemptToken: token, LeaseDurationMS: 30000}
}

// 比较临时实例中的全部业务 key，证明失败前没有任何可预见的部分写入。
func claimSnapshot(t *testing.T, rdb *redis.Client) map[string]string {
	t.Helper()
	ctx := context.Background()
	keys, err := rdb.Keys(ctx, "*").Result()
	if err != nil {
		t.Fatal(err)
	}
	snapshot := make(map[string]string, len(keys))
	for _, key := range keys {
		value, err := rdb.Dump(ctx, key).Result()
		if err != nil {
			t.Fatal(err)
		}
		snapshot[key] = value
	}
	return snapshot
}

func requireClaimState(t *testing.T, rdb *redis.Client, task *redisstore.ClaimResult) {
	t.Helper()
	ctx := context.Background()
	fields := submitHash(t, rdb, task.TaskID)
	if fields["status"] != "running" || fields["owner_id"] != task.OwnerID || fields["attempt_token"] != task.AttemptToken || fields["attempts"] != strconv.Itoa(task.Attempts) {
		t.Fatalf("task=%+v hash=%q", task, fields)
	}
	if active, err := rdb.SIsMember(ctx, claimActive, task.TaskID).Result(); err != nil || !active {
		t.Fatalf("active=%v err=%v", active, err)
	}
	claimed, err := strconv.ParseInt(fields["last_claimed_at"], 10, 64)
	if err != nil {
		t.Fatal(err)
	}
	if score, err := rdb.ZScore(ctx, claimLease, task.TaskID).Result(); err != nil || score != float64(claimed+30000) {
		t.Fatalf("lease=%v claimed=%d err=%v", score, claimed, err)
	}
}

func TestClaimRedis(t *testing.T) {
	rdb := startSubmitRedis(t)
	ctx := context.Background()
	c := integrationClient(t, rdb)
	if task, err := redisstore.Claim(ctx, rdb, claimParams("empty")); task != nil || err != nil {
		t.Fatalf("empty task=%+v err=%v", task, err)
	}
	payload := []byte{0, 255, 128, '\r', '\n'}
	id := " first\x00\r\n "
	mustSubmit(t, c, id, "job", payload, WithSubmitMaxRetry(3), WithSubmitExecutionTimeout(time.Millisecond))
	mustSubmit(t, c, "second", "job", nil)
	if err := rdb.HSet(ctx, "taskq:meta:"+id, "attempts", "2", "last_error", "previous failure", "last_failed_at", "123").Err(); err != nil {
		t.Fatal(err)
	}
	before := submitHash(t, rdb, id)
	start, err := rdb.Time(ctx).Result()
	if err != nil {
		t.Fatal(err)
	}
	// 缓存缺失允许 EVAL 回退，成功只消耗一次尝试预算。
	if err := rdb.ScriptFlush(ctx).Err(); err != nil {
		t.Fatal(err)
	}
	task, err := redisstore.Claim(ctx, rdb, claimParams("first-token"))
	if err != nil || task == nil {
		t.Fatalf("task=%+v err=%v", task, err)
	}
	want := &redisstore.ClaimResult{TaskID: id, TaskType: "job", Payload: payload, Queue: "default", Attempts: 3, ExecutionTimeoutMS: 1, OwnerID: "owner", AttemptToken: "first-token"}
	if !reflect.DeepEqual(task, want) {
		t.Fatalf("task=%+v want=%+v", task, want)
	}
	requireClaimState(t, rdb, task)
	end, err := rdb.Time(ctx).Result()
	if err != nil {
		t.Fatal(err)
	}
	after := submitHash(t, rdb, id)
	claimed, err := strconv.ParseInt(after["last_claimed_at"], 10, 64)
	if err != nil || claimed < start.UnixMilli() || claimed > end.UnixMilli() {
		t.Fatalf("claimed=%d err=%v", claimed, err)
	}
	before["status"], before["attempts"] = "running", "3"
	before["owner_id"], before["attempt_token"] = "owner", "first-token"
	before["last_claimed_at"] = after["last_claimed_at"]
	if !reflect.DeepEqual(before, after) {
		t.Fatalf("unexpected hash changes: before=%q after=%q", before, after)
	}
	requirePending(t, rdb, "second")
	second, err := redisstore.Claim(ctx, rdb, claimParams("second-token"))
	if err != nil || second == nil || second.TaskID != "second" || second.Attempts != 1 || len(second.Payload) != 0 || second.AttemptToken != "second-token" {
		t.Fatalf("second=%+v err=%v", second, err)
	}
	requireClaimState(t, rdb, second)
	// 后一次调用不能覆盖之前返回对象的执行身份。
	if task.AttemptToken != "first-token" {
		t.Fatal("previous token changed")
	}
	requirePending(t, rdb)
	if task, err := redisstore.Claim(ctx, rdb, claimParams("empty-again")); task != nil || err != nil {
		t.Fatalf("task=%+v err=%v", task, err)
	}
}

func TestClaimRedisRejectsWithoutWrites(t *testing.T) {
	rdb := startSubmitRedis(t)
	ctx := context.Background()
	c := integrationClient(t, rdb)
	type scenario struct {
		name   string
		mutate func() error
		want   error
	}
	cases := []scenario{
		{"missing-record", func() error { return rdb.Del(ctx, "taskq:meta:id").Err() }, redisstore.ErrClaimInconsistent},
		{"active-residue", func() error { return rdb.SAdd(ctx, claimActive, "id").Err() }, redisstore.ErrClaimInconsistent},
		{"lease-residue", func() error { return rdb.ZAdd(ctx, claimLease, redis.Z{Member: "id", Score: 1}).Err() }, redisstore.ErrClaimInconsistent},
	}
	for _, key := range []string{claimPending, claimActive, claimLease, "taskq:meta:id"} {
		cases = append(cases, scenario{"wrong-type-" + key, func() error { return rdb.Set(ctx, key, "sentinel", 0).Err() }, nil})
	}
	for _, field := range []string{"task_id", "task_type", "payload", "queue", "status", "attempts", "max_retry", "execution_timeout_ms"} {
		cases = append(cases, scenario{"missing-" + field, func() error { return rdb.HDel(ctx, "taskq:meta:id", field).Err() }, redisstore.ErrClaimInconsistent})
	}
	for field, values := range map[string][]string{
		"task_id": {"", "different"}, "task_type": {""}, "queue": {"other", ""},
		"status":               {"running", "retry_wait", "completed", "failed", "canceled", "unknown"},
		"attempts":             {"1", "-1", "1.5", "1e0", "nan", "inf", "", "2147483648", "9007199254740993", "999999999999999999999999999999"},
		"max_retry":            {"-1", "1.5", "1e0", "nan", "", "2147483647", "9007199254740993"},
		"execution_timeout_ms": {"0", "-1", "1.5", "", "86400001", "9223372036854775808"},
		"owner_id":             {"old-owner", ""}, "attempt_token": {"old-token", ""},
	} {
		for _, value := range values {
			cases = append(cases, scenario{field + "=" + value, func() error { return rdb.HSet(ctx, "taskq:meta:id", field, value).Err() }, redisstore.ErrClaimInconsistent})
		}
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if err := rdb.FlushDB(ctx).Err(); err != nil {
				t.Fatal(err)
			}
			mustSubmit(t, c, "id", "job", nil)
			mustSubmit(t, c, "second", "job", nil)
			if err := tc.mutate(); err != nil {
				t.Fatal(err)
			}
			before := claimSnapshot(t, rdb)
			task, err := redisstore.Claim(ctx, rdb, claimParams("token"))
			if task != nil || err == nil {
				t.Fatalf("task=%+v err=%v", task, err)
			}
			if tc.want != nil {
				if !errors.Is(err, tc.want) || !strings.Contains(err.Error(), "task id:") {
					t.Fatalf("classification/identity: %v", err)
				}
			} else {
				var redisErr redis.Error
				if !errors.As(err, &redisErr) || !strings.Contains(err.Error(), "WRONGTYPE") {
					t.Fatalf("storage error chain: %v", err)
				}
			}
			if !reflect.DeepEqual(before, claimSnapshot(t, rdb)) {
				t.Fatal("rejected candidate changed storage")
			}
		})
	}
	t.Run("invalid-request", func(t *testing.T) {
		if err := rdb.FlushDB(ctx).Err(); err != nil {
			t.Fatal(err)
		}
		mustSubmit(t, c, "id", "job", nil)
		before := claimSnapshot(t, rdb)
		for _, p := range []redisstore.ClaimParams{
			{OwnerID: "", AttemptToken: "token", LeaseDurationMS: 1},
			{OwnerID: "owner", AttemptToken: "", LeaseDurationMS: 1},
			{OwnerID: "owner", AttemptToken: "token", LeaseDurationMS: 0},
			{OwnerID: "owner", AttemptToken: "token", LeaseDurationMS: 1 << 53},
		} {
			if task, err := redisstore.Claim(ctx, rdb, p); task != nil || !errors.Is(err, redisstore.ErrClaimArgument) {
				t.Fatalf("task=%+v err=%v", task, err)
			}
		}
		if !reflect.DeepEqual(before, claimSnapshot(t, rdb)) {
			t.Fatal("invalid request changed storage")
		}
	})
	t.Run("lease-addition-overflow", func(t *testing.T) {
		if err := rdb.FlushDB(ctx).Err(); err != nil {
			t.Fatal(err)
		}
		mustSubmit(t, c, "id", "job", nil)
		before := claimSnapshot(t, rdb)
		p := claimParams("token")
		p.LeaseDurationMS = 1<<53 - 1
		task, err := redisstore.Claim(ctx, rdb, p)
		var redisErr redis.Error
		if task != nil || !errors.As(err, &redisErr) || !strings.Contains(err.Error(), "exact integer range") {
			t.Fatalf("task=%+v err=%v", task, err)
		}
		if !reflect.DeepEqual(before, claimSnapshot(t, rdb)) {
			t.Fatal("overflow changed storage")
		}
	})
	t.Run("largest-attempt-budget", func(t *testing.T) {
		if err := rdb.FlushDB(ctx).Err(); err != nil {
			t.Fatal(err)
		}
		mustSubmit(t, c, "id", "job", nil, WithSubmitMaxRetry(MaxSubmitRetries))
		if err := rdb.HSet(ctx, "taskq:meta:id", "attempts", "2147483646").Err(); err != nil {
			t.Fatal(err)
		}
		task, err := redisstore.Claim(ctx, rdb, claimParams("last"))
		if err != nil || task == nil || task.Attempts != 2147483647 {
			t.Fatalf("task=%+v err=%v", task, err)
		}
		requireClaimState(t, rdb, task)
	})
}

func TestClaimRedisConcurrent(t *testing.T) {
	rdb := startSubmitRedis(t)
	ctx := context.Background()
	c := integrationClient(t, rdb)
	var clients []*redis.Client
	for range 4 {
		client := redis.NewClient(&redis.Options{Addr: rdb.Options().Addr, MaxRetries: -1})
		t.Cleanup(func() { _ = client.Close() })
		clients = append(clients, client)
	}
	for _, count := range []int{1, 40} {
		t.Run(fmt.Sprintf("tasks-%d", count), func(t *testing.T) {
			if err := rdb.FlushDB(ctx).Err(); err != nil {
				t.Fatal(err)
			}
			for i := range count {
				mustSubmit(t, c, fmt.Sprintf("task-%d", i), "job", nil)
			}
			results := make(chan *redisstore.ClaimResult, 64)
			var wg sync.WaitGroup
			for i := range 64 {
				wg.Go(func() {
					p := claimParams(fmt.Sprintf("token-%d", i))
					p.OwnerID = fmt.Sprintf("owner-%d", i%4)
					task, err := redisstore.Claim(ctx, clients[i%4], p)
					if err != nil {
						t.Error(err)
						return
					}
					if task != nil {
						if task.OwnerID != p.OwnerID || task.AttemptToken != p.AttemptToken {
							t.Errorf("identity mismatch: %+v", task)
						}
						results <- task
					}
				})
			}
			wg.Wait()
			close(results)
			seen := make(map[string]bool)
			for task := range results {
				if seen[task.TaskID] || task.Attempts != 1 {
					t.Fatalf("duplicate or wrong attempts: %+v", task)
				}
				seen[task.TaskID] = true
				requireClaimState(t, rdb, task)
			}
			if len(seen) != count {
				t.Fatalf("claimed=%d want=%d", len(seen), count)
			}
			requirePending(t, rdb)
			if n, err := rdb.SCard(ctx, claimActive).Result(); err != nil || n != int64(count) {
				t.Fatalf("active=%d err=%v", n, err)
			}
			if n, err := rdb.ZCard(ctx, claimLease).Result(); err != nil || n != int64(count) {
				t.Fatalf("lease=%d err=%v", n, err)
			}
		})
	}
}

func TestClaimRedisLostReply(t *testing.T) {
	admin := startSubmitRedis(t)
	ctx := context.Background()
	// 在空队列上预热脚本，故障点将直接命中成功 EVALSHA 的六字段响应。
	if _, err := redisstore.Claim(ctx, admin, claimParams("warm")); err != nil {
		t.Fatal(err)
	}
	c := integrationClient(t, admin)
	mustSubmit(t, c, "first", "job", nil)
	mustSubmit(t, c, "second", "job", nil)
	var armed, dropped atomic.Bool
	expected := []byte("*6\r\n$5\r\nfirst\r\n$3\r\njob\r\n$0\r\n\r\n$7\r\ndefault\r\n$1\r\n1\r\n$5\r\n60000\r\n")
	rdb := redis.NewClient(&redis.Options{
		Addr: admin.Options().Addr, MaxRetries: -1, PoolSize: 1,
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
	armed.Store(true)
	task, err := redisstore.Claim(ctx, rdb, claimParams("lost-token"))
	if !dropped.Load() || task != nil || !(errors.Is(err, io.EOF) || errors.Is(err, net.ErrClosed)) {
		t.Fatalf("dropped=%v task=%+v err=%v", dropped.Load(), task, err)
	}
	requireClaimState(t, admin, &redisstore.ClaimResult{TaskID: "first", OwnerID: "owner", AttemptToken: "lost-token", Attempts: 1})
	requirePending(t, admin, "second")
	if fields := submitHash(t, admin, "second"); fields["status"] != "pending" || fields["attempts"] != "0" {
		t.Fatalf("second=%q", fields)
	}
	if admin.SCard(ctx, claimActive).Val() != 1 || admin.ZCard(ctx, claimLease).Val() != 1 {
		t.Fatal("unknown result replayed")
	}
}

// 在真实脚本已生效后破坏协议，验证解析失败不会猜测任务、回滚或重放。
type corruptClaimHook struct{ calls atomic.Int32 }

func (h *corruptClaimHook) DialHook(next redis.DialHook) redis.DialHook { return next }
func (h *corruptClaimHook) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return next
}
func (h *corruptClaimHook) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		err := next(ctx, cmd)
		if err == nil && (cmd.Name() == "eval" || cmd.Name() == "evalsha") {
			h.calls.Add(1)
			values, ok := cmd.(*redis.Cmd).Val().([]any)
			if !ok || len(values) != 6 {
				return fmt.Errorf("expected six business fields, got %#v", cmd.(*redis.Cmd).Val())
			}
			cmd.(*redis.Cmd).SetVal(append(values, "unexpected"))
		}
		return err
	}
}

func TestClaimRedisInvalidReply(t *testing.T) {
	admin := startSubmitRedis(t)
	ctx := context.Background()
	c := integrationClient(t, admin)
	mustSubmit(t, c, "first", "job", nil)
	mustSubmit(t, c, "second", "job", nil)
	rdb := redis.NewClient(&redis.Options{Addr: admin.Options().Addr, MaxRetries: -1})
	t.Cleanup(func() { _ = rdb.Close() })
	hook := &corruptClaimHook{}
	rdb.AddHook(hook)
	task, err := redisstore.Claim(ctx, rdb, claimParams("invalid-token"))
	if task != nil || !errors.Is(err, redisstore.ErrClaimResponse) || hook.calls.Load() != 1 {
		t.Fatalf("task=%+v err=%v calls=%d", task, err, hook.calls.Load())
	}
	requireClaimState(t, admin, &redisstore.ClaimResult{TaskID: "first", OwnerID: "owner", AttemptToken: "invalid-token", Attempts: 1})
	requirePending(t, admin, "second")
}
