//go:build integration

package taskq

import (
	"context"
	"errors"
	"fmt"
	"os"
	"reflect"
	"strconv"
	"strings"
	"testing"

	"github.com/redis/go-redis/v9"
	"github.com/wang-kang-tuoinai/TaskHarbor/internal/redisstore"
)

const reclaimRetry = "taskq:default:retry"

func TestReclaimRedisExpiredLeases(t *testing.T) {
	rdb := startSubmitRedis(t)
	ctx := context.Background()
	c := integrationClient(t, rdb)
	for _, task := range []struct {
		id       string
		maxRetry int
		expired  bool
	}{{"retry", 1, true}, {"failed", 0, true}, {"healthy", 1, false}} {
		mustSubmit(t, c, task.id, "job", nil, WithSubmitMaxRetry(task.maxRetry))
		if claim, err := redisstore.Claim(ctx, rdb, claimParams("old-"+task.id)); err != nil || claim == nil || claim.TaskID != task.id {
			t.Fatalf("claim=%+v err=%v", claim, err)
		}
		if task.expired {
			if err := rdb.ZAdd(ctx, claimLease, redis.Z{Member: task.id, Score: 1}).Err(); err != nil {
				t.Fatal(err)
			}
		}
	}
	// 终止任务不使用重试延迟；有额度的任务会清理残留的结束时间。
	if err := rdb.HDel(ctx, "taskq:meta:failed", "retry_delay_ms").Err(); err != nil {
		t.Fatal(err)
	}
	if err := rdb.HSet(ctx, "taskq:meta:retry", "finished_at", "1").Err(); err != nil {
		t.Fatal(err)
	}
	before, err := rdb.Time(ctx).Result()
	if err != nil {
		t.Fatal(err)
	}
	if err := redisstore.ReclaimLeases(ctx, rdb); err != nil {
		t.Fatal(err)
	}
	after, err := rdb.Time(ctx).Result()
	if err != nil {
		t.Fatal(err)
	}
	for id, status := range map[string]string{"retry": "retry_wait", "failed": "failed"} {
		hash := submitHash(t, rdb, id)
		failedAt, err := strconv.ParseInt(hash["last_failed_at"], 10, 64)
		if err != nil || failedAt < before.UnixMilli() || failedAt > after.UnixMilli() || hash["status"] != status || hash["attempts"] != "1" || hash["last_error"] != "lease expired" {
			t.Fatalf("%s: %v", id, hash)
		}
		for _, field := range []string{"owner_id", "attempt_token", "next_retry_at"} {
			if _, exists := hash[field]; exists {
				t.Fatalf("%s retained %s", id, field)
			}
		}
		score, err := rdb.ZScore(ctx, reclaimRetry, id).Result()
		if status == "retry_wait" {
			if _, finished := hash["finished_at"]; finished || err != nil || score != float64(failedAt+30000) {
				t.Fatalf("retry state=%v score=%v err=%v", hash, score, err)
			}
		} else if hash["finished_at"] != hash["last_failed_at"] || err != redis.Nil {
			t.Fatalf("terminal state=%v score=%v err=%v", hash, score, err)
		}
	}
	active, err := rdb.SMembers(ctx, claimActive).Result()
	if err != nil || len(active) != 1 || active[0] != "healthy" {
		t.Fatalf("active=%v err=%v", active, err)
	}
	leased, err := rdb.ZRange(ctx, claimLease, 0, -1).Result()
	if err != nil || len(leased) != 1 || leased[0] != "healthy" || submitHash(t, rdb, "healthy")["status"] != "running" {
		t.Fatalf("leased=%v err=%v", leased, err)
	}
	requirePending(t, rdb)
}

// 用真实 Submit、Claim 和上报建立候选，只控制源 score 来避免等待到期。
func seedReclaimCandidate(t *testing.T, rdb *redis.Client, id string, retryPhase bool, score float64) {
	t.Helper()
	ctx := context.Background()
	mustSubmit(t, integrationClient(t, rdb), id, "job", nil, WithSubmitMaxRetry(1))
	if claim, err := redisstore.Claim(ctx, rdb, claimParams("old-"+id)); err != nil || claim == nil || claim.TaskID != id {
		t.Fatalf("claim=%+v err=%v", claim, err)
	}
	source := claimLease
	if retryPhase {
		if applied, err := redisstore.ReportFailure(ctx, rdb, redisstore.FailureParams{TaskID: id, AttemptToken: "old-" + id, ErrorMessage: "job failed"}); !applied || err != nil {
			t.Fatalf("failure applied=%v err=%v", applied, err)
		}
		source = reclaimRetry
	}
	if err := rdb.ZAdd(ctx, source, redis.Z{Member: id, Score: score}).Err(); err != nil {
		t.Fatal(err)
	}
}

func TestReclaimRedisInvalidBatchMakesNoWrites(t *testing.T) {
	rdb := startSubmitRedis(t)
	ctx := context.Background()
	const bad = "taskq:meta:bad"
	for _, phase := range []struct {
		name, source string
		run          func(context.Context, *redis.Client) error
		retry        bool
	}{
		{"lease", claimLease, redisstore.ReclaimLeases, false},
		{"retry", reclaimRetry, redisstore.ScheduleRetries, true},
	} {
		t.Run(phase.name, func(t *testing.T) {
			type mutation struct {
				name   string
				change func() error
			}
			mutations := []mutation{
				{"missing-hash", func() error { return rdb.Del(ctx, bad).Err() }},
				{"wrong-hash-type", func() error { return rdb.Set(ctx, bad, "bad", 0).Err() }},
				{"missing-queue", func() error { return rdb.HDel(ctx, bad, "queue").Err() }},
				{"identity-mismatch", func() error { return rdb.HSet(ctx, bad, "task_id", "other").Err() }},
				{"terminal-residue", func() error { return rdb.HSet(ctx, bad, "status", "completed").Err() }},
				{"invalid-budget", func() error { return rdb.HSet(ctx, bad, "attempts", "2147483648").Err() }},
				{"over-budget", func() error { return rdb.HSet(ctx, bad, "attempts", "3").Err() }},
				{"empty-token", func() error { return rdb.HSet(ctx, bad, "attempt_token", "").Err() }},
				{"fractional-score", func() error { return rdb.ZAdd(ctx, phase.source, redis.Z{Member: "bad", Score: 2.5}).Err() }},
				{"negative-score", func() error { return rdb.ZAdd(ctx, phase.source, redis.Z{Member: "bad", Score: -1}).Err() }},
			}
			if phase.retry {
				mutations = append(mutations,
					mutation{"exhausted-budget", func() error { return rdb.HSet(ctx, bad, "attempts", "2").Err() }},
					mutation{"lease-residue", func() error { return rdb.ZAdd(ctx, claimLease, redis.Z{Member: "bad", Score: 2}).Err() }},
					mutation{"pending-type", func() error { return rdb.Set(ctx, claimPending, "bad", 0).Err() }},
				)
			} else {
				mutations = append(mutations,
					mutation{"active-missing", func() error { return rdb.SRem(ctx, claimActive, "bad").Err() }},
					mutation{"invalid-delay", func() error { return rdb.HSet(ctx, bad, "retry_delay_ms", "14999").Err() }},
					mutation{"retry-type", func() error { return rdb.Set(ctx, reclaimRetry, "bad", 0).Err() }},
				)
			}
			for _, tc := range mutations {
				t.Run(tc.name, func(t *testing.T) {
					if err := rdb.FlushDB(ctx).Err(); err != nil {
						t.Fatal(err)
					}
					seedReclaimCandidate(t, rdb, "good", phase.retry, 1)
					seedReclaimCandidate(t, rdb, "bad", phase.retry, 2)
					if err := tc.change(); err != nil {
						t.Fatal(err)
					}
					before := claimSnapshot(t, rdb)
					err := phase.run(ctx, rdb)
					if !errors.Is(err, redisstore.ErrReclaimInconsistent) {
						t.Fatalf("expected identifiable inconsistency, got %v", err)
					}
					if after := claimSnapshot(t, rdb); !reflect.DeepEqual(before, after) {
						t.Fatal("validation failure changed the batch")
					}
				})
			}
		})
	}
}

func TestReclaimRedisBatchLimit(t *testing.T) {
	rdb := startSubmitRedis(t)
	ctx := context.Background()
	for i := range 101 {
		seedReclaimCandidate(t, rdb, fmt.Sprintf("task-%03d", i), false, float64(i))
	}
	for pass, wantLeases := range []int64{1, 0} {
		if err := redisstore.ReclaimLeases(ctx, rdb); err != nil {
			t.Fatal(err)
		}
		leases, err := rdb.ZCard(ctx, claimLease).Result()
		if err != nil || leases != wantLeases || submitHash(t, rdb, "task-100")["status"] != []string{"running", "retry_wait"}[pass] {
			t.Fatalf("pass=%d leases=%d err=%v", pass, leases, err)
		}
	}
	ids, err := rdb.ZRange(ctx, reclaimRetry, 0, -1).Result()
	if err != nil || len(ids) != 101 {
		t.Fatalf("retry entries=%d err=%v", len(ids), err)
	}
	entries := make([]redis.Z, len(ids))
	for i, id := range ids {
		entries[i] = redis.Z{Member: id, Score: float64(i)}
	}
	if err := rdb.ZAdd(ctx, reclaimRetry, entries...).Err(); err != nil {
		t.Fatal(err)
	}
	for _, want := range [][]string{ids[:100], ids} {
		if err := redisstore.ScheduleRetries(ctx, rdb); err != nil {
			t.Fatal(err)
		}
		requirePending(t, rdb, want...)
	}
}

func TestReclaimRedisExactTimeAndOverflow(t *testing.T) {
	rdb := startSubmitRedis(t)
	ctx := context.Background()
	for _, tc := range []struct {
		name, seconds string
		score         float64
		retryPhase    bool
		count         int64
		wantError     string
	}{
		{"lease-equal", "100", 100000, false, 1, ""},
		{"lease-future", "100", 100001, false, 0, ""},
		{"retry-equal", "100", 100000, true, 1, ""},
		{"retry-future", "100", 100001, true, 0, ""},
		{"lease-overflow", "9007199254720", 2, false, 0, "retry timestamp exceeds exact integer range"},
		{"retry-invalid-score", "9007199254741", 1 << 53, true, 0, "invalid source score"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if err := rdb.FlushDB(ctx).Err(); err != nil {
				t.Fatal(err)
			}
			if tc.name == "lease-overflow" {
				seedReclaimCandidate(t, rdb, "good", false, 1)
				if err := rdb.HSet(ctx, "taskq:meta:good", "retry_delay_ms", "15000").Err(); err != nil {
					t.Fatal(err)
				}
			}
			seedReclaimCandidate(t, rdb, "task", tc.retryPhase, tc.score)
			before := claimSnapshot(t, rdb)
			prefix := `local backing=redis
local calls=0
local redis={error_reply=backing.error_reply,call=function(command,...)
if command=='TIME' then
    calls=calls+1
    if calls>1 then error('Redis TIME read twice') end
    return {'` + tc.seconds + `','0'}
end
return backing.call(command,...)
end}
`
			file := "reclaim_leases.lua"
			keys := []string{claimLease, claimActive, reclaimRetry}
			if tc.retryPhase {
				file = "schedule_retries.lua"
				keys = []string{reclaimRetry, claimActive, claimLease, claimPending}
			}
			common, err := os.ReadFile("internal/redisstore/scripts/reclaim_candidates.lua")
			if err != nil {
				t.Fatal(err)
			}
			body, err := os.ReadFile("internal/redisstore/scripts/" + file)
			if err != nil {
				t.Fatal(err)
			}
			count, err := rdb.Eval(ctx, prefix+string(common)+"\n"+string(body), keys, 100).Int64()
			if tc.wantError == "" && (err != nil || count != tc.count) || tc.wantError != "" && (err == nil || !strings.Contains(err.Error(), tc.wantError)) {
				t.Fatalf("count=%d err=%v", count, err)
			}
			if tc.count == 0 && !reflect.DeepEqual(before, claimSnapshot(t, rdb)) {
				t.Fatal("boundary rejection changed data")
			}
		})
	}
}

func TestReclaimRedisDueRetries(t *testing.T) {
	rdb := startSubmitRedis(t)
	ctx := context.Background()
	c := integrationClient(t, rdb)
	for _, id := range []string{"due", "waiting"} {
		mustSubmit(t, c, id, "job", nil, WithSubmitMaxRetry(1))
		if claim, err := redisstore.Claim(ctx, rdb, claimParams("old-"+id)); err != nil || claim == nil || claim.TaskID != id {
			t.Fatalf("claim=%+v err=%v", claim, err)
		}
		if applied, err := redisstore.ReportFailure(ctx, rdb, redisstore.FailureParams{TaskID: id, AttemptToken: "old-" + id, ErrorMessage: "job failed"}); !applied || err != nil {
			t.Fatalf("failure applied=%v err=%v", applied, err)
		}
	}
	mustSubmit(t, c, "already-pending", "job", nil)
	if err := rdb.ZAdd(ctx, reclaimRetry, redis.Z{Member: "due", Score: 1}).Err(); err != nil {
		t.Fatal(err)
	}
	want := submitHash(t, rdb, "due")
	want["status"] = "pending"
	waiting := submitHash(t, rdb, "waiting")
	for range 2 {
		if err := redisstore.ScheduleRetries(ctx, rdb); err != nil {
			t.Fatal(err)
		}
	}
	requirePending(t, rdb, "already-pending", "due")
	if got := submitHash(t, rdb, "due"); !reflect.DeepEqual(want, got) {
		t.Fatalf("changed unrelated fields: %v", got)
	}
	if got := submitHash(t, rdb, "waiting"); !reflect.DeepEqual(waiting, got) {
		t.Fatalf("moved a future retry: %v", got)
	}
	if ids, err := rdb.ZRange(ctx, reclaimRetry, 0, -1).Result(); err != nil || !reflect.DeepEqual(ids, []string{"waiting"}) {
		t.Fatalf("retry=%q err=%v", ids, err)
	}
}
