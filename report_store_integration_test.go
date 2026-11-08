//go:build integration

package taskq

import (
	"context"
	"errors"
	"math"
	"os"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/redis/go-redis/v9"

	"github.com/wang-kang-tuoinai/TaskHarbor/internal/redisstore"
)

func TestCompleteRedisResults(t *testing.T) {
	rdb := startSubmitRedis(t)
	ctx := context.Background()
	for i, result := range [][]byte{nil, {}, []byte("null"), []byte(`""`), []byte(`{"ok":true}`), []byte(strings.Repeat("a", 1000))} {
		id := strconv.Itoa(i)
		mustSubmit(t, integrationClient(t, rdb), id, "job", []byte("payload"))
		claimed, err := redisstore.Claim(ctx, rdb, claimParams("token"))
		if err != nil || claimed == nil {
			t.Fatalf("claim=%v err=%v", claimed, err)
		}
		if err := rdb.HSet(ctx, "taskq:meta:"+id, "last_error", "history", "last_failed_at", "123").Err(); err != nil {
			t.Fatal(err)
		}
		before := submitHash(t, rdb, id)
		began, err := rdb.Time(ctx).Result()
		if err != nil {
			t.Fatal(err)
		}
		applied, err := redisstore.Complete(ctx, rdb, redisstore.CompleteParams{TaskID: id, AttemptToken: "token", Result: result})
		if !applied || err != nil {
			t.Fatalf("applied=%v err=%v", applied, err)
		}
		ended, err := rdb.Time(ctx).Result()
		if err != nil {
			t.Fatal(err)
		}
		after := submitHash(t, rdb, id)
		finished, err := strconv.ParseInt(after["finished_at"], 10, 64)
		if err != nil || finished < began.UnixMilli() || finished > ended.UnixMilli() {
			t.Fatalf("finished=%v err=%v", finished, err)
		}
		if got, ok := after["result"]; !ok || got != string(result) || after["status"] != "completed" {
			t.Fatalf("hash=%q", after)
		}
		delete(before, "owner_id")
		delete(before, "attempt_token")
		before["result"] = string(result)
		before["status"] = "completed"
		before["finished_at"] = after["finished_at"]
		if !reflect.DeepEqual(before, after) {
			t.Fatalf("unexpected field changes: %q", after)
		}
		if n, err := rdb.Exists(ctx, claimActive, claimLease, "taskq:default:retry", claimPending).Result(); n != 0 || err != nil {
			t.Fatalf("indexes=%v err=%v", n, err)
		}
		// 大结果会使 Hash 转为哈希表，读取也可能推进内部 rehash；比较字段语义而非 DUMP 顺序。
		if applied, err := redisstore.Complete(ctx, rdb, redisstore.CompleteParams{TaskID: id, AttemptToken: "token", Result: []byte("different")}); applied || err != nil || !reflect.DeepEqual(after, submitHash(t, rdb, id)) {
			t.Fatalf("duplicate applied=%v err=%v", applied, err)
		}
		if n, err := rdb.Exists(ctx, claimActive, claimLease, "taskq:default:retry", claimPending).Result(); err != nil || n != 0 {
			t.Fatalf("duplicate changed indexes=%d err=%v", n, err)
		}
	}
}

func seedReporting(t *testing.T, rdb *redis.Client) {
	t.Helper()
	ctx := context.Background()
	if err := rdb.FlushDB(ctx).Err(); err != nil {
		t.Fatal(err)
	}
	mustSubmit(t, integrationClient(t, rdb), "task", "job", nil, WithSubmitMaxRetry(1))
	if claimed, err := redisstore.Claim(ctx, rdb, claimParams("token")); err != nil || claimed == nil {
		t.Fatalf("claim=%v err=%v", claimed, err)
	}
}

func callStoreReport(ctx context.Context, rdb *redis.Client, failure bool) (bool, error) {
	if failure {
		return redisstore.ReportFailure(ctx, rdb, redisstore.FailureParams{TaskID: "task", AttemptToken: "token", ErrorMessage: "failure"})
	}
	return redisstore.Complete(ctx, rdb, redisstore.CompleteParams{TaskID: "task", AttemptToken: "token", Result: []byte("result")})
}

func TestReportRedisRejectionsAndConsistency(t *testing.T) {
	rdb := startSubmitRedis(t)
	ctx := context.Background()
	type mutation struct {
		name         string
		change       func() error
		inconsistent bool
		failureOnly  bool
	}
	cases := []mutation{
		{"missing", func() error { return rdb.Del(ctx, "taskq:meta:task").Err() }, false, false},
		{"old-token", func() error { return rdb.HSet(ctx, "taskq:meta:task", "attempt_token", "new").Err() }, false, false},
		{"expired", func() error { return rdb.ZAdd(ctx, claimLease, redis.Z{Member: "task", Score: 1}).Err() }, false, false},
		{"active-missing", func() error { return rdb.SRem(ctx, claimActive, "task").Err() }, true, false},
		{"lease-missing", func() error { return rdb.ZRem(ctx, claimLease, "task").Err() }, true, false},
	}
	for _, key := range []string{"taskq:meta:task", claimActive, claimLease, "taskq:default:retry"} {
		cases = append(cases, mutation{"type-" + key, func() error { return rdb.Set(ctx, key, "bad", 0).Err() }, true, key == "taskq:default:retry"})
	}
	for field, values := range map[string][]string{"status": {"invalid", ""}, "attempt_token": {""}, "attempts": {"0", "-1", "1.5", "3", "2147483648"}, "max_retry": {"-1", "2147483647", "NaN"}, "retry_delay_ms": {"0", "14999", "604800001", "1.5", "inf"}} {
		for _, value := range values {
			cases = append(cases, mutation{field + "=" + value, func() error { return rdb.HSet(ctx, "taskq:meta:task", field, value).Err() }, true, field != "status" && field != "attempt_token"})
		}
		cases = append(cases, mutation{"missing-" + field, func() error { return rdb.HDel(ctx, "taskq:meta:task", field).Err() }, true, field != "status" && field != "attempt_token"})
	}
	for _, score := range []float64{-1, 1.5, 1 << 53, math.Inf(1), math.Inf(-1)} {
		cases = append(cases, mutation{"score-" + strconv.FormatFloat(score, 'g', -1, 64), func() error { return rdb.ZAdd(ctx, claimLease, redis.Z{Member: "task", Score: score}).Err() }, true, false})
	}
	for _, status := range []string{"pending", "retry_wait", "completed", "failed", "canceled"} {
		cases = append(cases, mutation{status, func() error {
			if err := rdb.HSet(ctx, "taskq:meta:task", "status", status).Err(); err != nil {
				return err
			}
			if err := rdb.HDel(ctx, "taskq:meta:task", "attempt_token").Err(); err != nil {
				return err
			}
			return rdb.Del(ctx, claimActive, claimLease).Err()
		}, false, false})
	}
	for _, failure := range []bool{false, true} {
		for _, tc := range cases {
			if tc.failureOnly && !failure {
				continue
			}
			t.Run(strconv.FormatBool(failure)+"/"+tc.name, func(t *testing.T) {
				seedReporting(t, rdb)
				if err := tc.change(); err != nil {
					t.Fatal(err)
				}
				before := claimSnapshot(t, rdb)
				applied, err := callStoreReport(ctx, rdb, failure)
				if applied || tc.inconsistent && !errors.Is(err, redisstore.ErrReportInconsistent) || !tc.inconsistent && err != nil {
					t.Fatalf("applied=%v err=%v", applied, err)
				}
				if !reflect.DeepEqual(before, claimSnapshot(t, rdb)) {
					t.Fatal("rejection or consistency error changed Redis data")
				}
			})
		}
	}
	// complete 不读取失败预算；额度耗尽的 ReportFailure 也不读取不需要的等待配置。
	seedReporting(t, rdb)
	if err := rdb.HDel(ctx, "taskq:meta:task", "attempts", "max_retry", "retry_delay_ms").Err(); err != nil {
		t.Fatal(err)
	}
	if applied, err := callStoreReport(ctx, rdb, false); !applied || err != nil {
		t.Fatalf("minimal complete=%v err=%v", applied, err)
	}
	seedReporting(t, rdb)
	if err := rdb.HSet(ctx, "taskq:meta:task", "max_retry", 0).Err(); err != nil {
		t.Fatal(err)
	}
	if err := rdb.HDel(ctx, "taskq:meta:task", "retry_delay_ms").Err(); err != nil {
		t.Fatal(err)
	}
	if applied, err := callStoreReport(ctx, rdb, true); !applied || err != nil {
		t.Fatalf("exhausted failure=%v err=%v", applied, err)
	}
}

func TestReportRedisExactTimeAndOverflow(t *testing.T) {
	rdb := startSubmitRedis(t)
	ctx := context.Background()
	authority, err := os.ReadFile("internal/redisstore/scripts/report_authority.lua")
	if err != nil {
		t.Fatal(err)
	}
	for _, failure := range []bool{false, true} {
		file := "complete_task.lua"
		if failure {
			file = "report_failure_task.lua"
		}
		body, err := os.ReadFile("internal/redisstore/scripts/" + file)
		if err != nil {
			t.Fatal(err)
		}
		for _, tc := range []struct {
			seconds   string
			score     float64
			want      int64
			wantError bool
		}{{"100", 100000, 0, false}, {"100", 100001, 1, false}, {"9007199254740", 9007199254740991, 0, failure}} {
			if !failure && tc.seconds != "100" {
				continue
			}
			seedReporting(t, rdb)
			if err := rdb.ZAdd(ctx, claimLease, redis.Z{Member: "task", Score: tc.score}).Err(); err != nil {
				t.Fatal(err)
			}
			before := claimSnapshot(t, rdb)
			prefix := `local backing=redis
local redis={error_reply=backing.error_reply,call=function(command,...)
if command=='TIME' then return {'` + tc.seconds + `','0'} end
return backing.call(command,...)
end}
`
			keys := []string{"taskq:meta:task", claimActive, claimLease}
			if failure {
				keys = append(keys, "taskq:default:retry")
			}
			applied, err := rdb.Eval(ctx, prefix+string(authority)+"\n"+string(body), keys, "task", "token", "content").Int64()
			if (err != nil) != tc.wantError || !tc.wantError && applied != tc.want {
				t.Fatalf("failure=%v seconds=%s applied=%v err=%v", failure, tc.seconds, applied, err)
			}
			if (tc.wantError || tc.want == 0) && !reflect.DeepEqual(before, claimSnapshot(t, rdb)) {
				t.Fatal("boundary rejection changed data")
			}
			if tc.want == 1 {
				fields := submitHash(t, rdb, "task")
				if failure {
					score, err := rdb.ZScore(ctx, "taskq:default:retry", "task").Result()
					if err != nil || score != 130000 || fields["last_failed_at"] != "100000" {
						t.Fatal("failure did not use same Redis TIME")
					}
				} else if fields["finished_at"] != "100000" {
					t.Fatal("complete did not use Redis TIME")
				}
			}
		}
	}
}

func TestReportRedisConcurrentAttempts(t *testing.T) {
	rdb := startSubmitRedis(t)
	ctx := context.Background()
	for range 12 {
		seedReporting(t, rdb)
		gate := make(chan struct{})
		var workers sync.WaitGroup
		results := make(chan bool, 2)
		for _, failure := range []bool{false, true} {
			workers.Go(func() {
				<-gate
				applied, err := callStoreReport(ctx, rdb, failure)
				if err != nil {
					t.Errorf("report: %v", err)
				}
				results <- applied
			})
		}
		close(gate)
		workers.Wait()
		wins := 0
		for range 2 {
			if <-results {
				wins++
			}
		}
		if wins != 1 {
			t.Fatalf("accepted %d concurrent results", wins)
		}
		before := claimSnapshot(t, rdb)
		for _, failure := range []bool{false, true} {
			if applied, err := callStoreReport(ctx, rdb, failure); applied || err != nil {
				t.Fatalf("duplicate=%v err=%v", applied, err)
			}
		}
		if !reflect.DeepEqual(before, claimSnapshot(t, rdb)) {
			t.Fatal("duplicate report changed first result")
		}
	}
	for _, failure := range []bool{false, true} {
		for _, status := range []string{"running", "canceled"} {
			seedReporting(t, rdb)
			gate := make(chan struct{})
			var workers sync.WaitGroup
			workers.Go(func() {
				<-gate
				_, err := callStoreReport(ctx, rdb, failure)
				if err != nil {
					t.Errorf("old report: %v", err)
				}
			})
			workers.Go(func() {
				<-gate
				err := rdb.Eval(ctx, `redis.call('HSET',KEYS[1],'status',ARGV[1],'attempt_token','new','attempts','2');redis.call('HDEL',KEYS[1],'result','finished_at','last_error','last_failed_at');redis.call('SADD',KEYS[2],'task');redis.call('ZADD',KEYS[3],9007199254740991,'task');redis.call('DEL',KEYS[4]);return 1`, []string{"taskq:meta:task", claimActive, claimLease, "taskq:default:retry"}, status).Err()
				if err != nil {
					t.Errorf("controlled transition: %v", err)
				}
			})
			close(gate)
			workers.Wait()
			fields := submitHash(t, rdb, "task")
			if fields["status"] != status || fields["attempt_token"] != "new" || fields["attempts"] != "2" || fields["result"] != "" || fields["last_error"] != "" {
				t.Fatalf("old report overwrote transition %q", fields)
			}
			before := claimSnapshot(t, rdb)
			if applied, err := callStoreReport(ctx, rdb, failure); applied || err != nil || !reflect.DeepEqual(before, claimSnapshot(t, rdb)) {
				t.Fatalf("old token report=%v err=%v", applied, err)
			}
		}
	}
}

func TestReportFailureRedisBudget(t *testing.T) {
	rdb := startSubmitRedis(t)
	ctx := context.Background()
	for _, retries := range []int{0, 1} {
		id := strconv.Itoa(retries)
		mustSubmit(t, integrationClient(t, rdb), id, "job", nil, WithSubmitMaxRetry(retries))
		claimed, err := redisstore.Claim(ctx, rdb, claimParams("token"))
		if err != nil || claimed == nil {
			t.Fatalf("claim=%v err=%v", claimed, err)
		}
		before := submitHash(t, rdb, id)
		applied, err := redisstore.ReportFailure(ctx, rdb, redisstore.FailureParams{TaskID: id, AttemptToken: "token", ErrorMessage: "business failure"})
		if !applied || err != nil {
			t.Fatalf("applied=%v err=%v", applied, err)
		}
		after := submitHash(t, rdb, id)
		failed, err := strconv.ParseInt(after["last_failed_at"], 10, 64)
		if err != nil || failed <= 0 {
			t.Fatalf("failed=%d err=%v", failed, err)
		}
		delete(before, "owner_id")
		delete(before, "attempt_token")
		before["last_error"] = "business failure"
		before["last_failed_at"] = after["last_failed_at"]
		if retries == 0 {
			before["status"] = "failed"
			before["finished_at"] = after["last_failed_at"]
			if n, err := rdb.Exists(ctx, "taskq:default:retry").Result(); err != nil || n != 0 {
				t.Fatalf("retry exists=%d err=%v", n, err)
			}
		} else {
			before["status"] = "retry_wait"
			if score, err := rdb.ZScore(ctx, "taskq:default:retry", id).Result(); err != nil || score != float64(failed+30000) {
				t.Fatalf("retry score=%v err=%v", score, err)
			}
		}
		if !reflect.DeepEqual(before, after) {
			t.Fatalf("hash=%q expected=%q", after, before)
		}
		if n, err := rdb.Exists(ctx, claimActive, claimLease, claimPending).Result(); err != nil || n != 0 {
			t.Fatalf("execution indexes=%d err=%v", n, err)
		}
		snapshot := claimSnapshot(t, rdb)
		if applied, err := redisstore.ReportFailure(ctx, rdb, redisstore.FailureParams{TaskID: id, AttemptToken: "token", ErrorMessage: "other"}); applied || err != nil || !reflect.DeepEqual(snapshot, claimSnapshot(t, rdb)) {
			t.Fatalf("duplicate=%v err=%v", applied, err)
		}
	}
}
